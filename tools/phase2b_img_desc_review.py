"""
Phase2B img_description + DeepSeek增量补全审核报告生成器。

目标：
1) 支持 latest/all/显式任务 三种模式批量生成；
2) 每个任务输出到对应 intermediates 目录；
3) JSON 缩进可配置；
4) 报告中不输出 system prompt。
"""

from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


DEFAULT_STORAGE_ROOT = Path("var/storage/storage")
DEFAULT_OUTPUT_NAME = "phase2b_img_desc_augment_review.json"
DEFAULT_MODE = "latest"
DEFAULT_INDENT = 2
IMG_DESC_STEP_NAME = "img_desc_augment"


@dataclass(frozen=True)
class ReviewConfig:
    storage_root: Path
    mode: str
    task_dirs: Tuple[str, ...]
    task_ids: Tuple[str, ...]
    output_name: str
    indent: int
    include_user_prompt: bool
    max_user_prompt_chars: int
    strict: bool


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        with open(path, "r", encoding="utf-8") as file_obj:
            return json.load(file_obj)
    except Exception:
        return default


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    try:
        with open(path, "r", encoding="utf-8") as file_obj:
            for raw in file_obj:
                line = raw.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except Exception:
                    continue
                if isinstance(payload, dict):
                    rows.append(payload)
    except Exception:
        return []
    return rows


def _bool_value(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"1", "true", "yes", "y", "on"}:
            return True
        if text in {"0", "false", "no", "n", "off"}:
            return False
    return default


def _str_list(value: Any) -> Tuple[str, ...]:
    if value is None:
        return tuple()
    if isinstance(value, (list, tuple)):
        return tuple(str(item).strip() for item in value if str(item).strip())
    text = str(value).strip()
    if not text:
        return tuple()
    return (text,)


def _merge_config(args: argparse.Namespace) -> ReviewConfig:
    config_file = Path(args.config).resolve() if args.config else None
    config_payload = _load_json(config_file, {}) if config_file else {}
    if not isinstance(config_payload, dict):
        config_payload = {}

    storage_root_raw = args.storage_root if args.storage_root is not None else config_payload.get("storage_root")
    mode_raw = args.mode if args.mode is not None else config_payload.get("mode")
    task_dirs_raw = args.task_dir if args.task_dir else config_payload.get("task_dirs")
    task_ids_raw = args.task_id if args.task_id else config_payload.get("task_ids")
    output_name_raw = args.output_name if args.output_name is not None else config_payload.get("output_name")
    indent_raw = args.indent if args.indent is not None else config_payload.get("indent")
    include_user_prompt_raw = (
        args.include_user_prompt if args.include_user_prompt is not None else config_payload.get("include_user_prompt")
    )
    max_user_prompt_chars_raw = (
        args.max_user_prompt_chars
        if args.max_user_prompt_chars is not None
        else config_payload.get("max_user_prompt_chars")
    )
    strict_raw = args.strict if args.strict is not None else config_payload.get("strict")

    storage_root = Path(str(storage_root_raw or DEFAULT_STORAGE_ROOT)).resolve()
    mode = str(mode_raw or DEFAULT_MODE).strip().lower()
    if mode not in {"latest", "all", "explicit"}:
        mode = DEFAULT_MODE

    output_name = str(output_name_raw or DEFAULT_OUTPUT_NAME).strip() or DEFAULT_OUTPUT_NAME

    try:
        indent = int(indent_raw if indent_raw is not None else DEFAULT_INDENT)
    except Exception:
        indent = DEFAULT_INDENT
    if indent < 0:
        indent = DEFAULT_INDENT

    try:
        max_user_prompt_chars = int(max_user_prompt_chars_raw if max_user_prompt_chars_raw is not None else 0)
    except Exception:
        max_user_prompt_chars = 0
    if max_user_prompt_chars < 0:
        max_user_prompt_chars = 0

    return ReviewConfig(
        storage_root=storage_root,
        mode=mode,
        task_dirs=_str_list(task_dirs_raw),
        task_ids=_str_list(task_ids_raw),
        output_name=output_name,
        indent=indent,
        include_user_prompt=_bool_value(include_user_prompt_raw, False),
        max_user_prompt_chars=max_user_prompt_chars,
        strict=_bool_value(strict_raw, False),
    )


def _dedupe_paths(paths: Iterable[Path]) -> List[Path]:
    seen: set[str] = set()
    output: List[Path] = []
    for path in paths:
        key = str(path.resolve())
        if key in seen:
            continue
        seen.add(key)
        output.append(path.resolve())
    return output


def _discover_task_dirs(config: ReviewConfig) -> List[Path]:
    explicit: List[Path] = []
    for task_dir in config.task_dirs:
        path = Path(task_dir).resolve()
        if path.exists() and path.is_dir():
            explicit.append(path)

    for task_id in config.task_ids:
        path = (config.storage_root / task_id).resolve()
        if path.exists() and path.is_dir():
            explicit.append(path)

    if config.mode == "explicit":
        return _dedupe_paths(explicit)

    candidates: List[Path] = []
    if config.storage_root.exists() and config.storage_root.is_dir():
        for item in config.storage_root.iterdir():
            if item.is_dir():
                candidates.append(item.resolve())
    candidates.sort(key=lambda item: item.stat().st_mtime, reverse=True)

    if config.mode == "all":
        return _dedupe_paths([*explicit, *candidates])

    # latest
    latest_candidates = [c for c in candidates if (c / "result.json").exists()]
    chosen = latest_candidates[0:1] if latest_candidates else candidates[0:1]
    return _dedupe_paths([*explicit, *chosen])


def _preview_text(text: str, max_chars: int) -> str:
    raw = str(text or "").strip()
    if max_chars <= 0 or len(raw) <= max_chars:
        return raw
    return raw[:max_chars] + "...[TRUNCATED]"


def _is_placeholder_desc(desc: str, label: str = "", source_id: str = "") -> bool:
    text = str(desc or "").strip()
    lower = text.lower()
    if not text:
        return True
    if lower in {"description unavailable", "fallback_unit_scan", "n/a", "na", "unknown"}:
        return True
    if re.fullmatch(r"image[_-]?\d+", lower):
        return True
    if label and text == str(label).strip():
        return True
    if source_id and text == str(source_id).strip():
        return True
    return False


def _phase2a_text_index(payload: Any) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    if not isinstance(payload, list):
        return mapping
    for item in payload:
        if not isinstance(item, dict):
            continue
        unit_id = str(item.get("unit_id") or "").strip()
        if not unit_id:
            continue
        mapping[unit_id] = str(item.get("text") or item.get("full_text") or "").strip()
    return mapping


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _collect_deepseek_trace_by_unit(
    llm_trace_rows: List[Dict[str, Any]],
    include_user_prompt: bool,
    max_user_prompt_chars: int,
) -> Dict[str, List[Dict[str, Any]]]:
    by_unit: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in llm_trace_rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("step_name") or "").strip() != IMG_DESC_STEP_NAME:
            continue
        unit_id = str(row.get("unit_id") or "").strip()
        record = {
            "timestamp": str(row.get("timestamp") or ""),
            "success": bool(row.get("success", False)),
            "duration_ms": _safe_float(row.get("duration_ms"), 0.0),
            "model": str(row.get("model") or ""),
            "prompt_tokens": _safe_int(row.get("prompt_tokens"), 0),
            "completion_tokens": _safe_int(row.get("completion_tokens"), 0),
            "total_tokens": _safe_int(row.get("total_tokens"), 0),
            "response_chars": len(str(row.get("response_text") or "")),
            "error": str(row.get("error") or ""),
        }
        # 明确不输出 system_prompt。
        if include_user_prompt:
            record["user_prompt_preview"] = _preview_text(str(row.get("user_prompt") or ""), max_user_prompt_chars)
        by_unit[unit_id].append(record)
    return by_unit


def _build_review_payload(task_dir: Path, config: ReviewConfig) -> Dict[str, Any]:
    inter = task_dir / "intermediates"
    result_path = task_dir / "result.json"
    phase2a_path = inter / "semantic_units_phase2a.json"
    if not phase2a_path.exists():
        fallback_phase2a = task_dir / "semantic_units_phase2a.json"
        if fallback_phase2a.exists():
            phase2a_path = fallback_phase2a

    deepseek_audit_path = inter / "phase2b_deepseek_call_audit.json"
    image_match_audit_path = inter / "phase2b_image_match_audit.json"
    llm_trace_path = inter / "phase2b_llm_trace.jsonl"

    result_obj = _load_json(result_path, {})
    phase2a_obj = _load_json(phase2a_path, [])
    deepseek_obj = _load_json(deepseek_audit_path, {})
    image_match_obj = _load_json(image_match_audit_path, {})
    llm_rows = _load_jsonl(llm_trace_path)

    sections = result_obj.get("sections", []) if isinstance(result_obj, dict) else []
    if not isinstance(sections, list):
        sections = []

    phase2a_text_by_uid = _phase2a_text_index(phase2a_obj)

    deepseek_records = deepseek_obj.get("records", []) if isinstance(deepseek_obj, dict) else []
    if not isinstance(deepseek_records, list):
        deepseek_records = []

    image_records = image_match_obj.get("records", []) if isinstance(image_match_obj, dict) else []
    if not isinstance(image_records, list):
        image_records = []

    trace_by_unit = _collect_deepseek_trace_by_unit(
        llm_trace_rows=llm_rows,
        include_user_prompt=config.include_user_prompt,
        max_user_prompt_chars=config.max_user_prompt_chars,
    )

    sections_total = 0
    sections_with_images = 0
    sections_changed = 0
    sections_with_images_changed = 0
    sections_with_deepseek_calls = 0
    sections_with_deepseek_calls_changed = 0
    screenshot_items_total = 0
    img_desc_non_empty_count = 0
    img_desc_effective_count = 0
    img_desc_placeholder_count = 0
    alignment_evidence_count = 0

    unit_rows: List[Dict[str, Any]] = []

    for section in sections:
        if not isinstance(section, dict):
            continue
        unit_id = str(section.get("unit_id") or "").strip()
        if not unit_id:
            continue

        sections_total += 1
        knowledge_type = str(section.get("knowledge_type") or "").strip()
        materials = section.get("materials", {})
        if not isinstance(materials, dict):
            materials = {}
        screenshot_items = materials.get("screenshot_items", [])
        if not isinstance(screenshot_items, list):
            screenshot_items = []

        base_text = phase2a_text_by_uid.get(unit_id, "")
        result_text = str(section.get("body_text") or "").strip()
        text_changed = (base_text != result_text)
        if text_changed:
            sections_changed += 1

        unit_item_total = 0
        unit_non_empty = 0
        unit_effective = 0
        unit_placeholder = 0
        unit_alignment = 0

        fallback_examples: List[Dict[str, Any]] = []
        effective_examples: List[Dict[str, Any]] = []

        for item in screenshot_items:
            if not isinstance(item, dict):
                continue
            unit_item_total += 1

            img_description = str(item.get("img_description") or item.get("img_desription") or "").strip()
            label = str(item.get("label") or "").strip()
            source_id = str(item.get("source_id") or "").strip()
            sentence_id = str(item.get("sentence_id") or "").strip()
            sentence_text = str(item.get("sentence_text") or "").strip()
            timestamp_sec = item.get("timestamp_sec")

            has_alignment = bool(sentence_id or sentence_text or timestamp_sec not in (None, ""))
            placeholder = _is_placeholder_desc(img_description, label=label, source_id=source_id)

            if img_description:
                unit_non_empty += 1
            if has_alignment:
                unit_alignment += 1
            if placeholder:
                unit_placeholder += 1
                if len(fallback_examples) < 2:
                    fallback_examples.append(
                        {
                            "img_id": str(item.get("img_id") or ""),
                            "label": label,
                            "img_description": img_description,
                        }
                    )
            else:
                unit_effective += 1
                if len(effective_examples) < 2:
                    effective_examples.append(
                        {
                            "img_id": str(item.get("img_id") or ""),
                            "img_description": img_description,
                        }
                    )

        if unit_item_total > 0:
            sections_with_images += 1
            if text_changed:
                sections_with_images_changed += 1

        deepseek_calls = trace_by_unit.get(unit_id, [])
        deepseek_call_count = len(deepseek_calls)
        deepseek_success_count = sum(1 for call in deepseek_calls if bool(call.get("success", False)))
        if deepseek_call_count > 0:
            sections_with_deepseek_calls += 1
            if text_changed:
                sections_with_deepseek_calls_changed += 1

        screenshot_items_total += unit_item_total
        img_desc_non_empty_count += unit_non_empty
        img_desc_effective_count += unit_effective
        img_desc_placeholder_count += unit_placeholder
        alignment_evidence_count += unit_alignment

        notes: List[str] = []
        if unit_item_total > 0 and unit_effective == 0:
            notes.append("all_img_description_fallback_or_placeholder")
        if unit_item_total > 0 and unit_alignment < unit_item_total:
            notes.append("alignment_evidence_incomplete")
        if deepseek_call_count > 0 and not text_changed:
            notes.append("deepseek_called_but_no_observable_body_change")

        unit_payload = {
            "unit_id": unit_id,
            "knowledge_type": knowledge_type,
            "screenshot_item_count": unit_item_total,
            "img_description_non_empty_count": unit_non_empty,
            "img_description_effective_count": unit_effective,
            "img_description_placeholder_count": unit_placeholder,
            "alignment_evidence_count": unit_alignment,
            "phase2a_text_chars": len(base_text),
            "result_text_chars": len(result_text),
            "body_text_changed_vs_phase2a": text_changed,
            "deepseek_img_desc_augment_calls": deepseek_call_count,
            "deepseek_img_desc_augment_success_calls": deepseek_success_count,
            "fallback_examples": fallback_examples,
            "effective_examples": effective_examples,
            "notes": notes,
        }
        if deepseek_calls:
            unit_payload["deepseek_calls"] = deepseek_calls
        unit_rows.append(unit_payload)

    unit_rows.sort(key=lambda item: str(item.get("unit_id") or ""))

    deepseek_total_calls = _safe_int(deepseek_obj.get("total_calls"), len(deepseek_records))
    deepseek_success_calls = 0
    deepseek_failed_calls = 0
    for record in deepseek_records:
        if not isinstance(record, dict):
            continue
        output_obj = record.get("output", {})
        if not isinstance(output_obj, dict):
            deepseek_failed_calls += 1
            continue
        success = bool(output_obj.get("success", False))
        has_error = bool(str(output_obj.get("error") or "").strip())
        if success and not has_error:
            deepseek_success_calls += 1
        else:
            deepseek_failed_calls += 1

    trace_img_desc_calls = sum(len(calls) for calls in trace_by_unit.values())
    trace_img_desc_success_calls = sum(
        1 for calls in trace_by_unit.values() for call in calls if bool(call.get("success", False))
    )

    effective_ratio = (img_desc_effective_count / screenshot_items_total) if screenshot_items_total else 0.0
    alignment_ratio = (alignment_evidence_count / screenshot_items_total) if screenshot_items_total else 0.0

    units_with_all_placeholder = [
        str(item.get("unit_id") or "")
        for item in unit_rows
        if int(item.get("screenshot_item_count", 0) or 0) > 0 and int(item.get("img_description_effective_count", 0) or 0) == 0
    ]

    risk_findings: List[str] = []
    if screenshot_items_total > 0 and effective_ratio < 0.5:
        risk_findings.append("effective img_description ratio is below 50%, augmentation evidence quality is weak")
    if deepseek_total_calls == 0 and trace_img_desc_calls == 0:
        risk_findings.append("deepseek img_desc_augment calls are zero; incremental augmentation did not execute")
    if sections_changed == 0:
        risk_findings.append("no section body_text change vs phase2a, no observable incremental augmentation effect")

    payload = {
        "review_version": "2.0",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "task_dir": str(task_dir),
        "summary": {
            "sections_total": sections_total,
            "sections_with_screenshot_items": sections_with_images,
            "sections_body_text_changed_vs_phase2a": sections_changed,
            "sections_with_screenshot_items_changed": sections_with_images_changed,
            "sections_with_deepseek_img_desc_calls": sections_with_deepseek_calls,
            "sections_with_deepseek_img_desc_calls_changed": sections_with_deepseek_calls_changed,
            "screenshot_items_total": screenshot_items_total,
            "img_description_non_empty_count": img_desc_non_empty_count,
            "img_description_effective_count": img_desc_effective_count,
            "img_description_placeholder_count": img_desc_placeholder_count,
            "img_description_effective_ratio": round(effective_ratio, 4),
            "alignment_evidence_count": alignment_evidence_count,
            "alignment_evidence_ratio": round(alignment_ratio, 4),
            "deepseek_img_desc_augment_calls_from_audit": deepseek_total_calls,
            "deepseek_img_desc_augment_success_calls_from_audit": deepseek_success_calls,
            "deepseek_img_desc_augment_failed_calls_from_audit": deepseek_failed_calls,
            "deepseek_img_desc_augment_calls_from_trace": trace_img_desc_calls,
            "deepseek_img_desc_augment_success_calls_from_trace": trace_img_desc_success_calls,
            "units_with_all_placeholder_descriptions": units_with_all_placeholder,
        },
        "risk_findings": risk_findings,
        "unit_breakdown": unit_rows,
        "sources": {
            "result_json": str(result_path),
            "semantic_units_phase2a_json": str(phase2a_path),
            "phase2b_deepseek_call_audit_json": str(deepseek_audit_path),
            "phase2b_llm_trace_jsonl": str(llm_trace_path),
            "phase2b_image_match_audit_json": str(image_match_audit_path),
        },
        "constraints": {
            "no_system_content_included": True,
            "include_user_prompt": config.include_user_prompt,
            "max_user_prompt_chars": config.max_user_prompt_chars,
        },
    }
    return payload


def _write_review_file(task_dir: Path, output_name: str, payload: Dict[str, Any], indent: int) -> Path:
    output_path = task_dir / "intermediates" / output_name
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as file_obj:
        json.dump(payload, file_obj, ensure_ascii=False, indent=indent)
    return output_path


def _validate_task_dir(task_dir: Path) -> Tuple[bool, str]:
    result_path = task_dir / "result.json"
    if not result_path.exists():
        return False, f"missing file: {result_path}"
    return True, ""


def run(config: ReviewConfig) -> int:
    task_dirs = _discover_task_dirs(config)
    if not task_dirs:
        print("No task directories found.")
        return 2

    exit_code = 0
    for task_dir in task_dirs:
        ok, message = _validate_task_dir(task_dir)
        if not ok:
            line = f"[SKIP] {task_dir} -> {message}"
            if config.strict:
                print(f"[ERROR] {line}")
                return 3
            print(line)
            continue

        try:
            payload = _build_review_payload(task_dir=task_dir, config=config)
            output_path = _write_review_file(
                task_dir=task_dir,
                output_name=config.output_name,
                payload=payload,
                indent=config.indent,
            )
            summary = payload.get("summary", {})
            print(
                "[OK] "
                f"task={task_dir.name} -> {output_path} | "
                f"img_items={summary.get('screenshot_items_total', 0)} | "
                f"effective_desc={summary.get('img_description_effective_count', 0)} | "
                f"deepseek_calls(trace)={summary.get('deepseek_img_desc_augment_calls_from_trace', 0)}"
            )
        except Exception as exc:
            exit_code = 1
            print(f"[ERROR] {task_dir}: {exc}")
            if config.strict:
                return 4
    return exit_code


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate Phase2B img_description + DeepSeek augment review JSON for one or many tasks.",
    )
    parser.add_argument("--config", help="Path to JSON config file", default=None)
    parser.add_argument("--storage-root", help="Task root directory, e.g. var/storage/storage", default=None)
    parser.add_argument(
        "--mode",
        choices=["latest", "all", "explicit"],
        default=None,
        help="Task discovery mode. latest: newest task, all: all tasks, explicit: only task-dir/task-id.",
    )
    parser.add_argument("--task-dir", action="append", help="Explicit task directory path (can repeat)", default=None)
    parser.add_argument("--task-id", action="append", help="Explicit task id under storage root (can repeat)", default=None)
    parser.add_argument("--output-name", help="Output file name under intermediates/", default=None)
    parser.add_argument("--indent", type=int, help="JSON indentation spaces", default=None)
    parser.add_argument(
        "--include-user-prompt",
        dest="include_user_prompt",
        action="store_true",
        help="Include user_prompt preview in report (system_prompt is always excluded).",
    )
    parser.add_argument(
        "--exclude-user-prompt",
        dest="include_user_prompt",
        action="store_false",
        help="Do not include user_prompt preview.",
    )
    parser.set_defaults(include_user_prompt=None)
    parser.add_argument(
        "--max-user-prompt-chars",
        type=int,
        default=None,
        help="Max chars for user_prompt preview; <=0 means no truncation.",
    )
    parser.add_argument(
        "--strict",
        dest="strict",
        action="store_true",
        help="Fail fast when task file is missing.",
    )
    parser.add_argument(
        "--non-strict",
        dest="strict",
        action="store_false",
        help="Skip invalid task and continue.",
    )
    parser.set_defaults(strict=None)
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    config = _merge_config(args)
    code = run(config)
    raise SystemExit(code)


if __name__ == "__main__":
    main()
