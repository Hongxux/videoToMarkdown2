"""
模块说明：Phase2B 视频级分类服务。
执行逻辑：
1) 在 Phase2B 最终产物落盘后，抽取标题、多组正文证据和大纲组名。
2) 复用 category_classifier prompt 调用 LLM，并通过二次校验压制示例串台。
3) 立即回写分类路径库、任务级分类结果、video_meta.json 和 var/storage 汇总 JSON。
实现方式：prompt_loader + llm_gateway + 文件级原子回写。
核心价值：把视频分类正式纳入 Phase2B 末尾，而不是依赖离线脚本补跑。
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from services.python_grpc.src.content_pipeline.common.utils.json_payload_repair import (
    extract_top_level_objects,
    parse_json_payload,
)
from services.python_grpc.src.content_pipeline.infra.llm import llm_gateway
from services.python_grpc.src.content_pipeline.infra.llm.prompt_loader import get_prompt, render_prompt
from services.python_grpc.src.content_pipeline.infra.llm.prompt_registry import PromptKeys
from services.python_grpc.src.common.utils.runtime_recovery_store import RuntimeRecoveryStore


logger = logging.getLogger(__name__)

_DEFAULT_DEEPSEEK_BASE_URL = "https://api.deepseek.com/v1"
_DEFAULT_DEEPSEEK_MODEL = "deepseek-chat"
_DEFAULT_CATEGORY_TARGET_LEVEL = 2
_DEFAULT_CATEGORY_MAX_TARGET_LEVEL = 4
_DEFAULT_CATEGORY_LEAF_TASK_LIMIT = 10
_CATEGORY_EVIDENCE_MAX_GROUPS = 8
_CATEGORY_EVIDENCE_UNITS_PER_GROUP = 2
_CATEGORY_EVIDENCE_UNIT_CHARS = 320
_CATEGORY_EVIDENCE_TOTAL_CHARS = 4000
_CATEGORY_LIBRARY_FILE = "category_paths.txt"
_TASK_CLASSIFICATION_FILE = "category_classification.json"
_SUMMARY_JSON_FILE = "category_classification_results.json"


@dataclass
class VideoCategoryInput:
    """封装单个视频分类所需的最小输入。"""

    task_dir: Path
    title: str
    first_unit_text: str
    group_names: List[str]
    content_evidence_text: str
    group_evidence: List[Dict[str, Any]]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: Path) -> Dict[str, Any]:
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    if path.name == "result.json":
        try:
            store = RuntimeRecoveryStore(
                output_dir=str(path.parent),
                task_id=path.parent.name,
                storage_key=path.parent.name,
            )
            payload = store.load_projection_payload(
                stage="phase2b",
                projection_name="result_document",
            )
            if isinstance(payload, dict):
                return payload
        except Exception as error:
            logger.warning("[Phase2B-Category] load result artifact failed: path=%s err=%s", path, error)
    raise FileNotFoundError(str(path))


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _normalize_lines(lines: Iterable[str]) -> List[str]:
    values: List[str] = []
    seen: set[str] = set()
    for line in lines:
        value = str(line or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        values.append(value)
    return values


def _normalize_category_path(value: str) -> str:
    return "/".join(part.strip() for part in str(value or "").replace("\\", "/").split("/") if part.strip())


def _split_category_path(value: str) -> List[str]:
    normalized = _normalize_category_path(value)
    if not normalized:
        return []
    return normalized.split("/")


def _category_depth(value: str) -> int:
    return len(_split_category_path(value))


def _load_category_library(path: Path) -> List[str]:
    if not path.exists():
        return []
    return _normalize_lines(path.read_text(encoding="utf-8").splitlines())


def _write_category_library(path: Path, categories: Iterable[str]) -> None:
    normalized = sorted(_normalize_lines(categories))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(normalized) + ("\n" if normalized else ""),
        encoding="utf-8",
    )


def _compact_evidence_text(value: Any, *, max_chars: int) -> str:
    text = " ".join(str(value or "").split())
    if not text:
        return ""
    if len(text) <= max_chars:
        return text
    return text[: max(0, max_chars - 3)].rstrip() + "..."


def _extract_first_unit_text(result_payload: Dict[str, Any]) -> str:
    groups = result_payload.get("knowledge_groups")
    if not isinstance(groups, list):
        return ""
    for group in groups:
        if not isinstance(group, dict):
            continue
        units = group.get("units")
        if not isinstance(units, list):
            continue
        for unit in units:
            if not isinstance(unit, dict):
                continue
            text = str(unit.get("body_text") or unit.get("text") or "").strip()
            if text:
                return text[:2000]
    return ""


def _extract_group_names(result_payload: Dict[str, Any]) -> List[str]:
    groups = result_payload.get("knowledge_groups")
    if not isinstance(groups, list):
        return []
    return _normalize_lines(
        str(group.get("group_name") or "").strip()
        for group in groups
        if isinstance(group, dict)
    )


def _extract_group_evidence(result_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    groups = result_payload.get("knowledge_groups")
    if not isinstance(groups, list):
        return []

    evidence: List[Dict[str, Any]] = []
    total_chars = 0
    for index, group in enumerate(groups, start=1):
        if not isinstance(group, dict):
            continue
        units = group.get("units")
        if not isinstance(units, list):
            continue
        excerpts: List[str] = []
        for unit in units:
            if not isinstance(unit, dict):
                continue
            excerpt = _compact_evidence_text(
                unit.get("body_text") or unit.get("text") or "",
                max_chars=_CATEGORY_EVIDENCE_UNIT_CHARS,
            )
            if not excerpt:
                continue
            excerpts.append(excerpt)
            if len(excerpts) >= _CATEGORY_EVIDENCE_UNITS_PER_GROUP:
                break
        if not excerpts:
            continue
        group_name = str(group.get("group_name") or "").strip() or f"知识点分组{index}"
        estimated_chars = len(group_name) + sum(len(item) for item in excerpts)
        if evidence and total_chars + estimated_chars > _CATEGORY_EVIDENCE_TOTAL_CHARS:
            break
        evidence.append(
            {
                "group_name": group_name,
                "unit_excerpts": excerpts,
            }
        )
        total_chars += estimated_chars
        if len(evidence) >= _CATEGORY_EVIDENCE_MAX_GROUPS:
            break
    return evidence


def _build_content_evidence_text(group_evidence: Iterable[Dict[str, Any]]) -> str:
    blocks: List[str] = []
    for item in group_evidence:
        group_name = str(item.get("group_name") or "").strip()
        excerpts = item.get("unit_excerpts")
        excerpt_text = ""
        if isinstance(excerpts, list):
            excerpt_text = " ".join(
                _compact_evidence_text(excerpt, max_chars=_CATEGORY_EVIDENCE_UNIT_CHARS)
                for excerpt in excerpts
                if str(excerpt or "").strip()
            ).strip()
        if group_name and excerpt_text:
            blocks.append(f"{group_name}: {excerpt_text}")
        elif excerpt_text:
            blocks.append(excerpt_text)
        elif group_name:
            blocks.append(group_name)
    joined = "\n".join(blocks).strip()
    if len(joined) <= _CATEGORY_EVIDENCE_TOTAL_CHARS:
        return joined
    return joined[: max(0, _CATEGORY_EVIDENCE_TOTAL_CHARS - 3)].rstrip() + "..."


def _format_group_evidence_for_prompt(group_evidence: Iterable[Dict[str, Any]]) -> str:
    lines: List[str] = []
    for index, item in enumerate(group_evidence, start=1):
        group_name = str(item.get("group_name") or "").strip() or f"知识点分组{index}"
        lines.append(f"{index}. {group_name}")
        excerpts = item.get("unit_excerpts")
        if not isinstance(excerpts, list):
            continue
        for excerpt_index, excerpt in enumerate(excerpts, start=1):
            text = _compact_evidence_text(excerpt, max_chars=_CATEGORY_EVIDENCE_UNIT_CHARS)
            if text:
                lines.append(f"   - 证据{excerpt_index}: {text}")
    if not lines:
        return "(无可用正文证据)"
    return "\n".join(lines)


def _build_video_input(task_dir: Path, title: str = "") -> VideoCategoryInput:
    video_meta_path = task_dir / "video_meta.json"
    result_path = task_dir / "result.json"
    result_payload = _read_json(result_path)
    title_candidates = [
        str(title or "").strip(),
        str(result_payload.get("title") or "").strip(),
    ]
    if video_meta_path.exists():
        video_meta = _read_json(video_meta_path)
        title_candidates.insert(1, str(video_meta.get("title") or "").strip())

    resolved_title = next((item for item in title_candidates if item), "")
    first_unit_text = _extract_first_unit_text(result_payload)
    group_names = _extract_group_names(result_payload)
    group_evidence = _extract_group_evidence(result_payload)
    if not group_names and group_evidence:
        group_names = _normalize_lines(item.get("group_name") for item in group_evidence)
    content_evidence_text = _build_content_evidence_text(group_evidence)
    if not first_unit_text:
        first_unit_text = _compact_evidence_text(content_evidence_text, max_chars=2000)
    if not resolved_title or not content_evidence_text or not group_names:
        raise ValueError(
            f"分类输入不完整: title={bool(resolved_title)}, "
            f"content_evidence_text={bool(content_evidence_text)}, group_names={bool(group_names)}"
        )
    return VideoCategoryInput(
        task_dir=task_dir,
        title=resolved_title,
        first_unit_text=first_unit_text,
        group_names=group_names,
        content_evidence_text=content_evidence_text,
        group_evidence=group_evidence,
    )


def _extract_json_object(text: str) -> Dict[str, Any]:
    payload, error = parse_json_payload(text)
    if isinstance(payload, dict):
        return payload
    for candidate in extract_top_level_objects(text):
        payload, error = parse_json_payload(candidate)
        if isinstance(payload, dict):
            return payload
    raise ValueError(f"模型输出不是合法 JSON: {error or text[:200]}")


def _normalize_category_result(
    payload: Dict[str, Any],
    categories: List[str],
    *,
    target_level: int,
    max_target_level: int,
    required_prefix: str = "",
) -> Dict[str, Any]:
    category_path = _normalize_category_path(payload.get("category_path") or "")
    reasoning = str(payload.get("reasoning") or "").strip()
    path_parts = _split_category_path(category_path)
    depth = len(path_parts)
    if not category_path or depth < target_level or depth > max_target_level:
        raise ValueError(f"非法分类路径: {category_path!r}")
    normalized_prefix = _normalize_category_path(required_prefix)
    if normalized_prefix:
        prefix_parts = _split_category_path(normalized_prefix)
        if path_parts[: len(prefix_parts)] != prefix_parts:
            raise ValueError(
                f"分类路径 {category_path!r} 没有在指定父目录 {normalized_prefix!r} 下进一步细分"
            )

    actual_is_new = category_path not in set(categories)
    is_new_value = payload.get("is_new")
    if not isinstance(is_new_value, bool) or bool(is_new_value) != actual_is_new:
        is_new_value = actual_is_new

    return {
        "category_path": category_path,
        "is_new": bool(is_new_value),
        "reasoning": reasoning,
        "target_level": depth,
    }


def _task_dir_from_output_dir(output_dir: str) -> Path:
    resolved = Path(str(output_dir or "")).resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"output_dir not found: {resolved}")
    return resolved


def _storage_root_from_task_dir(task_dir: Path) -> Path:
    return task_dir.parent


def _summary_json_path(storage_root: Path) -> Path:
    return storage_root.parent / _SUMMARY_JSON_FILE


def _bootstrap_existing_categories(storage_root: Path) -> List[str]:
    categories: List[str] = []
    for child in sorted(storage_root.iterdir(), key=lambda item: item.name):
        if not child.is_dir() or child.name.startswith("."):
            continue
        classification_path = child / _TASK_CLASSIFICATION_FILE
        video_meta_path = child / "video_meta.json"
        candidate = ""
        try:
            if classification_path.exists():
                candidate = str(_read_json(classification_path).get("category_path") or "").strip()
            elif video_meta_path.exists():
                candidate = str(_read_json(video_meta_path).get("category_path") or "").strip()
        except Exception:
            candidate = ""
        if candidate:
            categories.append(candidate)
    return _normalize_lines(categories)


def _collect_existing_classifications(storage_root: Path) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    for child in sorted(storage_root.iterdir(), key=lambda item: item.name):
        if not child.is_dir() or child.name.startswith("."):
            continue
        classification_path = child / _TASK_CLASSIFICATION_FILE
        if not classification_path.exists():
            continue
        try:
            payload = _read_json(classification_path)
        except Exception:
            continue
        if isinstance(payload, dict):
            results.append(payload)
    return results


def _normalize_task_path(value: str) -> str:
    return "/".join(part for part in str(value or "").replace("\\", "/").split("/") if part)


def _task_path_for_task_dir(task_dir: Path) -> str:
    return f"storage/{task_dir.name}"


def _task_dir_from_task_path(storage_root: Path, task_path: str) -> Optional[Path]:
    normalized = _normalize_task_path(task_path)
    if not normalized:
        return None
    parts = normalized.split("/")
    if parts and parts[0] == "storage":
        parts = parts[1:]
    if not parts:
        return None
    task_dir = storage_root.joinpath(*parts)
    if not task_dir.exists() or not task_dir.is_dir():
        return None
    return task_dir


def _list_active_child_categories(
    classifications: Iterable[Dict[str, Any]],
    *,
    parent_category: str,
) -> List[str]:
    next_level = _category_depth(parent_category) + 1
    return _filter_categories_for_target_level(
        (str(item.get("category_path") or "") for item in classifications),
        target_level=next_level,
        required_prefix=parent_category,
    )


def _collect_task_dirs_for_category(
    storage_root: Path,
    classifications: Iterable[Dict[str, Any]],
    *,
    category_path: str,
    current_task_dir: Optional[Path] = None,
) -> List[Path]:
    normalized_category = _normalize_category_path(category_path)
    task_dirs_by_path: Dict[str, Path] = {}
    for item in classifications:
        item_category = _normalize_category_path(str(item.get("category_path") or ""))
        if item_category != normalized_category:
            continue
        task_path = _resolve_task_path_from_summary_item(item)
        task_dir = _task_dir_from_task_path(storage_root, task_path)
        if task_dir is None:
            continue
        task_dirs_by_path[_normalize_task_path(task_path)] = task_dir
    ordered_task_dirs = [task_dirs_by_path[key] for key in sorted(task_dirs_by_path)]
    if current_task_dir is None:
        return ordered_task_dirs
    current_task_path = _task_path_for_task_dir(current_task_dir)
    if current_task_path not in task_dirs_by_path:
        ordered_task_dirs.append(current_task_dir)
    return ordered_task_dirs


def _build_task_inputs(task_dirs: Iterable[Path]) -> List[VideoCategoryInput]:
    task_inputs: List[VideoCategoryInput] = []
    for task_dir in task_dirs:
        task_inputs.append(_build_video_input(task_dir=task_dir))
    return task_inputs


def _filter_categories_for_target_level(
    categories: Iterable[str],
    *,
    target_level: int,
    required_prefix: str = "",
) -> List[str]:
    normalized_prefix = _normalize_category_path(required_prefix)
    prefix_parts = _split_category_path(normalized_prefix)
    filtered: List[str] = []
    for category in categories:
        normalized = _normalize_category_path(category)
        parts = _split_category_path(normalized)
        if len(parts) != target_level:
            continue
        if prefix_parts and parts[: len(prefix_parts)] != prefix_parts:
            continue
        filtered.append(normalized)
    return _normalize_lines(filtered)


def _count_tasks_in_category_leaf(
    classifications: Iterable[Dict[str, Any]],
    *,
    category_path: str,
    current_task_path: str,
) -> int:
    normalized_category = _normalize_category_path(category_path)
    normalized_task_path = _normalize_category_path(current_task_path)
    count = 0
    for item in classifications:
        item_task_path = _resolve_task_path_from_summary_item(item)
        if not item_task_path or item_task_path == normalized_task_path:
            continue
        item_category = _normalize_category_path(str(item.get("category_path") or ""))
        if item_category == normalized_category:
            count += 1
    return count


def _write_summary_json(summary_file: Path, classifications: List[Dict[str, Any]]) -> None:
    existing_summary: Dict[str, Any] = {}
    if summary_file.exists():
        try:
            loaded = _read_json(summary_file)
            if isinstance(loaded, dict):
                existing_summary = loaded
        except Exception:
            existing_summary = {}

    category_counts: Dict[str, int] = {}
    for item in classifications:
        category_path = str(item.get("category_path") or "").strip()
        if not category_path:
            continue
        category_counts[category_path] = category_counts.get(category_path, 0) + 1

    existing_bindings = _normalize_collection_bindings(existing_summary.get("collectionBindings"))
    existing_archived_task_paths = _normalize_archived_task_paths(existing_summary.get("archivedTaskPaths"))

    payload = {
        "updated_at": _utc_now_iso(),
        "total_videos": len(classifications),
        "category_counts": dict(sorted(category_counts.items(), key=lambda pair: pair[0])),
        "results": classifications,
        "collectionBindings": dict(sorted(existing_bindings.items(), key=lambda pair: pair[0])),
        "archivedTaskPaths": dict(sorted(existing_archived_task_paths.items(), key=lambda pair: pair[0])),
    }
    _write_json(summary_file, payload)


def _update_video_meta(task_dir: Path, classification: Dict[str, Any]) -> None:
    meta_path = task_dir / "video_meta.json"
    payload: Dict[str, Any] = {}
    if meta_path.exists():
        try:
            loaded = _read_json(meta_path)
            if isinstance(loaded, dict):
                payload = dict(loaded)
        except Exception as exc:
            logger.warning("Failed to read existing video_meta.json from %s: %s", meta_path, exc)

    category_path = _normalize_category_path(classification["category_path"])
    category_levels = _split_category_path(category_path)
    payload["category_path"] = category_path
    payload["category_domain"] = category_levels[0] if category_levels else ""
    payload["category_subdomain"] = category_levels[1] if len(category_levels) > 1 else ""
    payload["category_leaf"] = category_levels[-1] if category_levels else ""
    payload["category_levels"] = category_levels
    payload["category_depth"] = len(category_levels)
    payload["category_target_level"] = int(classification.get("target_level") or len(category_levels))
    payload["category_is_new"] = bool(classification["is_new"])
    payload["category_reasoning"] = str(classification["reasoning"] or "")
    payload["category_classified_at"] = str(classification["generated_at"] or _utc_now_iso())
    _write_json(meta_path, payload)


def _write_task_classification(task_input: VideoCategoryInput, classification: Dict[str, Any]) -> None:
    task_path = f"storage/{task_input.task_dir.name}"
    artifact = {
        "video_id": task_input.task_dir.name,
        "task_path": task_path,
        "video_title": task_input.title,
        "category_path": classification["category_path"],
        "target_level": int(classification.get("target_level") or _category_depth(classification["category_path"])),
        "is_new": classification["is_new"],
        "reasoning": classification["reasoning"],
        "generated_at": classification["generated_at"],
        "usage": classification.get("usage", {}),
        "input_snapshot": {
            "first_unit_text": task_input.first_unit_text,
            "group_names": task_input.group_names,
            "content_evidence_text": task_input.content_evidence_text,
            "group_evidence": task_input.group_evidence,
        },
        "raw_response": classification.get("raw_response", ""),
        "verified_raw_response": classification.get("verified_raw_response", ""),
    }
    _write_json(task_input.task_dir / _TASK_CLASSIFICATION_FILE, artifact)


def _normalize_collection_bindings(payload: Any) -> Dict[str, str]:
    if not isinstance(payload, dict):
        return {}
    normalized: Dict[str, str] = {}
    for raw_task_path, raw_collection_path in payload.items():
        task_path = str(raw_task_path or "").strip().replace("\\", "/").lstrip("/")
        collection_path = str(raw_collection_path or "").strip().replace("\\", "/")
        if not task_path or not collection_path:
            continue
        task_path = "/".join(part for part in task_path.split("/") if part)
        collection_path = "/".join(part for part in collection_path.split("/") if part)
        if not task_path or not collection_path:
            continue
        normalized[task_path] = collection_path
    return normalized


def _normalize_archived_task_paths(payload: Any) -> Dict[str, str]:
    if not isinstance(payload, dict):
        return {}
    normalized: Dict[str, str] = {}
    for raw_task_path, raw_archived_at in payload.items():
        task_path = str(raw_task_path or "").strip().replace("\\", "/").lstrip("/")
        archived_at = str(raw_archived_at or "").strip()
        if not task_path:
            continue
        task_path = "/".join(part for part in task_path.split("/") if part)
        if not task_path:
            continue
        normalized[task_path] = archived_at
    return normalized


def _resolve_task_path_from_summary_item(item: Dict[str, Any]) -> str:
    task_path = str(item.get("task_path") or item.get("taskPath") or "").strip()
    if task_path:
        return _normalize_task_path(task_path)
    video_id = str(item.get("video_id") or "").strip()
    if not video_id:
        return ""
    return f"storage/{video_id}"


async def _verify_classification(
    *,
    task_input: VideoCategoryInput,
    categories: List[str],
    prompt_categories: List[str],
    system_prompt: str,
    candidate: Dict[str, Any],
    target_level: int,
    max_target_level: int,
    required_prefix: str,
    api_key: str,
    base_url: str,
    model: str,
) -> Dict[str, Any]:
    prefix_constraint_text = ""
    normalized_prefix = _normalize_category_path(required_prefix)
    if normalized_prefix:
        prefix_constraint_text = (
            f"\n6. `category_path` 没有保持在父目录 `{normalized_prefix}` 下进一步细分；"
        )
    group_evidence_text = _format_group_evidence_for_prompt(task_input.group_evidence)
    verify_prompt = f"""请审查下面这个视频分类结果是否严格基于输入事实。

如果候选结果存在任一问题，你必须直接纠正：
1. `category_path` 没有反映跨多个分组重复出现的核心知识主题；
2. `category_path` 主要依赖标题、开场首段、单个分组或偶发示例，而不是整片主体内容；
3. `reasoning` 引用了输入中不存在的证据词、示例内容或串台信息；
4. `is_new` 与当前分类路径库是否包含该 `category_path` 不一致；
5. `category_path` 不是唯一、互斥，且层级深度小于 `target_level={target_level}` 或大于 `max_target_level={max_target_level}`。{prefix_constraint_text}

审查时必须遵守：
- 如果标题、首段和多组正文证据冲突，以多组正文证据为准；
- `group_names` 只能作为结构线索，不能代替正文证据；
- 优先复用现有分类路径，不要为同义主题新造近义路径。

你必须自己重新核对，不要盲从候选结果。
请只输出修正后的合法 JSON。

## 视频标题
{task_input.title}

## 首段正文（仅作辅助，不能单独决定分类）
{task_input.first_unit_text}

## 大纲组名
{json.dumps(task_input.group_names, ensure_ascii=False)}

## 多组正文证据
{group_evidence_text}

## 整片内容证据摘要
{task_input.content_evidence_text}

## 当前分类路径库（仅展示符合 target_level 和父目录约束的候选路径）
{json.dumps(prompt_categories, ensure_ascii=False)}

## 候选结果
{json.dumps(candidate, ensure_ascii=False)}
"""
    content, _metadata, _ = await llm_gateway.deepseek_complete_text(
        prompt=verify_prompt,
        system_message=system_prompt,
        api_key=api_key,
        base_url=base_url,
        model=model,
        temperature=0.0,
        cache_enabled=False,
        inflight_dedup_enabled=False,
        hedge_context={"batch_text_chars": len(verify_prompt)},
    )
    normalized = _normalize_category_result(
        _extract_json_object(content),
        categories,
        target_level=target_level,
        max_target_level=max_target_level,
        required_prefix=required_prefix,
    )
    normalized["verified_raw_response"] = content
    return normalized


async def _classify_for_target_level(
    *,
    task_input: VideoCategoryInput,
    categories: List[str],
    system_prompt: str,
    api_key: str,
    base_url: str,
    model: str,
    target_level: int,
    max_target_level: int,
    required_prefix: str,
) -> Dict[str, Any]:
    prompt_categories = _filter_categories_for_target_level(
        categories,
        target_level=target_level,
        required_prefix=required_prefix,
    )
    normalized_prefix = _normalize_category_path(required_prefix)
    user_prompt = render_prompt(
        PromptKeys.DEEPSEEK_CATEGORY_CLASSIFIER_USER,
        context={
            "video_title": task_input.title,
            "first_unit_text": task_input.first_unit_text,
            "group_names": "\n".join(f"- {name}" for name in task_input.group_names),
            "group_evidence": _format_group_evidence_for_prompt(task_input.group_evidence),
            "content_evidence_text": task_input.content_evidence_text,
            "categories": "\n".join(prompt_categories),
            "target_level": target_level,
            "max_target_level": max_target_level,
            "required_prefix": normalized_prefix,
        },
    )
    content, metadata, _ = await llm_gateway.deepseek_complete_text(
        prompt=user_prompt,
        system_message=system_prompt,
        api_key=api_key,
        base_url=base_url,
        model=model,
        temperature=0.1,
        cache_enabled=False,
        inflight_dedup_enabled=False,
        hedge_context={"batch_text_chars": len(user_prompt)},
    )
    candidate = _normalize_category_result(
        _extract_json_object(content),
        categories,
        target_level=target_level,
        max_target_level=max_target_level,
        required_prefix=required_prefix,
    )
    verified = await _verify_classification(
        task_input=task_input,
        categories=categories,
        prompt_categories=prompt_categories,
        system_prompt=system_prompt,
        candidate=candidate,
        target_level=target_level,
        max_target_level=max_target_level,
        required_prefix=required_prefix,
        api_key=api_key,
        base_url=base_url,
        model=model,
    )
    if isinstance(metadata, dict):
        verified["usage"] = {
            key: metadata.get(key)
            for key in (
                "model",
                "prompt_tokens",
                "completion_tokens",
                "total_tokens",
                "latency_ms",
                "cache_hit",
            )
            if key in metadata
        }
    else:
        verified["usage"] = {}
    verified["raw_response"] = content
    verified["generated_at"] = _utc_now_iso()
    return verified


async def _route_task_to_active_leaf(
    *,
    task_input: VideoCategoryInput,
    categories: List[str],
    existing_classifications: List[Dict[str, Any]],
    system_prompt: str,
    api_key: str,
    base_url: str,
    model: str,
    base_target_level: int,
    max_target_level: int,
) -> Dict[str, Any]:
    classification = await _classify_for_target_level(
        task_input=task_input,
        categories=categories,
        system_prompt=system_prompt,
        api_key=api_key,
        base_url=base_url,
        model=model,
        target_level=base_target_level,
        max_target_level=max_target_level,
        required_prefix="",
    )
    while _category_depth(classification["category_path"]) < max_target_level:
        active_children = _list_active_child_categories(
            existing_classifications,
            parent_category=classification["category_path"],
        )
        if not active_children:
            break
        classification = await _classify_for_target_level(
            task_input=task_input,
            categories=categories,
            system_prompt=system_prompt,
            api_key=api_key,
            base_url=base_url,
            model=model,
            target_level=_category_depth(classification["category_path"]) + 1,
            max_target_level=max_target_level,
            required_prefix=classification["category_path"],
        )
    return classification


async def _reclassify_overloaded_category(
    *,
    task_inputs: List[VideoCategoryInput],
    categories: List[str],
    system_prompt: str,
    api_key: str,
    base_url: str,
    model: str,
    parent_category: str,
    target_level: int,
    max_target_level: int,
) -> Dict[str, Dict[str, Any]]:
    mutable_categories = list(categories)
    results: Dict[str, Dict[str, Any]] = {}
    for task_input in task_inputs:
        try:
            classification = await _classify_for_target_level(
                task_input=task_input,
                categories=mutable_categories,
                system_prompt=system_prompt,
                api_key=api_key,
                base_url=base_url,
                model=model,
                target_level=target_level,
                max_target_level=max_target_level,
                required_prefix=parent_category,
            )
        except Exception as exc:
            logger.warning(
                "[Phase2B-Category] keep overloaded leaf for task=%s parent=%s err=%s",
                task_input.task_dir.name,
                parent_category,
                exc,
            )
            classification = {
                "category_path": _normalize_category_path(parent_category),
                "is_new": False,
                "reasoning": f"rebalance fallback: {exc}",
                "target_level": _category_depth(parent_category),
                "generated_at": _utc_now_iso(),
                "usage": {},
                "raw_response": "",
                "verified_raw_response": "",
            }
        results[_task_path_for_task_dir(task_input.task_dir)] = classification
        mutable_categories = _normalize_lines([*mutable_categories, classification["category_path"]])
    return results


async def classify_phase2b_output(
    *,
    output_dir: str,
    title: str,
    result_json_path: str,
) -> Optional[Dict[str, Any]]:
    """
    在 Phase2B 最终产物落盘后执行视频级分类。
    失败策略：记录告警并返回 None，不阻塞 Phase2B 主链路。
    """
    api_key = str(os.getenv("DEEPSEEK_API_KEY", "") or "").strip()
    if not api_key:
        logger.info("[Phase2B-Category] skip: DEEPSEEK_API_KEY not set")
        return None

    try:
        task_dir = _task_dir_from_output_dir(output_dir)
        storage_root = _storage_root_from_task_dir(task_dir)
        library_path = storage_root / _CATEGORY_LIBRARY_FILE
        summary_json_path = _summary_json_path(storage_root)

        task_input = _build_video_input(task_dir=task_dir, title=title)
        categories = _load_category_library(library_path)
        categories.extend(_bootstrap_existing_categories(storage_root))
        categories = _normalize_lines(categories)
        existing_classifications = _collect_existing_classifications(storage_root)
        task_path = f"storage/{task_dir.name}"

        system_prompt = get_prompt(PromptKeys.DEEPSEEK_CATEGORY_CLASSIFIER_SYSTEM)
        base_url = str(os.getenv("MODULE2_CATEGORY_CLASSIFIER_BASE_URL", _DEFAULT_DEEPSEEK_BASE_URL) or "").strip()
        model = str(os.getenv("MODULE2_CATEGORY_CLASSIFIER_MODEL", _DEFAULT_DEEPSEEK_MODEL) or "").strip()
        leaf_task_limit = max(
            1,
            int(
                os.getenv(
                    "MODULE2_CATEGORY_CLASSIFIER_LEAF_TASK_LIMIT",
                    str(_DEFAULT_CATEGORY_LEAF_TASK_LIMIT),
                )
                or _DEFAULT_CATEGORY_LEAF_TASK_LIMIT
            ),
        )
        target_level = max(
            2,
            int(
                os.getenv(
                    "MODULE2_CATEGORY_CLASSIFIER_BASE_TARGET_LEVEL",
                    str(_DEFAULT_CATEGORY_TARGET_LEVEL),
                )
                or _DEFAULT_CATEGORY_TARGET_LEVEL
            ),
        )
        max_target_level = max(
            target_level,
            int(
                os.getenv(
                    "MODULE2_CATEGORY_CLASSIFIER_MAX_TARGET_LEVEL",
                    str(_DEFAULT_CATEGORY_MAX_TARGET_LEVEL),
                )
                or _DEFAULT_CATEGORY_MAX_TARGET_LEVEL
            ),
        )

        verified = await _route_task_to_active_leaf(
            task_input=task_input,
            categories=categories,
            existing_classifications=existing_classifications,
            system_prompt=system_prompt,
            api_key=api_key,
            base_url=base_url,
            model=model,
            base_target_level=target_level,
            max_target_level=max_target_level,
        )
        verified["leaf_task_count"] = _count_tasks_in_category_leaf(
            existing_classifications,
            category_path=verified["category_path"],
            current_task_path=task_path,
        ) + 1
        final_results: Dict[str, Dict[str, Any]] = {task_path: verified}
        task_inputs_to_write: List[VideoCategoryInput] = [task_input]

        if (
            verified["leaf_task_count"] > leaf_task_limit
            and _category_depth(verified["category_path"]) < max_target_level
        ):
            overloaded_category = verified["category_path"]
            next_target_level = _category_depth(overloaded_category) + 1
            logger.info(
                "[Phase2B-Category] rebalance overloaded leaf task=%s leaf=%s leaf_count=%s limit=%s target_level=%s",
                task_dir.name,
                overloaded_category,
                verified["leaf_task_count"],
                leaf_task_limit,
                next_target_level,
            )
            cohort_task_dirs = _collect_task_dirs_for_category(
                storage_root,
                existing_classifications,
                category_path=overloaded_category,
                current_task_dir=task_dir,
            )
            cohort_task_inputs = _build_task_inputs(cohort_task_dirs)
            try:
                refined_results = await _reclassify_overloaded_category(
                    task_inputs=cohort_task_inputs,
                    categories=categories,
                    system_prompt=system_prompt,
                    api_key=api_key,
                    base_url=base_url,
                    model=model,
                    parent_category=overloaded_category,
                    target_level=next_target_level,
                    max_target_level=max_target_level,
                )
            except Exception as exc:
                logger.warning(
                    "[Phase2B-Category] rebalance failed, keep leaf task=%s leaf=%s err=%s",
                    task_dir.name,
                    overloaded_category,
                    exc,
                )
            else:
                refined_counts: Dict[str, int] = {}
                for classification in refined_results.values():
                    refined_path = _normalize_category_path(classification["category_path"])
                    refined_counts[refined_path] = refined_counts.get(refined_path, 0) + 1
                for classification in refined_results.values():
                    refined_path = _normalize_category_path(classification["category_path"])
                    classification["leaf_task_count"] = refined_counts.get(refined_path, 0)
                final_results = refined_results
                task_inputs_to_write = cohort_task_inputs

        updated_categories = _normalize_lines(
            [*categories, *(classification["category_path"] for classification in final_results.values())]
        )
        _write_category_library(library_path, updated_categories)
        for item in task_inputs_to_write:
            classification = final_results[_task_path_for_task_dir(item.task_dir)]
            _update_video_meta(item.task_dir, classification)
            _write_task_classification(item, classification)
        _write_summary_json(summary_json_path, _collect_existing_classifications(storage_root))
        result = final_results[task_path]
        logger.info(
            "[Phase2B-Category] classified task=%s category=%s is_new=%s",
            task_dir.name,
            result["category_path"],
            result["is_new"],
        )
        return result
    except Exception as exc:
        logger.warning("[Phase2B-Category] failed for output_dir=%s: %s", output_dir, exc)
        return None
