"""
模块说明：Phase2B DeepSeek 调用审计工具。

职责边界：
1) 维护“当前任务”的审计上下文（使用 contextvars，支持并发隔离）。
2) 将单次 DeepSeek 调用的 input/output 成对记录为 JSON。

设计目标：
- 一次调用对应一条记录，input/output 相邻存放，便于审计与复盘。
- 默认落盘到 `output_dir/intermediates/phase2b_deepseek_call_audit.json`。
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from contextvars import ContextVar, Token
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple

from services.python_grpc.src.content_pipeline.infra.llm.token_costing import (
    build_token_cost_estimate,
    get_token_pricing_snapshot,
    normalize_usage_payload,
    summarize_token_cost_records,
)

logger = logging.getLogger(__name__)
_SLOW_CALL_THRESHOLD_MS = 10_000.0
_TEXT_PREVIEW_CHARS = 160


@dataclass(frozen=True)
class DeepSeekAuditContext:
    """描述当前协程上下文中的 DeepSeek 审计配置。"""

    enabled: bool
    output_path: str
    task_id: str = ""
    video_path: str = ""
    scene: str = "phase2b_img_desc_augment"
    only_img_desc_augment: bool = True
    max_text_chars: int = 0


_AUDIT_CONTEXT: ContextVar[Optional[DeepSeekAuditContext]] = ContextVar(
    "module2_phase2b_deepseek_audit_context",
    default=None,
)
_AUDIT_FILE_LOCK = Lock()


def _parse_bool(value: Any, default: bool) -> bool:
    """方法说明：_parse_bool 工具方法。
    执行步骤：
    1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
    2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
    3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
    if value is None:
        return bool(default)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def _now_iso() -> str:
    """方法说明：_now_iso 工具方法。
    执行步骤：
    1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
    2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
    3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
    return datetime.now().isoformat(timespec="seconds")


def _is_img_desc_augment_call(prompt: str, system_message: str) -> bool:
    """方法说明：_is_img_desc_augment_call 工具方法。
    执行步骤：
    1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
    2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
    3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
    sys_text = str(system_message or "")
    prompt_text = str(prompt or "")
    merged_lower = f"{sys_text}\n{prompt_text}".lower()

    if "教学文本补全助手" in sys_text:
        return True

    keywords = [
        "img_description",
        "图片描述",
        "增量补全",
        "图像描述",
    ]
    return any(key in merged_lower for key in keywords)


def _apply_text_limit(text: str, max_text_chars: int) -> str:
    """方法说明：_apply_text_limit 工具方法。
    执行步骤：
    1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
    2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
    3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
    raw = str(text or "")
    if max_text_chars <= 0 or len(raw) <= max_text_chars:
        return raw
    return raw[:max_text_chars] + "\n...[TRUNCATED]"


def _metadata_to_dict(metadata: Any) -> Dict[str, Any]:
    """方法说明：_metadata_to_dict 工具方法。
    执行步骤：
    1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
    2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
    3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
    if metadata is None:
        return {}

    if isinstance(metadata, dict):
        return dict(metadata)

    return {
        "model": str(getattr(metadata, "model", "") or ""),
        "prompt_tokens": int(getattr(metadata, "prompt_tokens", 0) or 0),
        "completion_tokens": int(getattr(metadata, "completion_tokens", 0) or 0),
        "total_tokens": int(getattr(metadata, "total_tokens", 0) or 0),
        "latency_ms": float(getattr(metadata, "latency_ms", 0.0) or 0.0),
        "cache_hit": bool(getattr(metadata, "cache_hit", False)),
        "usage_details": normalize_usage_payload(getattr(metadata, "usage_details", None)),
    }


def _build_payload_summary(records: Any) -> Dict[str, Any]:
    canonical_records = []
    for record in records or []:
        if not isinstance(record, dict):
            continue
        canonical_records.append(
            {
                "model": str(record.get("input", {}).get("model", "") or ""),
                "provider": str(record.get("cost_estimate", {}).get("provider", "") or ""),
                "token_usage": dict(record.get("token_usage", {}) or {}),
                "cost_estimate": dict(record.get("cost_estimate", {}) or {}),
            }
        )
    return summarize_token_cost_records(canonical_records)


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


def _preview_text(text: Any, max_chars: int = _TEXT_PREVIEW_CHARS) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    if max_chars <= 0 or len(raw) <= max_chars:
        return raw
    return raw[:max_chars] + "...[TRUNCATED]"


def _compact_text_preview(text: Any, max_chars: int = _TEXT_PREVIEW_CHARS) -> str:
    normalized = " ".join(str(text or "").split())
    return _preview_text(normalized, max_chars=max_chars)


def _fingerprint_text(text: Any) -> str:
    return hashlib.sha1(str(text or "").encode("utf-8")).hexdigest()[:12]


def _extract_prompt_outline(prompt: Any) -> Dict[str, Any]:
    text = str(prompt or "")
    if not text.strip():
        return {}

    title = ""
    knowledge_type = ""
    previous_section = ""
    next_section = ""
    raw_text_lines: List[str] = []
    current_section = ""

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if line.startswith("## "):
            current_section = line
            continue
        if current_section == "## 语义单元":
            if line.startswith("- 标题:"):
                title = line.split(":", 1)[1].strip()
            elif line.startswith("- 知识类型:"):
                knowledge_type = line.split(":", 1)[1].strip()
        elif current_section == "## 话题上下文":
            if line.startswith("- Previous section:"):
                previous_section = line.split(":", 1)[1].strip()
            elif line.startswith("- Next section:"):
                next_section = line.split(":", 1)[1].strip()
        elif current_section == "## 原始文本" and line:
            raw_text_lines.append(line)

    raw_text = "\n".join(raw_text_lines).strip()
    outline: Dict[str, Any] = {
        "title": title,
        "knowledge_type": knowledge_type,
        "previous_section": previous_section,
        "next_section": next_section,
        "raw_text_preview": _compact_text_preview(raw_text),
        "raw_text_line_count": len(raw_text_lines),
    }
    return {key: value for key, value in outline.items() if value not in ("", 0, None)}


def _build_shared_texts(records: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[Tuple[str, str], str]]:
    entries_by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}
    refs_by_key: Dict[Tuple[str, str], str] = {}
    counters = {"system_message": 0, "prompt": 0}
    prefix_map = {"system_message": "SYS", "prompt": "PRM"}

    for record in records:
        if not isinstance(record, dict):
            continue
        call_index = _safe_int(record.get("call_index"), 0)
        input_obj = record.get("input", {})
        if not isinstance(input_obj, dict):
            continue
        for kind in ("system_message", "prompt"):
            text = str(input_obj.get(kind) or "")
            if not text.strip():
                continue
            key = (kind, text)
            if key not in entries_by_key:
                counters[kind] += 1
                text_id = f"{prefix_map[kind]}_{counters[kind]:03d}"
                refs_by_key[key] = text_id
                entries_by_key[key] = {
                    "text_id": text_id,
                    "kind": kind,
                    "fingerprint": _fingerprint_text(text),
                    "ref_count": 0,
                    "used_by_call_indexes": [],
                    "char_count": len(text),
                    "line_count": max(1, len(text.splitlines())),
                    "preview": _compact_text_preview(text),
                }
                if kind == "prompt":
                    outline = _extract_prompt_outline(text)
                    if outline:
                        entries_by_key[key]["outline"] = outline
            entry = entries_by_key[key]
            entry["ref_count"] = _safe_int(entry.get("ref_count"), 0) + 1
            used_by = entry.get("used_by_call_indexes")
            if isinstance(used_by, list) and call_index > 0:
                used_by.append(call_index)

    shared_texts = list(entries_by_key.values())
    shared_texts.sort(key=lambda item: (str(item.get("kind") or ""), str(item.get("text_id") or "")))
    return shared_texts, refs_by_key


def _build_compact_record(
    record: Dict[str, Any],
    refs_by_key: Dict[Tuple[str, str], str],
) -> Dict[str, Any]:
    input_obj = record.get("input", {})
    output_obj = record.get("output", {})
    token_usage = record.get("token_usage", {})
    cost_estimate = record.get("cost_estimate", {})

    if not isinstance(input_obj, dict):
        input_obj = {}
    if not isinstance(output_obj, dict):
        output_obj = {}
    if not isinstance(token_usage, dict):
        token_usage = {}
    if not isinstance(cost_estimate, dict):
        cost_estimate = {}

    prompt_text = str(input_obj.get("prompt") or "")
    system_text = str(input_obj.get("system_message") or "")
    metadata = output_obj.get("metadata", {})
    if not isinstance(metadata, dict):
        metadata = {}

    prompt_outline = _extract_prompt_outline(prompt_text)
    latency_ms = _safe_float(metadata.get("latency_ms"), 0.0)
    success = bool(output_obj.get("success", False)) and not bool(str(output_obj.get("error") or "").strip())

    compact: Dict[str, Any] = {
        "call_index": _safe_int(record.get("call_index"), 0),
        "timestamp": str(record.get("timestamp") or ""),
        "status": "SUCCESS" if success else "FAILED",
        "step_name": str(record.get("step_name") or ""),
        "model": str(input_obj.get("model") or metadata.get("model") or ""),
        "latency_ms": round(latency_ms, 2),
        "prompt_tokens": _safe_int(token_usage.get("prompt_tokens"), 0),
        "completion_tokens": _safe_int(token_usage.get("completion_tokens"), 0),
        "total_tokens": _safe_int(token_usage.get("total_tokens"), 0),
        "cost_status": str(cost_estimate.get("status") or ""),
        "cost_total": round(_safe_float(cost_estimate.get("total_cost"), 0.0), 8),
        "currency": str(cost_estimate.get("currency") or ""),
        "output_preview": _compact_text_preview(output_obj.get("content")),
        "error_preview": _compact_text_preview(output_obj.get("error")),
    }

    if prompt_outline:
        compact["prompt_outline"] = prompt_outline
    if system_text.strip():
        compact["system_message_ref"] = refs_by_key.get(("system_message", system_text), "")
    if prompt_text.strip():
        compact["prompt_ref"] = refs_by_key.get(("prompt", prompt_text), "")

    return {key: value for key, value in compact.items() if value not in ("", None, [], {})}


def _build_problem_summary(
    compact_records: List[Dict[str, Any]],
    shared_texts: List[Dict[str, Any]],
) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    total_calls = len(compact_records)
    failed_records = [record for record in compact_records if str(record.get("status") or "") == "FAILED"]
    slow_records = [
        record
        for record in compact_records
        if _safe_float(record.get("latency_ms"), 0.0) >= _SLOW_CALL_THRESHOLD_MS
    ]
    duplicate_system_templates = [
        item
        for item in shared_texts
        if str(item.get("kind") or "") == "system_message" and _safe_int(item.get("ref_count"), 0) > 1
    ]
    duplicate_prompt_templates = [
        item
        for item in shared_texts
        if str(item.get("kind") or "") == "prompt" and _safe_int(item.get("ref_count"), 0) > 1
    ]

    problem_summary: List[Dict[str, Any]] = []
    if total_calls == 0:
        problem_summary.append(
            {
                "severity": "warning",
                "type": "empty_audit",
                "message": "当前没有任何 DeepSeek 审计记录。",
            }
        )
    if failed_records:
        indexes = [str(record.get("call_index") or "") for record in failed_records]
        problem_summary.append(
            {
                "severity": "error",
                "type": "failed_calls",
                "message": f"存在 {len(failed_records)} 次失败调用，序号: {', '.join(indexes)}。",
            }
        )
    if slow_records:
        indexes = [str(record.get("call_index") or "") for record in slow_records]
        problem_summary.append(
            {
                "severity": "warning",
                "type": "slow_calls",
                "message": f"存在 {len(slow_records)} 次慢调用（>= {_safe_int(_SLOW_CALL_THRESHOLD_MS)} ms），序号: {', '.join(indexes)}。",
            }
        )
    if duplicate_system_templates:
        max_ref = max(_safe_int(item.get("ref_count"), 0) for item in duplicate_system_templates)
        problem_summary.append(
            {
                "severity": "info",
                "type": "duplicate_system_message",
                "message": f"system_message 模板重复出现 {len(duplicate_system_templates)} 组，最高复用 {max_ref} 次。",
            }
        )
    if duplicate_prompt_templates:
        max_ref = max(_safe_int(item.get("ref_count"), 0) for item in duplicate_prompt_templates)
        problem_summary.append(
            {
                "severity": "info",
                "type": "duplicate_prompt",
                "message": f"prompt 模板重复出现 {len(duplicate_prompt_templates)} 组，最高复用 {max_ref} 次。",
            }
        )
    if not problem_summary:
        problem_summary.append(
            {
                "severity": "ok",
                "type": "healthy",
                "message": "没有明显失败、慢调用或重复模板问题。",
            }
        )

    overview = {
        "total_calls": total_calls,
        "success_calls": total_calls - len(failed_records),
        "failed_calls": len(failed_records),
        "slow_calls_over_10s": len(slow_records),
        "duplicate_system_message_templates": len(duplicate_system_templates),
        "duplicate_prompt_templates": len(duplicate_prompt_templates),
    }
    return overview, problem_summary, failed_records, slow_records


def _build_readable_audit_sections(records: List[Dict[str, Any]], summary: Dict[str, Any]) -> Dict[str, Any]:
    shared_texts, refs_by_key = _build_shared_texts(records)
    compact_records = [_build_compact_record(record, refs_by_key) for record in records if isinstance(record, dict)]
    overview, problem_summary, failed_records, slow_records = _build_problem_summary(
        compact_records=compact_records,
        shared_texts=shared_texts,
    )
    overview["estimated_cost_by_currency"] = dict(summary.get("estimated_cost_by_currency", {}) or {})
    overview["total_tokens"] = _safe_int(summary.get("total_tokens"), 0)
    overview["total_prompt_tokens"] = _safe_int(summary.get("total_prompt_tokens"), 0)
    overview["total_completion_tokens"] = _safe_int(summary.get("total_completion_tokens"), 0)

    return {
        "overview": overview,
        "problem_summary": problem_summary,
        "failure_records": failed_records,
        "slow_records": slow_records,
        "compact_records": compact_records,
        "shared_texts": shared_texts,
    }


def _build_deepseek_audit_markdown(payload: Dict[str, Any]) -> str:
    overview = payload.get("overview", {})
    problem_summary = payload.get("problem_summary", [])
    compact_records = payload.get("compact_records", [])
    shared_texts = payload.get("shared_texts", [])

    lines: List[str] = []
    lines.append("# Phase2B DeepSeek Audit")
    lines.append("")
    lines.append("## 一眼结论")
    lines.append(f"- total_calls: {overview.get('total_calls', 0)}")
    lines.append(f"- success_calls: {overview.get('success_calls', 0)}")
    lines.append(f"- failed_calls: {overview.get('failed_calls', 0)}")
    lines.append(f"- slow_calls_over_10s: {overview.get('slow_calls_over_10s', 0)}")
    lines.append(f"- duplicate_system_message_templates: {overview.get('duplicate_system_message_templates', 0)}")
    lines.append(f"- duplicate_prompt_templates: {overview.get('duplicate_prompt_templates', 0)}")
    lines.append(f"- estimated_cost_by_currency: {overview.get('estimated_cost_by_currency', {})}")
    lines.append("")

    lines.append("## 问题摘要")
    if isinstance(problem_summary, list) and problem_summary:
        for item in problem_summary:
            if not isinstance(item, dict):
                continue
            severity = str(item.get("severity") or "").upper()
            message = str(item.get("message") or "")
            lines.append(f"- [{severity}] {message}")
    else:
        lines.append("- [INFO] 没有问题摘要。")
    lines.append("")

    lines.append("## 精简记录")
    if isinstance(compact_records, list) and compact_records:
        for item in compact_records:
            if not isinstance(item, dict):
                continue
            outline = item.get("prompt_outline", {})
            title = str(outline.get("title") or "")
            line = (
                f"- #{item.get('call_index', 0)} "
                f"[{item.get('status', '')}] "
                f"step={item.get('step_name', '')} "
                f"model={item.get('model', '')} "
                f"latency_ms={item.get('latency_ms', 0)} "
                f"tokens={item.get('total_tokens', 0)}"
            )
            if title:
                line += f" title={title}"
            lines.append(line)
            error_preview = str(item.get("error_preview") or "")
            output_preview = str(item.get("output_preview") or "")
            if error_preview:
                lines.append(f"  error: {error_preview}")
            elif output_preview:
                lines.append(f"  output: {output_preview}")
    else:
        lines.append("- 无记录。")
    lines.append("")

    lines.append("## 共享文本")
    if isinstance(shared_texts, list) and shared_texts:
        for item in shared_texts:
            if not isinstance(item, dict):
                continue
            lines.append(
                "- "
                f"{item.get('text_id', '')} "
                f"kind={item.get('kind', '')} "
                f"ref_count={item.get('ref_count', 0)} "
                f"line_count={item.get('line_count', 0)} "
                f"preview={item.get('preview', '')}"
            )
    else:
        lines.append("- 无共享文本。")
    lines.append("")
    return "\n".join(lines)


def _write_deepseek_audit_markdown(output_path: Path, payload: Dict[str, Any]) -> None:
    markdown_path = output_path.with_suffix(".md")
    with open(markdown_path, "w", encoding="utf-8") as file_obj:
        file_obj.write(_build_deepseek_audit_markdown(payload))


def _build_audit_payload(
    *,
    context: DeepSeekAuditContext,
    records: List[Dict[str, Any]],
    created_at: str,
) -> Dict[str, Any]:
    summary = _build_payload_summary(records)
    readable = _build_readable_audit_sections(records=records, summary=summary)
    return {
        "version": "1.1",
        "scene": context.scene,
        "task_id": context.task_id,
        "video_path": context.video_path,
        "created_at": created_at,
        "updated_at": _now_iso(),
        "overview": readable.get("overview", {}),
        "problem_summary": readable.get("problem_summary", []),
        "failure_records": readable.get("failure_records", []),
        "slow_records": readable.get("slow_records", []),
        "compact_records": readable.get("compact_records", []),
        "shared_texts": readable.get("shared_texts", []),
        "total_calls": len(records),
        "pricing_snapshot": get_token_pricing_snapshot(),
        "summary": summary,
        "records": records,
    }


def _write_audit_payload(output_path: Path, payload: Dict[str, Any]) -> None:
    with open(output_path, "w", encoding="utf-8") as file_obj:
        json.dump(payload, file_obj, ensure_ascii=False, indent=2)
    _write_deepseek_audit_markdown(output_path, payload)


def build_phase2b_audit_context(
    *,
    output_dir: str,
    task_id: str = "",
    video_path: str = "",
    file_name: str = "phase2b_deepseek_call_audit.json",
    enabled: Optional[bool] = None,
    only_img_desc_augment: bool = True,
) -> DeepSeekAuditContext:
    """
    基于视频输出目录构造 Phase2B 审计上下文。

    优先级：
    1) 显式参数 enabled
    2) 环境变量 MODULE2_PHASE2B_DEEPSEEK_AUDIT_ENABLED
    3) 默认开启（True）
    """

    if enabled is None:
        enabled = _parse_bool(os.getenv("MODULE2_PHASE2B_DEEPSEEK_AUDIT_ENABLED"), True)

    max_text_chars = 0
    try:
        max_text_chars = int(str(os.getenv("MODULE2_PHASE2B_DEEPSEEK_AUDIT_MAX_TEXT_CHARS", "0") or "0"))
    except Exception:
        max_text_chars = 0

    output_path = Path(output_dir) / "intermediates" / file_name
    return DeepSeekAuditContext(
        enabled=bool(enabled),
        output_path=str(output_path),
        task_id=str(task_id or ""),
        video_path=str(video_path or ""),
        scene="phase2b_img_desc_augment",
        only_img_desc_augment=bool(only_img_desc_augment),
        max_text_chars=max(0, max_text_chars),
    )


def push_deepseek_audit_context(context: DeepSeekAuditContext) -> Token:
    """设置当前协程审计上下文，并初始化审计文件。"""

    token = _AUDIT_CONTEXT.set(context)
    _initialize_audit_file(context)
    return token


def pop_deepseek_audit_context(token: Token) -> None:
    """恢复上一层审计上下文。"""

    _AUDIT_CONTEXT.reset(token)


def get_deepseek_audit_context() -> Optional[DeepSeekAuditContext]:
    """方法说明：get_deepseek_audit_context 核心方法。
    执行步骤：
    1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
    2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
    3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
    return _AUDIT_CONTEXT.get()


def _initialize_audit_file(context: DeepSeekAuditContext) -> None:
    """方法说明：_initialize_audit_file 工具方法。
    执行步骤：
    1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
    2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
    3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
    if not context.enabled:
        return

    output_path = Path(context.output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with _AUDIT_FILE_LOCK:
        if output_path.exists():
            return
        created_at = _now_iso()
        payload = _build_audit_payload(
            context=context,
            records=[],
            created_at=created_at,
        )
        _write_audit_payload(output_path, payload)


def append_deepseek_call_record(
    *,
    prompt: str,
    system_message: str,
    model: str,
    temperature: float,
    need_logprobs: bool,
    output_text: str,
    metadata: Any,
    error: str = "",
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    """
    记录一次 DeepSeek 调用审计。

    记录格式保证每条记录内 input/output 相邻，便于逐调用核对。
    """

    context = get_deepseek_audit_context()
    if context is None or not context.enabled:
        return

    if context.only_img_desc_augment and not _is_img_desc_augment_call(prompt, system_message):
        return

    output_path = Path(context.output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    system_text = _apply_text_limit(system_message or "", context.max_text_chars)
    prompt_text = _apply_text_limit(prompt or "", context.max_text_chars)
    output_text_safe = _apply_text_limit(output_text or "", context.max_text_chars)
    record_timestamp = _now_iso()
    metadata_payload = _metadata_to_dict(metadata)
    usage_source = metadata_payload.get("usage_details") or metadata_payload
    token_usage = normalize_usage_payload(usage_source)
    requested_model = str(model or "")
    actual_model = str(metadata_payload.get("model") or requested_model)
    cost_estimate = build_token_cost_estimate(
        usage=token_usage,
        model=actual_model,
        timestamp_utc=record_timestamp,
        local_cache_hit=bool(metadata_payload.get("cache_hit", False)),
    )

    record: Dict[str, Any] = {
        "timestamp": record_timestamp,
        "scene": context.scene,
        "step_name": "img_desc_augment" if _is_img_desc_augment_call(prompt, system_message) else "deepseek_complete_text",
        "input": {
            "model": actual_model,
            "temperature": float(temperature or 0.0),
            "need_logprobs": bool(need_logprobs),
            "system_message": system_text,
            "prompt": prompt_text,
        },
        "output": {
            "success": not bool(error),
            "content": output_text_safe,
            "metadata": metadata_payload,
            "error": str(error or ""),
        },
        "token_usage": token_usage,
        "cost_estimate": cost_estimate,
    }
    if requested_model and requested_model != actual_model:
        record["input"]["requested_model"] = requested_model
    if extra:
        record["extra"] = dict(extra)

    with _AUDIT_FILE_LOCK:
        payload: Dict[str, Any]
        if output_path.exists():
            try:
                with open(output_path, "r", encoding="utf-8") as file_obj:
                    loaded = json.load(file_obj)
                payload = loaded if isinstance(loaded, dict) else {}
            except Exception:
                payload = {}
        else:
            payload = {}

        created_at = str(payload.get("created_at") or _now_iso())
        loaded_records = payload.get("records")
        records = list(loaded_records) if isinstance(loaded_records, list) else []
        record["call_index"] = len(records) + 1
        records.append(record)
        payload = _build_audit_payload(
            context=context,
            records=records,
            created_at=created_at,
        )
        _write_audit_payload(output_path, payload)

    logger.debug("DeepSeek audit appended: %s", output_path)
