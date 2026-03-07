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

import json
import logging
import os
from contextvars import ContextVar, Token
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Optional

from services.python_grpc.src.content_pipeline.infra.llm.token_costing import (
    build_token_cost_estimate,
    get_token_pricing_snapshot,
    normalize_usage_payload,
    summarize_token_cost_records,
)

logger = logging.getLogger(__name__)


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

        payload = {
            "version": "1.0",
            "scene": context.scene,
            "task_id": context.task_id,
            "video_path": context.video_path,
            "created_at": _now_iso(),
            "updated_at": _now_iso(),
            "total_calls": 0,
            "pricing_snapshot": get_token_pricing_snapshot(),
            "summary": _build_payload_summary([]),
            "records": [],
        }
        with open(output_path, "w", encoding="utf-8") as file_obj:
            json.dump(payload, file_obj, ensure_ascii=False, indent=2)


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
    metadata_payload = _metadata_to_dict(metadata)
    usage_source = metadata_payload.get("usage_details") or metadata_payload
    token_usage = normalize_usage_payload(usage_source)
    cost_estimate = build_token_cost_estimate(
        usage=token_usage,
        model=model or metadata_payload.get("model", ""),
        timestamp_utc=_now_iso(),
        local_cache_hit=bool(metadata_payload.get("cache_hit", False)),
    )

    record: Dict[str, Any] = {
        "timestamp": _now_iso(),
        "scene": context.scene,
        "step_name": "img_desc_augment" if _is_img_desc_augment_call(prompt, system_message) else "deepseek_complete_text",
        "input": {
            "model": str(model or ""),
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

        if not isinstance(payload.get("records"), list):
            payload["records"] = []
        if not payload.get("created_at"):
            payload["created_at"] = _now_iso()

        records = payload["records"]
        record["call_index"] = len(records) + 1
        records.append(record)

        payload["version"] = "1.0"
        payload["scene"] = context.scene
        payload["task_id"] = context.task_id
        payload["video_path"] = context.video_path
        payload["updated_at"] = _now_iso()
        payload["total_calls"] = len(records)
        payload["pricing_snapshot"] = get_token_pricing_snapshot()
        payload["summary"] = _build_payload_summary(records)

        with open(output_path, "w", encoding="utf-8") as file_obj:
            json.dump(payload, file_obj, ensure_ascii=False, indent=2)

    logger.debug("DeepSeek audit appended: %s", output_path)
