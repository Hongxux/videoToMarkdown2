"""Task-scoped audit records for Vision AI calls."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Optional

from services.python_grpc.src.common.utils.runtime_llm_context import current_runtime_llm_context
from services.python_grpc.src.content_pipeline.infra.llm.token_costing import (
    build_token_cost_estimate,
    get_token_pricing_snapshot,
    normalize_usage_payload,
    summarize_token_cost_records,
)

logger = logging.getLogger(__name__)

_AUDIT_FILE_LOCK = Lock()
_DEFAULT_FILE_NAME = "vision_ai_call_audit.json"


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _metadata_to_dict(metadata: Any) -> Dict[str, Any]:
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


def _resolve_audit_path() -> Optional[Path]:
    runtime_context = current_runtime_llm_context()
    if runtime_context is None:
        return None
    output_dir = str(getattr(runtime_context, "output_dir", "") or "").strip()
    if not output_dir:
        return None
    return Path(output_dir) / "intermediates" / _DEFAULT_FILE_NAME


def _initialize_audit_file(audit_path: Path, *, task_id: str, stage: str) -> None:
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": "1.0",
        "scene": "phase2a_vision_ai",
        "stage": str(stage or ""),
        "task_id": str(task_id or ""),
        "created_at": _now_iso(),
        "updated_at": _now_iso(),
        "total_calls": 0,
        "pricing_snapshot": get_token_pricing_snapshot(),
        "summary": _build_payload_summary([]),
        "records": [],
    }
    with open(audit_path, "w", encoding="utf-8") as file_obj:
        json.dump(payload, file_obj, ensure_ascii=False, indent=2)


def append_vision_ai_call_record(
    *,
    request_name: str,
    request_payload: Dict[str, Any],
    response_payload: Any,
    response_metadata: Any,
    error: str = "",
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    runtime_context = current_runtime_llm_context()
    audit_path = _resolve_audit_path()
    if runtime_context is None or audit_path is None:
        return

    metadata_payload = _metadata_to_dict(response_metadata)
    usage_source = metadata_payload.get("usage_details") or metadata_payload
    token_usage = normalize_usage_payload(usage_source)
    actual_model = str(metadata_payload.get("model") or request_payload.get("model", "") or "")
    cost_estimate = build_token_cost_estimate(
        usage=token_usage,
        model=actual_model,
        base_url=request_payload.get("base_url", ""),
        local_cache_hit=bool(metadata_payload.get("cache_hit", False)),
    )

    record: Dict[str, Any] = {
        "timestamp": _now_iso(),
        "scene": "phase2a_vision_ai",
        "stage": str(getattr(runtime_context, "stage", "") or ""),
        "step_name": str(request_name or ""),
        "input": dict(request_payload or {}),
        "output": {
            "success": not bool(error),
            "response": response_payload,
            "metadata": metadata_payload,
            "error": str(error or ""),
        },
        "token_usage": token_usage,
        "cost_estimate": cost_estimate,
    }
    if extra:
        record["extra"] = dict(extra)

    with _AUDIT_FILE_LOCK:
        if not audit_path.exists():
            _initialize_audit_file(
                audit_path,
                task_id=str(getattr(runtime_context, "task_id", "") or ""),
                stage=str(getattr(runtime_context, "stage", "") or ""),
            )

        try:
            with open(audit_path, "r", encoding="utf-8") as file_obj:
                payload = json.load(file_obj)
            if not isinstance(payload, dict):
                payload = {}
        except Exception:
            payload = {}

        records = payload.get("records")
        if not isinstance(records, list):
            records = []
            payload["records"] = records

        record["call_index"] = len(records) + 1
        records.append(record)

        payload["version"] = "1.0"
        payload["scene"] = "phase2a_vision_ai"
        payload["stage"] = str(getattr(runtime_context, "stage", "") or "")
        payload["task_id"] = str(getattr(runtime_context, "task_id", "") or "")
        payload["updated_at"] = _now_iso()
        payload["total_calls"] = len(records)
        payload["pricing_snapshot"] = get_token_pricing_snapshot()
        payload["summary"] = _build_payload_summary(records)

        with open(audit_path, "w", encoding="utf-8") as file_obj:
            json.dump(payload, file_obj, ensure_ascii=False, indent=2)

    logger.debug("Vision AI audit appended: %s", audit_path)
