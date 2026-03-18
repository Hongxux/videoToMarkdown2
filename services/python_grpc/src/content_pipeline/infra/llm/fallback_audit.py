"""LLM fallback 专项审计。"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Optional

from services.python_grpc.src.common.utils.runtime_llm_context import current_runtime_llm_context

logger = logging.getLogger(__name__)

_AUDIT_FILE_LOCK = Lock()
_DEFAULT_FILE_NAME = "fallback_records.jsonl"


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _resolve_audit_path() -> Optional[Path]:
    runtime_context = current_runtime_llm_context()
    if runtime_context is None:
        return None
    store = getattr(runtime_context, "store", None)
    runtime_root = getattr(store, "runtime_root", None)
    if runtime_root:
        return Path(str(runtime_root)) / _DEFAULT_FILE_NAME
    output_dir = str(getattr(runtime_context, "output_dir", "") or "").strip()
    if not output_dir:
        return None
    return Path(output_dir) / "intermediates" / "rt" / _DEFAULT_FILE_NAME


def _to_jsonable(payload: Any) -> Any:
    if payload is None:
        return None
    if isinstance(payload, (str, int, float, bool)):
        return payload
    if isinstance(payload, dict):
        return {
            str(key): _to_jsonable(value)
            for key, value in payload.items()
        }
    if isinstance(payload, (list, tuple, set)):
        return [_to_jsonable(item) for item in payload]
    return str(payload)


def append_llm_fallback_event(
    *,
    step_name: str,
    unit_id: str,
    llm_call_id: str = "",
    chunk_id: str = "",
    scope_ref: str = "",
    request_payload: Optional[Dict[str, Any]] = None,
    fallback_payload: Optional[Dict[str, Any]] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    runtime_context = current_runtime_llm_context()
    audit_path = _resolve_audit_path()
    if runtime_context is None or audit_path is None:
        return

    normalized_fallback = dict(fallback_payload or {})
    if not bool(normalized_fallback.get("is_fallback")):
        return

    propagated_scope_refs = [
        str(item or "").strip()
        for item in list(normalized_fallback.get("propagated_scope_refs", []) or [])
        if str(item or "").strip()
    ]
    if scope_ref and scope_ref not in propagated_scope_refs:
        propagated_scope_refs.append(scope_ref)
    normalized_fallback["propagated_scope_refs"] = propagated_scope_refs
    normalized_fallback.setdefault(
        "repair_stage",
        str(getattr(runtime_context, "stage", "") or "").strip(),
    )

    record: Dict[str, Any] = {
        "schema_version": "llm_fallback_event_v1",
        "timestamp": _now_iso(),
        "task_id": str(getattr(runtime_context, "task_id", "") or ""),
        "stage": str(getattr(runtime_context, "stage", "") or ""),
        "step_name": str(step_name or ""),
        "unit_id": str(unit_id or ""),
        "llm_call_id": str(llm_call_id or ""),
        "chunk_id": str(chunk_id or ""),
        "scope_ref": str(scope_ref or ""),
        "request": _to_jsonable(dict(request_payload or {})),
        "fallback": _to_jsonable(normalized_fallback),
    }
    if extra:
        record["extra"] = _to_jsonable(dict(extra))

    try:
        with _AUDIT_FILE_LOCK:
            store = getattr(runtime_context, "store", None)
            if store is not None and hasattr(store, "append_rt_fallback_record"):
                store.append_rt_fallback_record(dict(record))
            else:
                audit_path.parent.mkdir(parents=True, exist_ok=True)
                with open(audit_path, "a", encoding="utf-8") as file_obj:
                    file_obj.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as error:
        logger.warning("Append llm fallback audit failed: path=%s err=%s", audit_path, error)
