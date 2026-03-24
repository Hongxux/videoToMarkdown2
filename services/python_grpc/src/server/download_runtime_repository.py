from __future__ import annotations

import copy
import os
import time
from typing import Any, Dict, Optional

DOWNLOAD_REPOSITORY_SCHEMA_VERSION = "download.runtime_repository.v1"
_DOWNLOAD_VIEW_DEFAULTS = {
    "video_path": "",
    "file_size_bytes": 0,
    "duration_sec": 0.0,
    "resolved_url": "",
    "source_platform": "",
    "canonical_id": "",
    "link_resolver": "",
    "video_title": "",
    "content_type": "unknown",
}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _apply_download_view_aliases(payload: Dict[str, Any]) -> Dict[str, Any]:
    views = payload.get("views", {})
    if not isinstance(views, dict):
        views = {}
        payload["views"] = views
    for field_name, default_value in _DOWNLOAD_VIEW_DEFAULTS.items():
        payload[field_name] = copy.deepcopy(views.get(field_name, default_value))
    return payload


def get_download_repository_views(payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    views = payload.get("views", {})
    if isinstance(views, dict) and views:
        return dict(views)
    return {field_name: copy.deepcopy(payload.get(field_name, default_value)) for field_name, default_value in _DOWNLOAD_VIEW_DEFAULTS.items()}


def build_download_runtime_repository(
    *,
    output_dir: str,
    task_id: str = "",
    raw_video_input: str = "",
) -> Dict[str, Any]:
    payload = {
        "schema_version": DOWNLOAD_REPOSITORY_SCHEMA_VERSION,
        "output_dir": os.path.abspath(str(output_dir or "").strip()),
        "task_id": str(task_id or "").strip(),
        "raw_video_input": str(raw_video_input or "").strip(),
        "status": "PLANNED",
        "ready": False,
        "reused": False,
        "views": copy.deepcopy(_DOWNLOAD_VIEW_DEFAULTS),
        "updated_at_ms": _now_ms(),
    }
    return _apply_download_view_aliases(payload)


def update_download_repository_views(
    payload: Dict[str, Any],
    *,
    flow_result: Any,
    reused: bool = False,
    updated_at_ms: int = 0,
) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("payload must be a dict")
    views = {
        "video_path": str(getattr(flow_result, "video_path", "") or ""),
        "file_size_bytes": max(0, _safe_int(getattr(flow_result, "file_size_bytes", 0), 0)),
        "duration_sec": max(0.0, _safe_float(getattr(flow_result, "duration_sec", 0.0), 0.0)),
        "resolved_url": str(getattr(flow_result, "resolved_url", "") or ""),
        "source_platform": str(getattr(flow_result, "source_platform", "") or ""),
        "canonical_id": str(getattr(flow_result, "canonical_id", "") or ""),
        "link_resolver": str(getattr(flow_result, "link_resolver", "") or ""),
        "video_title": str(getattr(flow_result, "video_title", "") or ""),
        "content_type": str(getattr(flow_result, "content_type", "unknown") or "unknown"),
    }
    payload["views"] = views
    payload["status"] = "READY"
    payload["ready"] = True
    payload["reused"] = bool(reused)
    payload["updated_at_ms"] = int(updated_at_ms or _now_ms())
    return _apply_download_view_aliases(payload)
