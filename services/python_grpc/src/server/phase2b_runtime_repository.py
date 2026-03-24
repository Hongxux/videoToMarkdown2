from __future__ import annotations

import copy
import json
import os
import time
from typing import Any, Dict, Optional

from services.python_grpc.src.common.utils.hash_policy import sha256_bytes

PHASE2B_REPOSITORY_SCHEMA_VERSION = "phase2b.runtime_repository.v2"
_PHASE2B_VIEW_DEFAULTS = {
    "markdown_path": "",
    "json_path": "",
    "title": "",
    "fingerprint": "",
}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _normalize_path(path_value: str) -> str:
    normalized = str(path_value or "").strip()
    if not normalized:
        return ""
    return os.path.abspath(normalized)


def _build_phase2b_views(
    *,
    markdown_path: str,
    json_path: str,
    title: str,
) -> Dict[str, Any]:
    normalized_markdown_path = _normalize_path(markdown_path)
    normalized_json_path = _normalize_path(json_path)
    normalized_title = str(title or "").strip()
    fingerprint = sha256_bytes(
        json.dumps(
            {
                "markdown_path": normalized_markdown_path,
                "json_path": normalized_json_path,
                "title": normalized_title,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    )
    return {
        "markdown_path": normalized_markdown_path,
        "json_path": normalized_json_path,
        "title": normalized_title,
        "fingerprint": fingerprint,
    }


def get_phase2b_repository_views(payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    views = payload.get("views", {})
    if isinstance(views, dict) and views:
        return dict(views)
    return {field_name: copy.deepcopy(default_value) for field_name, default_value in _PHASE2B_VIEW_DEFAULTS.items()}


def build_phase2b_runtime_repository(
    *,
    output_dir: str,
    task_id: str = "",
    title: str = "",
) -> Dict[str, Any]:
    payload = {
        "schema_version": PHASE2B_REPOSITORY_SCHEMA_VERSION,
        "output_dir": os.path.abspath(str(output_dir or "").strip()),
        "task_id": str(task_id or "").strip(),
        "status": "PLANNED",
        "ready": False,
        "reused": False,
        "views": _build_phase2b_views(
            markdown_path="",
            json_path="",
            title=title,
        ),
        "updated_at_ms": _now_ms(),
    }
    return payload


def update_phase2b_repository_views(
    payload: Dict[str, Any],
    *,
    markdown_path: str,
    json_path: str,
    title: str = "",
    reused: bool = False,
    updated_at_ms: int = 0,
) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("payload must be a dict")
    payload["views"] = _build_phase2b_views(
        markdown_path=markdown_path,
        json_path=json_path,
        title=title,
    )
    payload["status"] = "READY"
    payload["ready"] = True
    payload["reused"] = bool(reused)
    payload["updated_at_ms"] = int(updated_at_ms or _now_ms())
    return payload
