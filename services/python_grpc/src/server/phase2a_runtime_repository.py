from __future__ import annotations

import copy
import gzip
import json
import os
import time
import uuid
from typing import Any, Dict, List, Optional

from services.python_grpc.src.common.utils.hash_policy import sha256_bytes

PHASE2A_REPOSITORY_SCHEMA_VERSION = "phase2a.runtime_repository.v2"
_PHASE2A_VIEW_FIELD_DEFAULTS = {
    "semantic_units_path": "",
    "semantic_units": [],
    "unit_count": 0,
    "fingerprint": "",
    "inline_payload": b"",
    "inline_codec": "json-utf8",
    "inline_sha256": "",
}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _normalize_output_dir(output_dir: str) -> str:
    return os.path.abspath(str(output_dir or "").strip())


def _normalize_path(path_value: str) -> str:
    normalized = str(path_value or "").strip()
    if not normalized:
        return ""
    return os.path.abspath(normalized)


def _build_phase2a_views(
    *,
    semantic_units_path: str,
    semantic_units: List[Dict[str, Any]],
) -> Dict[str, Any]:
    canonical_bytes = json.dumps(
        semantic_units,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    fingerprint = sha256_bytes(canonical_bytes)
    compressed_bytes = gzip.compress(canonical_bytes)
    if len(compressed_bytes) < len(canonical_bytes):
        inline_payload = compressed_bytes
        inline_codec = "json-utf8-gzip"
    else:
        inline_payload = canonical_bytes
        inline_codec = "json-utf8"
    inline_sha256 = sha256_bytes(inline_payload)
    return {
        "semantic_units_path": _normalize_path(semantic_units_path),
        "semantic_units": copy.deepcopy(list(semantic_units or [])),
        "unit_count": len(list(semantic_units or [])),
        "fingerprint": fingerprint,
        "inline_payload": inline_payload,
        "inline_codec": inline_codec,
        "inline_sha256": inline_sha256,
    }


def get_phase2a_repository_views(payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    views = payload.get("views", {})
    if isinstance(views, dict) and views:
        normalized = dict(views)
        normalized["inline_payload"] = bytes(normalized.get("inline_payload") or b"")
        return normalized
    return {
        field_name: (bytes(default_value or b"") if field_name == "inline_payload" else copy.deepcopy(default_value))
        for field_name, default_value in _PHASE2A_VIEW_FIELD_DEFAULTS.items()
    }


def build_phase2a_runtime_repository(
    *,
    output_dir: str,
    semantic_units_path: str = "",
    semantic_units: Optional[List[Dict[str, Any]]] = None,
    task_id: str = "",
    ref_id: str = "",
) -> Dict[str, Any]:
    normalized_output_dir = _normalize_output_dir(output_dir)
    normalized_ref_id = str(ref_id or "").strip() or f"{(task_id or 'phase2a')}_{uuid.uuid4().hex}"
    safe_units = list(semantic_units or [])
    payload = {
        "schema_version": PHASE2A_REPOSITORY_SCHEMA_VERSION,
        "output_dir": normalized_output_dir,
        "task_id": str(task_id or "").strip(),
        "ref_id": normalized_ref_id,
        "status": "READY" if safe_units else "PLANNED",
        "ready": bool(safe_units),
        "reused": False,
        "views": _build_phase2a_views(
            semantic_units_path=semantic_units_path,
            semantic_units=safe_units,
        ),
        "updated_at_ms": _now_ms(),
    }
    return payload


def update_phase2a_repository_views(
    payload: Dict[str, Any],
    *,
    semantic_units_path: str,
    semantic_units: List[Dict[str, Any]],
    reused: bool = False,
    updated_at_ms: int = 0,
) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("payload must be a dict")
    payload["views"] = _build_phase2a_views(
        semantic_units_path=semantic_units_path,
        semantic_units=list(semantic_units or []),
    )
    payload["status"] = "READY"
    payload["ready"] = True
    payload["reused"] = bool(reused)
    payload["updated_at_ms"] = int(updated_at_ms or _now_ms())
    return payload
