from __future__ import annotations

import copy
import os
import time
from typing import Any, Dict, Iterable, List, Optional

from services.python_grpc.src.common.utils.hash_policy import sha256_json, sha256_text
from services.python_grpc.src.common.utils.time import format_hhmmss

TRANSCRIBE_REPOSITORY_SCHEMA_VERSION = "transcribe.runtime_repository.v1"

_STATUS_ALIASES = {
    "PLANNED": "PLANNED",
    "RUNNING": "RUNNING",
    "EXECUTING": "RUNNING",
    "SUCCESS": "SUCCESS",
    "COMPLETED": "SUCCESS",
    "LOCAL_COMMITTED": "SUCCESS",
    "FAILED": "FAILED",
    "ERROR": "ERROR",
    "MANUAL_NEEDED": "MANUAL_NEEDED",
    "MANUAL_RETRY_REQUIRED": "MANUAL_NEEDED",
}
_RESOURCE_EXHAUSTION_MARKERS = (
    "mkl_malloc",
    "failed to allocate memory",
    "cannot allocate memory",
    "out of memory",
    "insufficient memory",
    "std::bad_alloc",
    "bad allocation",
    "memoryerror",
    "resource exhausted",
)


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


def _normalize_status(status: Any) -> str:
    normalized = str(status or "").strip().upper()
    return _STATUS_ALIASES.get(normalized, normalized or "PLANNED")


def _normalize_subtitles(subtitles: Any) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for item in list(subtitles or []):
        if not isinstance(item, dict):
            continue
        normalized.append(
            {
                "start": _safe_float(item.get("start", 0.0), 0.0),
                "end": _safe_float(item.get("end", item.get("start", 0.0)), _safe_float(item.get("start", 0.0), 0.0)),
                "text": str(item.get("text", "") or ""),
            }
        )
    normalized.sort(key=lambda item: (float(item.get("start", 0.0) or 0.0), str(item.get("text", "") or "")))
    return normalized


def _format_subtitle_text(subtitles: Iterable[Dict[str, Any]]) -> str:
    lines: List[str] = []
    for subtitle in list(subtitles or []):
        if not isinstance(subtitle, dict):
            continue
        lines.append(f"[{format_hhmmss(_safe_float(subtitle.get('start', 0.0), 0.0))}] {str(subtitle.get('text', '') or '')}")
    return "\n".join(lines)


def _classify_error_status(error_message: str) -> str:
    lowered = str(error_message or "").strip().lower()
    if lowered and any(marker in lowered for marker in _RESOURCE_EXHAUSTION_MARKERS):
        return "ERROR"
    return "FAILED"


def _normalize_segment_identity(segment: Optional[Dict[str, Any]], total_segments: int) -> Dict[str, Any]:
    raw_segment = dict(segment or {})
    segment_id = max(0, _safe_int(raw_segment.get("id", raw_segment.get("segment_id", 0)), 0))
    segment_index = max(1, _safe_int(raw_segment.get("segment_index", segment_id + 1), segment_id + 1))
    normalized_total_segments = max(segment_index, _safe_int(total_segments, segment_index))
    return {
        "segment_id": segment_id,
        "segment_index": segment_index,
        "total_segments": normalized_total_segments,
        "segment": {
            "id": segment_id,
            "start": _safe_float(raw_segment.get("start", raw_segment.get("segment_start_sec", 0.0)), 0.0),
            "end": _safe_float(raw_segment.get("end", raw_segment.get("segment_end_sec", 0.0)), 0.0),
            "duration": _safe_float(raw_segment.get("duration", raw_segment.get("segment_duration_sec", 0.0)), 0.0),
        },
    }


def build_transcribe_runtime_repository(
    *,
    output_dir: str,
    subtitle_path: str,
    task_id: str = "",
    video_path: str = "",
    language: str = "",
    input_fingerprint: str = "",
) -> Dict[str, Any]:
    normalized_output_dir = os.path.abspath(str(output_dir or "").strip())
    normalized_subtitle_path = os.path.abspath(str(subtitle_path or "").strip()) if subtitle_path else ""
    return {
        "schema_version": TRANSCRIBE_REPOSITORY_SCHEMA_VERSION,
        "output_dir": normalized_output_dir,
        "task_id": str(task_id or "").strip(),
        "video_path": str(video_path or "").strip(),
        "subtitle_path": normalized_subtitle_path,
        "language": str(language or "").strip(),
        "input_fingerprint": str(input_fingerprint or "").strip(),
        "segments": {},
        "segment_count": 0,
        "planned_segment_count": 0,
        "running_segment_count": 0,
        "successful_segment_count": 0,
        "failed_segment_count": 0,
        "ready_segment_count": 0,
        "subtitle_count": 0,
        "subtitle_text": "",
        "subtitle_sha256": "",
        "status": "PLANNED",
        "ready": False,
        "reused": False,
        "updated_at_ms": 0,
    }


def _refresh_transcribe_runtime_repository(payload: Dict[str, Any], *, updated_at_ms: int) -> Dict[str, Any]:
    segments = payload.setdefault("segments", {})
    if not isinstance(segments, dict):
        segments = {}
        payload["segments"] = segments

    ordered_segments: List[Dict[str, Any]] = []
    planned_count = 0
    running_count = 0
    success_count = 0
    failed_count = 0
    total_segments = max(0, _safe_int(payload.get("segment_count", 0), 0))

    for chunk_id, raw_entry in list(segments.items()):
        if not isinstance(raw_entry, dict):
            continue
        entry = dict(raw_entry)
        entry["chunk_id"] = str(chunk_id or entry.get("chunk_id", "") or "")
        entry["status"] = _normalize_status(entry.get("status"))
        entry["segment_id"] = max(0, _safe_int(entry.get("segment_id", 0), 0))
        entry["segment_index"] = max(1, _safe_int(entry.get("segment_index", entry["segment_id"] + 1), entry["segment_id"] + 1))
        entry["total_segments"] = max(
            entry["segment_index"],
            _safe_int(entry.get("total_segments", entry["segment_index"]), entry["segment_index"]),
        )
        entry["subtitle_count"] = max(0, _safe_int(entry.get("subtitle_count", 0), 0))
        segments[entry["chunk_id"]] = entry
        ordered_segments.append(entry)
        total_segments = max(total_segments, entry["total_segments"], entry["segment_index"])
        if entry["status"] == "PLANNED":
            planned_count += 1
        elif entry["status"] == "RUNNING":
            running_count += 1
        elif entry["status"] == "SUCCESS":
            success_count += 1
        elif entry["status"] in {"FAILED", "ERROR", "MANUAL_NEEDED"}:
            failed_count += 1

    ordered_segments.sort(key=lambda item: (item.get("segment_index", 0), item.get("chunk_id", "")))

    all_subtitles: List[Dict[str, Any]] = []
    for entry in ordered_segments:
        if entry.get("status") != "SUCCESS":
            continue
        result_payload = dict(entry.get("result_payload", {}) or {})
        all_subtitles.extend(_normalize_subtitles(result_payload.get("subtitles")))

    if all_subtitles:
        payload["subtitle_text"] = _format_subtitle_text(all_subtitles)
        payload["subtitle_sha256"] = sha256_text(payload["subtitle_text"])
    else:
        payload["subtitle_text"] = str(payload.get("subtitle_text", "") or "")
        payload["subtitle_sha256"] = sha256_text(payload["subtitle_text"]) if payload["subtitle_text"] else ""

    payload["segment_count"] = total_segments
    payload["planned_segment_count"] = planned_count
    payload["running_segment_count"] = running_count
    payload["successful_segment_count"] = success_count
    payload["failed_segment_count"] = failed_count
    payload["ready_segment_count"] = success_count
    payload["subtitle_count"] = len(all_subtitles)

    if failed_count > 0:
        if any(str(entry.get("status", "") or "") == "MANUAL_NEEDED" for entry in ordered_segments):
            payload["status"] = "MANUAL_NEEDED"
        elif any(str(entry.get("status", "") or "") == "ERROR" for entry in ordered_segments):
            payload["status"] = "ERROR"
        else:
            payload["status"] = "FAILED"
    elif total_segments > 0 and success_count >= total_segments:
        payload["status"] = "SUCCESS"
    elif running_count > 0:
        payload["status"] = "RUNNING"
    elif planned_count > 0:
        payload["status"] = "PLANNED"
    elif payload.get("subtitle_text"):
        payload["status"] = "SUCCESS"
    else:
        payload["status"] = "PLANNED"

    payload["ready"] = bool(payload.get("subtitle_text")) and str(payload.get("status", "") or "") == "SUCCESS"
    payload["updated_at_ms"] = int(updated_at_ms or _now_ms())
    return payload


def upsert_transcribe_runtime_segment(
    payload: Dict[str, Any],
    *,
    segment: Optional[Dict[str, Any]],
    total_segments: int,
    chunk_id: str,
    input_fingerprint: str,
    status: str,
    scope_ref: str = "",
    result_payload: Optional[Dict[str, Any]] = None,
    error: Any = None,
    source: str = "runtime",
    updated_at_ms: int = 0,
) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("payload must be a dict")
    normalized_updated_at_ms = int(updated_at_ms or _now_ms())
    segments = payload.setdefault("segments", {})
    if not isinstance(segments, dict):
        segments = {}
        payload["segments"] = segments

    identity = _normalize_segment_identity(segment, total_segments)
    normalized_chunk_id = str(chunk_id or "").strip() or f"ts{identity['segment_index']:06d}"
    entry = dict(segments.get(normalized_chunk_id, {}) or {})
    entry.update(identity)
    entry["chunk_id"] = normalized_chunk_id
    entry["input_fingerprint"] = str(input_fingerprint or entry.get("input_fingerprint", "") or "").strip()
    entry["scope_ref"] = str(scope_ref or entry.get("scope_ref", "") or "").strip()
    entry["source"] = str(source or entry.get("source", "") or "").strip()
    entry["status"] = _normalize_status(status)
    entry["updated_at_ms"] = normalized_updated_at_ms

    if isinstance(result_payload, dict):
        normalized_result_payload = copy.deepcopy(result_payload)
        entry["result_payload"] = normalized_result_payload
        entry["result_hash"] = sha256_json(normalized_result_payload)
        subtitles = _normalize_subtitles(normalized_result_payload.get("subtitles"))
        entry["subtitle_count"] = len(subtitles)
        entry["error_class"] = ""
        entry["error_message"] = ""

    if error is not None:
        error_message = str(error or "").strip()
        entry["error_class"] = getattr(error, "__class__", type(error)).__name__ if not isinstance(error, str) else "RuntimeError"
        entry["error_message"] = error_message
        if entry["status"] not in {"FAILED", "ERROR", "MANUAL_NEEDED"}:
            entry["status"] = _classify_error_status(error_message)

    segments[normalized_chunk_id] = entry
    return _refresh_transcribe_runtime_repository(payload, updated_at_ms=normalized_updated_at_ms)


def mark_transcribe_repository_completed(
    payload: Dict[str, Any],
    *,
    subtitle_path: str = "",
    subtitle_text: str = "",
    reused: bool = False,
    updated_at_ms: int = 0,
) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("payload must be a dict")
    if subtitle_path:
        payload["subtitle_path"] = os.path.abspath(str(subtitle_path or "").strip())
    if subtitle_text:
        payload["subtitle_text"] = str(subtitle_text or "")
    if reused:
        payload["reused"] = True
    return _refresh_transcribe_runtime_repository(payload, updated_at_ms=int(updated_at_ms or _now_ms()))


def build_transcribe_repository_from_restored_rows(
    *,
    output_dir: str,
    subtitle_path: str,
    restored_rows: Optional[List[Dict[str, Any]]],
    task_id: str = "",
    video_path: str = "",
    language: str = "",
    input_fingerprint: str = "",
) -> Dict[str, Any]:
    payload = build_transcribe_runtime_repository(
        output_dir=output_dir,
        subtitle_path=subtitle_path,
        task_id=task_id,
        video_path=video_path,
        language=language,
        input_fingerprint=input_fingerprint,
    )

    for row in list(restored_rows or []):
        if not isinstance(row, dict):
            continue
        request_payload = dict(row.get("request", {}) or {})
        restored_payload = dict(row.get("restored", {}) or {})
        result_payload = dict(restored_payload.get("result_payload", {}) or {})
        if not isinstance(result_payload, dict):
            continue

        segment_payload = result_payload.get("segment")
        if not isinstance(segment_payload, dict):
            segment_payload = {
                "id": _safe_int(result_payload.get("segment_id", request_payload.get("segment_id", 0)), 0),
                "start": 0.0,
                "end": 0.0,
                "duration": 0.0,
            }
        total_segments = max(
            1,
            _safe_int(result_payload.get("total_segments", 0), 0)
            or _safe_int(request_payload.get("total_segments", 0), 0)
            or (_safe_int(result_payload.get("segment_id", 0), 0) + 1),
        )

        upsert_transcribe_runtime_segment(
            payload,
            segment=segment_payload,
            total_segments=total_segments,
            chunk_id=str(request_payload.get("chunk_id", "") or ""),
            input_fingerprint=str(request_payload.get("input_fingerprint", "") or ""),
            status="SUCCESS",
            result_payload=result_payload,
            source="sqlite_restore",
        )

    return payload
