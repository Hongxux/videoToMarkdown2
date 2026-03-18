from __future__ import annotations

import copy
import os
import time
from typing import Any, Dict, Optional

from services.python_grpc.src.common.utils.hash_policy import sha256_json

STAGE1_REPOSITORY_SCHEMA_VERSION = "stage1.runtime_repository.v2"
_STAGE1_STEP_ORDER = (
    "step1_validate",
    "step2_correction",
    "step3_merge",
    "step3_5_translate",
    "step4_clean_local",
    "step5_6_dedup_merge",
)
_STEP_INDEX_MAP = {name: index + 1 for index, name in enumerate(_STAGE1_STEP_ORDER)}
_VIEW_FIELD_DEFAULTS = {
    "corrected_subtitles": [],
    "merged_sentences": [],
    "translated_sentences": [],
    "cleaned_sentences": [],
    "non_redundant_sentences": [],
    "pure_text_script": [],
    "step2_subtitles": [],
    "step6_paragraphs": [],
    "sentence_timestamps": {},
    "domain": "",
    "main_topic": "",
}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _normalize_stage1_step_name(raw_step_name: Any) -> str:
    normalized = str(raw_step_name or "").strip()
    if normalized.startswith("stage1_"):
        normalized = normalized.removeprefix("stage1_")
    if ".llm_call" in normalized:
        normalized = normalized.split(".llm_call", 1)[0].strip()
    return normalized


def _normalize_stage1_runtime_outputs(final_state: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(final_state, dict):
        return None

    corrected_subtitles = final_state.get("corrected_subtitles", [])
    merged_sentences = final_state.get("merged_sentences", [])
    translated_sentences = final_state.get("translated_sentences", [])
    cleaned_sentences = final_state.get("cleaned_sentences", [])
    non_redundant_sentences = final_state.get("non_redundant_sentences", [])
    pure_text_script = final_state.get("pure_text_script", [])
    sentence_timestamps = final_state.get("sentence_timestamps", {})

    if not isinstance(corrected_subtitles, list):
        corrected_subtitles = []
    if not isinstance(merged_sentences, list):
        merged_sentences = []
    if not isinstance(translated_sentences, list):
        translated_sentences = []
    if not isinstance(cleaned_sentences, list):
        cleaned_sentences = []
    if not isinstance(non_redundant_sentences, list):
        non_redundant_sentences = []
    if not isinstance(pure_text_script, list):
        pure_text_script = []
    if not isinstance(sentence_timestamps, dict):
        sentence_timestamps = {}

    if not sentence_timestamps:
        sentence_sources = translated_sentences if translated_sentences else merged_sentences
        rebuilt_timestamps: Dict[str, Dict[str, float]] = {}
        for item in list(sentence_sources or []):
            if not isinstance(item, dict):
                continue
            sentence_id = str(item.get("sentence_id", "") or "").strip()
            if not sentence_id:
                continue
            try:
                start_sec = float(item.get("start_sec", 0.0) or 0.0)
            except Exception:
                start_sec = 0.0
            try:
                end_sec = float(item.get("end_sec", start_sec) or start_sec)
            except Exception:
                end_sec = start_sec
            rebuilt_timestamps[sentence_id] = {
                "start_sec": start_sec,
                "end_sec": max(start_sec, end_sec),
            }
        sentence_timestamps = rebuilt_timestamps

    if not corrected_subtitles and not pure_text_script and not sentence_timestamps:
        return None

    return {
        "corrected_subtitles": copy.deepcopy(corrected_subtitles),
        "merged_sentences": copy.deepcopy(merged_sentences),
        "translated_sentences": copy.deepcopy(translated_sentences),
        "cleaned_sentences": copy.deepcopy(cleaned_sentences),
        "non_redundant_sentences": copy.deepcopy(non_redundant_sentences),
        "pure_text_script": copy.deepcopy(pure_text_script),
        "step2_subtitles": copy.deepcopy(corrected_subtitles),
        "step6_paragraphs": copy.deepcopy(pure_text_script),
        "sentence_timestamps": copy.deepcopy(sentence_timestamps),
        "domain": str(final_state.get("domain", "") or ""),
        "main_topic": str(final_state.get("main_topic", "") or ""),
    }


def _build_output_fingerprint(outputs: Dict[str, Any]) -> str:
    fingerprint_payload = {
        "corrected_subtitles": outputs.get("corrected_subtitles", []),
        "merged_sentences": outputs.get("merged_sentences", []),
        "translated_sentences": outputs.get("translated_sentences", []),
        "cleaned_sentences": outputs.get("cleaned_sentences", []),
        "pure_text_script": outputs.get("pure_text_script", []),
        "sentence_timestamps": outputs.get("sentence_timestamps", {}),
        "domain": outputs.get("domain", ""),
        "main_topic": outputs.get("main_topic", ""),
    }
    if not any(value not in (None, "", [], {}) for value in fingerprint_payload.values()):
        return ""
    return sha256_json(fingerprint_payload)


def get_stage1_repository_views(payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    views = payload.get("views", {})
    if isinstance(views, dict) and views:
        return dict(views)
    fallback: Dict[str, Any] = {}
    for field_name, default_value in _VIEW_FIELD_DEFAULTS.items():
        fallback[field_name] = copy.deepcopy(payload.get(field_name, default_value))
    return fallback


def build_stage1_runtime_repository(
    *,
    output_dir: str,
    subtitle_path: str = "",
    task_id: str = "",
    video_path: str = "",
    max_step: int = 6,
    input_fingerprint: str = "",
    resume_from_step: str = "",
    resume_entry_step: str = "",
    recovery_plan_digest: str = "",
) -> Dict[str, Any]:
    normalized_output_dir = os.path.abspath(str(output_dir or "").strip())
    normalized_subtitle_path = os.path.abspath(str(subtitle_path or "").strip()) if subtitle_path else ""
    safe_max_step = max(1, _safe_int(max_step, 6))
    payload = {
        "schema_version": STAGE1_REPOSITORY_SCHEMA_VERSION,
        "output_dir": normalized_output_dir,
        "task_id": str(task_id or "").strip(),
        "video_path": str(video_path or "").strip(),
        "subtitle_path": normalized_subtitle_path,
        "max_step": safe_max_step,
        "input_fingerprint": str(input_fingerprint or "").strip(),
        "resume_from_step": str(resume_from_step or "").strip(),
        "resume_entry_step": str(resume_entry_step or "").strip(),
        "recovery_plan_digest": str(recovery_plan_digest or "").strip(),
        "status": "PLANNED",
        "current_step": "",
        "current_checkpoint": "",
        "completed": 0,
        "pending": safe_max_step,
        "ready": False,
        "reused": False,
        "step_statuses": {step_name: "PLANNED" for step_name in _STAGE1_STEP_ORDER},
        "step2_count": 0,
        "step6_count": 0,
        "sentence_timestamps_count": 0,
        "output_fingerprint": "",
        "views": copy.deepcopy(_VIEW_FIELD_DEFAULTS),
        "last_event": {},
        "error_message": "",
        "updated_at_ms": 0,
    }
    return payload


def apply_stage1_progress_event(
    payload: Dict[str, Any],
    *,
    event: Optional[Dict[str, Any]],
    updated_at_ms: int = 0,
) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("payload must be a dict")

    normalized_event = dict(event or {})
    safe_updated_at_ms = int(updated_at_ms or _now_ms())
    step_name = _normalize_stage1_step_name(
        normalized_event.get("stage_step")
        or normalized_event.get("step_name")
        or normalized_event.get("checkpoint")
        or ""
    )
    event_name = str(normalized_event.get("event", "") or "").strip().lower()
    checkpoint = str(normalized_event.get("checkpoint", "") or "").strip()
    status_raw = str(normalized_event.get("status", "") or "").strip().lower()
    completed = max(0, _safe_int(normalized_event.get("completed", payload.get("completed", 0)), payload.get("completed", 0)))
    pending = max(0, _safe_int(normalized_event.get("pending", payload.get("pending", 0)), payload.get("pending", 0)))

    payload["current_step"] = step_name or str(payload.get("current_step", "") or "")
    payload["current_checkpoint"] = checkpoint or str(payload.get("current_checkpoint", "") or "")
    payload["completed"] = completed
    payload["pending"] = pending
    payload["updated_at_ms"] = safe_updated_at_ms
    payload["last_event"] = {
        "event": event_name,
        "checkpoint": checkpoint,
        "step_name": step_name,
        "status": status_raw,
        "completed": completed,
        "pending": pending,
        "timestamp_ms": int(normalized_event.get("timestamp_ms", safe_updated_at_ms) or safe_updated_at_ms),
    }

    step_statuses = payload.setdefault("step_statuses", {})
    if not isinstance(step_statuses, dict):
        step_statuses = {}
        payload["step_statuses"] = step_statuses
    for known_step in _STAGE1_STEP_ORDER:
        step_statuses.setdefault(known_step, "PLANNED")

    if step_name in _STEP_INDEX_MAP:
        if event_name == "step_completed" or (
            checkpoint == step_name and status_raw in {"completed", "success"} and event_name != "pipeline_error"
        ):
            for known_step, step_index in _STEP_INDEX_MAP.items():
                if step_index <= _STEP_INDEX_MAP[step_name]:
                    step_statuses[known_step] = "SUCCESS"
        elif status_raw in {"failed", "error"} or event_name == "pipeline_error":
            step_statuses[step_name] = "FAILED"
        elif step_statuses.get(step_name) != "SUCCESS":
            step_statuses[step_name] = "RUNNING"

    if event_name == "pipeline_error" or status_raw in {"failed", "error"}:
        payload["status"] = "FAILED"
        payload["error_message"] = str(normalized_event.get("error", "") or "").strip()
    elif event_name == "pipeline_end" or status_raw in {"completed", "success"}:
        payload["status"] = "SUCCESS"
    else:
        payload["status"] = "RUNNING"
        payload["error_message"] = ""

    return payload


def mark_stage1_runtime_outputs_ready(
    payload: Dict[str, Any],
    *,
    final_state: Optional[Dict[str, Any]],
    reused: bool = False,
    updated_at_ms: int = 0,
) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("payload must be a dict")
    outputs = _normalize_stage1_runtime_outputs(final_state)
    if outputs is None:
        return payload

    safe_updated_at_ms = int(updated_at_ms or _now_ms())
    payload["views"] = copy.deepcopy(outputs)
    payload["step2_count"] = len(list(outputs.get("step2_subtitles", []) or []))
    payload["step6_count"] = len(list(outputs.get("step6_paragraphs", []) or []))
    payload["sentence_timestamps_count"] = len(dict(outputs.get("sentence_timestamps", {}) or {}))
    payload["output_fingerprint"] = _build_output_fingerprint(get_stage1_repository_views(payload))
    payload["ready"] = True
    payload["reused"] = bool(reused)
    payload["status"] = "SUCCESS"
    payload["current_step"] = str(payload.get("current_step", "") or "step5_6_dedup_merge").strip() or "step5_6_dedup_merge"
    payload["current_checkpoint"] = str(payload.get("current_checkpoint", "") or payload["current_step"]).strip() or payload["current_step"]
    payload["completed"] = max(_safe_int(payload.get("completed", 0), 0), _safe_int(payload.get("max_step", 6), 6))
    payload["pending"] = 0
    payload["updated_at_ms"] = safe_updated_at_ms
    step_statuses = payload.setdefault("step_statuses", {})
    if isinstance(step_statuses, dict):
        for step_name in _STAGE1_STEP_ORDER:
            step_statuses[step_name] = "SUCCESS"
    payload["error_message"] = ""
    return payload


def build_stage1_repository_from_projected_state(
    *,
    output_dir: str,
    projected_state: Optional[Dict[str, Any]],
    subtitle_path: str = "",
    task_id: str = "",
    video_path: str = "",
    max_step: int = 6,
    input_fingerprint: str = "",
) -> Optional[Dict[str, Any]]:
    outputs = _normalize_stage1_runtime_outputs(projected_state)
    if outputs is None:
        return None
    payload = build_stage1_runtime_repository(
        output_dir=output_dir,
        subtitle_path=subtitle_path,
        task_id=task_id,
        video_path=video_path,
        max_step=max_step,
        input_fingerprint=input_fingerprint,
    )
    return mark_stage1_runtime_outputs_ready(
        payload,
        final_state=outputs,
        reused=True,
    )
