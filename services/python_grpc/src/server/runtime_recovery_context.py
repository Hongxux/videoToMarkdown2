from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from services.python_grpc.src.common.utils.runtime_recovery_store import RuntimeRecoveryStore
from services.python_grpc.src.common.utils.stage_artifact_paths import (
    phase2a_semantic_units_path as helper_phase2a_semantic_units_path,
    stage1_sentence_timestamps_candidates as helper_stage1_sentence_timestamps_candidates,
)
from services.python_grpc.src.server.stage1_runtime_repository import get_stage1_repository_views

logger = logging.getLogger(__name__)


def load_phase2b_runtime_outputs_from_store(output_dir: str, *, task_id: str = "") -> Optional[Dict[str, Any]]:
    normalized_output_dir = str(output_dir or "").strip()
    if not normalized_output_dir:
        return None
    try:
        store = RuntimeRecoveryStore(
            output_dir=normalized_output_dir,
            task_id=task_id or Path(normalized_output_dir).name,
            storage_key=Path(normalized_output_dir).name,
        )
        restored = store.load_latest_committed_chunk_payload(
            stage="phase2b",
            chunk_id="phase2b.document_assemble.wave_0001",
        )
        if not isinstance(restored, dict):
            return None
        result_payload = dict(restored.get("result_payload", {}) or {})
        markdown_path = str(result_payload.get("markdown_path", "") or "").strip()
        json_path = str(result_payload.get("json_path", "") or "").strip()
        title = str(result_payload.get("title", "") or "").strip()
        if not markdown_path and not json_path:
            return None
        return {
            "markdown_path": markdown_path,
            "json_path": json_path,
            "title": title,
            "reused": True,
        }
    except Exception as error:
        logger.warning("Load phase2b runtime outputs failed: output_dir=%s err=%s", normalized_output_dir, error)
        return None


@dataclass(frozen=True)
class RuntimeRecoveryContext:
    resolved_start_stage: str
    download_ready: bool
    video_path: str
    video_duration_sec: float
    video_title: str
    resolved_url: str
    source_platform: str
    canonical_id: str
    content_type: str
    transcribe_ready: bool
    subtitle_path: str
    stage1_ready: bool
    step2_json_path: str
    step6_json_path: str
    sentence_timestamps_path: str
    phase2a_ready: bool
    semantic_units_path: str
    phase2b_ready: bool
    markdown_path: str
    json_path: str
    reused_llm_call_count: int
    reused_chunk_count: int
    decision_reason: str

    def to_response_payload(self) -> Dict[str, Any]:
        return {
            "resolved_start_stage": self.resolved_start_stage,
            "download_ready": self.download_ready,
            "video_path": self.video_path,
            "video_duration_sec": self.video_duration_sec,
            "video_title": self.video_title,
            "resolved_url": self.resolved_url,
            "source_platform": self.source_platform,
            "canonical_id": self.canonical_id,
            "content_type": self.content_type,
            "transcribe_ready": self.transcribe_ready,
            "subtitle_path": self.subtitle_path,
            "stage1_ready": self.stage1_ready,
            "step2_json_path": self.step2_json_path,
            "step6_json_path": self.step6_json_path,
            "sentence_timestamps_path": self.sentence_timestamps_path,
            "phase2a_ready": self.phase2a_ready,
            "semantic_units_path": self.semantic_units_path,
            "phase2b_ready": self.phase2b_ready,
            "markdown_path": self.markdown_path,
            "json_path": self.json_path,
            "reused_llm_call_count": self.reused_llm_call_count,
            "reused_chunk_count": self.reused_chunk_count,
            "decision_reason": self.decision_reason,
        }


@dataclass(frozen=True)
class RuntimeRecoveryResolverCallbacks:
    resolve_stage_entry_paths: Callable[..., Dict[str, str]]
    read_video_meta_payload: Callable[[str], Dict[str, Any]]
    normalize_video_title: Callable[[str], str]
    first_non_blank: Callable[..., str]
    safe_float: Callable[[Any, float], float]
    get_runtime_recovery_store: Callable[..., Optional[RuntimeRecoveryStore]]
    get_stage1_runtime_outputs: Callable[[str], Optional[Dict[str, Any]]]
    get_transcribe_runtime_outputs: Callable[..., Optional[Dict[str, Any]]]
    materialize_subtitle_from_transcribe_runtime: Callable[..., bool]
    get_phase2a_runtime_semantic_units: Callable[..., Optional[Any]]
    get_phase2b_runtime_outputs: Callable[..., Optional[Dict[str, Any]]]
    build_stage1_runtime_outputs_fingerprint: Callable[[Optional[Dict[str, Any]]], str]
    load_stage1_output_list: Callable[[str, str], Tuple[Optional[List[Dict[str, Any]]], str]]
    write_resource_meta: Callable[..., None]
    file_signature: Callable[[str], Dict[str, Any]]


class RuntimeRecoveryResolver:
    def __init__(
        self,
        *,
        callbacks: RuntimeRecoveryResolverCallbacks,
        log: Optional[logging.Logger] = None,
    ) -> None:
        self._callbacks = callbacks
        self._logger = log or logger

    def resolve_download_recovery_metadata(
        self,
        *,
        output_dir: str,
        task_id: str,
        resolved_video_path: str,
    ) -> Dict[str, Any]:
        normalized_output_dir = os.path.abspath(str(output_dir or "").strip())
        metadata = {
            "video_path": str(resolved_video_path or "").strip(),
            "video_duration_sec": 0.0,
            "video_title": "",
            "resolved_url": "",
            "source_platform": "",
            "canonical_id": "",
            "content_type": "unknown",
        }
        if not normalized_output_dir:
            return metadata

        video_meta_payload = self._callbacks.read_video_meta_payload(normalized_output_dir)
        if isinstance(video_meta_payload, dict):
            metadata["video_path"] = self._callbacks.first_non_blank(
                str(video_meta_payload.get("video_path", "") or "").strip(),
                metadata["video_path"],
            )
            metadata["video_title"] = self._callbacks.first_non_blank(
                self._callbacks.normalize_video_title(str(video_meta_payload.get("title", "") or "")),
                metadata["video_title"],
            )
            metadata["resolved_url"] = self._callbacks.first_non_blank(
                str(video_meta_payload.get("resolved_url", "") or "").strip(),
                metadata["resolved_url"],
            )
            metadata["source_platform"] = self._callbacks.first_non_blank(
                str(video_meta_payload.get("platform", "") or "").strip(),
                metadata["source_platform"],
            )
            metadata["canonical_id"] = self._callbacks.first_non_blank(
                str(video_meta_payload.get("canonical_id", "") or "").strip(),
                metadata["canonical_id"],
            )

        runtime_store = self._callbacks.get_runtime_recovery_store(
            output_dir=normalized_output_dir,
            task_id=task_id or Path(normalized_output_dir).name,
        )
        if runtime_store is None:
            return metadata

        try:
            download_snapshot = runtime_store.load_stage_snapshot(stage="download")
        except Exception as error:
            self._logger.warning(
                "[%s] Download snapshot load failed during RecoverRuntimeContext: output_dir=%s error=%s",
                task_id,
                normalized_output_dir,
                error,
            )
            return metadata

        if not isinstance(download_snapshot, dict):
            return metadata

        metadata["video_path"] = self._callbacks.first_non_blank(
            str(download_snapshot.get("video_path", "") or "").strip(),
            metadata["video_path"],
        )
        metadata["video_duration_sec"] = self._callbacks.safe_float(
            download_snapshot.get("duration_sec", metadata["video_duration_sec"]),
            metadata["video_duration_sec"],
        )
        metadata["video_title"] = self._callbacks.first_non_blank(
            self._callbacks.normalize_video_title(str(download_snapshot.get("video_title", "") or "")),
            metadata["video_title"],
        )
        metadata["resolved_url"] = self._callbacks.first_non_blank(
            str(download_snapshot.get("resolved_url", "") or "").strip(),
            metadata["resolved_url"],
        )
        metadata["source_platform"] = self._callbacks.first_non_blank(
            str(download_snapshot.get("source_platform", "") or "").strip(),
            metadata["source_platform"],
        )
        metadata["canonical_id"] = self._callbacks.first_non_blank(
            str(download_snapshot.get("canonical_id", "") or "").strip(),
            metadata["canonical_id"],
        )
        metadata["content_type"] = self._callbacks.first_non_blank(
            str(download_snapshot.get("content_type", "") or "").strip(),
            metadata["content_type"],
        ) or "unknown"
        return metadata

    def materialize_stage1_recovery_artifacts(
        self,
        *,
        output_dir: str,
        runtime_state: Optional[Dict[str, Any]],
    ) -> Dict[str, str]:
        normalized_output_dir = os.path.abspath(str(output_dir or "").strip())
        artifact_paths = {
            "step2_json_path": "",
            "step6_json_path": "",
            "sentence_timestamps_path": "",
        }
        if not normalized_output_dir or not isinstance(runtime_state, dict):
            return artifact_paths

        runtime_views = get_stage1_repository_views(runtime_state)
        if not isinstance(runtime_views, dict) or not runtime_views:
            return artifact_paths

        intermediates_dir = os.path.join(normalized_output_dir, "intermediates")
        os.makedirs(intermediates_dir, exist_ok=True)
        stage1_fp = self._callbacks.build_stage1_runtime_outputs_fingerprint(runtime_state)

        step2_payload = runtime_views.get("step2_subtitles", [])
        step2_path = os.path.join(intermediates_dir, "step2_correction_output.json")
        existing_step2_payload, _ = self._callbacks.load_stage1_output_list(step2_path, "corrected_subtitles")
        if isinstance(existing_step2_payload, list) and existing_step2_payload:
            artifact_paths["step2_json_path"] = step2_path
        elif isinstance(step2_payload, list) and step2_payload:
            with open(step2_path, "w", encoding="utf-8") as output_stream:
                json.dump(
                    {"output": {"corrected_subtitles": step2_payload}},
                    output_stream,
                    ensure_ascii=False,
                    indent=2,
                )
            artifact_paths["step2_json_path"] = step2_path
        if artifact_paths["step2_json_path"]:
            self._callbacks.write_resource_meta(
                artifact_paths["step2_json_path"],
                group="stage1_text",
                input_fingerprint=stage1_fp,
                dependencies={},
            )

        step6_payload = runtime_views.get("step6_paragraphs", [])
        step6_path = os.path.join(intermediates_dir, "step6_merge_cross_output.json")
        existing_step6_payload, _ = self._callbacks.load_stage1_output_list(step6_path, "pure_text_script")
        if isinstance(existing_step6_payload, list) and existing_step6_payload:
            artifact_paths["step6_json_path"] = step6_path
        elif isinstance(step6_payload, list) and step6_payload:
            with open(step6_path, "w", encoding="utf-8") as output_stream:
                json.dump(
                    {"output": {"pure_text_script": step6_payload}},
                    output_stream,
                    ensure_ascii=False,
                    indent=2,
                )
            artifact_paths["step6_json_path"] = step6_path
        if artifact_paths["step6_json_path"]:
            step6_dependencies: Dict[str, Any] = {}
            if artifact_paths["step2_json_path"]:
                step6_dependencies["step2"] = self._callbacks.file_signature(artifact_paths["step2_json_path"])
            self._callbacks.write_resource_meta(
                artifact_paths["step6_json_path"],
                group="stage1_text",
                input_fingerprint=stage1_fp,
                dependencies=step6_dependencies,
            )

        sentence_timestamps_path = next(
            (
                os.path.abspath(str(candidate_path or "").strip())
                for candidate_path in helper_stage1_sentence_timestamps_candidates(normalized_output_dir)
                if candidate_path and os.path.exists(candidate_path)
            ),
            "",
        )
        runtime_sentence_timestamps = runtime_views.get("sentence_timestamps", {})
        if not sentence_timestamps_path and isinstance(runtime_sentence_timestamps, dict) and runtime_sentence_timestamps:
            sentence_timestamps_path = os.path.join(intermediates_dir, "sentence_timestamps.json")
            with open(sentence_timestamps_path, "w", encoding="utf-8") as output_stream:
                json.dump(runtime_sentence_timestamps, output_stream, ensure_ascii=False, indent=2)
        if sentence_timestamps_path:
            sentence_dependencies: Dict[str, Any] = {}
            if artifact_paths["step2_json_path"]:
                sentence_dependencies["step2"] = self._callbacks.file_signature(artifact_paths["step2_json_path"])
            if artifact_paths["step6_json_path"]:
                sentence_dependencies["step6"] = self._callbacks.file_signature(artifact_paths["step6_json_path"])
            self._callbacks.write_resource_meta(
                sentence_timestamps_path,
                group="stage1_text",
                input_fingerprint=stage1_fp,
                dependencies=sentence_dependencies,
            )
            artifact_paths["sentence_timestamps_path"] = sentence_timestamps_path

        return artifact_paths

    def count_reusable_runtime_nodes(
        self,
        *,
        output_dir: str,
        task_id: str,
    ) -> Tuple[int, int]:
        normalized_output_dir = os.path.abspath(str(output_dir or "").strip())
        if not normalized_output_dir:
            return 0, 0
        store = self._callbacks.get_runtime_recovery_store(
            output_dir=normalized_output_dir,
            task_id=str(task_id or Path(normalized_output_dir).name),
        )
        if store is None:
            return 0, 0

        reusable_statuses = {"SUCCESS", "COMPLETED", "LOCAL_COMMITTED"}
        video_stages = {"stage1", "phase2a", "asset_extract_java", "phase2b"}
        llm_call_count = 0
        chunk_count = 0
        try:
            for node_payload in store.list_scope_nodes():
                if not isinstance(node_payload, dict):
                    continue
                stage_name = str(node_payload.get("stage", "") or "").strip().lower()
                if stage_name not in video_stages:
                    continue
                status_name = str(node_payload.get("status", "") or "").strip().upper()
                if status_name not in reusable_statuses:
                    continue
                scope_type = str(node_payload.get("scope_type", "") or "").strip().lower()
                if scope_type == "llm_call":
                    llm_call_count += 1
                elif scope_type == "chunk":
                    chunk_count += 1
        except Exception as error:
            self._logger.warning(
                "[%s] Runtime scope counting failed during RecoverRuntimeContext: output_dir=%s error=%s",
                task_id,
                normalized_output_dir,
                error,
            )
        return llm_call_count, chunk_count

    def resolve_runtime_recovery_context(
        self,
        *,
        task_id: str,
        output_dir: str,
        requested_start_stage: str,
        semantic_units_path: str,
        requested_video_path: str,
        requested_subtitle_path: str,
    ) -> RuntimeRecoveryContext:
        normalized_output_dir = os.path.abspath(str(output_dir or "").strip())
        normalized_requested_stage = str(requested_start_stage or "").strip().lower() or "download"
        normalized_semantic_units_path = str(semantic_units_path or "").strip()
        normalized_requested_video_path = str(requested_video_path or "").strip()
        normalized_requested_subtitle_path = str(requested_subtitle_path or "").strip()

        stage_paths = self._callbacks.resolve_stage_entry_paths(
            requested_video_path=normalized_requested_video_path,
            requested_output_dir=normalized_output_dir,
            requested_subtitle_path=normalized_requested_subtitle_path,
        )
        resolved_video_path = str(stage_paths.get("video_path", "") or "").strip()
        resolved_subtitle_path = str(stage_paths.get("subtitle_path", "") or "").strip()

        runtime_stage1_outputs = self._callbacks.get_stage1_runtime_outputs(normalized_output_dir)
        stage1_payload = dict(runtime_stage1_outputs or {}) if isinstance(runtime_stage1_outputs, dict) else {}
        if not resolved_video_path:
            stage1_video_path = str(stage1_payload.get("video_path", "") or "").strip()
            if stage1_video_path:
                resolved_video_path = stage1_video_path
        if not resolved_subtitle_path:
            stage1_subtitle_path = str(stage1_payload.get("subtitle_path", "") or "").strip()
            if stage1_subtitle_path:
                resolved_subtitle_path = stage1_subtitle_path

        download_metadata = self.resolve_download_recovery_metadata(
            output_dir=normalized_output_dir,
            task_id=task_id,
            resolved_video_path=resolved_video_path,
        )
        resolved_video_path = self._callbacks.first_non_blank(
            str(download_metadata.get("video_path", "") or "").strip(),
            resolved_video_path,
        )
        download_ready = bool(resolved_video_path and os.path.exists(resolved_video_path))

        transcribe_runtime_state = self._callbacks.get_transcribe_runtime_outputs(
            output_dir=normalized_output_dir,
            subtitle_path=resolved_subtitle_path,
            deep_copy=True,
        )
        if not resolved_subtitle_path and isinstance(transcribe_runtime_state, dict):
            restored_subtitle_path = str(transcribe_runtime_state.get("subtitle_path", "") or "").strip()
            if restored_subtitle_path:
                resolved_subtitle_path = restored_subtitle_path
        if resolved_subtitle_path and not os.path.exists(resolved_subtitle_path):
            try:
                self._callbacks.materialize_subtitle_from_transcribe_runtime(
                    output_dir=normalized_output_dir,
                    subtitle_path=resolved_subtitle_path,
                )
            except Exception as error:
                self._logger.warning(
                    "[%s] Subtitle materialization failed during RecoverRuntimeContext: output_dir=%s subtitle_path=%s error=%s",
                    task_id,
                    normalized_output_dir,
                    resolved_subtitle_path,
                    error,
                )
        transcribe_ready = bool(resolved_subtitle_path and os.path.exists(resolved_subtitle_path))

        stage1_ready = isinstance(runtime_stage1_outputs, dict)
        stage1_artifact_paths = self.materialize_stage1_recovery_artifacts(
            output_dir=normalized_output_dir,
            runtime_state=runtime_stage1_outputs,
        )
        if stage1_ready:
            download_ready = True
            transcribe_ready = True

        sentence_timestamps_path = str(stage1_artifact_paths.get("sentence_timestamps_path", "") or "").strip()
        if not sentence_timestamps_path:
            for candidate_path in helper_stage1_sentence_timestamps_candidates(normalized_output_dir):
                normalized_candidate_path = os.path.abspath(str(candidate_path or "").strip())
                if normalized_candidate_path and os.path.exists(normalized_candidate_path):
                    sentence_timestamps_path = normalized_candidate_path
                    break

        phase2a_units = self._callbacks.get_phase2a_runtime_semantic_units(
            normalized_output_dir,
            semantic_units_path=normalized_semantic_units_path,
        )
        phase2a_ready = phase2a_units is not None
        if phase2a_ready:
            download_ready = True
            transcribe_ready = True
            stage1_ready = True
        resolved_semantic_units_path = (
            str(helper_phase2a_semantic_units_path(normalized_output_dir))
            if phase2a_ready
            else ""
        )

        asset_extract_ready = False
        runtime_store = self._callbacks.get_runtime_recovery_store(
            output_dir=normalized_output_dir,
            task_id=task_id or Path(normalized_output_dir).name,
        )
        if runtime_store is not None:
            try:
                asset_extract_snapshot = runtime_store.load_stage_snapshot(stage="asset_extract_java")
                if isinstance(asset_extract_snapshot, dict):
                    asset_extract_status = str(asset_extract_snapshot.get("status", "") or "").strip().upper()
                    asset_extract_checkpoint = str(asset_extract_snapshot.get("checkpoint", "") or "").strip().lower()
                    asset_extract_ready = (
                        asset_extract_status in {"SUCCESS", "COMPLETED", "LOCAL_COMMITTED"}
                        and asset_extract_checkpoint == "outputs_ready"
                    )
            except Exception as error:
                self._logger.warning(
                    "[%s] Asset extract snapshot load failed during RecoverRuntimeContext: output_dir=%s error=%s",
                    task_id,
                    normalized_output_dir,
                    error,
                )
        if asset_extract_ready:
            download_ready = True
            transcribe_ready = True
            stage1_ready = True
            phase2a_ready = True
            if not resolved_semantic_units_path:
                resolved_semantic_units_path = str(helper_phase2a_semantic_units_path(normalized_output_dir))

        phase2b_runtime_outputs = self._callbacks.get_phase2b_runtime_outputs(
            normalized_output_dir,
            deep_copy=True,
        )
        resolved_markdown_path = ""
        resolved_json_path = ""
        phase2b_ready = False
        if isinstance(phase2b_runtime_outputs, dict):
            candidate_markdown_path = str(phase2b_runtime_outputs.get("markdown_path", "") or "").strip()
            candidate_json_path = str(phase2b_runtime_outputs.get("json_path", "") or "").strip()
            if candidate_markdown_path and os.path.exists(candidate_markdown_path):
                resolved_markdown_path = os.path.abspath(candidate_markdown_path)
            if candidate_json_path and os.path.exists(candidate_json_path):
                resolved_json_path = os.path.abspath(candidate_json_path)
            phase2b_ready = bool(resolved_markdown_path)
        if phase2b_ready:
            download_ready = True
            transcribe_ready = True
            stage1_ready = True
            phase2a_ready = True
            asset_extract_ready = True
            if not resolved_semantic_units_path:
                resolved_semantic_units_path = str(helper_phase2a_semantic_units_path(normalized_output_dir))

        resolved_start_stage = "download"
        decision_reason = "download_required"
        if download_ready:
            resolved_start_stage = "transcribe"
            decision_reason = "download_reusable"
        if transcribe_ready:
            resolved_start_stage = "stage1"
            decision_reason = "transcribe_reusable"
        if stage1_ready:
            resolved_start_stage = "phase2a"
            decision_reason = "stage1_runtime_reusable"
        if phase2a_ready:
            resolved_start_stage = "asset_extract_java"
            decision_reason = "phase2a_semantic_units_reusable"
        if asset_extract_ready:
            resolved_start_stage = "phase2b"
            decision_reason = "asset_extract_outputs_reusable"
        if phase2b_ready:
            resolved_start_stage = "completed"
            decision_reason = "phase2b_outputs_reusable"

        reused_llm_call_count, reused_chunk_count = self.count_reusable_runtime_nodes(
            output_dir=normalized_output_dir,
            task_id=task_id,
        )
        self._logger.info(
            "[%s] RecoverRuntimeContext resolved: requested=%s resolved=%s download_ready=%s transcribe_ready=%s stage1_ready=%s phase2a_ready=%s asset_extract_ready=%s phase2b_ready=%s reused_llm_calls=%s reused_chunks=%s reason=%s",
            task_id,
            normalized_requested_stage,
            resolved_start_stage,
            download_ready,
            transcribe_ready,
            stage1_ready,
            phase2a_ready,
            asset_extract_ready,
            phase2b_ready,
            reused_llm_call_count,
            reused_chunk_count,
            decision_reason,
        )
        return RuntimeRecoveryContext(
            resolved_start_stage=resolved_start_stage,
            download_ready=download_ready,
            video_path=resolved_video_path,
            video_duration_sec=self._callbacks.safe_float(download_metadata.get("video_duration_sec", 0.0), 0.0),
            video_title=str(download_metadata.get("video_title", "") or ""),
            resolved_url=str(download_metadata.get("resolved_url", "") or ""),
            source_platform=str(download_metadata.get("source_platform", "") or ""),
            canonical_id=str(download_metadata.get("canonical_id", "") or ""),
            content_type=str(download_metadata.get("content_type", "unknown") or "unknown"),
            transcribe_ready=transcribe_ready,
            subtitle_path=resolved_subtitle_path,
            stage1_ready=stage1_ready,
            step2_json_path=str(stage1_artifact_paths.get("step2_json_path", "") or ""),
            step6_json_path=str(stage1_artifact_paths.get("step6_json_path", "") or ""),
            sentence_timestamps_path=sentence_timestamps_path,
            phase2a_ready=phase2a_ready,
            semantic_units_path=resolved_semantic_units_path,
            phase2b_ready=phase2b_ready,
            markdown_path=resolved_markdown_path,
            json_path=resolved_json_path,
            reused_llm_call_count=reused_llm_call_count,
            reused_chunk_count=reused_chunk_count,
            decision_reason=decision_reason,
        )
