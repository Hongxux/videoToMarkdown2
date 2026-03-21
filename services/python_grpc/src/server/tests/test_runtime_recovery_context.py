import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

from services.python_grpc.src.server.runtime_recovery_context import (
    RuntimeRecoveryResolver,
    RuntimeRecoveryResolverCallbacks,
)


class _FakeRuntimeStore:
    def __init__(self, snapshots=None, scope_nodes=None):
        self._snapshots = snapshots or {}
        self._scope_nodes = list(scope_nodes or [])

    def load_stage_snapshot(self, *, stage: str):
        return self._snapshots.get(stage)

    def list_scope_nodes(self):
        return list(self._scope_nodes)


def _build_callbacks(*, store, runtime_state, phase2a_units=None, phase2b_outputs=None):
    def _resolve_stage_entry_paths(*, requested_video_path, requested_output_dir="", requested_subtitle_path=""):
        output_dir = str(requested_output_dir or "").strip()
        return {
            "video_path": str(Path(output_dir) / "video.mp4"),
            "subtitle_path": str(Path(output_dir) / "subtitles.txt"),
            "output_dir": output_dir,
            "output_dir_source": "request.output_dir",
            "video_path_source": "output_dir/video_meta.json",
            "subtitle_path_source": "output_dir/subtitles.txt",
        }

    def _load_stage1_output_list(resource_path: str, output_field: str):
        path = Path(resource_path)
        if not path.exists():
            return None, "missing_resource"
        payload = json.loads(path.read_text(encoding="utf-8"))
        data = payload.get("output", payload)
        value = data.get(output_field)
        if isinstance(value, list):
            return value, "ok"
        return None, "missing_output_field"

    def _write_resource_meta(resource_path: str, **kwargs):
        meta_path = Path(resource_path + ".meta.json")
        meta_path.write_text(json.dumps(kwargs, ensure_ascii=False, indent=2), encoding="utf-8")

    return RuntimeRecoveryResolverCallbacks(
        resolve_stage_entry_paths=_resolve_stage_entry_paths,
        read_video_meta_payload=lambda task_dir: {
            "video_path": str(Path(task_dir) / "video.mp4"),
            "title": "Meta Title",
            "resolved_url": "https://example.com/meta",
            "platform": "meta-platform",
            "canonical_id": "meta-id",
        },
        normalize_video_title=lambda raw_title: str(raw_title or "").strip(),
        first_non_blank=lambda *values: next((str(value) for value in values if str(value or "").strip()), ""),
        safe_float=lambda value, default=0.0: float(value) if value not in (None, "") else float(default),
        get_runtime_recovery_store=lambda *, output_dir, task_id: store,
        get_stage1_runtime_outputs=lambda output_dir: runtime_state,
        get_transcribe_runtime_outputs=lambda **kwargs: {"subtitle_path": kwargs.get("subtitle_path", "")},
        materialize_subtitle_from_transcribe_runtime=lambda **kwargs: False,
        get_phase2a_runtime_semantic_units=lambda output_dir, semantic_units_path="": phase2a_units,
        get_phase2b_runtime_outputs=lambda output_dir, deep_copy=True: phase2b_outputs,
        build_stage1_runtime_outputs_fingerprint=lambda runtime_state: "fp-stage1",
        load_stage1_output_list=_load_stage1_output_list,
        write_resource_meta=_write_resource_meta,
        file_signature=lambda path: {"exists": Path(path).exists(), "path": str(Path(path))},
    )


def test_materialize_stage1_recovery_artifacts_writes_missing_files(tmp_path):
    runtime_state = {
        "views": {
            "step2_subtitles": [{"subtitle_id": "SUB001", "corrected_text": "hello"}],
            "step6_paragraphs": [{"paragraph_id": "P001", "text": "world", "source_sentence_ids": ["S001"]}],
            "sentence_timestamps": {"S001": {"start_sec": 0.0, "end_sec": 1.0}},
        }
    }
    resolver = RuntimeRecoveryResolver(
        callbacks=_build_callbacks(
            store=_FakeRuntimeStore(),
            runtime_state=runtime_state,
        )
    )

    artifact_paths = resolver.materialize_stage1_recovery_artifacts(
        output_dir=str(tmp_path),
        runtime_state=runtime_state,
    )

    assert Path(artifact_paths["step2_json_path"]).exists()
    assert Path(artifact_paths["step6_json_path"]).exists()
    assert Path(artifact_paths["sentence_timestamps_path"]).exists()


def test_resolve_download_recovery_metadata_prefers_download_snapshot(tmp_path):
    snapshot_store = _FakeRuntimeStore(
        snapshots={
            "download": {
                "video_path": str(tmp_path / "video.mp4"),
                "duration_sec": 321.0,
                "video_title": "Recovered Title",
                "resolved_url": "https://example.com/resolved",
                "source_platform": "bilibili",
                "canonical_id": "BV1xx",
                "content_type": "video",
            }
        }
    )
    resolver = RuntimeRecoveryResolver(
        callbacks=_build_callbacks(
            store=snapshot_store,
            runtime_state=None,
        )
    )

    metadata = resolver.resolve_download_recovery_metadata(
        output_dir=str(tmp_path),
        task_id="task-download",
        resolved_video_path="",
    )

    assert metadata["video_duration_sec"] == 321.0
    assert metadata["video_title"] == "Recovered Title"
    assert metadata["resolved_url"] == "https://example.com/resolved"
    assert metadata["source_platform"] == "bilibili"
    assert metadata["canonical_id"] == "BV1xx"
    assert metadata["content_type"] == "video"


def test_resolve_runtime_recovery_context_returns_completed_when_phase2b_outputs_ready(tmp_path):
    task_dir = tmp_path / "task"
    task_dir.mkdir(parents=True, exist_ok=True)
    (task_dir / "video.mp4").write_bytes(b"video")
    (task_dir / "subtitles.txt").write_text("subtitle", encoding="utf-8")
    markdown_path = task_dir / "result.md"
    json_path = task_dir / "result.json"
    markdown_path.write_text("# recovered", encoding="utf-8")
    json_path.write_text("{\"ok\":true}", encoding="utf-8")

    runtime_state = {
        "video_path": str(task_dir / "video.mp4"),
        "subtitle_path": str(task_dir / "subtitles.txt"),
        "views": {
            "step2_subtitles": [{"subtitle_id": "SUB001", "corrected_text": "hello"}],
            "step6_paragraphs": [{"paragraph_id": "P001", "text": "world", "source_sentence_ids": ["S001"]}],
            "sentence_timestamps": {"S001": {"start_sec": 0.0, "end_sec": 1.0}},
        },
    }
    scope_nodes = [
        {"stage": "stage1", "status": "SUCCESS", "scope_type": "llm_call"},
        {"stage": "phase2a", "status": "SUCCESS", "scope_type": "chunk"},
    ]
    store = _FakeRuntimeStore(
        snapshots={
            "download": {
                "video_path": str(task_dir / "video.mp4"),
                "duration_sec": 111.0,
                "video_title": "Recovered",
                "resolved_url": "https://example.com/video",
                "source_platform": "bilibili",
                "canonical_id": "BV1xx",
                "content_type": "video",
            },
            "asset_extract_java": {
                "status": "SUCCESS",
                "checkpoint": "outputs_ready",
            },
        },
        scope_nodes=scope_nodes,
    )
    resolver = RuntimeRecoveryResolver(
        callbacks=_build_callbacks(
            store=store,
            runtime_state=runtime_state,
            phase2a_units=[{"unit_id": "U001"}],
            phase2b_outputs={
                "markdown_path": str(markdown_path),
                "json_path": str(json_path),
                "title": "Recovered",
            },
        )
    )

    context = resolver.resolve_runtime_recovery_context(
        task_id="task-completed",
        output_dir=str(task_dir),
        requested_start_stage="phase2b",
        semantic_units_path="",
        requested_video_path="https://example.com/video",
        requested_subtitle_path="",
    )

    assert context.resolved_start_stage == "completed"
    assert context.phase2b_ready is True
    assert context.markdown_path == str(markdown_path.resolve())
    assert context.json_path == str(json_path.resolve())
    assert context.reused_llm_call_count == 1
    assert context.reused_chunk_count == 1
