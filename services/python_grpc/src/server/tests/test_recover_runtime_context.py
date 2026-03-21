import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[5]))


try:
    from services.python_grpc.src.server import grpc_service_impl as impl
except Exception as import_error:
    pytest.skip(f"grpc_service_impl import unavailable: {import_error}", allow_module_level=True)


class _FakeRuntimeStore:
    def __init__(self, snapshot=None):
        self._snapshot = snapshot

    def load_stage_snapshot(self, *, stage: str):
        if isinstance(self._snapshot, dict) and stage in self._snapshot:
            return self._snapshot.get(stage)
        return self._snapshot


def _build_servicer():
    servicer = impl._VideoProcessingServicerCore.__new__(impl._VideoProcessingServicerCore)
    servicer._get_phase2b_runtime_outputs = lambda output_dir, deep_copy=True: None
    return servicer


def _write_video_meta(task_dir: Path, video_path: Path) -> None:
    task_dir.mkdir(parents=True, exist_ok=True)
    (task_dir / "video_meta.json").write_text(
        json.dumps({"video_path": str(video_path)}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def test_recover_runtime_context_returns_stage1_when_download_and_transcribe_are_ready(tmp_path):
    task_dir = tmp_path / "task"
    video_path = task_dir / "video.mp4"
    subtitle_path = task_dir / "subtitles.txt"
    task_dir.mkdir(parents=True, exist_ok=True)
    video_path.write_bytes(b"video")
    subtitle_path.write_text("demo subtitle", encoding="utf-8")
    _write_video_meta(task_dir, video_path)

    servicer = _build_servicer()
    servicer._get_stage1_runtime_outputs = lambda output_dir: None
    servicer._get_phase2a_runtime_semantic_units = lambda output_dir, semantic_units_path="": None
    servicer._get_transcribe_runtime_outputs = lambda output_dir, subtitle_path="", deep_copy=True: {"subtitle_path": str(subtitle_path)}
    servicer._materialize_subtitle_from_transcribe_runtime = lambda output_dir, subtitle_path: False
    servicer._get_runtime_recovery_store = lambda output_dir, task_id: None
    servicer._count_reusable_runtime_nodes = lambda output_dir, task_id: (3, 7)

    request = SimpleNamespace(
        task_id="task-stage1-resume",
        output_dir=str(task_dir),
        requested_start_stage="transcribe",
        semantic_units_path="",
        requested_video_path="https://example.com/video",
        requested_subtitle_path="",
    )

    response = asyncio.run(servicer.RecoverRuntimeContext(request, context=None))

    assert response.success is True
    assert response.resolved_start_stage == "stage1"
    assert response.download_ready is True
    assert response.transcribe_ready is True
    assert response.video_path == str(video_path.resolve())
    assert response.subtitle_path == str(subtitle_path.resolve())
    assert response.stage1_ready is False
    assert response.reused_llm_call_count == 3
    assert response.reused_chunk_count == 7


def test_recover_runtime_context_materializes_subtitle_from_transcribe_runtime(tmp_path):
    task_dir = tmp_path / "task"
    video_path = task_dir / "video.mp4"
    subtitle_path = task_dir / "subtitles.txt"
    task_dir.mkdir(parents=True, exist_ok=True)
    video_path.write_bytes(b"video")
    _write_video_meta(task_dir, video_path)

    servicer = _build_servicer()
    servicer._get_stage1_runtime_outputs = lambda output_dir: None
    servicer._get_phase2a_runtime_semantic_units = lambda output_dir, semantic_units_path="": None
    servicer._get_transcribe_runtime_outputs = lambda output_dir, subtitle_path="", deep_copy=True: {
        "subtitle_path": str(subtitle_path),
        "subtitle_text": "restored subtitle",
    }

    def _materialize(output_dir, subtitle_path):
        _ = output_dir
        Path(subtitle_path).write_text("restored subtitle", encoding="utf-8")
        return True

    servicer._materialize_subtitle_from_transcribe_runtime = _materialize
    servicer._get_runtime_recovery_store = lambda output_dir, task_id: None
    servicer._count_reusable_runtime_nodes = lambda output_dir, task_id: (0, 1)

    request = SimpleNamespace(
        task_id="task-materialize-subtitle",
        output_dir=str(task_dir),
        requested_start_stage="stage1",
        semantic_units_path="",
        requested_video_path="https://example.com/video",
        requested_subtitle_path="",
    )

    response = asyncio.run(servicer.RecoverRuntimeContext(request, context=None))

    assert response.success is True
    assert response.resolved_start_stage == "stage1"
    assert response.transcribe_ready is True
    assert Path(response.subtitle_path).read_text(encoding="utf-8") == "restored subtitle"


def test_recover_runtime_context_returns_asset_extract_when_phase2a_is_ready(tmp_path):
    task_dir = tmp_path / "task"
    video_path = task_dir / "video.mp4"
    subtitle_path = task_dir / "subtitles.txt"
    task_dir.mkdir(parents=True, exist_ok=True)
    video_path.write_bytes(b"video")
    subtitle_path.write_text("demo subtitle", encoding="utf-8")
    _write_video_meta(task_dir, video_path)

    servicer = _build_servicer()
    servicer._get_stage1_runtime_outputs = lambda output_dir: {
        "video_path": str(video_path),
        "subtitle_path": str(subtitle_path),
        "views": {"step2_subtitles": [{"subtitle_id": "SUB001"}]},
    }
    servicer._get_phase2a_runtime_semantic_units = lambda output_dir, semantic_units_path="": [{"unit_id": "U001"}]
    servicer._get_transcribe_runtime_outputs = lambda output_dir, subtitle_path="", deep_copy=True: {"subtitle_path": str(subtitle_path)}
    servicer._materialize_subtitle_from_transcribe_runtime = lambda output_dir, subtitle_path: False
    servicer._get_runtime_recovery_store = lambda output_dir, task_id: _FakeRuntimeStore(None)
    servicer._count_reusable_runtime_nodes = lambda output_dir, task_id: (5, 11)

    request = SimpleNamespace(
        task_id="task-phase2a-reuse",
        output_dir=str(task_dir),
        requested_start_stage="phase2a",
        semantic_units_path="",
        requested_video_path="https://example.com/video",
        requested_subtitle_path="",
    )

    response = asyncio.run(servicer.RecoverRuntimeContext(request, context=None))

    assert response.success is True
    assert response.resolved_start_stage == "asset_extract_java"
    assert response.stage1_ready is True
    assert response.phase2a_ready is True
    assert response.semantic_units_path.endswith("intermediates/stages/phase2a/outputs/semantic_units.json")
    assert response.decision_reason == "phase2a_semantic_units_reusable"


def test_recover_runtime_context_returns_phase2b_when_asset_extract_outputs_are_ready(tmp_path):
    task_dir = tmp_path / "task"
    video_path = task_dir / "video.mp4"
    subtitle_path = task_dir / "subtitles.txt"
    task_dir.mkdir(parents=True, exist_ok=True)
    video_path.write_bytes(b"video")
    subtitle_path.write_text("demo subtitle", encoding="utf-8")
    _write_video_meta(task_dir, video_path)

    servicer = _build_servicer()
    servicer._get_stage1_runtime_outputs = lambda output_dir: {
        "video_path": str(video_path),
        "subtitle_path": str(subtitle_path),
        "views": {"step2_subtitles": [{"subtitle_id": "SUB001"}]},
    }
    servicer._get_phase2a_runtime_semantic_units = lambda output_dir, semantic_units_path="": [{"unit_id": "U001"}]
    servicer._get_transcribe_runtime_outputs = lambda output_dir, subtitle_path="", deep_copy=True: {"subtitle_path": str(subtitle_path)}
    servicer._materialize_subtitle_from_transcribe_runtime = lambda output_dir, subtitle_path: False
    servicer._get_runtime_recovery_store = lambda output_dir, task_id: _FakeRuntimeStore({"status": "SUCCESS", "checkpoint": "outputs_ready"})
    servicer._count_reusable_runtime_nodes = lambda output_dir, task_id: (8, 15)

    request = SimpleNamespace(
        task_id="task-phase2b-reuse",
        output_dir=str(task_dir),
        requested_start_stage="phase2b",
        semantic_units_path="",
        requested_video_path="https://example.com/video",
        requested_subtitle_path="",
    )

    response = asyncio.run(servicer.RecoverRuntimeContext(request, context=None))

    assert response.success is True
    assert response.resolved_start_stage == "phase2b"
    assert response.phase2a_ready is True
    assert response.decision_reason == "asset_extract_outputs_reusable"

def test_recover_runtime_context_materializes_stage1_outputs_and_download_metadata(tmp_path):
    task_dir = tmp_path / "task"
    video_path = task_dir / "video.mp4"
    subtitle_path = task_dir / "subtitles.txt"
    task_dir.mkdir(parents=True, exist_ok=True)
    video_path.write_bytes(b"video")
    subtitle_path.write_text("demo subtitle", encoding="utf-8")
    _write_video_meta(task_dir, video_path)

    servicer = _build_servicer()
    servicer._get_stage1_runtime_outputs = lambda output_dir: {
        "video_path": str(video_path),
        "subtitle_path": str(subtitle_path),
        "views": {
            "step2_subtitles": [
                {
                    "subtitle_id": "SUB001",
                    "corrected_text": "hello",
                    "start_sec": 0.0,
                    "end_sec": 1.0,
                }
            ],
            "step6_paragraphs": [
                {
                    "paragraph_id": "P001",
                    "text": "hello world",
                    "source_sentence_ids": ["S001"],
                }
            ],
            "sentence_timestamps": {
                "S001": {"start_sec": 0.0, "end_sec": 1.0}
            },
        },
    }
    servicer._get_phase2a_runtime_semantic_units = lambda output_dir, semantic_units_path="": None
    servicer._get_transcribe_runtime_outputs = lambda output_dir, subtitle_path="", deep_copy=True: {
        "subtitle_path": str(subtitle_path),
    }
    servicer._materialize_subtitle_from_transcribe_runtime = lambda output_dir, subtitle_path: False
    servicer._get_runtime_recovery_store = lambda output_dir, task_id: _FakeRuntimeStore(
        {
            "download": {
                "video_path": str(video_path),
                "duration_sec": 321.0,
                "video_title": "Recovered Title",
                "resolved_url": "https://example.com/resolved",
                "source_platform": "bilibili",
                "canonical_id": "BV1xx",
                "content_type": "video",
            }
        }
    )
    servicer._count_reusable_runtime_nodes = lambda output_dir, task_id: (4, 9)

    request = SimpleNamespace(
        task_id="task-stage1-materialize",
        output_dir=str(task_dir),
        requested_start_stage="phase2a",
        semantic_units_path="",
        requested_video_path="https://example.com/video",
        requested_subtitle_path="",
    )

    response = asyncio.run(servicer.RecoverRuntimeContext(request, context=None))

    assert response.success is True
    assert response.resolved_start_stage == "phase2a"
    assert response.stage1_ready is True
    assert response.video_duration_sec == 321.0
    assert response.video_title == "Recovered Title"
    assert response.resolved_url == "https://example.com/resolved"
    assert response.source_platform == "bilibili"
    assert response.canonical_id == "BV1xx"
    assert response.content_type == "video"
    assert Path(response.step2_json_path).exists()
    assert Path(response.step6_json_path).exists()
    assert Path(response.sentence_timestamps_path).exists()
    step2_payload = json.loads(Path(response.step2_json_path).read_text(encoding="utf-8"))
    step6_payload = json.loads(Path(response.step6_json_path).read_text(encoding="utf-8"))
    sentence_payload = json.loads(Path(response.sentence_timestamps_path).read_text(encoding="utf-8"))
    assert step2_payload["output"]["corrected_subtitles"][0]["subtitle_id"] == "SUB001"
    assert step6_payload["output"]["pure_text_script"][0]["paragraph_id"] == "P001"
    assert sentence_payload["S001"]["start_sec"] == 0.0

def test_recover_runtime_context_returns_completed_when_phase2b_outputs_are_ready(tmp_path):
    task_dir = tmp_path / "task"
    video_path = task_dir / "video.mp4"
    subtitle_path = task_dir / "subtitles.txt"
    markdown_path = task_dir / "result.md"
    json_path = task_dir / "result.json"
    task_dir.mkdir(parents=True, exist_ok=True)
    video_path.write_bytes(b"video")
    subtitle_path.write_text("demo subtitle", encoding="utf-8")
    markdown_path.write_text("# recovered", encoding="utf-8")
    json_path.write_text("{\"ok\": true}", encoding="utf-8")
    _write_video_meta(task_dir, video_path)

    servicer = _build_servicer()
    servicer._get_stage1_runtime_outputs = lambda output_dir: {
        "video_path": str(video_path),
        "subtitle_path": str(subtitle_path),
        "views": {"step2_subtitles": [{"subtitle_id": "SUB001"}]},
    }
    servicer._get_phase2a_runtime_semantic_units = lambda output_dir, semantic_units_path="": [{"unit_id": "U001"}]
    servicer._get_phase2b_runtime_outputs = lambda output_dir, deep_copy=True: {
        "markdown_path": str(markdown_path),
        "json_path": str(json_path),
        "title": "Recovered",
    }
    servicer._get_transcribe_runtime_outputs = lambda output_dir, subtitle_path="", deep_copy=True: {"subtitle_path": str(subtitle_path)}
    servicer._materialize_subtitle_from_transcribe_runtime = lambda output_dir, subtitle_path: False
    servicer._get_runtime_recovery_store = lambda output_dir, task_id: _FakeRuntimeStore({"status": "SUCCESS", "checkpoint": "outputs_ready"})
    servicer._count_reusable_runtime_nodes = lambda output_dir, task_id: (9, 16)

    request = SimpleNamespace(
        task_id="task-phase2b-complete",
        output_dir=str(task_dir),
        requested_start_stage="phase2b",
        semantic_units_path="",
        requested_video_path="https://example.com/video",
        requested_subtitle_path="",
    )

    response = asyncio.run(servicer.RecoverRuntimeContext(request, context=None))

    assert response.success is True
    assert response.resolved_start_stage == "completed"
    assert response.phase2b_ready is True
    assert response.markdown_path == str(markdown_path.resolve())
    assert response.json_path == str(json_path.resolve())
    assert response.decision_reason == "phase2b_outputs_reusable"