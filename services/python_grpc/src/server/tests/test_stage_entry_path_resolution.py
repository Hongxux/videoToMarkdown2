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


class _FakeRuntimeSession:
    def mark(self, *args, **kwargs):
        return None

    def start_soft_heartbeat_loop(self, *args, **kwargs):
        return None

    def stop_soft_heartbeat_loop(self, *args, **kwargs):
        return None


def test_resolve_stage_entry_paths_prefers_task_dir_video_meta_and_subtitles(tmp_path):
    output_dir = tmp_path / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    video_path = output_dir / "video.mp4"
    subtitle_path = output_dir / "subtitles.txt"
    video_path.write_bytes(b"video")
    subtitle_path.write_text("demo subtitle", encoding="utf-8")
    (output_dir / "video_meta.json").write_text(
        json.dumps(
            {
                "video_path": str(video_path),
                "title": "demo",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    resolved = impl._resolve_stage_entry_paths(
        requested_video_path="https://www.bilibili.com/video/BV1k8411r7E4",
        requested_output_dir=str(output_dir),
        requested_subtitle_path="",
    )

    assert resolved["output_dir"] == str(output_dir.resolve())
    assert resolved["output_dir_source"] == "request.output_dir"
    assert resolved["video_path"] == str(video_path.resolve())
    assert resolved["video_path_source"] == "output_dir/video_meta.json"
    assert resolved["subtitle_path"] == str(subtitle_path.resolve())
    assert resolved["subtitle_path_source"] == "output_dir/subtitles.txt"


def test_process_stage1_restores_task_local_video_from_output_dir(tmp_path):
    output_dir = tmp_path / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    video_path = output_dir / "video.mp4"
    subtitle_path = output_dir / "subtitles.txt"
    video_path.write_bytes(b"video")
    subtitle_path.write_text("demo subtitle", encoding="utf-8")
    (output_dir / "video_meta.json").write_text(
        json.dumps(
            {
                "video_path": str(video_path),
                "title": "demo",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    servicer = impl._VideoProcessingServicerCore.__new__(impl._VideoProcessingServicerCore)
    servicer._cache_metrics_begin = lambda *args, **kwargs: None
    servicer._increment_tasks = lambda: None
    servicer._decrement_tasks = lambda: None
    servicer._materialize_subtitle_from_transcribe_runtime = lambda output_dir, subtitle_path: False

    captured = {}

    def _create_runtime_stage_session(*, output_dir, task_id, stage, base_payload, **kwargs):
        captured["output_dir"] = output_dir
        captured["base_payload"] = dict(base_payload)
        captured["task_id"] = task_id
        captured["stage"] = stage
        return _FakeRuntimeSession()

    def _raise_after_capture(output_dir):
        captured["resume_output_dir"] = output_dir
        raise RuntimeError("stage1-path-resolution-sentinel")

    servicer._create_runtime_stage_session = _create_runtime_stage_session
    servicer._get_stage1_runtime_outputs = _raise_after_capture

    request = SimpleNamespace(
        task_id="task-restart-stage1",
        video_path="https://www.bilibili.com/video/BV1k8411r7E4",
        subtitle_path=str(subtitle_path),
        output_dir=str(output_dir),
        max_step=6,
    )

    response = asyncio.run(servicer.ProcessStage1(request, context=None))

    assert captured["output_dir"] == str(output_dir.resolve())
    assert captured["resume_output_dir"] == str(output_dir.resolve())
    assert captured["task_id"] == "task-restart-stage1"
    assert captured["stage"] == "stage1"
    assert captured["base_payload"]["video_path"] == str(video_path.resolve())
    assert captured["base_payload"]["subtitle_path"] == str(subtitle_path.resolve())
    assert "stage1-path-resolution-sentinel" in response.error_msg


def test_generate_material_requests_restores_runtime_lookup_from_output_dir(tmp_path):
    output_dir = tmp_path / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    video_path = output_dir / "video.mp4"
    video_path.write_bytes(b"video")
    (output_dir / "video_meta.json").write_text(
        json.dumps(
            {
                "video_path": str(video_path),
                "title": "demo",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    servicer = impl._VideoProcessingServicerCore.__new__(impl._VideoProcessingServicerCore)
    servicer._increment_tasks = lambda: None
    servicer._decrement_tasks = lambda: None

    captured = {}

    def _raise_after_capture(runtime_output_dir):
        captured["output_dir"] = runtime_output_dir
        raise RuntimeError("material-path-resolution-sentinel")

    servicer._get_stage1_runtime_outputs = _raise_after_capture

    request = SimpleNamespace(
        task_id="task-restart-material",
        video_path="https://www.bilibili.com/video/BV1k8411r7E4",
        output_dir=str(output_dir),
        units=[],
        video_duration=0.0,
    )

    response = asyncio.run(servicer._phase2a_generate_material_requests_impl(request, context=None))

    assert captured["output_dir"] == str(output_dir.resolve())
    assert "material-path-resolution-sentinel" in response.error_msg
