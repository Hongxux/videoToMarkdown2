"""
VL 鏁欑▼妯″紡娴嬭瘯锛?
1) 瑙ｆ瀽 step schema 鏄惁姝ｇ‘
2) 鏍￠獙澶氭楠?process 鍦?Phase2B 鍓嶇殑浜х墿瀵煎嚭锛坢ock VL + mock ffmpeg锛?
"""

from __future__ import annotations

import asyncio
import json
import logging
import shutil
import sys
from pathlib import Path
from typing import Any, Dict
from types import SimpleNamespace
import time
import cv2
import numpy as np

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor


from services.python_grpc.src.content_pipeline.phase2a.materials.vl_material_generator import VLMaterialGenerator
import services.python_grpc.src.content_pipeline.phase2a.materials.vl_material_generator as vl_material_generator_module
import services.python_grpc.src.content_pipeline.phase2a.materials.flow_ops as flow_ops_module
from services.python_grpc.src.content_pipeline.infra.runtime.vl_prefetch_utils import build_screenshot_prefetch_chunks
from services.python_grpc.src.content_pipeline.infra.llm import llm_gateway
from services.python_grpc.src.content_pipeline.phase2a.materials.vl_video_analyzer import (
    VLAnalysisResult,
    VLClipAnalysisResponse,
    VLVideoAnalyzer,
)


def _build_analyzer_config() -> Dict[str, Any]:
    return {
        "api": {
            "api_key": "test",
            "model": "test-model",
        },
        "tutorial_mode": {
            "min_step_duration_sec": 5.0,
        },
        "screenshot_optimization": {"enabled": False},
        "fallback": {"enabled": True},
    }


def _build_generator_config() -> Dict[str, Any]:
    return {
        "enabled": True,
        "use_cache": False,
        "save_cache": False,
        "merge_multistep_clip_requests": False,
        "routing": {
            "process_duration_threshold_sec": 20.0,
        },
        "tutorial_mode": {
            "enabled": True,
            "min_step_duration_sec": 5.0,
            "export_assets": True,
            "assets_root_dir": "vl_tutorial_units",
            "save_step_json": True,
            "keyframe_image_ext": "png",
        },
        "pre_vl_static_pruning": {
            "enabled": False,
        },
        "screenshot_optimization": {
            "enabled": False,
        },
        "fallback": {
            "enabled": True,
        },
        "api": {
            "api_key": "test",
            "model": "test-model",
        },
    }


def test_vl_init_supports_bearer_token_with_qianfan_defaults():
    analyzer = VLVideoAnalyzer(
        {
            "api": {
                "base_url": "https://qianfan.baidubce.com/v2/chat/completions",
                "bearer_token": "test-bearer-token",
            }
        }
    )

    assert analyzer._api_key == "test-bearer-token"
    assert analyzer.model == "ernie-4.5-turbo-vl-32k"
    assert analyzer.video_input_mode == "keyframes"


def test_vl_init_uses_dashscope_defaults_and_env_api_key(monkeypatch):
    monkeypatch.setenv("DASHSCOPE_API_KEY", "dashscope-test-key")

    analyzer = VLVideoAnalyzer(
        {
            "api": {
                "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
            }
        }
    )

    assert analyzer._api_key == "dashscope-test-key"
    assert analyzer.model == "qwen-vl-max-2025-08-13"
    assert analyzer.video_input_mode == "auto"


def test_export_keyframe_wrapper_passes_iframe_selection_config(monkeypatch, tmp_path):
    generator = VLMaterialGenerator(
        {
            "enabled": True,
            "tutorial_mode": {
                "enabled": True,
                "keyframe_iframe_search_window_sec": 0.35,
                "keyframe_iframe_search_before_sec": 0.0,
                "keyframe_iframe_search_after_sec": 0.35,
                "keyframe_select_sharpest_iframe": True,
            },
            "screenshot_optimization": {"enabled": False},
            "fallback": {"enabled": True},
        }
    )

    captured: Dict[str, Any] = {}

    async def _fake_export_keyframe_with_ffmpeg(**kwargs):
        captured.update(kwargs)
        output_path = kwargs["output_path"]
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"frame")
        return True

    monkeypatch.setattr(vl_material_generator_module, "export_keyframe_with_ffmpeg", _fake_export_keyframe_with_ffmpeg)

    output_path = Path(tmp_path) / "key.png"
    ok = asyncio.run(
        generator._export_keyframe_with_ffmpeg(
            video_path="demo.mp4",
            timestamp_sec=12.5,
            output_path=output_path,
        )
    )

    assert ok is True
    assert captured["video_path"] == "demo.mp4"
    assert float(captured["timestamp_sec"]) == 12.5
    assert captured["output_path"] == output_path
    assert float(captured["iframe_search_window_sec"]) == 0.35
    assert float(captured["iframe_search_before_sec"]) == 0.0
    assert float(captured["iframe_search_after_sec"]) == 0.35
    assert captured["select_sharpest_iframe"] is True


def test_generator_reads_draw_bbox_use_expanded_flag():
    generator = VLMaterialGenerator(
        {
            "enabled": True,
            "tutorial_mode": {
                "enabled": True,
                "keyframe_draw_bbox_use_expanded": True,
            },
            "screenshot_optimization": {"enabled": False},
            "fallback": {"enabled": True},
        }
    )
    assert generator.tutorial_keyframe_draw_bbox_use_expanded is True


def test_generator_reads_draw_on_original_frame_flag():
    generator = VLMaterialGenerator(
        {
            "enabled": True,
            "tutorial_mode": {
                "enabled": True,
                "keyframe_draw_on_original_frame": True,
            },
            "screenshot_optimization": {"enabled": False},
            "fallback": {"enabled": True},
        }
    )
    assert generator.tutorial_keyframe_draw_on_original_frame is True


def test_generator_reads_original_draw_crop_expand_and_thickness_flags():
    generator = VLMaterialGenerator(
        {
            "enabled": True,
            "tutorial_mode": {
                "enabled": True,
                "keyframe_original_draw_crop_expand_ratio": 0.45,
                "keyframe_red_box_thickness_ratio": 0.003,
            },
            "screenshot_optimization": {"enabled": False},
            "fallback": {"enabled": True},
        }
    )
    assert abs(generator.tutorial_keyframe_original_draw_crop_expand_ratio - 0.45) < 1e-6
    assert abs(generator.tutorial_keyframe_red_box_thickness_ratio - 0.003) < 1e-6


def test_generator_reads_original_draw_crop_min_expand_flag():
    generator = VLMaterialGenerator(
        {
            "enabled": True,
            "tutorial_mode": {
                "enabled": True,
                "keyframe_original_draw_crop_min_border_span_1000": 33,
            },
            "screenshot_optimization": {"enabled": False},
            "fallback": {"enabled": True},
        }
    )
    assert int(generator.tutorial_keyframe_original_draw_crop_min_border_span_1000) == 33


def test_generator_reads_skip_post_draw_processing_flag():
    generator = VLMaterialGenerator(
        {
            "enabled": True,
            "tutorial_mode": {
                "enabled": True,
                "keyframe_skip_post_draw_processing": True,
            },
            "screenshot_optimization": {"enabled": False},
            "fallback": {"enabled": True},
        }
    )
    assert generator.tutorial_keyframe_skip_post_draw_processing is True


def test_generator_disables_grid_anchor_by_default():
    generator = VLMaterialGenerator(
        {
            "enabled": True,
            "tutorial_mode": {
                "enabled": True,
            },
            "screenshot_optimization": {"enabled": False},
            "fallback": {"enabled": True},
        }
    )
    assert generator.tutorial_grid_anchor_enabled is False


def test_vl_analyze_clips_batch_preserves_order_and_exceptions(monkeypatch):
    analyzer = VLVideoAnalyzer(_build_analyzer_config())

    async def _fake_analyze_clip(
        clip_path: str,
        semantic_unit_start_sec: float,
        semantic_unit_id: str,
        extra_prompt: str | None = None,
        analysis_mode: str = "default",
    ) -> VLClipAnalysisResponse:
        _ = (clip_path, semantic_unit_start_sec, extra_prompt, analysis_mode)
        if semantic_unit_id == "U2":
            raise RuntimeError("boom-u2")
        response = VLClipAnalysisResponse(success=True)
        response.error_msg = semantic_unit_id
        return response

    monkeypatch.setattr(analyzer, "analyze_clip", _fake_analyze_clip)

    results = asyncio.run(
        analyzer.analyze_clips_batch(
            tasks=[
                {
                    "clip_path": "u1.mp4",
                    "semantic_unit_start_sec": 0.0,
                    "semantic_unit_id": "U1",
                    "analysis_mode": "default",
                },
                {
                    "clip_path": "u2.mp4",
                    "semantic_unit_start_sec": 10.0,
                    "semantic_unit_id": "U2",
                    "analysis_mode": "default",
                },
                {
                    "clip_path": "u3.mp4",
                    "semantic_unit_start_sec": 20.0,
                    "semantic_unit_id": "U3",
                    "analysis_mode": "tutorial_stepwise",
                },
            ],
            max_inflight=2,
            return_exceptions=True,
        )
    )

    assert isinstance(results[0], VLClipAnalysisResponse)
    assert isinstance(results[1], Exception)
    assert isinstance(results[2], VLClipAnalysisResponse)
    assert results[0].error_msg == "U1"
    assert results[2].error_msg == "U3"


def test_vl_analyze_clips_batch_result_callback_runs_non_blocking(monkeypatch):
    analyzer = VLVideoAnalyzer(_build_analyzer_config())

    async def _fake_analyze_clip(
        clip_path: str,
        semantic_unit_start_sec: float,
        semantic_unit_id: str,
        extra_prompt: str | None = None,
        analysis_mode: str = "default",
    ) -> VLClipAnalysisResponse:
        _ = (clip_path, semantic_unit_start_sec, extra_prompt, analysis_mode)
        response = VLClipAnalysisResponse(success=True)
        response.error_msg = semantic_unit_id
        return response

    callback_state: Dict[str, Any] = {
        "started": [],
        "finished": [],
    }

    async def _fake_result_callback(index: int, item: Any):
        _ = index
        callback_state["started"].append(str(getattr(item, "error_msg", "")))
        await asyncio.sleep(0.1)
        callback_state["finished"].append(str(getattr(item, "error_msg", "")))

    monkeypatch.setattr(analyzer, "analyze_clip", _fake_analyze_clip)

    start_ts = time.perf_counter()
    results = asyncio.run(
        analyzer.analyze_clips_batch(
            tasks=[
                {
                    "clip_path": "u1.mp4",
                    "semantic_unit_start_sec": 0.0,
                    "semantic_unit_id": "U1",
                    "analysis_mode": "default",
                },
                {
                    "clip_path": "u2.mp4",
                    "semantic_unit_start_sec": 10.0,
                    "semantic_unit_id": "U2",
                    "analysis_mode": "default",
                },
                {
                    "clip_path": "u3.mp4",
                    "semantic_unit_start_sec": 20.0,
                    "semantic_unit_id": "U3",
                    "analysis_mode": "default",
                },
            ],
            max_inflight=3,
            return_exceptions=True,
            result_callback=_fake_result_callback,
        )
    )
    elapsed_sec = time.perf_counter() - start_ts

    assert [str(getattr(item, "error_msg", "")) for item in results] == ["U1", "U2", "U3"]
    assert sorted(callback_state["started"]) == ["U1", "U2", "U3"]
    assert sorted(callback_state["finished"]) == ["U1", "U2", "U3"]
    # 若 callback 串行阻塞，总耗时会接近 0.3s；并发调度下应明显低于该值。
    assert elapsed_sec < 0.28


def test_build_messages_skip_dashscope_upload_for_qianfan(monkeypatch, tmp_path: Path):
    clip = tmp_path / "demo.mp4"
    clip.write_bytes(b"fake-video")

    analyzer = VLVideoAnalyzer(
        {
            "api": {
                "base_url": "https://qianfan.baidubce.com/v2/chat/completions",
                "bearer_token": "test-bearer-token",
                "video_input_mode": "dashscope_upload",
            }
        }
    )

    call_counter = {"upload_calls": 0}

    async def _fake_upload(_video_path: str):
        call_counter["upload_calls"] += 1
        return "https://example.com/fake.mp4"

    async def _fake_extract(_video_path: str, max_frames: int = 6):
        _ = max_frames
        return [{"timestamp_sec": 0.5, "data_uri": "data:image/jpeg;base64,AA=="}]

    monkeypatch.setattr(analyzer, "_try_get_dashscope_temp_url", _fake_upload)
    monkeypatch.setattr(analyzer, "_extract_keyframes", _fake_extract)

    messages = asyncio.run(analyzer._build_messages(str(clip), analysis_mode="default"))

    assert call_counter["upload_calls"] == 0
    assert len(messages) == 2
    assert messages[0]["role"] == "system"
    assert messages[1]["role"] == "user"
    assert any(item.get("type") == "image_url" for item in messages[1]["content"])


def test_build_messages_dashscope_upload_requires_temp_url_before_vl_analysis(monkeypatch, tmp_path: Path):
    clip = tmp_path / "demo.mp4"
    clip.write_bytes(b"fake-video")

    analyzer = VLVideoAnalyzer(
        {
            "api": {
                "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
                "api_key": "test-key",
                "video_input_mode": "dashscope_upload",
            }
        }
    )

    async def _fake_upload(_video_path: str, raise_on_failure: bool = False):
        _ = raise_on_failure
        return None

    async def _fail_if_keyframes_called(_video_path: str, max_frames: int = 6):
        _ = max_frames
        raise AssertionError("keyframes fallback should not run in dashscope_upload mode")

    monkeypatch.setattr(analyzer, "_try_get_dashscope_temp_url", _fake_upload)
    monkeypatch.setattr(analyzer, "_extract_keyframes", _fail_if_keyframes_called)

    try:
        asyncio.run(analyzer._build_messages(str(clip), analysis_mode="default"))
        raise AssertionError("expected dashscope_upload mode to require temp_url")
    except RuntimeError as exc:
        assert "temp_url" in str(exc)


def test_prepare_video_for_dashscope_upload_compresses_without_duration_threshold(monkeypatch):
    analyzer = VLVideoAnalyzer(_build_analyzer_config())
    analyzer.long_video_upload_compress_enabled = True

    captured: Dict[str, Any] = {}

    async def _fake_compress(video_path: str) -> str:
        captured["video_path"] = video_path
        return "compressed.mp4"

    monkeypatch.setattr(analyzer, "_compress_video_for_dashscope_upload", _fake_compress)

    prepared = asyncio.run(analyzer._prepare_video_for_dashscope_upload("demo.mp4"))

    assert prepared == "compressed.mp4"
    assert captured["video_path"] == "demo.mp4"


def test_prepare_video_for_dashscope_upload_skips_compression_for_stream_unit_subset(monkeypatch):
    analyzer = VLVideoAnalyzer(_build_analyzer_config())
    analyzer.long_video_upload_compress_enabled = True

    async def _fail_if_called(video_path: str) -> str:
        raise AssertionError(f"unexpected recompress: {video_path}")

    monkeypatch.setattr(analyzer, "_compress_video_for_dashscope_upload", _fail_if_called)

    video_path = (
        r"var\storage\storage\demo123\semantic_unit_clips_vl\_stream_units\SU001"
        r"\001_SU001_demo_0.00-10.00.mp4"
    )
    prepared = asyncio.run(analyzer._prepare_video_for_dashscope_upload(video_path))

    assert prepared == video_path


def test_prepare_video_for_dashscope_upload_defaults_to_original_video():
    analyzer = VLVideoAnalyzer(_build_analyzer_config())
    analyzer.long_video_upload_compress_enabled = False

    prepared = asyncio.run(analyzer._prepare_video_for_dashscope_upload("origin.mp4"))

    assert prepared == "origin.mp4"


def test_compress_video_for_dashscope_upload_ignores_larger_cached_file(tmp_path):
    source = Path(tmp_path) / "source.mp4"
    cached = Path(tmp_path) / "_vl_upload_cache" / "source_cached_720p.mp4"
    source.write_bytes(b"a" * 100)
    cached.parent.mkdir(parents=True, exist_ok=True)
    cached.write_bytes(b"b" * 200)

    analyzer = VLVideoAnalyzer(_build_analyzer_config())
    analyzer.long_video_upload_compress_enabled = True
    analyzer._build_long_video_upload_output_path = lambda _video_path: cached

    compressed = asyncio.run(analyzer._compress_video_for_dashscope_upload(str(source)))

    assert compressed == str(source)
    assert not cached.exists()


def test_compress_video_for_dashscope_upload_uses_crf_fps_and_keeps_audio_by_default(monkeypatch, tmp_path):
    source = Path(tmp_path) / "source.mp4"
    output = Path(tmp_path) / "_vl_upload_cache" / "source_out_1080p.mp4"
    source.write_bytes(b"a" * 1024)

    analyzer = VLVideoAnalyzer(_build_analyzer_config())
    analyzer.long_video_upload_compress_enabled = True
    analyzer.long_video_upload_target_height = 1080
    analyzer.long_video_upload_crf = 28
    analyzer.long_video_upload_preset = "fast"
    analyzer.long_video_upload_target_fps = 15.0
    analyzer.long_video_upload_timeout_sec = 30
    assert analyzer.long_video_upload_drop_audio is False

    captured: Dict[str, Any] = {}

    def _fake_run(command, capture_output=True, text=True, timeout=0):
        _ = (capture_output, text)
        captured["command"] = list(command)
        captured["timeout"] = timeout
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(b"x" * 100)
        return SimpleNamespace(returncode=0, stderr="")

    monkeypatch.setattr(
        "services.python_grpc.src.content_pipeline.phase2a.materials.vl_video_analyzer.resolve_ffmpeg_bin",
        lambda: "ffmpeg",
    )
    monkeypatch.setattr(
        "services.python_grpc.src.content_pipeline.phase2a.materials.vl_video_analyzer.subprocess.run",
        _fake_run,
    )
    monkeypatch.setattr(analyzer, "_build_long_video_upload_output_path", lambda _video_path: output)

    compressed = asyncio.run(analyzer._compress_video_for_dashscope_upload(str(source)))

    command = captured["command"]
    assert compressed == str(output)
    assert "-c:v" in command and "libx264" in command
    assert "-crf" in command and command[command.index("-crf") + 1] == "28"
    assert "-preset" in command and command[command.index("-preset") + 1] == "fast"
    assert "-r" in command and command[command.index("-r") + 1] == "15"
    assert "-an" not in command
    assert "-map" in command and "0:a?" in command
    assert "-c:a" in command and command[command.index("-c:a") + 1] == "aac"
    assert "-b:a" in command and command[command.index("-b:a") + 1] == "128k"
    assert "-vf" in command
    assert "1080" in command[command.index("-vf") + 1]
    assert "-b:v" not in command


def test_compress_video_for_dashscope_upload_uses_crf_fps_and_drop_audio(monkeypatch, tmp_path):
    source = Path(tmp_path) / "source.mp4"
    output = Path(tmp_path) / "_vl_upload_cache" / "source_out_1080p.mp4"
    source.write_bytes(b"a" * 1024)

    analyzer = VLVideoAnalyzer(_build_analyzer_config())
    analyzer.long_video_upload_compress_enabled = True
    analyzer.long_video_upload_target_height = 1080
    analyzer.long_video_upload_crf = 28
    analyzer.long_video_upload_preset = "fast"
    analyzer.long_video_upload_target_fps = 15.0
    analyzer.long_video_upload_drop_audio = True
    analyzer.long_video_upload_timeout_sec = 30

    captured: Dict[str, Any] = {}

    def _fake_run(command, capture_output=True, text=True, timeout=0):
        _ = (capture_output, text)
        captured["command"] = list(command)
        captured["timeout"] = timeout
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(b"x" * 100)
        return SimpleNamespace(returncode=0, stderr="")

    monkeypatch.setattr(
        "services.python_grpc.src.content_pipeline.phase2a.materials.vl_video_analyzer.resolve_ffmpeg_bin",
        lambda: "ffmpeg",
    )
    monkeypatch.setattr(
        "services.python_grpc.src.content_pipeline.phase2a.materials.vl_video_analyzer.subprocess.run",
        _fake_run,
    )
    monkeypatch.setattr(analyzer, "_build_long_video_upload_output_path", lambda _video_path: output)

    compressed = asyncio.run(analyzer._compress_video_for_dashscope_upload(str(source)))

    command = captured["command"]
    assert compressed == str(output)
    assert "-c:v" in command and "libx264" in command
    assert "-crf" in command and command[command.index("-crf") + 1] == "28"
    assert "-preset" in command and command[command.index("-preset") + 1] == "fast"
    assert "-r" in command and command[command.index("-r") + 1] == "15"
    assert "-an" in command
    assert "-vf" in command
    assert "1080" in command[command.index("-vf") + 1]
    assert "-b:v" not in command


def test_try_get_dashscope_temp_url_uses_chunk_size_and_video_duration_timeout(monkeypatch, caplog):
    analyzer = VLVideoAnalyzer(_build_analyzer_config())
    analyzer._api_key = "test-key"
    analyzer.base_url = "https://dashscope.aliyuncs.com/compatible-mode/v1"
    analyzer.dashscope_upload_chunk_size_bytes = 2 * 1024 * 1024
    analyzer.dashscope_upload_timeout_by_video_duration = True
    analyzer.dashscope_upload_timeout_min_sec = 1.0

    captured: Dict[str, Any] = {}
    caplog.set_level(logging.INFO, logger="services.python_grpc.src.content_pipeline.phase2a.materials.vl_video_analyzer")

    async def _fake_prepare(_video_path: str) -> str:
        return "prepared.mp4"

    monkeypatch.setattr(analyzer, "_prepare_video_for_dashscope_upload", _fake_prepare)
    monkeypatch.setattr(analyzer, "_resolve_video_duration_sec", lambda _video_path: 913.0)

    class _FakeUploadResponse:
        status_code = 200
        output = {"url": "https://example.com/video.mp4"}
        request_id = "req-001"

    class _FakeFiles:
        @staticmethod
        def upload(**kwargs):
            captured.update(kwargs)
            return _FakeUploadResponse()

    class _FakeDashScope:
        api_key = ""
        Files = _FakeFiles

    monkeypatch.setitem(sys.modules, "dashscope", _FakeDashScope)

    temp_url = asyncio.run(analyzer._try_get_dashscope_temp_url("demo.mp4", raise_on_failure=True))

    assert temp_url == "https://example.com/video.mp4"
    assert captured["purpose"] == "file-extract"
    assert int(captured["chunk_size"]) == 2 * 1024 * 1024
    assert float(captured["timeout"]) == 913.0
    assert captured["headers"]["Authorization"] == "Bearer test-key"
    assert "tmp_url=https://example.com/video.mp4" in caplog.text


def test_try_get_dashscope_temp_url_normalizes_http_oss_to_https(monkeypatch):
    analyzer = VLVideoAnalyzer(_build_analyzer_config())
    analyzer._api_key = "test-key"
    analyzer.base_url = "https://dashscope.aliyuncs.com/compatible-mode/v1"

    async def _fake_prepare(_video_path: str) -> str:
        return "prepared.mp4"

    monkeypatch.setattr(analyzer, "_prepare_video_for_dashscope_upload", _fake_prepare)
    monkeypatch.setattr(analyzer, "_resolve_video_duration_sec", lambda _video_path: 913.0)

    class _FakeUploadResponse:
        status_code = 200
        output = {
            "url": "http://dashscope-file-mgr.oss-cn-beijing.aliyuncs.com/demo.mp4?signature=abc"
        }
        request_id = "req-http-url"

    class _FakeFiles:
        @staticmethod
        def upload(**kwargs):
            _ = kwargs
            return _FakeUploadResponse()

    class _FakeDashScope:
        api_key = ""
        Files = _FakeFiles

    monkeypatch.setitem(sys.modules, "dashscope", _FakeDashScope)

    temp_url = asyncio.run(analyzer._try_get_dashscope_temp_url("demo.mp4", raise_on_failure=True))

    assert temp_url.startswith("https://dashscope-file-mgr.oss-")


def test_try_get_dashscope_temp_url_retries_with_exponential_backoff(monkeypatch):
    analyzer = VLVideoAnalyzer(_build_analyzer_config())
    analyzer._api_key = "test-key"
    analyzer.base_url = "https://dashscope.aliyuncs.com/compatible-mode/v1"
    analyzer.dashscope_upload_retry_max_attempts = 3
    analyzer.dashscope_upload_retry_initial_backoff_sec = 2.0
    analyzer.dashscope_upload_retry_multiplier = 2.0
    analyzer.dashscope_upload_retry_max_backoff_sec = 10.0
    analyzer.dashscope_upload_retry_jitter_sec = 0.0
    analyzer.dashscope_upload_timeout_by_video_duration = True
    analyzer.dashscope_upload_timeout_min_sec = 1.0

    captured: Dict[str, Any] = {"upload_calls": 0, "sleep_values": []}

    async def _fake_prepare(_video_path: str) -> str:
        return "prepared.mp4"

    async def _fake_sleep(seconds: float):
        captured["sleep_values"].append(float(seconds))
        return None

    monkeypatch.setattr(analyzer, "_prepare_video_for_dashscope_upload", _fake_prepare)
    monkeypatch.setattr(analyzer, "_resolve_video_duration_sec", lambda _video_path: 913.0)
    monkeypatch.setattr(
        "services.python_grpc.src.content_pipeline.phase2a.materials.vl_video_analyzer.asyncio.sleep",
        _fake_sleep,
    )

    class _FakeUploadResponse:
        status_code = 200
        output = {"url": "https://example.com/video.mp4"}
        request_id = "req-retry-ok"

    class _FakeFiles:
        @staticmethod
        def upload(**kwargs):
            _ = kwargs
            captured["upload_calls"] += 1
            if captured["upload_calls"] < 3:
                raise RuntimeError("simulated upload timeout")
            return _FakeUploadResponse()

    class _FakeDashScope:
        api_key = ""
        Files = _FakeFiles

    monkeypatch.setitem(sys.modules, "dashscope", _FakeDashScope)

    temp_url = asyncio.run(analyzer._try_get_dashscope_temp_url("demo.mp4", raise_on_failure=True))

    assert temp_url == "https://example.com/video.mp4"
    assert captured["upload_calls"] == 3
    assert captured["sleep_values"] == [2.0, 4.0]


def test_call_vl_api_prefers_dashscope_offline_task_when_enabled(monkeypatch):
    analyzer = VLVideoAnalyzer(_build_analyzer_config())
    analyzer.vl_offline_task_enabled = True
    analyzer.base_url = "https://dashscope.aliyuncs.com/compatible-mode/v1"

    async def _fake_build_messages(video_path: str, extra_prompt=None, analysis_mode="default", **kwargs):
        _ = (video_path, extra_prompt, analysis_mode, kwargs)
        return [{"role": "system", "content": "sys"}]

    async def _fake_offline_call(*, messages):
        assert messages == [{"role": "system", "content": "sys"}]
        return (
            '[{"id":1,"knowledge_type":"process","clip_start_sec":0,"clip_end_sec":5}]',
            "stop",
            {"prompt_tokens": 11, "completion_tokens": 7, "total_tokens": 18},
            {"task_id": "task_123", "task_status": "SUCCEEDED", "poll_count": 2},
        )

    def _fake_parse(content: str, finish_reason=None, analysis_mode="default"):
        _ = (content, finish_reason, analysis_mode)
        return [VLAnalysisResult(id=1, knowledge_type="process", clip_start_sec=0.0, clip_end_sec=5.0)], []

    async def _should_not_call_sync(**kwargs):
        raise AssertionError("sync vl_chat_completion should not be called when offline task is enabled")

    monkeypatch.setattr(analyzer, "_build_messages", _fake_build_messages)
    monkeypatch.setattr(analyzer, "_call_vl_api_with_dashscope_offline_task", _fake_offline_call)
    monkeypatch.setattr(analyzer, "_parse_response_with_payload", _fake_parse)
    monkeypatch.setattr(llm_gateway, "vl_chat_completion", _should_not_call_sync)

    parsed_results, token_usage, raw_json, interactions = asyncio.run(
        analyzer._call_vl_api("demo_0.00-10.00.mp4", analysis_mode="default")
    )

    assert len(parsed_results) == 1
    assert token_usage["total_tokens"] == 18
    assert raw_json == []
    assert interactions[0]["request"]["offline_task_enabled"] is True
    assert interactions[0]["request"]["offline_task_meta"]["task_id"] == "task_123"
    assert interactions[0]["response"]["model"] == analyzer.model
    assert interactions[0]["response"]["cache_hit"] is False


def test_call_vl_api_sync_path_passes_timeout_and_hedge(monkeypatch):
    analyzer = VLVideoAnalyzer(_build_analyzer_config())
    analyzer.vl_offline_task_enabled = False

    async def _fake_build_messages(video_path: str, extra_prompt=None, analysis_mode="default", **kwargs):
        _ = (video_path, extra_prompt, analysis_mode, kwargs)
        return [{"role": "system", "content": "sys"}]

    def _fake_parse(content: str, finish_reason=None, analysis_mode="default"):
        _ = (content, finish_reason, analysis_mode)
        return [VLAnalysisResult(id=1, knowledge_type="process", clip_start_sec=0.0, clip_end_sec=5.0)], []

    captured: Dict[str, Any] = {}

    async def _fake_vl_chat_completion(**kwargs):
        captured["timeout"] = kwargs.get("timeout")
        captured["hedge_delay_ms"] = kwargs.get("hedge_delay_ms")
        return llm_gateway.VLChatResult(
            content='[{"id":1,"knowledge_type":"process","clip_start_sec":0,"clip_end_sec":5}]',
            finish_reason="stop",
            usage={"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3},
            model="test-model",
        )

    monkeypatch.setattr(analyzer, "_build_messages", _fake_build_messages)
    monkeypatch.setattr(analyzer, "_parse_response_with_payload", _fake_parse)
    monkeypatch.setattr(llm_gateway, "vl_chat_completion", _fake_vl_chat_completion)

    asyncio.run(analyzer._call_vl_api("demo_100.00-220.00.mp4", analysis_mode="default"))

    assert float(captured["timeout"]) == 60.0
    assert int(captured["hedge_delay_ms"]) == 60000


def test_call_vl_api_retries_with_configured_backoff(monkeypatch):
    analyzer = VLVideoAnalyzer(_build_analyzer_config())
    analyzer.vl_offline_task_enabled = False
    analyzer.max_retries = 4
    analyzer.vl_retry_initial_backoff_sec = 2.0
    analyzer.vl_retry_multiplier = 2.0
    analyzer.vl_retry_max_backoff_sec = 16.0

    captured: Dict[str, Any] = {"sleep_values": [], "call_count": 0}

    async def _fake_build_messages(video_path: str, extra_prompt=None, analysis_mode="default", **kwargs):
        _ = (video_path, extra_prompt, analysis_mode, kwargs)
        return [{"role": "system", "content": "sys"}]

    def _fake_parse(content: str, finish_reason=None, analysis_mode="default"):
        _ = (content, finish_reason, analysis_mode)
        return [VLAnalysisResult(id=1, knowledge_type="process", clip_start_sec=0.0, clip_end_sec=5.0)], []

    async def _fake_sleep(seconds: float):
        captured["sleep_values"].append(float(seconds))
        return None

    async def _fake_vl_chat_completion(**kwargs):
        _ = kwargs
        captured["call_count"] += 1
        if captured["call_count"] < 5:
            raise RuntimeError(f"transient failure {captured['call_count']}")
        return llm_gateway.VLChatResult(
            content='[{"id":1,"knowledge_type":"process","clip_start_sec":0,"clip_end_sec":5}]',
            finish_reason="stop",
            usage={"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3},
            model="test-model",
        )

    monkeypatch.setattr(analyzer, "_build_messages", _fake_build_messages)
    monkeypatch.setattr(analyzer, "_parse_response_with_payload", _fake_parse)
    monkeypatch.setattr(llm_gateway, "vl_chat_completion", _fake_vl_chat_completion)
    monkeypatch.setattr(
        "services.python_grpc.src.content_pipeline.phase2a.materials.vl_video_analyzer.asyncio.sleep",
        _fake_sleep,
    )

    parsed_results, token_usage, raw_json, interactions = asyncio.run(
        analyzer._call_vl_api("demo_0.00-10.00.mp4", analysis_mode="default")
    )

    assert len(parsed_results) == 1
    assert token_usage["total_tokens"] == 3
    assert raw_json == []
    assert interactions[-1]["success"] is True
    assert captured["call_count"] == 5
    assert captured["sleep_values"] == [2.0, 4.0, 8.0, 16.0]


def test_dashscope_offline_task_uses_openai_batch_with_5s_poll(monkeypatch):
    analyzer = VLVideoAnalyzer(_build_analyzer_config())
    analyzer._api_key = "test-key"
    analyzer.model = "test-model"
    analyzer.vl_offline_poll_interval_sec = 5.0

    captured: Dict[str, Any] = {
        "files_create_calls": 0,
        "batches_create_calls": 0,
        "batches_retrieve_calls": 0,
        "files_content_calls": 0,
        "sleep_calls": 0,
        "sleep_seconds": [],
    }

    class _FakeFileContent:
        def __init__(self, text: str):
            self.text = text

    class _FakeFiles:
        async def create(self, *, file, purpose: str, **kwargs):
            _ = kwargs
            captured["files_create_calls"] += 1
            captured["purpose"] = purpose
            captured["uploaded_file"] = file

            payload_bytes = b""
            if isinstance(file, tuple) and len(file) >= 2:
                payload_bytes = file[1] if isinstance(file[1], (bytes, bytearray)) else b""
            elif hasattr(file, "read") and callable(getattr(file, "read")):
                payload = file.read()
                if isinstance(payload, str):
                    payload_bytes = payload.encode("utf-8")
                elif isinstance(payload, (bytes, bytearray)):
                    payload_bytes = bytes(payload)
            payload_text = payload_bytes.decode("utf-8", errors="replace")
            row = json.loads(payload_text.strip().splitlines()[0])
            captured["custom_id"] = row["custom_id"]
            captured["batch_body"] = row["body"]

            return SimpleNamespace(id="file-input-001")

        async def content(self, file_id: str, **kwargs):
            _ = kwargs
            captured["files_content_calls"] += 1
            captured["output_file_id"] = file_id
            output_row = {
                "custom_id": captured.get("custom_id", ""),
                "response": {
                    "status_code": 200,
                    "body": {
                        "choices": [
                            {
                                "message": {
                                    "content": '[{"id":1,"knowledge_type":"process","clip_start_sec":0,"clip_end_sec":1}]'
                                },
                                "finish_reason": "stop",
                            }
                        ],
                        "usage": {"prompt_tokens": 2, "completion_tokens": 3, "total_tokens": 5},
                    },
                },
            }
            return _FakeFileContent(json.dumps(output_row, ensure_ascii=False) + "\n")

    class _FakeBatches:
        async def create(self, *, input_file_id: str, endpoint: str, completion_window: str, **kwargs):
            _ = kwargs
            captured["batches_create_calls"] += 1
            captured["input_file_id"] = input_file_id
            captured["endpoint"] = endpoint
            captured["completion_window"] = completion_window
            return SimpleNamespace(id="batch-001", status="in_progress")

        async def retrieve(self, batch_id: str, **kwargs):
            _ = kwargs
            captured["batches_retrieve_calls"] += 1
            captured["task_id"] = batch_id
            if captured["batches_retrieve_calls"] == 1:
                return SimpleNamespace(id=batch_id, status="in_progress")
            return SimpleNamespace(id=batch_id, status="completed", output_file_id="file-output-001")

    class _FakeClient:
        def __init__(self):
            self.files = _FakeFiles()
            self.batches = _FakeBatches()

    analyzer.client = _FakeClient()

    async def _fake_sleep(_seconds: float):
        captured["sleep_calls"] += 1
        captured["sleep_seconds"].append(float(_seconds))
        return None

    monkeypatch.setattr(
        "services.python_grpc.src.content_pipeline.phase2a.materials.vl_video_analyzer.asyncio.sleep",
        _fake_sleep,
    )

    content, finish_reason, usage, meta = asyncio.run(
        analyzer._call_vl_api_with_dashscope_offline_task(
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "video_url", "video_url": {"url": "https://example.com/video.mp4"}},
                        {"type": "text", "text": "analyze"},
                    ],
                }
            ]
        )
    )

    assert captured["files_create_calls"] == 1
    assert captured["batches_create_calls"] == 1
    assert captured["batches_retrieve_calls"] == 2
    assert captured["files_content_calls"] == 1
    assert captured["purpose"] == "batch"
    assert captured["endpoint"] == "/v1/chat/completions"
    assert captured["completion_window"] == "24h"
    assert captured["task_id"] == "batch-001"
    assert captured["sleep_calls"] == 1
    assert captured["sleep_seconds"] == [5.0]
    assert captured["batch_body"]["model"] == analyzer.model
    assert captured["batch_body"]["max_tokens"] == analyzer.max_tokens
    assert content.startswith("[{")
    assert finish_reason == "stop"
    assert usage["total_tokens"] == 5
    assert meta["task_id"] == "batch-001"
    assert meta["batch_id"] == "batch-001"
    assert meta["input_file_id"] == "file-input-001"
    assert meta["output_file_id"] == "file-output-001"
    assert meta["task_status"] == "COMPLETED"
    assert int(meta["poll_count"]) == 2
    assert int(meta["body_bytes"]) > 0
    assert int(meta["jsonl_bytes"]) >= int(meta["body_bytes"])
    assert meta["message_transport"] == "temp_url"
    assert meta["message_transport_meta"]["temp_url_count"] == 1


def test_build_messages_uses_concrete_mode_prompts(monkeypatch):
    sandbox_dir = Path("tmp_vl_concrete_messages_test")
    if sandbox_dir.exists():
        shutil.rmtree(sandbox_dir, ignore_errors=True)
    sandbox_dir.mkdir(parents=True, exist_ok=True)
    clip = sandbox_dir / "demo.mp4"
    clip.write_bytes(b"fake-video")
    analyzer = VLVideoAnalyzer(_build_analyzer_config())

    async def _fake_extract(_video_path: str, max_frames: int = 6):
        _ = max_frames
        return [{"timestamp_sec": 0.5, "data_uri": "data:image/jpeg;base64,AA=="}]

    monkeypatch.setattr(analyzer, "_extract_keyframes", _fake_extract)
    try:
        messages = asyncio.run(analyzer._build_messages(str(clip), analysis_mode="concrete"))
    finally:
        shutil.rmtree(sandbox_dir, ignore_errors=True)
    assert analyzer._normalize_analysis_mode("concrete_focus") == "concrete"
    assert len(messages) == 2
    assert messages[0]["role"] == "system"
    system_text = str(messages[0]["content"] or "")
    assert "Analyze concrete visual segments and output JSON only." in system_text


def test_parse_response_with_payload_concrete_schema():
    analyzer = VLVideoAnalyzer(_build_analyzer_config())
    payload = [
        {
            "segment_id": 1,
            "segment_description": "系统架构总览",
            "main_content": "> **核心观点**：这是一个示例 [KEYFRAME_1]",
            "clip_start_sec": 0.0,
            "clip_end_sec": 9.5,
            "instructional_keyframes": [
                {
                    "keyframe_id": "keyframe-1",
                    "timestamp_sec": 4.2,
                    "frame_reason": "架构图完整出现",
                }
            ],
        }
    ]
    results, normalized = analyzer._parse_response_with_payload(
        json.dumps(payload, ensure_ascii=False),
        analysis_mode="concrete",
    )
    assert len(results) == 1
    assert results[0].analysis_mode == "concrete"
    assert results[0].knowledge_type == "concrete"
    assert results[0].step_id == 1
    assert results[0].step_description == "系统架构总览"
    assert results[0].main_operation == ["> **核心观点**：这是一个示例 [KEYFRAME_1]"]
    assert len(results[0].instructional_keyframes) == 1
    assert results[0].instructional_keyframes[0]["keyframe_id"] == "KEYFRAME_1"
    assert normalized[0]["segment_id"] == 1
    assert normalized[0]["main_content"] == "> **核心观点**：这是一个示例 [KEYFRAME_1]"
    assert normalized[0]["instructional_keyframes"][0]["keyframe_id"] == "KEYFRAME_1"


def test_postprocess_unit_main_content_updates_raw_json(monkeypatch):
    generator = VLMaterialGenerator(_build_generator_config())
    analysis_result = SimpleNamespace(
        analysis_mode="concrete",
        raw_response_json=[
            {
                "segment_id": 1,
                "segment_description": "示例片段",
                "main_content": "原始内容 [KEYFRAME_1]",
                "instructional_keyframes": [{"timestamp_sec": 1.0, "frame_reason": "a"}],
            }
        ],
    )

    monkeypatch.setattr(
        generator,
        "_build_vl_arg_subtitle_context",
        lambda **kwargs: "字幕上下文",
    )

    async def _fake_deepseek_complete_text(**kwargs):
        _ = kwargs
        return (
            json.dumps(
                [
                    {
                        "segment_id": 1,
                        "main_content": "补全后内容 [KEYFRAME_1]",
                    }
                ],
                ensure_ascii=False,
            ),
            {},
            [],
        )

    monkeypatch.setattr(
        vl_material_generator_module.llm_gateway,
        "deepseek_complete_text",
        _fake_deepseek_complete_text,
    )

    asyncio.run(
        generator._postprocess_unit_main_content(
            analysis_result=analysis_result,
            semantic_unit={"unit_id": "SU_CONCRETE", "start_sec": 0.0, "end_sec": 10.0},
            output_dir=".",
        )
    )
    assert analysis_result.raw_response_json[0]["main_content"] == "补全后内容 [KEYFRAME_1]"


def test_tutorial_schema_parse_and_normalize():
    analyzer = VLVideoAnalyzer(_build_analyzer_config())
    payload = [
        {
            "step_id": 1,
            "step_description": "Open settings",
            "main_action": "Open settings panel",
            "main_operation": "1. Click the settings icon\n2. Enter network settings\n[KEYFRAME_1]\n[CLIP_1]",
            "precautions": ["Do not edit unrelated options"],
            "step_summary": "settings page opened",
            "operation_guidance": ["click settings", "open network settings"],
            "clip_start_sec": 0.0,
            "clip_end_sec": 7.0,
            "instructional_keyframes": [
                {
                    "timestamp_sec": 6.2,
                    "frame_reason": "settings page visible",
                    "target_ui_type": "menu_item",
                    "target_text": "Settings",
                    "target_relative_position": "top-left in toolbar",
                    "bbox": [120, 80, 760, 920],
                }
            ],
            "instructional_clips": [
                {
                    "clip_id": 1,
                    "start_sec": 5.0,
                    "end_sec": 6.5,
                    "clip_reason": "watch the network indicator turn green",
                }
            ],
        },
        {
            "step_id": 2,
            "step_description": "Change port",
            "main_action": "Change port value",
            "main_operation": ["Update port input", "Save settings"],
            "precautions": [],
            "clip_start_sec": 7.0,
            "clip_end_sec": 13.0,
            "instructional_keyframe_timestamp": [12.2],
        },
    ]
    text = json.dumps(payload, ensure_ascii=False)

    results, normalized = analyzer._parse_response_with_payload(
        text,
        analysis_mode="tutorial_stepwise",
    )

    assert len(results) == 2
    assert results[0].step_id == 1
    assert results[0].step_description == "Open settings"
    assert results[0].main_action == "Open settings panel"
    assert results[0].main_operation == ["1. Click the settings icon\n2. Enter network settings\n[KEYFRAME_1]\n[CLIP_1]"]
    assert results[0].instructional_keyframes == [
        {
            "timestamp_sec": 6.2,
            "frame_reason": "settings page visible",
            "keyframe_id": "KEYFRAME_1",
            "bbox": [120, 80, 760, 920],
        }
    ]
    assert results[0].instructional_clips == [
        {
            "clip_id": "CLIP_1",
            "start_sec": 5.0,
            "end_sec": 6.5,
            "clip_reason": "watch the network indicator turn green",
        }
    ]
    assert results[0].precautions == ["Do not edit unrelated options"]
    assert results[0].step_summary == "settings page opened"
    assert results[0].operation_guidance == ["click settings", "open network settings"]
    assert results[0].step_type == "MAIN_FLOW"
    assert results[1].precautions == []
    assert results[0].knowledge_type == "process"
    assert results[1].suggested_screenshoot_timestamps == [12.2]
    assert results[1].step_type == "MAIN_FLOW"
    assert normalized[0]["instructional_keyframe_timestamp"] == [6.2]
    assert normalized[0]["instructional_keyframes"] == [
        {
            "timestamp_sec": 6.2,
            "frame_reason": "settings page visible",
            "keyframe_id": "KEYFRAME_1",
            "bbox": [120, 80, 760, 920],
        }
    ]
    assert normalized[0]["instructional_clips"] == [
        {
            "clip_id": "CLIP_1",
            "start_sec": 5.0,
            "end_sec": 6.5,
            "clip_reason": "watch the network indicator turn green",
        }
    ]
    assert "target_ui_type" not in results[0].instructional_keyframes[0]
    assert "target_text" not in results[0].instructional_keyframes[0]
    assert "target_relative_position" not in results[0].instructional_keyframes[0]
    assert "target_ui_type" not in normalized[0]["instructional_keyframes"][0]
    assert "target_text" not in normalized[0]["instructional_keyframes"][0]
    assert "target_relative_position" not in normalized[0]["instructional_keyframes"][0]
    assert normalized[0]["step_type"] == "MAIN_FLOW"
    assert set(normalized[0].keys()) == {
        "step_id",
        "step_description",
        "step_type",
        "main_action",
        "main_operation",
        "instructional_keyframes",
        "instructional_clips",
        "precautions",
        "step_summary",
        "operation_guidance",
        "clip_start_sec",
        "clip_end_sec",
        "instructional_keyframe_timestamp",
    }

def test_tutorial_schema_parse_handles_unescaped_newlines_in_main_operation():
    analyzer = VLVideoAnalyzer(_build_analyzer_config())
    content = """
[
  {
    "step_id": 1,
    "step_description": "Open settings",
    "step_type": "MAIN_FLOW",
    "main_operation": "Line 1
Line 2
[KEYFRAME_1]",
    "clip_start_sec": 0.0,
    "clip_end_sec": 7.0,
    "instructional_keyframe_timestamp": [6.2]
  }
]
""".strip()

    results, normalized = analyzer._parse_response_with_payload(
        content,
        analysis_mode="tutorial_stepwise",
    )

    assert len(results) == 1
    assert results[0].step_id == 1
    assert results[0].main_operation == ["Line 1\nLine 2\n[KEYFRAME_1]"]
    assert normalized[0]["main_operation"] == ["Line 1\nLine 2\n[KEYFRAME_1]"]


def test_tutorial_schema_parse_salvages_truncated_array():
    analyzer = VLVideoAnalyzer(_build_analyzer_config())
    content = """
[
  {
    "step_id": 1,
    "step_description": "Open settings",
    "step_type": "MAIN_FLOW",
    "main_operation": "click settings",
    "clip_start_sec": 0.0,
    "clip_end_sec": 7.0,
    "instructional_keyframe_timestamp": [6.2]
  },
  {
    "step_id": 2,
    "step_description": "Change port",
    "step_type": "MAIN_FLOW",
    "main_operation": "update port and save",
    "clip_start_sec": 7.0,
    "clip_end_sec": 13.0,
    "instructional_keyframe_timestamp": [12.2]
  }
""".strip()

    results, normalized = analyzer._parse_response_with_payload(
        content,
        analysis_mode="tutorial_stepwise",
    )

    assert len(results) == 2
    assert [item.step_id for item in results] == [1, 2]
    assert [item.step_description for item in results] == ["Open settings", "Change port"]
    assert [item["step_description"] for item in normalized] == ["Open settings", "Change port"]


def test_tutorial_schema_ignores_no_needed_video_and_should_type_override():
    analyzer = VLVideoAnalyzer(_build_analyzer_config())
    payload = [
        {
            "step_id": 1,
            "step_description": "Explain concept",
            "clip_start_sec": 0.0,
            "clip_end_sec": 8.0,
            "instructional_keyframe_timestamp": [6.0],
            "no_needed_video": True,
            "should_type": "concrete",
        },
        {
            "step_id": 2,
            "step_description": "Show operation",
            "clip_start_sec": 8.0,
            "clip_end_sec": 16.0,
            "instructional_keyframe_timestamp": [14.0],
            "no_needed_video": False,
            "should_type": "concrete",
        },
    ]
    text = json.dumps(payload, ensure_ascii=False)

    results, normalized = analyzer._parse_response_with_payload(
        text,
        analysis_mode="tutorial_stepwise",
    )

    assert len(results) == 2
    assert results[0].no_needed_video is False
    assert results[0].should_type == ""
    assert "no_needed_video" not in normalized[0]
    assert "should_type" not in normalized[0]

    assert results[1].no_needed_video is False
    assert results[1].should_type == ""
    assert "no_needed_video" not in normalized[1]
    assert "should_type" not in normalized[1]


def test_normalize_route_controls_no_needed_video_has_highest_priority():
    analyzer = VLVideoAnalyzer(_build_analyzer_config())

    no_needed_video, should_type = analyzer._normalize_route_controls(True, "concrete")
    assert no_needed_video is True
    assert should_type == "abstract"

    no_needed_video_2, should_type_2 = analyzer._normalize_route_controls(False, "concrete")
    assert no_needed_video_2 is False
    assert should_type_2 == "concrete"


class _FakeKeyframeCapture:
    def __init__(self):
        self.released = False

    def isOpened(self):
        return True

    def get(self, _prop):
        return 25.0

    def set(self, _prop, _value):
        return True

    def read(self):
        if self.released:
            return False, None
        return True, b"frame"

    def release(self):
        self.released = True


def test_extract_keyframes_forces_inline_transcode_for_semantic_unit_clips_vl(monkeypatch):
    analyzer = VLVideoAnalyzer(_build_analyzer_config())
    call_args: Dict[str, Any] = {}

    def _fake_open_video_capture_with_fallback(video_path: str, **kwargs):
        cap = _FakeKeyframeCapture()
        call_args["cap"] = cap
        call_args["video_path"] = video_path
        call_args["allow_inline_transcode"] = kwargs.get("allow_inline_transcode")
        return cap, video_path, False

    monkeypatch.setattr(
        "services.python_grpc.src.content_pipeline.phase2a.materials.vl_video_analyzer.open_video_capture_with_fallback",
        _fake_open_video_capture_with_fallback,
    )

    import cv2  # type: ignore
    from PIL import Image  # type: ignore

    monkeypatch.setattr(cv2, "cvtColor", lambda frame, _code: frame)
    monkeypatch.setattr(Image, "fromarray", lambda _frame: object())
    monkeypatch.setattr(
        analyzer,
        "_encode_image_as_jpeg_data_uri",
        lambda _image: "data:image/jpeg;base64,AA==",
    )

    frames = asyncio.run(
        analyzer._extract_keyframes(
            r"var\storage\storage\57018a9f0c5fe43f4622fb60ce8a9957\semantic_unit_clips_vl\001_SU001_demo_0.00-10.00.mp4",
            max_frames=1,
        )
    )

    assert call_args["allow_inline_transcode"] is True
    assert len(frames) == 1
    assert call_args["cap"].released is True


def test_extract_keyframes_keeps_default_transcode_policy_for_non_subset_path(monkeypatch):
    analyzer = VLVideoAnalyzer(_build_analyzer_config())
    call_args: Dict[str, Any] = {}

    def _fake_open_video_capture_with_fallback(video_path: str, **kwargs):
        cap = _FakeKeyframeCapture()
        call_args["cap"] = cap
        call_args["video_path"] = video_path
        call_args["allow_inline_transcode"] = kwargs.get("allow_inline_transcode")
        return cap, video_path, False

    monkeypatch.setattr(
        "services.python_grpc.src.content_pipeline.phase2a.materials.vl_video_analyzer.open_video_capture_with_fallback",
        _fake_open_video_capture_with_fallback,
    )

    import cv2  # type: ignore
    from PIL import Image  # type: ignore

    monkeypatch.setattr(cv2, "cvtColor", lambda frame, _code: frame)
    monkeypatch.setattr(Image, "fromarray", lambda _frame: object())
    monkeypatch.setattr(
        analyzer,
        "_encode_image_as_jpeg_data_uri",
        lambda _image: "data:image/jpeg;base64,AA==",
    )

    frames = asyncio.run(
        analyzer._extract_keyframes(
            r"var\storage\storage\57018a9f0c5fe43f4622fb60ce8a9957\semantic_unit_clips\001_SU001_demo_0.00-10.00.mp4",
            max_frames=1,
        )
    )

    assert call_args["allow_inline_transcode"] is None
    assert len(frames) == 1
    assert call_args["cap"].released is True


def test_material_generator_capture_policy_forces_inline_transcode_for_subset(monkeypatch):
    generator = VLMaterialGenerator(_build_generator_config())
    call_args: Dict[str, Any] = {}

    def _fake_open_video_capture_with_fallback(video_path: str, **kwargs):
        call_args["video_path"] = video_path
        call_args["allow_inline_transcode"] = kwargs.get("allow_inline_transcode")
        return None, video_path, False

    monkeypatch.setattr(
        "services.python_grpc.src.content_pipeline.phase2a.materials.vl_material_generator.open_video_capture_with_fallback",
        _fake_open_video_capture_with_fallback,
    )

    generator._open_video_capture_with_subset_policy(
        r"var\storage\storage\57018a9f0c5fe43f4622fb60ce8a9957\semantic_unit_clips_vl\001_SU001_demo_0.00-10.00.mp4"
    )
    assert call_args["allow_inline_transcode"] is True


def test_material_generator_capture_policy_keeps_default_for_non_subset(monkeypatch):
    generator = VLMaterialGenerator(_build_generator_config())
    call_args: Dict[str, Any] = {}

    def _fake_open_video_capture_with_fallback(video_path: str, **kwargs):
        call_args["video_path"] = video_path
        call_args["allow_inline_transcode"] = kwargs.get("allow_inline_transcode")
        return None, video_path, False

    monkeypatch.setattr(
        "services.python_grpc.src.content_pipeline.phase2a.materials.vl_material_generator.open_video_capture_with_fallback",
        _fake_open_video_capture_with_fallback,
    )

    generator._open_video_capture_with_subset_policy(
        r"var\storage\storage\57018a9f0c5fe43f4622fb60ce8a9957\video.mp4"
    )
    assert call_args["allow_inline_transcode"] is None


def test_analyze_clip_uses_unit_relative_ids_for_default_mode(monkeypatch):
    analyzer = VLVideoAnalyzer(_build_analyzer_config())

    async def _fake_call_vl_api(clip_path, extra_prompt=None, analysis_mode="default"):
        return (
            [
                VLAnalysisResult(
                    id=1,
                    knowledge_type="process",
                    clip_start_sec=2.0,
                    clip_end_sec=8.0,
                    suggested_screenshoot_timestamps=[3.0, 6.0],
                    step_id=1,
                    step_description="open dashboard",
                )
            ],
            {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
            [],
        )

    monkeypatch.setattr(analyzer, "_call_vl_api", _fake_call_vl_api)

    result = asyncio.run(
        analyzer.analyze_clip(
            clip_path="dummy.mp4",
            semantic_unit_start_sec=100.0,
            semantic_unit_id="SU100",
            analysis_mode="default",
        )
    )

    assert result.success is True
    assert result.clip_requests[0]["clip_id"] == "SU100/SU100_clip_vl_001"
    assert result.screenshot_requests[0]["screenshot_id"] == "SU100/SU100_ss_vl_01_01"
    assert result.screenshot_requests[1]["screenshot_id"] == "SU100/SU100_ss_vl_01_02"


def test_analyze_clip_no_needed_video_forces_abstract_and_skips_media(monkeypatch):
    analyzer = VLVideoAnalyzer(_build_analyzer_config())

    async def _fake_call_vl_api(clip_path, extra_prompt=None, analysis_mode="default"):
        return (
            [
                VLAnalysisResult(
                    id=1,
                    knowledge_type="process",
                    no_needed_video=True,
                    clip_start_sec=1.0,
                    clip_end_sec=6.0,
                    suggested_screenshoot_timestamps=[2.0, 4.0],
                    step_id=1,
                    step_description="explain concept",
                )
            ],
            {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
            [{"id": 1, "knowledge_type": "process", "no_needed_video": True}],
        )

    monkeypatch.setattr(analyzer, "_call_vl_api", _fake_call_vl_api)

    result = asyncio.run(
        analyzer.analyze_clip(
            clip_path="dummy.mp4",
            semantic_unit_start_sec=50.0,
            semantic_unit_id="SU050",
            analysis_mode="default",
        )
    )

    assert result.success is True
    assert len(result.analysis_results) == 1
    assert result.analysis_results[0].no_needed_video is True
    assert result.analysis_results[0].knowledge_type == "abstract"
    assert result.clip_requests == []
    assert result.screenshot_requests == []


def test_analyze_clip_should_type_abstract_routes_as_abstract(monkeypatch):
    analyzer = VLVideoAnalyzer(_build_analyzer_config())

    async def _fake_call_vl_api(clip_path, extra_prompt=None, analysis_mode="default"):
        return (
            [
                VLAnalysisResult(
                    id=1,
                    knowledge_type="process",
                    should_type="abstract",
                    no_needed_video=False,
                    clip_start_sec=1.0,
                    clip_end_sec=6.0,
                    suggested_screenshoot_timestamps=[2.0, 4.0],
                    step_id=1,
                    step_description="abstract explanation",
                )
            ],
            {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
            [{"id": 1, "knowledge_type": "process", "should_type": "abstract"}],
        )

    monkeypatch.setattr(analyzer, "_call_vl_api", _fake_call_vl_api)

    result = asyncio.run(
        analyzer.analyze_clip(
            clip_path="dummy.mp4",
            semantic_unit_start_sec=70.0,
            semantic_unit_id="SU070",
            analysis_mode="default",
        )
    )

    assert result.success is True
    assert len(result.analysis_results) == 1
    assert result.analysis_results[0].knowledge_type == "abstract"
    assert result.analysis_results[0].should_type == "abstract"
    assert result.clip_requests == []
    assert result.screenshot_requests == []


def test_analyze_clip_should_type_concrete_routes_as_concrete(monkeypatch):
    analyzer = VLVideoAnalyzer(_build_analyzer_config())

    async def _fake_call_vl_api(clip_path, extra_prompt=None, analysis_mode="default"):
        return (
            [
                VLAnalysisResult(
                    id=1,
                    knowledge_type="process",
                    should_type="concrete",
                    no_needed_video=False,
                    clip_start_sec=1.0,
                    clip_end_sec=6.0,
                    suggested_screenshoot_timestamps=[2.0, 4.0],
                    step_id=1,
                    step_description="concrete demo",
                )
            ],
            {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
            [{"id": 1, "knowledge_type": "process", "should_type": "concrete"}],
        )

    monkeypatch.setattr(analyzer, "_call_vl_api", _fake_call_vl_api)

    result = asyncio.run(
        analyzer.analyze_clip(
            clip_path="dummy.mp4",
            semantic_unit_start_sec=80.0,
            semantic_unit_id="SU080",
            analysis_mode="default",
        )
    )

    assert result.success is True
    assert len(result.analysis_results) == 1
    assert result.analysis_results[0].knowledge_type == "concrete"
    assert result.analysis_results[0].should_type == "concrete"
    assert result.clip_requests == []
    assert len(result.screenshot_requests) == 2


def test_analyze_clip_emits_instructional_clip_requests_for_tutorial_mode(monkeypatch):
    analyzer = VLVideoAnalyzer(_build_analyzer_config())

    async def _fake_call_vl_api(clip_path, extra_prompt=None, analysis_mode="default"):
        return (
            [
                VLAnalysisResult(
                    id=1,
                    knowledge_type="process",
                    clip_start_sec=1.0,
                    clip_end_sec=9.0,
                    suggested_screenshoot_timestamps=[7.5],
                    instructional_keyframes=[
                        {
                            "timestamp_sec": 7.5,
                            "frame_reason": "port value changed",
                            "bbox": [100, 120, 900, 980],
                        }
                    ],
                    instructional_clips=[
                        {
                            "clip_id": "CLIP_1",
                            "start_sec": 6.0,
                            "end_sec": 8.5,
                            "clip_reason": "watch the save confirmation animate in",
                        }
                    ],
                    step_id=2,
                    step_description="change port",
                )
            ],
            {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
            [],
        )

    monkeypatch.setattr(analyzer, "_call_vl_api", _fake_call_vl_api)

    result = asyncio.run(
        analyzer.analyze_clip(
            clip_path="dummy.mp4",
            semantic_unit_start_sec=200.0,
            semantic_unit_id="SU201",
            analysis_mode="tutorial_stepwise",
        )
    )

    assert result.success is True
    assert len(result.clip_requests) == 2
    assert result.clip_requests[0]["clip_id"] == "SU201/SU201_clip_step_02_clip_01_change_port"
    assert result.clip_requests[0]["instructional_clip_id"] == "CLIP_1"
    assert result.clip_requests[0]["clip_reason"] == "watch the save confirmation animate in"
    assert result.clip_requests[0]["start_sec"] == 206.0
    assert result.clip_requests[0]["end_sec"] == 208.5
    assert result.clip_requests[1]["clip_id"] == "SU201/SU201_clip_step_02_change_port"


def test_analyze_clip_uses_unit_relative_ids_for_tutorial_mode(monkeypatch):
    analyzer = VLVideoAnalyzer(_build_analyzer_config())

    async def _fake_call_vl_api(clip_path, extra_prompt=None, analysis_mode="default"):
        return (
            [
                VLAnalysisResult(
                    id=1,
                    knowledge_type="process",
                    clip_start_sec=1.0,
                    clip_end_sec=9.0,
                    suggested_screenshoot_timestamps=[7.5],
                    instructional_keyframes=[
                        {
                            "timestamp_sec": 7.5,
                            "frame_reason": "port value changed",
                            "bbox": [100, 120, 900, 980],
                        }
                    ],
                    step_id=2,
                    step_description="change port",
                )
            ],
            {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
            [],
        )

    monkeypatch.setattr(analyzer, "_call_vl_api", _fake_call_vl_api)

    result = asyncio.run(
        analyzer.analyze_clip(
            clip_path="dummy.mp4",
            semantic_unit_start_sec=200.0,
            semantic_unit_id="SU200",
            analysis_mode="tutorial_stepwise",
        )
    )

    assert result.success is True
    assert result.clip_requests[0]["clip_id"] == "SU200/SU200_clip_step_02_change_port"
    assert result.screenshot_requests[0]["screenshot_id"] == "SU200/SU200_ss_step_02_key_01_change_port"
    assert result.screenshot_requests[0]["frame_reason"] == "port value changed"
    assert result.screenshot_requests[0]["bbox"] == [100, 120, 900, 980]
    assert "target_ui_type" not in result.screenshot_requests[0]
    assert "target_text" not in result.screenshot_requests[0]
    assert "target_relative_position" not in result.screenshot_requests[0]


def test_generate_marks_unit_abstract_when_no_needed_video(monkeypatch):
    sandbox_dir = Path("tmp_vl_no_needed_video_test")
    if sandbox_dir.exists():
        shutil.rmtree(sandbox_dir, ignore_errors=True)
    sandbox_dir.mkdir(parents=True, exist_ok=True)
    video_path = sandbox_dir / "video.mp4"
    video_path.write_bytes(b"video")
    output_dir = sandbox_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    semantic_units = [
        {
            "unit_id": "SU_NO_VIDEO",
            "knowledge_type": "process",
            "mult_steps": False,
            "start_sec": 10.0,
            "end_sec": 30.0,
        }
    ]

    generator = VLMaterialGenerator(_build_generator_config())

    class _NoNeededVideoAnalyzer:
        async def analyze_clip(
            self,
            clip_path: str,
            semantic_unit_start_sec: float,
            semantic_unit_id: str,
            extra_prompt: str | None = None,
            analysis_mode: str = "default",
        ) -> VLClipAnalysisResponse:
            result = VLClipAnalysisResponse(success=True)
            result.token_usage = {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}
            result.analysis_results = [
                VLAnalysisResult(
                    id=1,
                    knowledge_type="process",
                    no_needed_video=True,
                    clip_start_sec=0.0,
                    clip_end_sec=20.0,
                    suggested_screenshoot_timestamps=[5.0],
                )
            ]
            result.clip_requests = [
                {
                    "clip_id": f"{semantic_unit_id}/{semantic_unit_id}_clip_vl_001",
                    "start_sec": semantic_unit_start_sec,
                    "end_sec": semantic_unit_start_sec + 20.0,
                    "knowledge_type": "process",
                    "semantic_unit_id": semantic_unit_id,
                }
            ]
            result.screenshot_requests = [
                {
                    "screenshot_id": f"{semantic_unit_id}/{semantic_unit_id}_ss_vl_01_01",
                    "timestamp_sec": semantic_unit_start_sec + 5.0,
                    "semantic_unit_id": semantic_unit_id,
                    "_relative_timestamp": 5.0,
                }
            ]
            result.raw_response_json = [{"id": 1, "knowledge_type": "process", "no_needed_video": True}]
            return result

    generator._analyzer = _NoNeededVideoAnalyzer()

    clips_dir = sandbox_dir / "clips"
    clips_dir.mkdir(parents=True, exist_ok=True)
    clip_file = clips_dir / "001_SU_NO_VIDEO_demo_10.00-30.00.mp4"
    clip_file.write_bytes(b"clip")

    async def _fake_split_video_by_semantic_units(video_path, semantic_units, output_dir=None):
        return str(clips_dir)

    def _fake_find_clip_for_unit(clips_dir, unit_id, start_sec, end_sec):
        return str(clip_file)

    monkeypatch.setattr(generator, "_split_video_by_semantic_units", _fake_split_video_by_semantic_units)
    monkeypatch.setattr(generator, "_find_clip_for_unit", _fake_find_clip_for_unit)

    try:
        result = asyncio.run(
            generator.generate(
                video_path=str(video_path),
                semantic_units=semantic_units,
                output_dir=str(output_dir),
            )
        )
    finally:
        shutil.rmtree(sandbox_dir, ignore_errors=True)

    assert result.success is True
    assert result.clip_requests == []
    assert result.screenshot_requests == []
    assert semantic_units[0]["knowledge_type"] == "abstract"
    assert semantic_units[0]["_vl_no_needed_video"] is True


def test_generate_tutorial_mode_ignores_no_needed_video_and_should_type(monkeypatch):
    sandbox_dir = Path("tmp_vl_tutorial_ignore_route_controls_test")
    if sandbox_dir.exists():
        shutil.rmtree(sandbox_dir, ignore_errors=True)
    sandbox_dir.mkdir(parents=True, exist_ok=True)
    video_path = sandbox_dir / "video.mp4"
    video_path.write_bytes(b"video")
    output_dir = sandbox_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    semantic_units = [
        {
            "unit_id": "SU_TUTORIAL",
            "knowledge_type": "process",
            "mult_steps": True,
            "start_sec": 10.0,
            "end_sec": 35.0,
        }
    ]

    generator = VLMaterialGenerator(_build_generator_config())
    analyze_modes: list[str] = []

    class _TutorialAnalyzerWithRouteFields:
        async def analyze_clip(
            self,
            clip_path: str,
            semantic_unit_start_sec: float,
            semantic_unit_id: str,
            extra_prompt: str | None = None,
            analysis_mode: str = "default",
        ) -> VLClipAnalysisResponse:
            _ = (clip_path, extra_prompt)
            analyze_modes.append(str(analysis_mode))
            result = VLClipAnalysisResponse(success=True, analysis_mode="tutorial_stepwise")
            result.token_usage = {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}
            result.analysis_results = [
                VLAnalysisResult(
                    id=1,
                    knowledge_type="process",
                    no_needed_video=True,
                    should_type="abstract",
                    clip_start_sec=0.0,
                    clip_end_sec=20.0,
                    suggested_screenshoot_timestamps=[5.0],
                    step_id=1,
                    step_description="demo step",
                    main_operation=["do demo [KEYFRAME_1]"],
                )
            ]
            result.clip_requests = [
                {
                    "clip_id": f"{semantic_unit_id}/{semantic_unit_id}_clip_step_01_demo_step",
                    "start_sec": semantic_unit_start_sec,
                    "end_sec": semantic_unit_start_sec + 20.0,
                    "knowledge_type": "process",
                    "semantic_unit_id": semantic_unit_id,
                    "analysis_mode": "tutorial_stepwise",
                }
            ]
            result.screenshot_requests = [
                {
                    "screenshot_id": f"{semantic_unit_id}/{semantic_unit_id}_ss_step_01_key_01_demo_step",
                    "timestamp_sec": semantic_unit_start_sec + 5.0,
                    "semantic_unit_id": semantic_unit_id,
                    "_relative_timestamp": 5.0,
                    "analysis_mode": "tutorial_stepwise",
                }
            ]
            result.raw_response_json = [
                {
                    "step_id": 1,
                    "step_description": "demo step",
                    "no_needed_video": True,
                    "should_type": "abstract",
                }
            ]
            return result

    generator._analyzer = _TutorialAnalyzerWithRouteFields()

    clips_dir = sandbox_dir / "clips"
    clips_dir.mkdir(parents=True, exist_ok=True)
    clip_file = clips_dir / "001_SU_TUTORIAL_demo_10.00-35.00.mp4"
    clip_file.write_bytes(b"clip")

    async def _fake_split_video_by_semantic_units(video_path, semantic_units, output_dir=None):
        return str(clips_dir)

    def _fake_find_clip_for_unit(clips_dir, unit_id, start_sec, end_sec):
        return str(clip_file)

    monkeypatch.setattr(generator, "_split_video_by_semantic_units", _fake_split_video_by_semantic_units)
    monkeypatch.setattr(generator, "_find_clip_for_unit", _fake_find_clip_for_unit)

    try:
        result = asyncio.run(
            generator.generate(
                video_path=str(video_path),
                semantic_units=semantic_units,
                output_dir=str(output_dir),
            )
        )
    finally:
        shutil.rmtree(sandbox_dir, ignore_errors=True)

    assert result.success is True
    assert analyze_modes == ["tutorial_stepwise"]
    assert len(result.clip_requests) == 1
    assert len(result.screenshot_requests) == 1
    assert semantic_units[0]["knowledge_type"] == "process"
    assert "_vl_route_override" not in semantic_units[0]
    assert "_vl_no_needed_video" not in semantic_units[0]
    assert result.token_stats["no_needed_video_units"] == 0
    assert result.token_stats["should_type_abstract_units"] == 0


def test_generate_marks_unit_concrete_when_should_type_concrete(monkeypatch):
    sandbox_dir = Path("tmp_vl_should_type_concrete_test")
    if sandbox_dir.exists():
        shutil.rmtree(sandbox_dir, ignore_errors=True)
    sandbox_dir.mkdir(parents=True, exist_ok=True)
    video_path = sandbox_dir / "video.mp4"
    video_path.write_bytes(b"video")
    output_dir = sandbox_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    semantic_units = [
        {
            "unit_id": "SU_CONCRETE",
            "knowledge_type": "process",
            "mult_steps": False,
            "start_sec": 10.0,
            "end_sec": 30.0,
        }
    ]

    generator = VLMaterialGenerator(_build_generator_config())

    class _ShouldTypeConcreteAnalyzer:
        async def analyze_clip(
            self,
            clip_path: str,
            semantic_unit_start_sec: float,
            semantic_unit_id: str,
            extra_prompt: str | None = None,
            analysis_mode: str = "default",
        ) -> VLClipAnalysisResponse:
            result = VLClipAnalysisResponse(success=True)
            result.token_usage = {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}
            result.analysis_results = [
                VLAnalysisResult(
                    id=1,
                    knowledge_type="process",
                    should_type="concrete",
                    no_needed_video=False,
                    clip_start_sec=0.0,
                    clip_end_sec=20.0,
                    suggested_screenshoot_timestamps=[5.0],
                )
            ]
            result.clip_requests = [
                {
                    "clip_id": f"{semantic_unit_id}/{semantic_unit_id}_clip_vl_001",
                    "start_sec": semantic_unit_start_sec,
                    "end_sec": semantic_unit_start_sec + 20.0,
                    "knowledge_type": "process",
                    "semantic_unit_id": semantic_unit_id,
                }
            ]
            result.screenshot_requests = [
                {
                    "screenshot_id": f"{semantic_unit_id}/{semantic_unit_id}_ss_vl_01_01",
                    "timestamp_sec": semantic_unit_start_sec + 5.0,
                    "semantic_unit_id": semantic_unit_id,
                    "_relative_timestamp": 5.0,
                }
            ]
            result.raw_response_json = [{"id": 1, "knowledge_type": "process", "should_type": "concrete"}]
            return result

    generator._analyzer = _ShouldTypeConcreteAnalyzer()

    clips_dir = sandbox_dir / "clips"
    clips_dir.mkdir(parents=True, exist_ok=True)
    clip_file = clips_dir / "001_SU_CONCRETE_demo_10.00-30.00.mp4"
    clip_file.write_bytes(b"clip")

    async def _fake_split_video_by_semantic_units(video_path, semantic_units, output_dir=None):
        return str(clips_dir)

    def _fake_find_clip_for_unit(clips_dir, unit_id, start_sec, end_sec):
        return str(clip_file)

    monkeypatch.setattr(generator, "_split_video_by_semantic_units", _fake_split_video_by_semantic_units)
    monkeypatch.setattr(generator, "_find_clip_for_unit", _fake_find_clip_for_unit)

    try:
        result = asyncio.run(
            generator.generate(
                video_path=str(video_path),
                semantic_units=semantic_units,
                output_dir=str(output_dir),
            )
        )
    finally:
        shutil.rmtree(sandbox_dir, ignore_errors=True)

    assert result.success is True
    assert result.clip_requests == []
    assert len(result.screenshot_requests) == 1
    assert semantic_units[0]["knowledge_type"] == "concrete"
    assert semantic_units[0]["_vl_route_override"] == "concrete"
    assert semantic_units[0]["_vl_no_needed_video"] is False


def test_generate_should_type_concrete_reruns_concrete_mode_and_postprocesses_main_content(monkeypatch):
    sandbox_dir = Path("tmp_vl_should_type_concrete_rerun_test")
    if sandbox_dir.exists():
        shutil.rmtree(sandbox_dir, ignore_errors=True)
    sandbox_dir.mkdir(parents=True, exist_ok=True)
    video_path = sandbox_dir / "video.mp4"
    video_path.write_bytes(b"video")
    output_dir = sandbox_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    semantic_units = [
        {
            "unit_id": "SU_CONCRETE_RERUN",
            "knowledge_type": "process",
            "mult_steps": False,
            "start_sec": 10.0,
            "end_sec": 30.0,
        }
    ]

    generator = VLMaterialGenerator(_build_generator_config())
    generator.vl_arg_postprocess_concrete_enabled = True

    analyze_modes: list[str] = []
    deepseek_calls: list[dict[str, Any]] = []

    class _ConcreteRerunAnalyzer:
        async def analyze_clip(
            self,
            clip_path: str,
            semantic_unit_start_sec: float,
            semantic_unit_id: str,
            extra_prompt: str | None = None,
            analysis_mode: str = "default",
        ) -> VLClipAnalysisResponse:
            _ = (clip_path, extra_prompt)
            analyze_modes.append(str(analysis_mode))
            if str(analysis_mode) != "concrete":
                result = VLClipAnalysisResponse(success=True, analysis_mode="default")
                result.token_usage = {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}
                result.analysis_results = [
                    VLAnalysisResult(
                        id=1,
                        knowledge_type="process",
                        should_type="concrete",
                        no_needed_video=False,
                        clip_start_sec=0.0,
                        clip_end_sec=20.0,
                        suggested_screenshoot_timestamps=[5.0],
                    )
                ]
                result.clip_requests = [
                    {
                        "clip_id": f"{semantic_unit_id}/{semantic_unit_id}_clip_vl_001",
                        "start_sec": semantic_unit_start_sec,
                        "end_sec": semantic_unit_start_sec + 20.0,
                        "knowledge_type": "process",
                        "semantic_unit_id": semantic_unit_id,
                    }
                ]
                result.screenshot_requests = [
                    {
                        "screenshot_id": f"{semantic_unit_id}/{semantic_unit_id}_ss_vl_01_01",
                        "timestamp_sec": semantic_unit_start_sec + 5.0,
                        "semantic_unit_id": semantic_unit_id,
                        "_relative_timestamp": 5.0,
                    }
                ]
                result.raw_response_json = [{"id": 1, "knowledge_type": "process", "should_type": "concrete"}]
                return result

            concrete_result = VLClipAnalysisResponse(success=True, analysis_mode="concrete")
            concrete_result.token_usage = {"prompt_tokens": 7, "completion_tokens": 3, "total_tokens": 10}
            concrete_result.analysis_results = [
                VLAnalysisResult(
                    id=1,
                    knowledge_type="concrete",
                    no_needed_video=False,
                    clip_start_sec=0.0,
                    clip_end_sec=20.0,
                    suggested_screenshoot_timestamps=[6.0],
                    analysis_mode="concrete",
                )
            ]
            concrete_result.clip_requests = []
            concrete_result.screenshot_requests = [
                {
                    "screenshot_id": f"{semantic_unit_id}/{semantic_unit_id}_ss_concrete_seg_01_key_01",
                    "timestamp_sec": semantic_unit_start_sec + 6.0,
                    "semantic_unit_id": semantic_unit_id,
                    "_relative_timestamp": 6.0,
                    "analysis_mode": "concrete",
                    "knowledge_type": "concrete",
                }
            ]
            concrete_result.raw_response_json = [
                {
                    "segment_id": 1,
                    "segment_description": "demo",
                    "main_content": "原始内容 [KEYFRAME_1]",
                    "clip_start_sec": 0.0,
                    "clip_end_sec": 20.0,
                    "instructional_keyframes": [{"timestamp_sec": 6.0}],
                }
            ]
            return concrete_result

    generator._analyzer = _ConcreteRerunAnalyzer()

    async def _fake_deepseek_complete_text_with_backoff(**kwargs):
        deepseek_calls.append(kwargs)
        return ('[{"segment_id":1,"main_content":"增量补充后 [KEYFRAME_1]"}]', None, None)

    monkeypatch.setattr(
        generator,
        "_call_deepseek_complete_text_with_backoff",
        _fake_deepseek_complete_text_with_backoff,
    )

    clips_dir = sandbox_dir / "clips"
    clips_dir.mkdir(parents=True, exist_ok=True)
    clip_file = clips_dir / "001_SU_CONCRETE_RERUN_demo_10.00-30.00.mp4"
    clip_file.write_bytes(b"clip")

    async def _fake_split_video_by_semantic_units(video_path, semantic_units, output_dir=None):
        return str(clips_dir)

    def _fake_find_clip_for_unit(clips_dir, unit_id, start_sec, end_sec):
        return str(clip_file)

    monkeypatch.setattr(generator, "_split_video_by_semantic_units", _fake_split_video_by_semantic_units)
    monkeypatch.setattr(generator, "_find_clip_for_unit", _fake_find_clip_for_unit)

    try:
        result = asyncio.run(
            generator.generate(
                video_path=str(video_path),
                semantic_units=semantic_units,
                output_dir=str(output_dir),
            )
        )
    finally:
        shutil.rmtree(sandbox_dir, ignore_errors=True)

    assert result.success is True
    assert analyze_modes == ["default", "concrete"]
    assert result.clip_requests == []
    assert len(result.screenshot_requests) == 1
    assert "ss_concrete_seg_01_key_01" in result.screenshot_requests[0]["screenshot_id"]
    assert len(deepseek_calls) == 1
    assert result.token_stats["should_type_concrete_reanalysis_units"] == 1
    assert result.token_stats["prompt_tokens_actual"] == 17
    assert result.token_stats["completion_tokens_actual"] == 8
    assert result.token_stats["total_tokens_actual"] == 25
    assert semantic_units[0]["knowledge_type"] == "concrete"
    assert semantic_units[0]["_vl_analysis_mode_override"] == "concrete"
    assert len(result.unit_analysis_outputs) == 1
    assert result.unit_analysis_outputs[0]["analysis_mode"] == "concrete"
    assert result.unit_analysis_outputs[0]["raw_response_json"][0]["main_content"] == "增量补充后 [KEYFRAME_1]"


def test_generate_uses_stream_unit_pipeline_when_enabled(monkeypatch, tmp_path):
    config = _build_generator_config()
    config["vl_analysis"] = {
        "parallel_workers": 2,
        "parallel_hard_cap": 8,
        "stream_unit_pipeline_enabled": True,
        "stream_pipeline_knowledge_types": ["process", "concrete"],
    }
    generator = VLMaterialGenerator(config)

    video_path = Path(tmp_path) / "video.mp4"
    video_path.write_bytes(b"video")
    output_dir = Path(tmp_path) / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    semantic_units = [
        {
            "unit_id": "SU_STREAM_001",
            "knowledge_type": "process",
            "mult_steps": True,
            "start_sec": 0.0,
            "end_sec": 12.0,
        }
    ]

    called: Dict[str, int] = {"stream": 0, "legacy_split": 0}

    async def _fake_stream_pipeline(
        *,
        video_path: str,
        semantic_units: list[dict[str, Any]],
        resolved_output_dir: str,
        all_clip_requests: list[dict[str, Any]],
        all_screenshot_requests: list[dict[str, Any]],
        token_stats: dict[str, Any],
    ):
        _ = (video_path, semantic_units, resolved_output_dir)
        called["stream"] += 1
        token_stats["vl_units"] = 1
        token_stats["prompt_tokens_actual"] = 10
        token_stats["completion_tokens_actual"] = 5
        token_stats["total_tokens_actual"] = 15
        all_clip_requests.append(
            {
                "clip_id": "SU_STREAM_001/SU_STREAM_001_clip_001",
                "start_sec": 0.0,
                "end_sec": 6.0,
            }
        )
        all_screenshot_requests.append(
            {
                "screenshot_id": "SU_STREAM_001/SU_STREAM_001_ss_001",
                "timestamp_sec": 3.0,
            }
        )
        return [], [], {}, 0

    async def _fake_legacy_split(*args, **kwargs):
        _ = (args, kwargs)
        called["legacy_split"] += 1
        return str(output_dir / "semantic_unit_clips_vl")

    monkeypatch.setattr(generator, "_run_stream_unit_pipeline", _fake_stream_pipeline)
    monkeypatch.setattr(generator, "_split_video_by_semantic_units", _fake_legacy_split)
    monkeypatch.setattr(generator, "_serialize_unit_analysis_outputs", lambda **kwargs: [])

    result = asyncio.run(
        generator.generate(
            video_path=str(video_path),
            semantic_units=semantic_units,
            output_dir=str(output_dir),
        )
    )

    assert result.success is True
    assert called["stream"] == 1
    assert called["legacy_split"] == 0
    assert len(result.clip_requests) == 1
    assert len(result.screenshot_requests) == 1


def test_generate_uses_hybrid_stream_pipeline_for_mixed_units(monkeypatch, tmp_path):
    config = _build_generator_config()
    config["vl_analysis"] = {
        "parallel_workers": 2,
        "parallel_hard_cap": 8,
        "stream_unit_pipeline_enabled": True,
        "stream_pipeline_knowledge_types": ["process", "concrete"],
    }
    generator = VLMaterialGenerator(config)

    video_path = Path(tmp_path) / "video.mp4"
    video_path.write_bytes(b"video")
    output_dir = Path(tmp_path) / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    legacy_clips_dir = output_dir / "semantic_unit_clips_vl"
    legacy_clips_dir.mkdir(parents=True, exist_ok=True)
    legacy_clip_file = legacy_clips_dir / "001_SU_LEGACY_001_demo_12.00-24.00.mp4"
    legacy_clip_file.write_bytes(b"clip")

    semantic_units = [
        {
            "unit_id": "SU_STREAM_001",
            "knowledge_type": "process",
            "mult_steps": True,
            "start_sec": 0.0,
            "end_sec": 12.0,
        },
        {
            "unit_id": "SU_LEGACY_001",
            "knowledge_type": "unknown",
            "mult_steps": False,
            "start_sec": 12.0,
            "end_sec": 24.0,
        },
    ]

    called: Dict[str, list[str]] = {
        "stream_units": [],
        "legacy_split_units": [],
        "legacy_analyze_units": [],
    }

    async def _fake_stream_pipeline(
        *,
        video_path: str,
        semantic_units: list[dict[str, Any]],
        resolved_output_dir: str,
        all_clip_requests: list[dict[str, Any]],
        all_screenshot_requests: list[dict[str, Any]],
        token_stats: dict[str, Any],
    ):
        _ = (video_path, resolved_output_dir)
        called["stream_units"] = [str(unit.get("unit_id", "") or "") for unit in semantic_units]
        token_stats["vl_units"] += 1
        token_stats["prompt_tokens_actual"] += 10
        token_stats["completion_tokens_actual"] += 5
        token_stats["total_tokens_actual"] += 15
        all_clip_requests.append(
            {
                "clip_id": "SU_STREAM_001/SU_STREAM_001_clip_001",
                "start_sec": 0.0,
                "end_sec": 6.0,
            }
        )
        all_screenshot_requests.append(
            {
                "screenshot_id": "SU_STREAM_001/SU_STREAM_001_ss_001",
                "timestamp_sec": 3.0,
            }
        )
        return [SimpleNamespace(success=True)], [{"unit_id": "SU_STREAM_001"}], {}, 0

    async def _fake_legacy_split(video_path, semantic_units, output_dir=None):
        _ = (video_path, output_dir)
        called["legacy_split_units"] = [str(unit.get("unit_id", "") or "") for unit in semantic_units]
        return str(legacy_clips_dir)

    def _fake_find_clip_for_unit(clips_dir, unit_id, start_sec, end_sec):
        _ = (clips_dir, unit_id, start_sec, end_sec)
        return str(legacy_clip_file)

    async def _fake_resolve_pre_prune_results_for_unit_tasks(clips_dir, unit_tasks, force_preprocess=False):
        _ = (clips_dir, force_preprocess)
        return [
            generator._build_default_pre_prune_info(
                semantic_unit=task.get("semantic_unit", {}),
                clip_path=str(task.get("clip_path", "") or ""),
            )
            for task in unit_tasks
        ]

    async def _fake_analyze_unit_tasks_in_parallel(*, unit_tasks, pre_prune_results, on_result):
        _ = (pre_prune_results, on_result)
        called["legacy_analyze_units"] = [str(task.get("unit_id", "") or "") for task in unit_tasks]
        return [SimpleNamespace(success=True)], [{"unit_id": "SU_LEGACY_001"}], 0

    async def _fake_consume_unit_analysis_result_streaming(
        *,
        result_index,
        analysis_result,
        meta,
        legacy_fallback_materials,
        all_clip_requests,
        all_screenshot_requests,
        token_stats,
        resolved_output_dir,
        original_video_path,
    ):
        _ = (
            result_index,
            analysis_result,
            legacy_fallback_materials,
            token_stats,
            resolved_output_dir,
            original_video_path,
        )
        unit_id = str(meta.get("unit_id", "") or "")
        if unit_id != "SU_LEGACY_001":
            return
        all_clip_requests.append(
            {
                "clip_id": "SU_LEGACY_001/SU_LEGACY_001_clip_001",
                "start_sec": 12.0,
                "end_sec": 18.0,
            }
        )
        all_screenshot_requests.append(
            {
                "screenshot_id": "SU_LEGACY_001/SU_LEGACY_001_ss_001",
                "timestamp_sec": 15.0,
            }
        )

    monkeypatch.setattr(generator, "_run_stream_unit_pipeline", _fake_stream_pipeline)
    monkeypatch.setattr(generator, "_split_video_by_semantic_units", _fake_legacy_split)
    monkeypatch.setattr(generator, "_find_clip_for_unit", _fake_find_clip_for_unit)
    monkeypatch.setattr(generator, "_resolve_pre_prune_results_for_unit_tasks", _fake_resolve_pre_prune_results_for_unit_tasks)
    monkeypatch.setattr(generator, "_should_use_stable_action_legacy_branch", lambda **kwargs: False)
    monkeypatch.setattr(generator, "_analyze_unit_tasks_in_parallel", _fake_analyze_unit_tasks_in_parallel)
    monkeypatch.setattr(generator, "_consume_unit_analysis_result_streaming", _fake_consume_unit_analysis_result_streaming)
    monkeypatch.setattr(
        generator,
        "_serialize_unit_analysis_outputs",
        lambda **kwargs: [{"unit_id": meta.get("unit_id", ""), "success": True} for meta in kwargs.get("task_metadata", [])],
    )

    result = asyncio.run(
        generator.generate(
            video_path=str(video_path),
            semantic_units=semantic_units,
            output_dir=str(output_dir),
        )
    )

    assert result.success is True
    assert called["stream_units"] == ["SU_STREAM_001"]
    assert called["legacy_split_units"] == ["SU_LEGACY_001"]
    assert called["legacy_analyze_units"] == ["SU_LEGACY_001"]
    assert result.token_stats["vl_units"] == 2
    assert len(result.clip_requests) == 2
    assert len(result.screenshot_requests) == 2
    assert [item["unit_id"] for item in result.unit_analysis_outputs] == ["SU_STREAM_001", "SU_LEGACY_001"]


def test_generate_uses_concrete_mode_override_and_exposes_unit_outputs(monkeypatch):
    sandbox_dir = Path("tmp_vl_concrete_mode_override_test")
    if sandbox_dir.exists():
        shutil.rmtree(sandbox_dir, ignore_errors=True)
    sandbox_dir.mkdir(parents=True, exist_ok=True)
    video_path = sandbox_dir / "video.mp4"
    video_path.write_bytes(b"video")
    output_dir = sandbox_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    semantic_units = [
        {
            "unit_id": "SU_CONCRETE_MODE",
            "knowledge_type": "concrete",
            "_vl_analysis_mode_override": "concrete",
            "mult_steps": False,
            "start_sec": 0.0,
            "end_sec": 20.0,
        }
    ]

    generator = VLMaterialGenerator(_build_generator_config())
    generator.vl_arg_postprocess_concrete_enabled = False

    class _ConcreteModeAnalyzer:
        async def analyze_clip(
            self,
            clip_path: str,
            semantic_unit_start_sec: float,
            semantic_unit_id: str,
            extra_prompt: str | None = None,
            analysis_mode: str = "default",
        ) -> VLClipAnalysisResponse:
            _ = (clip_path, extra_prompt)
            assert analysis_mode == "concrete"
            result = VLClipAnalysisResponse(success=True, analysis_mode="concrete")
            result.token_usage = {"prompt_tokens": 8, "completion_tokens": 6, "total_tokens": 14}
            result.raw_response_json = [
                {
                    "segment_id": 1,
                    "segment_description": "示例片段",
                    "main_content": "具体讲解内容 [KEYFRAME_1]",
                    "clip_start_sec": 0.0,
                    "clip_end_sec": 12.0,
                    "instructional_keyframes": [
                        {"timestamp_sec": 3.0, "frame_reason": "关键画面"}
                    ],
                }
            ]
            result.screenshot_requests = [
                {
                    "screenshot_id": f"{semantic_unit_id}/{semantic_unit_id}_ss_001",
                    "timestamp_sec": semantic_unit_start_sec + 3.0,
                    "semantic_unit_id": semantic_unit_id,
                }
            ]
            result.clip_requests = []
            return result

    generator._analyzer = _ConcreteModeAnalyzer()

    clips_dir = sandbox_dir / "clips"
    clips_dir.mkdir(parents=True, exist_ok=True)
    clip_file = clips_dir / "001_SU_CONCRETE_MODE_demo_0.00-20.00.mp4"
    clip_file.write_bytes(b"clip")

    async def _fake_split_video_by_semantic_units(video_path, semantic_units, output_dir=None):
        return str(clips_dir)

    def _fake_find_clip_for_unit(clips_dir, unit_id, start_sec, end_sec):
        return str(clip_file)

    monkeypatch.setattr(generator, "_split_video_by_semantic_units", _fake_split_video_by_semantic_units)
    monkeypatch.setattr(generator, "_find_clip_for_unit", _fake_find_clip_for_unit)

    try:
        result = asyncio.run(
            generator.generate(
                video_path=str(video_path),
                semantic_units=semantic_units,
                output_dir=str(output_dir),
            )
        )
    finally:
        shutil.rmtree(sandbox_dir, ignore_errors=True)

    assert result.success is True
    assert len(result.unit_analysis_outputs) == 1
    assert result.unit_analysis_outputs[0]["analysis_mode"] == "concrete"
    assert result.unit_analysis_outputs[0]["raw_response_json"][0]["main_content"] == "具体讲解内容 [KEYFRAME_1]"


class _FakeAnalyzer:
    async def analyze_clip(
        self,
        clip_path: str,
        semantic_unit_start_sec: float,
        semantic_unit_id: str,
        extra_prompt: str | None = None,
        analysis_mode: str = "default",
    ) -> VLClipAnalysisResponse:
        assert analysis_mode == "tutorial_stepwise"

        result = VLClipAnalysisResponse(
            success=True,
            analysis_mode="tutorial_stepwise",
            token_usage={"prompt_tokens": 10, "completion_tokens": 20, "total_tokens": 30},
        )
        result.raw_response_json = [
            {
                "step_id": 1,
                "step_description": "open settings",
                "main_action": "open settings page",
                "main_operation": "1. click settings\n2. open config tab\n[KEYFRAME_1]",
                "precautions": ["do not change unrelated fields"],
                "step_summary": "settings opened",
                "operation_guidance": ["open settings", "enter config tab"],
                "clip_start_sec": 0.0,
                "clip_end_sec": 8.0,
                "instructional_keyframes": [
                    {
                        "timestamp_sec": 7.2,
                        "frame_reason": "settings panel loaded",
                        "bbox": [120, 80, 760, 920],
                    }
                ],
            },
            {
                "step_id": 2,
                "step_description": "change port",
                "main_action": "modify port",
                "main_operation": "1. update port value\n2. click save\n[KEYFRAME_1]",
                "precautions": [],
                "clip_start_sec": 8.0,
                "clip_end_sec": 17.0,
                "instructional_keyframes": [
                    {
                        "timestamp_sec": 16.5,
                        "frame_reason": "save button clicked",
                        "bbox": [100, 120, 900, 980],
                    }
                ],
            },
        ]
        result.analysis_results = [
            VLAnalysisResult(step_id=1, step_description="open settings"),
            VLAnalysisResult(step_id=2, step_description="change port"),
        ]
        result.clip_requests = [
            {
                "clip_id": f"{semantic_unit_id}/{semantic_unit_id}_clip_step_01_open_settings",
                "start_sec": semantic_unit_start_sec + 0.0,
                "end_sec": semantic_unit_start_sec + 8.0,
                "knowledge_type": "process",
                "semantic_unit_id": semantic_unit_id,
                "step_id": 1,
                "step_description": "open settings",
                "action_brief": "open_settings",
                "main_action": "open settings page",
                "main_operation": ["click settings", "open config tab"],
                "precautions": ["do not change unrelated fields"],
                "step_summary": "settings opened",
                "operation_guidance": ["open settings", "enter config tab"],
                "analysis_mode": "tutorial_stepwise",
            },
            {
                "clip_id": f"{semantic_unit_id}/{semantic_unit_id}_clip_step_02_change_port",
                "start_sec": semantic_unit_start_sec + 8.0,
                "end_sec": semantic_unit_start_sec + 17.0,
                "knowledge_type": "process",
                "semantic_unit_id": semantic_unit_id,
                "step_id": 2,
                "step_description": "change port",
                "action_brief": "change_port",
                "main_action": "modify port",
                "main_operation": ["update port value", "click save"],
                "precautions": [],
                "analysis_mode": "tutorial_stepwise",
            },
        ]
        result.screenshot_requests = [
            {
                "screenshot_id": f"{semantic_unit_id}/{semantic_unit_id}_ss_step_01_key_01_open_settings",
                "timestamp_sec": semantic_unit_start_sec + 7.2,
                "label": "step_01 keyframe",
                "semantic_unit_id": semantic_unit_id,
                "step_id": 1,
                "step_description": "open settings",
                "action_brief": "open_settings",
                "analysis_mode": "tutorial_stepwise",
            },
            {
                "screenshot_id": f"{semantic_unit_id}/{semantic_unit_id}_ss_step_02_key_01_change_port",
                "timestamp_sec": semantic_unit_start_sec + 16.5,
                "label": "step_02 keyframe",
                "semantic_unit_id": semantic_unit_id,
                "step_id": 2,
                "step_description": "change port",
                "action_brief": "change_port",
                "analysis_mode": "tutorial_stepwise",
            },
        ]
        return result


def test_generate_tutorial_assets_per_unit_full_flow_before_phase2b(tmp_path, monkeypatch):
    video_path = tmp_path / "video.mp4"
    video_path.write_bytes(b"video")
    source_video_path = str(video_path)
    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    semantic_units = [
        {
            "unit_id": "SU001",
            "knowledge_type": "process",
            "mult_steps": True,
            "start_sec": 100.0,
            "end_sec": 130.0,
        }
    ]

    generator = VLMaterialGenerator(_build_generator_config())
    generator._analyzer = _FakeAnalyzer()

    clips_dir = tmp_path / "semantic_unit_clips"
    clips_dir.mkdir(parents=True, exist_ok=True)
    clip_file = clips_dir / "001_SU001_demo_100.00-130.00.mp4"
    clip_file.write_bytes(b"clip")

    async def _fake_split_video_by_semantic_units(video_path, semantic_units, output_dir=None):
        return str(clips_dir)

    def _fake_find_clip_for_unit(clips_dir, unit_id, start_sec, end_sec):
        return str(clip_file)

    async def _fake_prepare_pruned_clip_for_vl(
        clips_dir,
        semantic_unit,
        original_clip_path,
        force_preprocess=False,
        stable_intervals_override=None,
    ):
        return {
            "applied": False,
            "clip_path_for_vl": original_clip_path,
            "pre_context_prompt": "",
            "kept_segments": [],
        }

    async def _fake_export_clip_asset_with_ffmpeg(video_path, start_sec, end_sec, output_path):
        assert video_path in {source_video_path, str(clip_file)}
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"step-clip")
        return True

    async def _fake_export_keyframe_with_ffmpeg(video_path, timestamp_sec, output_path):
        assert video_path in {source_video_path, str(clip_file)}
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"keyframe")
        return True

    monkeypatch.setattr(generator, "_split_video_by_semantic_units", _fake_split_video_by_semantic_units)
    monkeypatch.setattr(generator, "_find_clip_for_unit", _fake_find_clip_for_unit)
    monkeypatch.setattr(generator, "_prepare_pruned_clip_for_vl", _fake_prepare_pruned_clip_for_vl)
    monkeypatch.setattr(generator, "_export_clip_asset_with_ffmpeg", _fake_export_clip_asset_with_ffmpeg)
    monkeypatch.setattr(generator, "_export_keyframe_with_ffmpeg", _fake_export_keyframe_with_ffmpeg)

    result = asyncio.run(
        generator.generate(
            video_path=str(video_path),
            semantic_units=semantic_units,
            output_dir=str(output_dir),
        )
    )

    assert result.success is True
    # Encoding fixed: corrupted comment cleaned.
    assert len(result.clip_requests) == 2

    unit_dir = output_dir / "vl_tutorial_units" / "SU001"
    assert unit_dir.exists()

    assert (unit_dir / "SU001_clip_step_01_open_settings.mp4").exists()
    assert (unit_dir / "SU001_ss_step_01_key_01_open_settings.png").exists()
    assert (unit_dir / "SU001_clip_step_02_change_port.mp4").exists()
    assert (unit_dir / "SU001_ss_step_02_key_01_change_port.png").exists()

    json_path = unit_dir / "SU001_steps.json"
    assert json_path.exists()
    data = json.loads(json_path.read_text(encoding="utf-8"))
    assert data.get("schema") == "tutorial_stepwise_v1"
    assert len(data.get("raw_response", [])) == 2
    assert len(data.get("steps", [])) == 2
    assert data["steps"][0]["main_action"] == "open settings page"
    assert data["steps"][0]["main_operation"] == "1. click settings\n2. open config tab\n[KEYFRAME_1]"
    assert data["steps"][0]["precautions"] == ["do not change unrelated fields"]
    assert data["steps"][0]["step_summary"] == "settings opened"
    assert data["steps"][0]["operation_guidance"] == ["open settings", "enter config tab"]
    assert data["steps"][0]["instructional_keyframe_details"][0]["frame_reason"] == "settings panel loaded"
    assert data["steps"][0]["instructional_keyframe_details"][0]["bbox"] == [120, 80, 760, 920]


def test_generate_postprocesses_main_operation_with_unit_context(tmp_path, monkeypatch):
    class _FakeAnalyzerForPostprocess:
        async def analyze_clip(
            self,
            clip_path: str,
            semantic_unit_start_sec: float,
            semantic_unit_id: str,
            extra_prompt: str | None = None,
            analysis_mode: str = "default",
        ) -> VLClipAnalysisResponse:
            _ = (clip_path, extra_prompt)
            assert analysis_mode == "tutorial_stepwise"
            result = VLClipAnalysisResponse(
                success=True,
                analysis_mode="tutorial_stepwise",
                token_usage={"prompt_tokens": 12, "completion_tokens": 24, "total_tokens": 36},
            )
            result.raw_response_json = [
                {
                    "step_id": 1,
                    "step_description": "open settings",
                    "main_operation": "click settings",
                    "clip_start_sec": 0.0,
                    "clip_end_sec": 7.0,
                },
                {
                    "step_id": 2,
                    "step_description": "change port",
                    "main_operation": "change port and save",
                    "clip_start_sec": 7.0,
                    "clip_end_sec": 15.0,
                },
            ]
            result.analysis_results = [
                VLAnalysisResult(
                    step_id=1,
                    step_description="open settings",
                    analysis_mode="tutorial_stepwise",
                    main_operation=["click settings"],
                ),
                VLAnalysisResult(
                    step_id=2,
                    step_description="change port",
                    analysis_mode="tutorial_stepwise",
                    main_operation=["change port and save"],
                ),
            ]
            result.clip_requests = [
                {
                    "clip_id": f"{semantic_unit_id}/{semantic_unit_id}_clip_step_01_open_settings",
                    "start_sec": semantic_unit_start_sec + 0.0,
                    "end_sec": semantic_unit_start_sec + 7.0,
                    "knowledge_type": "process",
                    "semantic_unit_id": semantic_unit_id,
                    "step_id": 1,
                    "step_description": "open settings",
                    "analysis_mode": "tutorial_stepwise",
                    "main_operation": ["click settings"],
                },
                {
                    "clip_id": f"{semantic_unit_id}/{semantic_unit_id}_clip_step_02_change_port",
                    "start_sec": semantic_unit_start_sec + 7.0,
                    "end_sec": semantic_unit_start_sec + 15.0,
                    "knowledge_type": "process",
                    "semantic_unit_id": semantic_unit_id,
                    "step_id": 2,
                    "step_description": "change port",
                    "analysis_mode": "tutorial_stepwise",
                    "main_operation": ["change port and save"],
                },
            ]
            result.screenshot_requests = []
            return result

    video_path = tmp_path / "video.mp4"
    video_path.write_bytes(b"video")
    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    clips_dir = tmp_path / "semantic_unit_clips"
    clips_dir.mkdir(parents=True, exist_ok=True)
    clip_file = clips_dir / "001_SU010_demo_100.00-130.00.mp4"
    clip_file.write_bytes(b"clip")

    semantic_units = [
        {
            "unit_id": "SU010",
            "knowledge_type": "process",
            "mult_steps": True,
            "start_sec": 100.0,
            "end_sec": 130.0,
            "knowledge_topic": "代理端口配置",
            "full_text": "先打开设置，再把端口改成 8899，最后保存。",
        }
    ]

    generator = VLMaterialGenerator(_build_generator_config())
    generator._analyzer = _FakeAnalyzerForPostprocess()

    async def _fake_split_video_by_semantic_units(video_path, semantic_units, output_dir=None):
        _ = (video_path, semantic_units, output_dir)
        return str(clips_dir)

    def _fake_find_clip_for_unit(clips_dir, unit_id, start_sec, end_sec):
        _ = (clips_dir, unit_id, start_sec, end_sec)
        return str(clip_file)

    monkeypatch.setattr(generator, "_split_video_by_semantic_units", _fake_split_video_by_semantic_units)
    monkeypatch.setattr(generator, "_find_clip_for_unit", _fake_find_clip_for_unit)
    monkeypatch.setattr(
        generator,
        "_build_unit_relative_subtitles",
        lambda subtitles, unit_start_sec, unit_end_sec: [
            {"start_sec": 0.5, "end_sec": 2.4, "text": "先打开设置页面"},
            {"start_sec": 2.5, "end_sec": 5.8, "text": "把端口改成八八九九然后保存"},
        ],
    )

    saved_payload: Dict[str, Any] = {}

    async def _fake_save_tutorial_assets_for_unit(
        video_path: str,
        output_dir: str,
        unit_id: str,
        clip_requests: list[Dict[str, Any]],
        screenshot_requests: list[Dict[str, Any]],
        raw_response_json: list[Dict[str, Any]],
        raw_llm_interactions: list[Dict[str, Any]] | None = None,
        use_analysis_relative_timestamps: bool = False,
        prefer_screenshot_requests_keyframes: bool = False,
    ) -> None:
        _ = (
            video_path,
            output_dir,
            unit_id,
            screenshot_requests,
            raw_llm_interactions,
            use_analysis_relative_timestamps,
            prefer_screenshot_requests_keyframes,
        )
        saved_payload["clip_requests"] = clip_requests
        saved_payload["raw_response_json"] = raw_response_json

    monkeypatch.setattr(generator, "_save_tutorial_assets_for_unit", _fake_save_tutorial_assets_for_unit)

    deepseek_calls: list[Dict[str, Any]] = []

    async def _fake_deepseek_complete_text(*, prompt: str, system_message: str | None = None, **kwargs):
        deepseek_calls.append(
            {
                "prompt": prompt,
                "system_message": system_message,
                "kwargs": kwargs,
            }
        )
        return (
            "[STEP_ID=1]\n"
            "- 增强步骤1：打开设置并确认进入配置页。\n\n"
            "[STEP_ID=2]\n"
            "- 增强步骤2：修改端口并保存配置。",
            {"prompt_tokens": 21},
            None,
        )

    monkeypatch.setattr(
        vl_material_generator_module.llm_gateway,
        "deepseek_complete_text",
        _fake_deepseek_complete_text,
    )

    result = asyncio.run(
        generator.generate(
            video_path=str(video_path),
            semantic_units=semantic_units,
            output_dir=str(output_dir),
        )
    )

    assert result.success is True
    assert len(deepseek_calls) == 1
    assert "click settings" in deepseek_calls[0]["prompt"]
    assert "change port and save" in deepseek_calls[0]["prompt"]
    assert "先打开设置页面" in deepseek_calls[0]["prompt"]
    assert "把端口改成八八九九然后保存" in deepseek_calls[0]["prompt"]
    assert "[STEP_ID=1]" in deepseek_calls[0]["prompt"]
    assert "[STEP_ID=2]" in deepseek_calls[0]["prompt"]
    assert deepseek_calls[0]["kwargs"]["hedge_context"]["stage"] == "vl_arg_main_operation_postprocess_batch"
    assert deepseek_calls[0]["kwargs"]["hedge_context"]["step_ids"] == [1, 2]

    by_step = {int(item.get("step_id", 0)): item for item in result.clip_requests}
    assert by_step[1]["main_operation"] == ["- 增强步骤1：打开设置并确认进入配置页。"]
    assert by_step[2]["main_operation"] == ["- 增强步骤2：修改端口并保存配置。"]

    saved_steps = saved_payload["raw_response_json"]
    assert saved_steps[0]["main_operation"] == "- 增强步骤1：打开设置并确认进入配置页。"
    assert saved_steps[0]["main_operations"] == "- 增强步骤1：打开设置并确认进入配置页。"
    assert saved_steps[1]["main_operation"] == "- 增强步骤2：修改端口并保存配置。"
    assert saved_steps[1]["main_operations"] == "- 增强步骤2：修改端口并保存配置。"


def test_resolve_pre_vl_parallel_workers_prefers_cv_executor():
    pool = ProcessPoolExecutor(max_workers=2)
    try:
        generator = VLMaterialGenerator(
            {
                "enabled": True,
                "pre_vl_static_pruning": {
                    "enabled": True,
                    "parallel_workers": "auto",
                    "parallel_hard_cap": 8,
                },
                "screenshot_optimization": {"enabled": False},
                "fallback": {"enabled": True},
            },
            cv_executor=pool,
        )

        workers = generator._resolve_pre_vl_parallel_workers(10)
        assert workers == 2
    finally:
        pool.shutdown(wait=True, cancel_futures=True)


def test_resolve_pre_vl_parallel_workers_respects_hard_cap():
    generator = VLMaterialGenerator(
        {
            "enabled": True,
            "pre_vl_static_pruning": {
                "enabled": True,
                "parallel_workers": 16,
                "parallel_hard_cap": 3,
            },
            "screenshot_optimization": {"enabled": False},
            "fallback": {"enabled": True},
        }
    )

    assert generator._resolve_pre_vl_parallel_workers(8) == 3


def test_prepare_pruned_clips_for_units_parallel_and_order(monkeypatch):
    generator = VLMaterialGenerator(
        {
            "enabled": True,
            "pre_vl_static_pruning": {
                "enabled": True,
                "parallel_workers": 2,
                "parallel_hard_cap": 8,
            },
            "screenshot_optimization": {"enabled": False},
            "fallback": {"enabled": True},
        }
    )

    async def _fake_prepare(
        clips_dir,
        semantic_unit,
        original_clip_path,
        force_preprocess=False,
        stable_intervals_override=None,
    ):
        unit_id = semantic_unit.get("unit_id")
        if unit_id == "U2":
            raise RuntimeError("boom")
        return {
            "applied": True,
            "clip_path_for_vl": original_clip_path,
            "kept_segments": [(0.0, 1.0)],
            "removed_segments": [],
            "pre_context_prompt": f"ctx-{unit_id}",
        }

    monkeypatch.setattr(generator, "_prepare_pruned_clip_for_vl", _fake_prepare)

    unit_tasks = [
        {"semantic_unit": {"unit_id": "U1", "start_sec": 0.0, "end_sec": 2.0}, "clip_path": "u1.mp4"},
        {"semantic_unit": {"unit_id": "U2", "start_sec": 2.0, "end_sec": 4.0}, "clip_path": "u2.mp4"},
        {"semantic_unit": {"unit_id": "U3", "start_sec": 4.0, "end_sec": 6.0}, "clip_path": "u3.mp4"},
    ]

    results = asyncio.run(
        generator._prepare_pruned_clips_for_units(
            clips_dir="dummy",
            unit_tasks=unit_tasks,
            force_preprocess=False,
        )
    )

    assert len(results) == 3
    assert results[0]["pre_context_prompt"] == "ctx-U1"
    assert results[1]["applied"] is False
    assert results[1]["clip_path_for_vl"] == "u2.mp4"
    assert results[2]["pre_context_prompt"] == "ctx-U3"


def test_resolve_vl_parallel_workers_respects_hard_cap():
    generator = VLMaterialGenerator(
        {
            "enabled": True,
            "vl_analysis": {
                "parallel_workers": "auto",
                "parallel_hard_cap": 2,
            },
            "screenshot_optimization": {"enabled": False},
            "fallback": {"enabled": True},
        }
    )

    assert generator._resolve_vl_parallel_workers(10) == 2


def test_analyze_unit_tasks_parallel_one_unit_one_api(monkeypatch):
    generator = VLMaterialGenerator(
        {
            "enabled": True,
            "vl_analysis": {
                "parallel_workers": 2,
                "parallel_hard_cap": 8,
            },
            "screenshot_optimization": {"enabled": False},
            "fallback": {"enabled": True},
        }
    )

    class _CountingAnalyzer:
        def __init__(self):
            self.calls = []

        async def analyze_clip(
            self,
            clip_path: str,
            semantic_unit_start_sec: float,
            semantic_unit_id: str,
            extra_prompt: str | None = None,
            analysis_mode: str = "default",
        ) -> VLClipAnalysisResponse:
            self.calls.append(
                {
                    "clip_path": clip_path,
                    "start": semantic_unit_start_sec,
                    "unit_id": semantic_unit_id,
                    "extra_prompt": extra_prompt,
                    "analysis_mode": analysis_mode,
                }
            )
            return VLClipAnalysisResponse(success=True)

    analyzer = _CountingAnalyzer()
    generator._analyzer = analyzer

    unit_tasks = [
        {
            "unit_id": "U1",
            "start_sec": 0.0,
            "end_sec": 10.0,
            "duration": 10.0,
            "clip_path": "u1.mp4",
            "analysis_mode": "default",
            "extra_prompt": None,
            "semantic_unit": {"unit_id": "U1"},
        },
        {
            "unit_id": "U2",
            "start_sec": 10.0,
            "end_sec": 20.0,
            "duration": 10.0,
            "clip_path": "u2.mp4",
            "analysis_mode": "tutorial_stepwise",
            "extra_prompt": "tutorial",
            "semantic_unit": {"unit_id": "U2"},
        },
        {
            "unit_id": "U3",
            "start_sec": 20.0,
            "end_sec": 30.0,
            "duration": 10.0,
            "clip_path": "u3.mp4",
            "analysis_mode": "default",
            "extra_prompt": None,
            "semantic_unit": {"unit_id": "U3"},
        },
    ]
    pre_prune_results = [
        {
            "applied": False,
            "clip_path_for_vl": "u1.mp4",
            "pre_context_prompt": "",
            "kept_segments": [(0.0, 10.0)],
        },
        {
            "applied": True,
            "clip_path_for_vl": "u2_pruned.mp4",
            "pre_context_prompt": "ctx-u2",
            "kept_segments": [(0.0, 3.0)],
        },
        {
            "applied": False,
            "clip_path_for_vl": "u3.mp4",
            "pre_context_prompt": "",
            "kept_segments": [(0.0, 10.0)],
        },
    ]

    analysis_results, task_metadata, pruned_units = asyncio.run(
        generator._analyze_unit_tasks_in_parallel(
            unit_tasks=unit_tasks,
            pre_prune_results=pre_prune_results,
        )
    )

    assert len(analysis_results) == 3
    assert len(task_metadata) == 3
    assert pruned_units == 1
    assert len(analyzer.calls) == 3
    assert sorted(call["unit_id"] for call in analyzer.calls) == ["U1", "U2", "U3"]

    call_u2 = next(call for call in analyzer.calls if call["unit_id"] == "U2")
    assert call_u2["clip_path"] == "u2_pruned.mp4"
    assert call_u2["analysis_mode"] == "tutorial_stepwise"
    assert "ctx-u2" in (call_u2["extra_prompt"] or "")


def test_analyze_unit_tasks_prefers_batch_entry_when_available():
    generator = VLMaterialGenerator(
        {
            "enabled": True,
            "vl_analysis": {
                "parallel_workers": 2,
                "parallel_hard_cap": 8,
            },
            "screenshot_optimization": {"enabled": False},
            "fallback": {"enabled": True},
        }
    )

    class _BatchAnalyzer:
        def __init__(self):
            self.batch_calls = []
            self.single_calls = []

        async def analyze_clips_batch(
            self,
            *,
            tasks: list[dict[str, Any]],
            max_inflight: int | None = None,
            return_exceptions: bool = True,
        ) -> list[VLClipAnalysisResponse]:
            self.batch_calls.append(
                {
                    "tasks": tasks,
                    "max_inflight": max_inflight,
                    "return_exceptions": return_exceptions,
                }
            )
            return [VLClipAnalysisResponse(success=True) for _ in tasks]

        async def analyze_clip(
            self,
            clip_path: str,
            semantic_unit_start_sec: float,
            semantic_unit_id: str,
            extra_prompt: str | None = None,
            analysis_mode: str = "default",
        ) -> VLClipAnalysisResponse:
            _ = (clip_path, semantic_unit_start_sec, semantic_unit_id, extra_prompt, analysis_mode)
            self.single_calls.append(semantic_unit_id)
            return VLClipAnalysisResponse(success=True)

    analyzer = _BatchAnalyzer()
    generator._analyzer = analyzer

    unit_tasks = [
        {
            "unit_id": "B1",
            "start_sec": 0.0,
            "end_sec": 10.0,
            "duration": 10.0,
            "clip_path": "b1.mp4",
            "analysis_mode": "default",
            "extra_prompt": None,
            "semantic_unit": {"unit_id": "B1"},
        },
        {
            "unit_id": "B2",
            "start_sec": 10.0,
            "end_sec": 20.0,
            "duration": 10.0,
            "clip_path": "b2.mp4",
            "analysis_mode": "default",
            "extra_prompt": None,
            "semantic_unit": {"unit_id": "B2"},
        },
    ]
    pre_prune_results = [
        {
            "applied": False,
            "clip_path_for_vl": "b1.mp4",
            "pre_context_prompt": "",
            "kept_segments": [(0.0, 10.0)],
        },
        {
            "applied": False,
            "clip_path_for_vl": "b2.mp4",
            "pre_context_prompt": "",
            "kept_segments": [(0.0, 10.0)],
        },
    ]

    analysis_results, task_metadata, pruned_units = asyncio.run(
        generator._analyze_unit_tasks_in_parallel(
            unit_tasks=unit_tasks,
            pre_prune_results=pre_prune_results,
        )
    )

    assert len(analysis_results) == 2
    assert len(task_metadata) == 2
    assert pruned_units == 0
    assert len(analyzer.batch_calls) == 1
    assert analyzer.batch_calls[0]["max_inflight"] == 2
    assert analyzer.batch_calls[0]["return_exceptions"] is True
    assert analyzer.single_calls == []


def test_analyze_unit_tasks_interleaves_concrete_and_process_dispatch(monkeypatch):
    generator = VLMaterialGenerator(
        {
            "enabled": True,
            "vl_analysis": {
                "parallel_workers": 1,
                "parallel_hard_cap": 1,
            },
            "screenshot_optimization": {"enabled": False},
            "fallback": {"enabled": True},
        }
    )

    class _BatchAnalyzer:
        def __init__(self):
            self.batch_calls = []

        async def analyze_clips_batch(
            self,
            *,
            tasks: list[dict[str, Any]],
            max_inflight: int | None = None,
            return_exceptions: bool = True,
        ) -> list[VLClipAnalysisResponse]:
            self.batch_calls.append(
                {
                    "tasks": tasks,
                    "max_inflight": max_inflight,
                    "return_exceptions": return_exceptions,
                }
            )
            return [
                VLClipAnalysisResponse(
                    success=True,
                    error_msg=str(task.get("semantic_unit_id", "") or ""),
                )
                for task in tasks
            ]

    analyzer = _BatchAnalyzer()
    generator._analyzer = analyzer

    unit_tasks = [
        {
            "unit_id": "P1",
            "start_sec": 0.0,
            "end_sec": 10.0,
            "duration": 10.0,
            "clip_path": "p1.mp4",
            "analysis_mode": "tutorial_stepwise",
            "extra_prompt": "p1",
            "semantic_unit": {"unit_id": "P1", "knowledge_type": "process"},
        },
        {
            "unit_id": "P2",
            "start_sec": 10.0,
            "end_sec": 20.0,
            "duration": 10.0,
            "clip_path": "p2.mp4",
            "analysis_mode": "tutorial_stepwise",
            "extra_prompt": "p2",
            "semantic_unit": {"unit_id": "P2", "knowledge_type": "process"},
        },
        {
            "unit_id": "C1",
            "start_sec": 20.0,
            "end_sec": 30.0,
            "duration": 10.0,
            "clip_path": "c1.mp4",
            "analysis_mode": "concrete",
            "extra_prompt": None,
            "semantic_unit": {"unit_id": "C1", "knowledge_type": "concrete"},
        },
        {
            "unit_id": "C2",
            "start_sec": 30.0,
            "end_sec": 40.0,
            "duration": 10.0,
            "clip_path": "c2.mp4",
            "analysis_mode": "concrete",
            "extra_prompt": None,
            "semantic_unit": {"unit_id": "C2", "knowledge_type": "concrete"},
        },
    ]
    pre_prune_results = [
        {
            "applied": False,
            "clip_path_for_vl": "p1.mp4",
            "pre_context_prompt": "",
            "kept_segments": [(0.0, 10.0)],
        },
        {
            "applied": False,
            "clip_path_for_vl": "p2.mp4",
            "pre_context_prompt": "",
            "kept_segments": [(0.0, 10.0)],
        },
        {
            "applied": False,
            "clip_path_for_vl": "c1.mp4",
            "pre_context_prompt": "",
            "kept_segments": [(0.0, 10.0)],
        },
        {
            "applied": False,
            "clip_path_for_vl": "c2.mp4",
            "pre_context_prompt": "",
            "kept_segments": [(0.0, 10.0)],
        },
    ]

    analysis_results, task_metadata, pruned_units = asyncio.run(
        generator._analyze_unit_tasks_in_parallel(
            unit_tasks=unit_tasks,
            pre_prune_results=pre_prune_results,
        )
    )

    assert pruned_units == 0
    assert len(analyzer.batch_calls) == 1
    dispatched_order = [
        str(task.get("semantic_unit_id", "") or "")
        for task in analyzer.batch_calls[0]["tasks"]
    ]
    assert dispatched_order == ["C1", "P1", "C2", "P2"]
    assert [str(meta.get("unit_id", "") or "") for meta in task_metadata] == ["P1", "P2", "C1", "C2"]
    assert [str(item.error_msg) for item in analysis_results] == ["P1", "P2", "C1", "C2"]


def test_analyze_unit_tasks_prefers_existing_pruned_clip(tmp_path):
    generator = VLMaterialGenerator(
        {
            "enabled": True,
            "vl_analysis": {
                "parallel_workers": 1,
                "parallel_hard_cap": 4,
            },
            "screenshot_optimization": {"enabled": False},
            "fallback": {"enabled": True},
        }
    )

    clips_dir = tmp_path / "semantic_unit_clips_vl"
    pruned_dir = clips_dir / "vl_pruned_clips"
    clips_dir.mkdir(parents=True, exist_ok=True)
    pruned_dir.mkdir(parents=True, exist_ok=True)

    original_clip = clips_dir / "SU101.mp4"
    pruned_clip = pruned_dir / "SU101_pruned.mp4"
    original_clip.write_bytes(b"original")
    pruned_clip.write_bytes(b"pruned")

    class _CountingAnalyzer:
        def __init__(self):
            self.calls = []

        async def analyze_clip(
            self,
            clip_path: str,
            semantic_unit_start_sec: float,
            semantic_unit_id: str,
            extra_prompt: str | None = None,
            analysis_mode: str = "default",
        ) -> VLClipAnalysisResponse:
            self.calls.append(
                {
                    "clip_path": clip_path,
                    "start": semantic_unit_start_sec,
                    "unit_id": semantic_unit_id,
                    "extra_prompt": extra_prompt,
                    "analysis_mode": analysis_mode,
                }
            )
            return VLClipAnalysisResponse(success=True)

    analyzer = _CountingAnalyzer()
    generator._analyzer = analyzer

    unit_tasks = [
        {
            "unit_id": "SU101",
            "start_sec": 0.0,
            "end_sec": 20.0,
            "duration": 20.0,
            "clip_path": str(original_clip),
            "analysis_mode": "tutorial_stepwise",
            "extra_prompt": None,
            "semantic_unit": {"unit_id": "SU101"},
        }
    ]
    pre_prune_results = [
        {
            "applied": False,
            "clip_path_for_vl": str(original_clip),
            "pre_context_prompt": "",
            "kept_segments": [(0.0, 20.0)],
        }
    ]

    _, task_metadata, _ = asyncio.run(
        generator._analyze_unit_tasks_in_parallel(
            unit_tasks=unit_tasks,
            pre_prune_results=pre_prune_results,
        )
    )

    assert len(analyzer.calls) == 1
    assert analyzer.calls[0]["clip_path"] == str(pruned_clip)
    assert task_metadata[0]["vl_clip_path"] == str(pruned_clip)


def test_save_tutorial_assets_uses_analysis_relative_timestamps(tmp_path, monkeypatch):
    generator = VLMaterialGenerator(
        {
            "enabled": True,
            "tutorial_mode": {
                "enabled": True,
                "export_assets": True,
                "save_step_json": False,
                "top_reason_banner_enabled": True,
                "asset_export_parallel_workers": 1,
                "asset_export_parallel_hard_cap": 1,
            },
            "screenshot_optimization": {"enabled": False},
            "fallback": {"enabled": True},
        }
    )

    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    analysis_clip_path = str(tmp_path / "analysis_source.mp4")

    clip_calls = []
    keyframe_calls = []

    async def _fake_export_clip_asset_with_ffmpeg(video_path, start_sec, end_sec, output_path):
        clip_calls.append((video_path, float(start_sec), float(end_sec)))
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"clip")
        return True

    async def _fake_export_keyframe_with_ffmpeg(video_path, timestamp_sec, output_path):
        keyframe_calls.append((video_path, float(timestamp_sec)))
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"key")
        return True

    monkeypatch.setattr(generator, "_export_clip_asset_with_ffmpeg", _fake_export_clip_asset_with_ffmpeg)
    monkeypatch.setattr(generator, "_export_keyframe_with_ffmpeg", _fake_export_keyframe_with_ffmpeg)

    clip_requests = [
        {
            "semantic_unit_id": "SU777",
            "analysis_mode": "tutorial_stepwise",
            "step_id": 1,
            "step_description": "step one",
            "action_brief": "step_one",
            "start_sec": 100.0,
            "end_sec": 120.0,
            "_analysis_relative_start_sec": 1.0,
            "_analysis_relative_end_sec": 3.0,
        }
    ]
    screenshot_requests = [
        {
            "semantic_unit_id": "SU777",
            "analysis_mode": "tutorial_stepwise",
            "step_id": 1,
            "timestamp_sec": 110.0,
            "_relative_timestamp": 10.0,
            "_analysis_relative_timestamp": 2.0,
        }
    ]

    asyncio.run(
        generator._save_tutorial_assets_for_unit(
            video_path=analysis_clip_path,
            output_dir=str(output_dir),
            unit_id="SU777",
            clip_requests=clip_requests,
            screenshot_requests=screenshot_requests,
            raw_response_json=[],
            use_analysis_relative_timestamps=True,
        )
    )

    assert len(clip_calls) == 1
    assert clip_calls[0][0] == analysis_clip_path
    assert clip_calls[0][1] == 1.0
    assert clip_calls[0][2] == 3.0

    assert len(keyframe_calls) == 1
    assert keyframe_calls[0][0] == analysis_clip_path
    assert keyframe_calls[0][1] == 2.0


def test_save_tutorial_assets_prefers_mapped_screenshot_timestamps_for_keyframes(tmp_path, monkeypatch):
    generator = VLMaterialGenerator(
        {
            "enabled": True,
            "tutorial_mode": {
                "enabled": True,
                "export_assets": True,
                "save_step_json": False,
                "asset_export_parallel_workers": 1,
                "asset_export_parallel_hard_cap": 1,
            },
            "screenshot_optimization": {"enabled": False},
            "fallback": {"enabled": True},
        }
    )

    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    original_clip_path = str(tmp_path / "original_source.mp4")

    clip_calls = []
    keyframe_calls = []

    async def _fake_export_clip_asset_with_ffmpeg(video_path, start_sec, end_sec, output_path):
        clip_calls.append((video_path, float(start_sec), float(end_sec)))
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"clip")
        return True

    async def _fake_export_keyframe_with_ffmpeg(video_path, timestamp_sec, output_path):
        keyframe_calls.append((video_path, float(timestamp_sec)))
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"key")
        return True

    monkeypatch.setattr(generator, "_export_clip_asset_with_ffmpeg", _fake_export_clip_asset_with_ffmpeg)
    monkeypatch.setattr(generator, "_export_keyframe_with_ffmpeg", _fake_export_keyframe_with_ffmpeg)

    clip_requests = [
        {
            "semantic_unit_id": "SU778",
            "analysis_mode": "tutorial_stepwise",
            "step_id": 1,
            "step_description": "step one",
            "action_brief": "step_one",
            "start_sec": 100.0,
            "end_sec": 120.0,
            "_analysis_relative_start_sec": 1.0,
            "_analysis_relative_end_sec": 3.0,
        }
    ]
    screenshot_requests = [
        {
            "semantic_unit_id": "SU778",
            "analysis_mode": "tutorial_stepwise",
            "step_id": 1,
            "timestamp_sec": 110.0,
            "_relative_timestamp": 10.0,
            "_analysis_relative_timestamp": 2.0,
            "bbox": [100, 200, 400, 300],
        }
    ]
    raw_response_json = [
        {
            "step_id": 1,
            "step_description": "step one",
            "clip_start_sec": 1.0,
            "clip_end_sec": 3.0,
            "instructional_keyframes": [
                {
                    "timestamp_sec": 2.0,
                    "bbox": [100, 200, 400, 300],
                }
            ],
        }
    ]

    asyncio.run(
        generator._save_tutorial_assets_for_unit(
            video_path=original_clip_path,
            output_dir=str(output_dir),
            unit_id="SU778",
            clip_requests=clip_requests,
            screenshot_requests=screenshot_requests,
            raw_response_json=raw_response_json,
            use_analysis_relative_timestamps=False,
            prefer_screenshot_requests_keyframes=True,
        )
    )

    assert len(clip_calls) == 1
    assert clip_calls[0][0] == original_clip_path
    assert clip_calls[0][1] == 100.0
    assert clip_calls[0][2] == 120.0

    assert len(keyframe_calls) == 1
    assert keyframe_calls[0][0] == original_clip_path
    assert keyframe_calls[0][1] == 110.0


def test_save_tutorial_assets_exports_instructional_clips_manifest(tmp_path, monkeypatch):
    generator = VLMaterialGenerator(
        {
            "enabled": True,
            "tutorial_mode": {
                "enabled": True,
                "export_assets": True,
                "save_step_json": True,
                "asset_export_parallel_workers": 1,
                "asset_export_parallel_hard_cap": 1,
            },
            "screenshot_optimization": {"enabled": False},
            "fallback": {"enabled": True},
        }
    )

    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    demo_video = tmp_path / "demo.mp4"
    demo_video.write_bytes(b"video")

    async def _fake_export_clip_asset_with_ffmpeg(video_path, start_sec, end_sec, output_path):
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(f"{start_sec}-{end_sec}".encode("utf-8"))
        return True

    async def _fake_export_keyframe_with_ffmpeg(video_path, timestamp_sec, output_path):
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"keyframe")
        return True

    monkeypatch.setattr(generator, "_export_clip_asset_with_ffmpeg", _fake_export_clip_asset_with_ffmpeg)
    monkeypatch.setattr(generator, "_export_keyframe_with_ffmpeg", _fake_export_keyframe_with_ffmpeg)

    clip_requests = [
        {
            "semantic_unit_id": "SU780",
            "analysis_mode": "tutorial_stepwise",
            "step_id": 1,
            "step_description": "step one",
            "action_brief": "step_one",
            "clip_id": "SU780/SU780_clip_step_01_step_one",
            "start_sec": 10.0,
            "end_sec": 20.0,
        },
        {
            "semantic_unit_id": "SU780",
            "analysis_mode": "tutorial_stepwise",
            "step_id": 1,
            "step_description": "step one",
            "action_brief": "step_one",
            "start_sec": 14.0,
            "end_sec": 16.0,
            "instructional_clip_id": "CLIP_1",
            "clip_id": "SU780/SU780_clip_step_01_clip_01_step_one",
            "clip_reason": "watch the animation",
            "_analysis_relative_start_sec": 4.0,
            "_analysis_relative_end_sec": 6.0,
        },
    ]
    screenshot_requests = [
        {
            "semantic_unit_id": "SU780",
            "analysis_mode": "tutorial_stepwise",
            "step_id": 1,
            "timestamp_sec": 15.0,
            "frame_reason": "state visible",
        }
    ]
    raw_response_json = [
        {
            "step_id": 1,
            "step_description": "step one",
            "step_type": "MAIN_FLOW",
            "main_action": "open panel",
            "main_operation": "1. open panel\n[KEYFRAME_1]\n[CLIP_1]",
            "clip_start_sec": 0.0,
            "clip_end_sec": 10.0,
            "instructional_keyframes": [{"timestamp_sec": 5.0, "frame_reason": "state visible"}],
            "instructional_clips": [{"clip_id": "CLIP_1", "start_sec": 4.0, "end_sec": 6.0, "clip_reason": "watch the animation"}],
        }
    ]

    asyncio.run(
        generator._save_tutorial_assets_for_unit(
            video_path=str(demo_video),
            output_dir=str(output_dir),
            unit_id="SU780",
            clip_requests=clip_requests,
            screenshot_requests=screenshot_requests,
            raw_response_json=raw_response_json,
            use_analysis_relative_timestamps=True,
            prefer_screenshot_requests_keyframes=False,
        )
    )

    unit_dir = output_dir / "vl_tutorial_units" / "SU780"
    data = json.loads((unit_dir / "SU780_steps.json").read_text(encoding="utf-8"))
    assert data["steps"][0]["instructional_clips"] == ["SU780_clip_step_01_clip_01_step_one.mp4"]
    assert data["steps"][0]["instructional_clip_details"][0]["instructional_clip_id"] == "CLIP_1"
    assert data["steps"][0]["instructional_clip_details"][0]["clip_reason"] == "watch the animation"

def test_save_tutorial_assets_applies_top_reason_banner_to_keyframe_image(tmp_path, monkeypatch):
    generator = VLMaterialGenerator(
        {
            "enabled": True,
            "tutorial_mode": {
                "enabled": True,
                "export_assets": True,
                "save_step_json": False,
                "asset_export_parallel_workers": 1,
                "asset_export_parallel_hard_cap": 1,
            },
            "screenshot_optimization": {"enabled": False},
            "fallback": {"enabled": True},
        }
    )

    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    async def _fake_export_clip_asset_with_ffmpeg(video_path, start_sec, end_sec, output_path):
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"clip")
        return True

    async def _fake_export_keyframe_with_ffmpeg(video_path, timestamp_sec, output_path):
        output_path.parent.mkdir(parents=True, exist_ok=True)
        image = np.full((1080, 1920, 3), 235, dtype=np.uint8)
        assert cv2.imwrite(str(output_path), image)
        return True

    monkeypatch.setattr(generator, "_export_clip_asset_with_ffmpeg", _fake_export_clip_asset_with_ffmpeg)
    monkeypatch.setattr(generator, "_export_keyframe_with_ffmpeg", _fake_export_keyframe_with_ffmpeg)

    clip_requests = [
        {
            "semantic_unit_id": "SU779",
            "analysis_mode": "tutorial_stepwise",
            "step_id": 1,
            "step_description": "step one",
            "action_brief": "step_one",
            "start_sec": 10.0,
            "end_sec": 20.0,
        }
    ]
    screenshot_requests = [
        {
            "semantic_unit_id": "SU779",
            "analysis_mode": "tutorial_stepwise",
            "step_id": 1,
            "timestamp_sec": 15.0,
            "frame_reason": "大家请看画面顶部的设置区域，这里展示了需要重点核对的关键状态。",
        }
    ]
    raw_response_json = [
        {
            "step_id": 1,
            "step_description": "step one",
            "step_type": "MAIN_FLOW",
            "main_action": "open panel",
            "main_operation": "1. open panel\n[KEYFRAME_1]",
            "clip_start_sec": 10.0,
            "clip_end_sec": 20.0,
            "instructional_keyframes": [
                {
                    "timestamp_sec": 15.0,
                    "frame_reason": "大家请看画面顶部的设置区域，这里展示了需要重点核对的关键状态。",
                }
            ],
        }
    ]

    asyncio.run(
        generator._save_tutorial_assets_for_unit(
            video_path="dummy.mp4",
            output_dir=str(output_dir),
            unit_id="SU779",
            clip_requests=clip_requests,
            screenshot_requests=screenshot_requests,
            raw_response_json=raw_response_json,
        )
    )

    unit_asset_dir = output_dir / "vl_tutorial_units" / "SU779"
    matched_images = list(unit_asset_dir.glob("SU779_ss_step_01_key_01_*"))
    assert len(matched_images) == 1
    rendered = cv2.imread(str(matched_images[0]), cv2.IMREAD_COLOR)
    assert rendered is not None
    top_sample = rendered[40:170, 100:1820]
    assert top_sample.size > 0
    assert int(np.mean(top_sample)) < 235


def test_save_tutorial_assets_skips_top_reason_banner_when_disabled(tmp_path, monkeypatch):
    generator = VLMaterialGenerator(
        {
            "enabled": True,
            "tutorial_mode": {
                "enabled": True,
                "export_assets": True,
                "save_step_json": False,
                "top_reason_banner_enabled": False,
                "asset_export_parallel_workers": 1,
                "asset_export_parallel_hard_cap": 1,
            },
            "screenshot_optimization": {"enabled": False},
            "fallback": {"enabled": True},
        }
    )

    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    async def _fake_export_clip_asset_with_ffmpeg(video_path, start_sec, end_sec, output_path):
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"clip")
        return True

    async def _fake_export_keyframe_with_ffmpeg(video_path, timestamp_sec, output_path):
        output_path.parent.mkdir(parents=True, exist_ok=True)
        image = np.full((1080, 1920, 3), 235, dtype=np.uint8)
        assert cv2.imwrite(str(output_path), image)
        return True

    monkeypatch.setattr(generator, "_export_clip_asset_with_ffmpeg", _fake_export_clip_asset_with_ffmpeg)
    monkeypatch.setattr(generator, "_export_keyframe_with_ffmpeg", _fake_export_keyframe_with_ffmpeg)

    clip_requests = [{
        "semantic_unit_id": "SU780",
        "analysis_mode": "tutorial_stepwise",
        "step_id": 1,
        "step_description": "step one",
        "action_brief": "step_one",
        "start_sec": 10.0,
        "end_sec": 20.0,
    }]
    screenshot_requests = [{
        "semantic_unit_id": "SU780",
        "analysis_mode": "tutorial_stepwise",
        "step_id": 1,
        "timestamp_sec": 15.0,
        "frame_reason": "大家请看画面顶部的设置区域，这里展示了需要重点核对的关键状态。",
    }]
    raw_response_json = [{
        "step_id": 1,
        "step_description": "step one",
        "step_type": "MAIN_FLOW",
        "main_action": "open panel",
        "main_operation": "1. open panel\n[KEYFRAME_1]",
        "clip_start_sec": 10.0,
        "clip_end_sec": 20.0,
        "instructional_keyframes": [{
            "timestamp_sec": 15.0,
            "frame_reason": "大家请看画面顶部的设置区域，这里展示了需要重点核对的关键状态。",
        }],
    }]

    asyncio.run(
        generator._save_tutorial_assets_for_unit(
            video_path="dummy.mp4",
            output_dir=str(output_dir),
            unit_id="SU780",
            clip_requests=clip_requests,
            screenshot_requests=screenshot_requests,
            raw_response_json=raw_response_json,
        )
    )

    unit_asset_dir = output_dir / "vl_tutorial_units" / "SU780"
    matched_images = list(unit_asset_dir.glob("SU780_ss_step_01_key_01_*"))
    assert len(matched_images) == 1
    rendered = cv2.imread(str(matched_images[0]), cv2.IMREAD_COLOR)
    assert rendered is not None
    top_sample = rendered[40:170, 100:1820]
    assert top_sample.size > 0
    assert int(np.mean(top_sample)) == 235

    data = json.loads((unit_asset_dir / "SU780_steps.json").read_text(encoding="utf-8")) if (unit_asset_dir / "SU780_steps.json").exists() else {"steps": []}
    if data.get("steps"):
        assert data["steps"][0]["instructional_keyframe_details"][0]["top_reason_banner_status"] == "disabled_by_config"


def test_apply_grid_anchor_crop_for_keyframe(tmp_path, monkeypatch):
    generator = VLMaterialGenerator(
        {
            "enabled": True,
            "tutorial_mode": {
                "enabled": True,
                "grid_anchor_enabled": True,
                "grid_rows": 20,
                "grid_cols": 20,
                "grid_overlay_alpha": 0.4,
                "grid_overlay_line_thickness": 1,
                "grid_crop_expand_ratio": 0.15,
                "grid_crop_min_border_px": 6,
            },
            "screenshot_optimization": {"enabled": False},
            "fallback": {"enabled": True},
        }
    )

    image_path = tmp_path / "frame.png"
    image = np.full((200, 300, 3), 160, dtype=np.uint8)
    assert cv2.imwrite(str(image_path), image)

    async def _fake_vision_validate_image(**kwargs):
        assert kwargs.get("image_path", "").endswith("_grid_overlay.png")
        return {
            "visual_verification": "located target area",
            "grid_start": "C4",
            "grid_end": "E7",
        }

    monkeypatch.setattr(
        vl_material_generator_module.llm_gateway,
        "vision_validate_image",
        _fake_vision_validate_image,
    )

    meta = asyncio.run(
        generator._apply_grid_anchor_crop_for_keyframe(
            keyframe_path=image_path,
        )
    )

    assert meta["grid_anchor_status"] == "ok"
    assert meta["grid_start"] == "C4"
    assert meta["grid_end"] == "E7"
    assert meta["grid_overlay_file"] == "frame_grid_overlay.png"
    assert (tmp_path / "frame_grid_overlay.png").exists()

    cropped = cv2.imread(str(image_path), cv2.IMREAD_COLOR)
    assert cropped is not None
    assert int(cropped.shape[0]) < 200
    assert int(cropped.shape[1]) < 300


def test_tutorial_assets_export_uses_parallel_limited_workers(tmp_path, monkeypatch):
    """
    验证教程资产导出采用“并行 + 并发上限”调度。
    场景：2 个 step，每个 step 1 个 clip + 1 个 keyframe，共 4 个导出任务。
    配置并发上限=2，期望观测到峰值并发为 2（而不是串行 1 或无限并发 4）。
    """
    generator = VLMaterialGenerator(
        {
            "enabled": True,
            "tutorial_mode": {
                "enabled": True,
                "export_assets": True,
                "save_step_json": False,
                "asset_export_parallel_workers": 2,
                "asset_export_parallel_hard_cap": 2,
            },
            "screenshot_optimization": {"enabled": False},
            "fallback": {"enabled": True},
        }
    )

    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    clip_requests = [
        {
            "semantic_unit_id": "SU900",
            "analysis_mode": "tutorial_stepwise",
            "step_id": 1,
            "step_description": "step one",
            "action_brief": "step_one",
            "start_sec": 1.0,
            "end_sec": 3.0,
        },
        {
            "semantic_unit_id": "SU900",
            "analysis_mode": "tutorial_stepwise",
            "step_id": 2,
            "step_description": "step two",
            "action_brief": "step_two",
            "start_sec": 3.0,
            "end_sec": 5.0,
        },
    ]
    screenshot_requests = [
        {
            "semantic_unit_id": "SU900",
            "analysis_mode": "tutorial_stepwise",
            "step_id": 1,
            "timestamp_sec": 2.5,
        },
        {
            "semantic_unit_id": "SU900",
            "analysis_mode": "tutorial_stepwise",
            "step_id": 2,
            "timestamp_sec": 4.5,
        },
    ]

    state = {"running": 0, "peak": 0}

    async def _fake_export_clip_asset_with_ffmpeg(video_path, start_sec, end_sec, output_path):
        state["running"] += 1
        state["peak"] = max(state["peak"], state["running"])
        await asyncio.sleep(0.03)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"clip")
        state["running"] -= 1
        return True

    async def _fake_export_keyframe_with_ffmpeg(video_path, timestamp_sec, output_path):
        state["running"] += 1
        state["peak"] = max(state["peak"], state["running"])
        await asyncio.sleep(0.03)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"key")
        state["running"] -= 1
        return True

    monkeypatch.setattr(generator, "_export_clip_asset_with_ffmpeg", _fake_export_clip_asset_with_ffmpeg)
    monkeypatch.setattr(generator, "_export_keyframe_with_ffmpeg", _fake_export_keyframe_with_ffmpeg)

    t0 = time.perf_counter()
    asyncio.run(
        generator._save_tutorial_assets_for_unit(
            video_path="dummy.mp4",
            output_dir=str(output_dir),
            unit_id="SU900",
            clip_requests=clip_requests,
            screenshot_requests=screenshot_requests,
            raw_response_json=[],
        )
    )
    elapsed = time.perf_counter() - t0

    # 关键断言：并发峰值受限于 2（不是串行 1，也不会无限增长）
    assert state["peak"] == 2

    # 额外健壮性断言：导出确实执行且有并行收益（4*0.03 串行约 0.12s，这里应明显小于串行）
    assert elapsed < 0.12


def test_apply_screenshot_optimization_with_bypass_only_skips_explicit_requests(monkeypatch):
    config = _build_generator_config()
    config["screenshot_optimization"]["enabled"] = True
    config["screenshot_optimization"]["best_frame_vision_select_enabled"] = False
    generator = VLMaterialGenerator(config)

    captured: Dict[str, Any] = {"requests": []}

    async def _fake_optimize(video_path: str, screenshot_requests: list[Dict[str, Any]]):
        captured["video_path"] = video_path
        captured["requests"] = [dict(item) for item in screenshot_requests]
        optimized: list[Dict[str, Any]] = []
        for item in screenshot_requests:
            updated = dict(item)
            updated["_optimized_by_cv"] = True
            updated["timestamp_sec"] = float(updated.get("timestamp_sec", 0.0)) + 0.25
            optimized.append(updated)
        return optimized

    monkeypatch.setattr(generator, "_optimize_screenshots_parallel", _fake_optimize)

    screenshot_requests = [
        {
            "screenshot_id": "SU001/SU001_ss_concrete_seg_01_key_01",
            "analysis_mode": "concrete",
            "timestamp_sec": 1.0,
        },
        {
            "screenshot_id": "SU002/SU002_ss_vl_01_01",
            "analysis_mode": "default",
            "timestamp_sec": 2.0,
        },
        {
            "screenshot_id": "SU003/SU003_ss_vl_01_01",
            "analysis_mode": "default",
            "knowledge_type": "concrete",
            "timestamp_sec": 3.0,
        },
        {
            "screenshot_id": "SU004/SU004_ss_vl_01_01",
            "analysis_mode": "default",
            "_skip_cv_optimization": True,
            "timestamp_sec": 4.0,
        },
    ]

    merged = asyncio.run(
        generator._apply_screenshot_optimization_with_bypass(
            video_path="dummy.mp4",
            screenshot_requests=[dict(item) for item in screenshot_requests],
        )
    )

    assert captured["video_path"] == "dummy.mp4"
    assert len(captured["requests"]) == 3
    assert {item["screenshot_id"] for item in captured["requests"]} == {
        "SU001/SU001_ss_concrete_seg_01_key_01",
        "SU002/SU002_ss_vl_01_01",
        "SU003/SU003_ss_vl_01_01",
    }

    assert len(merged) == 4
    assert merged[0]["timestamp_sec"] == 1.25
    assert merged[0]["_optimized_by_cv"] is True
    assert merged[1]["timestamp_sec"] == 2.25
    assert merged[1]["_optimized_by_cv"] is True
    assert merged[2]["timestamp_sec"] == 3.25
    assert merged[2]["_optimized_by_cv"] is True
    assert merged[3]["timestamp_sec"] == 4.0
    assert "_optimized_by_cv" not in merged[3]


def test_optimize_screenshots_parallel_uses_iframe_only_mode_when_forced(monkeypatch):
    from services.python_grpc.src.content_pipeline.phase2a.materials import vl_material_generator as generator_module

    config = _build_generator_config()
    config["screenshot_optimization"]["iframe_only_mode"] = True
    generator = VLMaterialGenerator(config)

    async def _fake_probe_iframe_timestamps(**kwargs):
        assert kwargs["video_path"] == "dummy.mp4"
        assert abs(float(kwargs["target_timestamp_sec"]) - 10.0) < 1e-6
        return [9.8, 10.2]

    async def _fail_if_task_gate(**kwargs):
        raise AssertionError("task gate path should be bypassed in iframe-only mode")

    monkeypatch.setattr(generator_module, "_probe_iframe_timestamps", _fake_probe_iframe_timestamps)
    monkeypatch.setattr(generator, "_run_screenshot_optimization_with_task_gate", _fail_if_task_gate)

    results = asyncio.run(
        generator._optimize_screenshots_parallel(
            "dummy.mp4",
            [
                {
                    "screenshot_id": "SU010/SU010_ss_vl_01_01",
                    "timestamp_sec": 10.0,
                    "_window_start_sec": 10.0,
                    "_window_end_sec": 10.4,
                }
            ],
        )
    )

    assert len(results) == 1
    assert abs(float(results[0]["timestamp_sec"]) - 10.2) < 1e-6
    assert results[0]["_skip_cv_optimization"] is True
    assert results[0]["_iframe_only_selected"] is True
    assert results[0]["_iframe_only_reason"] == "forced_by_config"


def test_annotate_screenshot_requests_with_unit_context_does_not_force_skip():
    generator = VLMaterialGenerator(_build_generator_config())
    requests = [
        {
            "screenshot_id": "SU900/SU900_ss_vl_01_01",
            "timestamp_sec": 12.34,
        }
    ]

    generator._annotate_screenshot_requests_with_unit_context(
        screenshot_requests=requests,
        semantic_unit={"knowledge_type": "concrete"},
        analysis_mode="default",
    )

    assert requests[0]["analysis_mode"] == "default"
    assert requests[0]["knowledge_type"] == "concrete"
    assert "_skip_cv_optimization" not in requests[0]


def test_annotate_screenshot_requests_with_unit_context_sets_concrete_window_start_to_timestamp():
    config = _build_generator_config()
    config["screenshot_optimization"].update(
        {
            "concrete_forward_search_after_seconds": 2.0,
            "concrete_prefetch_chunk_max_span_seconds": 3.0,
            "concrete_prefetch_chunk_max_requests": 64,
            "concrete_prefetch_sample_rate": 3,
            "concrete_prefetch_target_height": 240,
        }
    )
    generator = VLMaterialGenerator(config)
    requests = [
        {
            "screenshot_id": "SU901/SU901_ss_concrete_seg_01_key_01",
            "timestamp_sec": 12.34,
        }
    ]

    generator._annotate_screenshot_requests_with_unit_context(
        screenshot_requests=requests,
        semantic_unit={"knowledge_type": "concrete"},
        analysis_mode="concrete",
    )

    assert requests[0]["analysis_mode"] == "concrete"
    assert requests[0]["knowledge_type"] == "concrete"
    assert requests[0]["_window_start_sec"] == 12.34
    assert requests[0]["_window_end_sec"] == 14.34
    assert requests[0]["_cv_prefetch_profile"] == "concrete_forward"
    assert requests[0]["_prefetch_chunk_max_span_seconds"] == 3.0
    assert requests[0]["_prefetch_chunk_max_requests"] == 64
    assert requests[0]["_prefetch_sample_rate"] == 3
    assert requests[0]["_prefetch_target_height"] == 240


def test_build_screenshot_prefetch_chunks_respects_concrete_profile_overrides():
    requests = [
        {
            "screenshot_id": "SU901/SU901_ss_concrete_seg_01_key_01",
            "semantic_unit_id": "SU901",
            "timestamp_sec": 12.34,
            "_window_start_sec": 12.34,
            "_window_end_sec": 14.34,
            "_cv_prefetch_profile": "concrete_forward",
            "_prefetch_chunk_max_span_seconds": 3.0,
            "_prefetch_chunk_max_requests": 64,
            "_prefetch_sample_rate": 3,
            "_prefetch_target_height": 240,
        },
        {
            "screenshot_id": "SU999/SU999_ss_vl_01_01",
            "semantic_unit_id": "SU999",
            "timestamp_sec": 12.80,
        },
    ]

    chunks = build_screenshot_prefetch_chunks(
        screenshot_requests=requests,
        max_span_seconds=8.0,
        max_requests=256,
        time_window_before=1.0,
        time_window_after=2.0,
    )

    assert len(chunks) == 2
    assert chunks[0]["prefetch_profile"] == "concrete_forward"
    assert chunks[0]["prefetch_sample_rate"] == 3
    assert chunks[0]["prefetch_target_height"] == 240
    assert chunks[0]["max_chunk_span_seconds"] == 3.0
    assert chunks[1]["prefetch_profile"] == "default"


def test_is_tutorial_process_unit_no_longer_depends_on_duration_threshold():
    generator = VLMaterialGenerator(_build_generator_config())

    assert generator._is_tutorial_process_unit(
        {"knowledge_type": "process", "mult_steps": True},
        duration_sec=5.0,
    ) is True


def test_should_merge_multistep_unit_no_longer_depends_on_duration_threshold():
    generator = VLMaterialGenerator(_build_generator_config())

    assert generator._should_merge_multistep_unit(
        {"knowledge_type": "process", "mult_steps": True, "start_sec": 0.0, "end_sec": 5.0}
    ) is True


def test_resolve_vl_parallel_workers_auto_is_conservative(monkeypatch):
    config = _build_generator_config()
    config["vl_analysis"] = {
        "parallel_workers": "auto",
        "parallel_hard_cap": 32,
    }
    generator = VLMaterialGenerator(config)

    monkeypatch.setattr(vl_material_generator_module.os, "cpu_count", lambda: 16)

    assert generator._resolve_vl_parallel_workers(30) == 4


def test_resolve_stream_split_process_workers_auto_is_conservative(monkeypatch):
    config = _build_generator_config()
    config["vl_analysis"] = {
        "stream_split_process_workers": "auto",
        "stream_split_process_hard_cap": 8,
    }
    generator = VLMaterialGenerator(config)

    monkeypatch.setattr(vl_material_generator_module.os, "cpu_count", lambda: 16)

    assert generator._resolve_stream_split_process_workers(30) == 2


def test_create_stream_split_executor_uses_thread_pool():
    generator = VLMaterialGenerator(_build_generator_config())

    executor = generator._create_stream_split_executor(worker_count=2)
    try:
        assert isinstance(executor, ThreadPoolExecutor)
        assert getattr(executor, "_max_workers", None) == 2
    finally:
        executor.shutdown(wait=True)


def test_should_use_threaded_cv_executor_for_windows_concrete_forward(monkeypatch):
    monkeypatch.setattr(flow_ops_module.os, "name", "nt", raising=False)
    generator = SimpleNamespace(screenshot_config={})

    assert flow_ops_module._should_use_threaded_cv_executor(
        generator,
        [{"_cv_prefetch_profile": "concrete_forward"}],
    ) is True
    assert flow_ops_module._should_use_threaded_cv_executor(
        generator,
        [{"_cv_prefetch_profile": "default"}],
    ) is False


def test_apply_selection_result_persists_candidate_screenshots():
    generator = VLMaterialGenerator(_build_generator_config())
    request = {"screenshot_id": "SU700/SU700_ss_vl_01_01", "timestamp_sec": 10.0}

    generator._apply_selection_result(
        req=request,
        original_ts=10.0,
        unit_id="SU700",
        result={
            "selected_timestamp": 10.4,
            "quality_score": 0.81,
            "static_island_threshold_ms": 200.0,
            "candidate_screenshots": [
                {"timestamp_sec": 10.4, "score": 0.81, "island_index": 1},
                {"timestamp_sec": 10.1, "score": 0.72, "island_index": 0},
            ],
        },
    )

    assert request["timestamp_sec"] == 10.4
    assert request["_cv_quality_score"] == 0.81
    assert request["_cv_static_island_threshold_ms"] == 200.0
    assert len(request["_cv_candidate_screenshots"]) == 2
    assert request["_cv_candidate_screenshots"][0]["timestamp_sec"] == 10.4


def test_apply_best_frame_vision_selection_collapses_candidates(monkeypatch):
    config = _build_generator_config()
    config["screenshot_optimization"]["enabled"] = True
    config["screenshot_optimization"]["best_frame_vision_select_enabled"] = True
    generator = VLMaterialGenerator(config)

    candidates = [
        {"timestamp_sec": 20.10, "score": 0.62, "island_index": 0},
        {"timestamp_sec": 20.35, "score": 0.58, "island_index": 1},
        {"timestamp_sec": 20.60, "score": 0.55, "island_index": 2},
    ]

    async def _fake_pick(**kwargs):
        return dict(candidates[1]), "ai"

    monkeypatch.setattr(generator, "_select_best_frame_candidate_with_vision", _fake_pick)

    requests = [
        {
            "screenshot_id": "SU710/SU710_ss_vl_01_01",
            "timestamp_sec": 20.10,
            "_cv_quality_score": 0.62,
            "_cv_candidate_screenshots": [dict(item) for item in candidates],
            "frame_reason": "点击设置按钮",
        }
    ]

    updated = asyncio.run(
        generator._apply_best_frame_vision_selection(
            video_path="dummy.mp4",
            screenshot_requests=requests,
        )
    )

    assert len(updated) == 1
    assert updated[0]["timestamp_sec"] == 20.35
    assert updated[0]["_cv_quality_score"] == 0.58
    assert updated[0]["_cv_vision_selection_source"] == "ai"
    assert len(updated[0]["_cv_candidate_screenshots"]) == 1
    assert updated[0]["_cv_candidate_screenshots"][0]["timestamp_sec"] == 20.35


def test_write_phase2a_token_cost_audit_writes_records(tmp_path):
    generator = VLMaterialGenerator(_build_generator_config())
    unit_analysis_outputs = [
        {
            "unit_id": "SU_AUDIT",
            "analysis_mode": "concrete",
            "raw_llm_interactions": [
                {
                    "stage": "vl_video_analysis",
                    "attempt": 1,
                    "success": True,
                    "timestamp_utc": "2026-03-07T01:00:00+00:00",
                    "request": {
                        "model": "qwen-vl-max-latest",
                        "analysis_mode": "concrete",
                        "video_path": "demo.mp4",
                        "timeout_sec": 30.0,
                        "hedge_delay_ms": 0,
                    },
                    "response": {
                        "model": "qwen-vl-max-latest",
                        "cache_hit": False,
                        "finish_reason": "stop",
                        "usage": {
                            "prompt_tokens": 1000,
                            "completion_tokens": 500,
                            "total_tokens": 1500,
                        },
                    },
                }
            ],
        }
    ]

    audit_path = generator._write_phase2a_token_cost_audit(
        output_dir=str(tmp_path),
        token_stats={"total_tokens_actual": 1500},
        unit_analysis_outputs=unit_analysis_outputs,
        video_path="demo.mp4",
    )

    payload = json.loads(Path(audit_path).read_text(encoding="utf-8"))
    assert payload["scene"] == "phase2a_vl"
    assert payload["summary"]["total_records"] == 1
    assert payload["summary"]["priced_records"] == 1
    assert payload["records"][0]["unit_id"] == "SU_AUDIT"
    assert payload["records"][0]["cost_estimate"]["status"] == "ok"
    assert payload["records"][0]["cost_estimate"]["currency"] == "CNY"


def test_select_best_frame_candidate_with_vision_invalid_response_falls_back(monkeypatch):
    config = _build_generator_config()
    config["screenshot_optimization"]["enabled"] = True
    config["screenshot_optimization"]["best_frame_vision_select_enabled"] = True
    generator = VLMaterialGenerator(config)
    generator._analyzer = SimpleNamespace(client=object(), model="test-vl-model")

    candidates = [
        {"timestamp_sec": 8.2, "score": 0.90, "island_index": 0},
        {"timestamp_sec": 8.6, "score": 0.75, "island_index": 1},
    ]

    monkeypatch.setattr(
        generator,
        "_build_candidate_images_for_vision_selection",
        lambda **kwargs: [
            {"image_id": "image_1", "candidate": dict(candidates[0]), "data_uri": "data:image/jpeg;base64,AA=="},
            {"image_id": "image_2", "candidate": dict(candidates[1]), "data_uri": "data:image/jpeg;base64,AA=="},
        ],
    )

    async def _fake_vl_chat_completion(**_kwargs):
        return llm_gateway.VLChatResult(
            content="invalid_choice_text",
            finish_reason="stop",
            usage={},
            model="test-vl-model",
        )

    monkeypatch.setattr(llm_gateway, "vl_chat_completion", _fake_vl_chat_completion)

    selected, source = asyncio.run(
        generator._select_best_frame_candidate_with_vision(
            video_path="dummy.mp4",
            request={"frame_reason": "显示设置页面"},
            candidates=[dict(item) for item in candidates],
        )
    )

    assert source == "fallback_cv_top"
    assert selected["timestamp_sec"] == 8.2
