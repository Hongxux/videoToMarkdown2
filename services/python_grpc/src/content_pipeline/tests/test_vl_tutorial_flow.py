"""
VL 鏁欑▼妯″紡娴嬭瘯锛?
1) 瑙ｆ瀽 step schema 鏄惁姝ｇ‘
2) 鏍￠獙澶氭楠?process 鍦?Phase2B 鍓嶇殑浜х墿瀵煎嚭锛坢ock VL + mock ffmpeg锛?
"""

from __future__ import annotations

import asyncio
import json
import shutil
from pathlib import Path
from typing import Any, Dict
import time
import cv2
import numpy as np

from concurrent.futures import ProcessPoolExecutor


from services.python_grpc.src.content_pipeline.phase2a.materials.vl_material_generator import VLMaterialGenerator
import services.python_grpc.src.content_pipeline.phase2a.materials.vl_material_generator as vl_material_generator_module
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


def test_tutorial_schema_parse_and_normalize():
    analyzer = VLVideoAnalyzer(_build_analyzer_config())
    payload = [
        {
            "step_id": 1,
            "step_description": "Open settings",
            "main_action": "Open settings panel",
            "main_operation": "1. Click the settings icon\n2. Enter network settings\n[KEYFRAME_1]",
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
    assert results[0].main_operation == ["1. Click the settings icon\n2. Enter network settings\n[KEYFRAME_1]"]
    assert results[0].instructional_keyframes == [
        {
            "timestamp_sec": 6.2,
            "frame_reason": "settings page visible",
            "target_ui_type": "menu_item",
            "target_text": "Settings",
            "target_relative_position": "top-left in toolbar",
            "bbox": [120, 80, 760, 920],
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
            "target_ui_type": "menu_item",
            "target_text": "Settings",
            "target_relative_position": "top-left in toolbar",
            "bbox": [120, 80, 760, 920],
        }
    ]
    assert normalized[0]["step_type"] == "MAIN_FLOW"
    assert set(normalized[0].keys()) == {
        "step_id",
        "step_description",
        "main_action",
        "main_operation",
        "instructional_keyframes",
        "precautions",
        "step_summary",
        "operation_guidance",
        "no_needed_video",
        "should_type",
        "clip_start_sec",
        "clip_end_sec",
        "instructional_keyframe_timestamp",
        "step_type",
    }
    assert normalized[0]["no_needed_video"] is False
    assert normalized[0]["should_type"] == ""


def test_tutorial_schema_preserves_no_needed_video_and_should_type_override():
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
    assert results[0].no_needed_video is True
    assert results[0].should_type == "abstract"
    assert normalized[0]["no_needed_video"] is True
    assert normalized[0]["should_type"] == "abstract"

    assert results[1].no_needed_video is False
    assert results[1].should_type == "concrete"
    assert normalized[1]["no_needed_video"] is False
    assert normalized[1]["should_type"] == "concrete"


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
            target_text="Settings",
            target_ui_type="menu_item",
            target_relative_position="top-left",
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


