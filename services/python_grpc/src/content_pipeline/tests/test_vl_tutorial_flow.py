"""
VL 鏁欑▼妯″紡娴嬭瘯锛?
1) 瑙ｆ瀽 step schema 鏄惁姝ｇ‘
2) 鏍￠獙澶氭楠?process 鍦?Phase2B 鍓嶇殑浜х墿瀵煎嚭锛坢ock VL + mock ffmpeg锛?
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any, Dict


from services.python_grpc.src.content_pipeline.phase2a.materials.vl_material_generator import VLMaterialGenerator
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


def test_tutorial_schema_parse_and_normalize():
    analyzer = VLVideoAnalyzer(_build_analyzer_config())
    payload = [
        {
            "step_id": 1,
            "step_description": "Open settings",
            "clip_start_sec": 0.0,
            "clip_end_sec": 7.0,
            "instructional_keyframe_timestamp": [6.2],
        },
        {
            "step_id": 2,
            "step_description": "Change port",
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
    assert results[0].knowledge_type == "process"
    assert results[1].suggested_screenshoot_timestamps == [12.2]
    assert normalized[0]["instructional_keyframe_timestamp"] == [6.2]
    assert set(normalized[0].keys()) == {
        "step_id",
        "step_description",
        "clip_start_sec",
        "clip_end_sec",
        "instructional_keyframe_timestamp",
    }


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
                "clip_start_sec": 0.0,
                "clip_end_sec": 8.0,
                "instructional_keyframe_timestamp": [7.2],
            },
            {
                "step_id": 2,
                "step_description": "change port",
                "clip_start_sec": 8.0,
                "clip_end_sec": 17.0,
                "instructional_keyframe_timestamp": [16.5],
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

    async def _fake_prepare_pruned_clip_for_vl(clips_dir, semantic_unit, original_clip_path, force_preprocess=False):
        return {
            "applied": False,
            "clip_path_for_vl": original_clip_path,
            "pre_context_prompt": "",
            "kept_segments": [],
        }

    async def _fake_export_clip_asset_with_ffmpeg(video_path, start_sec, end_sec, output_path):
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"step-clip")
        return True

    async def _fake_export_keyframe_with_ffmpeg(video_path, timestamp_sec, output_path):
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


