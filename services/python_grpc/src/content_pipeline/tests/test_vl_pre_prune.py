"""
VL pre-prune unit tests.
Coverage goals: 1) stable core interval removal keeps expected kept intervals;
2) pruned-relative time maps back to original timeline; 3) prompt includes knowledge_topic and text context."""

from __future__ import annotations

import asyncio
import shutil
from pathlib import Path
from typing import Dict, Any
from uuid import uuid4

from services.python_grpc.src.content_pipeline.phase2a.materials.vl_material_generator import VLMaterialGenerator


def _build_generator():
    return VLMaterialGenerator(
        {
            "enabled": True,
            "pre_vl_static_pruning": {
                "enabled": True,
                "only_process": True,
                "min_unit_duration_sec": 10.0,
                "min_stable_interval_sec": 3.0,
                "keep_edge_sec": 1.0,
                "min_cut_span_sec": 0.8,
                "min_keep_segment_sec": 0.5,
                "min_removed_ratio": 0.1,
                "context_text_max_chars": 200,
            },
            "screenshot_optimization": {"enabled": False},
            "fallback": {"enabled": True},
        }
    )


def test_pre_vl_pruning_defaults_disabled_for_process_and_concrete():
    generator = VLMaterialGenerator(
        {
            "enabled": True,
            "pre_vl_static_pruning": {},
        }
    )
    assert generator.pre_vl_pruning_enabled is False
    assert generator.pre_vl_process_pruning_enabled is False
    assert generator.pre_vl_concrete_pruning_enabled is False
    assert generator._is_pre_vl_pruning_enabled_for_knowledge_type("process") is False
    assert generator._is_pre_vl_pruning_enabled_for_knowledge_type("concrete") is False


def test_pre_vl_pruning_can_enable_process_and_concrete_independently():
    process_only = VLMaterialGenerator(
        {
            "enabled": True,
            "pre_vl_static_pruning": {
                "enabled": False,
                "process_enabled": True,
                "concrete_enabled": False,
            },
        }
    )
    assert process_only.pre_vl_process_pruning_enabled is True
    assert process_only.pre_vl_concrete_pruning_enabled is False
    assert process_only._is_pre_vl_pruning_enabled_for_knowledge_type("process") is True
    assert process_only._is_pre_vl_pruning_enabled_for_knowledge_type("concrete") is False

    concrete_only = VLMaterialGenerator(
        {
            "enabled": True,
            "pre_vl_static_pruning": {
                "enabled": False,
                "process_enabled": False,
                "concrete_enabled": True,
            },
        }
    )
    assert concrete_only.pre_vl_process_pruning_enabled is False
    assert concrete_only.pre_vl_concrete_pruning_enabled is True
    assert concrete_only._is_pre_vl_pruning_enabled_for_knowledge_type("process") is False
    assert concrete_only._is_pre_vl_pruning_enabled_for_knowledge_type("concrete") is True


def test_generate_skips_abstract_units_before_vl_analysis(tmp_path, monkeypatch):
    generator = VLMaterialGenerator(
        {
            "enabled": True,
            "use_cache": False,
            "screenshot_optimization": {"enabled": False},
            "fallback": {"enabled": True},
        }
    )
    called = {"split": False}

    async def _should_not_split(*args, **kwargs):
        called["split"] = True
        raise AssertionError("abstract-only input should return before split")

    monkeypatch.setattr(generator, "_split_video_by_semantic_units", _should_not_split)

    result = asyncio.run(
        generator.generate(
            video_path=str(tmp_path / "demo.mp4"),
            semantic_units=[
                {"unit_id": "SU001", "knowledge_type": "abstract"},
                {"unit_id": "SU002", "knowledge_type": "讲解"},
            ],
            output_dir=str(tmp_path),
        )
    )

    assert result.success is True
    assert result.screenshot_requests == []
    assert result.clip_requests == []
    assert result.token_stats["total_units"] == 2
    assert result.token_stats["skipped_abstract_units"] == 2
    assert result.token_stats["vl_units"] == 0
    assert called["split"] is False


def test_pre_vl_parallel_mode_resolution_process_auto_and_async():
    generator_process = VLMaterialGenerator(
        {
            "enabled": True,
            "pre_vl_static_pruning": {
                "enabled": True,
                "process_stable_detect_enabled": True,
                "parallel_mode": "process",
                "parallel_workers": 4,
                "parallel_hard_cap": 8,
            },
        }
    )
    assert generator_process._should_use_pre_vl_process_mode(worker_count=4) is True

    generator_auto_without_pool = VLMaterialGenerator(
        {
            "enabled": True,
            "pre_vl_static_pruning": {
                "enabled": True,
                "process_stable_detect_enabled": True,
                "parallel_mode": "auto",
                "parallel_workers": 4,
                "parallel_hard_cap": 8,
            },
        }
    )
    assert generator_auto_without_pool._should_use_pre_vl_process_mode(worker_count=4) is False

    class _PoolStub:
        _max_workers = 4

    generator_auto_with_pool = VLMaterialGenerator(
        {
            "enabled": True,
            "pre_vl_static_pruning": {
                "enabled": True,
                "process_stable_detect_enabled": True,
                "parallel_mode": "auto",
                "parallel_workers": 4,
                "parallel_hard_cap": 8,
            },
        },
        cv_executor=_PoolStub(),
    )
    assert generator_auto_with_pool._should_use_pre_vl_process_mode(worker_count=4) is True

    generator_async = VLMaterialGenerator(
        {
            "enabled": True,
            "pre_vl_static_pruning": {
                "enabled": True,
                "process_stable_detect_enabled": True,
                "parallel_mode": "async",
                "parallel_workers": 4,
                "parallel_hard_cap": 8,
            },
        }
    )
    assert generator_async._should_use_pre_vl_process_mode(worker_count=4) is False


def test_pre_vl_process_stable_detect_disabled_by_default():
    generator_default = VLMaterialGenerator(
        {
            "enabled": True,
            "pre_vl_static_pruning": {
                "enabled": True,
                "parallel_mode": "process",
                "parallel_workers": 4,
                "parallel_hard_cap": 8,
            },
        }
    )
    assert generator_default._should_use_pre_vl_process_mode(worker_count=4) is False

    generator_explicit_off = VLMaterialGenerator(
        {
            "enabled": True,
            "pre_vl_static_pruning": {
                "enabled": True,
                "process_stable_detect_enabled": False,
                "parallel_mode": "auto",
                "parallel_workers": 4,
                "parallel_hard_cap": 8,
            },
        }
    )
    assert generator_explicit_off._should_use_pre_vl_process_mode(worker_count=4) is False


def test_prepare_pruned_clips_uses_override_stable_intervals(tmp_path, monkeypatch):
    generator = VLMaterialGenerator(
        {
            "enabled": True,
            "pre_vl_static_pruning": {
                "enabled": True,
                "only_process": True,
                "process_stable_detect_enabled": True,
                "parallel_mode": "process",
                "parallel_workers": 2,
                "parallel_hard_cap": 4,
            },
        },
        cv_executor=object(),
    )

    clips_dir = tmp_path / "clips"
    clips_dir.mkdir(parents=True, exist_ok=True)

    unit_tasks = [
        {
            "semantic_unit": {"unit_id": "U1", "start_sec": 0.0, "end_sec": 12.0, "knowledge_type": "process"},
            "clip_path": str(clips_dir / "u1.mp4"),
        },
        {
            "semantic_unit": {"unit_id": "U2", "start_sec": 0.0, "end_sec": 12.0, "knowledge_type": "process"},
            "clip_path": str(clips_dir / "u2.mp4"),
        },
    ]

    async def _fake_detect_stable_islands_for_units_via_process_pool(*, unit_tasks, worker_count):
        return [[(1.0, 6.0)], [(2.0, 7.0)]]

    async def _fake_prepare_pruned_clip_for_vl(clips_dir, semantic_unit, original_clip_path, force_preprocess=False, stable_intervals_override=None):
        return {
            "applied": True,
            "clip_path_for_vl": original_clip_path,
            "kept_segments": stable_intervals_override or [],
            "removed_segments": [],
            "pre_context_prompt": "",
        }

    monkeypatch.setattr(
        generator,
        "_detect_stable_islands_for_units_via_process_pool",
        _fake_detect_stable_islands_for_units_via_process_pool,
    )
    monkeypatch.setattr(generator, "_prepare_pruned_clip_for_vl", _fake_prepare_pruned_clip_for_vl)

    results = asyncio.run(
        generator._prepare_pruned_clips_for_units(
            clips_dir=str(clips_dir),
            unit_tasks=unit_tasks,
            force_preprocess=False,
        )
    )

    assert len(results) == 2
    assert results[0]["kept_segments"] == [(1.0, 6.0)]
    assert results[1]["kept_segments"] == [(2.0, 7.0)]


def test_prepare_pruned_clips_process_mode_skips_detect_for_disabled_knowledge_type(tmp_path, monkeypatch):
    generator = VLMaterialGenerator(
        {
            "enabled": True,
            "pre_vl_static_pruning": {
                "enabled": False,
                "process_enabled": True,
                "concrete_enabled": False,
                "process_stable_detect_enabled": True,
                "parallel_mode": "process",
                "parallel_workers": 2,
                "parallel_hard_cap": 4,
            },
        },
        cv_executor=object(),
    )

    clips_dir = tmp_path / "clips"
    clips_dir.mkdir(parents=True, exist_ok=True)

    unit_tasks = [
        {
            "semantic_unit": {"unit_id": "P1", "start_sec": 0.0, "end_sec": 12.0, "knowledge_type": "process"},
            "clip_path": str(clips_dir / "p1.mp4"),
        },
        {
            "semantic_unit": {"unit_id": "C1", "start_sec": 0.0, "end_sec": 12.0, "knowledge_type": "concrete"},
            "clip_path": str(clips_dir / "c1.mp4"),
        },
    ]

    captured = {"detect_units": [], "prepared_units": []}

    async def _fake_detect_stable_islands_for_units_via_process_pool(*, unit_tasks, worker_count):
        captured["detect_units"] = [
            str((task.get("semantic_unit", {}) or {}).get("unit_id", ""))
            for task in (unit_tasks or [])
        ]
        return [[(1.0, 6.0)]]

    async def _fake_prepare_pruned_clip_for_vl(
        clips_dir,
        semantic_unit,
        original_clip_path,
        force_preprocess=False,
        stable_intervals_override=None,
    ):
        captured["prepared_units"].append(str((semantic_unit or {}).get("unit_id", "")))
        return {
            "applied": True,
            "clip_path_for_vl": original_clip_path,
            "kept_segments": stable_intervals_override or [],
            "removed_segments": [],
            "pre_context_prompt": "",
        }

    monkeypatch.setattr(
        generator,
        "_detect_stable_islands_for_units_via_process_pool",
        _fake_detect_stable_islands_for_units_via_process_pool,
    )
    monkeypatch.setattr(generator, "_prepare_pruned_clip_for_vl", _fake_prepare_pruned_clip_for_vl)

    results = asyncio.run(
        generator._prepare_pruned_clips_for_units(
            clips_dir=str(clips_dir),
            unit_tasks=unit_tasks,
            force_preprocess=False,
        )
    )

    assert captured["detect_units"] == ["P1"]
    assert captured["prepared_units"] == ["P1"]
    assert len(results) == 2
    assert results[0]["applied"] is True
    assert results[1]["applied"] is False


def test_prepare_pruned_clips_process_mode_fallbacks_on_detect_failure(tmp_path, monkeypatch):
    generator = VLMaterialGenerator(
        {
            "enabled": True,
            "pre_vl_static_pruning": {
                "enabled": True,
                "only_process": True,
                "process_stable_detect_enabled": True,
                "parallel_mode": "process",
                "parallel_workers": 2,
                "parallel_hard_cap": 4,
            },
        },
        cv_executor=object(),
    )

    clips_dir = tmp_path / "clips"
    clips_dir.mkdir(parents=True, exist_ok=True)

    unit_tasks = [
        {
            "semantic_unit": {"unit_id": "U1", "start_sec": 0.0, "end_sec": 12.0, "knowledge_type": "process"},
            "clip_path": str(clips_dir / "u1.mp4"),
        },
        {
            "semantic_unit": {"unit_id": "U2", "start_sec": 0.0, "end_sec": 15.0, "knowledge_type": "process"},
            "clip_path": str(clips_dir / "u2.mp4"),
        },
    ]

    async def _fake_detect_stable_islands_for_units_via_process_pool(*, unit_tasks, worker_count):
        return [None, [(2.0, 7.0)]]

    async def _fake_prepare_pruned_clip_for_vl(clips_dir, semantic_unit, original_clip_path, force_preprocess=False, stable_intervals_override=None):
        return {
            "applied": True,
            "clip_path_for_vl": original_clip_path,
            "kept_segments": stable_intervals_override or [],
            "removed_segments": [],
            "pre_context_prompt": "",
        }

    monkeypatch.setattr(
        generator,
        "_detect_stable_islands_for_units_via_process_pool",
        _fake_detect_stable_islands_for_units_via_process_pool,
    )
    monkeypatch.setattr(generator, "_prepare_pruned_clip_for_vl", _fake_prepare_pruned_clip_for_vl)

    results = asyncio.run(
        generator._prepare_pruned_clips_for_units(
            clips_dir=str(clips_dir),
            unit_tasks=unit_tasks,
            force_preprocess=False,
        )
    )

    assert len(results) == 2
    assert results[0]["applied"] is False
    assert results[0]["clip_path_for_vl"].endswith("u1.mp4")
    assert results[0]["kept_segments"] == [(0.0, 12.0)]
    assert results[1]["kept_segments"] == [(2.0, 7.0)]


def test_build_reusable_routing_pre_prune_info_accepts_valid_routing_result(tmp_path):
    generator = _build_generator()

    clip_path = str(tmp_path / "u1.mp4")
    pruned_path = tmp_path / "u1_pruned.mp4"
    pruned_path.write_bytes(b"pruned")

    semantic_unit = {
        "unit_id": "U1",
        "start_sec": 10.0,
        "end_sec": 20.0,
        "_routing_pre_prune": {
            "applied": True,
            "clip_path_for_vl": str(pruned_path),
            "kept_segments": [[0.0, 2.0], {"start_sec": 3.0, "end_sec": 4.0}],
            "removed_segments": [{"start_sec": 2.0, "end_sec": 3.0}],
            "pre_context_prompt": "ctx",
        },
    }

    result = generator._build_reusable_routing_pre_prune_info(
        semantic_unit=semantic_unit,
        clip_path=clip_path,
    )

    assert result is not None
    assert result["applied"] is True
    assert result["clip_path_for_vl"] == str(pruned_path)
    assert result["kept_segments"] == [(0.0, 2.0), (3.0, 4.0)]
    assert result["removed_segments"] == [(2.0, 3.0)]
    assert result["pre_context_prompt"] == "ctx"


def test_build_reusable_routing_pre_prune_info_parses_raw_stable_intervals(tmp_path):
    generator = _build_generator()

    clip_path = str(tmp_path / "u1.mp4")
    pruned_path = tmp_path / "u1_pruned.mp4"
    pruned_path.write_bytes(b"pruned")

    semantic_unit = {
        "unit_id": "U1",
        "start_sec": 10.0,
        "end_sec": 20.0,
        "_routing_pre_prune": {
            "applied": True,
            "clip_path_for_vl": str(pruned_path),
            "kept_segments": [[0.0, 2.0]],
            "removed_segments": [],
            "stable_intervals_raw": [[1.0, 4.0], {"start_sec": 5.0, "end_sec": 9.0}, [3.0]],
            "pre_context_prompt": "ctx",
        },
    }

    result = generator._build_reusable_routing_pre_prune_info(
        semantic_unit=semantic_unit,
        clip_path=clip_path,
    )

    assert result is not None
    assert result["stable_intervals_raw"] == [(1.0, 4.0), (5.0, 9.0)]


def test_build_reusable_routing_pre_prune_info_skips_non_materialized_result():
    generator = _build_generator()

    work_dir = Path("var") / f"tmp_test_vl_non_materialized_{uuid4().hex}"
    work_dir.mkdir(parents=True, exist_ok=True)
    try:
        clip_path = str(work_dir / "u1.mp4")
        semantic_unit = {
            "unit_id": "U1",
            "start_sec": 10.0,
            "end_sec": 20.0,
            "_routing_pre_prune": {
                "applied": True,
                "materialized": False,
                "clip_path_for_vl": clip_path,
                "kept_segments": [[0.0, 2.0]],
                "removed_segments": [{"start_sec": 2.0, "end_sec": 3.0}],
                "pre_context_prompt": "ctx",
            },
        }

        result = generator._build_reusable_routing_pre_prune_info(
            semantic_unit=semantic_unit,
            clip_path=clip_path,
        )

        assert result is None
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


def test_build_reusable_routing_pre_prune_info_accepts_non_materialized_when_force_legacy():
    generator = _build_generator()

    work_dir = Path("var") / f"tmp_test_vl_non_materialized_legacy_{uuid4().hex}"
    work_dir.mkdir(parents=True, exist_ok=True)
    try:
        clip_path = work_dir / "u1.mp4"
        clip_path.write_bytes(b"raw")
        semantic_unit = {
            "unit_id": "U1",
            "start_sec": 10.0,
            "end_sec": 20.0,
            "_routing_force_legacy_action": True,
            "_routing_pre_prune": {
                "applied": True,
                "materialized": False,
                "clip_path_for_vl": str(clip_path),
                "kept_segments": [[0.0, 2.0]],
                "removed_segments": [{"start_sec": 2.0, "end_sec": 3.0}],
                "stable_intervals_raw": [[3.0, 6.0]],
                "pre_context_prompt": "ctx",
            },
        }

        result = generator._build_reusable_routing_pre_prune_info(
            semantic_unit=semantic_unit,
            clip_path=str(clip_path),
        )

        assert result is not None
        assert result["applied"] is True
        assert result["materialized"] is False
        assert result["clip_path_for_vl"] == str(clip_path)
        assert result["kept_segments"] == [(0.0, 2.0)]
        assert result["stable_intervals_raw"] == [(3.0, 6.0)]
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


def test_prepare_pruned_clip_for_vl_returns_raw_stable_when_not_pruned(monkeypatch):
    generator = _build_generator()

    async def _fake_detect_stable_islands_for_unit(_clip_path, _unit_id):
        return [(1.0, 3.5)]

    monkeypatch.setattr(generator, "_detect_stable_islands_for_unit", _fake_detect_stable_islands_for_unit)

    result = asyncio.run(
        generator._prepare_pruned_clip_for_vl(
            clips_dir=".",
            semantic_unit={"unit_id": "U1", "start_sec": 0.0, "end_sec": 12.0, "knowledge_type": "process"},
            original_clip_path="dummy.mp4",
            force_preprocess=False,
        )
    )

    assert result["applied"] is False
    assert result["clip_path_for_vl"] == "dummy.mp4"
    assert result["stable_intervals_raw"] == [(1.0, 3.5)]


def test_prepare_pruned_clip_for_vl_routing_only_skips_materialization(monkeypatch):
    generator = _build_generator()
    work_dir = Path("var") / f"tmp_test_vl_routing_only_{uuid4().hex}"
    work_dir.mkdir(parents=True, exist_ok=True)
    source_clip = work_dir / "u1.mp4"
    source_clip.write_bytes(b"raw")

    async def _fake_detect_stable_islands_for_unit(_clip_path, _unit_id):
        return [(1.0, 7.0)]

    async def _fake_refine_kept_segments_before_concat(*, clips_dir, semantic_unit, original_clip_path, kept_segments):
        return kept_segments

    concat_called = {"value": False}

    async def _fake_concat_segments_with_ffmpeg(*, source_clip_path, output_clip_path, segments):
        concat_called["value"] = True
        return True

    monkeypatch.setattr(generator, "_detect_stable_islands_for_unit", _fake_detect_stable_islands_for_unit)
    monkeypatch.setattr(generator, "_refine_kept_segments_before_concat", _fake_refine_kept_segments_before_concat)
    monkeypatch.setattr(generator, "_concat_segments_with_ffmpeg", _fake_concat_segments_with_ffmpeg)

    try:
        result = asyncio.run(
            generator._prepare_pruned_clip_for_vl(
                clips_dir=str(work_dir),
                semantic_unit={
                    "unit_id": "U1",
                    "start_sec": 0.0,
                    "end_sec": 12.0,
                    "knowledge_type": "process",
                    "_routing_preprocess_only": True,
                },
                original_clip_path=str(source_clip),
                force_preprocess=True,
            )
        )

        assert result["applied"] is True
        assert result["materialized"] is False
        assert result["clip_path_for_vl"] == str(source_clip)
        assert concat_called["value"] is False
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


def test_build_stable_action_material_requests_for_unit_generates_head_tail_windows(monkeypatch):
    generator = _build_generator()

    async def _fake_refine_kept_segments_before_concat(*, clips_dir, semantic_unit, original_clip_path, kept_segments):
        return kept_segments

    async def _fake_detect_typed_action_segments_for_unit(*, clip_path, unit_id, duration_sec):
        # Keep both kept segments by providing non-transition overlaps.
        return [
            {"start_sec": 2.0, "end_sec": 4.0, "action_type": "knowledge"},
            {"start_sec": 6.0, "end_sec": 10.0, "action_type": "knowledge"},
        ]

    monkeypatch.setattr(generator, "_refine_kept_segments_before_concat", _fake_refine_kept_segments_before_concat)
    monkeypatch.setattr(
        generator,
        "_detect_typed_action_segments_for_unit",
        _fake_detect_typed_action_segments_for_unit,
    )

    semantic_unit = {
        "unit_id": "SU100",
        "start_sec": 100.0,
        "end_sec": 110.0,
        "knowledge_type": "process",
        "mult_steps": True,
    }
    pre_prune_info = {
        "applied": True,
        "kept_segments": [(2.0, 4.0), (6.0, 10.0)],
        "stable_intervals_raw": [(0.0, 2.0), (4.0, 6.0)],
    }

    built = asyncio.run(
        generator._build_stable_action_material_requests_for_unit(
            clips_dir=".",
            semantic_unit=semantic_unit,
            original_clip_path="dummy.mp4",
            pre_prune_info=pre_prune_info,
        )
    )

    clips = built["clip_requests"]
    screenshots = built["screenshot_requests"]

    assert len(clips) == 2
    assert clips[0]["clip_id"] == "SU100/SU100_clip_vl_action_001"
    assert abs(float(clips[0]["start_sec"]) - 102.0) < 1e-6
    assert abs(float(clips[0]["end_sec"]) - 104.0) < 1e-6
    assert clips[1]["clip_id"] == "SU100/SU100_clip_vl_action_002"
    assert abs(float(clips[1]["start_sec"]) - 106.0) < 1e-6
    assert abs(float(clips[1]["end_sec"]) - 110.0) < 1e-6

    assert len(screenshots) == 4
    first_head = screenshots[0]
    first_tail = screenshots[1]
    assert first_head["screenshot_id"] == "SU100/SU100_ss_vl_action_001_head"
    assert first_head["anchor_role"] == "head"
    assert abs(float(first_head["_window_start_sec"]) - 101.0) < 1e-6
    assert abs(float(first_head["_window_end_sec"]) - 103.0) < 1e-6
    assert first_tail["screenshot_id"] == "SU100/SU100_ss_vl_action_001_tail"
    assert first_tail["anchor_role"] == "tail"
    assert abs(float(first_tail["_window_start_sec"]) - 103.0) < 1e-6
    assert abs(float(first_tail["_window_end_sec"]) - 105.0) < 1e-6


def test_build_stable_action_material_requests_prefers_kept_segments_when_stable_complement_is_tiny(monkeypatch):
    generator = _build_generator()

    async def _fake_refine_kept_segments_before_concat(*, clips_dir, semantic_unit, original_clip_path, kept_segments):
        return kept_segments

    async def _fake_detect_typed_action_segments_for_unit(*, clip_path, unit_id, duration_sec):
        # Keep both kept segments by providing non-transition overlaps.
        return [
            {"start_sec": 0.0, "end_sec": 7.1, "action_type": "knowledge"},
            {"start_sec": 56.8, "end_sec": 58.0, "action_type": "knowledge"},
        ]

    monkeypatch.setattr(generator, "_refine_kept_segments_before_concat", _fake_refine_kept_segments_before_concat)
    monkeypatch.setattr(
        generator,
        "_detect_typed_action_segments_for_unit",
        _fake_detect_typed_action_segments_for_unit,
    )

    semantic_unit = {
        "unit_id": "SU200",
        "start_sec": 100.0,
        "end_sec": 158.0,
        "knowledge_type": "process",
        "mult_steps": True,
    }
    pre_prune_info = {
        "applied": True,
        # 路由预处理已经给出应保留的动作片段；应优先复用它而不是再由 stable 取补集。
        "kept_segments": [(0.0, 7.1), (56.8, 58.0)],
        # 若走 stable 取补集，得到的仅是 <0.5s 的碎片，会被过滤为空。
        "stable_intervals_raw": [(0.2, 5.6), (5.8, 58.0)],
    }

    built = asyncio.run(
        generator._build_stable_action_material_requests_for_unit(
            clips_dir=".",
            semantic_unit=semantic_unit,
            original_clip_path="dummy.mp4",
            pre_prune_info=pre_prune_info,
        )
    )

    clips = built["clip_requests"]
    screenshots = built["screenshot_requests"]

    assert len(clips) == 2
    assert clips[0]["clip_id"] == "SU200/SU200_clip_vl_action_001"
    assert abs(float(clips[0]["start_sec"]) - 100.0) < 1e-6
    assert abs(float(clips[0]["end_sec"]) - 107.1) < 1e-6
    assert clips[1]["clip_id"] == "SU200/SU200_clip_vl_action_002"
    assert abs(float(clips[1]["start_sec"]) - 156.8) < 1e-6
    assert abs(float(clips[1]["end_sec"]) - 158.0) < 1e-6

    assert len(screenshots) == 4
    screenshot_ids = {item["screenshot_id"] for item in screenshots}
    assert "SU200/SU200_ss_vl_action_001_head" in screenshot_ids
    assert "SU200/SU200_ss_vl_action_001_tail" in screenshot_ids
    assert "SU200/SU200_ss_vl_action_002_head" in screenshot_ids
    assert "SU200/SU200_ss_vl_action_002_tail" in screenshot_ids


def test_build_stable_action_material_requests_filters_transition_segments_before_refine(monkeypatch):
    generator = _build_generator()
    observed = {"kept_segments": None}

    async def _fake_detect_typed_action_segments_for_unit(*, clip_path, unit_id, duration_sec):
        assert clip_path == "dummy.mp4"
        assert unit_id == "SU300"
        assert duration_sec == 5.0
        # Simulate transition/ppt switch inside the action segment.
        return [
            {"start_sec": 2.0, "end_sec": 3.0, "action_type": "transition"},
            {"start_sec": 3.1, "end_sec": 3.8, "action_type": "knowledge"},
        ]

    async def _fake_refine_kept_segments_before_concat(*, clips_dir, semantic_unit, original_clip_path, kept_segments):
        observed["kept_segments"] = list(kept_segments)
        return kept_segments

    monkeypatch.setattr(
        generator,
        "_detect_typed_action_segments_for_unit",
        _fake_detect_typed_action_segments_for_unit,
    )
    monkeypatch.setattr(generator, "_refine_kept_segments_before_concat", _fake_refine_kept_segments_before_concat)

    semantic_unit = {
        "unit_id": "SU300",
        "start_sec": 100.0,
        "end_sec": 105.0,
        "knowledge_type": "process",
        "mult_steps": True,
    }
    pre_prune_info = {
        "applied": True,
        "kept_segments": [(1.0, 4.0)],
        "stable_intervals_raw": [],
    }

    built = asyncio.run(
        generator._build_stable_action_material_requests_for_unit(
            clips_dir=".",
            semantic_unit=semantic_unit,
            original_clip_path="dummy.mp4",
            pre_prune_info=pre_prune_info,
        )
    )

    assert observed["kept_segments"] == [(1.0, 2.0), (3.0, 4.0)]
    assert len(built["clip_requests"]) == 2
    assert abs(float(built["clip_requests"][0]["start_sec"]) - 101.0) < 1e-6
    assert abs(float(built["clip_requests"][0]["end_sec"]) - 102.0) < 1e-6
    assert abs(float(built["clip_requests"][1]["start_sec"]) - 103.0) < 1e-6
    assert abs(float(built["clip_requests"][1]["end_sec"]) - 104.0) < 1e-6
    assert len(built["screenshot_requests"]) == 4


def test_build_stable_action_material_requests_drops_segment_when_only_transition_inside(monkeypatch):
    generator = _build_generator()
    observed = {"refine_called": False}

    async def _fake_detect_typed_action_segments_for_unit(*, clip_path, unit_id, duration_sec):
        assert clip_path == "dummy.mp4"
        assert unit_id == "SU301"
        assert duration_sec == 5.0
        # Entire kept segment overlaps transition-only actions.
        return [
            {"start_sec": 1.2, "end_sec": 2.0, "action_type": "transition"},
            {"start_sec": 2.1, "end_sec": 3.8, "action_type": "transition"},
        ]

    async def _fake_refine_kept_segments_before_concat(*, clips_dir, semantic_unit, original_clip_path, kept_segments):
        observed["refine_called"] = True
        return kept_segments

    monkeypatch.setattr(
        generator,
        "_detect_typed_action_segments_for_unit",
        _fake_detect_typed_action_segments_for_unit,
    )
    monkeypatch.setattr(generator, "_refine_kept_segments_before_concat", _fake_refine_kept_segments_before_concat)

    semantic_unit = {
        "unit_id": "SU301",
        "start_sec": 100.0,
        "end_sec": 105.0,
        "knowledge_type": "process",
        "mult_steps": True,
    }
    pre_prune_info = {
        "applied": True,
        "kept_segments": [(1.0, 4.0)],
        "stable_intervals_raw": [],
    }

    built = asyncio.run(
        generator._build_stable_action_material_requests_for_unit(
            clips_dir=".",
            semantic_unit=semantic_unit,
            original_clip_path="dummy.mp4",
            pre_prune_info=pre_prune_info,
        )
    )

    assert observed["refine_called"] is False
    assert built["clip_requests"] == []
    assert len(built["screenshot_requests"]) == 1
    dropped_tail = built["screenshot_requests"][0]
    assert dropped_tail["anchor_role"] == "tail"
    assert dropped_tail["label"] == "action_drop_001_tail"
    assert abs(float(dropped_tail["timestamp_sec"]) - 104.0) < 1e-6
    assert built["action_segments"] == []


def test_build_stable_action_material_requests_drops_segment_when_no_typed_actions(monkeypatch):
    generator = _build_generator()
    observed = {"refine_called": False}

    async def _fake_detect_typed_action_segments_for_unit(*, clip_path, unit_id, duration_sec):
        assert clip_path == "dummy.mp4"
        assert unit_id == "SU302"
        assert duration_sec == 5.0
        # Simulate CV miss: no typed actions detected in kept segment.
        return []

    async def _fake_refine_kept_segments_before_concat(*, clips_dir, semantic_unit, original_clip_path, kept_segments):
        observed["refine_called"] = True
        return kept_segments

    monkeypatch.setattr(
        generator,
        "_detect_typed_action_segments_for_unit",
        _fake_detect_typed_action_segments_for_unit,
    )
    monkeypatch.setattr(generator, "_refine_kept_segments_before_concat", _fake_refine_kept_segments_before_concat)

    semantic_unit = {
        "unit_id": "SU302",
        "start_sec": 100.0,
        "end_sec": 105.0,
        "knowledge_type": "process",
        "mult_steps": True,
    }
    pre_prune_info = {
        "applied": True,
        "kept_segments": [(1.0, 4.0)],
        "stable_intervals_raw": [],
    }

    built = asyncio.run(
        generator._build_stable_action_material_requests_for_unit(
            clips_dir=".",
            semantic_unit=semantic_unit,
            original_clip_path="dummy.mp4",
            pre_prune_info=pre_prune_info,
        )
    )

    assert observed["refine_called"] is False
    assert built["clip_requests"] == []
    assert len(built["screenshot_requests"]) == 1
    dropped_tail = built["screenshot_requests"][0]
    assert dropped_tail["anchor_role"] == "tail"
    assert dropped_tail["label"] == "action_drop_001_tail"
    assert abs(float(dropped_tail["timestamp_sec"]) - 104.0) < 1e-6
    assert built["action_segments"] == []


def test_build_stable_action_material_requests_drops_short_dynamic_action_and_keeps_tail(monkeypatch):
    generator = _build_generator()
    observed = {"refine_called": False}

    async def _fake_detect_typed_action_segments_for_unit(*, clip_path, unit_id, duration_sec):
        assert clip_path == "dummy.mp4"
        assert unit_id == "SU303"
        assert duration_sec == 5.0
        # Internal stable islands occupy most of the action window, leaving < 0.5s dynamic duration.
        return [
            {
                "start_sec": 1.0,
                "end_sec": 4.0,
                "action_type": "knowledge",
                "dynamic_duration_sec": 0.4,
            }
        ]

    async def _fake_refine_kept_segments_before_concat(*, clips_dir, semantic_unit, original_clip_path, kept_segments):
        observed["refine_called"] = True
        return kept_segments

    monkeypatch.setattr(
        generator,
        "_detect_typed_action_segments_for_unit",
        _fake_detect_typed_action_segments_for_unit,
    )
    monkeypatch.setattr(generator, "_refine_kept_segments_before_concat", _fake_refine_kept_segments_before_concat)

    semantic_unit = {
        "unit_id": "SU303",
        "start_sec": 100.0,
        "end_sec": 105.0,
        "knowledge_type": "process",
        "mult_steps": True,
    }
    pre_prune_info = {
        "applied": True,
        "kept_segments": [(1.0, 4.0)],
        "stable_intervals_raw": [],
    }

    built = asyncio.run(
        generator._build_stable_action_material_requests_for_unit(
            clips_dir=".",
            semantic_unit=semantic_unit,
            original_clip_path="dummy.mp4",
            pre_prune_info=pre_prune_info,
        )
    )

    assert observed["refine_called"] is False
    assert built["clip_requests"] == []
    assert len(built["screenshot_requests"]) == 1
    dropped_tail = built["screenshot_requests"][0]
    assert dropped_tail["anchor_role"] == "tail"
    assert dropped_tail["label"] == "action_drop_001_tail"
    assert abs(float(dropped_tail["timestamp_sec"]) - 104.0) < 1e-6
    assert built["action_segments"] == []


def test_dedupe_incremental_legacy_drop_tail_screenshots_keeps_latest_incremental(monkeypatch):
    generator = _build_generator()

    class _FakeCap:
        def __init__(self):
            self._frame_idx = 0

        def isOpened(self):
            return True

        def get(self, _prop):
            return 30.0

        def set(self, _prop, frame_idx):
            self._frame_idx = int(frame_idx)

        def read(self):
            return True, {"frame_idx": self._frame_idx}

        def release(self):
            return None

    import services.python_grpc.src.content_pipeline.phase2a.materials.vl_material_generator as vl_material_generator_module
    import services.python_grpc.src.vision_validation.worker as worker_module

    monkeypatch.setattr(
        vl_material_generator_module,
        "open_video_capture_with_fallback",
        lambda _video_path, logger=None, allow_inline_transcode=None: (_FakeCap(), "effective.mp4", False),
    )

    def _fake_extract_ocr_tokens(frame, _roi):
        frame_idx = int((frame or {}).get("frame_idx", 0))
        if frame_idx >= 330:
            return {"open", "settings"}
        return {"open"}

    def _fake_extract_shape_signature(_frame, _roi):
        return {"rect_count": 1, "component_count": 1, "edge_density": 0.1}

    def _fake_filter_incremental_screenshots(candidates):
        kept = []
        for base in candidates:
            base_tokens = set(base.get("ocr_tokens") or [])
            covered = False
            for cand in candidates:
                if cand is base:
                    continue
                cand_tokens = set(cand.get("ocr_tokens") or [])
                if base_tokens and base_tokens.issubset(cand_tokens) and len(cand_tokens) > len(base_tokens):
                    covered = True
                    break
            if not covered:
                kept.append(base)
        return kept

    monkeypatch.setattr(worker_module, "_extract_ocr_tokens", _fake_extract_ocr_tokens)
    monkeypatch.setattr(worker_module, "_extract_shape_signature", _fake_extract_shape_signature)
    monkeypatch.setattr(worker_module, "_filter_incremental_screenshots", _fake_filter_incremental_screenshots)

    screenshot_requests = [
        {
            "screenshot_id": "SU007/SU007_ss_vl_action_drop_001_tail",
            "timestamp_sec": 10.0,
            "label": "action_drop_001_tail",
            "semantic_unit_id": "SU007",
            "analysis_mode": "legacy_action_units",
            "anchor_role": "tail",
        },
        {
            "screenshot_id": "SU007/SU007_ss_vl_action_drop_002_tail",
            "timestamp_sec": 11.0,
            "label": "action_drop_002_tail",
            "semantic_unit_id": "SU007",
            "analysis_mode": "legacy_action_units",
            "anchor_role": "tail",
        },
        {
            "screenshot_id": "SU007/SU007_ss_vl_action_003_tail",
            "timestamp_sec": 12.0,
            "label": "action_003_tail",
            "semantic_unit_id": "SU007",
            "analysis_mode": "legacy_action_units",
            "anchor_role": "tail",
        },
        {
            "screenshot_id": "SU008/SU008_ss_vl_action_drop_001_tail",
            "timestamp_sec": 8.0,
            "label": "action_drop_001_tail",
            "semantic_unit_id": "SU008",
            "analysis_mode": "legacy_action_units",
            "anchor_role": "tail",
        },
    ]

    deduped = generator._dedupe_incremental_legacy_drop_tail_screenshots(
        video_path="dummy.mp4",
        screenshot_requests=screenshot_requests,
    )

    kept_ids = {str(item.get("screenshot_id")) for item in deduped}
    assert "SU007/SU007_ss_vl_action_drop_001_tail" not in kept_ids
    assert "SU007/SU007_ss_vl_action_drop_002_tail" in kept_ids
    assert "SU007/SU007_ss_vl_action_003_tail" in kept_ids
    assert "SU008/SU008_ss_vl_action_drop_001_tail" in kept_ids
    assert len(deduped) == 3


def test_generate_diverts_static_dominant_multistep_unit_to_legacy_action_branch(monkeypatch):
    work_dir = Path("var") / f"tmp_test_vl_legacy_action_{uuid4().hex}"
    work_dir.mkdir(parents=True, exist_ok=True)
    video_path = work_dir / "video.mp4"
    video_path.write_bytes(b"video")
    clips_dir = work_dir / "semantic_unit_clips_vl"
    clips_dir.mkdir(parents=True, exist_ok=True)

    generator = VLMaterialGenerator(
        {
            "enabled": True,
            "use_cache": False,
            "save_cache": False,
            "merge_multistep_clip_requests": False,
            "pre_vl_static_pruning": {
                "enabled": True,
                "only_process": True,
                "legacy_action_trigger_ratio": 1.0 / 3.0,
                "legacy_action_window_sec": 1.0,
            },
            "screenshot_optimization": {"enabled": False},
            "fallback": {"enabled": True},
        }
    )

    semantic_units = [
        {
            "unit_id": "SU001",
            "start_sec": 0.0,
            "end_sec": 30.0,
            "knowledge_type": "process",
            "mult_steps": True,
        }
    ]

    monkeypatch.setattr(generator, "_split_video_by_semantic_units", lambda *args, **kwargs: asyncio.sleep(0, result=str(clips_dir)))
    monkeypatch.setattr(
        generator,
        "_find_clip_for_unit",
        lambda clips_dir, unit_id, start_sec, end_sec: str(Path(clips_dir) / f"{unit_id}.mp4"),
    )

    async def _fake_resolve_pre_prune_results_for_unit_tasks(*, clips_dir, unit_tasks, force_preprocess=False):
        return [
            {
                "applied": True,
                "clip_path_for_vl": str(Path(clips_dir) / "SU001_pruned.mp4"),
                "kept_segments": [(2.0, 8.0)],
                "removed_segments": [(8.0, 30.0)],
                "stable_intervals_raw": [(0.0, 2.0), (8.0, 30.0)],
                "pre_context_prompt": "",
            }
        ]

    async def _fake_build_stable_action_material_requests_for_unit(*, clips_dir, semantic_unit, original_clip_path, pre_prune_info):
        return {
            "clip_requests": [
                {
                    "clip_id": "SU001/SU001_clip_vl_action_001",
                    "start_sec": 2.0,
                    "end_sec": 8.0,
                    "knowledge_type": "process",
                    "semantic_unit_id": "SU001",
                    "analysis_mode": "legacy_action_units",
                }
            ],
            "screenshot_requests": [
                {
                    "screenshot_id": "SU001/SU001_ss_vl_action_001_head",
                    "timestamp_sec": 2.0,
                    "semantic_unit_id": "SU001",
                    "analysis_mode": "legacy_action_units",
                    "_window_start_sec": 1.0,
                    "_window_end_sec": 3.0,
                }
            ],
            "action_segments": [(2.0, 8.0)],
        }

    async def _fake_build_legacy_action_pre_prune_info_for_unit(
        *,
        clips_dir,
        semantic_unit,
        original_clip_path,
        action_segments,
        base_pre_prune_info=None,
    ):
        assert semantic_unit.get("unit_id") == "SU001"
        assert action_segments == [(2.0, 8.0)]
        return {
            "applied": True,
            "materialized": True,
            "clip_path_for_vl": str(Path(clips_dir) / "SU001_legacy_action_pruned.mp4"),
            "kept_segments": [(2.0, 8.0)],
            "removed_segments": [(0.0, 2.0), (8.0, 30.0)],
            "stable_intervals_raw": [(0.0, 2.0), (8.0, 30.0)],
            "pre_context_prompt": "legacy-action-context",
        }

    async def _fake_analyze_unit_tasks_in_parallel(*, unit_tasks, pre_prune_results):
        assert len(unit_tasks) == 1
        assert len(pre_prune_results) == 1
        task = unit_tasks[0]
        assert task.get("unit_id") == "SU001"
        assert task.get("analysis_mode") == "tutorial_stepwise"
        assert "1-on-1 operational tutorial" in str(task.get("extra_prompt", ""))
        assert bool(pre_prune_results[0].get("applied")) is True

        class _FakeAnalysisResult:
            def __init__(self):
                self.success = True
                self.error_msg = ""
                self.clip_requests = [
                    {
                        "clip_id": "SU001/SU001_clip_step_01_mock_action",
                        "start_sec": 2.5,
                        "end_sec": 7.5,
                        "knowledge_type": "process",
                        "semantic_unit_id": "SU001",
                        "step_id": 1,
                        "step_description": "mock action",
                        "analysis_mode": "tutorial_stepwise",
                    }
                ]
                self.screenshot_requests = [
                    {
                        "screenshot_id": "SU001/SU001_ss_step_01_key_01_mock_action",
                        "timestamp_sec": 4.0,
                        "semantic_unit_id": "SU001",
                        "analysis_mode": "tutorial_stepwise",
                        "_relative_timestamp": 1.5,
                    }
                ]
                self.token_usage = {"prompt_tokens": 10, "completion_tokens": 2, "total_tokens": 12}
                self.analysis_results = []
                self.raw_response_json = []

        metadata = [
            {
                "unit_id": "SU001",
                "semantic_unit": dict(unit_tasks[0].get("semantic_unit", {})),
                "start_sec": 0.0,
                "end_sec": 30.0,
                "unit_duration": 30.0,
                "clip_path": str(Path("semantic_unit_clips_vl") / "SU001.mp4"),
                "vl_clip_path": str(Path("semantic_unit_clips_vl") / "SU001_legacy_action_pruned.mp4"),
                "pre_prune": pre_prune_results[0],
                "analysis_mode": "tutorial_stepwise",
            }
        ]
        return [_FakeAnalysisResult()], metadata, 1

    save_calls = []

    async def _fake_save_tutorial_assets_for_unit(**kwargs):
        save_calls.append(dict(kwargs))
        return None

    monkeypatch.setattr(generator, "_resolve_pre_prune_results_for_unit_tasks", _fake_resolve_pre_prune_results_for_unit_tasks)
    monkeypatch.setattr(generator, "_build_stable_action_material_requests_for_unit", _fake_build_stable_action_material_requests_for_unit)
    monkeypatch.setattr(generator, "_build_legacy_action_pre_prune_info_for_unit", _fake_build_legacy_action_pre_prune_info_for_unit)
    monkeypatch.setattr(generator, "_analyze_unit_tasks_in_parallel", _fake_analyze_unit_tasks_in_parallel)
    monkeypatch.setattr(generator, "_save_tutorial_assets_for_unit", _fake_save_tutorial_assets_for_unit)

    try:
        result = asyncio.run(
            generator.generate(
                video_path=str(video_path),
                semantic_units=semantic_units,
                output_dir=str(work_dir),
            )
        )

        assert result.success is True
        assert len(result.clip_requests) == 1
        assert result.clip_requests[0]["clip_id"] == "SU001/SU001_clip_step_01_mock_action"
        assert len(result.screenshot_requests) == 1
        assert result.screenshot_requests[0]["screenshot_id"] == "SU001/SU001_ss_step_01_key_01_mock_action"
        assert int(result.token_stats.get("stable_action_legacy_units", 0)) == 1
        assert int(result.token_stats.get("vl_units", 0)) == 1
        assert len(save_calls) == 1
        assert save_calls[0]["video_path"] == str(Path("semantic_unit_clips_vl") / "SU001.mp4")
        assert bool(save_calls[0].get("use_analysis_relative_timestamps")) is False
        assert bool(save_calls[0].get("prefer_screenshot_requests_keyframes")) is True
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


def test_generate_applies_incremental_dedupe_for_legacy_drop_tails(monkeypatch):
    work_dir = Path("var") / f"tmp_test_vl_legacy_drop_dedupe_{uuid4().hex}"
    work_dir.mkdir(parents=True, exist_ok=True)
    video_path = work_dir / "video.mp4"
    video_path.write_bytes(b"video")
    clips_dir = work_dir / "semantic_unit_clips_vl"
    clips_dir.mkdir(parents=True, exist_ok=True)

    generator = VLMaterialGenerator(
        {
            "enabled": True,
            "use_cache": False,
            "save_cache": False,
            "merge_multistep_clip_requests": False,
            "pre_vl_static_pruning": {
                "enabled": True,
                "only_process": True,
                "legacy_action_trigger_ratio": 1.0 / 3.0,
                "legacy_action_window_sec": 1.0,
            },
            "screenshot_optimization": {"enabled": False},
            "fallback": {"enabled": True},
        }
    )

    semantic_units = [
        {
            "unit_id": "SU007",
            "start_sec": 0.0,
            "end_sec": 30.0,
            "knowledge_type": "process",
            "mult_steps": True,
        }
    ]

    monkeypatch.setattr(generator, "_split_video_by_semantic_units", lambda *args, **kwargs: asyncio.sleep(0, result=str(clips_dir)))
    monkeypatch.setattr(
        generator,
        "_find_clip_for_unit",
        lambda clips_dir, unit_id, start_sec, end_sec: str(Path(clips_dir) / f"{unit_id}.mp4"),
    )

    async def _fake_resolve_pre_prune_results_for_unit_tasks(*, clips_dir, unit_tasks, force_preprocess=False):
        return [
            {
                "applied": True,
                "clip_path_for_vl": str(Path(clips_dir) / "SU007_pruned.mp4"),
                "kept_segments": [(2.0, 8.0)],
                "removed_segments": [(8.0, 30.0)],
                "stable_intervals_raw": [(0.0, 2.0), (8.0, 30.0)],
                "pre_context_prompt": "",
            }
        ]

    async def _fake_build_stable_action_material_requests_for_unit(*, clips_dir, semantic_unit, original_clip_path, pre_prune_info):
        return {
            "clip_requests": [],
            "screenshot_requests": [
                {
                    "screenshot_id": "SU007/SU007_ss_vl_action_drop_001_tail",
                    "timestamp_sec": 10.0,
                    "label": "action_drop_001_tail",
                    "semantic_unit_id": "SU007",
                    "analysis_mode": "legacy_action_units",
                    "anchor_role": "tail",
                },
                {
                    "screenshot_id": "SU007/SU007_ss_vl_action_drop_002_tail",
                    "timestamp_sec": 11.0,
                    "label": "action_drop_002_tail",
                    "semantic_unit_id": "SU007",
                    "analysis_mode": "legacy_action_units",
                    "anchor_role": "tail",
                },
                {
                    "screenshot_id": "SU007/SU007_ss_vl_action_003_tail",
                    "timestamp_sec": 12.0,
                    "label": "action_003_tail",
                    "semantic_unit_id": "SU007",
                    "analysis_mode": "legacy_action_units",
                    "anchor_role": "tail",
                },
            ],
            "action_segments": [(2.0, 8.0)],
        }

    async def _fake_build_legacy_action_pre_prune_info_for_unit(
        *,
        clips_dir,
        semantic_unit,
        original_clip_path,
        action_segments,
        base_pre_prune_info=None,
    ):
        return {
            "applied": True,
            "materialized": True,
            "clip_path_for_vl": str(Path(clips_dir) / "SU007_legacy_action_pruned.mp4"),
            "kept_segments": [(2.0, 8.0)],
            "removed_segments": [(0.0, 2.0), (8.0, 30.0)],
            "stable_intervals_raw": [(0.0, 2.0), (8.0, 30.0)],
            "pre_context_prompt": "legacy-action-context",
        }

    async def _fake_analyze_unit_tasks_in_parallel(*, unit_tasks, pre_prune_results):
        assert len(unit_tasks) == 1
        assert len(pre_prune_results) == 1
        assert unit_tasks[0].get("analysis_mode") == "tutorial_stepwise"

        class _FailedAnalysisResult:
            def __init__(self):
                self.success = False
                self.error_msg = "mock-failure"
                self.clip_requests = []
                self.screenshot_requests = []
                self.token_usage = {}
                self.analysis_results = []

        metadata = [
            {
                "unit_id": "SU007",
                "semantic_unit": dict(unit_tasks[0].get("semantic_unit", {})),
                "start_sec": 0.0,
                "end_sec": 30.0,
                "unit_duration": 30.0,
                "clip_path": str(Path("semantic_unit_clips_vl") / "SU007.mp4"),
                "vl_clip_path": str(Path("semantic_unit_clips_vl") / "SU007_legacy_action_pruned.mp4"),
                "pre_prune": pre_prune_results[0],
                "analysis_mode": "tutorial_stepwise",
            }
        ]
        return [_FailedAnalysisResult()], metadata, 1

    class _FakeCap:
        def __init__(self):
            self._frame_idx = 0

        def isOpened(self):
            return True

        def get(self, _prop):
            return 30.0

        def set(self, _prop, frame_idx):
            self._frame_idx = int(frame_idx)

        def read(self):
            return True, {"frame_idx": self._frame_idx}

        def release(self):
            return None

    import services.python_grpc.src.content_pipeline.phase2a.materials.vl_material_generator as vl_material_generator_module
    import services.python_grpc.src.vision_validation.worker as worker_module

    monkeypatch.setattr(
        vl_material_generator_module,
        "open_video_capture_with_fallback",
        lambda _video_path, logger=None, allow_inline_transcode=None: (_FakeCap(), "effective.mp4", False),
    )

    def _fake_extract_ocr_tokens(frame, _roi):
        frame_idx = int((frame or {}).get("frame_idx", 0))
        if frame_idx >= 330:
            return {"open", "settings"}
        return {"open"}

    def _fake_extract_shape_signature(_frame, _roi):
        return {"rect_count": 1, "component_count": 1, "edge_density": 0.1}

    def _fake_filter_incremental_screenshots(candidates):
        kept = []
        for base in candidates:
            base_tokens = set(base.get("ocr_tokens") or [])
            covered = False
            for cand in candidates:
                if cand is base:
                    continue
                cand_tokens = set(cand.get("ocr_tokens") or [])
                if base_tokens and base_tokens.issubset(cand_tokens) and len(cand_tokens) > len(base_tokens):
                    covered = True
                    break
            if not covered:
                kept.append(base)
        return kept

    monkeypatch.setattr(worker_module, "_extract_ocr_tokens", _fake_extract_ocr_tokens)
    monkeypatch.setattr(worker_module, "_extract_shape_signature", _fake_extract_shape_signature)
    monkeypatch.setattr(worker_module, "_filter_incremental_screenshots", _fake_filter_incremental_screenshots)

    monkeypatch.setattr(generator, "_resolve_pre_prune_results_for_unit_tasks", _fake_resolve_pre_prune_results_for_unit_tasks)
    monkeypatch.setattr(generator, "_build_stable_action_material_requests_for_unit", _fake_build_stable_action_material_requests_for_unit)
    monkeypatch.setattr(generator, "_build_legacy_action_pre_prune_info_for_unit", _fake_build_legacy_action_pre_prune_info_for_unit)
    monkeypatch.setattr(generator, "_analyze_unit_tasks_in_parallel", _fake_analyze_unit_tasks_in_parallel)

    try:
        result = asyncio.run(
            generator.generate(
                video_path=str(video_path),
                semantic_units=semantic_units,
                output_dir=str(work_dir),
            )
        )

        assert result.success is True
        kept_ids = {str(item.get("screenshot_id", "")) for item in result.screenshot_requests}
        assert "SU007/SU007_ss_vl_action_drop_001_tail" not in kept_ids
        assert "SU007/SU007_ss_vl_action_drop_002_tail" in kept_ids
        assert "SU007/SU007_ss_vl_action_003_tail" in kept_ids
        assert len(result.screenshot_requests) == 2
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


def test_resolve_pre_prune_results_for_unit_tasks_prefers_routing_reuse(tmp_path, monkeypatch):
    generator = _build_generator()

    clip1 = tmp_path / "u1.mp4"
    clip2 = tmp_path / "u2.mp4"
    clip3 = tmp_path / "u3.mp4"
    clip1.write_bytes(b"c1")
    clip2.write_bytes(b"c2")
    clip3.write_bytes(b"c3")

    pruned1 = tmp_path / "u1_pruned.mp4"
    pruned1.write_bytes(b"p1")

    called_units = []

    async def _fake_prepare_pruned_clips_for_units(*, clips_dir, unit_tasks, force_preprocess=False):
        results = []
        for task in unit_tasks:
            unit_id = task["semantic_unit"].get("unit_id")
            called_units.append(unit_id)
            results.append(
                {
                    "applied": True,
                    "clip_path_for_vl": str(task["clip_path"]),
                    "kept_segments": [(0.0, 1.0)],
                    "removed_segments": [],
                    "pre_context_prompt": f"calc-{unit_id}",
                }
            )
        return results

    monkeypatch.setattr(generator, "_prepare_pruned_clips_for_units", _fake_prepare_pruned_clips_for_units)

    unit_tasks = [
        {
            "semantic_unit": {
                "unit_id": "U1",
                "start_sec": 0.0,
                "end_sec": 10.0,
                "_routing_pre_prune": {
                    "applied": True,
                    "clip_path_for_vl": str(pruned1),
                    "kept_segments": [[0.0, 2.0]],
                    "removed_segments": [],
                    "pre_context_prompt": "reuse-u1",
                },
            },
            "clip_path": str(clip1),
        },
        {
            "semantic_unit": {
                "unit_id": "U2",
                "start_sec": 0.0,
                "end_sec": 10.0,
            },
            "clip_path": str(clip2),
        },
        {
            "semantic_unit": {
                "unit_id": "U3",
                "start_sec": 5.0,
                "end_sec": 15.0,
                "_routing_pre_prune": {
                    "applied": False,
                },
            },
            "clip_path": str(clip3),
        },
    ]

    results = asyncio.run(
        generator._resolve_pre_prune_results_for_unit_tasks(
            clips_dir=str(tmp_path),
            unit_tasks=unit_tasks,
            force_preprocess=False,
        )
    )

    assert called_units == ["U2"]
    assert len(results) == 3

    assert results[0]["applied"] is True
    assert results[0]["clip_path_for_vl"] == str(pruned1)
    assert results[0]["pre_context_prompt"] == "reuse-u1"

    assert results[1]["applied"] is True
    assert results[1]["pre_context_prompt"] == "calc-U2"

    assert results[2]["applied"] is False
    assert results[2]["clip_path_for_vl"] == str(clip3)
    assert results[2]["kept_segments"] == [(0.0, 10.0)]


def test_subtract_intervals_keeps_edges_for_stable_core_cut():
    """
    缁欏畾 stable=[1,5] 涓?keep_edge=1s 鏃讹紝浠呭墧闄?[2,4]锛屾渶缁堜繚鐣?[0,2] 鍜?[4,10]銆?    """
    generator = _build_generator()

    base = (0.0, 10.0)
    removed = [(2.0, 4.0)]
    keep = generator._subtract_intervals(base, removed, min_keep_segment_sec=0.5)

    assert len(keep) == 2
    assert abs(keep[0][0] - 0.0) < 1e-6
    assert abs(keep[0][1] - 2.0) < 1e-6
    assert abs(keep[1][0] - 4.0) < 1e-6
    assert abs(keep[1][1] - 10.0) < 1e-6


def test_map_pruned_relative_time_back_to_original_axis():
    """
    Keep segments [0,2] + [4,10]:
    - new relative 1.5s -> original 1.5s (inside first segment); new relative 3.0s -> original 5.0s (inside second segment, offset 1.0s)."""
    generator = _build_generator()
    kept = [(0.0, 2.0), (4.0, 10.0)]

    mapped_a = generator._map_pruned_relative_to_original(1.5, kept)
    mapped_b = generator._map_pruned_relative_to_original(3.0, kept)

    assert abs(mapped_a - 1.5) < 1e-6
    assert abs(mapped_b - 5.0) < 1e-6


def test_build_pruning_context_prompt_contains_topic_and_text():
    """
    Prompt should include knowledge_topic, text context, and kept/removed intervals."""
    generator = _build_generator()
    su: Dict[str, Any] = {
        "unit_id": "SU100",
        "knowledge_topic": "MCP setup",
        "text": "Install skill, configure params, then verify connectivity.",
    }

    prompt = generator._build_pruning_context_prompt(
        semantic_unit=su,
        kept_segments=[(0.0, 2.0), (4.0, 8.0)],
        removed_segments=[(2.0, 4.0)],
    )

    assert "MCP setup" in prompt
    assert "Install skill" in prompt
    assert "[0.00s-2.00s]" in prompt
    assert "[2.00s-4.00s]" in prompt


def test_token_saving_estimation_linear_seconds():
    """
    Validate the linear estimate logic for base token usage by kept duration.
    Example: original duration 10s, kept duration 4s, actual total_tokens=120.
    - 淇濈暀鏃堕暱 4s
    - 瀹為檯 total_tokens=120
    Then base estimate is about 120/4*10=300 and saved tokens are about 180."""
    generator = _build_generator()

    unit_duration = 10.0
    kept_segments = [(0.0, 2.0), (4.0, 6.0)]
    kept_duration = sum((e - s) for s, e in kept_segments)
    assert abs(kept_duration - 4.0) < 1e-6

    total_actual = 120
    prompt_actual = 80
    completion_actual = 40

    prompt_base = int(round(prompt_actual / kept_duration * unit_duration))
    completion_base = int(round(completion_actual / kept_duration * unit_duration))
    total_base = prompt_base + completion_base
    saved = total_base - total_actual

    assert total_base == 300
    assert saved == 180


def test_removed_intervals_require_stable_longer_than_3s():
    """
    New contract: stable original duration must be >3s to be pruned. 3.0s (e.g. 1-4) is not pruned;
    3.1s (e.g. 1-4.1) can prune core interval."""
    generator = _build_generator()

    # 边界：正好 3.0s，不应剔除
    removed_a = generator._build_removed_intervals_from_stable([(1.0, 4.0)])
    assert removed_a == []

    # 超过 3.0s，应剔除 [2.0, 3.1]
    removed_b = generator._build_removed_intervals_from_stable([(1.0, 4.1)])
    assert len(removed_b) == 1
    assert abs(removed_b[0][0] - 2.0) < 1e-6
    assert abs(removed_b[0][1] - 3.1) < 1e-6


def test_map_pruned_interval_to_original_segments_cross_gap():
    """
    When pruned interval crosses removed gaps, map to multiple original segments: kept=[0,2]+[4,6], pruned [1,3] -> original [1,2] + [4,5].
    """
    generator = _build_generator()
    kept = [(0.0, 2.0), (4.0, 6.0)]

    mapped = generator._map_pruned_interval_to_original_segments(1.0, 3.0, kept)
    assert len(mapped) == 2
    assert abs(mapped[0][0] - 1.0) < 1e-6
    assert abs(mapped[0][1] - 2.0) < 1e-6
    assert abs(mapped[1][0] - 4.0) < 1e-6
    assert abs(mapped[1][1] - 5.0) < 1e-6


def test_find_clip_for_unit_avoids_substring_collision(tmp_path):
    """
    SU01 and SU010 can co-exist; substring matching must not select wrong clip."""
    generator = _build_generator()
    clips_dir = tmp_path / "clips"
    clips_dir.mkdir(parents=True, exist_ok=True)

    file_su01 = clips_dir / "001_SU01_topic_10.00-20.00.mp4"
    file_su010 = clips_dir / "002_SU010_topic_30.00-40.00.mp4"
    file_su01.write_bytes(b"x")
    file_su010.write_bytes(b"x")

    selected = generator._find_clip_for_unit(
        clips_dir=str(clips_dir),
        unit_id="SU01",
        start_sec=10.0,
        end_sec=20.0,
    )

    assert selected is not None
    assert selected.endswith(file_su01.name)


def test_split_complete_sentences_by_pause_threshold():
    """
    Pauses >= 0.3s should split into a new spoken sentence."""
    generator = _build_generator()

    subtitles = [
        {"start_sec": 0.0, "end_sec": 0.5, "text": "first part"},
        {"start_sec": 0.5, "end_sec": 1.0, "text": "first tail"},
        {"start_sec": 1.4, "end_sec": 1.8, "text": "second"},
    ]

    sentences = generator._split_complete_sentences_by_pause(subtitles)
    assert len(sentences) == 2
    assert abs(sentences[0]["start_sec"] - 0.0) < 1e-6
    assert abs(sentences[0]["end_sec"] - 1.0) < 1e-6
    assert abs(sentences[1]["start_sec"] - 1.4) < 1e-6


def test_refine_kept_segments_before_concat_applies_semantic_physical_and_buffers(tmp_path, monkeypatch):
    """
    Before concat, kept segments after stable-prune should pass:
    1) semantic sentence-head pullback; 2) end merge by MSE jump; 3) speech-stream buffer."""
    generator = _build_generator()

    async def _fake_detect_segment_mse_jump_end(clip_path, semantic_end_sec, clip_duration_sec):
        return min(clip_duration_sec, semantic_end_sec + 0.5)

    monkeypatch.setattr(
        generator,
        "_detect_segment_mse_jump_end",
        _fake_detect_segment_mse_jump_end,
    )
    class _RepoStub:
        def list_subtitles(self):
            return [
                {"start_sec": 100.0, "end_sec": 100.4, "text": "look next"},
                {"start_sec": 100.4, "end_sec": 101.0, "text": "step one"},
                {"start_sec": 101.5, "end_sec": 102.5, "text": "wrap up"},
            ]

        def build_relative_subtitles(self, *, unit_start_sec: float, unit_end_sec: float):
            if unit_end_sec <= unit_start_sec:
                return []
            unit_duration = unit_end_sec - unit_start_sec
            result = []
            for sub in self.list_subtitles():
                sub_start = float(sub["start_sec"])
                sub_end = float(sub["end_sec"])
                if sub_end <= unit_start_sec or sub_start >= unit_end_sec:
                    continue
                rel_start = max(0.0, sub_start - unit_start_sec)
                rel_end = min(unit_duration, sub_end - unit_start_sec)
                if rel_end <= rel_start:
                    continue
                result.append({"start_sec": rel_start, "end_sec": rel_end, "text": sub["text"]})
            return result

    monkeypatch.setattr(generator, "_get_subtitle_repo_for_output_dir", lambda _output_dir: _RepoStub())

    clips_dir = tmp_path / "semantic_unit_clips_vl"
    clips_dir.mkdir(parents=True, exist_ok=True)

    refined = asyncio.run(
        generator._refine_kept_segments_before_concat(
            clips_dir=str(clips_dir),
            semantic_unit={"unit_id": "SU999", "start_sec": 100.0, "end_sec": 103.0},
            original_clip_path=str(clips_dir / "dummy.mp4"),
            kept_segments=[(0.5, 0.8)],
        )
    )

    assert len(refined) == 1
    # 起点：回拉到语义句头并加 -0.2s 缓冲后截断到 0
    assert abs(refined[0][0] - 0.0) < 1e-6
    # 终点：语义句末 1.0s -> MSE 跃变 1.5s -> +0.3s 缓冲 = 1.8s
    assert abs(refined[0][1] - 1.8) < 1e-6


