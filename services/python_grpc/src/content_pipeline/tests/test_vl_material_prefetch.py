"""
VLMaterialGenerator 棰勮绛栫暐鍗曞厓娴嬭瘯

鐩爣锛氶獙璇佲€滄寜鏃堕棿鑱氱被鎴?chunk + Union 棰勮鈥濈殑浠诲姟鏋勫缓閫昏緫锛岀‘淇濅笉浼氶€€鍖栦负閫愯姹傞殢鏈鸿闂€?"""

from __future__ import annotations

import asyncio

from typing import Any, Dict, List


def test_resolve_adaptive_prefetch_step_keeps_base_rate_under_cap():
    from services.python_grpc.src.content_pipeline.infra.runtime.vl_prefetch_utils import (
        resolve_adaptive_prefetch_step,
    )

    step = resolve_adaptive_prefetch_step(
        start_frame=0,
        end_frame=120,
        sample_rate=2,
        max_frames_per_chunk=240,
    )

    assert step == 2


def test_resolve_adaptive_prefetch_step_raises_rate_over_cap():
    from services.python_grpc.src.content_pipeline.infra.runtime.vl_prefetch_utils import (
        resolve_adaptive_prefetch_step,
    )

    # 跨度 3001 帧，若步长=1 会超出 cap=240，需自动放大步长。
    step = resolve_adaptive_prefetch_step(
        start_frame=0,
        end_frame=3000,
        sample_rate=1,
        max_frames_per_chunk=240,
    )

    assert step >= 13


def test_chunking_groups_requests_by_span():
    from services.python_grpc.src.content_pipeline.phase2a.materials.vl_material_generator import VLMaterialGenerator

    generator = VLMaterialGenerator(
        {
            "enabled": True,
            "screenshot_optimization": {
                "prefetch_union_max_span_seconds": 10.0,
                "prefetch_chunk_max_requests": 1000,
            },
        }
    )

    requests: List[Dict[str, Any]] = [
        {"unit_id": "u1", "timestamp_sec": 0.2},
        {"unit_id": "u2", "timestamp_sec": 0.9},
        {"unit_id": "u3", "timestamp_sec": 1.7},
    ]

    chunks = generator._build_screenshot_prefetch_chunks(
        screenshot_requests=requests,
        time_window=1.0,
        max_span_seconds=10.0,
        max_requests=1000,
    )

    assert len(chunks) == 1
    assert len(chunks[0]["windows"]) == 3
    assert chunks[0]["union_start"] == 0.0  # min(ts-1.0) clamp to 0
    assert abs(chunks[0]["union_end"] - 2.7) < 1e-6  # max(ts+1.0)


def test_task_params_filter_ts_map():
    from services.python_grpc.src.content_pipeline.phase2a.materials.vl_material_generator import VLMaterialGenerator

    generator = VLMaterialGenerator({"enabled": True, "screenshot_optimization": {}})

    requests: List[Dict[str, Any]] = [
        {"unit_id": "u1", "timestamp_sec": 0.2},
        {"unit_id": "u2", "timestamp_sec": 0.9},
    ]

    chunks = generator._build_screenshot_prefetch_chunks(
        screenshot_requests=requests,
        time_window=1.0,
        max_span_seconds=10.0,
        max_requests=1000,
    )
    windows = chunks[0]["windows"]

    # Encoding fixed: Union note.
    ts_to_shm_ref = {0.0: {"shm_name": "a", "shape": (1,), "dtype": "uint8"}, 1.0: {"shm_name": "b", "shape": (1,), "dtype": "uint8"}}

    task_params = generator._build_task_params_from_ts_map(windows=windows, ts_to_shm_ref=ts_to_shm_ref, fps=30.0)

    assert len(task_params) == 2
    assert all(not p.get("skip") for p in task_params)
    assert all(p.get("shm_frames") for p in task_params)


def test_chunking_prefers_request_level_window_over_global_time_window():
    from services.python_grpc.src.content_pipeline.phase2a.materials.vl_material_generator import VLMaterialGenerator

    generator = VLMaterialGenerator(
        {
            "enabled": True,
            "screenshot_optimization": {
                "prefetch_union_max_span_seconds": 10.0,
                "prefetch_chunk_max_requests": 1000,
            },
        }
    )

    requests: List[Dict[str, Any]] = [
        {"unit_id": "u1", "timestamp_sec": 5.0, "_window_start_sec": 4.2, "_window_end_sec": 4.8},
        {"unit_id": "u2", "timestamp_sec": 8.0},
    ]

    chunks = generator._build_screenshot_prefetch_chunks(
        screenshot_requests=requests,
        time_window=1.0,
        max_span_seconds=20.0,
        max_requests=1000,
    )

    assert len(chunks) == 1
    windows = chunks[0]["windows"]
    assert len(windows) == 2
    win_u1 = next(item for item in windows if item["unit_id"] == "u1")
    win_u2 = next(item for item in windows if item["unit_id"] == "u2")
    assert abs(float(win_u1["expanded_start"]) - 4.2) < 1e-6
    assert abs(float(win_u1["expanded_end"]) - 4.8) < 1e-6
    assert abs(float(win_u2["expanded_start"]) - 7.0) < 1e-6
    assert abs(float(win_u2["expanded_end"]) - 9.0) < 1e-6


def test_chunking_supports_asymmetric_time_window():
    from services.python_grpc.src.content_pipeline.phase2a.materials.vl_material_generator import VLMaterialGenerator

    generator = VLMaterialGenerator(
        {
            "enabled": True,
            "screenshot_optimization": {
                "prefetch_union_max_span_seconds": 20.0,
                "prefetch_chunk_max_requests": 1000,
            },
        }
    )

    requests: List[Dict[str, Any]] = [
        {"unit_id": "u1", "timestamp_sec": 10.0},
    ]

    chunks = generator._build_screenshot_prefetch_chunks(
        screenshot_requests=requests,
        max_span_seconds=20.0,
        max_requests=1000,
        time_window_before=1.0,
        time_window_after=2.0,
    )

    assert len(chunks) == 1
    assert len(chunks[0]["windows"]) == 1
    window = chunks[0]["windows"][0]
    assert abs(float(window["expanded_start"]) - 9.0) < 1e-6
    assert abs(float(window["expanded_end"]) - 12.0) < 1e-6


def test_resolve_max_workers_respects_explicit_limit_with_injected_executor():
    from services.python_grpc.src.content_pipeline.infra.runtime.vl_prefetch_utils import resolve_max_workers

    class _FakeExecutor:
        _max_workers = 4

    resolved = resolve_max_workers(
        request_count=10,
        cv_executor=_FakeExecutor(),
        screenshot_config={"max_workers": 1},
        hard_cap=6,
    )

    assert resolved == 1


def test_screenshot_task_gate_serializes_tasks_when_limit_is_one(monkeypatch):
    from services.python_grpc.src.content_pipeline.phase2a.materials import vl_material_generator as generator_module
    from services.python_grpc.src.content_pipeline.phase2a.materials.vl_material_generator import VLMaterialGenerator

    async def _main() -> None:
        generator_module._SCREENSHOT_TASK_GATES.clear()
        generator_module._SCREENSHOT_TASK_GATE_LIMITS.clear()

        generator_a = VLMaterialGenerator(
            {
                "enabled": True,
                "screenshot_optimization": {
                    "streaming_pipeline": True,
                    "task_max_concurrency": 1,
                },
            }
        )
        generator_b = VLMaterialGenerator(
            {
                "enabled": True,
                "screenshot_optimization": {
                    "streaming_pipeline": True,
                    "task_max_concurrency": 1,
                },
            }
        )

        release_event = asyncio.Event()
        first_started = asyncio.Event()
        inflight = 0
        max_inflight = 0

        async def _fake_streaming(video_path: str, screenshot_requests: List[Dict[str, Any]]):
            nonlocal inflight, max_inflight
            _ = video_path
            inflight += 1
            max_inflight = max(max_inflight, inflight)
            first_started.set()
            await release_event.wait()
            inflight -= 1
            return screenshot_requests

        monkeypatch.setattr(generator_a, "_optimize_screenshots_streaming_pipeline", _fake_streaming)
        monkeypatch.setattr(generator_b, "_optimize_screenshots_streaming_pipeline", _fake_streaming)

        task_a = asyncio.create_task(generator_a._optimize_screenshots_parallel("video.mp4", [{"id": "a"}]))
        await first_started.wait()
        task_b = asyncio.create_task(generator_b._optimize_screenshots_parallel("video.mp4", [{"id": "b"}]))
        await asyncio.sleep(0.05)

        assert inflight == 1
        assert max_inflight == 1

        release_event.set()
        await asyncio.gather(task_a, task_b)

    asyncio.run(_main())
