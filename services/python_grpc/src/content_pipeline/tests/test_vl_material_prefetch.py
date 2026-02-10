"""
VLMaterialGenerator 棰勮绛栫暐鍗曞厓娴嬭瘯

鐩爣锛氶獙璇佲€滄寜鏃堕棿鑱氱被鎴?chunk + Union 棰勮鈥濈殑浠诲姟鏋勫缓閫昏緫锛岀‘淇濅笉浼氶€€鍖栦负閫愯姹傞殢鏈鸿闂€?"""

from __future__ import annotations

from typing import Any, Dict, List


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


