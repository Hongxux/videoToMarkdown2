"""
VLMaterialGenerator 预读策略单元测试

目标：验证“按时间聚类成 chunk + Union 预读”的任务构建逻辑，确保不会退化为逐请求随机访问。
"""

from __future__ import annotations

from typing import Any, Dict, List


def test_chunking_groups_requests_by_span():
    from MVP_Module2_HEANCING.module2_content_enhancement.vl_material_generator import VLMaterialGenerator

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
    from MVP_Module2_HEANCING.module2_content_enhancement.vl_material_generator import VLMaterialGenerator

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

    # 模拟 Union 预读返回的候选时间戳集合
    ts_to_shm_ref = {0.0: {"shm_name": "a", "shape": (1,), "dtype": "uint8"}, 1.0: {"shm_name": "b", "shape": (1,), "dtype": "uint8"}}

    task_params = generator._build_task_params_from_ts_map(windows=windows, ts_to_shm_ref=ts_to_shm_ref, fps=30.0)

    assert len(task_params) == 2
    assert all(not p.get("skip") for p in task_params)
    assert all(p.get("shm_frames") for p in task_params)
