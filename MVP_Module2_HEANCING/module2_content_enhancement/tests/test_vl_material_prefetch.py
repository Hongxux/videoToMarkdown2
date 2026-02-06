"""
VLMaterialGenerator 预读策略单元测试

目标：验证“Union 预读”能把多次 extract_frames_fast 合并为 1 次，避免短片段高频 seek 导致看起来“没开多进程”。
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple


class _FakeExtractor:
    def __init__(self, fps: float = 30.0):
        self.fps = fps
        self.calls: List[Tuple[float, float, int, int]] = []

    def extract_frames_fast(
        self, *, start_sec: float, end_sec: float, sample_rate: int = 2, target_height: int = 360
    ) -> Tuple[List[Any], List[float]]:
        self.calls.append((start_sec, end_sec, sample_rate, target_height))
        # 构造 0.0~2.0 的候选时间戳，模拟已写入 SHM 的帧集合
        timestamps = [0.0, 0.5, 1.0, 1.5, 2.0]
        frames = [object() for _ in timestamps]
        return frames, timestamps


class _FakeShmRegistry:
    def get_shm_ref(self, frame_idx: int) -> str:
        return f"shm://frame/{frame_idx}"


def test_union_prefetch_calls_extract_once():
    from MVP_Module2_HEANCING.module2_content_enhancement.vl_material_generator import VLMaterialGenerator

    generator = VLMaterialGenerator(
        {
            "enabled": True,
            "screenshot_optimization": {
                # 降低阈值，方便测试触发 Union 预读
                "prefetch_union_min_requests": 3,
                "prefetch_union_max_span_seconds": 10.0,
                "prefetch_sample_rate": 2,
                "prefetch_target_height": 360,
            },
        }
    )

    requests: List[Dict[str, Any]] = [
        {"unit_id": "u1", "timestamp_sec": 0.2},
        {"unit_id": "u2", "timestamp_sec": 0.9},
        {"unit_id": "u3", "timestamp_sec": 1.7},
    ]

    extractor = _FakeExtractor(fps=30.0)
    shm_registry = _FakeShmRegistry()

    task_params = generator._build_parallel_cv_task_params(
        extractor=extractor,
        shm_registry=shm_registry,
        screenshot_requests=requests,
        time_window=1.0,
    )

    assert len(extractor.calls) == 1, "Union 预读应只调用一次 extract_frames_fast"
    assert len(task_params) == 3
    assert all(p.get("skip") is False for p in task_params)
    assert all(p.get("shm_frames") for p in task_params), "每个请求应得到非空候选帧集合"

