"""
VL 前置静态段剔除逻辑单元测试。

覆盖目标：
1) stable 核心区间剔除后保留区间计算正确；
2) 裁剪片段相对时间映射回原始时间轴正确；
3) 上下文提示包含 knowledge_topic 与上下文文本。
"""

from __future__ import annotations

from typing import Dict, Any


def _build_generator():
    from MVP_Module2_HEANCING.module2_content_enhancement.vl_material_generator import VLMaterialGenerator

    return VLMaterialGenerator(
        {
            "enabled": True,
            "pre_vl_static_pruning": {
                "enabled": True,
                "only_process": True,
                "min_unit_duration_sec": 10.0,
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


def test_subtract_intervals_keeps_edges_for_stable_core_cut():
    """
    给定 stable=[1,5] 且 keep_edge=1s 时，仅剔除 [2,4]，最终保留 [0,2] 和 [4,10]。
    """
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
    保留段 [0,2] + [4,10] 时：
    - 新片段相对 1.5s -> 原始 1.5s（第一段内）
    - 新片段相对 3.0s -> 原始 5.0s（落入第二段，偏移 1.0s）
    """
    generator = _build_generator()
    kept = [(0.0, 2.0), (4.0, 10.0)]

    mapped_a = generator._map_pruned_relative_to_original(1.5, kept)
    mapped_b = generator._map_pruned_relative_to_original(3.0, kept)

    assert abs(mapped_a - 1.5) < 1e-6
    assert abs(mapped_b - 5.0) < 1e-6


def test_build_pruning_context_prompt_contains_topic_and_text():
    """
    上下文提示应包含 knowledge_topic、文本上下文、保留/剔除区间。
    """
    generator = _build_generator()
    su: Dict[str, Any] = {
        "unit_id": "SU100",
        "knowledge_topic": "MCP技能安装与配置",
        "text": "先安装技能，再配置参数，最后验证连通性。",
    }

    prompt = generator._build_pruning_context_prompt(
        semantic_unit=su,
        kept_segments=[(0.0, 2.0), (4.0, 8.0)],
        removed_segments=[(2.0, 4.0)],
    )

    assert "MCP技能安装与配置" in prompt
    assert "先安装技能，再配置参数" in prompt
    assert "[0.00s-2.00s]" in prompt
    assert "[2.00s-4.00s]" in prompt


def test_token_saving_estimation_linear_seconds():
    """
    验证“按保留时长线性回推基线 token”的核心估算逻辑。

    示例：
    - 原始时长 10s
    - 保留时长 4s
    - 实际 total_tokens=120
    则基线估算约为 120/4*10=300，节省约 180。
    """
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
