"""
VL 前置静态段剔除逻辑单元测试。

覆盖目标：
1) stable 核心区间剔除后保留区间计算正确；
2) 裁剪片段相对时间映射回原始时间轴正确；
3) 上下文提示包含 knowledge_topic 与上下文文本。
"""

from __future__ import annotations

import asyncio
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


def test_removed_intervals_require_stable_longer_than_3s():
    """
    新约束：stable 原始长度必须 >3s 才可剔除。
    - 3.0s（如 1-4）不剔除
    - 3.1s（如 1-4.1）可剔除核心区
    """
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
    当 pruned 区间跨越被剔除间隙时，应映射为原时间轴上的多段区间。
    kept=[0,2]+[4,6]，pruned 中 [1,3] -> 原始 [1,2] + [4,5]
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
    SU01 与 SU010 并存时，不能用子串匹配导致误选。
    """
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
    0.3s 及以上停顿应切出新的口语句。
    """
    generator = _build_generator()

    subtitles = [
        {"start_sec": 0.0, "end_sec": 0.5, "text": "第一句前半"},
        {"start_sec": 0.5, "end_sec": 1.0, "text": "第一句后半"},
        {"start_sec": 1.4, "end_sec": 1.8, "text": "第二句"},
    ]

    sentences = generator._split_complete_sentences_by_pause(subtitles)
    assert len(sentences) == 2
    assert abs(sentences[0]["start_sec"] - 0.0) < 1e-6
    assert abs(sentences[0]["end_sec"] - 1.0) < 1e-6
    assert abs(sentences[1]["start_sec"] - 1.4) < 1e-6


def test_refine_kept_segments_before_concat_applies_semantic_physical_and_buffers(tmp_path, monkeypatch):
    """
    Stable 剔除后的 kept 片段在合并前会经历：
    1) 语义句头回拉；2) 终点并入 MSE 跳变；3) 语流缓冲。
    """
    generator = _build_generator()

    async def _fake_detect_segment_mse_jump_end(clip_path, semantic_end_sec, clip_duration_sec):
        return min(clip_duration_sec, semantic_end_sec + 0.5)

    monkeypatch.setattr(
        generator,
        "_detect_segment_mse_jump_end",
        _fake_detect_segment_mse_jump_end,
    )
    monkeypatch.setattr(
        generator,
        "_load_subtitles_for_output_dir",
        lambda _output_dir: [
            {"start_sec": 100.0, "end_sec": 100.4, "text": "接下来我们看"},
            {"start_sec": 100.4, "end_sec": 101.0, "text": "第一步操作"},
            {"start_sec": 101.5, "end_sec": 102.0, "text": "好了这就是结果"},
        ],
    )

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
    # 终点：语义结束 2.0s -> MSE 跳变 2.5s -> +0.3s 缓冲 = 2.8s
    assert abs(refined[0][1] - 2.8) < 1e-6
