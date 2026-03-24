"""
模块说明：VL 预裁剪区间工具。

执行逻辑：
1) 归一化并合并区间。
2) 计算区间差集。
3) 根据稳定区间生成剔除区间。

核心价值：将区间算法与生成器主流程分离，便于复用与测试。
"""

from typing import List, Tuple


def normalize_intervals(intervals: List[Tuple[float, float]], min_duration_sec: float = 1e-6) -> List[Tuple[float, float]]:
    """排序并合并重叠/相邻区间。"""
    if not intervals:
        return []

    ordered = sorted(
        [(float(start_sec), float(end_sec)) for start_sec, end_sec in intervals if float(end_sec) - float(start_sec) > min_duration_sec],
        key=lambda item: item[0],
    )
    if not ordered:
        return []

    merged: List[Tuple[float, float]] = [ordered[0]]
    for start_sec, end_sec in ordered[1:]:
        last_start, last_end = merged[-1]
        if start_sec <= last_end + 1e-6:
            merged[-1] = (last_start, max(last_end, end_sec))
        else:
            merged.append((start_sec, end_sec))
    return merged


def subtract_intervals(
    base_interval: Tuple[float, float],
    removed_intervals: List[Tuple[float, float]],
    min_keep_segment_sec: float,
) -> List[Tuple[float, float]]:
    """在 base 区间内扣除 removed 区间，返回保留区间。"""
    base_start, base_end = base_interval
    if base_end <= base_start:
        return []

    normalized_removed = normalize_intervals(removed_intervals)
    keep: List[Tuple[float, float]] = []
    cursor = base_start

    for remove_start, remove_end in normalized_removed:
        if remove_end <= base_start or remove_start >= base_end:
            continue
        cut_start = max(base_start, remove_start)
        cut_end = min(base_end, remove_end)
        if cut_start > cursor and (cut_start - cursor) >= min_keep_segment_sec:
            keep.append((cursor, cut_start))
        cursor = max(cursor, cut_end)

    if base_end > cursor and (base_end - cursor) >= min_keep_segment_sec:
        keep.append((cursor, base_end))
    return keep


def build_removed_intervals_from_stable(
    stable_intervals: List[Tuple[float, float]],
    *,
    min_stable_interval_sec: float,
    keep_edge_sec: float,
    min_cut_span_sec: float,
) -> List[Tuple[float, float]]:
    """根据稳定区间构造可剔除的核心区间。"""
    removed_intervals: List[Tuple[float, float]] = []
    for stable_start, stable_end in stable_intervals:
        stable_duration = max(0.0, stable_end - stable_start)
        if stable_duration <= min_stable_interval_sec:
            continue

        core_start = stable_start + keep_edge_sec
        core_end = stable_end - keep_edge_sec
        if core_end - core_start >= min_cut_span_sec:
            removed_intervals.append((core_start, core_end))

    return normalize_intervals(removed_intervals)

