"""
模块说明：RichTextPipeline 时间轴与区间工具。

执行逻辑：
1) 对齐字幕时间边界。
2) 安全裁剪时间区间。
3) 合并动作段并计算动作包络。

核心价值：把“时间处理规则”从主编排类中拆出，职责更清晰。
"""

from typing import Any, Dict, List, Tuple


def align_to_sentence_start(subtitles: List[Any], timestamp: float) -> float:
    """向前对齐到不大于 timestamp 的最近句子起点。"""
    best_start = 0.0
    for subtitle in subtitles:
        if subtitle.start_sec <= timestamp:
            best_start = subtitle.start_sec
        else:
            break
    return best_start


def align_to_sentence_end(subtitles: List[Any], timestamp: float) -> float:
    """向后对齐到不小于 timestamp 的最近句子终点。"""
    for subtitle in subtitles:
        if subtitle.end_sec >= timestamp:
            return subtitle.end_sec
    return subtitles[-1].end_sec if subtitles else timestamp


def clamp_time_range(start_sec: float, end_sec: float, video_duration: float = 0.0) -> Tuple[float, float]:
    """对区间做安全裁剪，避免负值、反向或越界。"""
    try:
        start = float(start_sec)
    except Exception:
        start = 0.0

    try:
        end = float(end_sec)
    except Exception:
        end = start

    start = max(0.0, start)
    end = max(start, end)

    if video_duration and video_duration > 0:
        max_end = float(video_duration)
        start = max(0.0, min(start, max_end))
        end = max(start, min(end, max_end))

    return start, end


def merge_action_segments(
    action_segments: List[Dict[str, Any]],
    gap_threshold_sec: float = 5.0,
) -> List[Dict[str, Any]]:
    """将相邻且间隔较小的动作段合并。"""
    if not action_segments:
        return []
    if len(action_segments) == 1:
        return [action_segments[0].copy()]

    merged_actions: List[Dict[str, Any]] = []
    current = action_segments[0].copy()

    for next_action in action_segments[1:]:
        next_copy = next_action.copy()
        gap = float(next_copy.get("start_sec", 0)) - float(current.get("end_sec", 0))

        if gap < gap_threshold_sec:
            current["end_sec"] = max(float(current.get("end_sec", 0)), float(next_copy.get("end_sec", 0)))
            current_islands = current.get("internal_stable_islands", [])
            next_islands = next_copy.get("internal_stable_islands", [])
            current["internal_stable_islands"] = current_islands + next_islands
        else:
            merged_actions.append(current)
            current = next_copy

    merged_actions.append(current)
    return merged_actions


def compute_action_envelope(
    unit: Any,
    action_start: float,
    action_end: float,
    sentence_start: float,
    sentence_end: float,
    knowledge_type: str,
    *,
    short_unit_threshold_sec: float = 20.0,
    pre_buffer_sec: float = 0.4,
    post_buffer_sec: float = 1.0,
    video_duration: float = 0.0,
) -> Tuple[float, float]:
    """根据知识类型与句子边界计算动作包络区间。"""
    knowledge_type_value = (knowledge_type or "").strip()
    target_keywords = ("实操", "推演", "环境配置", "配置")
    is_target_type = any(keyword in knowledge_type_value for keyword in target_keywords)

    unit_start = float(getattr(unit, "start_sec", 0.0))
    unit_end = float(getattr(unit, "end_sec", 0.0))
    unit_duration = unit_end - unit_start

    base_start = min(float(action_start), float(sentence_start))
    base_end = max(float(action_end), float(sentence_end))

    if unit_duration > 0 and unit_duration <= short_unit_threshold_sec and is_target_type:
        start_sec, end_sec = unit_start, unit_end
    elif is_target_type:
        start_sec = base_start - pre_buffer_sec
        end_sec = base_end + post_buffer_sec
    else:
        start_sec, end_sec = base_start, base_end

    end_sec = min(end_sec, unit_end)
    return clamp_time_range(start_sec, end_sec, video_duration=video_duration)

