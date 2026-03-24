"""VideoClip 边界辅助工具。"""

from __future__ import annotations

from typing import Dict, Optional, Tuple


def check_boundary_overlap(extractor, target_start, target_end) -> Tuple[bool, Optional[Dict]]:
    for segment in extractor.confirmed_segments:
        if not (target_end <= segment["start"] or target_start >= segment["end"]):
            return True, segment
    return False, None


def judge_sentence_completeness_no_punc(text: str) -> bool:
    if not text:
        return False
    subject_words = {"我们", "我", "这个", "这", "算法", "公式", "它", "大家"}
    predicate_words = {"看", "讲", "分析", "总结", "推导", "理解", "做", "算", "求"}
    has_subject_predicate = any(word in text for word in subject_words) and any(
        word in text for word in predicate_words
    )
    return has_subject_predicate and len(text) >= 5


def add_speech_flow_padding(extractor, start_time: float, end_time: float) -> Tuple[float, float]:
    final_start = max(0, start_time - 0.2)
    final_end = end_time + 0.3

    if extractor.confirmed_segments:
        previous_segment = extractor.confirmed_segments[-1]
        if final_start < previous_segment["end"]:
            if previous_segment["end"] >= final_end - 0.1:
                final_start = previous_segment["end"]
            else:
                final_start = max(final_start, previous_segment["end"])

    return final_start, final_end
