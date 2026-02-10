"""VideoClip 的字幕与场景辅助能力。"""

from __future__ import annotations

from typing import Dict, List

from services.python_grpc.src.content_pipeline.shared.subtitle.subtitle_utils import (
    extract_subtitle_text_in_range,
)


async def check_scene_switch(extractor, timestamp: float) -> bool:
    features = await extractor.visual_extractor.extract_visual_features(
        max(0, timestamp - 0.5),
        timestamp + 0.5,
        sample_rate=4,
    )
    return bool(features.is_dynamic and features.confidence > 0.6)


def get_subtitles_near(extractor, t: float, before_s: float, after_s: float) -> List[Dict]:
    if not extractor.subtitles:
        return []

    start_search = t - before_s
    end_search = t + after_s

    results: List[Dict] = []
    for subtitle in extractor.subtitles:
        if isinstance(subtitle, dict):
            subtitle_start = float(subtitle.get("start_sec", 0))
            subtitle_end = float(subtitle.get("end_sec", 0))
            text = subtitle.get("text", "")
            corrected = subtitle.get("corrected_text", "")
        else:
            subtitle_start = float(getattr(subtitle, "start_sec", 0))
            subtitle_end = float(getattr(subtitle, "end_sec", 0))
            text = getattr(subtitle, "text", "")
            corrected = getattr(subtitle, "corrected_text", "")

        if subtitle_start < end_search and subtitle_end > start_search:
            results.append(
                {
                    "text": text,
                    "corrected_text": corrected,
                    "start_sec": subtitle_start,
                    "end_sec": subtitle_end,
                }
            )
    return results


def has_transition_at_boundary(extractor, timestamp: float, is_start: bool) -> bool:
    subtitles = extractor.subtitles
    if not subtitles:
        return False

    start = timestamp - 1.0 if is_start else timestamp - 0.5
    end = timestamp + 0.5 if is_start else timestamp + 1.0
    text = extract_subtitle_text_in_range(subtitles, start, end)
    return any(keyword in text for keyword in extractor.TRANSITION_KEYWORDS)
