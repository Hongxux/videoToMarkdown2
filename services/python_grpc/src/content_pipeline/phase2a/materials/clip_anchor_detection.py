"""VideoClip 锚点检测相关流程。"""

from __future__ import annotations

import asyncio
import logging

import cv2

logger = logging.getLogger(__name__)


async def detect_best_physical_anchors(
    extractor,
    video_path,
    s_scan,
    e_scan,
    asr_s,
    asr_e,
    fault_text="",
):
    """在扫描窗口内通过视觉变化寻找更优物理锚点。"""
    _ = video_path, fault_text

    base_start_threshold = extractor.ACTION_START_THRESHOLD
    base_end_threshold = extractor.ACTION_END_THRESHOLD

    frames, timestamps = extractor.visual_extractor.extract_frames_fast(
        s_scan,
        e_scan,
        sample_rate=2,
        target_height=360,
    )
    if len(frames) < 2:
        return asr_s, asr_e

    try:
        from services.python_grpc.src.content_pipeline.phase2a.vision.visual_feature_extractor import (
            get_visual_process_pool,
        )

        loop = asyncio.get_running_loop()
        executor = get_visual_process_pool()

        _, buf = cv2.imencode(".jpg", frames[0], [cv2.IMWRITE_JPEG_QUALITY, 85])
        sample_node = await loop.run_in_executor(
            executor,
            extractor.visual_extractor.visual_detector.analyze_frame,
            buf.tobytes(),
        )

        if sample_node.get("rect_count", 0) > 3 or sample_node.get("rectangle_count", 0) > 3:
            base_start_threshold = 80
            base_end_threshold = 64
            logger.info("🎵 [Video Type]: PPT/Diagram detected, lowering MSE threshold")
        elif sample_node.get("total", 0) < 1:
            base_start_threshold = 120
            base_end_threshold = 96
            logger.info("🟌 [Video Type]: Blackboard/Operation detected, raising MSE threshold")
    except Exception as error:  # noqa: BLE001
        logger.warning("Video type detection skipped: %s", error)

    mse_list, _ = extractor.visual_extractor.calculate_all_diffs(frames)

    def calculate_anchor_score(mse_val, anchor_time, target_time):
        intensity = min(1.0, mse_val / base_start_threshold)
        time_gap = abs(anchor_time - target_time)
        zone_weight = 1.0 if time_gap <= 1.5 else 0.7
        temporal_similarity = max(0.0, 1.0 - (time_gap / 5.0))
        return (0.7 * intensity + 0.3 * temporal_similarity) * zone_weight

    best_vis_start = asr_s
    max_start_score = -1.0
    for index, mse in enumerate(mse_list):
        if mse > base_start_threshold:
            current_score = calculate_anchor_score(mse, timestamps[index], asr_s)
            if current_score > max_start_score:
                max_start_score = current_score
                best_vis_start = timestamps[index]

    best_vis_end = asr_e
    max_end_score = -1.0
    for index in range(len(mse_list) - 1, -1, -1):
        if mse_list[index] > base_end_threshold:
            current_score = calculate_anchor_score(mse_list[index], timestamps[index], asr_e)
            if current_score > max_end_score:
                max_end_score = current_score
                best_vis_end = timestamps[index]

    return best_vis_start, best_vis_end
