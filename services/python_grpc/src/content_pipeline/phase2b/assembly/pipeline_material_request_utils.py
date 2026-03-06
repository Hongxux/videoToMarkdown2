"""
模块说明：RichTextPipeline 素材请求构造辅助。

执行逻辑：
1) 统一创建截图请求。
2) 统一创建切片请求。

核心价值：避免主流程中散落构造字典/对象，便于后续收敛字段规范。
"""

from typing import Any, Dict, List, Optional, Tuple


def create_screenshot_request(
    *,
    screenshot_request_type: Any,
    screenshot_id: str,
    timestamp_sec: float,
    label: str,
    semantic_unit_id: str,
    frame_reason: str = "",
    ocr_text: str = "",
):
    """构造 ScreenshotRequest。"""
    return screenshot_request_type(
        screenshot_id=screenshot_id,
        timestamp_sec=timestamp_sec,
        label=label,
        semantic_unit_id=semantic_unit_id,
        frame_reason=frame_reason,
        ocr_text=ocr_text,
    )


def create_clip_request(
    *,
    clip_request_type: Any,
    clip_id: str,
    start_sec: float,
    end_sec: float,
    knowledge_type: str,
    semantic_unit_id: str,
    segments: Optional[List[Dict[str, float]]] = None,
):
    """构造 ClipRequest。"""
    return clip_request_type(
        clip_id=clip_id,
        start_sec=start_sec,
        end_sec=end_sec,
        knowledge_type=knowledge_type,
        semantic_unit_id=semantic_unit_id,
        segments=segments,
    )


def clamp_clip_segments(segments: Optional[List[Tuple[float, float]]]) -> Optional[List[Dict[str, float]]]:
    """将 clip 段列表标准化为字典列表。"""
    if not segments:
        return None
    normalized: List[Dict[str, float]] = []
    for start_sec, end_sec in segments:
        start_value = float(start_sec)
        end_value = float(end_sec)
        if end_value <= start_value:
            continue
        normalized.append({"start_sec": start_value, "end_sec": end_value})
    return normalized or None
