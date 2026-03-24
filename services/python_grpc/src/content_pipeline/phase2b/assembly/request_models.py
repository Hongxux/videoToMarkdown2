"""富文本流水线请求与配置模型。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class PipelineConfig:
    """Pipeline 运行配置。"""

    screenshot_quality: int = 95
    clip_crf: int = 23
    head_offset_sec: float = 0.2
    tail_offset_sec: float = 0.2
    screenshot_sample_interval: float = 0.5
    assets_subdir: str = "assets"


@dataclass
class ScreenshotRequest:
    """截图请求。"""

    screenshot_id: str
    timestamp_sec: float
    label: str
    semantic_unit_id: str
    frame_reason: str = ""
    ocr_text: str = ""


@dataclass
class ClipRequest:
    """视频切片请求。"""

    clip_id: str
    start_sec: float
    end_sec: float
    knowledge_type: str
    semantic_unit_id: str
    segments: Optional[List[Dict[str, float]]] = None
    source_action_ids: Optional[List[str]] = None
    merged_from_count: Optional[int] = None


@dataclass
class MaterialRequests:
    """素材请求集合。"""

    screenshot_requests: List[ScreenshotRequest]
    clip_requests: List[ClipRequest]
    action_classifications: List[Dict[str, Any]]
    metadata: Optional[Dict[str, Any]] = None
