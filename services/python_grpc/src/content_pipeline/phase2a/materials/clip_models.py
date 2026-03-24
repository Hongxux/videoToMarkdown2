"""视频片段相关数据模型。"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class RichMediaMetadata:
    layout_type: str = "interactive_card"
    poster_path: str = ""
    poster_timestamp: float = 0.0
    clips: Optional[List[Dict[str, Any]]] = None
    transcript: str = ""


@dataclass
class VideoClip:
    clip_id: str
    fault_id: str
    original_start: float
    original_end: float
    extended_start: float
    extended_end: float
    clip_path: str
    action_start_detected: float
    action_end_detected: float
    transition_text: str
    rich_media: Optional[RichMediaMetadata] = None
