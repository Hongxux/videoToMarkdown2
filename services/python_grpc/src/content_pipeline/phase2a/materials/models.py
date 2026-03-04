"""Phase2A 素材生成请求的模型定义。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class VLGenerationResult:
    """VL 素材生成结果。"""

    success: bool = True
    screenshot_requests: List[Dict[str, Any]] = field(default_factory=list)
    clip_requests: List[Dict[str, Any]] = field(default_factory=list)
    error_msg: str = ""
    used_fallback: bool = False
    fallback_reason: str = ""
    token_stats: Dict[str, Any] = field(default_factory=dict)
    unit_analysis_outputs: List[Dict[str, Any]] = field(default_factory=list)
