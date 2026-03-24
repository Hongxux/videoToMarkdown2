"""Tools 包导出入口。

避免在包初始化阶段导入 OpenCV/skimage 这类重依赖。
通过 ``__getattr__`` 在真正访问符号时再按需加载。
"""

from .file_validator import extract_video_title, read_subtitle_sample, validate_subtitle, validate_video
from .storage import LocalStorage

__all__ = [
    "validate_video",
    "validate_subtitle",
    "read_subtitle_sample",
    "extract_video_title",
    "LocalStorage",
    "FrameCapture",
    "FrameBoundaryAnalyzer",
    "BoundaryCandidate",
    "BoundaryAnalysisResult",
]


def __getattr__(name: str):
    if name == "FrameCapture":
        from .opencv_capture import FrameCapture

        return FrameCapture

    if name in {"FrameBoundaryAnalyzer", "BoundaryCandidate", "BoundaryAnalysisResult"}:
        from .frame_analyzer import (
            BoundaryAnalysisResult,
            BoundaryCandidate,
            FrameBoundaryAnalyzer,
        )

        mapping = {
            "FrameBoundaryAnalyzer": FrameBoundaryAnalyzer,
            "BoundaryCandidate": BoundaryCandidate,
            "BoundaryAnalysisResult": BoundaryAnalysisResult,
        }
        return mapping[name]

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
