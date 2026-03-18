"""content_pipeline 包公共导出。

做什么：
1) 暴露历史兼容的顶层导出名称。
2) 将实际实现延迟到首次访问时再导入。

为什么：
1) 避免仅仅导入 `content_pipeline` 包时就拉起 RichTextPipeline、LLM、CV 等重依赖。
2) 保持既有 `from ...content_pipeline import RichTextPipeline` 这类调用方式不变。

权衡：
1) 首次访问某个导出时会发生一次真实导入。
2) 通过 `globals()` 缓存导出，后续访问不再重复导入。
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

__version__ = "2.0.0"
__author__ = "MVP Module 2 Team"

_EXPORT_MAP = {
    "load_corrected_subtitles": (
        "services.python_grpc.src.content_pipeline.shared.subtitle.data_loader",
        "load_corrected_subtitles",
    ),
    "load_merged_segments": (
        "services.python_grpc.src.content_pipeline.shared.subtitle.data_loader",
        "load_merged_segments",
    ),
    "CorrectedSubtitle": (
        "services.python_grpc.src.content_pipeline.shared.subtitle.data_loader",
        "CorrectedSubtitle",
    ),
    "CrossSentenceMergedSegment": (
        "services.python_grpc.src.content_pipeline.shared.subtitle.data_loader",
        "CrossSentenceMergedSegment",
    ),
    "Module2Input": (
        "services.python_grpc.src.content_pipeline.shared.subtitle.data_loader",
        "Module2Input",
    ),
    "RichTextPipeline": (
        "services.python_grpc.src.content_pipeline.phase2b.assembly.rich_text_pipeline",
        "RichTextPipeline",
    ),
    "PipelineConfig": (
        "services.python_grpc.src.content_pipeline.phase2b.assembly.rich_text_pipeline",
        "PipelineConfig",
    ),
    "ScreenshotRequest": (
        "services.python_grpc.src.content_pipeline.phase2b.assembly.rich_text_pipeline",
        "ScreenshotRequest",
    ),
    "ClipRequest": (
        "services.python_grpc.src.content_pipeline.phase2b.assembly.rich_text_pipeline",
        "ClipRequest",
    ),
    "MaterialRequests": (
        "services.python_grpc.src.content_pipeline.phase2b.assembly.rich_text_pipeline",
        "MaterialRequests",
    ),
    "SemanticUnitSegmenter": (
        "services.python_grpc.src.content_pipeline.phase2a.segmentation.semantic_unit_segmenter",
        "SemanticUnitSegmenter",
    ),
    "SubtitleRepository": (
        "services.python_grpc.src.content_pipeline.shared.subtitle.subtitle_repository",
        "SubtitleRepository",
    ),
}

__all__ = list(_EXPORT_MAP.keys())

if TYPE_CHECKING:
    from services.python_grpc.src.content_pipeline.phase2a.segmentation.semantic_unit_segmenter import (
        SemanticUnitSegmenter,
    )
    from services.python_grpc.src.content_pipeline.phase2b.assembly.rich_text_pipeline import (
        ClipRequest,
        MaterialRequests,
        PipelineConfig,
        RichTextPipeline,
        ScreenshotRequest,
    )
    from services.python_grpc.src.content_pipeline.shared.subtitle.data_loader import (
        CorrectedSubtitle,
        CrossSentenceMergedSegment,
        Module2Input,
        load_corrected_subtitles,
        load_merged_segments,
    )
    from services.python_grpc.src.content_pipeline.shared.subtitle.subtitle_repository import (
        SubtitleRepository,
    )


def __getattr__(name: str) -> Any:
    target = _EXPORT_MAP.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = target
    module = import_module(module_name)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals().keys()) | set(__all__))
