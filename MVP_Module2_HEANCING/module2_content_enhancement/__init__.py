"""
Module 2: Content Enhancement (内容增强模块)

MVP Implementation - Week 1 Day 1-2: Data Loading and Parsing

This module implements the complete content enhancement system including:
- Fault detection (断层识别)
- Multimodal decision (多模态判断)
- Media generation (素材生成)
- Confidence calculation (置信度计算)

V2: 支持 Java-Python 分层架构
- Phase2A: analyze_only() - 语义分析 + 素材需求收集
- Phase2B: assemble_only() - 外部素材应用 + 富文本组装
"""

__version__ = "2.0.0"
__author__ = "MVP Module 2 Team"

from .data_loader import (
    load_corrected_subtitles,
    load_merged_segments,
    CorrectedSubtitle,
    CrossSentenceMergedSegment,
    Module2Input
)

from .rich_text_pipeline import (
    RichTextPipeline,
    PipelineConfig,
    ScreenshotRequest,
    ClipRequest,
    MaterialRequests
)

__all__ = [
    # Data Loader
    'load_corrected_subtitles',
    'load_merged_segments',
    'CorrectedSubtitle',
    'CrossSentenceMergedSegment',
    'Module2Input',
    # Rich Text Pipeline
    'RichTextPipeline',
    'PipelineConfig',
    'ScreenshotRequest',
    'ClipRequest',
    'MaterialRequests',
]
