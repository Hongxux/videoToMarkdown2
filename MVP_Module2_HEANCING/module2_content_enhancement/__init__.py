"""
模块说明：包初始化与公共导出。
执行逻辑：
1) 聚合本模块的类/函数，对外提供核心能力。
2) 通过内部调用与外部依赖完成具体处理。
实现方式：通过模块内函数组合与外部依赖调用实现。
核心价值：统一模块职责边界，降低跨文件耦合成本。
输入：
- 调用方传入的参数与数据路径。
输出：
- 各函数/类返回的结构化结果或副作用。"""

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
