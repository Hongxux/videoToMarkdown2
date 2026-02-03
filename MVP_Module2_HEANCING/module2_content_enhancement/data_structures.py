"""
Centralized Data Structures for Content Enhancement Pipeline
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Dict, Optional, Any


# ============================================================================
# Input Structures
# ============================================================================

@dataclass
class CorrectedSubtitle:
    """校正后的单条字幕"""
    subtitle_id: str
    text: str
    start_sec: float  # Aliased from timestamp_start for data_loader compatibility
    end_sec: float
    confidence: float = 1.0 # Default values to avoid positional arg issues
    is_modified: bool = False
    corrections: List[Any] = field(default_factory=list)


@dataclass
class CrossSentenceMergedSegment:
    """
    跨句冗余合并后的完整语义句 - 来自步骤6的输出
    这是文字补充的基础输入
    """
    segment_id: str  # paragraph_id (P001, P002...)
    full_text: str  # 完整的语义段落
    source_sentence_ids: List[str]  # 来源句子ID
    merge_type: str = "无合并"  # 合并类型


@dataclass
class Module2Input:
    """Module 2 complete input"""
    corrected_subtitles: List[CorrectedSubtitle]
    merged_segments: List[CrossSentenceMergedSegment]
    video_path: str
    output_dir: str
    domain: str  # 领域标签 (算法/AI框架/数学等)
    main_topic: str = ""  # 主题 (可选)
    
    # 💥 全新数据源支持 (User Request)
    sentence_timestamps: Dict[str, Dict[str, float]] = field(default_factory=dict)


# ============================================================================
# Output Data Structures
# ============================================================================

class EnhancementType(Enum):
    """补充类型"""
    TEXT = "text"
    SCREENSHOT = "screenshot"
    VIDEO = "video"
    VIDEO_AND_SCREENSHOT = "video_and_screenshot"


class FaultClass(Enum):
    """断层分类"""
    CLASS_1 = 1  # 抽象逻辑缺失 + 指代缺失
    CLASS_2 = 2  # 视觉信息缺失
    CLASS_3 = 3  # 可文字补全但需验证


class ConfidenceLevel(Enum):
    """置信度等级"""
    HIGH = "high"      # ≥ 0.8
    MEDIUM = "medium"  # 0.6-0.8
    LOW = "low"        # < 0.6


@dataclass
class TextSupplement:
    """文字补充详细信息 (同步 TextGenerator 架构)"""
    supplement_id: str
    fault_id: str
    generated_text: str
    fusion_position: str  # "before" | "after" | "replace"
    source_segment_id: str
    original_segment_text: str
    fused_text: str
    C_text: float
    C_multi: float
    C_total: float
    confidence_level: str
    logprobs: Optional[Any] = None


@dataclass
class VideoMetadata:
    """视频片段元数据"""
    transition_text: str  # 过渡引导语
    viewing_guidance: str = "" # 观看要点  
    post_summary: str = "" # 总结句
    rich_media: Optional[Dict[str, Any]] = None # V6.9.7: 结构化富媒体元数据


@dataclass
class Enhancement:
    """增强结果项 - 最终导出的核心结构"""
    enhancement_id: str
    fault_id: str
    fault_class: FaultClass
    
    # 定位信息
    source_subtitle_ids: List[str]
    source_segment_id: str
    timestamp_start: float
    timestamp_end: float
    
    # 内容
    fault_text: str
    context_before: str
    context_after: str
    
    # 增强决策
    enhancement_type: EnhancementType
    
    # 产出物
    media_paths: List[str] = field(default_factory=list)
    text_supplement: Optional[TextSupplement] = None
    video_metadata: Optional[VideoMetadata] = None
    
    # 置信度指标
    confidence: Optional[Dict[str, float]] = None # {"C_text": ..., "C_multi": ..., "C_total": ...}
    confidence_level: Optional[str] = None # "high", "medium", "low"
    processing_suggestion: str = ""
    
    # 诊断信息 (User Request)
    detection_reason: str = "" # LLM 判定断层的理由
    material_reason: str = "" # V3 判定素材补全形式的理由 (Cognitive + Visual + Fusion)
    decision_trace: List[str] = field(default_factory=list) # 🚀 Phase 7.1 Trace log
    material_error: Optional[str] = None # 素材生成失败的原因
