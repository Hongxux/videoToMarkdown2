"""
模块说明：Module2 内容增强中的 data_structures 模块。
执行逻辑：
1) 聚合本模块的类/函数，对外提供核心能力。
2) 通过内部调用与外部依赖完成具体处理。
实现方式：通过模块内函数组合与外部依赖调用实现。
核心价值：统一模块职责边界，降低跨文件耦合成本。
输入：
- 调用方传入的参数与数据路径。
输出：
- 各函数/类返回的结构化结果或副作用。"""

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Dict, Optional, Any


# ============================================================================
# Input Structures
# ============================================================================

@dataclass
class CorrectedSubtitle:
    """
    类说明：封装 CorrectedSubtitle 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
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
    类说明：封装 CrossSentenceMergedSegment 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    segment_id: str  # paragraph_id (P001, P002...)
    full_text: str  # 完整的语义段落
    source_sentence_ids: List[str]  # 来源句子ID
    merge_type: str = "无合并"  # 合并类型


@dataclass
class Module2Input:
    """
    类说明：封装 Module2Input 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
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
    """
    类说明：封装 EnhancementType 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    TEXT = "text"
    SCREENSHOT = "screenshot"
    VIDEO = "video"
    VIDEO_AND_SCREENSHOT = "video_and_screenshot"


class FaultClass(Enum):
    """
    类说明：封装 FaultClass 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    CLASS_1 = 1  # 抽象逻辑缺失 + 指代缺失
    CLASS_2 = 2  # 视觉信息缺失
    CLASS_3 = 3  # 可文字补全但需验证


class ConfidenceLevel(Enum):
    """
    类说明：封装 ConfidenceLevel 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    HIGH = "high"      # ≥ 0.8
    MEDIUM = "medium"  # 0.6-0.8
    LOW = "low"        # < 0.6


@dataclass
class TextSupplement:
    """
    类说明：封装 TextSupplement 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
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
    """
    类说明：封装 VideoMetadata 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    transition_text: str  # 过渡引导语
    viewing_guidance: str = "" # 观看要点  
    post_summary: str = "" # 总结句
    rich_media: Optional[Dict[str, Any]] = None # V6.9.7: 结构化富媒体元数据


@dataclass
class Enhancement:
    """
    类说明：封装 Enhancement 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
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
