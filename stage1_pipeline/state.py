"""
Pipeline State Definition
定义 LangGraph 状态结构，包含全链路追踪所需的所有字段
"""

from typing import TypedDict, List, Optional, Dict, Any, Annotated
from pydantic import BaseModel, Field
from datetime import datetime
import operator


# ============================================================================
# 基础数据模型 (Pydantic)
# ============================================================================

class SubtitleItem(BaseModel):
    """原始字幕条目"""
    subtitle_id: str
    text: str
    start_sec: float
    end_sec: float


class CorrectedSubtitle(BaseModel):
    """纠错后的字幕"""
    subtitle_id: str
    corrected_text: str
    start_sec: float
    end_sec: float
    corrections: List[Dict[str, str]] = Field(default_factory=list)


class MergedSentence(BaseModel):
    """合并后的句子"""
    sentence_id: str
    text: str
    start_sec: float
    end_sec: float
    source_subtitle_ids: List[str] = Field(default_factory=list)


class CleanedSentence(BaseModel):
    """清理后的句子"""
    sentence_id: str
    cleaned_text: str
    removed_items: List[str] = Field(default_factory=list)


class Paragraph(BaseModel):
    """合并后的段落"""
    paragraph_id: str
    text: str
    source_sentence_ids: List[str] = Field(default_factory=list)
    merge_type: str = "无合并"  # 断句错误重复/同义转述/部分重复/无合并


class ExtractedElements(BaseModel):
    """提取的元素"""
    examples: List[Dict[str, str]] = Field(default_factory=list)
    analogies: List[Dict[str, str]] = Field(default_factory=list)
    concrete_words: List[Dict[str, str]] = Field(default_factory=list)
    insights: List[Dict[str, str]] = Field(default_factory=list)


class SemanticDimension(BaseModel):
    """语义维度"""
    logic_relation: str = ""  # 因果/对比/递进/并列/条件
    hierarchy_type: str = ""  # 定义层/原理层/实现层/应用层/边界层
    description: str = ""


class CoreSemantic(BaseModel):
    """核心语义"""
    summary: str = ""  # 20-50字摘要
    label: str = ""    # 8字以内命名标签


class KnowledgeSegment(BaseModel):
    """知识点片段"""
    segment_id: str
    full_text: str
    knowledge_point: str
    darpa_question: str  # Q1-Q8
    darpa_question_name: str
    semantic_dimension: SemanticDimension
    core_semantic: CoreSemantic
    extracted_elements: ExtractedElements = Field(default_factory=ExtractedElements)
    source_paragraph_ids: List[str] = Field(default_factory=list)


class FaultLocation(BaseModel):
    """断层位置"""
    start_sec: float
    end_sec: float


class MissingContent(BaseModel):
    """缺失内容"""
    must_supplement: str = ""  # 必须补全的内容
    secondary_supplement: str = ""  # 次要补全内容


class FaultCandidate(BaseModel):
    """断层候选"""
    fault_id: str
    segment_id: str
    fault_type: int  # 1-10
    fault_type_name: str
    trigger_sentence_id: str
    trigger_text: str
    trigger_keywords: List[str] = Field(default_factory=list)


class SemanticFault(BaseModel):
    """语义断层（精确定位后）"""
    fault_id: str
    segment_id: str
    fault_type: int
    fault_type_name: str
    fault_location: FaultLocation
    visual_form: str
    missing_content: MissingContent


class ScreenshotInstruction(BaseModel):
    """截帧指令"""
    instruction_id: str
    fault_id: str
    opencv_params: Dict[str, Any]
    validation_questions: List[Dict[str, Any]]


class CapturedFrame(BaseModel):
    """截取的帧"""
    frame_id: str
    instruction_id: str
    timestamp: float
    frame_path: str
    is_valid: bool = True
    invalid_reason: Optional[str] = None


class ValidatedFrame(BaseModel):
    """校验后的帧"""
    frame_id: str
    instruction_id: str
    fault_id: str
    frame_path: str
    grade: str  # A/B/C/不合格
    answers: List[Dict[str, Any]] = Field(default_factory=list)
    extracted_content: Dict[str, Any] = Field(default_factory=dict)


class RetryRecord(BaseModel):
    """重试记录"""
    round: int
    capture_params: Dict[str, Any]
    frame_path: str
    validation_result: Dict[str, Any]
    failure_analysis: str = ""


# ============================================================================
# 监控相关模型
# ============================================================================

class StepTrace(BaseModel):
    """步骤执行追踪"""
    step_name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_ms: Optional[float] = None
    input_summary: Dict[str, Any] = Field(default_factory=dict)
    output_summary: Dict[str, Any] = Field(default_factory=dict)
    status: str = "running"  # running/success/error
    error_message: Optional[str] = None


class LLMCallRecord(BaseModel):
    """LLM调用记录"""
    step_name: str
    model: str
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    latency_ms: float = 0
    timestamp: datetime = Field(default_factory=datetime.now)


class ErrorRecord(BaseModel):
    """错误记录"""
    step_name: str
    error_type: str
    error_message: str
    timestamp: datetime = Field(default_factory=datetime.now)
    context: Dict[str, Any] = Field(default_factory=dict)


# ============================================================================
# LangGraph State 定义
# ============================================================================

def merge_lists(left: List, right: List) -> List:
    """合并列表的 reducer"""
    return left + right


def merge_dicts(left: Dict, right: Dict) -> Dict:
    """合并字典的 reducer"""
    merged = left.copy()
    merged.update(right)
    return merged


class PipelineState(TypedDict):
    """
    Pipeline 状态定义
    
    使用 Annotated 类型来指定 reducer，支持增量更新
    """
    # ========== 输入 ==========
    video_path: str
    subtitle_path: str
    output_dir: str
    
    # ========== Step 1 输出 ==========
    is_valid: bool
    domain: str
    main_topic: str
    video_title: str
    
    # ========== Step 2 输出 ==========
    corrected_subtitles: List[Dict]  # CorrectedSubtitle
    correction_summary: List[Dict]
    
    # ========== Step 3 输出 ==========
    merged_sentences: List[Dict]  # MergedSentence
    
    # ========== Step 4 输出 ==========
    cleaned_sentences: List[Dict]  # CleanedSentence
    
    # ========== Step 5 输出 ==========
    non_redundant_sentences: List[Dict]  # CleanedSentence
    
    # ========== Step 6 输出 ==========
    pure_text_script: List[Dict]  # Paragraph
    
    # ========== Step 7 输出 ==========
    knowledge_segments: List[Dict]  # KnowledgeSegment
    knowledge_points: List[Dict]    # KnowledgePoint (Merged)
    
    # ========== Step 7b 输出 ==========
    visualization_candidates: List[Dict]  # 可视化场景候选
    
    # ========== Step 8a 输出 ==========
    fault_candidates: List[Dict]  # FaultCandidate
    
    # ========== Step 8b 输出 ==========
    semantic_faults: List[Dict]  # SemanticFault
    
    # ========== Step 9-11 输出 ==========
    strategy_matches: List[Dict]
    capture_times: List[Dict]
    screenshot_instructions: List[Dict]  # ScreenshotInstruction
    
    # ========== Step 12-15 输出 ==========
    captured_frames: List[Dict]  # CapturedFrame
    valid_frames: List[Dict]
    validated_frames: List[Dict]  # ValidatedFrame
    qualified_frames: List[Dict]
    retry_results: List[Dict]
    permanently_failed: List[Dict]
    
    # ========== Step 15b 输出 ==========
    processed_frames: List[Dict]  # 裁剪后的截图
    
    # ========== Step 16-19 输出 ==========
    visualization_needed: List[Dict]
    visualization_forms: List[Dict]
    core_content_judgment: List[Dict]
    auxiliary_information: List[Dict]
    
    # ========== Step 20-22 输出 ==========
    integrated_materials: List[Dict]
    reconstructed_materials: List[Dict]
    output_markdown_path: str
    
    # ========== Step 23-24 输出 ==========
    named_video_clips: List[Dict]
    named_screenshots: List[Dict]
    
    # ========== 监控字段 ==========
    execution_trace: Annotated[List[Dict], merge_lists]  # StepTrace
    llm_calls: Annotated[List[Dict], merge_lists]  # LLMCallRecord
    token_usage: Annotated[Dict[str, int], merge_dicts]  # 按步骤统计
    step_timings: Annotated[Dict[str, float], merge_dicts]  # 按步骤耗时
    errors: Annotated[List[Dict], merge_lists]  # ErrorRecord
    
    # ========== 当前步骤 ==========
    current_step: str
    current_step_status: str


def create_initial_state(
    video_path: str,
    subtitle_path: str,
    output_dir: str = "output"
) -> PipelineState:
    """创建初始状态"""
    return PipelineState(
        # 输入
        video_path=video_path,
        subtitle_path=subtitle_path,
        output_dir=output_dir,
        
        # Step 1
        is_valid=False,
        domain="",
        main_topic="",
        video_title="",
        
        # Step 2
        corrected_subtitles=[],
        correction_summary=[],
        
        # Step 3
        merged_sentences=[],
        
        # Step 4
        cleaned_sentences=[],
        
        # Step 5
        non_redundant_sentences=[],
        
        # Step 6
        pure_text_script=[],
        
        # Step 7
        knowledge_segments=[],
        knowledge_points=[],
        
        # Step 7b
        visualization_candidates=[],
        
        # Step 8a
        fault_candidates=[],
        
        # Step 8b
        semantic_faults=[],
        
        # Step 9-11
        strategy_matches=[],
        capture_times=[],
        screenshot_instructions=[],
        
        # Step 12-15
        captured_frames=[],
        valid_frames=[],
        validated_frames=[],
        qualified_frames=[],
        retry_results=[],
        permanently_failed=[],
        
        # Step 15b
        processed_frames=[],
        
        # Step 16-19
        visualization_needed=[],
        visualization_forms=[],
        core_content_judgment=[],
        auxiliary_information=[],
        
        # Step 20-22
        integrated_materials=[],
        reconstructed_materials=[],
        output_markdown_path="",
        
        # Step 23-24
        named_video_clips=[],
        named_screenshots=[],
        
        # 监控
        execution_trace=[],
        llm_calls=[],
        token_usage={},
        step_timings={},
        errors=[],
        
        # 当前状态
        current_step="",
        current_step_status="initialized"
    )
