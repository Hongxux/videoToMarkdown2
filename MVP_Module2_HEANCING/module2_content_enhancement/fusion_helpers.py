"""
Helper methods for text_generator.py fusion logic
"""

import re
import logging
from typing import Optional, List

logger = logging.getLogger(__name__)


def decide_fusion_position_enhanced(
    fault_candidate,
    merged_segment,
    config: Optional[dict] = None
) -> tuple[str, Optional[str]]:
    """
    智能决定补充内容的融合位置 (增强版)
    
    决策策略 (按优先级):
    1. **指代模糊** → replace (直接替换指代词)
    2. **定义缺失** → after (在术语后补充解释)
    3. **因果缺失** → before (在结果前补充原因)
    4. **步骤缺失** → insert (在步骤序列中插入)
    5. **其他** → after (默认)
    
    Returns:
        (position, anchor_text)
        - position: "before" | "after" | "replace" | "insert"
        - anchor_text: 定位锚点文本 (用于replace/insert)
    """
    fault_class = fault_candidate.fault_class.value
    fault_text = fault_candidate.fault_text
    detection_reason = fault_candidate.detection_reason
    segment_text = merged_segment.full_text
    
    # Load dictionaries
    fusion_dicts = {}
    if config:
        fusion_dicts = config.get("fusion_helpers", {})
    
    # Check if fusion_dicts is actually populated (it might be empty if config didn't have it)
    if not fusion_dicts:
        try:
             from .config_loader import get_config_loader
             dicts = get_config_loader().load_dictionaries()
             fusion_dicts = dicts.get("fusion_helpers", {})
        except:
             pass

    # === 策略1: 指代模糊 → replace ===
    if _is_pronoun_ambiguity(fault_text, detection_reason, fusion_dicts):
        # 找到指代词,直接替换
        pronouns = fusion_dicts.get("pronouns", ["这个", "那个", "它", "这里", "那里", "该", "这种", "那种"])
        for pronoun in pronouns:
            if pronoun in fault_text:
                logger.info(f"Fusion: REPLACE pronoun '{pronoun}'")
                return "replace", pronoun
        # 如果没找到明确代词,替换整个fault_text
        return "replace", fault_text
    
    # === 策略2: 定义缺失 → after ===
    if _is_definition_missing(fault_text, detection_reason, fusion_dicts):
        # 在术语/概念后面补充定义
        terms = _extract_terms(fault_text, fusion_dicts)
        if terms:
            logger.info(f"Fusion: AFTER term '{terms[0]}'")
            return "after", terms[0]
        return "after", None
    
    # === 策略3: 因果缺失 → before ===
    if _is_causality_missing(fault_text, detection_reason, fusion_dicts):
        # 在"结果"描述前补充"原因"
        result_indicators = fusion_dicts.get("result_indicators", ["所以", "因此", "导致", "产生", "结果"])
        for indicator in result_indicators:
            if indicator in segment_text:
                logger.info(f"Fusion: BEFORE result indicator '{indicator}'")
                return "before", indicator
        # 默认在fault_text前补充
        return "before", fault_text
    
    # === 策略4: 步骤缺失 → insert ===
    if fault_class == 3 or _is_sequence_pattern(segment_text, fusion_dicts):
        # 在步骤序列中插入
        step_match = re.search(r'(第[一二三四五六七八九十\d]+步?|步骤\d+|[1-9]\.)', fault_text)
        if step_match:
            logger.info(f"Fusion: INSERT at step marker")
            return "insert", step_match.group(1)
        # 否则在fault_text后插入
        return "after", fault_text
    
    # === 策略5: 视觉信息描述 → after ===
    if fault_class == 2:
        return "after", None
    
    # === 默认: after ===
    logger.info(f"Fusion: DEFAULT after")
    return "after", None


def _is_pronoun_ambiguity(fault_text: str, reason: str, dicts: dict) -> bool:
    """判断是否为指代模糊"""
    pronouns = dicts.get("pronouns", ["这个", "那个", "它", "这里", "那里", "该", "这种", "那种", "其", "此"])
    ambiguity_keywords = dicts.get("ambiguity", ["指代", "不明", "模糊", "指的是", "所指"])
    
    has_pronoun = any(p in fault_text for p in pronouns)
    has_ambiguity_reason = any(k in reason for k in ambiguity_keywords)
    
    return has_pronoun and has_ambiguity_reason


def _is_definition_missing(fault_text: str, reason: str, dicts: dict) -> bool:
    """判断是否为定义缺失"""
    definition_keywords = dicts.get("definition", ["什么是", "定义", "概念", "术语", "解释", "含义"])
    return any(k in fault_text or k in reason for k in definition_keywords)


def _is_causality_missing(fault_text: str, reason: str, dicts: dict) -> bool:
    """判断是否为因果关系缺失"""
    causality_keywords = dicts.get("causality", ["为什么", "原因", "导致", "因果", "目的", "作用"])
    return any(k in fault_text or k in reason for k in causality_keywords)


def _is_sequence_pattern(text: str, dicts: dict) -> bool:
    """判断是否包含序列模式"""
    sequence_indicators = dicts.get("sequence_indicators", ["首先", "然后", "接着", "最后", "第一", "第二", "步骤"])
    return any(ind in text for ind in sequence_indicators)


def _extract_terms(text: str, dicts: dict) -> List[str]:
    """提取文本中的术语 (简化版: 提取名词短语)"""
    # 简单策略: 找中文术语 (连续的中文字符)
    terms = re.findall(r'[\u4e00-\u9fa5]{2,10}', text)
    # 过滤常见词
    # 过滤常见词
    pronouns = dicts.get("pronouns", ["这个", "那个"])
    common_words = pronouns + ["什么", "如何", "为什么"]
    terms = [t for t in terms if t not in common_words]
    return terms[:3]  # 返回前3个


def fuse_text_with_segment(
    generated_text: str,
    original_segment_text: str,
    position: str,
    anchor_text: Optional[str] = None
) -> str:
    """
    将补充内容融合到原segment (增强版)
    
    支持4种模式:
    1. before: 在锚点前插入
    2. after: 在锚点后插入  
    3. replace: 替换锚点文本
    4. insert: 在锚点位置插入 (保留锚点)
    
    Args:
        generated_text: 生成的补充文本
        original_segment_text: 原始segment文本
        position: 融合位置
        anchor_text: 定位锚点 (可选)
    
    Returns:
        融合后的完整文本
    """
    if position == "replace" and anchor_text:
        # 替换锚点文本
        if anchor_text in original_segment_text:
            fused = original_segment_text.replace(anchor_text, generated_text, 1)
            logger.info(f"Replaced '{anchor_text}' with supplement")
        else:
            # 锚点未找到,降级为after
            fused = f"{original_segment_text} {generated_text}"
            logger.warning(f"Anchor '{anchor_text}' not found, fallback to after")
    
    elif position == "before":
        if anchor_text and anchor_text in original_segment_text:
            # 在锚点前插入
            parts = original_segment_text.split(anchor_text, 1)
            fused = f"{parts[0]}{generated_text},{anchor_text}{parts[1] if len(parts) > 1 else ''}"
            logger.info(f"Inserted before '{anchor_text}'")
        else:
            # 无锚点或未找到,在整个文本前
            fused = f"{generated_text} {original_segment_text}"
    
    elif position == "insert":
        if anchor_text and anchor_text in original_segment_text:
            # 在锚点后插入 (类似after,但保留锚点上下文)
            parts = original_segment_text.split(anchor_text, 1)
            fused = f"{parts[0]}{anchor_text} {generated_text}{parts[1] if len(parts) > 1 else ''}"
            logger.info(f"Inserted at '{anchor_text}'")
        else:
            # 降级为after
            fused = f"{original_segment_text} {generated_text}"
    
    else:  # "after" (默认)
        if anchor_text and anchor_text in original_segment_text:
            # 在锚点后插入
            parts = original_segment_text.split(anchor_text, 1)
            fused = f"{parts[0]}{anchor_text},{generated_text}{parts[1] if len(parts) > 1 else ''}"
            logger.info(f"Inserted after '{anchor_text}'")
        else:
            # 无锚点,在整个文本后
            fused = f"{original_segment_text} {generated_text}"
    
    return fused
