"""
Subtitle Text Extractor - Helper for Confidence Calculation

Extracts text from existing corrected_subtitles within time range.
No need for Whisper re-transcription!
"""

import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


def extract_subtitle_text_in_range(
    corrected_subtitles: List[Any],
    start_sec: float,
    end_sec: float
) -> str:
    """
    从已有字幕中提取时间范围内的文本
    
    **核心**: 复用Stage 1已转录且纠正的字幕,不重复调用Whisper
    
    Args:
        corrected_subtitles: 字幕列表 (CorrectedSubtitle对象或字典)
        start_sec: 起始时间(秒)
        end_sec: 结束时间(秒)
    
    Returns:
        合并后的文本字符串
    
    Example:
        >>> subs = [
        ...     {'start_sec': 10.0, 'end_sec': 15.0, 'corrected_text': 'Hello'},
        ...     {'start_sec': 15.0, 'end_sec': 20.0, 'corrected_text': 'World'}
        ... ]
        >>> extract_subtitle_text_in_range(subs, 12.0, 18.0)
        'Hello World'
    """
    texts = []
    
    for sub in corrected_subtitles:
        try:
            # 兼容对象属性和字典两种格式
            if isinstance(sub, dict):
                sub_start = float(sub.get('start_sec', 0))
                sub_end = float(sub.get('end_sec', 0))
                sub_text = sub.get('corrected_text', sub.get('text', ''))
            elif hasattr(sub, 'start_sec'):
                sub_start = float(getattr(sub, 'start_sec', 0))
                sub_end = float(getattr(sub, 'end_sec', 0))
                sub_text = getattr(sub, 'corrected_text', getattr(sub, 'text', ''))
            else:
                continue

            # 判断字幕与时间范围是否有重叠
            if sub_start < end_sec and sub_end > start_sec:
                texts.append(sub_text)
        except (AttributeError, TypeError, ValueError):
            continue
    
    merged_text = " ".join(texts)
    
    logger.debug(f"Extracted {len(texts)} subtitle segments "
                f"from [{float(start_sec):.1f}s, {float(end_sec):.1f}s], "
                f"total {len(merged_text)} chars")
    
    return merged_text


async def calculate_subtitle_similarity(
    completion_text: str,
    subtitle_text: str,
    semantic_extractor=None
) -> float:
    """
    计算补全文本与字幕文本的相似度
    
    Args:
        completion_text: LLM生成的补全文本
        subtitle_text: 从字幕提取的文本
        semantic_extractor: SemanticFeatureExtractor实例 (可选)
    
    Returns:
        相似度分数 0-1
    """
    if not subtitle_text or not completion_text:
        return 0.0
    
    if semantic_extractor:
        # 使用Sentence-BERT计算语义相似度 (异步调用)
        try:
            similarity = await semantic_extractor.calculate_context_similarity(
                completion_text,
                subtitle_text
            )
            return similarity
        except Exception as e:
            logger.error(f"Semantic similarity async calculation failed: {e}")
    
    # 简单词汇重叠率 (降级方案)
    words_comp = set(completion_text.lower().split())
    words_sub = set(subtitle_text.lower().split())
    
    if not words_comp or not words_sub:
        return 0.0
    
    intersection = words_comp & words_sub
    union = words_comp | words_sub
    
    return similarity


def jaccard_similarity(str1: str, str2: str) -> float:
    """计算两个字符串的 Jaccard 相似度"""
    if not str1 or not str2:
        return 0.0
    s1 = set(list(str1))
    s2 = set(list(str2))
    intersection = s1.intersection(s2)
    union = s1.union(s2)
    return len(intersection) / len(union) if union else 0.0
