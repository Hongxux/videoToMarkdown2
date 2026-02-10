"""
模块说明：Module2 内容增强中的 subtitle_utils 模块。
执行逻辑：
1) 聚合本模块的类/函数，对外提供核心能力。
2) 通过内部调用与外部依赖完成具体处理。
实现方式：通过模块内函数组合与外部依赖调用实现。
核心价值：统一模块职责边界，降低跨文件耦合成本。
输入：
- 调用方传入的参数与数据路径。
输出：
- 各函数/类返回的结构化结果或副作用。"""

import logging
from typing import List, Dict, Any

from services.python_grpc.src.content_pipeline.shared.subtitle.subtitle_repository import SubtitleRepository

logger = logging.getLogger(__name__)


def extract_subtitle_text_in_range(
    corrected_subtitles: List[Any],
    start_sec: float,
    end_sec: float
) -> str:
    """
    执行逻辑：
    1) 扫描输入内容。
    2) 过滤并提取目标子集。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：聚焦关键信息，减少后续处理成本。
    决策逻辑：
    - 条件：isinstance(sub, dict)
    - 条件：sub_start < end_sec and sub_end > start_sec
    - 条件：hasattr(sub, 'start_sec')
    依据来源（证据链）：
    - 输入参数：end_sec, start_sec。
    输入参数：
    - corrected_subtitles: 函数入参（类型：List[Any]）。
    - start_sec: 起止时间/区间边界（类型：float）。
    - end_sec: 起止时间/区间边界（类型：float）。
    输出参数：
    - 字符串结果。"""
    repository = SubtitleRepository()
    repository.set_raw_subtitles(corrected_subtitles)
    merged_text = repository.get_subtitles_in_range(
        start_sec,
        end_sec,
        expand_to_sentence_boundary=False,
        include_ts_prefix=False,
        empty_fallback="",
    )
    
    logger.debug("Extracted subtitle text "
                f"from [{float(start_sec):.1f}s, {float(end_sec):.1f}s], "
                f"total {len(merged_text)} chars")
    
    return merged_text


async def calculate_subtitle_similarity(
    completion_text: str,
    subtitle_text: str,
    semantic_extractor=None
) -> float:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：not subtitle_text or not completion_text
    - 条件：semantic_extractor
    - 条件：not words_comp or not words_sub
    依据来源（证据链）：
    - 输入参数：completion_text, semantic_extractor, subtitle_text。
    输入参数：
    - completion_text: 函数入参（类型：str）。
    - subtitle_text: 函数入参（类型：str）。
    - semantic_extractor: 函数入参（类型：未标注）。
    输出参数：
    - 数值型计算结果。"""
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
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：not str1 or not str2
    - 条件：union
    依据来源（证据链）：
    - 输入参数：str1, str2。
    输入参数：
    - str1: 函数入参（类型：str）。
    - str2: 函数入参（类型：str）。
    输出参数：
    - 数值型计算结果。"""
    if not str1 or not str2:
        return 0.0
    s1 = set(list(str1))
    s2 = set(list(str2))
    intersection = s1.intersection(s2)
    union = s1.union(s2)
    return len(intersection) / len(union) if union else 0.0
