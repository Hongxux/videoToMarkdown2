"""
Timestamp Mapper - Utility to map sentence IDs to timestamps

Handles the mapping from:
- sentence_id (S001) from step3-5 → subtitle_id (SUB001) from step2
- Then retrieve precise timestamps from CorrectedSubtitle

This is needed because merged_segments use sentence_ids as source,
but we need subtitle timestamps for precise localization.
"""

import logging
from typing import Dict, List, Optional
from typing import Dict, List, Optional, Tuple # Added Tuple for type hint
from .data_structures import CorrectedSubtitle
from functools import lru_cache # Moved import to top

logger = logging.getLogger(__name__)


from .subtitle_utils import jaccard_similarity


class TimestampMapper:
    """
    Maps sentence IDs to subtitle timestamps
    
    Strategy:
    - In the existing pipeline, step3 creates sentence_id (S001) from subtitles
    - Step6's merged_segments reference these sentence_ids
    - We need to reverse-map: sentence_id → subtitle_ids → timestamps
    
    Since we don't have direct sentence→subtitle mapping in our input,
    we use a heuristic: parse sentence_id position and map to subtitle position
    """
    
    def __init__(self, corrected_subtitles: List[CorrectedSubtitle], sentence_timestamps: Dict = None, **kwargs):
        """
        Args:
            corrected_subtitles: List of subtitles with timestamps
            sentence_timestamps: Pre-calculated mappings from Stage 1 (Dict[sid, {start, end}])
        """
        self.subtitles = corrected_subtitles
        self.mappings = sentence_timestamps or {}
        
        # 💥 性能优化: 构建关键词倒排索引
        self._keyword_index = {}
        for idx, sub in enumerate(self.subtitles):
            # 简单分词（按空格或常用标点）
            words = set(sub.text.replace("，", " ").replace("。", " ").split())
            for word in words:
                if len(word) > 1: # 过滤单字
                    if word not in self._keyword_index:
                        self._keyword_index[word] = []
                    self._keyword_index[word].append(idx)
        
        # 💥 性能优化: 内存缓存结果 (lru_cache applied to _get_time_range_cached)
        self._get_time_range_cached = lru_cache(maxsize=1000)(self._get_time_range_cached) # Apply cache here
        
        logger.info(f"TimestampMapper: Indexed {len(self._keyword_index)} terms across {len(self.subtitles)} subs")
    
    def get_time_range(
        self,
        sentence_ids: List[str],
        full_text: str = ""
    ) -> Dict[str, float]:
        """公有接口：自动处理 list 转 tuple 以支持缓存"""
        return self._get_time_range_cached(tuple(sentence_ids), full_text)

    # 💥 性能优化: 内存缓存结果 (使用内部方法处理 tuple 以便缓存)
    def _get_time_range_cached( # Renamed from get_time_range
        self,
        sentence_ids_tuple: Tuple[str, ...], # Changed type hint and variable name
        full_text: str = ""
    ) -> Dict[str, float]:
        """
        获取时间范围 (Stage 1 映射优先) - 实际执行逻辑
        """
        if not sentence_ids_tuple: # Used sentence_ids_tuple
            return {"start_sec": 0.0, "end_sec": 0.0}
            
        # 💥 1. 尝试直接从 Stage 1 映射获取 (最准)
        valid_ranges = []
        for sid in sentence_ids_tuple: # Used sentence_ids_tuple
            if sid in self.mappings:
                valid_ranges.append(self.mappings[sid])
        
        if valid_ranges:
            rough_start = min(r["start_sec"] for r in valid_ranges)
            rough_end = max(r["end_sec"] for r in valid_ranges)
            
            # 🚀 进阶：在句子的范围内进行“子句/字幕级”精细化匹配
            if full_text:
                matching_subs = []
                SIM_THRESHOLD = 0.8 # 💥 核心参数: 0.8 相似度判定命中
                
                for sub in self.subtitles:
                    # 仅在 Stage 1 标记的范围内搜索 (前后各放宽 1s 容错)
                    if sub.start_sec >= (rough_start - 1.0) and sub.end_sec <= (rough_end + 1.0):
                        # 使用 Jaccard 相似度进行模糊匹配
                        sim = jaccard_similarity(full_text, sub.text)
                        if sim >= SIM_THRESHOLD or full_text in sub.text or sub.text in full_text:
                            matching_subs.append(sub)
                
                if matching_subs:
                    best_sub = max(matching_subs, key=lambda s: jaccard_similarity(full_text, s.text))
                    logger.info(f"📍 [Subtitle Precision]: Found best fragment in {len(matching_subs)} subs with fuzzy match -> {best_sub.start_sec:.1f}s - {best_sub.end_sec:.1f}s")
                    return {"start_sec": float(best_sub.start_sec), "end_sec": float(best_sub.end_sec)}
            
            # 兜底：如果片段匹配不到，则返回整个句子的范围
            return {"start_sec": float(rough_start), "end_sec": float(rough_end)}
            
        # 2. 如果映射缺失, 尝试回退到文本搜索 (鲁棒性保障)
        if full_text:
            return self._get_time_range_via_search(full_text)
            
        # 3. 最后兜底: 索引搜索
        indices = [int(sid.replace("S", "")) for sid in sentence_ids if "S" in sid]
        if not indices: return {"start_sec": 0.0, "end_sec": 0.0}
        s_idx = max(0, min(min(indices)-1, len(self.subtitles)-1))
        e_idx = max(0, min(max(indices)-1, len(self.subtitles)-1))
        return {"start_sec": float(self.subtitles[s_idx].start_sec), "end_sec": float(self.subtitles[e_idx].end_sec)}

    def _get_time_range_via_search(self, full_text: str) -> Dict[str, float]:
        """文本搜索逻辑 (作为映射缺失时的兜底)"""
        start_probe = full_text[:30].strip()
        start_idx = self._find_best_matching_subtitle_idx(start_probe, start=True)
        end_probe = full_text[-30:].strip()
        end_idx = self._find_best_matching_subtitle_idx(end_probe, start=False)
        if end_idx < start_idx: end_idx = start_idx
        return {"start_sec": float(self.subtitles[start_idx].start_sec), "end_sec": float(self.subtitles[end_idx].end_sec)}

    def _find_best_matching_subtitle_idx(self, query: str, start: bool = True) -> int:
        if not query: return 0 if start else len(self.subtitles) - 1
        
        # 🚀 性能优化: 预筛选候选人 (Index lookup)
        candidate_indices = []
        words = query.replace("，", " ").replace("。", " ").split()
        for word in words:
            if word in self._keyword_index:
                candidate_indices.extend(self._keyword_index[word])
        
        # 如果索引命中了候选人，只在候选人中找
        search_pool = list(set(candidate_indices)) if candidate_indices else range(len(self.subtitles))
        
        best_idx = 0 if start else len(self.subtitles) - 1
        max_overlap = -1
        
        for i in search_pool:
            sub = self.subtitles[i]
            # 快速包含判断
            if query in sub.text or sub.text in query: return i
            
            # 兼容性重叠度判断
            overlap = len(set(query) & set(sub.text))
            if overlap > max_overlap:
                max_overlap = overlap
                best_idx = i
        return best_idx

    def get_subtitle_ids(self, sentence_ids: List[str]) -> List[str]:
        """Legacy placeholder: No longer needed for direct-time mode"""
        return []
