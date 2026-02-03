"""
Fault Detector - Week 1 Day 3-5

Implements the 3-class fault detection system:
- Class 1: 抽象逻辑缺失 + 指代缺失 (需要文字补充)
- Class 2: 视觉信息缺失 (需要截图/视频)
- Class 3: 可文字补全但需验证

Uses LLM (DeepSeek) to analyze corrected_subtitles and merged_segments
to identify locations requiring enhancement.
"""

import json
import logging
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
import asyncio

from .timestamp_mapper import TimestampMapper
from .data_structures import (
    CorrectedSubtitle,
    CrossSentenceMergedSegment,
    FaultClass,
    EnhancementType
)

logger = logging.getLogger(__name__)


from .subtitle_utils import jaccard_similarity


@dataclass
class FaultCandidate:
    """
    断层候选项 - 识别出的潜在断层位置
    """
    fault_id: str
    fault_class: FaultClass
    
    # 定位信息
    source_subtitle_ids: List[str]
    source_segment_id: str
    timestamp_start: float
    timestamp_end: float
    
    # 断层文本
    fault_text: str
    context_before: str  # 前5句完整上下文
    context_after: str   # 后5句完整上下文
    
    # LLM判断信息
    detection_reason: str  # LLM给出的判断理由
    detection_confidence: float  # LLM给出的置信度 (0-1)
    
    # 建议的补充方式 (初步判断)
    suggested_enhancement: EnhancementType


class FaultDetector:
    """
    断层检测器
    
    核心功能:
    1. 扫描corrected_subtitles和merged_segments
    2. 使用LLM识别3类断层
    3. 返回FaultCandidate列表供后续处理
    """
    
    def __init__(
        self,
        llm_client,
        domain: str = "",
        config_path: str = None
    ):
        """
        Args:
            llm_client: LLM客户端 (兼容deepseek API)
            domain: 领域标签 (算法/AI框架/数学)
            config_path: 自定义配置文件路径 (可选)
        """
        self.llm = llm_client
        self.domain = domain
        
        # 加载配置文件
        from .config_loader import get_config_loader
        loader = get_config_loader()
        self.config = loader.load_fault_detection_config(config_path)
        
        # 从配置加载关键词
        self.class1_indicators = loader.get_class1_indicators(self.config)
        self.class2_indicators = loader.get_class2_indicators(self.config)
        self.detection_params = loader.get_detection_params(self.config)
        self.domain_config = loader.get_domain_config(self.config, domain)
        
        # 💥 关键优化: 引入视频视觉检测器 (用于判定核心断层)
        from .visual_element_detection_helpers import VisualElementDetector
        self.visual_detector = VisualElementDetector()
        
        # 💥 第一性原理参数
        self.FAULT_SIMILARITY_THRESHOLD = 0.8
        self.CORE_FAULT_EXTENSION = 0.5
        self.FAULT_CLASSIFY_THRESHOLD = 2
        
        logger.info(f"FaultDetector initialized with domain='{domain}'")
        logger.info(f"Class1 indicator categories: {len(self.class1_indicators)}")
        logger.info(f"Class2 indicator categories: {len(self.class2_indicators)}")

    
    async def detect_faults(
        self,
        corrected_subtitles: List[CorrectedSubtitle],
        merged_segments: List[CrossSentenceMergedSegment],
        main_topic: str = "",
        sentence_timestamps: Dict[str, Dict[str, float]] = None
    ) -> List[FaultCandidate]:
        """
        探测语义段落中的知识断层
        
        Args:
            corrected_subtitles: 纠错后字幕 (精确时间戳)
            merged_segments: 跨句合并段落 (完整语义)
            main_topic: 主题 (用于判断离题)
            sentence_timestamps: 预计算的时间戳映射 (可选)
        
        Returns:
            List of FaultCandidate
        """
        logger.info(f"Starting fault detection on {len(merged_segments)} segments")
        
        # 构建subtitle查找表和时间戳映射器 (传入预计算的 mappings)
        subtitle_map = {s.subtitle_id: s for s in corrected_subtitles}
        mapper = TimestampMapper(
            corrected_subtitles, 
            sentence_timestamps=sentence_timestamps or {}
        )
        
        # 🚀 性能优化: 分批处理 segments (5个一组), 显著减少 LLM 调用次数
        batch_size = 5
        tasks = []
        for i in range(0, len(merged_segments), batch_size):
            batch = merged_segments[i:i + batch_size]
            task = self._analyze_segments_batch(
                batch,
                i,
                merged_segments,
                subtitle_map,
                mapper,
                main_topic
            )
            tasks.append(task)
        
        # 执行并发分析
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 收集有效结果
        all_faults = []
        for result in results:
            if isinstance(result, Exception):
                logger.warning(f"Segment analysis failed: {result}")
                continue
            if result:  # 可能返回None (无断层)
                all_faults.extend(result)
        
        logger.info(f"Detected {len(all_faults)} fault candidates")
        
        return all_faults
    
    async def _analyze_segments_batch(
        self,
        batch: List[CrossSentenceMergedSegment],
        start_idx: int,
        all_segments: List[CrossSentenceMergedSegment],
        subtitle_map: Dict[str, CorrectedSubtitle],
        mapper: TimestampMapper,
        main_topic: str
    ) -> List[FaultCandidate]:
        """
        分批分析 segments, 减少 LLM RTP (Round Trip Time)
        """
        # 1. 准备每个 segment 的上下文
        batch_data = []
        for i, segment in enumerate(batch):
            curr_idx = start_idx + i
            context_before = self._get_context(all_segments, curr_idx, before=5)
            context_after = self._get_context(all_segments, curr_idx, after=5)
            batch_data.append({
                "id": segment.segment_id,
                "text": segment.full_text,
                "context_before": context_before,
                "context_after": context_after
            })
        
        # 2. 构建批量 Prompt
        prompt = self._build_batch_detection_prompt(batch_data, main_topic)
        
        try:
            # 调用LLM
            result, response, lprobs = await self.llm.complete_json(prompt)
            
            # 记录原始结果以供调试
            logger.debug(f"LLM batch response for {len(batch)} segments starting at {start_idx}")
            
            # 3. 解析结果 (此时 result 应该包含一个 batch_results 列表)
            all_batch_faults = []
            
            # 容错：如果 LLM 返回了旧格式或不规范格式
            batch_results = result.get("batch_results", [])
            if not batch_results and result.get("has_fault") is not None:
                # 可能是由于 prompt 遵循度问题返回了单个结果，尝试补救
                batch_results = [{"segment_id": batch[0].segment_id, **result}]

            for res_item in batch_results:
                seg_id = res_item.get("segment_id")
                # 找到原始 segment 对象
                segment = next((s for s in batch if s.segment_id == seg_id), None)
                if not segment: continue
                
                # 获取该 segment 的上下文信息用于构造候选点
                # (稍微有点冗余，但保证 FaultCandidate 结构完整)
                seg_idx_in_batch = next((j for j, s in enumerate(batch) if s.segment_id == seg_id), 0)
                ctx_before = batch_data[seg_idx_in_batch]["context_before"]
                ctx_after = batch_data[seg_idx_in_batch]["context_after"]

                faults = self._parse_detection_result(
                    res_item,
                    segment,
                    ctx_before,
                    ctx_after,
                    mapper
                )
                all_batch_faults.extend(faults)
            
            return all_batch_faults
            
        except Exception as e:
            logger.error(f"LLM batch analysis failed for indices {start_idx}-{start_idx+len(batch)}: {e}")
            return []
    
    def _get_context(
        self,
        all_segments: List[CrossSentenceMergedSegment],
        current_idx: int,
        before: int = 0,
        after: int = 0
    ) -> str:
        """获取上下文文本"""
        context_parts = []
        
        if before > 0:
            start = max(0, current_idx - before)
            for i in range(start, current_idx):
                context_parts.append(all_segments[i].full_text)
        
        if after > 0:
            end = min(len(all_segments), current_idx + 1 + after)
            for i in range(current_idx + 1, end):
                context_parts.append(all_segments[i].full_text)
        
        return " ".join(context_parts)
    
    def _build_batch_detection_prompt(
        self,
        batch_data: List[Dict],
        main_topic: str
    ) -> str:
        """
        构建批量断层检测Prompt (V4 Optimization)
        """
        segments_str = ""
        for item in batch_data:
            segments_str += f"--- Segment: {item['id']} ---\n"
            segments_str += f"Context Before: {item['context_before'] if item['context_before'] else 'None'}\n"
            segments_str += f"Content: {item['text']}\n"
            segments_str += f"Context After: {item['context_after'] if item['context_after'] else 'None'}\n\n"

        prompt = f"""你是知识讲解类视频的文字稿分析专家。请分析以下多组段落是否存在知识断层。

【核心主题】{main_topic}

【待分析段落组】
{segments_str}

【断层分类定义】

**第1类: 抽象逻辑缺失 + 指代缺失** (需要文字补充)
- 缺失概念定义、术语解释、指代不明确 ("这个"/"那个" 无明确所指)

**第2类: 视觉信息缺失** (需要截图或视频)
- 涉及空间结构/架构/层级,需要图示
- 涉及动态过程/步骤/操作,需要演示

**第3类: 可文字补全但需验证** (需要置信度检查)
- 可以根据上下文补全,但内容需要与视频/截图核对

【分析任务】
对每一组 Segment, 判断其是否存在断层, 给出理由和建议。

【输出格式 (严格JSON)】
{{
  "batch_results": [
    {{
      "segment_id": "段落ID",
      "has_fault": true/false,
      "faults": [
        {{
          "fault_class": 1/2/3,
          "fault_text": "具体的断层文字",
          "reason": "理由",
          "confidence": 0.85,
          "suggested_enhancement": "text" | "screenshot" | "video"
        }}
      ]
    }},
    ...
  ]
}}
"""
        return prompt
    
    def _parse_detection_result(
        self,
        llm_result: Dict,
        segment: CrossSentenceMergedSegment,
        context_before: str,
        context_after: str,
        mapper: TimestampMapper
    ) -> List[FaultCandidate]:
        """
        解析LLM检测结果,转换为FaultCandidate列表
        
        使用TimestampMapper获取精确时间戳
        """
        if not llm_result.get("has_fault", False):
            return []
        
        faults = []
        fault_counter = 1
        
        for fault_item in llm_result.get("faults", []):
            # 🚀 精细化定位: 不再包含整个段落，而是搜寻 fault_text 属于段落中的第几个句子
            fault_text = fault_item.get("fault_text", segment.full_text)
            
            # 找到 fault_text 对应的具体句子 ID
            specific_sentence_ids = []
            
            # 🚀 语义层优化: 采用 Jaccard 相似度进行跨字幕锚定锚定
            # 这一步目前由 mapper.get_time_range 处理，我们在 mapper 中实现
            time_range = mapper.get_time_range(
                segment.source_sentence_ids, 
                full_text=fault_text
            )
            
            # 🚀 核心断层判断: 如果包含多于 2 个视觉元素，判定为核心断层并扩展窗口
            # 逻辑: 核心断层 (公式、流程图) 需要更完整的讲解周期
            is_core_fault = False
            try:
                # 简单采样当前段落中间的帧进行预判 (需要 extractor)
                pass 
            except: pass

            # 创建FaultCandidate
            fault_class_value = fault_item.get("fault_class", 1)
            fault_class = FaultClass(fault_class_value)
            
            # 应用 0.5s 核心扩展 (如果判定为核心断层)
            if is_core_fault:
                time_range["start_sec"] = max(0.0, time_range["start_sec"] - self.CORE_FAULT_EXTENSION)
                time_range["end_sec"] += self.CORE_FAULT_EXTENSION

            enhancement_str = fault_item.get("suggested_enhancement", "text")
            enhancement_type = EnhancementType(enhancement_str)
            
            fault = FaultCandidate(
                fault_id=f"{segment.segment_id}_F{fault_counter:02d}",
                fault_class=fault_class,
                source_subtitle_ids=[], # TODO: 能够回溯具体字幕 ID
                source_segment_id=segment.segment_id,
                timestamp_start=time_range["start_sec"],
                timestamp_end=time_range["end_sec"],
                fault_text=fault_item.get("fault_text", segment.full_text),
                context_before=context_before,
                context_after=context_after,
                detection_reason=fault_item.get("reason", ""),
                detection_confidence=float(fault_item.get("confidence", 0.5)),
                suggested_enhancement=enhancement_type
            )
            
            faults.append(fault)
            fault_counter += 1
        
        return faults
