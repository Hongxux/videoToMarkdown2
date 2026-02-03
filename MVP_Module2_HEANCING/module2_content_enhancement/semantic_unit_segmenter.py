"""
Semantic Unit Segmenter - Phase 1: LLM Semantic Aggregation

基于第一性原理的语义单元切分模块：
- 输入: step6_merge_cross_output.json 中的段落
- 输出: 满足"语义闭环 + 知识主题唯一"的语义单元

核心逻辑:
1. 合并: 连续段落属于同一知识点 → 合并为1个语义单元
2. 拆分: 单个段落包含多个知识点 → 拆分为多个语义单元
"""

import os
import json
import logging
import asyncio
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Any, Optional, Tuple
from enum import Enum

logger = logging.getLogger(__name__)


# =============================================================================
# Data Structures
# =============================================================================

@dataclass
class SemanticUnit:
    """
    语义单元 v2.1 - 最小的知识承载单元
    
    核心特征:
    1. 语义闭环: 能独立表达一个完整知识点
    2. 主知识类型唯一: 抽象/具象/过程 三选一，无平级混杂
    3. 最小颗粒: 不能再拆分
    4. 时序连续: 连续时间区间
    
    V7.x 扩展:
    - modality: 素材模态 (screenshot/video_screenshot/video_only/discard)
    - knowledge_subtype: 子分类 (K1_K2/K3/K4/presentation)
    - screenshot_times: 截图时间点列表
    - materials: 生成的素材集合 (由 RichTextPipeline 填充)
    """
    unit_id: str                          # SU001, SU002, ...
    knowledge_type: str                   # abstract | concrete | process
    knowledge_topic: str                  # 核心知识点标签
    full_text: str                        # 完整文本
    source_paragraph_ids: List[str]       # 来源段落ID (P001, P002, ...)
    source_sentence_ids: List[str]        # 来源句子ID (S001, S002, ...)
    start_sec: float = 0.0                # 起始时间
    end_sec: float = 0.0                  # 结束时间
    confidence: float = 0.0               # LLM判定置信度
    action_segments: List[Dict] = None    # V7.x: 动作区间详情 [{start, end, type}]
    stable_islands: List[Dict] = None     # V7.x: 稳定岛区间 [{start, end, mid, duration}]
    materials: Any = None                 # V7.x: 生成的素材集合 (MaterialSet)

    def __post_init__(self):
        if self.action_segments is None:
            self.action_segments = []
        if self.stable_islands is None:
            self.stable_islands = []


@dataclass
class SegmentationResult:
    """分割结果"""
    semantic_units: List[SemanticUnit]
    total_paragraphs_input: int
    total_units_output: int
    merge_count: int                      # 合并次数
    split_count: int                      # 拆分次数
    llm_token_usage: int = 0
    processing_time_ms: float = 0.0


# =============================================================================
# LLM Prompts - v1.3: Knowledge Type Based Segmentation
# =============================================================================

SYSTEM_PROMPT = """你是一个专业的教育内容分析专家，擅长识别知识点边界和知识类型。

你的任务是将视频字幕段落切分为"语义单元"，确保每个单元满足：
1. **语义闭环**: 能独立表达一个完整知识点/观点/步骤
2. **主知识类型唯一**: 抽象/具象/过程三选一，无平级类型混杂
3. **最小颗粒**: 不能再拆分，拆分后会破坏语义完整

## 三种知识类型定义
- **抽象 (abstract)**: 定义、概念、理论、原理、公式含义解释
- **具象 (concrete)**: 图表、截图、界面、实物展示、可视化结构
- **过程 (process)**: 操作步骤、执行流程、推导过程、动态变化

## 关键规则
- 展示形式（PPT/实操/动画/口头）与知识类型是多对多映射，**不影响切分**
- 只要主知识类型不变，展示形式再多样也属于同一单元
- 例：讲操作步骤时顺带提一句按钮样子 → 仍以"过程"为主，不切分
- 例：先讲定义（抽象），再讲图表（具象），再讲执行流程（过程）→ 必须切为3个单元

输出格式为JSON。"""

USER_PROMPT_TEMPLATE = """请分析以下视频字幕段落，按"主知识类型唯一"原则切分为语义单元。

## 输入段落
{paragraphs_json}

## 切分规则
1. **合并规则**: 连续段落的主知识类型相同 → 合并为一个语义单元
2. **拆分规则**: 一个段落包含平级混杂的多种知识类型 → 按类型边界拆分
3. **保持规则**: 段落本身是单一知识类型的完整表达 → 保持不变
4. **弱辅助允许**: 主类型占绝对主导、辅助类型极弱时 → 不拆分

## 输出格式
```json
{{
    "semantic_units": [
        {{
            "unit_id": "SU001",
            "knowledge_type": "abstract" | "concrete" | "process",
            "knowledge_topic": "知识点的简短标签 (5-15字)",
            "source_paragraph_ids": ["P001", "P002"],
            "text": "合并/拆分后的完整文本",
            "action": "merge" | "split" | "keep",
            "confidence": 0.95
        }}
    ],
    "reasoning": "简要说明切分逻辑"
}}
```

请开始分析:"""


# =============================================================================
# LLM Prompts - Phase 3: Cross-modal Conflict Resolution
# =============================================================================

RESEGMENT_SYSTEM_PROMPT = """你是语义单元切分专家，擅长处理跨模态冲突。
CV模块检测到某些语义单元存在视觉与文本的不一致，你需要决定如何处理这些冲突。

决策原则：
1. 如果文本语义确实跨越了不同知识类型边界 → 拆分
2. 如果只是首尾包含冗余画面但核心语义完整 → 微调边界
3. 如果视觉变化只是动画效果或无关干扰 → 保持原判"""

RESEGMENT_USER_PROMPT = """CV模块检测到以下语义单元存在"跨模态冲突"，请结合视觉锚点信息重新审视。

## 冲突单元信息
- **Unit ID**: {unit_id}
- **当前文本**: "{text}"
- **当前时序**: {start_sec:.1f}s - {end_sec:.1f}s
- **预判类型**: {llm_type}
- **视觉统计**: 稳定岛={s_stable:.0%}, 动作={s_action:.0%}, 冗余={s_redundant:.0%}
- **视觉锚点**: {anchors} (这些时间点发生了显著的视觉状态切换)
- **冲突原因**: {reason}

## 决策选项

### 1. Split (强制拆分)
- 场景: 文字明显对应了不同的视觉状态(如前半是概念讲解，后半是操作演示)
- 视觉锚点确实是知识点的自然分界线
- 返回拆分点时间戳(必须接近某个视觉锚点)

### 2. Adjust (边界微调)
- 场景: 单元核心语义完整，但首尾包含了无关的转场/冗余画面
- 收缩或扩展 start_sec/end_sec 以避开冗余

### 3. Keep (保持原判)
- 场景: 视觉变化仅是PPT动画或无关干扰，文本语义是不可分割的整体
- 维持原时序，标记为"跨模态存疑"

## 输出格式 (JSON)
```json
{{
    "decision": "split" | "adjust" | "keep",
    "rationale": "决策理由(20字以内)",
    "split_point": 12.5,
    "new_timeline": [10.0, 25.0]
}}
```

注意：
- split时必须提供split_point(秒)
- adjust时必须提供new_timeline([start, end])
- keep时两者都不需要

请输出JSON决策:"""




# =============================================================================
# Main Segmenter Class
# =============================================================================

class SemanticUnitSegmenter:
    """
    语义单元切分器
    
    基于 step6 输出的段落数据，使用 LLM 进行知识点聚类
    """
    
    def __init__(self, llm_client=None):
        """
        Args:
            llm_client: LLMClient 实例, 如果为 None 则自动创建
        """
        self.llm_client = llm_client
        self._ensure_llm_client()
    
    def _ensure_llm_client(self):
        """确保 LLM 客户端已初始化"""
        if self.llm_client is None:
            from module2_content_enhancement.llm_client import LLMClient
            self.llm_client = LLMClient()
            logger.info("SemanticUnitSegmenter: LLM client initialized")
    
    def load_step6_output(self, json_path: str) -> List[Dict[str, Any]]:
        """
        加载 step6_merge_cross_output.json
        
        Returns:
            段落列表 [{"paragraph_id": "P001", "text": "...", "source_sentence_ids": [...]}]
        """
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        paragraphs = data.get("output", {}).get("pure_text_script", [])
        logger.info(f"Loaded {len(paragraphs)} paragraphs from step6 output")
        return paragraphs
    
    async def segment(
        self, 
        paragraphs: List[Dict[str, Any]],
        sentence_timestamps: Dict[str, Dict[str, float]] = None,
        batch_size: int = 10,
        cache_path: str = None
    ) -> SegmentationResult:
        """
        执行语义单元切分
        
        Args:
            paragraphs: step6 输出的段落列表
            sentence_timestamps: 句子时间戳映射 {"S001": {"start_sec": 0.0, "end_sec": 5.0}}
            batch_size: LLM 每批处理的段落数
            cache_path: 缓存文件路径 (若存在则直接读取，若不存在则计算后保存)
        
        Returns:
            SegmentationResult
        """
        import time
        start_time = time.time()
        
        # 🚀 缓存检查
        if cache_path and os.path.exists(cache_path):
            try:
                cached_result = self._load_from_cache(cache_path)
                logger.info(f"Loaded cached segmentation result from {cache_path}")
                return cached_result
            except Exception as e:
                logger.warning(f"Failed to load cache from {cache_path}: {e}, re-computing...")
        
        if not paragraphs:
            return SegmentationResult(
                semantic_units=[],
                total_paragraphs_input=0,
                total_units_output=0,
                merge_count=0,
                split_count=0
            )
        
        # 🚀 性能优化: 并行批处理 (Parallel Batch Processing)
        # 将所有批次请求同时发送给 LLMClient，利用其内部的自适应并发控制
        
        tasks = []
        batches = []
        
        for i in range(0, len(paragraphs), batch_size):
            batch = paragraphs[i:i+batch_size]
            batches.append((i, batch)) # 保存索引以便排序 (虽然 gather 保持顺序，但为了清晰)
            
            # 构建 LLM 输入
            paragraphs_for_llm = [
                {
                    "paragraph_id": p.get("paragraph_id", f"P{idx+1:03d}"),
                    "text": p.get("text", ""),
                    "source_sentence_ids": p.get("source_sentence_ids", [])
                }
                for idx, p in enumerate(batch, start=i)
            ]
            
            # 调用 LLM (创建协程任务)
            prompt = USER_PROMPT_TEMPLATE.format(
                paragraphs_json=json.dumps(paragraphs_for_llm, ensure_ascii=False, indent=2)
            )
            
            task = self.llm_client.complete_json(
                prompt=prompt,
                system_message=SYSTEM_PROMPT
            )
            tasks.append(task)
            
        logger.info(f"Starting {len(tasks)} parallel LLM batches for {len(paragraphs)} paragraphs")
        
        # 并行执行所有任务
        # return_exceptions=True 允许部分失败而不中断整体
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 处理结果
        all_units = []
        total_tokens = 0
        merge_count = 0
        split_count = 0
        unit_counter = 1
        
        for i, result_or_exc in enumerate(results):
            batch_idx, batch = batches[i]
            
            if isinstance(result_or_exc, Exception):
                logger.error(f"LLM call failed for batch {i+1}: {result_or_exc}")
                # 降级: 保持原段落不变
                for p in batch:
                    unit = SemanticUnit(
                        unit_id=f"SU{unit_counter:03d}",
                        knowledge_type="abstract",  # Default fallback
                        knowledge_topic=p.get("text", "")[:20] + "...",
                        full_text=p.get("text", ""),
                        source_paragraph_ids=[p.get("paragraph_id", "")],
                        source_sentence_ids=p.get("source_sentence_ids", []),
                        confidence=0.5  # 低置信度标记降级
                    )
                    all_units.append(unit)
                    unit_counter += 1
                continue
            
            # 正常结果
            result_json, metadata, _ = result_or_exc
            total_tokens += metadata.total_tokens
            
            units_data = result_json.get("semantic_units", [])
            logger.debug(f"Batch {i+1} completed: {len(units_data)} units")
            
            for u in units_data:
                # 计算时间戳
                start_sec, end_sec = self._calculate_timestamps(
                    u.get("source_paragraph_ids", []),
                    paragraphs,
                    sentence_timestamps
                )
                
                unit = SemanticUnit(
                    unit_id=f"SU{unit_counter:03d}",
                    knowledge_type=u.get("knowledge_type", "abstract"),
                    knowledge_topic=u.get("knowledge_topic", "未知主题"),
                    full_text=u.get("text", ""),
                    source_paragraph_ids=u.get("source_paragraph_ids", []),
                    source_sentence_ids=self._collect_sentence_ids(
                        u.get("source_paragraph_ids", []), 
                        paragraphs
                    ),
                    start_sec=start_sec,
                    end_sec=end_sec,
                    confidence=u.get("confidence", 0.8)
                )
                all_units.append(unit)
                unit_counter += 1
                
                # 统计合并/拆分
                action = u.get("action", u.get("merge_or_split", "keep"))
                if action == "merge":
                    merge_count += 1
                elif action == "split":
                    split_count += 1
        
        elapsed_ms = (time.time() - start_time) * 1000
        
        result = SegmentationResult(
            semantic_units=all_units,
            total_paragraphs_input=len(paragraphs),
            total_units_output=len(all_units),
            merge_count=merge_count,
            split_count=split_count,
            llm_token_usage=total_tokens,
            processing_time_ms=elapsed_ms
        )
        
        logger.info(f"Segmentation complete: {result.total_paragraphs_input} paragraphs → "
                   f"{result.total_units_output} units "
                   f"(merge: {merge_count}, split: {split_count}), "
                   f"{total_tokens} tokens, {elapsed_ms:.0f}ms")
        
        if cache_path:
            try:
                self._save_to_cache(result, cache_path)
                logger.info(f"Saved segmentation result to cache: {cache_path}")
            except Exception as e:
                logger.warning(f"Failed to save cache to {cache_path}: {e}")
        
        return result
    
    def _save_to_cache(self, result: SegmentationResult, path: str):
        """保存结果到缓存"""
        data = {
            "semantic_units": [asdict(u) for u in result.semantic_units],
            "total_paragraphs_input": result.total_paragraphs_input,
            "total_units_output": result.total_units_output,
            "merge_count": result.merge_count,
            "split_count": result.split_count,
            "llm_token_usage": result.llm_token_usage,
            "processing_time_ms": result.processing_time_ms
        }
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def _load_from_cache(self, path: str) -> SegmentationResult:
        """从缓存加载结果"""
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        units = []
        for u_data in data.get("semantic_units", []):
            # 重建 SemanticUnit 对象
            # 处理 dataclass 字段差异 (向后兼容)
            valid_keys = SemanticUnit.__dataclass_fields__.keys()
            filtered_data = {k: v for k, v in u_data.items() if k in valid_keys}
            units.append(SemanticUnit(**filtered_data))
            
        return SegmentationResult(
            semantic_units=units,
            total_paragraphs_input=data.get("total_paragraphs_input", 0),
            total_units_output=data.get("total_units_output", 0),
            merge_count=data.get("merge_count", 0),
            split_count=data.get("split_count", 0),
            llm_token_usage=data.get("llm_token_usage", 0),
            processing_time_ms=data.get("processing_time_ms", 0.0)
        )
    
    def _calculate_timestamps(
        self,
        paragraph_ids: List[str],
        paragraphs: List[Dict],
        sentence_timestamps: Dict[str, Dict[str, float]] = None
    ) -> Tuple[float, float]:
        """根据段落ID和句子时间戳计算时间范围"""
        if not sentence_timestamps:
            return 0.0, 0.0
        
        # 收集所有相关句子ID
        sentence_ids = self._collect_sentence_ids(paragraph_ids, paragraphs)
        
        if not sentence_ids:
            return 0.0, 0.0
        
        min_start = float('inf')
        max_end = 0.0
        
        for sid in sentence_ids:
            if sid in sentence_timestamps:
                ts = sentence_timestamps[sid]
                min_start = min(min_start, ts.get("start_sec", float('inf')))
                max_end = max(max_end, ts.get("end_sec", 0.0))
        
        if min_start == float('inf'):
            min_start = 0.0
        
        return min_start, max_end
    
    def _collect_sentence_ids(
        self, 
        paragraph_ids: List[str], 
        paragraphs: List[Dict]
    ) -> List[str]:
        """从段落列表中收集所有源句子ID"""
        sentence_ids = []
        pid_set = set(paragraph_ids)
        
        for p in paragraphs:
            if p.get("paragraph_id") in pid_set:
                sentence_ids.extend(p.get("source_sentence_ids", []))
        
        return sentence_ids
    
    # =========================================================================
    # Step 3: Display Form Validation and Splitting
    # =========================================================================
    
    def apply_display_form_validation(
        self,
        units: List[SemanticUnit],
        video_path: str,
        sample_interval: float = 2.0
    ) -> List[SemanticUnit]:
        """
        Step 3: 展示形式校验与拆分
        
        对每个语义单元进行展示形式检测，如果检测到切换点则拆分。
        
        Args:
            units: Phase 1/2 输出的语义单元列表
            video_path: 视频文件路径
            sample_interval: CV采样间隔(秒)
            
        Returns:
            拆分后的语义单元列表 (保证单元内展示形式单一)
        """
        from module2_content_enhancement.display_form_classifier import (
            DisplayFormClassifier, DisplayForm
        )
        
        classifier = DisplayFormClassifier(video_path)
        result_units = []
        split_count = 0
        
        try:
            for unit in units:
                start_sec = unit.start_sec
                end_sec = unit.end_sec
                
                if end_sec <= start_sec or end_sec == 0.0:
                    # 无有效时间范围，保持原样并标记为unknown
                    unit.display_form = "unknown"
                    result_units.append(unit)
                    logger.warning(f"Unit {unit.unit_id}: No valid time range [{start_sec}s-{end_sec}s], skipping form validation")
                    continue
                
                # 分类并检测切换点
                dominant_form, confidence, switch_points = classifier.classify_segment(
                    start_sec, end_sec, sample_interval
                )
                
                if not switch_points:
                    # 无切换点，直接更新展示形式
                    unit.display_form = dominant_form.value
                    result_units.append(unit)
                    logger.info(f"Unit {unit.unit_id}: Form={dominant_form.value} (confidence={confidence:.2f}), no switches")
                else:
                    # 有切换点，需要递归拆分
                    split_units = self._split_unit_at_switch_points(
                        unit, switch_points, classifier, sample_interval
                    )
                    result_units.extend(split_units)
                    split_count += len(switch_points)
                    logger.info(f"Unit {unit.unit_id}: Split into {len(split_units)} sub-units at {len(switch_points)} switch points")
                    
        finally:
            classifier.close()
        
        logger.info(f"Display form validation complete: "
                   f"{len(units)} units → {len(result_units)} units "
                   f"({split_count} display form splits)")
        
        return result_units
    
    def _split_unit_at_switch_points(
        self,
        unit: SemanticUnit,
        switch_points: List,
        classifier,
        sample_interval: float
    ) -> List[SemanticUnit]:
        """
        根据展示形式切换点拆分语义单元
        
        采用递归拆分策略：
        1. 取第一个切换点拆分为两个子单元
        2. 对每个子单元递归检测是否还有切换点
        3. 直到无切换点为止
        """
        from module2_content_enhancement.display_form_classifier import DisplayForm
        
        result = []
        
        # 取第一个切换点
        switch = switch_points[0]
        switch_time = switch.timestamp_sec
        
        # 计算拆分位置
        total_duration = unit.end_sec - unit.start_sec
        if total_duration <= 0:
            unit.display_form = switch.from_form.value
            return [unit]
        
        split_ratio = (switch_time - unit.start_sec) / total_duration
        
        # 拆分文本 (按比例)
        text = unit.full_text
        split_pos = max(1, int(len(text) * split_ratio))
        
        # 尝试在合理位置拆分（标点符号）
        for i in range(split_pos, min(split_pos + 20, len(text))):
            if i < len(text) and text[i] in '。，；！？,.;!?\n':
                split_pos = i + 1
                break
        
        # 创建第一个子单元
        unit1 = SemanticUnit(
            unit_id=f"{unit.unit_id}a",
            knowledge_topic=unit.knowledge_topic,
            full_text=text[:split_pos].strip(),
            source_paragraph_ids=unit.source_paragraph_ids.copy(),
            source_sentence_ids=unit.source_sentence_ids.copy(),
            start_sec=unit.start_sec,
            end_sec=switch_time,
            display_form=switch.from_form.value,
            confidence=unit.confidence * 0.9  # 略降置信度
        )
        
        # 创建第二个子单元
        unit2 = SemanticUnit(
            unit_id=f"{unit.unit_id}b",
            knowledge_topic=unit.knowledge_topic,
            full_text=text[split_pos:].strip(),
            source_paragraph_ids=unit.source_paragraph_ids.copy(),
            source_sentence_ids=unit.source_sentence_ids.copy(),
            start_sec=switch_time,
            end_sec=unit.end_sec,
            display_form=switch.to_form.value,
            confidence=unit.confidence * 0.9
        )
        
        # 递归检测子单元是否还需要拆分
        if len(switch_points) > 1:
            # 还有更多切换点，检查第二个单元
            remaining_switches = [
                sp for sp in switch_points[1:]
                if unit2.start_sec < sp.timestamp_sec < unit2.end_sec
            ]
            if remaining_switches:
                sub_units = self._split_unit_at_switch_points(
                    unit2, remaining_switches, classifier, sample_interval
                )
                result.append(unit1)
                result.extend(sub_units)
                return result
        
        result.append(unit1)
        result.append(unit2)
        return result
    
    async def segment_with_display_form(
        self,
        paragraphs: List[Dict[str, Any]],
        video_path: str,
        sentence_timestamps: Dict[str, Dict[str, float]] = None,
        batch_size: int = 10,
        sample_interval: float = 2.0
    ) -> SegmentationResult:
        """
        完整的三步语义单元切分流程:
        
        Step 1: (跳过，使用 step6 输入)
        Step 2: LLM 语义聚合 (知识主题唯一)
        Step 3: CV 展示形式校验 (形式单一)
        
        Args:
            paragraphs: step6 输出的段落列表
            video_path: 视频文件路径
            sentence_timestamps: 句子时间戳映射
            batch_size: LLM 每批处理的段落数
            sample_interval: CV采样间隔(秒)
            
        Returns:
            SegmentationResult (满足所有第一性原理特征)
        """
        import time
        start_time = time.time()
        
        # Step 2: LLM 语义聚合
        logger.info("=== Step 2: LLM Semantic Aggregation ===")
        step2_result = await self.segment(paragraphs, sentence_timestamps, batch_size)
        
        # Step 3: 展示形式校验 (已移除 - V7.x 废弃)
        final_units = step2_result.semantic_units
        
        elapsed_ms = (time.time() - start_time) * 1000
        
        # 更新结果
        return SegmentationResult(
            semantic_units=final_units,
            total_paragraphs_input=step2_result.total_paragraphs_input,
            total_units_output=len(final_units),
            merge_count=step2_result.merge_count,
            split_count=len(final_units) - len(step2_result.semantic_units) + step2_result.split_count,
            llm_token_usage=step2_result.llm_token_usage,
            processing_time_ms=elapsed_ms
        )
    
    async def segment_with_cv_validation(
        self,
        paragraphs: List[Dict[str, Any]],
        video_path: str,
        sentence_timestamps: Dict[str, Dict[str, float]] = None,
        batch_size: int = 10
    ) -> SegmentationResult:
        """
        完整的LLM+CV协同语义单元切分流程 v2.1:
        
        Step 1: (跳过，使用 step6 输入)
        Step 2: LLM 语义初分 (知识主题唯一)
        Step 3: CV 视觉校验 (批量校验所有单元)
        Step 4: 跨模态融合修正 (异常单元批量重判)
        
        Args:
            paragraphs: step6 输出的段落列表
            video_path: 视频文件路径
            sentence_timestamps: 句子时间戳映射
            batch_size: LLM 每批处理的段落数
            
        Returns:
            SegmentationResult (带视觉分析结果)
        """
        import time
        from .cv_knowledge_validator import CVKnowledgeValidator
        
        start_time = time.time()
        
        # Step 2: LLM 语义初分
        logger.info("=== Step 2: LLM Semantic Aggregation ===")
        step2_result = await self.segment(paragraphs, sentence_timestamps, batch_size)
        
        # 转换为CV校验格式
        units_for_cv = []
        for unit in step2_result.semantic_units:
            units_for_cv.append({
                "unit_id": unit.unit_id,
                "start_sec": unit.start_sec,
                "end_sec": unit.end_sec,
                "knowledge_type": unit.knowledge_type
            })
        
        # Step 3: CV 视觉校验 (批量)
        logger.info("=== Step 3: CV Visual Validation (Batch) ===")
        with CVKnowledgeValidator(video_path) as validator:
            cv_results = validator.validate_batch(units_for_cv)
            
            # 汇总异常单元
            abnormal_results = [r for r in cv_results if not r.is_normal]
            normal_results = [r for r in cv_results if r.is_normal]
            
            logger.info(f"CV Validation: {len(normal_results)} normal, "
                       f"{len(abnormal_results)} abnormal")
            
            # 更新语义单元的视觉统计信息
            cv_result_map = {r.unit_id: r for r in cv_results}
            for unit in step2_result.semantic_units:
                cv_result = cv_result_map.get(unit.unit_id)
                if cv_result:
                    unit.__dict__['vision_stats'] = {
                        's_stable': cv_result.vision_stats.s_stable,
                        's_action': cv_result.vision_stats.s_action,
                        's_redundant': cv_result.vision_stats.s_redundant
                    }
                    unit.__dict__['vision_type'] = cv_result.main_vision_type.value
                    unit.__dict__['vision_anchors'] = cv_result.vision_anchors
                    unit.__dict__['cross_modal_suspected'] = not cv_result.is_normal
                    unit.__dict__['cv_abnormal_reason'] = cv_result.abnormal_reason
            
            # Step 4: 异常单元处理 (LLM重切分)
            final_units = step2_result.semantic_units
            if abnormal_results:
                logger.info("=== Step 4: Cross-modal Conflict Resolution ===")
                conflict_packages = validator.generate_conflict_packages(cv_results)
                
                if conflict_packages:
                    # 调用LLM进行冲突解决
                    final_units = await self._resolve_conflicts(
                        conflict_packages,
                        step2_result.semantic_units,
                        cv_result_map,
                        sentence_timestamps
                    )
                    logger.info(f"Conflict resolution complete: {len(step2_result.semantic_units)} -> {len(final_units)} units")
        
        elapsed_ms = (time.time() - start_time) * 1000
        
        # 计算拆分数量变化
        split_delta = len(final_units) - len(step2_result.semantic_units)
        
        return SegmentationResult(
            semantic_units=final_units,
            total_paragraphs_input=step2_result.total_paragraphs_input,
            total_units_output=len(final_units),
            merge_count=step2_result.merge_count,
            split_count=step2_result.split_count + max(0, split_delta),
            llm_token_usage=step2_result.llm_token_usage,
            processing_time_ms=elapsed_ms
        )

    
    # =========================================================================
    # Phase 3: Cross-modal Conflict Resolution
    # =========================================================================
    
    async def _resolve_conflicts(
        self,
        conflict_packages: List,
        units: List[SemanticUnit],
        cv_result_map: Dict,
        sentence_timestamps: Dict[str, Dict[str, float]] = None
    ) -> List[SemanticUnit]:
        """
        解决跨模态冲突 (Phase 3)
        
        对每个冲突包调用LLM获取决策，然后执行split/adjust/keep
        
        Returns:
            更新后的语义单元列表
        """
        from .cv_knowledge_validator import ConflictPackage
        
        unit_map = {u.unit_id: u for u in units}
        updated_units = []
        processed_ids = set()
        
        for pkg in conflict_packages:
            if not isinstance(pkg, ConflictPackage):
                continue
            
            unit = unit_map.get(pkg.conflict_unit_id)
            if not unit:
                continue
            
            processed_ids.add(pkg.conflict_unit_id)
            cv_result = cv_result_map.get(pkg.conflict_unit_id)
            
            # 调用LLM获取决策
            try:
                decision = await self._call_llm_for_decision(unit, pkg, cv_result)
                
                # 执行决策
                result_units = self._execute_decision(
                    decision, unit, sentence_timestamps)
                updated_units.extend(result_units)
                
                logger.info(f"Conflict resolved: {unit.unit_id} -> {decision.get('decision', 'keep')}")
                
            except Exception as e:
                logger.warning(f"Conflict resolution failed for {unit.unit_id}: {e}, keeping original")
                unit.__dict__['cross_modal_suspected'] = True
                updated_units.append(unit)
        
        # 保留未处理的单元
        for unit in units:
            if unit.unit_id not in processed_ids:
                updated_units.append(unit)
        
        # 按时间排序
        updated_units.sort(key=lambda u: u.start_sec)
        
        return updated_units
    
    async def _call_llm_for_decision(
        self,
        unit: SemanticUnit,
        pkg,  # ConflictPackage
        cv_result  # CVValidationResult
    ) -> Dict[str, Any]:
        """
        调用LLM获取冲突解决决策
        """
        # 构建prompt参数
        vision_stats = cv_result.vision_stats if cv_result else None
        
        prompt = RESEGMENT_USER_PROMPT.format(
            unit_id=unit.unit_id,
            text=unit.full_text[:200] + "..." if len(unit.full_text) > 200 else unit.full_text,
            start_sec=unit.start_sec,
            end_sec=unit.end_sec,
            llm_type=unit.knowledge_type,
            s_stable=vision_stats.s_stable if vision_stats else 0,
            s_action=vision_stats.s_action if vision_stats else 0,
            s_redundant=vision_stats.s_redundant if vision_stats else 0,
            anchors=pkg.vision_anchors if pkg else [],
            reason=pkg.conflict_reason if pkg else "未知"
        )
        
        try:
            result, metadata, _ = await self.llm_client.complete_json(
                prompt=prompt,
                system_message=RESEGMENT_SYSTEM_PROMPT
            )
            return result
        except Exception as e:
            logger.error(f"LLM decision call failed: {e}")
            return {"decision": "keep", "rationale": f"LLM调用失败: {e}"}
    
    def _execute_decision(
        self,
        decision: Dict[str, Any],
        unit: SemanticUnit,
        sentence_timestamps: Dict[str, Dict[str, float]] = None
    ) -> List[SemanticUnit]:
        """
        执行LLM决策: split / adjust / keep
        """
        action = decision.get("decision", "keep").lower()
        
        if action == "split":
            return self._execute_split(decision, unit, sentence_timestamps)
        elif action == "adjust":
            return self._execute_adjust(decision, unit)
        else:  # keep
            unit.__dict__['cross_modal_suspected'] = True
            unit.__dict__['cv_abnormal_reason'] = decision.get("rationale", "保持原判")
            return [unit]
    
    def _execute_split(
        self,
        decision: Dict[str, Any],
        unit: SemanticUnit,
        sentence_timestamps: Dict[str, Dict[str, float]] = None
    ) -> List[SemanticUnit]:
        """
        执行拆分决策: 根据split_point将单元一分为二
        """
        split_point = decision.get("split_point")
        if not split_point or not sentence_timestamps:
            # 无法拆分,标记存疑
            unit.__dict__['cross_modal_suspected'] = True
            return [unit]
        
        # 根据split_point分割sentence_ids
        before_ids = []
        after_ids = []
        
        for sid in unit.source_sentence_ids:
            ts = sentence_timestamps.get(sid, {})
            end_sec = ts.get("end_sec", 0)
            if end_sec <= split_point:
                before_ids.append(sid)
            else:
                after_ids.append(sid)
        
        # 如果分割无效(一边为空)
        if not before_ids or not after_ids:
            unit.__dict__['cross_modal_suspected'] = True
            return [unit]
        
        # 创建两个子单元
        unit1 = SemanticUnit(
            unit_id=f"{unit.unit_id}_1",
            knowledge_type=unit.knowledge_type,
            knowledge_topic=unit.knowledge_topic,
            full_text=self._collect_text_by_ids(before_ids, sentence_timestamps),
            source_paragraph_ids=unit.source_paragraph_ids,
            source_sentence_ids=before_ids,
            start_sec=unit.start_sec,
            end_sec=split_point,
            display_form=unit.display_form,
            confidence=unit.confidence * 0.9
        )
        
        unit2 = SemanticUnit(
            unit_id=f"{unit.unit_id}_2",
            knowledge_type=unit.knowledge_type,
            knowledge_topic=unit.knowledge_topic,
            full_text=self._collect_text_by_ids(after_ids, sentence_timestamps),
            source_paragraph_ids=unit.source_paragraph_ids,
            source_sentence_ids=after_ids,
            start_sec=split_point,
            end_sec=unit.end_sec,
            display_form=unit.display_form,
            confidence=unit.confidence * 0.9
        )
        
        logger.info(f"Split {unit.unit_id} at {split_point}s -> {unit1.unit_id}, {unit2.unit_id}")
        return [unit1, unit2]
    
    def _execute_adjust(
        self,
        decision: Dict[str, Any],
        unit: SemanticUnit
    ) -> List[SemanticUnit]:
        """
        执行边界微调决策
        """
        new_timeline = decision.get("new_timeline", [])
        if len(new_timeline) != 2:
            unit.__dict__['cross_modal_suspected'] = True
            return [unit]
        
        new_start, new_end = new_timeline
        
        # 验证合理性
        if new_start >= new_end:
            unit.__dict__['cross_modal_suspected'] = True
            return [unit]
        
        # 更新时序
        old_start, old_end = unit.start_sec, unit.end_sec
        unit.start_sec = max(0, new_start)
        unit.end_sec = new_end
        
        logger.info(f"Adjusted {unit.unit_id}: [{old_start:.1f}, {old_end:.1f}] -> [{unit.start_sec:.1f}, {unit.end_sec:.1f}]")
        return [unit]
    
    def _collect_text_by_ids(
        self,
        sentence_ids: List[str],
        sentence_timestamps: Dict[str, Dict[str, float]]
    ) -> str:
        """
        根据sentence_ids收集文本
        注意: sentence_timestamps可能没有text字段,需要从原始数据获取
        """
        # 简化实现: 返回ID列表占位
        # 完整实现需要从step2_correction_output获取text
        return f"[Sentences: {', '.join(sentence_ids)}]"

    

    def to_json(self, result: SegmentationResult) -> Dict[str, Any]:
        """将结果转换为可序列化的JSON格式"""
        units_json = []
        for u in result.semantic_units:
            unit_data = {
                "unit_id": u.unit_id,
                "knowledge_type": u.knowledge_type,
                "knowledge_topic": u.knowledge_topic,
                "text": u.full_text,
                "source_paragraph_ids": u.source_paragraph_ids,
                "source_sentence_ids": u.source_sentence_ids,
                "start_sec": u.start_sec,
                "end_sec": u.end_sec,
                "confidence": u.confidence,
                "action_segments": u.action_segments
            }
            # 添加CV校验结果 (如果存在)
            if hasattr(u, '__dict__'):
                if 'vision_stats' in u.__dict__:
                    unit_data['vision_stats'] = u.__dict__['vision_stats']
                if 'vision_type' in u.__dict__:
                    unit_data['vision_type'] = u.__dict__['vision_type']
                if 'vision_anchors' in u.__dict__:
                    unit_data['vision_anchors'] = u.__dict__['vision_anchors']
                if 'cross_modal_suspected' in u.__dict__:
                    unit_data['cross_modal_suspected'] = u.__dict__['cross_modal_suspected']
                if 'cv_abnormal_reason' in u.__dict__:
                    unit_data['cv_abnormal_reason'] = u.__dict__['cv_abnormal_reason']
            units_json.append(unit_data)

        
        return {
            "step": "semantic_unit_segmentation",
            "input": {
                "total_paragraphs": result.total_paragraphs_input
            },
            "output": {
                "semantic_units": units_json
            },
            "_meta": {
                "total_units": result.total_units_output,
                "merge_count": result.merge_count,
                "split_count": result.split_count,
                "llm_token_usage": result.llm_token_usage,
                "processing_time_ms": result.processing_time_ms
            }
        }

    # =========================================================================
    # V7.x: Modality Classification (Material Completion Logic)
    # =========================================================================
    




# =============================================================================
# CLI Entry Point (for testing)
# =============================================================================

async def main():
    """
    测试入口
    
    Usage:
        # Step 2 only (LLM semantic aggregation)
        python semantic_unit_segmenter.py <step6_output.json>
        
        # Step 2 + Step 3 (with display form validation)
        python semantic_unit_segmenter.py <step6_output.json> <video_path>
    """
    import sys
    import os
    
    # Add parent directory to path for script mode
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    if len(sys.argv) < 2:
        print("Usage: python semantic_unit_segmenter.py <step6_output.json> [video_path]")
        print("       Without video_path: Step 2 only (LLM semantic aggregation)")
        print("       With video_path: Step 2 + Step 3 (with display form validation)")
        sys.exit(1)
    
    input_path = sys.argv[1]
    video_path = sys.argv[2] if len(sys.argv) > 2 else None
    
    # 🔧 自动加载时间戳文件 (同目录下的 sentence_timestamps.json)
    input_dir = os.path.dirname(os.path.abspath(input_path))
    timestamps_path = os.path.join(input_dir, "sentence_timestamps.json")
    sentence_timestamps = None
    
    if os.path.exists(timestamps_path):
        with open(timestamps_path, 'r', encoding='utf-8') as f:
            sentence_timestamps = json.load(f)
        print(f"📍 Loaded timestamps: {timestamps_path} ({len(sentence_timestamps)} sentences)")
    else:
        print(f"⚠️ No timestamp file found: {timestamps_path}")
    
    segmenter = SemanticUnitSegmenter()
    paragraphs = segmenter.load_step6_output(input_path)
    
    if video_path:
        # Full Step 2 + Step 3 workflow
        output_path = input_path.replace(".json", "_semantic_units_final.json")
        print(f"🚀 Running full workflow (Step 2 + Step 3)...")
        print(f"   Video: {video_path}")
        result = await segmenter.segment_with_display_form(
            paragraphs, 
            video_path,
            sentence_timestamps=sentence_timestamps
        )
    else:
        # Step 2 only
        output_path = input_path.replace(".json", "_semantic_units.json")
        print(f"🚀 Running Step 2 only (LLM semantic aggregation)...")
        result = await segmenter.segment(paragraphs, sentence_timestamps=sentence_timestamps)
    
    # 保存结果
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(segmenter.to_json(result), f, ensure_ascii=False, indent=2)
    
    print(f"✅ Segmentation complete: {output_path}")
    print(f"   {result.total_paragraphs_input} paragraphs → {result.total_units_output} semantic units")
    print(f"   Merges: {result.merge_count}, Splits: {result.split_count}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())

