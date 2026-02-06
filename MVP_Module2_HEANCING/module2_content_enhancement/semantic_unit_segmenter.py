"""
模块说明：Module2 内容增强中的 semantic_unit_segmenter 模块。
执行逻辑：
1) 聚合本模块的类/函数，对外提供核心能力。
2) 通过内部调用与外部依赖完成具体处理。
实现方式：通过模块内函数组合与外部依赖调用实现。
核心价值：统一模块职责边界，降低跨文件耦合成本。
输入：
- 调用方传入的参数与数据路径。
输出：
- 各函数/类返回的结构化结果或副作用。
补充说明：
基于第一性原理的语义单元切分模块：
- 输入: step6_merge_cross_output.json 中的段落
- 输出: 满足"语义闭环 + 知识主题唯一"的语义单元
核心逻辑:
1. 合并: 连续段落属于同一知识点 → 合并为1个语义单元
2. 拆分: 单个段落包含多个知识点 → 拆分为多个语义单元"""

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
    类说明：封装 SemanticUnit 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
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
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：self.action_segments is None
        - 条件：self.stable_islands is None
        依据来源（证据链）：
        - 对象内部状态：self.action_segments, self.stable_islands。
        输入参数：
        - 无。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        if self.action_segments is None:
            self.action_segments = []
        if self.stable_islands is None:
            self.stable_islands = []


@dataclass
class SegmentationResult:
    """
    类说明：封装 SegmentationResult 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
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

你的任务是将视频字幕段落切分为“语义单元”，并确保每个单元满足：
1. **完整目标（主要拆分依据）**：必须包含一个完整、有意义的目标闭环（从开始到达成）。
2. **语义闭环**：能独立表达一个完整知识点/观点/步骤。
3. **主知识类型唯一**：抽象/具体/过程三选一，不混杂。

## 三种知识类型定义
- **抽象 (abstract)**：定义、概念、理论、原理、公式含义解释。
- **具体 (concrete)**：图表、截图、界面、实物展示、可视化结构。
- **过程 (process)**：操作步骤、执行流程、推导过程、动态变化。

## 聚合与拆分准则

### 聚合逻辑（满足其一则合并）
- 连续操作服务于**同一个教学目的**。
- 连续操作属于**同一个子任务**。
- 连续操作服务于**同一个结论**。

### 拆分逻辑
- **一般拆分**：根据完整目标边界拆分（一个目标完成/新的目标开始）。
- **强制拆分**：当**操作对象发生本质改变**时必须拆分。

## 多步骤识别
判定是否为多步骤单元（mult_steps=true）：
- **多步骤推理**：需要多个逻辑推导步骤才能得出结论。
- **多步配置**：需要配置多个参数或选项。
- **多步骤实操**：需要执行多个连续操作才能完成目标。

单一步骤或原子操作标记为 mult_steps=false。

输出格式为 JSON。"""

USER_PROMPT_TEMPLATE = """请分析以下视频字幕段落，按“完整目标 + 聚合拆分准则”切分为语义单元。

## 输入段落
{paragraphs_json}

## 切分规则
1. **完整目标原则（主要拆分依据）**：每个语义单元必须包含完整、有意义的目标（从开始到达成）。
2. **聚合规则**：连续段落服务于同一教学目的/子任务/结论 → 合并为一个语义单元。
3. **拆分规则**：
   - 一般拆分：根据完整目标边界拆分。
   - 强制拆分：操作对象发生本质改变时必须拆分。
4. **多步骤识别**：判断是否包含多步骤推理/配置/实操。

## 输出格式
```json
{
    "semantic_units": [
        {
            "unit_id": "SU001",
            "knowledge_type": "abstract" | "concrete" | "process",
            "knowledge_topic": "知识点的简短标签(5-15字)",
            "source_paragraph_ids": ["P001", "P002"],
            "text": "合并/拆分后的完整文本",
            "action": "merge" | "split" | "keep",
            "mult_steps": true | false,
            "confidence": 0.95
        }
    ],
    "reasoning": "简要说明切分逻辑"
}
```

请开始分析。"""


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
    类说明：封装 SemanticUnitSegmenter 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    
    def __init__(self, llm_client=None):
        """
        执行逻辑：
        1) 解析配置或依赖，准备运行环境。
        2) 初始化对象状态、缓存与依赖客户端。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：在初始化阶段固化依赖，保证运行稳定性。
        输入参数：
        - llm_client: 客户端实例（类型：未标注）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self.llm_client = llm_client
        self._ensure_llm_client()
    
    def _ensure_llm_client(self):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：self.llm_client is None
        依据来源（证据链）：
        - 对象内部状态：self.llm_client。
        输入参数：
        - 无。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        if self.llm_client is None:
            from module2_content_enhancement.llm_client import LLMClient
            self.llm_client = LLMClient()
            logger.info("SemanticUnitSegmenter: LLM client initialized")
    
    async def segment(
        self, 
        paragraphs: List[Dict[str, Any]],
        sentence_timestamps: Dict[str, Dict[str, float]] = None,
        batch_size: int = 10,
        cache_path: str = None
    ) -> SegmentationResult:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化、asyncio 异步调度、文件系统读写实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：cache_path and os.path.exists(cache_path)
        - 条件：not paragraphs
        - 条件：cache_path
        依据来源（证据链）：
        - 输入参数：cache_path, paragraphs。
        输入参数：
        - paragraphs: 函数入参（类型：List[Dict[str, Any]]）。
        - sentence_timestamps: 函数入参（类型：Dict[str, Dict[str, float]]）。
        - batch_size: 函数入参（类型：int）。
        - cache_path: 文件路径（类型：str）。
        输出参数：
        - SegmentationResult 对象（包含字段：semantic_units, total_paragraphs_input, total_units_output, merge_count, split_count, llm_token_usage, processing_time_ms）。"""
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
                    confidence=u.get("confidence", 0.8),
                    mult_steps=u.get("mult_steps", False)
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
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过JSON 解析/序列化、文件系统读写实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - result: 函数入参（类型：SegmentationResult）。
        - path: 文件路径（类型：str）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
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
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过JSON 解析/序列化、文件系统读写实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - path: 文件路径（类型：str）。
        输出参数：
        - SegmentationResult 对象（包含字段：semantic_units, total_paragraphs_input, total_units_output, merge_count, split_count, llm_token_usage, processing_time_ms）。"""
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
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：not sentence_timestamps
        - 条件：not sentence_ids
        - 条件：min_start == float('inf')
        依据来源（证据链）：
        - 输入参数：sentence_timestamps。
        输入参数：
        - paragraph_ids: 函数入参（类型：List[str]）。
        - paragraphs: 函数入参（类型：List[Dict]）。
        - sentence_timestamps: 函数入参（类型：Dict[str, Dict[str, float]]）。
        输出参数：
        - 多值结果元组（各元素含义见实现）。"""
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
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：p.get('paragraph_id') in pid_set
        依据来源（证据链）：
        - 配置字段：paragraph_id。
        输入参数：
        - paragraph_ids: 函数入参（类型：List[str]）。
        - paragraphs: 函数入参（类型：List[Dict]）。
        输出参数：
        - str 列表（与输入或处理结果一一对应）。"""
        sentence_ids = []
        pid_set = set(paragraph_ids)
        
        for p in paragraphs:
            if p.get("paragraph_id") in pid_set:
                sentence_ids.extend(p.get("source_sentence_ids", []))
        
        return sentence_ids
    async def _resolve_conflicts(
        self,
        conflict_packages: List,
        units: List[SemanticUnit],
        cv_result_map: Dict,
        sentence_timestamps: Dict[str, Dict[str, float]] = None
    ) -> List[SemanticUnit]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：not isinstance(pkg, ConflictPackage)
        - 条件：not unit
        - 条件：unit.unit_id not in processed_ids
        依据来源（证据链）：
        输入参数：
        - conflict_packages: 函数入参（类型：List）。
        - units: 函数入参（类型：List[SemanticUnit]）。
        - cv_result_map: 函数入参（类型：Dict）。
        - sentence_timestamps: 函数入参（类型：Dict[str, Dict[str, float]]）。
        输出参数：
        - SemanticUnit 列表（与输入或处理结果一一对应）。"""
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
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：cv_result
        - 条件：len(unit.full_text) > 200
        - 条件：vision_stats
        依据来源（证据链）：
        - 输入参数：cv_result, pkg, unit。
        输入参数：
        - unit: 函数入参（类型：SemanticUnit）。
        - pkg: 函数入参（类型：未标注）。
        - cv_result: 函数入参（类型：未标注）。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
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
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：action == 'split'
        - 条件：action == 'adjust'
        依据来源（证据链）：
        输入参数：
        - decision: 函数入参（类型：Dict[str, Any]）。
        - unit: 函数入参（类型：SemanticUnit）。
        - sentence_timestamps: 函数入参（类型：Dict[str, Dict[str, float]]）。
        输出参数：
        - SemanticUnit 列表（与输入或处理结果一一对应）。"""
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
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：not split_point or not sentence_timestamps
        - 条件：not before_ids or not after_ids
        - 条件：end_sec <= split_point
        依据来源（证据链）：
        - 输入参数：sentence_timestamps。
        输入参数：
        - decision: 函数入参（类型：Dict[str, Any]）。
        - unit: 函数入参（类型：SemanticUnit）。
        - sentence_timestamps: 函数入参（类型：Dict[str, Dict[str, float]]）。
        输出参数：
        - SemanticUnit 列表（与输入或处理结果一一对应）。"""
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
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：len(new_timeline) != 2
        - 条件：new_start >= new_end
        依据来源（证据链）：
        输入参数：
        - decision: 函数入参（类型：Dict[str, Any]）。
        - unit: 函数入参（类型：SemanticUnit）。
        输出参数：
        - SemanticUnit 列表（与输入或处理结果一一对应）。"""
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
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - sentence_ids: 函数入参（类型：List[str]）。
        - sentence_timestamps: 函数入参（类型：Dict[str, Dict[str, float]]）。
        输出参数：
        - 字符串结果。"""
        # 简化实现: 返回ID列表占位
        # 完整实现需要从step2_correction_output获取text
        return f"[Sentences: {', '.join(sentence_ids)}]"

    

