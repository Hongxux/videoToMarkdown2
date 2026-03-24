"""
模块说明：Module2 语义单元切分（SemanticUnitSegmenter）。
执行逻辑：
1) 从 step6_merge_cross_output.json 读取段落输入。
2) 调用 LLM 完成语义单元切分与结构化映射。
核心价值：将“段落到语义单元”的规则化处理集中封装，降低主流程复杂度。
"""

import os
import json
import logging
import asyncio
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Any, Optional, Tuple
from enum import Enum
# 统一 LLM 调用入口
from services.python_grpc.src.content_pipeline.infra.llm import llm_gateway
from services.python_grpc.src.content_pipeline.infra.llm.prompt_loader import get_prompt, render_prompt
from services.python_grpc.src.content_pipeline.infra.llm.prompt_registry import PromptKeys
from services.python_grpc.src.content_pipeline.shared.semantic_payload import (
    build_grouped_semantic_units_payload,
    normalize_semantic_units_payload,
)

logger = logging.getLogger(__name__)


def _read_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return int(default)
    try:
        return int(str(raw).strip())
    except Exception:
        return int(default)


SEGMENTATION_MAX_OUTPUT_TOKENS = 8192
SEGMENTATION_PREV_TAIL_TEXT_CHARS = 400
SEGMENTATION_MAX_INPUT_TOKENS = 128000
SEGMENTATION_INPUT_TOKEN_BUDGET_RATIO = 0.75
SEGMENTATION_INPUT_TOKEN_BUDGET = int(
    SEGMENTATION_MAX_INPUT_TOKENS * SEGMENTATION_INPUT_TOKEN_BUDGET_RATIO
)
SEGMENTATION_PROMPT_TOKEN_BUFFER = 1500
SEGMENTATION_EST_CHARS_PER_TOKEN = 1.0
SEGMENTATION_BATCH_MAX_CONCURRENCY_DEFAULT = 48
SEGMENTATION_BOUNDARY_MAX_OUTPUT_TOKENS = 256
SEGMENTATION_BOUNDARY_RULE_REFERENCE_CHARS = 1800
KNOWLEDGE_TYPE_CODE_MAP = {
    0: "abstract",
    1: "concrete",
    2: "process",
}
DEFAULT_UNIT_CONFIDENCE = 0.8


def _segment_batch_max_concurrency() -> int:
    return max(
        1,
        _read_int_env(
            "MODULE2_SEMANTIC_SEGMENT_BATCH_MAX_CONCURRENCY",
            SEGMENTATION_BATCH_MAX_CONCURRENCY_DEFAULT,
        ),
    )


# =============================================================================
# Data Structures
# =============================================================================

@dataclass
class SemanticUnit:
    """类说明：SemanticUnit 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    unit_id: str                          # SU001, SU002, ...
    knowledge_type: str                   # abstract | concrete | process
    knowledge_topic: str                  # 核心知识点标签
    full_text: str                        # 完整文本
    source_paragraph_ids: List[str]       # 来源段落ID (P001, P002, ...)
    source_sentence_ids: List[str]        # 来源句子ID (S001, S002, ...)
    group_id: int = 0                     # 同一核心论点的分组ID（从1开始）
    group_name: str = ""                  # 同一核心论点名称
    group_reason: str = ""                # 分组依据（同论点聚合理由）
    start_sec: float = 0.0                # 起始时间
    end_sec: float = 0.0                  # 结束时间
    confidence: float = 0.0               # LLM判定置信度
    mult_steps: bool = False              # 是否为多步骤单元（推理/配置/实操）
    action_segments: List[Dict] = None    # V7.x: 动作区间详情 [{start, end, type}]
    stable_islands: List[Dict] = None     # V7.x: 稳定岛区间 [{start, end, mid, duration}]
    materials: Any = None                 # V7.x: 生成的素材集合 (MaterialSet)
    instructional_steps: List[Dict] = None # V8.0: 详细的操作步骤 (for tutorial_stepwise)

    def __post_init__(self):
        """
        方法说明：初始化可变字段，避免后续流程访问空值。
        设计原因：统一在对象构造后做兜底，减少调用方重复判空。
        权衡：仅对缺失字段填默认值，不覆盖上游已明确提供的数据。
        """
        if self.action_segments is None:
            self.action_segments = []
        if self.stable_islands is None:
            self.stable_islands = []


@dataclass
class SegmentationResult:
    """类说明：SegmentationResult 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    semantic_units: List[SemanticUnit]
    total_paragraphs_input: int
    total_units_output: int
    llm_token_usage: int = 0
    processing_time_ms: float = 0.0


# =============================================================================
# LLM Prompts - v1.3: Knowledge Type Based Segmentation
# =============================================================================

SYSTEM_PROMPT = """你是一名教学视频语义单元划分专家。目标是将输入文本划分为语义闭环、知识类型唯一、时间连续的单元。

## Unit 与 Group 定义
- Unit: 由知识类型(k)和时间连续性决定，必须保持类型纯净。
- Group: 由核心论点(Core Argument)决定，一个 Group 内可包含多个 Unit。
- 同一核心论点下的 Abstract / Process / Concrete 必须拆成不同 Unit，但放在同一个 Group 中。

## 知识类型判定顺序（必须按顺序）
1) Concrete(k=1): 重点在当前屏幕可见对象/代码/界面/图表的观察与指代。
2) Process(k=2): 重点在可执行、可跟随的演示或详细步骤指导。
3) Abstract(k=0): 不满足以上两者时默认归类。

## 合并与拆分规则
1) 同一视觉对象的连续讲解必须合并为同一 Unit。
2) 同一连贯操作任务的多步骤必须合并为同一 Process Unit。
3) 同一核心论点下的举例、佐证、反例应保持在同一 Group。
4) 当知识类型变化（abstract/concrete/process切换）时必须拆分 Unit。
5) Abstract 与 Process/Concrete 即便服务同一核心论点，也严禁合并为同一 Unit；必须拆分并共享同一 Group。

## 多步骤判定（字段 m）
- m=1: 至少两个有顺序依赖的步骤。
- m=0: 单步动作、单点说明、叙述或概念讲解。

## 输出约束（严格）
1. 只输出 JSON，不输出解释文字。
2. 顶层只允许字段：knowledge_groups。
3. Group 只允许字段：group_name, reason, units。
4. Unit 只允许字段：pids, k, m, title。
5. k 只能是 0/1/2，m 只能是 0/1（整数）。
6. 禁止输出额外字段：semantic_units, group_id, confidence, reasoning, text, full_text 等。"""

USER_PROMPT_TEMPLATE = """请基于“语义闭环 + 知识类型纯净 + 时间连续性”进行语义分割，并按 Group 输出。

输出要求（严格）：
1. 只输出 JSON，不输出解释文本。
2. 顶层字段只能是 `knowledge_groups`。
3. 每个 Group 只能有字段：`group_name`, `reason`, `units`。
4. 每个 Unit 只能有字段：`pids`, `k`, `m`, `title`。
5. `k` 只能是 `0/1/2`；`m` 只能是 `0/1`（整数）。
6. 同一核心论点下，Abstract 与 Process/Concrete 必须拆分为不同 Unit，但放在同一 Group。
7. 仅当核心论点变化时，才允许新建 Group。
8. 严禁输出 `semantic_units`、`group_id`、`reasoning`、`confidence`、`text`、`full_text` 等字段。

输入段落：
{paragraphs_json}

输出模板：
{{
  "knowledge_groups": [
    {{
      "group_name": "CloudBot 环境配置",
      "reason": "围绕 CloudBot 从准备、配置到验证的同一核心论点",
      "units": [
        {{"pids": ["P001", "P002"], "k": 0, "m": 0, "title": "CloudBot 配置前置说明"}},
        {{"pids": ["P003", "P004"], "k": 2, "m": 1, "title": "配置 CloudBot 运行环境"}}
      ]
    }}
  ]
}}
"""


# =============================================================================
# LLM Prompts - Phase 3: Cross-modal Conflict Resolution
# =============================================================================

RESEGMENT_SYSTEM_PROMPT = """你是语义单元切分专家，擅长处理跨模态冲突。
CV 模块检测到某些语义单元存在视觉与文本不一致，你需要决定如何处理这些冲突。

决策原则：
1. 如果文本语义确实跨越了不同知识类型边界 -> 拆分（split）。
2. 如果只是首尾包含冗余画面但核心语义完整 -> 微调边界（adjust）。
3. 如果视觉变化只是动画效果或无关干扰 -> 保持原判（keep）。"""

RESEGMENT_USER_PROMPT = """CV 模块检测到以下语义单元存在“跨模态冲突”，请结合视觉锚点信息重新审视。

## 冲突单元信息
- **Unit ID**: {unit_id}
- **当前文本**: "{text}"
- **当前时序**: {start_sec:.1f}s - {end_sec:.1f}s
- **预判类型**: {llm_type}
- **视觉统计**: 稳定岛={s_stable:.0%}, 动作={s_action:.0%}, 冗余={s_redundant:.0%}
- **视觉锚点**: {anchors}（这些时间点发生了显著视觉状态切换）
- **冲突原因**: {reason}

## 决策选项

### 1. Split（强制拆分）
- 场景：文本明显对应不同视觉状态（如前半是概念讲解，后半是操作演示）。
- 视觉锚点可以作为自然分界线。
- 返回 `split_point`（必须接近某个视觉锚点）。

### 2. Adjust（边界微调）
- 场景：单元核心语义完整，但首尾混入了无关转场/冗余画面。
- 返回 `new_timeline=[start_sec, end_sec]` 收缩或扩展边界。

### 3. Keep（保持原判）
- 场景：视觉变化仅是 PPT 动画或无关干扰，文本语义不可拆分。
- 保持原时间范围，并说明理由。

## 输出格式（JSON）
```json
{{
    "decision": "split" | "adjust" | "keep",
    "rationale": "决策理由（20字以内）",
    "split_point": 12.5,
    "new_timeline": [10.0, 25.0]
}}
```

注意：
- `split` 时必须提供 `split_point`（秒）。
- `adjust` 时必须提供 `new_timeline`（[start, end]）。
- `keep` 时两者都不需要。

请输出 JSON 决策:"""


# =============================================================================
# Main Segmenter Class
# =============================================================================

class SemanticUnitSegmenter:
    """类说明：SemanticUnitSegmenter 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    
    def __init__(self, llm_client=None):
        """
        方法说明：执行当前方法的核心处理流程并返回结果。
        设计原因：封装步骤与依赖，降低调用链复杂度。
        权衡：注释保持精简，具体细节以实现与测试为准。
        """
        self.llm_client = llm_client
        self._ensure_llm_client()
        self._segment_system_prompt = get_prompt(
            PromptKeys.DEEPSEEK_SEMANTIC_SEGMENT_SYSTEM,
            fallback=SYSTEM_PROMPT,
        )
        self._segment_user_template = get_prompt(
            PromptKeys.DEEPSEEK_SEMANTIC_SEGMENT_USER,
            fallback=USER_PROMPT_TEMPLATE,
        )
        self._resegment_system_prompt = get_prompt(
            PromptKeys.DEEPSEEK_SEMANTIC_RESEGMENT_SYSTEM,
            fallback=RESEGMENT_SYSTEM_PROMPT,
        )
        self._resegment_user_template = get_prompt(
            PromptKeys.DEEPSEEK_SEMANTIC_RESEGMENT_USER,
            fallback=RESEGMENT_USER_PROMPT,
        )
    
    def _ensure_llm_client(self):
        """方法说明：SemanticUnitSegmenter._ensure_llm_client 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        if self.llm_client is None:
            self.llm_client = llm_gateway.get_deepseek_client()
            logger.info("SemanticUnitSegmenter: LLM client initialized via gateway")
    
    async def segment(
        self,
        paragraphs: List[Dict[str, Any]],
        sentence_timestamps: Dict[str, Dict[str, float]] = None,
        batch_size: int = 10,
        cache_path: str = None
    ) -> SegmentationResult:
        """方法说明：SemanticUnitSegmenter.segment 核心方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        import time
        start_time = time.time()

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
                llm_token_usage=0,
                processing_time_ms=0.0,
            )

        paragraphs_for_llm = [
            {
                "paragraph_id": p.get("paragraph_id", f"P{idx + 1:03d}"),
                "text": p.get("text", ""),
                "source_sentence_ids": p.get("source_sentence_ids", []),
            }
            for idx, p in enumerate(paragraphs)
        ]
        effective_batch_size = max(1, int(batch_size or 1))
        paragraph_batches = self._chunk_paragraphs(paragraphs_for_llm, effective_batch_size)
        hedge_context_base = self._build_segmentation_hedge_context(
            paragraphs=paragraphs_for_llm,
            sentence_timestamps=sentence_timestamps,
        )
        logger.info(
            "Sending batched LLM requests for %s paragraphs "
            "(batch_size_ignored=%s, batches=%s, input_budget_tokens=%s)",
            len(paragraphs_for_llm),
            effective_batch_size,
            len(paragraph_batches),
            SEGMENTATION_INPUT_TOKEN_BUDGET,
        )

        all_units: List[SemanticUnit] = []
        total_tokens = 0
        unit_counter = 1

        try:
            batch_results = await self._segment_batches_concurrently(
                paragraph_batches=paragraph_batches,
                paragraphs=paragraphs,
                sentence_timestamps=sentence_timestamps,
                hedge_context_base=hedge_context_base,
            )
            total_tokens += sum(item["token_usage"] for item in batch_results)
            all_units = await self._merge_batches_with_boundary_judgement(
                batch_results=batch_results,
                hedge_context_base=hedge_context_base,
            )
            total_tokens += sum(item.get("boundary_token_usage", 0) for item in batch_results[1:])

            for idx, unit in enumerate(all_units, start=1):
                unit.unit_id = f"SU{idx:03d}"

        except Exception as e:
            logger.error(f"LLM call failed or strict parse failed: {e}")
            all_units = []
            total_tokens = 0
            unit_counter = 1

            for p in paragraphs:
                paragraph_id = p.get("paragraph_id", f"P{unit_counter:03d}")
                text = p.get("text", "")
                source_sentence_ids = p.get("source_sentence_ids", [])
                fallback_group_name = self._build_topic_from_text(text)
                start_sec, end_sec = self._calculate_timestamps(
                    [paragraph_id],
                    paragraphs,
                    sentence_timestamps,
                )

                unit = SemanticUnit(
                    unit_id=f"SU{unit_counter:03d}",
                    knowledge_type="abstract",
                    knowledge_topic=self._build_topic_from_text(text),
                    group_name=fallback_group_name,
                    full_text=text,
                    source_paragraph_ids=[paragraph_id],
                    source_sentence_ids=source_sentence_ids,
                    start_sec=start_sec,
                    end_sec=end_sec,
                    confidence=0.5,
                    mult_steps=False,
                )
                all_units.append(unit)
                unit_counter += 1

        self._assign_group_ids(all_units, start_id=1)

        elapsed_ms = (time.time() - start_time) * 1000

        result = SegmentationResult(
            semantic_units=all_units,
            total_paragraphs_input=len(paragraphs),
            total_units_output=len(all_units),
            llm_token_usage=total_tokens,
            processing_time_ms=elapsed_ms,
        )

        logger.info(
            f"Segmentation complete: {result.total_paragraphs_input} paragraphs 鈫?"
            f"{result.total_units_output} units, {total_tokens} tokens, {elapsed_ms:.0f}ms"
        )

        if cache_path:
            try:
                self._save_to_cache(result, cache_path)
                logger.info(f"Saved segmentation result to cache: {cache_path}")
            except Exception as e:
                logger.warning(f"Failed to save cache to {cache_path}: {e}")

        return result

    async def _segment_batches_concurrently(
        self,
        paragraph_batches: List[List[Dict[str, Any]]],
        paragraphs: List[Dict[str, Any]],
        sentence_timestamps: Dict[str, Dict[str, float]] = None,
        hedge_context_base: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        semaphore = asyncio.Semaphore(_segment_batch_max_concurrency())

        async def _run_one(
            batch_index: int,
            batch_paragraphs: List[Dict[str, Any]],
        ) -> Dict[str, Any]:
            async with semaphore:
                batch_units, token_usage = await self._segment_single_batch(
                    batch_index=batch_index,
                    batch_paragraphs=batch_paragraphs,
                    paragraphs=paragraphs,
                    sentence_timestamps=sentence_timestamps,
                    hedge_context_base=hedge_context_base,
                )
                return {
                    "batch_index": batch_index,
                    "batch_paragraphs": batch_paragraphs,
                    "batch_units": batch_units,
                    "token_usage": token_usage,
                    "boundary_token_usage": 0,
                }

        tasks = [
            asyncio.create_task(_run_one(idx, batch_paragraphs))
            for idx, batch_paragraphs in enumerate(paragraph_batches, start=1)
        ]
        if not tasks:
            return []

        results = await asyncio.gather(*tasks)
        results.sort(key=lambda item: item["batch_index"])
        return results

    async def _segment_single_batch(
        self,
        batch_index: int,
        batch_paragraphs: List[Dict[str, Any]],
        paragraphs: List[Dict[str, Any]],
        sentence_timestamps: Dict[str, Dict[str, float]] = None,
        hedge_context_base: Optional[Dict[str, Any]] = None,
    ) -> Tuple[List[SemanticUnit], int]:
        prompt = self._build_segment_prompt(
            batch_paragraphs=batch_paragraphs,
            prev_tail_unit=None,
        )

        hedge_context = self._build_batch_hedge_context(
            batch_paragraphs=batch_paragraphs,
            hedge_context_base=hedge_context_base,
        )
        try:
            result_json, metadata, _ = await llm_gateway.deepseek_complete_json(
                prompt=prompt,
                system_message=self._segment_system_prompt,
                max_tokens=SEGMENTATION_MAX_OUTPUT_TOKENS,
                hedge_context=hedge_context,
                client=self.llm_client,
            )
        except TypeError:
            result_json, metadata, _ = await llm_gateway.deepseek_complete_json(
                prompt=prompt,
                system_message=self._segment_system_prompt,
                hedge_context=hedge_context,
                client=self.llm_client,
            )

        batch_tokens = int(getattr(metadata, "total_tokens", 0) or 0)
        batch_units: List[SemanticUnit] = []
        grouped_units_count = 0
        legacy_units_count = 0

        # 新格式（推荐）: knowledge_groups -> units[]。
        if isinstance(result_json, dict) and isinstance(result_json.get("knowledge_groups"), list):
            for raw_group in result_json.get("knowledge_groups", []):
                parsed_group = self._parse_group_schema(raw_group, batch_paragraphs)
                if parsed_group is None:
                    raise ValueError("Invalid group schema under strict mode")
                group_name = parsed_group["group_name"]
                group_reason = parsed_group["reason"]
                for parsed_unit in parsed_group["units"]:
                    paragraph_ids = parsed_unit["pids"]
                    knowledge_type = self._decode_knowledge_type(parsed_unit["k"])
                    mult_steps = parsed_unit["m"] == 1
                    full_text = self._collect_text_by_paragraph_ids(paragraph_ids, paragraphs)
                    source_sentence_ids = self._collect_sentence_ids(paragraph_ids, paragraphs)
                    start_sec, end_sec = self._calculate_timestamps(
                        paragraph_ids,
                        paragraphs,
                        sentence_timestamps,
                    )
                    unit_title = parsed_unit.get("title") or self._build_topic_from_text(full_text)

                    batch_units.append(
                        SemanticUnit(
                            unit_id=f"SU_TMP_{batch_index}_{len(batch_units) + 1}",
                            knowledge_type=knowledge_type,
                            knowledge_topic=unit_title,
                            group_name=group_name,
                            group_reason=group_reason,
                            full_text=full_text,
                            source_paragraph_ids=paragraph_ids,
                            source_sentence_ids=source_sentence_ids,
                            start_sec=start_sec,
                            end_sec=end_sec,
                            confidence=DEFAULT_UNIT_CONFIDENCE,
                            mult_steps=mult_steps,
                        )
                    )
                    grouped_units_count += 1
        # 旧格式（兼容）: semantic_units[] + unit.group_name。
        elif isinstance(result_json, dict) and isinstance(result_json.get("semantic_units"), list):
            units_data = result_json.get("semantic_units", [])
            for raw_unit in units_data:
                parsed = self._parse_min_schema_unit(raw_unit, batch_paragraphs, require_group_name=True)
                if parsed is None:
                    raise ValueError("Invalid unit schema under strict mode")

                paragraph_ids = parsed["pids"]
                knowledge_type = self._decode_knowledge_type(parsed["k"])
                mult_steps = parsed["m"] == 1
                full_text = self._collect_text_by_paragraph_ids(paragraph_ids, paragraphs)
                source_sentence_ids = self._collect_sentence_ids(paragraph_ids, paragraphs)
                start_sec, end_sec = self._calculate_timestamps(
                    paragraph_ids,
                    paragraphs,
                    sentence_timestamps,
                )
                unit_title = parsed.get("title") or self._build_topic_from_text(full_text)
                group_name = parsed.get("group_name") or unit_title

                batch_units.append(
                    SemanticUnit(
                        unit_id=f"SU_TMP_{batch_index}_{len(batch_units) + 1}",
                        knowledge_type=knowledge_type,
                        knowledge_topic=unit_title,
                        group_name=group_name,
                        full_text=full_text,
                        source_paragraph_ids=paragraph_ids,
                        source_sentence_ids=source_sentence_ids,
                        start_sec=start_sec,
                        end_sec=end_sec,
                        confidence=DEFAULT_UNIT_CONFIDENCE,
                        mult_steps=mult_steps,
                    )
                )
                legacy_units_count += 1
        else:
            raise ValueError("Invalid segmentation output: require knowledge_groups or semantic_units")

        logger.debug(
            "Batch %s parsed semantic units=%s (grouped=%s, legacy=%s, tokens=%s)",
            batch_index,
            len(batch_units),
            grouped_units_count,
            legacy_units_count,
            batch_tokens,
        )

        return batch_units, batch_tokens

    async def _merge_batches_with_boundary_judgement(
        self,
        batch_results: List[Dict[str, Any]],
        hedge_context_base: Optional[Dict[str, Any]] = None,
    ) -> List[SemanticUnit]:
        if not batch_results:
            return []

        merged_units: List[SemanticUnit] = list(batch_results[0]["batch_units"])
        for batch_result in batch_results[1:]:
            batch_units = list(batch_result["batch_units"])
            if merged_units and batch_units:
                prev_tail = merged_units[-1]
                next_head = batch_units[0]
                if not self._can_merge_boundary_units(prev_tail, next_head):
                    should_merge = False
                    boundary_tokens = 0
                    merged_update = {}
                else:
                    should_merge, boundary_tokens, merged_update = await self._judge_boundary_merge_with_llm(
                        prev_tail=prev_tail,
                        next_head=next_head,
                        hedge_context_base=hedge_context_base,
                    )
                batch_result["boundary_token_usage"] = boundary_tokens
                if should_merge and self._can_merge_boundary_units(prev_tail, next_head, merged_update):
                    self._merge_unit_into_prev_tail(
                        prev_tail,
                        next_head,
                        merged_update=merged_update,
                    )
                    batch_units = batch_units[1:]
                elif should_merge:
                    logger.info(
                        "Boundary merge rejected by purity guard: prev=%s(%s,%s), next=%s(%s,%s)",
                        prev_tail.unit_id,
                        prev_tail.knowledge_type,
                        prev_tail.group_name,
                        next_head.unit_id,
                        next_head.knowledge_type,
                        next_head.group_name,
                    )

            merged_units.extend(batch_units)

        return merged_units

    async def _judge_boundary_merge_with_llm(
        self,
        prev_tail: SemanticUnit,
        next_head: SemanticUnit,
        hedge_context_base: Optional[Dict[str, Any]] = None,
    ) -> Tuple[bool, int, Dict[str, Any]]:
        system_prompt = (
            "You judge whether two adjacent semantic units should be merged. "
            "Follow the same segmentation merge/split rules and return JSON only."
        )
        prompt = self._build_boundary_merge_prompt(prev_tail, next_head)

        boundary_batch_chars = len(str(prev_tail.full_text or "")) + len(str(next_head.full_text or ""))
        hedge_context = dict(hedge_context_base or {})
        hedge_context["batch_text_chars"] = max(0, int(boundary_batch_chars))
        try:
            try:
                result_json, metadata, _ = await llm_gateway.deepseek_complete_json(
                    prompt=prompt,
                    system_message=system_prompt,
                    max_tokens=SEGMENTATION_BOUNDARY_MAX_OUTPUT_TOKENS,
                    hedge_context=hedge_context,
                    client=self.llm_client,
                )
            except TypeError:
                result_json, metadata, _ = await llm_gateway.deepseek_complete_json(
                    prompt=prompt,
                    system_message=system_prompt,
                    hedge_context=hedge_context,
                    client=self.llm_client,
                )

            merge_flag = self._normalize_boundary_merge_flag(result_json)
            merged_update = self._extract_boundary_merge_update(result_json)
            token_usage = int(getattr(metadata, "total_tokens", 0) or 0)
            return merge_flag, token_usage, merged_update
        except Exception as exc:
            logger.warning(
                "Boundary merge judgement failed, fallback to heuristic: %s",
                exc,
            )
            return self._should_merge_boundary_with_heuristic(prev_tail, next_head), 0, {}

    def _build_boundary_merge_prompt(
        self,
        prev_tail: SemanticUnit,
        next_head: SemanticUnit,
    ) -> str:
        rule_reference = (self._segment_system_prompt or "")[:SEGMENTATION_BOUNDARY_RULE_REFERENCE_CHARS]
        prev_payload = {
            "unit_id": prev_tail.unit_id,
            "knowledge_type": prev_tail.knowledge_type,
            "knowledge_topic": prev_tail.knowledge_topic,
            "group_name": prev_tail.group_name,
            "mult_steps": 1 if prev_tail.mult_steps else 0,
            "source_paragraph_ids": prev_tail.source_paragraph_ids,
            "text_tail": (prev_tail.full_text or "")[-SEGMENTATION_PREV_TAIL_TEXT_CHARS:],
        }
        next_payload = {
            "unit_id": next_head.unit_id,
            "knowledge_type": next_head.knowledge_type,
            "knowledge_topic": next_head.knowledge_topic,
            "group_name": next_head.group_name,
            "mult_steps": 1 if next_head.mult_steps else 0,
            "source_paragraph_ids": next_head.source_paragraph_ids,
            "text_head": (next_head.full_text or "")[:SEGMENTATION_PREV_TAIL_TEXT_CHARS],
        }
        return (
            "[Boundary Merge Decision]\n"
            "Reference segmentation rules (must follow):\n"
            f"{rule_reference}\n\n"
            "Decide whether these two adjacent units belong to one continuous semantic unit.\n"
            "Never merge if knowledge_type differs. Keep knowledge type purity.\n"
            "Output JSON only:\n"
            "{\"merge\": 0 or 1, \"reason\": \"...\", "
            "\"merged_unit\": {\"k\": 0|1|2, \"m\": 0|1, \"title\": \"...\", \"group_name\": \"...\"}}\n"
            "When merge=0, set merged_unit to {}.\n\n"
            f"previous_tail={json.dumps(prev_payload, ensure_ascii=False)}\n"
            f"next_head={json.dumps(next_payload, ensure_ascii=False)}\n"
        )

    def _normalize_boundary_merge_flag(self, result_json: Any) -> bool:
        if not isinstance(result_json, dict):
            return False

        value = result_json.get("merge", 0)
        if isinstance(value, bool):
            return bool(value)
        if isinstance(value, (int, float)):
            return int(value) == 1
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "y", "merge"}
        return False

    def _extract_boundary_merge_update(self, result_json: Any) -> Dict[str, Any]:
        if not isinstance(result_json, dict):
            return {}

        merged_unit = result_json.get("merged_unit")
        if not isinstance(merged_unit, dict):
            merged_unit = {}

        update: Dict[str, Any] = {}
        k_value = merged_unit.get("k")
        if isinstance(k_value, int) and k_value in KNOWLEDGE_TYPE_CODE_MAP:
            update["knowledge_type"] = self._decode_knowledge_type(k_value)

        title = merged_unit.get("title")
        if isinstance(title, str) and title.strip():
            update["knowledge_topic"] = title.strip()

        group_name = merged_unit.get("group_name")
        if isinstance(group_name, str) and group_name.strip():
            update["group_name"] = group_name.strip()

        m_value = merged_unit.get("m")
        if isinstance(m_value, int) and m_value in (0, 1):
            update["mult_steps"] = bool(m_value)

        return update

    def _should_merge_boundary_with_heuristic(
        self,
        prev_tail: SemanticUnit,
        next_head: SemanticUnit,
    ) -> bool:
        if prev_tail.knowledge_type != next_head.knowledge_type:
            return False
        if not self._are_group_names_compatible(prev_tail.group_name, next_head.group_name):
            return False
        if bool(prev_tail.mult_steps) != bool(next_head.mult_steps):
            return False
        return self._are_titles_compatible(prev_tail.knowledge_topic, next_head.knowledge_topic)

    def _chunk_paragraphs(
        self,
        paragraphs_for_llm: List[Dict[str, Any]],
        batch_size: int,
    ) -> List[List[Dict[str, Any]]]:
        # Keep `batch_size` in signature for backward compatibility; chunking is token-budget only.
        _ = batch_size
        if not paragraphs_for_llm:
            return []

        batches: List[List[Dict[str, Any]]] = []
        current_batch: List[Dict[str, Any]] = []

        for paragraph in paragraphs_for_llm:
            single_estimated_tokens = self._estimate_segment_input_tokens([paragraph])
            if single_estimated_tokens > SEGMENTATION_INPUT_TOKEN_BUDGET:
                logger.warning(
                    "Single paragraph estimated input tokens exceed budget: pid=%s, estimated=%s, budget=%s",
                    paragraph.get("paragraph_id"),
                    single_estimated_tokens,
                    SEGMENTATION_INPUT_TOKEN_BUDGET,
                )

            candidate_batch = current_batch + [paragraph]
            candidate_tokens = self._estimate_segment_input_tokens(candidate_batch)
            exceed_token_budget = candidate_tokens > SEGMENTATION_INPUT_TOKEN_BUDGET

            if current_batch and exceed_token_budget:
                batches.append(current_batch)
                current_batch = [paragraph]
                continue

            current_batch = candidate_batch

        if current_batch:
            batches.append(current_batch)

        return batches

    def _estimate_segment_input_tokens(
        self,
        batch_paragraphs: List[Dict[str, Any]],
    ) -> int:
        paragraphs_json = json.dumps(batch_paragraphs, ensure_ascii=False, indent=2)
        approx_chars = (
            len(self._segment_system_prompt)
            + len(self._segment_user_template)
            + len(paragraphs_json)
            + SEGMENTATION_PREV_TAIL_TEXT_CHARS
        )
        chars_per_token = max(0.1, float(SEGMENTATION_EST_CHARS_PER_TOKEN))
        estimated_tokens = int(approx_chars / chars_per_token)
        return estimated_tokens + SEGMENTATION_PROMPT_TOKEN_BUFFER

    def _build_segmentation_hedge_context(
        self,
        *,
        paragraphs: List[Dict[str, Any]],
        sentence_timestamps: Optional[Dict[str, Dict[str, float]]],
    ) -> Dict[str, Any]:
        step6_text_chars = 0
        for paragraph in paragraphs or []:
            step6_text_chars += len(str(paragraph.get("text", "") or ""))

        video_duration_sec = 0.0
        if isinstance(sentence_timestamps, dict) and sentence_timestamps:
            for ts in sentence_timestamps.values():
                if not isinstance(ts, dict):
                    continue
                end_sec = float(ts.get("end_sec", 0.0) or 0.0)
                if end_sec > video_duration_sec:
                    video_duration_sec = end_sec

        return {
            "step6_text_chars": max(0, int(step6_text_chars)),
            "video_duration_sec": max(0.0, float(video_duration_sec)),
        }

    def _build_batch_hedge_context(
        self,
        *,
        batch_paragraphs: List[Dict[str, Any]],
        hedge_context_base: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        batch_text_chars = 0
        for paragraph in batch_paragraphs or []:
            batch_text_chars += len(str(paragraph.get("text", "") or ""))
        hedge_context = dict(hedge_context_base or {})
        hedge_context["batch_text_chars"] = max(0, int(batch_text_chars))
        return hedge_context

    def _build_segment_prompt(
        self,
        batch_paragraphs: List[Dict[str, Any]],
        prev_tail_unit: Optional[SemanticUnit],
    ) -> str:
        paragraphs_json = json.dumps(batch_paragraphs, ensure_ascii=False, indent=2)
        prev_tail_unit_json = self._serialize_prev_tail_unit(prev_tail_unit)
        prompt = render_prompt(
            PromptKeys.DEEPSEEK_SEMANTIC_SEGMENT_USER,
            context={
                "paragraphs_json": paragraphs_json,
                "prev_tail_unit_json": prev_tail_unit_json,
            },
            fallback=self._segment_user_template,
        )
        if prev_tail_unit is None:
            return prompt

        continuation_hint = (
            "\n\n[Cross-batch continuation rules]\n"
            "You are processing one batch from a long sequence.\n"
            "If this batch starts with content that continues the previous batch tail unit, "
            "you may append these paragraphs into that previous unit.\n"
            "When doing so, do NOT repeat the previous tail unit as a new semantic unit in this batch output.\n"
            "Previous batch tail unit (empty JSON means first batch):\n"
            f"{prev_tail_unit_json}\n"
        )
        return f"{prompt}{continuation_hint}"

    def _serialize_prev_tail_unit(self, prev_tail_unit: Optional[SemanticUnit]) -> str:
        if prev_tail_unit is None:
            return "{}"

        tail_text = (prev_tail_unit.full_text or "").strip()
        if len(tail_text) > SEGMENTATION_PREV_TAIL_TEXT_CHARS:
            tail_text = tail_text[-SEGMENTATION_PREV_TAIL_TEXT_CHARS:]

        payload = {
            "unit_id": prev_tail_unit.unit_id,
            "knowledge_type": prev_tail_unit.knowledge_type,
            "knowledge_topic": prev_tail_unit.knowledge_topic,
            "group_name": prev_tail_unit.group_name,
            "mult_steps": 1 if prev_tail_unit.mult_steps else 0,
            "source_paragraph_ids": prev_tail_unit.source_paragraph_ids[-6:],
            "tail_text": tail_text,
        }
        return json.dumps(payload, ensure_ascii=False, indent=2)

    def _should_merge_batch_head_with_prev_tail(
        self,
        prev_tail: SemanticUnit,
        batch_head: SemanticUnit,
        batch_paragraphs: List[Dict[str, Any]],
    ) -> bool:
        if not batch_paragraphs or not batch_head.source_paragraph_ids:
            return False
        first_batch_pid = batch_paragraphs[0].get("paragraph_id")
        first_head_pid = batch_head.source_paragraph_ids[0]
        if first_batch_pid != first_head_pid:
            return False
        if prev_tail.knowledge_type != batch_head.knowledge_type:
            return False
        if not self._are_group_names_compatible(prev_tail.group_name, batch_head.group_name):
            return False
        if bool(prev_tail.mult_steps) != bool(batch_head.mult_steps):
            return False
        return self._are_titles_compatible(prev_tail.knowledge_topic, batch_head.knowledge_topic)

    def _are_titles_compatible(self, left: str, right: str) -> bool:
        left_norm = "".join(ch.lower() for ch in (left or "") if ch.isalnum())
        right_norm = "".join(ch.lower() for ch in (right or "") if ch.isalnum())
        if not left_norm or not right_norm:
            return False
        if left_norm == right_norm:
            return True
        if left_norm in right_norm or right_norm in left_norm:
            return True
        left_set = set(left_norm)
        right_set = set(right_norm)
        overlap = len(left_set & right_set)
        baseline = max(1, min(len(left_set), len(right_set)))
        return (overlap / baseline) >= 0.6

    def _normalize_group_name(self, group_name: str) -> str:
        normalized = "".join(ch.lower() for ch in (group_name or "").strip() if ch.isalnum())
        if normalized:
            return normalized
        return (group_name or "").strip().lower()

    def _build_group_name_fallback(self, unit: SemanticUnit) -> str:
        if isinstance(unit.group_name, str) and unit.group_name.strip():
            return unit.group_name.strip()
        if isinstance(unit.knowledge_topic, str) and unit.knowledge_topic.strip():
            return unit.knowledge_topic.strip()
        return self._build_topic_from_text(getattr(unit, "full_text", ""))

    def _are_group_names_compatible(self, left: str, right: str) -> bool:
        left_fallback = (left or "").strip()
        right_fallback = (right or "").strip()
        if not left_fallback or not right_fallback:
            return False
        left_norm = self._normalize_group_name(left_fallback)
        right_norm = self._normalize_group_name(right_fallback)
        if left_norm and right_norm:
            if left_norm == right_norm:
                return True
            if left_norm in right_norm or right_norm in left_norm:
                return True
        return self._are_titles_compatible(left_fallback, right_fallback)

    def _can_merge_boundary_units(
        self,
        prev_tail: SemanticUnit,
        next_head: SemanticUnit,
        merged_update: Optional[Dict[str, Any]] = None,
    ) -> bool:
        if prev_tail.knowledge_type != next_head.knowledge_type:
            return False

        merged_knowledge_type = ""
        merged_group_name = ""
        if isinstance(merged_update, dict):
            merged_knowledge_type = str(merged_update.get("knowledge_type", "") or "").strip()
            merged_group_name = str(merged_update.get("group_name", "") or "").strip()
        if merged_knowledge_type and merged_knowledge_type != prev_tail.knowledge_type:
            return False

        prev_group_name = self._build_group_name_fallback(prev_tail)
        next_group_name = self._build_group_name_fallback(next_head)
        target_group_name = merged_group_name or prev_group_name

        if not self._are_group_names_compatible(prev_group_name, next_group_name):
            return False
        if not self._are_group_names_compatible(prev_group_name, target_group_name):
            return False
        if not self._are_group_names_compatible(next_group_name, target_group_name):
            return False
        return True

    def _assign_group_ids(self, units: List[SemanticUnit], start_id: int = 1) -> None:
        group_name_to_id: Dict[str, int] = {}
        next_group_id = max(1, int(start_id or 1))

        for index, unit in enumerate(units):
            group_name = self._build_group_name_fallback(unit)
            unit.group_name = group_name
            normalized_group_name = self._normalize_group_name(group_name)
            if not normalized_group_name:
                normalized_group_name = f"unknown_group_{index + 1}"
            if normalized_group_name not in group_name_to_id:
                group_name_to_id[normalized_group_name] = next_group_id
                next_group_id += 1
            unit.group_id = group_name_to_id[normalized_group_name]

    def _merge_unit_into_prev_tail(
        self,
        prev_tail: SemanticUnit,
        continuation: SemanticUnit,
        merged_update: Optional[Dict[str, Any]] = None,
    ) -> None:
        combined_paragraph_ids = list(prev_tail.source_paragraph_ids)
        for pid in continuation.source_paragraph_ids:
            if pid not in combined_paragraph_ids:
                combined_paragraph_ids.append(pid)
        prev_tail.source_paragraph_ids = combined_paragraph_ids

        combined_sentence_ids = list(prev_tail.source_sentence_ids)
        for sid in continuation.source_sentence_ids:
            if sid not in combined_sentence_ids:
                combined_sentence_ids.append(sid)
        prev_tail.source_sentence_ids = combined_sentence_ids

        continuation_text = (continuation.full_text or "").strip()
        prev_text = (prev_tail.full_text or "").strip()
        if continuation_text:
            if not prev_text:
                prev_tail.full_text = continuation_text
            elif continuation_text not in prev_text:
                prev_tail.full_text = f"{prev_text}\n{continuation_text}"

        if prev_tail.start_sec <= 0.0:
            prev_tail.start_sec = continuation.start_sec
        elif continuation.start_sec > 0.0:
            prev_tail.start_sec = min(prev_tail.start_sec, continuation.start_sec)
        prev_tail.end_sec = max(prev_tail.end_sec, continuation.end_sec)
        prev_tail.confidence = min(prev_tail.confidence, continuation.confidence)
        prev_tail.mult_steps = bool(prev_tail.mult_steps or continuation.mult_steps)
        if (not prev_tail.group_name) and continuation.group_name:
            prev_tail.group_name = continuation.group_name
        if (not prev_tail.group_reason) and continuation.group_reason:
            prev_tail.group_reason = continuation.group_reason

        if isinstance(merged_update, dict):
            knowledge_type = merged_update.get("knowledge_type")
            if isinstance(knowledge_type, str) and knowledge_type in {"abstract", "concrete", "process"}:
                prev_tail.knowledge_type = knowledge_type

            knowledge_topic = merged_update.get("knowledge_topic")
            if isinstance(knowledge_topic, str) and knowledge_topic.strip():
                prev_tail.knowledge_topic = knowledge_topic.strip()

            group_name = merged_update.get("group_name")
            if isinstance(group_name, str) and group_name.strip():
                prev_tail.group_name = group_name.strip()

            if "mult_steps" in merged_update:
                prev_tail.mult_steps = bool(merged_update.get("mult_steps"))
    
    def _save_to_cache(self, result: SegmentationResult, path: str):
        """方法说明：SemanticUnitSegmenter._save_to_cache 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        grouped_payload = build_grouped_semantic_units_payload(
            [asdict(u) for u in result.semantic_units],
            schema_version="phase2a.grouped.v1",
            default_group_reason="同一核心论点聚合",
            strip_unit_group_fields=True,
        )
        data = {
            **grouped_payload,
            "total_paragraphs_input": result.total_paragraphs_input,
            "total_units_output": result.total_units_output,
            "llm_token_usage": result.llm_token_usage,
            "processing_time_ms": result.processing_time_ms,
        }
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def _load_from_cache(self, path: str) -> SegmentationResult:
        """方法说明：SemanticUnitSegmenter._load_from_cache 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        flat_units = normalize_semantic_units_payload(data)
        units = []
        for u_data in flat_units:
            # 重建 SemanticUnit 对象
            # 处理 dataclass 字段差异 (向后兼容)
            valid_keys = SemanticUnit.__dataclass_fields__.keys()
            filtered_data = {k: v for k, v in u_data.items() if k in valid_keys}
            units.append(SemanticUnit(**filtered_data))

        total_paragraphs_input = 0
        total_units_output = len(units)
        llm_token_usage = 0
        processing_time_ms = 0.0
        if isinstance(data, dict):
            total_paragraphs_input = int(data.get("total_paragraphs_input", 0) or 0)
            total_units_output = int(data.get("total_units_output", len(units)) or len(units))
            llm_token_usage = int(data.get("llm_token_usage", 0) or 0)
            processing_time_ms = float(data.get("processing_time_ms", 0.0) or 0.0)

        return SegmentationResult(
            semantic_units=units,
            total_paragraphs_input=total_paragraphs_input,
            total_units_output=total_units_output,
            llm_token_usage=llm_token_usage,
            processing_time_ms=processing_time_ms,
        )
    
    def _calculate_timestamps(
        self,
        paragraph_ids: List[str],
        paragraphs: List[Dict],
        sentence_timestamps: Dict[str, Dict[str, float]] = None
    ) -> Tuple[float, float]:
        """方法说明：SemanticUnitSegmenter._calculate_timestamps 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        if not sentence_timestamps:
            return 0.0, 0.0
        
        # 历史乱码注释已清理。
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
        """方法说明：SemanticUnitSegmenter._collect_sentence_ids 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        sentence_ids = []
        pid_set = set(paragraph_ids)
        
        for p in paragraphs:
            if p.get("paragraph_id") in pid_set:
                sentence_ids.extend(p.get("source_sentence_ids", []))
        
        return sentence_ids

    def _parse_min_schema_unit(
        self,
        raw_unit: Any,
        paragraphs: List[Dict[str, Any]],
        require_group_name: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """方法说明：SemanticUnitSegmenter._parse_min_schema_unit 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        if not isinstance(raw_unit, dict):
            return None

        allowed_keys = {"pids", "k", "m", "title"}
        if require_group_name:
            allowed_keys.add("group_name")
        if set(raw_unit.keys()) != allowed_keys:
            return None

        normalized_pids = self._normalize_paragraph_ids(raw_unit.get("pids"), paragraphs)
        if not normalized_pids:
            return None

        k_value = raw_unit.get("k")
        if not isinstance(k_value, int) or k_value not in KNOWLEDGE_TYPE_CODE_MAP:
            return None

        m_value = raw_unit.get("m")
        if not isinstance(m_value, int) or m_value not in (0, 1):
            return None

        title_value = raw_unit.get("title")
        if not isinstance(title_value, str):
            return None
        normalized_title = title_value.strip()
        if not normalized_title:
            return None

        parsed: Dict[str, Any] = {
            "pids": normalized_pids,
            "k": k_value,
            "m": m_value,
            "title": normalized_title,
        }
        if require_group_name:
            group_name_value = raw_unit.get("group_name")
            if not isinstance(group_name_value, str):
                return None
            normalized_group_name = group_name_value.strip()
            if not normalized_group_name:
                return None
            parsed["group_name"] = normalized_group_name
        return parsed

    def _parse_group_schema(
        self,
        raw_group: Any,
        paragraphs: List[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        if not isinstance(raw_group, dict):
            return None

        allowed_group_keys = {"group_name", "reason", "units"}
        if set(raw_group.keys()) != allowed_group_keys:
            return None

        group_name_value = raw_group.get("group_name")
        if not isinstance(group_name_value, str):
            return None
        group_name = group_name_value.strip()
        if not group_name:
            return None

        reason_value = raw_group.get("reason")
        if not isinstance(reason_value, str):
            return None
        reason = reason_value.strip()
        if not reason:
            return None

        units_value = raw_group.get("units")
        if not isinstance(units_value, list) or not units_value:
            return None

        parsed_units: List[Dict[str, Any]] = []
        for raw_unit in units_value:
            parsed_unit = self._parse_min_schema_unit(
                raw_unit,
                paragraphs,
                require_group_name=False,
            )
            if parsed_unit is None:
                return None
            parsed_units.append(parsed_unit)

        return {
            "group_name": group_name,
            "reason": reason,
            "units": parsed_units,
        }

    def _normalize_paragraph_ids(
        self,
        paragraph_ids: Any,
        paragraphs: List[Dict[str, Any]],
    ) -> List[str]:
        """方法说明：SemanticUnitSegmenter._normalize_paragraph_ids 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        if not isinstance(paragraph_ids, list):
            return []

        paragraph_order = [p.get("paragraph_id") for p in paragraphs if p.get("paragraph_id")]
        paragraph_set = set(paragraph_order)
        requested_set = {
            pid for pid in paragraph_ids
            if isinstance(pid, str) and pid in paragraph_set
        }
        if not requested_set:
            return []

        return [pid for pid in paragraph_order if pid in requested_set]

    def _collect_text_by_paragraph_ids(
        self,
        paragraph_ids: List[str],
        paragraphs: List[Dict[str, Any]],
    ) -> str:
        """方法说明：SemanticUnitSegmenter._collect_text_by_paragraph_ids 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        id_set = set(paragraph_ids)
        texts: List[str] = []
        for paragraph in paragraphs:
            paragraph_id = paragraph.get("paragraph_id")
            if paragraph_id in id_set:
                text = paragraph.get("text", "")
                if isinstance(text, str):
                    texts.append(text)
        return "\n".join(texts)

    def _decode_knowledge_type(self, k_value: int) -> str:
        """方法说明：SemanticUnitSegmenter._decode_knowledge_type 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        return KNOWLEDGE_TYPE_CODE_MAP[k_value]

    def _build_topic_from_text(self, text: str) -> str:
        """方法说明：SemanticUnitSegmenter._build_topic_from_text 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        if not isinstance(text, str):
            return "鏈煡涓婚"
        topic = text.strip().replace("\n", " ")
        if not topic:
            return "鏈煡涓婚"
        return topic[:20] + "..." if len(topic) > 20 else topic

    async def _resolve_conflicts(
        self,
        conflict_packages: List,
        units: List[SemanticUnit],
        cv_result_map: Dict,
        sentence_timestamps: Dict[str, Dict[str, float]] = None
    ) -> List[SemanticUnit]:
        """方法说明：SemanticUnitSegmenter._resolve_conflicts 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        from services.python_grpc.src.content_pipeline.phase2a.vision.cv_knowledge_validator import ConflictPackage
        
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
        
        
        updated_units.sort(key=lambda u: u.start_sec)
        
        return updated_units
    
    async def _call_llm_for_decision(
        self,
        unit: SemanticUnit,
        pkg,  # ConflictPackage
        cv_result  # CVValidationResult
    ) -> Dict[str, Any]:
        """方法说明：SemanticUnitSegmenter._call_llm_for_decision 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        # 构建prompt参数
        vision_stats = cv_result.vision_stats if cv_result else None
        
        prompt = render_prompt(
            PromptKeys.DEEPSEEK_SEMANTIC_RESEGMENT_USER,
            context={
                "unit_id": unit.unit_id,
                "text": unit.full_text[:200] + "..." if len(unit.full_text) > 200 else unit.full_text,
                "start_sec": unit.start_sec,
                "end_sec": unit.end_sec,
                "llm_type": unit.knowledge_type,
                "s_stable": vision_stats.s_stable if vision_stats else 0,
                "s_action": vision_stats.s_action if vision_stats else 0,
                "s_redundant": vision_stats.s_redundant if vision_stats else 0,
                "anchors": pkg.vision_anchors if pkg else [],
                "reason": pkg.conflict_reason if pkg else "unknown",
            },
            fallback=self._resegment_user_template,
        )

        try:
            result, metadata, _ = await llm_gateway.deepseek_complete_json(
                prompt=prompt,
                system_message=self._resegment_system_prompt,
                client=self.llm_client,
            )
            return result
        except Exception as e:
            logger.error(f"LLM decision call failed: {e}")
            return {"decision": "keep", "rationale": f"LLM璋冪敤澶辫触: {e}"}
    
    def _execute_decision(
        self,
        decision: Dict[str, Any],
        unit: SemanticUnit,
        sentence_timestamps: Dict[str, Dict[str, float]] = None
    ) -> List[SemanticUnit]:
        """方法说明：SemanticUnitSegmenter._execute_decision 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        action = decision.get("decision", "keep").lower()
        
        if action == "split":
            return self._execute_split(decision, unit, sentence_timestamps)
        elif action == "adjust":
            return self._execute_adjust(decision, unit)
        else:  # keep
            unit.__dict__['cross_modal_suspected'] = True
            unit.__dict__['cv_abnormal_reason'] = decision.get("rationale", "淇濇寔鍘熷垽")
            return [unit]
    
    def _execute_split(
        self,
        decision: Dict[str, Any],
        unit: SemanticUnit,
        sentence_timestamps: Dict[str, Dict[str, float]] = None
    ) -> List[SemanticUnit]:
        """方法说明：SemanticUnitSegmenter._execute_split 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
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
        
        
        if not before_ids or not after_ids:
            unit.__dict__['cross_modal_suspected'] = True
            return [unit]
        
        
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
        """方法说明：SemanticUnitSegmenter._execute_adjust 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        new_timeline = decision.get("new_timeline", [])
        if len(new_timeline) != 2:
            unit.__dict__['cross_modal_suspected'] = True
            return [unit]
        
        new_start, new_end = new_timeline
        
        
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
        """方法说明：SemanticUnitSegmenter._collect_text_by_ids 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        
        # 历史乱码注释已清理。
        return f"[Sentences: {', '.join(sentence_ids)}]"

    
