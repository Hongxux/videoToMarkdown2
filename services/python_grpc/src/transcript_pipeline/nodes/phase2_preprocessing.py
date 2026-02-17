"""
模块说明：阶段流程节点 phase2_preprocessing 的实现。
执行逻辑：
1) 聚合本模块的类/函数，对外提供核心能力。
2) 通过内部调用与外部依赖完成具体处理。
实现方式：通过模块内函数组合与外部依赖调用实现。
核心价值：统一模块职责边界，降低跨文件耦合成本。
输入：
- 调用方传入的参数与数据路径。
输出：
- 各函数/类返回的结构化结果或副作用。"""

import asyncio
import json
import os
import re
import time
from collections import Counter
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional, TypeVar

from ..state import PipelineState
from ..llm.client import create_llm_client
from ..tools.storage import LocalStorage
from ..monitoring.logger import get_logger
from .step_contracts import (
    apply_step2_corrections_to_text as _apply_step2_corrections_to_text_impl,
    assemble_step3_merged_sentences as _assemble_step3_merged_sentences_impl,
    assemble_step4_cleaned_sentences as _assemble_step4_cleaned_sentences_impl,
    build_fallback_paragraphs as _build_fallback_paragraphs_impl,
    build_step3_window_candidates as _build_step3_window_candidates_impl,
    deduplicate_paragraphs as _deduplicate_paragraphs_impl,
    merge_step4_cleaned_maps as _merge_step4_cleaned_maps_impl,
    normalize_step2_corrections as _normalize_step2_corrections_impl,
    order_records_by_reference_ids as _order_records_by_reference_ids,
    parse_step2_llm_payload as _parse_step2_llm_payload_impl,
    parse_step3_merged_sentences as _parse_step3_merged_sentences_impl,
    parse_step35_translated_sentences as _parse_step35_translated_sentences_impl,
    parse_step4_cleaned_sentences as _parse_step4_cleaned_sentences_impl,
    parse_step56_dedup_merge_payload as _parse_step56_dedup_merge_payload_impl,
    reconcile_step2_item as _reconcile_step2_item_impl,
    reconcile_step4_item as _reconcile_step4_item_impl,
    sentence_id_and_text_pairs as _sentence_id_and_text_pairs_impl,
)


_ItemT = TypeVar("_ItemT")
_ResultT = TypeVar("_ResultT")


def _summarize_error(error: Exception) -> str:
    """归一化错误摘要，避免日志刷屏。"""
    message = str(error).strip().replace("\n", " ")
    if len(message) > 200:
        message = message[:200] + "..."
    return f"{type(error).__name__}: {message}"


def _contains_cjk(text: str) -> bool:
    """判断文本是否包含中文字符（CJK Unified Ideographs）。"""
    return bool(re.search(r"[\u4e00-\u9fff]", str(text or "")))


_CJK_EN_GLOSSARY_PATTERN = re.compile(
    r"(?P<zh>[\u4e00-\u9fff][\u4e00-\u9fffA-Za-z0-9_+\-]{0,40})\s*[（(]\s*(?P<en>[A-Za-z][A-Za-z0-9_+./\- ]{0,60})\s*[）)]"
)


def _drops_cjk_en_glossary_pair(source_text: str, cleaned_text: str) -> bool:
    """检测清理后是否误删“中文（English）”术语对照。"""
    source = str(source_text or "")
    cleaned = str(cleaned_text or "")
    if not source or not cleaned:
        return False
    normalized_cleaned = cleaned.replace("（", "(").replace("）", ")").lower()
    lowered_cleaned = cleaned.lower()
    for match in _CJK_EN_GLOSSARY_PATTERN.finditer(source):
        full_pair = str(match.group(0) or "").strip()
        zh_term = str(match.group("zh") or "").strip()
        en_term = str(match.group("en") or "").strip()
        if not full_pair or not zh_term or not en_term:
            continue
        normalized_pair = full_pair.replace("（", "(").replace("）", ")").lower()
        if normalized_pair in normalized_cleaned:
            continue
        if zh_term in cleaned and en_term.lower() in lowered_cleaned:
            continue
        return True
    return False


def _read_int_env(name: str, default: int) -> int:
    """读取整数环境变量，异常时回退默认值。"""
    raw = os.getenv(name)
    if raw is None:
        return int(default)
    try:
        return int(str(raw).strip())
    except Exception:
        return int(default)


def _read_bool_env(name: str, default: bool) -> bool:
    """读取布尔环境变量。"""
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    value = str(raw).strip().lower()
    return value not in {"0", "false", "no", "off"}


def _resolve_step_max_inflight(step_env_prefix: str, default: int = 10) -> int:
    """
    解析步骤级有界并发上限。
    优先级：
    1) TRANSCRIPT_{STEP}_MAX_INFLIGHT
    2) TRANSCRIPT_NODE_MAX_INFLIGHT
    3) TRANSCRIPT_LLM_MAX_CONCURRENCY
    """
    value = _read_int_env(
        f"TRANSCRIPT_{step_env_prefix}_MAX_INFLIGHT",
        _read_int_env(
            "TRANSCRIPT_NODE_MAX_INFLIGHT",
            _read_int_env("TRANSCRIPT_LLM_MAX_CONCURRENCY", default),
        ),
    )
    return max(1, int(value))


async def _run_bounded_producer_consumer(
    items: List[_ItemT],
    *,
    max_inflight: int,
    handler: Callable[[int, _ItemT], Awaitable[_ResultT]],
) -> List[_ResultT]:
    """有界生产消费：限制 in-flight 数，避免全量 gather 带来的排队与内存压力。"""
    if not items:
        return []

    worker_count = max(1, min(int(max_inflight), len(items)))
    queue: "asyncio.Queue[Optional[tuple[int, _ItemT]]]" = asyncio.Queue()
    results: List[Optional[_ResultT]] = [None] * len(items)
    first_error: Optional[Exception] = None

    for idx, item in enumerate(items):
        queue.put_nowait((idx, item))
    for _ in range(worker_count):
        queue.put_nowait(None)

    async def _worker() -> None:
        nonlocal first_error
        while True:
            payload = await queue.get()
            if payload is None:
                queue.task_done()
                return
            idx, item = payload
            try:
                results[idx] = await handler(idx, item)
            except Exception as error:
                if first_error is None:
                    first_error = error
            finally:
                queue.task_done()

    workers = [asyncio.create_task(_worker()) for _ in range(worker_count)]
    await queue.join()
    await asyncio.gather(*workers)

    if first_error is not None:
        raise first_error

    return [item for item in results if item is not None]


# ============================================================================
# Step 2: 智能纠错 Prompt
# ============================================================================

STEP2_CONTEXT_WINDOW_DEFAULT = max(1, _read_int_env("TRANSCRIPT_STEP2_CONTEXT_DEFAULT_WINDOW", 1))
STEP2_CONTEXT_WINDOW_MAX = max(
    STEP2_CONTEXT_WINDOW_DEFAULT,
    _read_int_env("TRANSCRIPT_STEP2_CONTEXT_MAX_WINDOW", 5),
)

CORRECTION_PROMPT = """你是一个专业的 ASR 纠错助手，请修正以下字幕中的同音字错误。
【视频领域】{domain}

【字幕列表】{subtitles}

【纠错范围】
- 仅纠正明显的同音字错误；不确定时保留原文
- 不纠正语法错误或标点错误
- 基于领域推断专业术语
- 必须将繁体字转换为简体字
- 不做翻译任务，保留原始语言
- 仅返回需要纠错的最小片段，不要回传整句 corrected_text

【定位约束】
- 每条纠错必须包含 sid、o、c、l、r
- l/r 要尽可能短：默认从 {context_default} 个 token 起步，能唯一定位就不扩展（能 1 个就不要 2 个）
- 若上下文无法唯一定位，再逐步扩展窗口，最多到 {context_max} 个 token
- 若扩展到上限仍无法唯一定位，该纠错不要输出（宁缺毋滥）

【输出格式】只输出 JSON，不要输出其他解释文字：
{{
  "c": [
    {{
      "sid": "SUB001",
      "o": "被纠错片段",
      "c": "纠错后片段",
      "l": "原文中 o 左侧上下文",
      "r": "原文中 o 右侧上下文"
    }}
  ]
}}"""



CORRECTION_SYSTEM_PROMPT = (
    "你是字幕纠错专家。"
    "只修复明显同音/近音误识别与可确定术语错误，不擅自改写语义。"
    "输出必须是可解析 JSON。"
)


# Step2 与 Step4 合并模式补充约束：一次调用同时返回纠错补丁与清理补丁。
# 注意：保留原有 CORRECTION_PROMPT 主体，避免影响既有行为与提示词回归测试。
STEP2_MERGED_CLEANUP_APPEND_PROMPT = """
[ADDITIONAL TASK: LOCAL CLEANUP PATCH]
- In the same response, also return optional local cleanup removals.
- Cleanup scope is single subtitle only; do not rewrite sentence semantics.
- Keep bilingual glossary pairs like "中文(English)" intact.
- Return minimal removals using short keys: d[].{sid,o,l,r}
- If no cleanup is needed, return d as [].

[FINAL OUTPUT SHAPE]
{
  "c": [{"sid":"SUB001","o":"...","c":"...","l":"","r":""}],
  "d": [{"sid":"SUB001","o":"...","l":"","r":""}]
}
"""


STEP2_STEP4_MERGED_STATE_FLAG = "step2_step4_merged_done"
def _normalize_corrections(corrections: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    """标准化纠错项，过滤无效数据。"""
    normalized, _metrics = _normalize_step2_corrections_impl(corrections)
    return normalized


def _apply_corrections_to_text(
    text: str,
    corrections: List[Dict[str, str]],
) -> tuple[str, set[int]]:
    """将纠错项回放到文本，返回应用后的文本与已应用项下标集合。"""
    updated, applied_indexes, _metrics = _apply_step2_corrections_to_text_impl(text, corrections)
    return updated, applied_indexes


def _parse_step2_llm_payload(result: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    统一解析 Step2 LLM 输出，兼容：
    1) 新格式：{"c": [...]} 或 {"corrections": [...]}
    2) 旧格式：{"corrected_subtitles": [{"subtitle_id","corrected_text","corrections": [...]}]}
    """
    per_subtitle, _metrics = _parse_step2_llm_payload_impl(result)
    return per_subtitle


def _reconcile_step2_item(
    original_text: str,
    llm_corrected_text: str,
    llm_corrections: List[Dict[str, Any]],
    subtitle_id: str = "",
) -> tuple[str, List[Dict[str, str]]]:
    """统一 corrected_text 与 corrections，避免字段漂移。"""
    final_text, final_corrections, _metrics = _reconcile_step2_item_impl(
        original_text=original_text,
        llm_corrected_text=llm_corrected_text,
        llm_corrections=llm_corrections,
        subtitle_id=subtitle_id,
    )
    return final_text, final_corrections


def _reconcile_step2_item_with_metrics(
    original_text: str,
    llm_corrected_text: str,
    llm_corrections: List[Dict[str, Any]],
    subtitle_id: str = "",
) -> tuple[str, List[Dict[str, str]], Dict[str, int]]:
    """统一 corrected_text 与 corrections，并返回可观测计数。"""
    return _reconcile_step2_item_impl(
        original_text=original_text,
        llm_corrected_text=llm_corrected_text,
        llm_corrections=llm_corrections,
        subtitle_id=subtitle_id,
    )


async def step2_node(state: PipelineState) -> Dict[str, Any]:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过JSON 解析/序列化、asyncio 异步调度实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：original
    - 条件：s['subtitle_id'] not in processed_ids
    依据来源（证据链）：
    - 配置字段：subtitle_id。
    输入参数：
    - state: 函数入参（类型：PipelineState）。
    输出参数：
    - 结构化结果字典（包含关键字段信息）。"""
    logger = get_logger("step2_correction", state.get("output_dir", "output/logs"))
    logger.start()
    
    # 读取字幕（从 read_subtitle_sample 的完整版本）
    from ..tools.file_validator import read_subtitle_sample
    subtitles = read_subtitle_sample(state["subtitle_path"], count=None)
    
    logger.log_input({
        "domain": state.get("domain", ""),
        "subtitle_count": len(subtitles)
    })
    
    try:
        llm = create_llm_client(purpose="refinement")
        storage = LocalStorage(state.get("output_dir", "output") + "/local_storage")
        failure_reasons = Counter()
        step_observability = Counter()
        success_batches = 0
        failed_batches = 0
        total_latency_ms = 0.0
        schema_strict_mode = _read_bool_env("TRANSCRIPT_SCHEMA_STRICT_MODE", False)
        
        # 分批处理（减小批次从100到50，防止输出过长导致截断）
        batch_size = max(1, _read_int_env("TRANSCRIPT_STEP2_BATCH_SIZE", 20))
        async def process_batch(idx, batch):
            """
            执行逻辑：
            1) 组织处理流程与依赖调用。
            2) 汇总中间结果并输出。
            实现方式：通过JSON 解析/序列化实现。
            核心价值：编排流程，保证步骤顺序与可追踪性。
            决策逻辑：
            - 条件：original
            - 条件：s['subtitle_id'] not in processed_ids
            依据来源（证据链）：
            - 配置字段：subtitle_id。
            输入参数：
            - idx: 函数入参（类型：未标注）。
            - batch: 函数入参（类型：未标注）。
            输出参数：
            - 函数计算/封装后的结果对象。"""
            subtitles_text = "\n".join([
                f"[{s['subtitle_id']}] {s['text']}" 
                for s in batch
            ])
            
            prompt = CORRECTION_PROMPT.format(
                domain=state.get("domain", "通用"),
                subtitles=subtitles_text,
                context_default=STEP2_CONTEXT_WINDOW_DEFAULT,
                context_max=STEP2_CONTEXT_WINDOW_MAX,
            )
            prompt = f"{prompt}\n\n{STEP2_MERGED_CLEANUP_APPEND_PROMPT}"
            
            llm_started_at = time.perf_counter()
            try:
                result, response = await llm.complete_json(
                    prompt,
                    system_prompt=CORRECTION_SYSTEM_PROMPT,
                )
                llm_latency_ms = (time.perf_counter() - llm_started_at) * 1000
                
                parsed_payload, payload_metrics = _parse_step2_llm_payload_impl(result)
                batch_observability = Counter(payload_metrics)
                valid_subtitle_ids = {
                    str(item.get("subtitle_id", "")).strip()
                    for item in batch
                    if isinstance(item, dict)
                }
                # 避免与 Step2 顶层 c 冲突：Step4 仅解析 removals 相关键。
                cleanup_payload = {
                    key: result.get(key)
                    for key in ("d", "r", "removals", "cleaned_sentences")
                    if isinstance(result, dict) and key in result
                }
                cleanup_by_id, cleanup_metrics = _parse_step4_cleaned_sentences_impl(
                    cleanup_payload,
                    valid_sentence_ids=valid_subtitle_ids,
                )
                for metric_name, metric_value in cleanup_metrics.items():
                    batch_observability[f"cleanup_{metric_name}"] += int(metric_value)
                if schema_strict_mode and payload_metrics.get("legacy_corrected_subtitles_shape_hits", 0) > 0:
                    raise ValueError("Step2 strict schema mode rejects legacy corrected_subtitles payload")
                minimal_correction_count = sum(
                    len(item.get("corrections", []))
                    for item in parsed_payload.values()
                )
                minimal_cleanup_count = sum(
                    len(item.get("removals", []))
                    for item in cleanup_by_id.values()
                    if isinstance(item, dict)
                )
                legacy_items = result.get("corrected_subtitles", [])
                legacy_item_count = len(legacy_items) if isinstance(legacy_items, list) else 0
                logger.log_llm_call(
                    prompt=f"Batch {idx + 1}",
                    response=(
                        f"{minimal_correction_count} minimal-corrections, "
                        f"{minimal_cleanup_count} minimal-cleanups, "
                        f"{legacy_item_count} legacy-items"
                    ),
                    prompt_tokens=response.prompt_tokens,
                    completion_tokens=response.completion_tokens,
                    model=response.model,
                    latency_ms=response.latency_ms
                )
                
                batch_corrected = []
                batch_corrections = []
                batch_cleanup_removals = []
                for s in batch:
                    subtitle_id = str(s["subtitle_id"])
                    parsed = parsed_payload.get(subtitle_id, {})
                    parsed_cleanup = cleanup_by_id.get(subtitle_id, {})
                    reconciled_text, reconciled_corrections, reconcile_metrics = _reconcile_step2_item_with_metrics(
                        original_text=s["text"],
                        llm_corrected_text=str(parsed.get("corrected_text", "")),
                        llm_corrections=parsed.get("corrections", []),
                        subtitle_id=subtitle_id,
                    )
                    batch_observability.update(reconcile_metrics)
                    cleaned_text, applied_removals, cleanup_reconcile_metrics = _reconcile_step4_item_impl(
                        original_text=reconciled_text,
                        llm_cleaned_text=str(parsed_cleanup.get("cleaned_text", "")),
                        llm_removals=parsed_cleanup.get("removals", []),
                        sentence_id=subtitle_id,
                    )
                    for metric_name, metric_value in cleanup_reconcile_metrics.items():
                        batch_observability[f"cleanup_{metric_name}"] += int(metric_value)
                    if cleaned_text and _drops_cjk_en_glossary_pair(reconciled_text, cleaned_text):
                        batch_observability["cleanup_bilingual_pair_guard_fallback_used"] += 1
                        cleaned_text = reconciled_text
                    if not cleaned_text:
                        batch_observability["cleanup_empty_text_fallback_used"] += 1
                        cleaned_text = reconciled_text
                    batch_corrected.append({
                        "subtitle_id": subtitle_id,
                        "corrected_text": cleaned_text,
                        "start_sec": s["start_sec"],
                        "end_sec": s["end_sec"]
                    })
                    batch_corrections.extend(reconciled_corrections)
                    batch_cleanup_removals.extend(applied_removals)
                  
                return (
                    batch_corrected,
                    batch_corrections,
                    batch_cleanup_removals,
                    response.total_tokens,
                    llm_latency_ms,
                    None,
                    dict(batch_observability),
                )
            except Exception as e:
                # 失败回退：保留原文
                fallback = []
                for s in batch:
                    fallback.append({
                        "subtitle_id": s["subtitle_id"],
                        "corrected_text": s["text"],
                        "start_sec": s["start_sec"],
                        "end_sec": s["end_sec"]
                    })
                return (
                    fallback,
                    [],
                    [],
                    0,
                    (time.perf_counter() - llm_started_at) * 1000,
                    e,
                    {"batch_fallback_used": 1},
                )

        # 构建任务列表
        logger.info(f"并发执行 {len(subtitles)} 条字幕的纠错处理...")
        logger.log_substep("llm_batch", "正在进行LLM分批调用")
        batches = [subtitles[i:i + batch_size] for i in range(0, len(subtitles), batch_size)]
        max_inflight = _resolve_step_max_inflight("STEP2")
        results = await _run_bounded_producer_consumer(
            batches,
            max_inflight=max_inflight,
            handler=process_batch,
        )
        
        # 收集结果
        all_corrected = []
        all_corrections = []
        all_cleanup_removals = []
        total_tokens = 0
        total_latency_ms = 0.0
        success_batches = 0
        failed_batches = 0
        for batch_corrected, batch_corrections, batch_cleanup_removals, tokens, latency_ms, error, batch_metrics in results:
            all_corrected.extend(batch_corrected)
            all_corrections.extend(batch_corrections)
            all_cleanup_removals.extend(batch_cleanup_removals)
            total_tokens += tokens
            total_latency_ms += latency_ms
            step_observability.update(batch_metrics or {})
            if error is None:
                success_batches += 1
            else:
                failed_batches += 1
                failure_reasons[_summarize_error(error)] += 1

        logger.log_batch_summary(
            total_batches=len(batches),
            success_batches=success_batches,
            failed_batches=failed_batches,
            total_tokens=total_tokens,
            total_latency_ms=total_latency_ms,
            failure_reasons=dict(failure_reasons),
            unit_label="batch",
        )
        
        # 按原始字幕顺序重排，避免对 subtitle_id 命名格式的强依赖。
        subtitle_ids_in_order = [str(item.get("subtitle_id", "")) for item in subtitles]
        all_corrected = _order_records_by_reference_ids(
            all_corrected,
            subtitle_ids_in_order,
            id_key="subtitle_id",
        )
        
        # 存储纠错后的字幕时间戳到本地（更精确的时间定位，且与后续步骤文本一致）
        subtitle_timestamps = {
            s["subtitle_id"]: {
                "start_sec": s["start_sec"],
                "end_sec": s["end_sec"],
                "text": s["corrected_text"][:50]  # 存储纠错后文本用于匹配
            }
            for s in all_corrected
        }
        storage.save_subtitle_timestamps(subtitle_timestamps)
        logger.info(f"Saved {len(subtitle_timestamps)} subtitle timestamps to local storage")
        
        output = {
            "corrected_subtitles": all_corrected,
            "correction_summary": all_corrections,
            "cleanup_summary": all_cleanup_removals,
            "current_step": "step2_correction",
            "current_step_status": "completed",
            "token_usage": {"step2_correction": total_tokens},
            "llm_calls": [{
                "step_name": "step2_correction",
                "model": "deepseek-chat",
                "total_tokens": total_tokens
            }],
            "step_observability": {"step2_correction": dict(step_observability)},
            STEP2_STEP4_MERGED_STATE_FLAG: failed_batches == 0,
        }
        
        logger.log_output({"corrected_count": len(all_corrected), "corrections_made": len(all_corrections)})
        timing = logger.end(success=True)
        output["step_timings"] = {"step2_correction": timing["duration_ms"]}
        
        return output
        
    except Exception as e:
        logger.log_error(e)
        logger.log_degrade("LLM failed, using original subtitles as fallback")
        
        # Fallback: 使用原始字幕作为 corrected_subtitles
        from ..tools.file_validator import read_subtitle_sample
        subtitles = read_subtitle_sample(state["subtitle_path"], count=None)
        
        fallback_corrected = [
            {
                "subtitle_id": s["subtitle_id"],
                "corrected_text": s["text"],  # 原始文本
                "start_sec": s["start_sec"],
                "end_sec": s["end_sec"]
            }
            for s in subtitles
        ]
        
        logger.end(success=False)
        return {
            "corrected_subtitles": fallback_corrected,
            "errors": [{"step": "step2", "error": str(e), "fallback_used": True}],
            "current_step_status": "fallback",
            "step_observability": {"step2_correction": {"global_fallback_used": 1}},
            STEP2_STEP4_MERGED_STATE_FLAG: False,
        }


# ============================================================================
# Step 3: 自然语义合并 Prompt
# ============================================================================

MERGE_PROMPT = """请将以下 ASR 细切字幕合并成语义完整、表达通顺的句子。
【字幕列表】
{subtitles}

【合并规则】
1. 只输出“确实需要跨 subtitle 合并”的句子组。
2. 单条 subtitle 如果本身已经完整，不要输出；这些将由本地逻辑自动直通。
3. 每个合并组必须包含至少 2 个 subtitle_id，且保持原始字幕顺序。
4. 不得臆造内容，只能基于给定字幕做重组表达。

【字段约束】
- 使用短键：mg/t/sids。
- 仅返回合并语义所需字段，不返回时间字段。

【输出格式】只输出 JSON：
{{
  "mg": [
    {{
      "t": "合并后的完整句子",
      "sids": ["SUB001", "SUB002"]
    }}
  ]
}}"""


MERGE_SYSTEM_PROMPT = (
    "你是语义合并助手。"
    "按语义连续性合并字幕句子，保留事实信息，不得臆造内容。"
    "输出必须是可解析 JSON。"
)
async def step3_node(state: PipelineState) -> Dict[str, Any]:
    """
    执行逻辑：
    1) LLM 仅返回“需要合并”的 subtitle 组；
    2) 本地提取已合并 subtitle_id 集合，未覆盖字幕自动直通；
    3) 按原始时间顺序拼装最终 merged_sentences。
    """
    logger = get_logger("step3_merge", state.get("output_dir", "output/logs"))
    logger.start()

    corrected = state.get("corrected_subtitles", [])
    logger.log_input({"subtitle_count": len(corrected)})

    try:
        llm = create_llm_client(purpose="refinement")
        failure_reasons = Counter()
        step_observability = Counter()
        schema_strict_mode = _read_bool_env("TRANSCRIPT_SCHEMA_STRICT_MODE", False)

        subtitle_by_id: Dict[str, Dict[str, Any]] = {}
        ordered_subtitle_ids: List[str] = []
        for item in corrected:
            if not isinstance(item, dict):
                step_observability["dropped_invalid_subtitle_item"] += 1
                continue
            subtitle_id = str(item.get("subtitle_id", "")).strip()
            if not subtitle_id:
                step_observability["dropped_empty_subtitle_id"] += 1
                continue
            if subtitle_id in subtitle_by_id:
                # 字幕 ID 重复会引入覆盖歧义，仅保留首次出现。
                step_observability["dropped_duplicate_subtitle_id"] += 1
                continue
            subtitle_by_id[subtitle_id] = item
            ordered_subtitle_ids.append(subtitle_id)

        subtitle_index_by_id = {subtitle_id: idx for idx, subtitle_id in enumerate(ordered_subtitle_ids)}
        corrected_ordered = [subtitle_by_id[subtitle_id] for subtitle_id in ordered_subtitle_ids]

        window_size = 10

        async def process_window(idx, i):
            batch = corrected_ordered[i:i + window_size]
            subtitles_text = "\n".join(
                [
                    f"[{str(s.get('subtitle_id', '')).strip()}] {str(s.get('corrected_text', s.get('text', '')))}"
                    for s in batch
                ]
            )

            prompt = MERGE_PROMPT.format(subtitles=subtitles_text)
            llm_started_at = time.perf_counter()
            try:
                result, response = await llm.complete_json(
                    prompt,
                    system_prompt=MERGE_SYSTEM_PROMPT,
                )
                valid_subtitle_ids = {
                    str(item.get("subtitle_id", "")).strip()
                    for item in batch
                    if isinstance(item, dict)
                }
                parsed_items, parse_metrics = _parse_step3_merged_sentences_impl(
                    result,
                    valid_subtitle_ids=valid_subtitle_ids,
                )
                if schema_strict_mode and parse_metrics.get("compact_shape_hits", 0) > 0:
                    raise ValueError("Step3 strict schema mode rejects compact payload shape")

                logger.log_llm_call(
                    prompt=f"Window {idx + 1}",
                    response=f"{len(parsed_items)} merged groups",
                    prompt_tokens=response.prompt_tokens,
                    completion_tokens=response.completion_tokens,
                    model=response.model,
                    latency_ms=response.latency_ms,
                )

                window_candidates, candidate_metrics = _build_step3_window_candidates_impl(
                    parsed_items,
                    subtitle_index_by_id=subtitle_index_by_id,
                    ordered_subtitle_ids=ordered_subtitle_ids,
                    subtitle_by_id=subtitle_by_id,
                )
                for candidate in window_candidates:
                    candidate["window_index"] = idx
                parse_metrics.update(candidate_metrics)

                return (
                    window_candidates,
                    response.total_tokens,
                    (time.perf_counter() - llm_started_at) * 1000,
                    None,
                    parse_metrics,
                )
            except Exception as e:
                return [], 0, (time.perf_counter() - llm_started_at) * 1000, e, {"window_fallback_used": 1}

        logger.info(f"并发执行 {len(corrected_ordered)} 条字幕的语义合并...")
        logger.log_substep("llm_window", "正在进行 LLM 窗口调用")
        window_overlap = _read_int_env("TRANSCRIPT_STEP3_WINDOW_OVERLAP", 0)
        window_overlap = max(0, min(window_size - 1, int(window_overlap)))
        window_stride = max(1, window_size - window_overlap)
        window_starts = list(range(0, len(corrected_ordered), window_stride))
        max_inflight = _resolve_step_max_inflight("STEP3", default=48)
        results = await _run_bounded_producer_consumer(
            window_starts,
            max_inflight=max_inflight,
            handler=process_window,
        )

        all_candidates: List[Dict[str, Any]] = []
        total_tokens = 0
        total_latency_ms = 0.0
        success_windows = 0
        failed_windows = 0

        for window_candidates, tokens, latency_ms, error, parse_metrics in results:
            total_tokens += tokens
            total_latency_ms += latency_ms
            step_observability.update(parse_metrics or {})
            if error is None:
                success_windows += 1
            else:
                failed_windows += 1
                failure_reasons[_summarize_error(error)] += 1
            all_candidates.extend(window_candidates)

        all_merged, merged_subtitle_ids, assemble_metrics = _assemble_step3_merged_sentences_impl(
            all_candidates,
            ordered_subtitle_ids=ordered_subtitle_ids,
            subtitle_by_id=subtitle_by_id,
        )
        step_observability.update(assemble_metrics)

        for sentence_counter, sent in enumerate(all_merged, start=1):
            sent["sentence_id"] = f"S{sentence_counter:03d}"
        step_observability["merged_subtitle_count"] = len(merged_subtitle_ids)

        logger.log_batch_summary(
            total_batches=len(window_starts),
            success_batches=success_windows,
            failed_batches=failed_windows,
            total_tokens=total_tokens,
            total_latency_ms=total_latency_ms,
            failure_reasons=dict(failure_reasons),
            unit_label="window",
        )

        output = {
            "merged_sentences": all_merged,
            "current_step": "step3_merge",
            "current_step_status": "completed",
            "token_usage": {"step3_merge": total_tokens},
            "step_observability": {"step3_merge": dict(step_observability)},
        }

        logger.log_output({"merged_count": len(all_merged)})
        timing = logger.end(success=True)
        output["step_timings"] = {"step3_merge": timing["duration_ms"]}

        return output

    except Exception as e:
        logger.log_error(e)
        logger.end(success=False)
        return {
            "merged_sentences": [],
            "errors": [{"step": "step3", "error": str(e)}],
            "step_observability": {"step3_merge": {"global_fallback_used": 1}},
        }

# Step 3.5: 英文句子翻译与中文重写 Prompt
# ============================================================================

TRANSLATION_PROMPT = """请将以下英文句子翻译成中文，并进行中文母语化重写。

核心要求：请将这段译文按中文母语者的表达习惯进行重写，使其流畅、自然，同时保留原意和专有术语。
专有名词规则：
1) 对特定名词必须保留英文原词，建议统一使用“中文译名（英文原词）”格式。
2) 示例：deepseek 应翻译为“深度求索（deepseek）”。
3) 若无稳定中文译名，至少保留英文原词，不得丢失。

【句子列表】
{sentences}

【字段约束】
- 只返回句子标识与译文，不要返回 start_sec/end_sec/source_subtitle_ids

【输出格式】只输出 JSON，不要输出其他解释文字：
{{
  "t": [
    {{
      "sid": "S001",
      "tt": "翻译并重写后的中文句子"
    }}
  ]
}}"""



TRANSLATION_SYSTEM_PROMPT = (
    "你是中英字幕翻译与母语化改写助手。"
    "在忠实原意的前提下，输出自然流畅、口语化的中文表达。"
    "对特定名词保留英文原词，优先使用“中文译名（英文原词）”格式，例如“深度求索（deepseek）”。"
    "输出必须是可解析 JSON。"
)
async def step3_5_node(state: PipelineState) -> Dict[str, Any]:
    """
    执行逻辑：
    1) 基于 step3 产出的完整句子执行翻译与母语化重写。
    2) 按窗口分批调用 LLM（窗口大小 50），保留句子 ID 和时间戳映射。
    3) 对缺失项或失败批次回退为原句，保证后续步骤可继续执行。
    """
    logger = get_logger("step3_5_translate", state.get("output_dir", "output/logs"))
    logger.start()

    merged = state.get("merged_sentences", [])
    passthrough_by_id: Dict[str, Dict[str, Any]] = {}
    translate_candidates: List[Dict[str, Any]] = []
    for item in merged:
        sentence_id = str(item.get("sentence_id", ""))
        normalized_item = {
            "sentence_id": sentence_id,
            "text": item.get("text", ""),
            "start_sec": item.get("start_sec", 0),
            "end_sec": item.get("end_sec", 0),
            "source_subtitle_ids": item.get("source_subtitle_ids", []),
        }
        if _contains_cjk(normalized_item["text"]):
            passthrough_by_id[sentence_id] = normalized_item
        else:
            translate_candidates.append(normalized_item)

    logger.log_input(
        {
            "sentence_count": len(merged),
            "translate_candidate_count": len(translate_candidates),
            "passthrough_chinese_count": len(passthrough_by_id),
        }
    )

    try:
        step_observability = Counter()
        schema_strict_mode = _read_bool_env("TRANSCRIPT_SCHEMA_STRICT_MODE", False)
        step_observability["translate_candidate_count"] += len(translate_candidates)
        step_observability["passthrough_chinese_count"] += len(passthrough_by_id)
        if not translate_candidates:
            output = {
                "translated_sentences": [
                    passthrough_by_id.get(
                        str(source.get("sentence_id", "")),
                        {
                            "sentence_id": str(source.get("sentence_id", "")),
                            "text": source.get("text", ""),
                            "start_sec": source.get("start_sec", 0),
                            "end_sec": source.get("end_sec", 0),
                            "source_subtitle_ids": source.get("source_subtitle_ids", []),
                        },
                    )
                    for source in merged
                ],
                "current_step": "step3_5_translate",
                "current_step_status": "completed",
                "token_usage": {"step3_5_translate": 0},
                "step_observability": {"step3_5_translate": dict(step_observability)},
            }
            logger.log_output(
                {
                    "translated_count": 0,
                    "passthrough_chinese_count": len(passthrough_by_id),
                }
            )
            timing = logger.end(success=True)
            output["step_timings"] = {"step3_5_translate": timing["duration_ms"]}
            return output

        llm = create_llm_client(purpose="refinement")
        failure_reasons = Counter()

        window_size = max(1, _read_int_env("TRANSCRIPT_STEP35_WINDOW_SIZE", 50))

        async def process_window(idx: int, i: int):
            batch = translate_candidates[i:i + window_size]
            source_by_id = {str(item.get("sentence_id", "")): item for item in batch}
            sentences_text = "\n".join(
                f"[{s['sentence_id']}] {s['text']}"
                for s in batch
            )
            if sentences_text:
                # 确保第一条句子从新行开始，避免模板前缀吞并首条 ID。
                sentences_text = "\n" + sentences_text
            prompt = TRANSLATION_PROMPT.format(sentences=sentences_text)
            llm_started_at = time.perf_counter()
            try:
                result, response = await llm.complete_json(
                    prompt,
                    system_prompt=TRANSLATION_SYSTEM_PROMPT,
                )
                translated_by_id, parse_metrics = _parse_step35_translated_sentences_impl(
                    result,
                    valid_sentence_ids=set(source_by_id.keys()),
                )
                if schema_strict_mode and parse_metrics.get("compact_shape_hits", 0) > 0:
                    raise ValueError("Step3.5 strict schema mode rejects compact payload shape")
                logger.log_llm_call(
                    prompt=f"Window {idx + 1}",
                    response=f"{len(translated_by_id)} translated",
                    prompt_tokens=response.prompt_tokens,
                    completion_tokens=response.completion_tokens,
                    model=response.model,
                    latency_ms=response.latency_ms,
                )

                translated_batch = []
                for source in batch:
                    sentence_id = str(source.get("sentence_id", ""))
                    translated_batch.append(
                        {
                            "sentence_id": sentence_id,
                            "text": translated_by_id.get(sentence_id, source.get("text", "")),
                            "start_sec": source.get("start_sec", 0),
                            "end_sec": source.get("end_sec", 0),
                            "source_subtitle_ids": source.get("source_subtitle_ids", []),
                        }
                    )

                return (
                    translated_batch,
                    response.total_tokens,
                    (time.perf_counter() - llm_started_at) * 1000,
                    None,
                    parse_metrics,
                )
            except Exception as e:
                fallback_batch = []
                for source in batch:
                    fallback_batch.append(
                        {
                            "sentence_id": str(source.get("sentence_id", "")),
                            "text": source.get("text", ""),
                            "start_sec": source.get("start_sec", 0),
                            "end_sec": source.get("end_sec", 0),
                            "source_subtitle_ids": source.get("source_subtitle_ids", []),
                        }
                    )
                return (
                    fallback_batch,
                    0,
                    (time.perf_counter() - llm_started_at) * 1000,
                    e,
                    {"window_fallback_used": 1},
                )

        logger.info(f"并发执行 {len(translate_candidates)} 条非中文完整句子的翻译重写...")
        logger.log_substep("llm_window", "正在进行 Step3.5 分窗翻译调用")
        window_starts = list(range(0, len(translate_candidates), window_size))
        max_inflight = _resolve_step_max_inflight("STEP35")
        results = await _run_bounded_producer_consumer(
            window_starts,
            max_inflight=max_inflight,
            handler=process_window,
        )

        translated_sentences = []
        total_tokens = 0
        total_latency_ms = 0.0
        success_windows = 0
        failed_windows = 0

        for window_results, tokens, latency_ms, error, parse_metrics in results:
            translated_sentences.extend(window_results)
            total_tokens += tokens
            total_latency_ms += latency_ms
            step_observability.update(parse_metrics or {})
            if error is None:
                success_windows += 1
            else:
                failed_windows += 1
                failure_reasons[_summarize_error(error)] += 1

        translated_by_id = {str(item.get("sentence_id", "")): item for item in translated_sentences}
        final_sentences: List[Dict[str, Any]] = []
        for source in merged:
            sentence_id = str(source.get("sentence_id", ""))
            if sentence_id in passthrough_by_id:
                final_sentences.append(passthrough_by_id[sentence_id])
                continue
            if sentence_id in translated_by_id:
                final_sentences.append(translated_by_id[sentence_id])
                continue
            final_sentences.append(
                {
                    "sentence_id": sentence_id,
                    "text": source.get("text", ""),
                    "start_sec": source.get("start_sec", 0),
                    "end_sec": source.get("end_sec", 0),
                    "source_subtitle_ids": source.get("source_subtitle_ids", []),
                }
            )

        logger.log_batch_summary(
            total_batches=len(window_starts),
            success_batches=success_windows,
            failed_batches=failed_windows,
            total_tokens=total_tokens,
            total_latency_ms=total_latency_ms,
            failure_reasons=dict(failure_reasons),
            unit_label="window",
        )

        output = {
            "translated_sentences": final_sentences,
            "current_step": "step3_5_translate",
            "current_step_status": "completed",
            "token_usage": {"step3_5_translate": total_tokens},
            "step_observability": {"step3_5_translate": dict(step_observability)},
        }

        logger.log_output(
            {
                "translated_count": len(translate_candidates),
                "passthrough_chinese_count": len(passthrough_by_id),
                "final_count": len(final_sentences),
            }
        )
        timing = logger.end(success=True)
        output["step_timings"] = {"step3_5_translate": timing["duration_ms"]}

        return output
    except Exception as e:
        logger.log_error(e)
        logger.end(success=False)
        fallback_sentences = []
        for source in merged:
            fallback_sentences.append(
                {
                    "sentence_id": str(source.get("sentence_id", "")),
                    "text": source.get("text", ""),
                    "start_sec": source.get("start_sec", 0),
                    "end_sec": source.get("end_sec", 0),
                    "source_subtitle_ids": source.get("source_subtitle_ids", []),
                }
            )
        return {
            "translated_sentences": fallback_sentences,
            "errors": [{"step": "step3_5", "error": str(e), "fallback_used": True}],
            "current_step_status": "fallback",
            "step_observability": {"step3_5_translate": {"global_fallback_used": 1}},
        }


# ============================================================================
# Step 4: 局部冗余删除 Prompt
# ============================================================================

CLEAN_LOCAL_PROMPT = """请清理以下句子中的无价值冗余内容。

【句子列表】
{sentences}

【清理类型（全部直接删除）】
1. 结巴类：单句内连续重复词汇/音节，如"我我我想说"→"我想说"
2. 单句内无意义重复：如"这个这个方案可行"→"这个方案可行"
3. 中文口语填充词：如"那个"、"就是说"、"然后"、"嗯"、"啊"、"呃"、"其实"、"基本上"、"的话"（句末无效语气）、"这一块"（无义指代）、"反正"、"就是"（无实义连接）
4. 英文口语填充词：如"you know"、"like"（非比喻义）、"basically"、"I mean"、"sort of"、"kind of"、"um"、"uh"、"well"（句首无义）、"actually"（非转折义）、"right"（句末口语确认）、"so"（句首无义连接）、"literally"（非字面义强调）
5. 引出式废话：如"怎么说呢"、"我是说"、"你知道"、"也就是说"、"在这个层面上"、"说白了"、"换句话说"、"how should I put it"、"what I'm saying is"、"let me put it this way"
6. 强调性反问/修辞词（不增加信息量的口头确认）：如"对吧"、"是不是"、"对不对"、"不是吗"、"你说对吧"、"你说是不是"、"对吧对吧"、"right?"、"you see"、"isn't it"、"you know what I mean"
7. 同音/近音词误判：如"产品的的质量"→"产品的质量"
8. 背景噪音误判：如句末的"呃嘶"等无意义音
9. 单句语义赘述：如"我个人认为我觉得"→"我认为"

【注意】
- 重点攻击无信息增量的“口水词”（Conversational Fillers）
- 保留有意义的重复（如强调性重复）
- 保留中英文对照术语（如"智能体（agent）"），不得删除括号内外任一部分
- 仅处理单句内的冗余
- 仅返回需要删除的最小片段，不要回传整句 cleaned_text
- 每条删除必须包含 sentence_id、original、left_context、right_context
- left/right 要尽可能短：能唯一定位就不扩展；无法唯一定位时再逐步扩展
- 若扩展后仍无法唯一定位，该删除不要输出（宁缺毋滥）
- 可只返回发生删除的句子；未返回句子会由本地自动直通原文
- 不要输出 removed_items 字段

【输出格式】
{{
  "d": [
    {{
      "sid": "S001",
      "o": "要删除的片段",
      "l": "original 左侧上下文",
      "r": "original 右侧上下文"
    }}
  ]
}}"""



CLEAN_LOCAL_SYSTEM_PROMPT = (
    "你是文本精炼助手。"
    "只删除单句内部无信息增量的冗余，不改变原句核心语义。"
    "积极删除中英文口语填充词、引出式废话、强调性反问/修辞词。"
    "输出必须是可解析 JSON。"
)
async def step4_node(state: PipelineState) -> Dict[str, Any]:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过JSON 解析/序列化、asyncio 异步调度实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    输入参数：
    - state: 函数入参（类型：PipelineState）。
    输出参数：
    - 结构化结果字典（包含关键字段信息）。"""
    logger = get_logger("step4_clean_local", state.get("output_dir", "output/logs"))
    logger.start()
    
    merged = state.get("translated_sentences") or state.get("merged_sentences", [])
    logger.log_input({"sentence_count": len(merged)})
    
    try:
        if bool(state.get(STEP2_STEP4_MERGED_STATE_FLAG, False)):
            step_observability = Counter()
            step_observability["compat_passthrough_mode_used"] += 1
            step_observability["passthrough_sentence_count"] += len(merged)

            ordered_sources = sorted(
                enumerate(merged),
                key=lambda pair: (
                    float(pair[1].get("start_sec", 0.0)) if isinstance(pair[1], dict) else 0.0,
                    float(pair[1].get("end_sec", 0.0)) if isinstance(pair[1], dict) else 0.0,
                    pair[0],
                ),
            )
            all_cleaned: List[Dict[str, Any]] = []
            for _, source in ordered_sources:
                if not isinstance(source, dict):
                    step_observability["dropped_invalid_source_item_type"] += 1
                    continue
                sentence_id = str(source.get("sentence_id", "")).strip()
                if not sentence_id:
                    step_observability["dropped_missing_sentence_id_in_source"] += 1
                    continue
                source_text = str(source.get("text", source.get("cleaned_text", "")))
                all_cleaned.append(
                    {
                        "sentence_id": sentence_id,
                        "cleaned_text": source_text,
                    }
                )

            storage = LocalStorage(state.get("output_dir", "output") + "/local_storage")
            timestamps = {
                str(s.get("sentence_id", "")): {
                    "start_sec": s.get("start_sec", 0),
                    "end_sec": s.get("end_sec", 0),
                }
                for s in merged
                if isinstance(s, dict) and str(s.get("sentence_id", "")).strip()
            }
            storage.save_sentence_timestamps(timestamps)
            logger.info(f"Saved {len(timestamps)} sentence timestamps to local storage")

            try:
                # 兼容下游读取路径：继续写入 intermediates/sentence_timestamps.json。
                intermediates_dir = Path(state.get("output_dir", "output")) / "intermediates"
                intermediates_dir.mkdir(parents=True, exist_ok=True)
                sentence_timestamps_path = intermediates_dir / "sentence_timestamps.json"
                with open(sentence_timestamps_path, "w", encoding="utf-8") as output_stream:
                    json.dump(timestamps, output_stream, ensure_ascii=False, indent=2)
                logger.info(f"Saved {len(timestamps)} sentence timestamps to intermediates")
            except Exception as error:
                logger.warning(f"Save sentence_timestamps to intermediates failed: {error}")

            output = {
                "cleaned_sentences": all_cleaned,
                "current_step": "step4_clean_local",
                "current_step_status": "completed",
                "token_usage": {"step4_clean_local": 0},
                "step_observability": {"step4_clean_local": dict(step_observability)},
            }
            logger.log_output({"cleaned_count": len(all_cleaned), "compat_passthrough_mode": True})
            timing = logger.end(success=True)
            output["step_timings"] = {"step4_clean_local": timing["duration_ms"]}
            return output

        llm = create_llm_client(purpose="refinement")
        storage = LocalStorage(state.get("output_dir", "output") + "/local_storage")
        failure_reasons = Counter()
        step_observability = Counter()
        schema_strict_mode = _read_bool_env("TRANSCRIPT_SCHEMA_STRICT_MODE", False)
        
        async def process_batch(idx, batch):
            """
            执行逻辑：
            1) 组织处理流程与依赖调用。
            2) 汇总中间结果并输出。
            实现方式：通过JSON 解析/序列化实现。
            核心价值：编排流程，保证步骤顺序与可追踪性。
            输入参数：
            - idx: 函数入参（类型：未标注）。
            - batch: 函数入参（类型：未标注）。
            输出参数：
            - 函数计算/封装后的结果对象。"""
            sentences_text = "\n".join([
                f"[{s['sentence_id']}] {s['text']}" 
                for s in batch
            ])
            
            prompt = CLEAN_LOCAL_PROMPT.format(sentences=sentences_text)
            llm_started_at = time.perf_counter()
            try:
                result, response = await llm.complete_json(
                    prompt,
                    system_prompt=CLEAN_LOCAL_SYSTEM_PROMPT,
                )
                valid_sentence_ids = {
                    str(item.get("sentence_id", "")).strip()
                    for item in batch
                    if isinstance(item, dict)
                }
                cleaned_by_id, parse_metrics = _parse_step4_cleaned_sentences_impl(
                    result,
                    valid_sentence_ids=valid_sentence_ids,
                )
                if schema_strict_mode and parse_metrics.get("compact_shape_hits", 0) > 0:
                    raise ValueError("Step4 strict schema mode rejects compact payload shape")
                removal_count = sum(
                    len(item.get("removals", []))
                    for item in cleaned_by_id.values()
                    if isinstance(item, dict)
                )
                legacy_cleaned_count = sum(
                    1
                    for item in cleaned_by_id.values()
                    if isinstance(item, dict) and str(item.get("cleaned_text", "")).strip()
                )

                logger.log_llm_call(
                    prompt=f"Batch {idx + 1}",
                    response=(
                        f"{removal_count} removals, "
                        f"{legacy_cleaned_count} legacy-cleaned"
                    ),
                    prompt_tokens=response.prompt_tokens,
                    completion_tokens=response.completion_tokens,
                    model=response.model,
                    latency_ms=response.latency_ms
                )
                 
                return (
                    cleaned_by_id,
                    response.total_tokens,
                    (time.perf_counter() - llm_started_at) * 1000,
                    None,
                    parse_metrics,
                )
            except Exception as e:
                return (
                    {},
                    0,
                    (time.perf_counter() - llm_started_at) * 1000,
                    e,
                    {"batch_fallback_used": 1},
                )

        logger.info(f"并发执行 {len(merged)} 条字幕的局部冗余处理...")
        logger.log_substep("llm_batch", "正在进行LLM分批调用")
        batch_size = max(1, _read_int_env("TRANSCRIPT_STEP4_BATCH_SIZE", 20))
        batches = [merged[i:i + batch_size] for i in range(0, len(merged), batch_size)]
        max_inflight = _resolve_step_max_inflight("STEP4")
        results = await _run_bounded_producer_consumer(
            batches,
            max_inflight=max_inflight,
            handler=process_batch,
        )
        
        # 收集结果
        all_cleaned = []
        batch_cleaned_maps: List[Dict[str, Dict[str, Any]]] = []
        total_tokens = 0
        total_latency_ms = 0.0
        success_batches = 0
        failed_batches = 0
        for batch_cleaned_by_id, tokens, latency_ms, error, parse_metrics in results:
            batch_cleaned_maps.append(batch_cleaned_by_id or {})
            total_tokens += tokens
            total_latency_ms += latency_ms
            step_observability.update(parse_metrics or {})
            if error is None:
                success_batches += 1
            else:
                failed_batches += 1
                failure_reasons[_summarize_error(error)] += 1

        llm_cleaned_by_id, merge_metrics = _merge_step4_cleaned_maps_impl(batch_cleaned_maps)
        step_observability.update(merge_metrics)
        all_cleaned, assemble_metrics = _assemble_step4_cleaned_sentences_impl(
            merged,
            llm_cleaned_by_id=llm_cleaned_by_id,
            glossary_guard=_drops_cjk_en_glossary_pair,
        )
        step_observability.update(assemble_metrics)

        logger.log_batch_summary(
            total_batches=len(batches),
            success_batches=success_batches,
            failed_batches=failed_batches,
            total_tokens=total_tokens,
            total_latency_ms=total_latency_ms,
            failure_reasons=dict(failure_reasons),
            unit_label="batch",
        )
        
        # 存储时间戳到本地
        timestamps = {
            s["sentence_id"]: {
                "start_sec": s["start_sec"],
                "end_sec": s["end_sec"]
            }
            for s in merged
        }
        storage.save_sentence_timestamps(timestamps)
        logger.info(f"Saved {len(timestamps)} sentence timestamps to local storage")

        try:
            # 同步输出到 intermediates：统一下游读取路径，避免服务层复制缺失时出现找不到文件。
            intermediates_dir = Path(state.get("output_dir", "output")) / "intermediates"
            intermediates_dir.mkdir(parents=True, exist_ok=True)
            sentence_timestamps_path = intermediates_dir / "sentence_timestamps.json"
            with open(sentence_timestamps_path, "w", encoding="utf-8") as output_stream:
                json.dump(timestamps, output_stream, ensure_ascii=False, indent=2)
            logger.info(f"Saved {len(timestamps)} sentence timestamps to intermediates")
        except Exception as error:
            # 不中断主流程：本地缓存已写入，intermediates 写入失败交由上层回退处理。
            logger.warning(f"Save sentence_timestamps to intermediates failed: {error}")
        
        output = {
            "cleaned_sentences": all_cleaned,
            "current_step": "step4_clean_local",
            "current_step_status": "completed",
            "token_usage": {"step4_clean_local": total_tokens},
            "step_observability": {"step4_clean_local": dict(step_observability)},
        }
        
        logger.log_output({"cleaned_count": len(all_cleaned)})
        timing = logger.end(success=True)
        output["step_timings"] = {"step4_clean_local": timing["duration_ms"]}
        
        return output
        
    except Exception as e:
        logger.log_error(e)
        logger.end(success=False)
        return {
            "cleaned_sentences": [],
            "errors": [{"step": "step4", "error": str(e)}],
            "step_observability": {"step4_clean_local": {"global_fallback_used": 1}},
        }


# ============================================================================
# Step 5+6: 跨句冗余删除 + 合并（单次 LLM）
# ============================================================================

CLEAN_CROSS_PROMPT = """请对下面句子执行两项任务：
1. 删除“无增量信息”的跨句冗余，输出保留句 ID；
2. 将“语义重叠但有增量信息”的句子合并为段落。

【核心主题】{main_topic}

【句子列表】
{sentences}

【删除规则】
1. 仅删除无增量信息的重复；
2. 如句子有新增信息，不要删除；
3. 无法判断时优先保留。

【段落规则】
1. 可将多个句子合并为一个段落；
2. 段落必须给出 source_sentence_ids；
3. 不需要合并的句子可单独成段；
4. 文本必须忠实于原句，不要虚构。

【输出 JSON】
{{
  "keep_sentence_ids": ["S001", "S002"],
  "paragraphs": [
    {{
      "text": "段落文本",
      "source_sentence_ids": ["S001", "S002"],
      "merge_type": "同义补充"
    }}
  ]
}}
"""


STEP56_DEDUP_MERGE_SYSTEM_PROMPT = (
    "你是跨句冗余清理与合并助手。"
    "请严格输出可解析 JSON，不要输出额外解释。"
)

# 兼容旧测试与旧调用方：保留历史常量名。
MERGE_CROSS_PROMPT = CLEAN_CROSS_PROMPT
CLEAN_CROSS_SYSTEM_PROMPT = STEP56_DEDUP_MERGE_SYSTEM_PROMPT
MERGE_CROSS_SYSTEM_PROMPT = STEP56_DEDUP_MERGE_SYSTEM_PROMPT


STEP5_6_NODE_NAME = "step5_6_dedup_merge"


def _resolve_step56_window_size() -> tuple[int, List[str]]:
    """解析 Step56 window_size，新变量优先，旧变量兜底。"""
    legacy_used: List[str] = []
    if os.getenv("TRANSCRIPT_STEP56_WINDOW_SIZE") is not None:
        return max(1, _read_int_env("TRANSCRIPT_STEP56_WINDOW_SIZE", 8)), legacy_used
    if os.getenv("TRANSCRIPT_STEP6_WINDOW_SIZE") is not None:
        legacy_used.append("TRANSCRIPT_STEP6_WINDOW_SIZE")
        return max(1, _read_int_env("TRANSCRIPT_STEP6_WINDOW_SIZE", 8)), legacy_used
    if os.getenv("TRANSCRIPT_STEP5_WINDOW_SIZE") is not None:
        legacy_used.append("TRANSCRIPT_STEP5_WINDOW_SIZE")
        return max(1, _read_int_env("TRANSCRIPT_STEP5_WINDOW_SIZE", 8)), legacy_used
    return 8, legacy_used


def _resolve_step56_window_overlap(window_size: int) -> tuple[int, List[str]]:
    """解析 Step56 overlap，新变量优先，旧变量兜底。"""
    legacy_used: List[str] = []
    if os.getenv("TRANSCRIPT_STEP56_WINDOW_OVERLAP") is not None:
        value = _read_int_env("TRANSCRIPT_STEP56_WINDOW_OVERLAP", 0)
    elif os.getenv("TRANSCRIPT_STEP6_WINDOW_OVERLAP") is not None:
        legacy_used.append("TRANSCRIPT_STEP6_WINDOW_OVERLAP")
        value = _read_int_env("TRANSCRIPT_STEP6_WINDOW_OVERLAP", 0)
    elif os.getenv("TRANSCRIPT_STEP5_WINDOW_OVERLAP") is not None:
        legacy_used.append("TRANSCRIPT_STEP5_WINDOW_OVERLAP")
        value = _read_int_env("TRANSCRIPT_STEP5_WINDOW_OVERLAP", 0)
    else:
        value = 0
    value = max(0, min(window_size - 1, int(value)))
    return value, legacy_used


def _resolve_step56_max_inflight(default: int = 24) -> tuple[int, List[str]]:
    """解析 Step56 并发上限，新变量优先，旧变量兜底。"""
    legacy_used: List[str] = []
    if os.getenv("TRANSCRIPT_STEP56_MAX_INFLIGHT") is not None:
        return _resolve_step_max_inflight("STEP56", default=default), legacy_used
    if os.getenv("TRANSCRIPT_STEP6_MAX_INFLIGHT") is not None:
        legacy_used.append("TRANSCRIPT_STEP6_MAX_INFLIGHT")
        return _resolve_step_max_inflight("STEP6", default=default), legacy_used
    if os.getenv("TRANSCRIPT_STEP5_MAX_INFLIGHT") is not None:
        legacy_used.append("TRANSCRIPT_STEP5_MAX_INFLIGHT")
        return _resolve_step_max_inflight("STEP5", default=default), legacy_used
    return _resolve_step_max_inflight("STEP56", default=default), legacy_used


def _warn_step56_legacy_env(logger, legacy_names: List[str]) -> None:
    """记录旧环境变量兼容读取，便于后续收敛配置。"""
    if not legacy_names:
        return
    unique_names = sorted(set(legacy_names))
    logger.warning(
        "Step56 uses legacy env vars: %s; prefer TRANSCRIPT_STEP56_WINDOW_SIZE/"
        "TRANSCRIPT_STEP56_WINDOW_OVERLAP/TRANSCRIPT_STEP56_MAX_INFLIGHT",
        ", ".join(unique_names),
    )


async def step5_6_node(state: PipelineState) -> Dict[str, Any]:
    """Step5+6 合并节点：单次 LLM 返回 keep_sentence_ids 与 paragraphs。"""
    logger = get_logger(STEP5_6_NODE_NAME, state.get("output_dir", "output/logs"))
    logger.start()

    cleaned_sentences = state.get("cleaned_sentences", [])
    main_topic = str(state.get("main_topic", "")).strip()
    logger.log_input({"sentence_count": len(cleaned_sentences), "main_topic": main_topic})

    try:
        llm = create_llm_client(purpose="analysis")
        failure_reasons = Counter()
        step_observability = Counter()
        legacy_env_used: List[str] = []
        schema_strict_mode = _read_bool_env("TRANSCRIPT_SCHEMA_STRICT_MODE", False)

        window_size, legacy = _resolve_step56_window_size()
        legacy_env_used.extend(legacy)
        window_overlap, legacy = _resolve_step56_window_overlap(window_size)
        legacy_env_used.extend(legacy)
        window_stride = max(1, window_size - window_overlap)
        max_inflight, legacy = _resolve_step56_max_inflight(default=24)
        legacy_env_used.extend(legacy)
        _warn_step56_legacy_env(logger, legacy_env_used)

        window_starts = list(range(0, len(cleaned_sentences), window_stride))
        logger.info(f"并发执行 {len(cleaned_sentences)} 条句子的跨句删除+合并（Step56）...")
        logger.log_substep("llm_window", "正在进行 Step56 LLM 窗口调用")

        async def process_window(idx: int, start_index: int):
            batch = cleaned_sentences[start_index:start_index + window_size]
            id_text_pairs = _sentence_id_and_text_pairs_impl(batch)
            ordered_batch_ids = [sentence_id for sentence_id, _ in id_text_pairs]
            sentence_text_map = {sentence_id: text for sentence_id, text in id_text_pairs}
            sentences_text = "\n".join(
                f"[{sentence_id}] {sentence_text_map.get(sentence_id, '')}"
                for sentence_id in ordered_batch_ids
            )
            prompt = CLEAN_CROSS_PROMPT.format(main_topic=main_topic, sentences=sentences_text)

            llm_started_at = time.perf_counter()
            try:
                result, response = await llm.complete_json(
                    prompt,
                    system_prompt=STEP56_DEDUP_MERGE_SYSTEM_PROMPT,
                )
                keep_ids, paragraphs, parse_metrics = _parse_step56_dedup_merge_payload_impl(
                    result,
                    ordered_batch_ids=ordered_batch_ids,
                    sentence_text_map=sentence_text_map,
                )
                if schema_strict_mode and parse_metrics.get("compact_shape_hits", 0) > 0:
                    raise ValueError("Step56 strict schema mode rejects compact payload shape")
                logger.log_llm_call(
                    prompt=f"Window {idx + 1}",
                    response=f"kept={len(keep_ids)}, paragraphs={len(paragraphs)}",
                    prompt_tokens=response.prompt_tokens,
                    completion_tokens=response.completion_tokens,
                    model=response.model,
                    latency_ms=response.latency_ms,
                )
                return (
                    keep_ids,
                    paragraphs,
                    response.total_tokens,
                    (time.perf_counter() - llm_started_at) * 1000,
                    None,
                    parse_metrics,
                )
            except Exception as error:
                # 出错时采用保守策略：该窗口全保留，避免误删。
                fallback_ids = list(ordered_batch_ids)
                fallback_paragraphs = _build_fallback_paragraphs_impl(fallback_ids, sentence_text_map)
                return (
                    fallback_ids,
                    fallback_paragraphs,
                    0,
                    (time.perf_counter() - llm_started_at) * 1000,
                    error,
                    {"window_fallback_used": 1},
                )

        results = await _run_bounded_producer_consumer(
            window_starts,
            max_inflight=max_inflight,
            handler=process_window,
        )

        all_keep_ids: set[str] = set()
        all_paragraphs: List[Dict[str, Any]] = []
        total_tokens = 0
        total_latency_ms = 0.0
        success_windows = 0
        failed_windows = 0
        for keep_ids, paragraphs, tokens, latency_ms, error, parse_metrics in results:
            all_keep_ids.update(keep_ids)
            all_paragraphs.extend(paragraphs)
            total_tokens += tokens
            total_latency_ms += latency_ms
            step_observability.update(parse_metrics or {})
            if error is None:
                success_windows += 1
            else:
                failed_windows += 1
                failure_reasons[_summarize_error(error)] += 1

        logger.log_batch_summary(
            total_batches=len(window_starts),
            success_batches=success_windows,
            failed_batches=failed_windows,
            total_tokens=total_tokens,
            total_latency_ms=total_latency_ms,
            failure_reasons=dict(failure_reasons),
            unit_label="window",
        )

        ordered_all_pairs = _sentence_id_and_text_pairs_impl(cleaned_sentences)
        ordered_all_ids = [sentence_id for sentence_id, _ in ordered_all_pairs]
        sentence_text_map = {sentence_id: text for sentence_id, text in ordered_all_pairs}

        if all_keep_ids:
            ordered_keep_ids = [sid for sid in ordered_all_ids if sid in all_keep_ids]
        else:
            ordered_keep_ids = list(ordered_all_ids)

        keep_id_set = set(ordered_keep_ids)
        non_redundant_sentences = [
            sentence
            for sentence in cleaned_sentences
            if str(sentence.get("sentence_id", "")).strip() in keep_id_set
        ]
        if not non_redundant_sentences and cleaned_sentences:
            non_redundant_sentences = list(cleaned_sentences)
            ordered_keep_ids = [str(item.get("sentence_id", "")).strip() for item in cleaned_sentences]
            keep_id_set = set(filter(None, ordered_keep_ids))

        normalized_paragraphs: List[Dict[str, Any]] = []
        for paragraph in all_paragraphs:
            if not isinstance(paragraph, dict):
                continue
            source_sentence_ids = paragraph.get("source_sentence_ids", [])
            if not isinstance(source_sentence_ids, list):
                continue
            valid_source_ids = []
            seen: set[str] = set()
            for sentence_id in source_sentence_ids:
                normalized = str(sentence_id or "").strip()
                if not normalized or normalized not in keep_id_set or normalized in seen:
                    continue
                seen.add(normalized)
                valid_source_ids.append(normalized)
            if not valid_source_ids:
                continue
            text = str(paragraph.get("text", "")).strip()
            if not text:
                text = " ".join(
                    str(sentence_text_map.get(sentence_id, "")).strip()
                    for sentence_id in valid_source_ids
                ).strip()
            if not text:
                continue
            merge_type = str(paragraph.get("merge_type", "未合并")).strip() or "未合并"
            normalized_paragraphs.append(
                {
                    "text": text,
                    "source_sentence_ids": valid_source_ids,
                    "merge_type": merge_type,
                }
            )

        if not normalized_paragraphs:
            normalized_paragraphs = _build_fallback_paragraphs_impl(ordered_keep_ids, sentence_text_map)

        deduplicated_paragraphs = _deduplicate_paragraphs_impl(normalized_paragraphs)
        final_paragraphs = []
        for index, paragraph in enumerate(deduplicated_paragraphs):
            final_paragraphs.append(
                {
                    "paragraph_id": f"P{index + 1:03d}",
                    "text": paragraph["text"],
                    "source_sentence_ids": paragraph["source_sentence_ids"],
                    "merge_type": paragraph.get("merge_type", "未合并"),
                }
            )

        output = {
            "non_redundant_sentences": non_redundant_sentences,
            "pure_text_script": final_paragraphs,
            "current_step": STEP5_6_NODE_NAME,
            "current_step_status": "completed",
            "token_usage": {STEP5_6_NODE_NAME: total_tokens},
            "step_observability": {STEP5_6_NODE_NAME: dict(step_observability)},
        }
        logger.log_output(
            {
                "original_count": len(cleaned_sentences),
                "kept_count": len(non_redundant_sentences),
                "paragraph_count": len(final_paragraphs),
            }
        )
        timing = logger.end(success=True)
        output["step_timings"] = {STEP5_6_NODE_NAME: timing["duration_ms"]}
        return output

    except Exception as error:
        logger.log_error(error)
        logger.end(success=False)
        return {
            "non_redundant_sentences": [],
            "pure_text_script": [],
            "errors": [{"step": "step5_6", "error": str(error)}],
            "step_observability": {STEP5_6_NODE_NAME: {"global_fallback_used": 1}},
        }


async def step5_node(state: PipelineState) -> Dict[str, Any]:
    """兼容入口：旧 Step5 调用映射到 Step56。"""
    merged = await step5_6_node(state)
    if "errors" in merged:
        return {"non_redundant_sentences": [], "errors": merged["errors"]}

    step56_tokens = int(merged.get("token_usage", {}).get(STEP5_6_NODE_NAME, 0))
    step56_timing = float(merged.get("step_timings", {}).get(STEP5_6_NODE_NAME, 0.0))
    return {
        "non_redundant_sentences": merged.get("non_redundant_sentences", []),
        "current_step": "step5_clean_cross",
        "current_step_status": "completed",
        "token_usage": {"step5_clean_cross": step56_tokens},
        "step_timings": {"step5_clean_cross": step56_timing},
        "step_observability": {
            "step5_clean_cross": dict(
                (merged.get("step_observability", {}) or {}).get(STEP5_6_NODE_NAME, {})
            )
        },
    }


async def step6_node(state: PipelineState) -> Dict[str, Any]:
    """兼容入口：旧 Step6 调用映射到 Step56。"""
    compat_state = dict(state)
    if not compat_state.get("cleaned_sentences"):
        compat_state["cleaned_sentences"] = list(state.get("non_redundant_sentences", []))
    merged = await step5_6_node(compat_state)
    if "errors" in merged:
        return {"pure_text_script": [], "errors": merged["errors"]}

    step56_tokens = int(merged.get("token_usage", {}).get(STEP5_6_NODE_NAME, 0))
    step56_timing = float(merged.get("step_timings", {}).get(STEP5_6_NODE_NAME, 0.0))
    return {
        "pure_text_script": merged.get("pure_text_script", []),
        "current_step": "step6_merge_cross",
        "current_step_status": "completed",
        "token_usage": {"step6_merge_cross": step56_tokens},
        "step_timings": {"step6_merge_cross": step56_timing},
        "step_observability": {
            "step6_merge_cross": dict(
                (merged.get("step_observability", {}) or {}).get(STEP5_6_NODE_NAME, {})
            )
        },
    }
