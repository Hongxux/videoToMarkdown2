"""
模块说明：阶段流程节点 phase1_preparation 的实现。
执行逻辑：
1) 聚合本模块的类/函数，对外提供核心能力。
2) 通过内部调用与外部依赖完成具体处理。
实现方式：通过模块内函数组合与外部依赖调用实现。
核心价值：统一模块职责边界，降低跨文件耦合成本。
输入：
- 调用方传入的参数与数据路径。
输出：
- 各函数/类返回的结构化结果或副作用。"""

import os
import time
from collections import Counter
from typing import Dict, Any, List

from ..state import PipelineState
from ..tools.file_validator import (
    validate_video, 
    validate_subtitle, 
    read_subtitle_sample,
    extract_video_title
)
from ..llm.client import create_llm_client
from ..monitoring.logger import get_logger
from .step_contracts import parse_step1_topic_payload


# Step 1 主题推断 Prompt
TOPIC_INFERENCE_PROMPT = """请根据以下视频字幕样本，推断视频的领域和主题：

【字幕样本】
{sample_subtitles}

【视频标题（如有）】
{video_title}

【输出要求】
1. domain：视频所属领域，如"计算机科学"、"数学"、"物理"、"经济学"、"哲学"等
2. main_topic：核心主题，20字以内，概括视频讲解的核心内容

【输出格式】
{{"domain": "string", "main_topic": "string"}}"""

TOPIC_INFERENCE_SYSTEM_PROMPT = (
    "你是视频内容理解助手。"
    "根据字幕样本和标题判断视频 domain 与 main_topic。"
    "必须严格输出 JSON 对象，不要输出任何额外说明。"
)

TOPIC_SAMPLE_BASE_COUNT = 20
TOPIC_SAMPLE_MAX_COUNT = 120
TOPIC_SAMPLE_EXTRA_INTERVAL_MINUTES = 15
TOPIC_SAMPLE_EXTRA_PER_INTERVAL = 5
TOPIC_SAMPLE_MAX_CHARS = 6000


def _build_stage1_runtime_call_kwargs(
    *,
    stage_step: str,
    unit_id: str,
    scope_variant: str = "",
) -> Dict[str, Any]:
    normalized_stage_step = str(stage_step or "").strip() or "stage1_unknown"
    normalized_unit_id = str(unit_id or "").strip() or "unit_0001"
    normalized_scope_variant = str(scope_variant or "").strip() or normalized_unit_id
    return {
        "__runtime_identity__": {
            "step_name": normalized_stage_step,
            "request_name": "complete_json",
            "unit_id": normalized_unit_id,
            "llm_call_id": f"{normalized_stage_step}.{normalized_unit_id}",
        },
        "__runtime_metadata__": {
            "stage_step": normalized_stage_step.removeprefix("stage1_"),
            "scope_variant": normalized_scope_variant,
            "unit_id": normalized_unit_id,
        },
    }


async def _complete_json_with_runtime_identity(
    llm: Any,
    prompt: str,
    *,
    system_prompt: str,
    runtime_kwargs: Dict[str, Any],
):
    try:
        return await llm.complete_json(
            prompt,
            system_prompt=system_prompt,
            **runtime_kwargs,
        )
    except TypeError as error:
        error_text = str(error)
        if "__runtime_" not in error_text:
            raise
        return await llm.complete_json(
            prompt,
            system_prompt=system_prompt,
        )


def _read_int_env(name: str, default: int) -> int:
    """读取整数环境变量，异常时返回默认值。"""
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


def _estimate_subtitle_duration_sec(subtitles: List[Dict[str, Any]]) -> float:
    """根据字幕估算视频时长（秒）。"""
    if not subtitles:
        return 0.0
    return max(float(item.get("end_sec", 0.0) or 0.0) for item in subtitles)


def _resolve_topic_sample_count(video_duration_sec: float, subtitle_count: int) -> int:
    """按视频时长动态计算主题推断样本数量。"""
    if subtitle_count <= 0:
        return 0

    duration_minutes = max(0.0, float(video_duration_sec) / 60.0)
    extra_intervals = int(duration_minutes // TOPIC_SAMPLE_EXTRA_INTERVAL_MINUTES)
    target = TOPIC_SAMPLE_BASE_COUNT + extra_intervals * TOPIC_SAMPLE_EXTRA_PER_INTERVAL
    target = min(TOPIC_SAMPLE_MAX_COUNT, target)
    return max(1, min(subtitle_count, target))


def _pick_uniform_subtitle_samples(
    subtitles: List[Dict[str, Any]],
    target_count: int,
) -> List[Dict[str, Any]]:
    """从全量字幕中均匀抽样，覆盖开头/中段/结尾。"""
    if target_count <= 0 or not subtitles:
        return []
    if len(subtitles) <= target_count:
        return list(subtitles)

    total = len(subtitles)
    if target_count == 1:
        return [subtitles[0]]

    step = (total - 1) / float(target_count - 1)
    ordered_indices: List[int] = []
    seen = set()
    for idx in range(target_count):
        picked = max(0, min(total - 1, int(round(idx * step))))
        if picked in seen:
            continue
        ordered_indices.append(picked)
        seen.add(picked)

    # In rare rounding-collision cases, backfill nearest untouched indices.
    if len(ordered_indices) < target_count:
        for candidate in range(total):
            if candidate in seen:
                continue
            ordered_indices.append(candidate)
            seen.add(candidate)
            if len(ordered_indices) >= target_count:
                break

    ordered_indices.sort()
    return [subtitles[idx] for idx in ordered_indices]


def _build_topic_sample_text(
    sample_subtitles: List[Dict[str, Any]],
    max_chars: int = TOPIC_SAMPLE_MAX_CHARS,
) -> str:
    """构建主题推断字幕样本，限制总字符数以避免输入过长。"""
    lines: List[str] = []
    used = 0
    budget = max(200, int(max_chars))

    for subtitle in sample_subtitles:
        line = f"[{subtitle['start_sec']:.1f}s] {subtitle['text']}"
        line_len = len(line) + 1
        if used + line_len > budget:
            remaining = budget - used
            if remaining > 16:
                lines.append(line[: remaining - 3] + "...")
            break
        lines.append(line)
        used += line_len

    return "\n".join(lines)


async def step1_node(state: PipelineState) -> Dict[str, Any]:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过JSON 解析/序列化实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：not video_valid
    - 条件：not subtitle_valid
    - 条件：not video_valid or not subtitle_valid
    依据来源（证据链）：
    输入参数：
    - state: 函数入参（类型：PipelineState）。
    输出参数：
    - 结构化结果字典（包含关键字段信息）。
    补充说明：
    核心动作：
    1. 校验视频和字幕文件有效性
    2. 读取字幕样本，推断视频领域和主题"""
    logger = get_logger("step1_validate", state.get("output_dir", "output/logs"))
    logger.start()
    
    logger.log_input({
        "video_path": state["video_path"],
        "subtitle_path": state["subtitle_path"]
    })
    observability = Counter()
    
    errors = []
    
    try:
        # 1. [Tool] 校验视频文件
        logger.info("Validating video file...")
        video_valid, video_error = validate_video(state["video_path"])
        logger.log_tool_call("validate_video", {"path": state["video_path"]}, video_valid)
        
        if not video_valid:
            errors.append({"step": "step1", "type": "video_validation", "error": video_error})
            logger.log_warning(f"Video validation failed: {video_error}")
            observability["video_validation_failed"] += 1
        
        # 2. [Tool] 校验字幕文件
        logger.info("Validating subtitle file...")
        subtitle_valid, subtitle_error = validate_subtitle(state["subtitle_path"])
        logger.log_tool_call("validate_subtitle", {"path": state["subtitle_path"]}, subtitle_valid)
        
        if not subtitle_valid:
            errors.append({"step": "step1", "type": "subtitle_validation", "error": subtitle_error})
            logger.log_warning(f"Subtitle validation failed: {subtitle_error}")
            observability["subtitle_validation_failed"] += 1
        
        # 如果文件校验失败，返回错误
        if not video_valid or not subtitle_valid:
            output = {
                "is_valid": False,
                "domain": "",
                "main_topic": "",
                "video_title": "",
                "errors": errors,
                "current_step": "step1_validate",
                "current_step_status": "failed",
                "step_observability": {"step1_validate": dict(observability)},
            }
            logger.log_output(output, summary_only=True)
            logger.end(success=False)
            return output
        
        # 3. [Tool] 提取视频标题
        video_title = extract_video_title(state["video_path"])
        logger.info(f"Extracted video title: {video_title}")

        # 3.5 [Fast path] 若已有稳定主题信息，跳过字幕采样与 LLM 推断
        reuse_inferred_topic = _read_bool_env("TRANSCRIPT_STEP1_REUSE_INFERRED_TOPIC", True)
        cached_domain = str(state.get("domain", "") or "").strip()
        cached_main_topic = str(state.get("main_topic", "") or "").strip()
        if reuse_inferred_topic and cached_domain and cached_main_topic:
            logger.info("Reusing existing domain/main_topic from state, skip topic inference")
            observability["topic_reuse_hit"] += 1
            output = {
                "is_valid": True,
                "domain": cached_domain,
                "main_topic": cached_main_topic,
                "video_title": video_title,
                "current_step": "step1_validate",
                "current_step_status": "completed",
                "execution_trace": [{
                    "step_name": "step1_validate",
                    "status": "success",
                    "duration_ms": 0
                }],
                "llm_calls": [],
                "token_usage": {
                    "step1_validate": 0
                },
                "step_observability": {"step1_validate": dict(observability)},
            }
            logger.log_output(output, summary_only=True)
            timing = logger.end(success=True)
            output["step_timings"] = {"step1_validate": timing["duration_ms"]}
            return output
        
        # 4. [Tool] 读取字幕样本
        logger.info("Reading subtitles for dynamic topic sampling...")
        all_subtitles = read_subtitle_sample(state["subtitle_path"], count=None)
        subtitle_count = len(all_subtitles)
        video_duration_sec = _estimate_subtitle_duration_sec(all_subtitles)
        sample_count = _resolve_topic_sample_count(video_duration_sec, subtitle_count)
        sample_subtitles = _pick_uniform_subtitle_samples(all_subtitles, sample_count)
        sample_budget_chars = max(200, _read_int_env("TRANSCRIPT_STEP1_SAMPLE_MAX_CHARS", TOPIC_SAMPLE_MAX_CHARS))
        sample_text = _build_topic_sample_text(sample_subtitles, max_chars=sample_budget_chars)
        observability["subtitle_count"] += subtitle_count
        observability["sample_count"] += len(sample_subtitles)
        observability["sample_budget_chars"] += sample_budget_chars

        logger.log_tool_call(
            "read_subtitle_sample",
            {
                "path": state["subtitle_path"],
                "count": None,
                "subtitle_count": subtitle_count,
                "video_duration_sec": round(video_duration_sec, 2),
                "sample_count": sample_count,
                "sample_budget_chars": sample_budget_chars,
            },
            f"{len(sample_subtitles)} sampled subtitles"
        )

        # 5. [LLM] 推断领域和主题
        logger.info("Inferring domain and topic with LLM...")
        llm = create_llm_client(purpose="topic")
        
        prompt = TOPIC_INFERENCE_PROMPT.format(
            sample_subtitles=sample_text,
            video_title=video_title or "(无)"
        )
        
        result, response = await _complete_json_with_runtime_identity(
            llm,
            prompt,
            system_prompt=TOPIC_INFERENCE_SYSTEM_PROMPT,
            runtime_kwargs=_build_stage1_runtime_call_kwargs(
                stage_step="stage1_step1_validate",
                unit_id="topic_inference",
                scope_variant="topic_inference",
            ),
        )
        
        logger.log_llm_call(
            prompt=prompt,
            response=response.content,
            prompt_tokens=response.prompt_tokens,
            completion_tokens=response.completion_tokens,
            model=response.model,
            latency_ms=response.latency_ms
        )
        
        domain, main_topic, payload_metrics = parse_step1_topic_payload(result)
        observability.update(payload_metrics)
        if not domain:
            domain = "未知"
        
        logger.info(f"Inferred domain: {domain}, topic: {main_topic}")
        
        # 6. 构建输出
        output = {
            "is_valid": True,
            "domain": domain,
            "main_topic": main_topic,
            "video_title": video_title,
            "current_step": "step1_validate",
            "current_step_status": "completed",
            "execution_trace": [{
                "step_name": "step1_validate",
                "status": "success",
                "duration_ms": 0  # Will be filled by logger.end()
            }],
            "llm_calls": [{
                "step_name": "step1_validate",
                "model": response.model,
                "prompt_tokens": response.prompt_tokens,
                "completion_tokens": response.completion_tokens,
                "total_tokens": response.total_tokens,
                "latency_ms": response.latency_ms
            }],
            "token_usage": {
                "step1_validate": response.total_tokens
            },
            "step_observability": {"step1_validate": dict(observability)},
        }
        
        logger.log_output(output, summary_only=True)
        timing = logger.end(success=True)
        output["step_timings"] = {"step1_validate": timing["duration_ms"]}
        
        return output
        
    except Exception as e:
        logger.log_error(e, {"video_path": state["video_path"]})
        logger.end(success=False)
        
        return {
            "is_valid": False,
            "domain": "",
            "main_topic": "",
            "video_title": "",
            "errors": [{"step": "step1", "type": "exception", "error": str(e)}],
            "current_step": "step1_validate",
            "current_step_status": "error",
            "step_observability": {"step1_validate": dict(observability)},
        }

