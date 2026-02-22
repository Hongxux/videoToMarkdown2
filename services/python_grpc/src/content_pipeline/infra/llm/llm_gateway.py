"""
模块说明：Module2 统一 LLM 调用网关。
职责边界：
1) 统一 DeepSeek / VL / Vision AI 的调用入口，集中治理缓存、并发与去重策略。
2) 对外提供轻量函数封装，不改变既有业务类的对外 API。
实现方式：内部单例缓存 + 轻量 TTL 缓存 + in-flight 去重 + 自适应并发。
核心价值：降低重复调用、统一优化入口、避免策略散落在业务模块中。
"""

from __future__ import annotations

import asyncio
import glob
import hashlib
import json
import logging
import math
import os
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Deque, Dict, Optional, Tuple, TypeVar
import threading

from services.python_grpc.src.content_pipeline.infra.runtime import cache_metrics
from services.python_grpc.src.common.utils.deepseek_model_router import resolve_deepseek_model
from services.python_grpc.src.content_pipeline.infra.llm.llm_client import (
    LLMClient,
    AdaptiveConcurrencyLimiter,
    _AsyncLRUTTLCache,
    _AsyncInFlightDeduper,
)
from services.python_grpc.src.content_pipeline.infra.llm.deepseek_audit import append_deepseek_call_record
from services.python_grpc.src.content_pipeline.infra.llm.vision_ai_client import (
    VisionAIClient,
    VisionAIConfig,
    _VISION_BG_LOOP,
    get_vision_ai_client,
)

logger = logging.getLogger(__name__)
_HedgeResultT = TypeVar("_HedgeResultT")


# =============================================================================
# 基础配置工具
# =============================================================================


def _env_bool(name: str, default: bool) -> bool:
    """方法说明：_env_bool 工具方法。
    执行步骤：
    1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
    2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
    3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    val = str(raw).strip().lower()
    if val in {"1", "true", "yes", "y", "on"}:
        return True
    if val in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def _env_int(name: str, default: int) -> int:
    """方法说明：_env_int 工具方法。
    执行步骤：
    1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
    2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
    3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
    raw = os.getenv(name)
    if raw is None:
        return int(default)
    try:
        return int(str(raw).strip())
    except Exception:
        return int(default)


def _env_float(name: str, default: float) -> float:
    """方法说明：_env_float 工具方法。
    执行步骤：
    1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
    2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
    3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
    raw = os.getenv(name)
    if raw is None:
        return float(default)
    try:
        return float(str(raw).strip())
    except Exception:
        return float(default)


_LLM_HEDGE_ENABLED = _env_bool("MODULE2_LLM_HEDGE_ENABLED", True)
_LLM_HEDGE_DELAY_MS = max(0, _env_int("MODULE2_LLM_HEDGE_DELAY_MS", 25000))

_DEEPSEEK_HEDGE_ENABLED = _env_bool("MODULE2_DEEPSEEK_HEDGE_ENABLED", _LLM_HEDGE_ENABLED)
_DEEPSEEK_HEDGE_DELAY_MS_RAW = os.getenv("MODULE2_DEEPSEEK_HEDGE_DELAY_MS")
_DEEPSEEK_HEDGE_DELAY_MS = max(0, _env_int("MODULE2_DEEPSEEK_HEDGE_DELAY_MS", _LLM_HEDGE_DELAY_MS))
_DEEPSEEK_DYNAMIC_HEDGE_DELAY_ENABLED = _env_bool(
    "MODULE2_DEEPSEEK_HEDGE_DYNAMIC_DELAY_ENABLED",
    _DEEPSEEK_HEDGE_DELAY_MS_RAW is None,
)
_DEEPSEEK_HEDGE_DELAY_QUANTILE = min(
    0.98,
    max(0.50, _env_float("MODULE2_DEEPSEEK_HEDGE_DELAY_QUANTILE", 0.82)),
)
_DEEPSEEK_HEDGE_MIN_POOL_SAMPLES = max(
    8,
    _env_int("MODULE2_DEEPSEEK_HEDGE_MIN_POOL_SAMPLES", 24),
)
_DEEPSEEK_HEDGE_SAMPLE_WINDOW = max(
    64,
    _env_int("MODULE2_DEEPSEEK_HEDGE_SAMPLE_WINDOW", 2048),
)
_DEEPSEEK_HEDGE_BOOTSTRAP_ENABLED = _env_bool("MODULE2_DEEPSEEK_HEDGE_BOOTSTRAP_ENABLED", True)
_DEEPSEEK_HEDGE_BOOTSTRAP_GLOB = str(
    os.getenv("MODULE2_DEEPSEEK_HEDGE_BOOTSTRAP_GLOB", "var/artifacts/benchmarks/**/requests_*.json")
    or "var/artifacts/benchmarks/**/requests_*.json"
)
_DEEPSEEK_HEDGE_BOOTSTRAP_MAX_FILES = max(
    0,
    _env_int("MODULE2_DEEPSEEK_HEDGE_BOOTSTRAP_MAX_FILES", 120),
)
_DEEPSEEK_HEDGE_BOOTSTRAP_MAX_RECORDS = max(
    0,
    _env_int("MODULE2_DEEPSEEK_HEDGE_BOOTSTRAP_MAX_RECORDS", 4000),
)
_DEEPSEEK_HEDGE_CTX_CHARS_PER_TOKEN = max(
    0.2,
    _env_float("MODULE2_DEEPSEEK_HEDGE_CTX_CHARS_PER_TOKEN", 1.0),
)

_VISION_HEDGE_ENABLED = _env_bool("MODULE2_VISION_HEDGE_ENABLED", _LLM_HEDGE_ENABLED)
_VISION_HEDGE_DELAY_MS = max(0, _env_int("MODULE2_VISION_HEDGE_DELAY_MS", _LLM_HEDGE_DELAY_MS))

_VL_HEDGE_ENABLED = _env_bool("MODULE2_VL_HEDGE_ENABLED", _LLM_HEDGE_ENABLED)
_VL_HEDGE_DELAY_MS = max(0, _env_int("MODULE2_VL_HEDGE_DELAY_MS", _LLM_HEDGE_DELAY_MS))
_VL_BATCH_MAX_INFLIGHT = max(1, _env_int("MODULE2_VL_BATCH_MAX_INFLIGHT", 8))


@dataclass(frozen=True)
class _LatencySample:
    prompt_tokens: int
    total_tokens: int
    latency_ms: float


def _estimate_prompt_tokens(prompt: str, system_message: Optional[str]) -> int:
    chars = len(prompt or "") + len(system_message or "")
    return max(1, int(chars / 4))


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _normalize_deepseek_hedge_context(hedge_context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(hedge_context, dict):
        return {}
    video_duration_sec = max(0.0, _to_float(hedge_context.get("video_duration_sec", 0.0), 0.0))
    step6_text_chars = max(0, _to_int(hedge_context.get("step6_text_chars", 0), 0))
    batch_text_chars = max(0, _to_int(hedge_context.get("batch_text_chars", 0), 0))
    return {
        "video_duration_sec": video_duration_sec,
        "step6_text_chars": step6_text_chars,
        "batch_text_chars": batch_text_chars,
    }


def _quantile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    q = min(1.0, max(0.0, float(q)))
    ordered = sorted(float(v) for v in values)
    idx = (len(ordered) - 1) * q
    lo = int(idx)
    hi = min(lo + 1, len(ordered) - 1)
    if lo == hi:
        return float(ordered[lo])
    frac = idx - lo
    return float(ordered[lo] + (ordered[hi] - ordered[lo]) * frac)


class _DeepseekHedgeDelayEstimator:
    def __init__(
        self,
        *,
        quantile: float,
        min_pool_samples: int,
        sample_window: int,
        bootstrap_enabled: bool,
        bootstrap_glob: str,
        bootstrap_max_files: int,
        bootstrap_max_records: int,
    ) -> None:
        self._quantile = min(0.98, max(0.50, float(quantile)))
        self._min_pool_samples = max(8, int(min_pool_samples))
        self._samples: Deque[_LatencySample] = deque(maxlen=max(64, int(sample_window)))
        self._bootstrap_enabled = bool(bootstrap_enabled)
        self._bootstrap_glob = str(bootstrap_glob or "")
        self._bootstrap_max_files = max(0, int(bootstrap_max_files))
        self._bootstrap_max_records = max(0, int(bootstrap_max_records))
        self._bootstrapped = False
        self._lock = threading.Lock()

    def _append_sample_locked(self, prompt_tokens: int, total_tokens: int, latency_ms: float) -> None:
        prompt_tokens = max(1, int(prompt_tokens))
        total_tokens = max(prompt_tokens, int(total_tokens))
        latency_ms = float(latency_ms)
        if latency_ms <= 0:
            return
        self._samples.append(
            _LatencySample(
                prompt_tokens=prompt_tokens,
                total_tokens=total_tokens,
                latency_ms=latency_ms,
            )
        )

    def _maybe_bootstrap_locked(self) -> None:
        if self._bootstrapped:
            return
        self._bootstrapped = True
        if not self._bootstrap_enabled or not self._bootstrap_glob:
            return
        try:
            paths = glob.glob(self._bootstrap_glob, recursive=True)
        except Exception as exc:
            logger.debug("deepseek hedge bootstrap glob failed: %s", exc)
            return
        if not paths:
            return
        try:
            paths.sort(key=lambda p: os.path.getmtime(p), reverse=True)
        except Exception:
            paths.sort(reverse=True)

        imported = 0
        file_count = 0
        for path in paths:
            if self._bootstrap_max_files > 0 and file_count >= self._bootstrap_max_files:
                break
            if self._bootstrap_max_records > 0 and imported >= self._bootstrap_max_records:
                break
            file_count += 1
            try:
                with open(path, "r", encoding="utf-8") as file_obj:
                    payload = json.load(file_obj)
            except Exception:
                continue
            rows = payload if isinstance(payload, list) else [payload]
            for row in rows:
                if self._bootstrap_max_records > 0 and imported >= self._bootstrap_max_records:
                    break
                if not isinstance(row, dict):
                    continue
                status_code = int(row.get("status_code", 0) or 0)
                if status_code and status_code != 200:
                    continue
                prompt_tokens = int(row.get("prompt_tokens", 0) or 0)
                total_tokens = int(row.get("total_tokens", 0) or 0)
                completion_tokens = int(row.get("completion_tokens", 0) or 0)
                elapsed_ms = float(row.get("elapsed_ms", 0.0) or 0.0)
                if prompt_tokens <= 0 or elapsed_ms <= 0:
                    continue
                if total_tokens <= 0:
                    total_tokens = prompt_tokens + max(0, completion_tokens)
                self._append_sample_locked(
                    prompt_tokens=prompt_tokens,
                    total_tokens=total_tokens,
                    latency_ms=elapsed_ms,
                )
                imported += 1
        if imported > 0:
            logger.info("[deepseek_hedge_estimator] bootstrapped %s samples", imported)

    def _estimate_total_tokens(
        self,
        prompt_tokens: int,
        max_tokens: Optional[int],
        pool: list[_LatencySample],
    ) -> int:
        ratios = [
            max(0.0, (float(s.total_tokens) - float(s.prompt_tokens)) / float(max(s.prompt_tokens, 1)))
            for s in pool
            if s.prompt_tokens > 0 and s.total_tokens >= s.prompt_tokens
        ]
        completion_ratio = _quantile(ratios, 0.50) if ratios else 0.0
        completion_est = max(1, int(round(float(prompt_tokens) * completion_ratio)))
        if isinstance(max_tokens, int) and max_tokens > 0:
            completion_est = min(int(max_tokens), completion_est)
        return max(prompt_tokens + completion_est, prompt_tokens + 1)

    def _predict_latency_ms(self, total_tokens: int, pool: list[_LatencySample]) -> float:
        if not pool:
            return 0.0
        ms_per_token = _quantile(
            [float(max(1.0, s.latency_ms)) / float(max(1, s.total_tokens)) for s in pool if s.total_tokens > 0],
            self._quantile,
        )
        return float(total_tokens) * max(ms_per_token, 1e-6)

    def _estimate_prompt_tokens_by_context(
        self,
        *,
        prompt_tokens: int,
        hedge_context: Dict[str, Any],
    ) -> int:
        if not hedge_context:
            return max(1, int(prompt_tokens))

        # 目标：优先使用业务侧可观测输入规模（step6 文稿长度、当前批次文本长度）估算请求规模。
        # 原因：prompt 模板会引入固定噪声，直接用业务语义长度更稳定。
        # 权衡：当上下文缺失时回退 prompt_tokens，保持兼容。
        step6_text_chars = max(0, _to_int(hedge_context.get("step6_text_chars", 0), 0))
        batch_text_chars = max(0, _to_int(hedge_context.get("batch_text_chars", 0), 0))
        video_duration_sec = max(0.0, _to_float(hedge_context.get("video_duration_sec", 0.0), 0.0))
        chars_per_token = max(0.2, float(_DEEPSEEK_HEDGE_CTX_CHARS_PER_TOKEN))

        context_chars = batch_text_chars or step6_text_chars
        if context_chars <= 0:
            return max(1, int(prompt_tokens))

        context_tokens = float(context_chars) / chars_per_token
        if step6_text_chars > 0 and video_duration_sec > 0:
            # 做什么：引入“文本密度”修正，密度越高（同样视频时长文本越多）越容易触发长尾。
            # 为什么：用户明确要求视频时长 + Step6 文稿长度共同参与估算。
            # 权衡：使用对数缩放，避免极端值放大导致阈值振荡。
            transcript_density = float(step6_text_chars) / max(1.0, video_duration_sec)
            density_gain = 1.0 + (
                math.log1p(max(0.0, transcript_density))
                / max(1e-6, math.log1p(float(max(1, step6_text_chars))))
            )
            context_tokens *= max(1.0, density_gain)

        return max(int(round(context_tokens)), int(prompt_tokens), 1)

    def estimate_delay_ms(
        self,
        *,
        prompt_tokens: int,
        max_tokens: Optional[int],
        hedge_context: Optional[Dict[str, Any]] = None,
    ) -> Tuple[int, str]:
        prompt_tokens = max(1, int(prompt_tokens))
        normalized_context = _normalize_deepseek_hedge_context(hedge_context)
        effective_prompt_tokens = self._estimate_prompt_tokens_by_context(
            prompt_tokens=prompt_tokens,
            hedge_context=normalized_context,
        )
        with self._lock:
            self._maybe_bootstrap_locked()
            samples = list(self._samples)
        if not samples:
            return 0, "fallback_no_samples"

        lower = max(1, int(effective_prompt_tokens * 0.5))
        upper = max(lower, int(effective_prompt_tokens * 1.8))
        local_pool = [s for s in samples if lower <= s.prompt_tokens <= upper]
        if len(local_pool) >= self._min_pool_samples:
            pool = local_pool
            source = "local_tokens"
        else:
            pool = samples
            source = "global_tokens"

        est_total_tokens = self._estimate_total_tokens(
            prompt_tokens=effective_prompt_tokens,
            max_tokens=max_tokens,
            pool=pool,
        )
        est_latency_ms = self._predict_latency_ms(total_tokens=est_total_tokens, pool=pool)
        return max(1, int(round(est_latency_ms))), source

    def observe(
        self,
        *,
        prompt: str,
        system_message: Optional[str],
        metadata: Any,
    ) -> None:
        if metadata is None:
            return
        if isinstance(metadata, dict):
            prompt_tokens = int(metadata.get("prompt_tokens", 0) or 0)
            completion_tokens = int(metadata.get("completion_tokens", 0) or 0)
            total_tokens = int(metadata.get("total_tokens", 0) or 0)
            latency_ms = float(metadata.get("latency_ms", 0.0) or 0.0)
            cache_hit = bool(metadata.get("cache_hit", False))
        else:
            prompt_tokens = int(getattr(metadata, "prompt_tokens", 0) or 0)
            completion_tokens = int(getattr(metadata, "completion_tokens", 0) or 0)
            total_tokens = int(getattr(metadata, "total_tokens", 0) or 0)
            latency_ms = float(getattr(metadata, "latency_ms", 0.0) or 0.0)
            cache_hit = bool(getattr(metadata, "cache_hit", False))

        if cache_hit or latency_ms <= 0:
            return
        if prompt_tokens <= 0:
            prompt_tokens = _estimate_prompt_tokens(prompt, system_message)
        if total_tokens <= 0:
            total_tokens = prompt_tokens + max(0, completion_tokens)
        if total_tokens <= 0:
            total_tokens = prompt_tokens + 1

        with self._lock:
            self._append_sample_locked(
                prompt_tokens=prompt_tokens,
                total_tokens=total_tokens,
                latency_ms=latency_ms,
            )


_DEEPSEEK_HEDGE_ESTIMATOR = _DeepseekHedgeDelayEstimator(
    quantile=_DEEPSEEK_HEDGE_DELAY_QUANTILE,
    min_pool_samples=_DEEPSEEK_HEDGE_MIN_POOL_SAMPLES,
    sample_window=_DEEPSEEK_HEDGE_SAMPLE_WINDOW,
    bootstrap_enabled=_DEEPSEEK_HEDGE_BOOTSTRAP_ENABLED,
    bootstrap_glob=_DEEPSEEK_HEDGE_BOOTSTRAP_GLOB,
    bootstrap_max_files=_DEEPSEEK_HEDGE_BOOTSTRAP_MAX_FILES,
    bootstrap_max_records=_DEEPSEEK_HEDGE_BOOTSTRAP_MAX_RECORDS,
)


def _resolve_deepseek_hedge_delay_ms(
    *,
    request_name: str,
    prompt: str,
    system_message: Optional[str],
    max_tokens: Optional[int],
    hedge_context: Optional[Dict[str, Any]] = None,
) -> int:
    if not _DEEPSEEK_DYNAMIC_HEDGE_DELAY_ENABLED:
        return _DEEPSEEK_HEDGE_DELAY_MS

    prompt_tokens = _estimate_prompt_tokens(prompt, system_message)
    normalized_context = _normalize_deepseek_hedge_context(hedge_context)
    estimated_ms, source = _DEEPSEEK_HEDGE_ESTIMATOR.estimate_delay_ms(
        prompt_tokens=prompt_tokens,
        max_tokens=max_tokens,
        hedge_context=normalized_context,
    )
    if estimated_ms <= 0:
        return _DEEPSEEK_HEDGE_DELAY_MS
    logger.debug(
        "[%s] hedge delay estimated=%sms prompt_tokens=%s source=%s video_duration=%.2fs step6_chars=%s batch_chars=%s",
        request_name,
        estimated_ms,
        prompt_tokens,
        source,
        float(normalized_context.get("video_duration_sec", 0.0) or 0.0),
        int(normalized_context.get("step6_text_chars", 0) or 0),
        int(normalized_context.get("batch_text_chars", 0) or 0),
    )
    return estimated_ms


async def _run_hedged_async_request(
    *,
    request_name: str,
    enabled: bool,
    delay_ms: int,
    primary_factory: Callable[[], Awaitable[_HedgeResultT]],
    secondary_factory: Optional[Callable[[], Awaitable[_HedgeResultT]]] = None,
) -> _HedgeResultT:
    """统一执行 hedged request：慢请求超时后补发并行副本，返回先成功者并取消迟到请求。"""
    if not enabled or int(delay_ms) <= 0:
        return await primary_factory()

    secondary = secondary_factory or primary_factory
    delay_seconds = max(0.001, float(delay_ms) / 1000.0)
    primary_task = asyncio.create_task(primary_factory())
    hedge_task: Optional[asyncio.Task[_HedgeResultT]] = None
    pending_tasks: set[asyncio.Task[_HedgeResultT]] = set()

    try:
        try:
            # 主请求在阈值内返回时不额外放大流量。
            return await asyncio.wait_for(asyncio.shield(primary_task), timeout=delay_seconds)
        except asyncio.TimeoutError:
            logger.info("[%s] hedge triggered after %sms", request_name, delay_ms)

        hedge_task = asyncio.create_task(secondary())
        pending_tasks = {primary_task, hedge_task}
        errors: list[Exception] = []

        while pending_tasks:
            done, pending = await asyncio.wait(pending_tasks, return_when=asyncio.FIRST_COMPLETED)
            for finished in done:
                try:
                    result = finished.result()
                    for late in pending:
                        late.cancel()
                    if pending:
                        await asyncio.gather(*pending, return_exceptions=True)
                    return result
                except asyncio.CancelledError:
                    continue
                except Exception as exc:
                    errors.append(exc)
            pending_tasks = set(pending)

        if errors:
            raise errors[0]
        raise RuntimeError(f"{request_name} hedged request finished without result")
    finally:
        for task in (primary_task, hedge_task):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *[task for task in (primary_task, hedge_task) if task is not None],
            return_exceptions=True,
        )


async def _call_deepseek_text_once(
    *,
    client: LLMClient,
    prompt: str,
    system_message: Optional[str],
    need_logprobs: bool,
    disable_inflight_dedup: bool,
) -> Tuple[str, Any, Any]:
    if disable_inflight_dedup:
        try:
            return await client.complete_text(
                prompt=prompt,
                system_message=system_message,
                need_logprobs=need_logprobs,
                disable_inflight_dedup=True,
            )
        except TypeError:
            # 兼容注入的旧版 fake client（无 disable_inflight_dedup 参数）。
            pass
    return await client.complete_text(
        prompt=prompt,
        system_message=system_message,
        need_logprobs=need_logprobs,
    )


async def _call_deepseek_json_once(
    *,
    client: LLMClient,
    prompt: str,
    system_message: Optional[str],
    need_logprobs: bool,
    max_tokens: Optional[int],
    disable_inflight_dedup: bool,
) -> Tuple[Dict[str, Any], Any, Any]:
    if disable_inflight_dedup:
        try:
            return await client.complete_json(
                prompt=prompt,
                system_message=system_message,
                need_logprobs=need_logprobs,
                max_tokens=max_tokens,
                disable_inflight_dedup=True,
            )
        except TypeError:
            # 兼容注入的旧版 fake client（无 disable_inflight_dedup 参数）。
            pass
    return await client.complete_json(
        prompt=prompt,
        system_message=system_message,
        need_logprobs=need_logprobs,
        max_tokens=max_tokens,
    )


# =============================================================================
# DeepSeek 统一入口
# =============================================================================


_DEEPSEEK_CLIENTS: Dict[str, LLMClient] = {}
_DEEPSEEK_LOCK = threading.Lock()


def _hash_text(value: str) -> str:
    """方法说明：_hash_text 工具方法。
    执行步骤：
    1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
    2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
    3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
    h = hashlib.sha256()
    h.update((value or "").encode("utf-8"))
    return h.hexdigest()


def _build_deepseek_client_key(
    api_key: str,
    base_url: str,
    model: str,
    temperature: float,
) -> str:
    # 为什么：避免把明文 API Key 作为 dict key 暴露在内存中，降低误用风险
    """方法说明：_build_deepseek_client_key 工具方法。
    执行步骤：
    1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
    2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
    3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
    key = "|".join(
        [
            _hash_text(api_key),
            str(base_url or ""),
            str(model or ""),
            f"{float(temperature):.3f}",
        ]
    )
    return key


def get_deepseek_client(
    api_key: Optional[str] = None,
    base_url: str = "https://api.deepseek.com/v1",
    model: str = "deepseek-chat",
    temperature: float = 0.3,
    enable_logprobs: Optional[bool] = None,
    cache_enabled: Optional[bool] = None,
    inflight_dedup_enabled: Optional[bool] = None,
) -> LLMClient:
    """
    作用：获取或创建 DeepSeek LLMClient 单例。
    为什么：统一客户端池，避免各模块重复初始化导致连接池与并发策略漂移。
    权衡：单例按 key 复用，若要强制隔离，可显式传不同参数生成新实例。
    """
    resolved_model = resolve_deepseek_model(model, default_model="deepseek-chat")
    key = _build_deepseek_client_key(api_key or "", base_url, resolved_model, temperature)
    with _DEEPSEEK_LOCK:
        client = _DEEPSEEK_CLIENTS.get(key)
        if client is not None:
            return client
        client = LLMClient(
            api_key=api_key,
            base_url=base_url,
            model=resolved_model,
            temperature=temperature,
            enable_logprobs=enable_logprobs,
            cache_enabled=cache_enabled,
            inflight_dedup_enabled=inflight_dedup_enabled,
        )
        _DEEPSEEK_CLIENTS[key] = client
        return client


async def deepseek_complete_text(
    *,
    prompt: str,
    system_message: Optional[str] = None,
    need_logprobs: bool = False,
    hedge_context: Optional[Dict[str, Any]] = None,
    client: Optional[LLMClient] = None,
    api_key: Optional[str] = None,
    base_url: str = "https://api.deepseek.com/v1",
    model: str = "deepseek-chat",
    temperature: float = 0.3,
    enable_logprobs: Optional[bool] = None,
    cache_enabled: Optional[bool] = None,
    inflight_dedup_enabled: Optional[bool] = None,
) -> Tuple[str, Any, Any]:
    """
    作用：统一调用 DeepSeek 文本输出。
    为什么：把重试/缓存/并发治理沉到统一入口。
    权衡：保留 client 注入通道，便于测试或自定义模型。
    """
    resolved_model = resolve_deepseek_model(model, default_model="deepseek-chat")
    if client is None:
        client = get_deepseek_client(
            api_key=api_key,
            base_url=base_url,
            model=resolved_model,
            temperature=temperature,
            enable_logprobs=enable_logprobs,
            cache_enabled=cache_enabled,
            inflight_dedup_enabled=inflight_dedup_enabled,
        )
    output_text = ""
    metadata = None
    logprobs = None
    error_text = ""
    delay_ms = _resolve_deepseek_hedge_delay_ms(
        request_name="deepseek_complete_text",
        prompt=prompt,
        system_message=system_message,
        max_tokens=None,
        hedge_context=hedge_context,
    )
    try:
        output_text, metadata, logprobs = await _run_hedged_async_request(
            request_name="deepseek_complete_text",
            enabled=_DEEPSEEK_HEDGE_ENABLED,
            delay_ms=delay_ms,
            primary_factory=lambda: _call_deepseek_text_once(
                client=client,
                prompt=prompt,
                system_message=system_message,
                need_logprobs=need_logprobs,
                disable_inflight_dedup=False,
            ),
            secondary_factory=lambda: _call_deepseek_text_once(
                client=client,
                prompt=prompt,
                system_message=system_message,
                need_logprobs=need_logprobs,
                disable_inflight_dedup=True,
            ),
        )
        return output_text, metadata, logprobs
    except Exception as exc:
        error_text = str(exc)
        raise
    finally:
        try:
            _DEEPSEEK_HEDGE_ESTIMATOR.observe(
                prompt=prompt,
                system_message=system_message,
                metadata=metadata,
            )
        except Exception as hedge_observe_exc:
            logger.debug("DeepSeek hedge estimator observe failed: %s", hedge_observe_exc)
        try:
            append_deepseek_call_record(
                prompt=prompt,
                system_message=str(system_message or ""),
                model=resolved_model,
                temperature=float(temperature),
                need_logprobs=bool(need_logprobs),
                output_text=str(output_text or ""),
                metadata=metadata,
                error=error_text,
                extra={
                    "gateway": "deepseek_complete_text",
                },
            )
        except Exception as audit_exc:
            logger.warning(f"DeepSeek audit append failed: {audit_exc}")


async def deepseek_complete_json(
    *,
    prompt: str,
    system_message: Optional[str] = None,
    need_logprobs: bool = False,
    max_tokens: Optional[int] = None,
    hedge_context: Optional[Dict[str, Any]] = None,
    client: Optional[LLMClient] = None,
    api_key: Optional[str] = None,
    base_url: str = "https://api.deepseek.com/v1",
    model: str = "deepseek-chat",
    temperature: float = 0.3,
    enable_logprobs: Optional[bool] = None,
    cache_enabled: Optional[bool] = None,
    inflight_dedup_enabled: Optional[bool] = None,
) -> Tuple[Dict[str, Any], Any, Any]:
    """
    作用：统一调用 DeepSeek JSON 输出。
    为什么：集中控制 JSON 解析、缓存与并发策略。
    权衡：允许 max_tokens 透传，但需注意缓存 key 维度变化。
    """
    resolved_model = resolve_deepseek_model(model, default_model="deepseek-chat")
    if client is None:
        client = get_deepseek_client(
            api_key=api_key,
            base_url=base_url,
            model=resolved_model,
            temperature=temperature,
            enable_logprobs=enable_logprobs,
            cache_enabled=cache_enabled,
            inflight_dedup_enabled=inflight_dedup_enabled,
        )
    metadata = None
    delay_ms = _resolve_deepseek_hedge_delay_ms(
        request_name="deepseek_complete_json",
        prompt=prompt,
        system_message=system_message,
        max_tokens=max_tokens,
        hedge_context=hedge_context,
    )
    try:
        result_json, metadata, logprobs = await _run_hedged_async_request(
            request_name="deepseek_complete_json",
            enabled=_DEEPSEEK_HEDGE_ENABLED,
            delay_ms=delay_ms,
            primary_factory=lambda: _call_deepseek_json_once(
                client=client,
                prompt=prompt,
                system_message=system_message,
                need_logprobs=need_logprobs,
                max_tokens=max_tokens,
                disable_inflight_dedup=False,
            ),
            secondary_factory=lambda: _call_deepseek_json_once(
                client=client,
                prompt=prompt,
                system_message=system_message,
                need_logprobs=need_logprobs,
                max_tokens=max_tokens,
                disable_inflight_dedup=True,
            ),
        )
        return result_json, metadata, logprobs
    finally:
        try:
            _DEEPSEEK_HEDGE_ESTIMATOR.observe(
                prompt=prompt,
                system_message=system_message,
                metadata=metadata,
            )
        except Exception as hedge_observe_exc:
            logger.debug("DeepSeek hedge estimator observe failed: %s", hedge_observe_exc)


# =============================================================================
# Vision AI 统一入口
# =============================================================================


async def vision_validate_image(
    *,
    image_path: str,
    prompt: str = "",
    system_prompt: Optional[str] = None,
    skip_duplicate_check: bool = False,
    client: Optional[VisionAIClient] = None,
    config: Optional[VisionAIConfig] = None,
) -> Dict[str, Any]:
    """
    作用：统一 Vision AI 异步调用入口。
    为什么：避免业务模块直接操作 VisionAIClient，便于统一策略演进。
    权衡：保留 client 注入，避免重复初始化与便于测试。
    """
    if client is None:
        client = get_vision_ai_client(config)
    return await _run_hedged_async_request(
        request_name="vision_validate_image",
        enabled=_VISION_HEDGE_ENABLED,
        delay_ms=_VISION_HEDGE_DELAY_MS,
        primary_factory=lambda: client.validate_image(
            image_path=image_path,
            prompt=prompt,
            system_prompt=system_prompt,
            skip_duplicate_check=skip_duplicate_check,
        ),
        secondary_factory=lambda: client.validate_image(
            image_path=image_path,
            prompt=prompt,
            system_prompt=system_prompt,
            skip_duplicate_check=skip_duplicate_check,
        ),
    )


async def vision_validate_images(
    *,
    image_paths: list[str],
    prompt: str = "",
    system_prompt: Optional[str] = None,
    skip_duplicate_check: bool = False,
    max_batch_size: Optional[int] = None,
    client: Optional[VisionAIClient] = None,
    config: Optional[VisionAIConfig] = None,
) -> list[Dict[str, Any]]:
    """
    作用：统一 Vision AI 批量异步调用入口。
    为什么：集中管理批量参数与回退逻辑，避免业务层直接操作客户端细节。
    """
    if client is None:
        client = get_vision_ai_client(config)
    return await _run_hedged_async_request(
        request_name="vision_validate_images",
        enabled=_VISION_HEDGE_ENABLED,
        delay_ms=_VISION_HEDGE_DELAY_MS,
        primary_factory=lambda: client.validate_images_batch(
            image_paths=image_paths,
            prompt=prompt,
            system_prompt=system_prompt,
            skip_duplicate_check=skip_duplicate_check,
            max_batch_size=max_batch_size,
        ),
        secondary_factory=lambda: client.validate_images_batch(
            image_paths=image_paths,
            prompt=prompt,
            system_prompt=system_prompt,
            skip_duplicate_check=skip_duplicate_check,
            max_batch_size=max_batch_size,
        ),
    )


def vision_validate_image_sync(
    *,
    image_path: str,
    prompt: str = "",
    system_prompt: Optional[str] = None,
    skip_duplicate_check: bool = False,
    client: Optional[VisionAIClient] = None,
    config: Optional[VisionAIConfig] = None,
    timeout: Optional[float] = None,
) -> Dict[str, Any]:
    """
    作用：统一 Vision AI 同步调用入口（复用后台事件循环）。
    为什么：同步路径更常见于旧代码，统一入口便于后续收敛。
    权衡：同步调用占用线程，但对外 API 行为保持不变。
    """
    if client is None:
        client = get_vision_ai_client(config)
    return _VISION_BG_LOOP.submit(
        _run_hedged_async_request(
            request_name="vision_validate_image_sync",
            enabled=_VISION_HEDGE_ENABLED,
            delay_ms=_VISION_HEDGE_DELAY_MS,
            primary_factory=lambda: client.validate_image(
                image_path=image_path,
                prompt=prompt,
                system_prompt=system_prompt,
                skip_duplicate_check=skip_duplicate_check,
            ),
            secondary_factory=lambda: client.validate_image(
                image_path=image_path,
                prompt=prompt,
                system_prompt=system_prompt,
                skip_duplicate_check=skip_duplicate_check,
            ),
        ),
        timeout=timeout,
    )


def vision_validate_images_sync(
    *,
    image_paths: list[str],
    prompt: str = "",
    system_prompt: Optional[str] = None,
    skip_duplicate_check: bool = False,
    max_batch_size: Optional[int] = None,
    client: Optional[VisionAIClient] = None,
    config: Optional[VisionAIConfig] = None,
    timeout: Optional[float] = None,
) -> list[Dict[str, Any]]:
    """
    作用：统一 Vision AI 批量同步调用入口（复用后台事件循环）。
    为什么：兼容同步调用方，减少业务层对异步模型的耦合。
    """
    if client is None:
        client = get_vision_ai_client(config)
    return _VISION_BG_LOOP.submit(
        _run_hedged_async_request(
            request_name="vision_validate_images_sync",
            enabled=_VISION_HEDGE_ENABLED,
            delay_ms=_VISION_HEDGE_DELAY_MS,
            primary_factory=lambda: client.validate_images_batch(
                image_paths=image_paths,
                prompt=prompt,
                system_prompt=system_prompt,
                skip_duplicate_check=skip_duplicate_check,
                max_batch_size=max_batch_size,
            ),
            secondary_factory=lambda: client.validate_images_batch(
                image_paths=image_paths,
                prompt=prompt,
                system_prompt=system_prompt,
                skip_duplicate_check=skip_duplicate_check,
                max_batch_size=max_batch_size,
            ),
        ),
        timeout=timeout,
    )


# =============================================================================
# VL 统一入口
# =============================================================================


@dataclass
class VLChatResult:
    """
    作用：统一封装 VL ChatCompletion 响应。
    为什么：便于缓存与调用方解析，降低对 SDK 响应结构的耦合。
    权衡：仅保留关键字段，忽略不常用的扩展信息。
    """
    content: str
    finish_reason: Optional[str]
    usage: Dict[str, int]
    model: str


@dataclass
class _VLCacheEntry:
    """类说明：_VLCacheEntry 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    content: str
    finish_reason: Optional[str]
    usage: Dict[str, int]
    model: str
    created_at: float
    expires_at: float


_VL_CACHE_ENABLED = _env_bool("MODULE2_VL_CACHE_ENABLED", True)
_VL_CACHE_MAX_ITEMS = _env_int("MODULE2_VL_CACHE_MAX_ITEMS", 512)
_VL_CACHE_TTL_SECONDS = _env_int("MODULE2_VL_CACHE_TTL_SECONDS", 3600)
_VL_INFLIGHT_DEDUP_ENABLED = _env_bool("MODULE2_VL_INFLIGHT_DEDUP_ENABLED", True)

_VL_CACHE = _AsyncLRUTTLCache(
    max_items=_VL_CACHE_MAX_ITEMS,
    ttl_seconds=_VL_CACHE_TTL_SECONDS,
)
_VL_DEDUPER = _AsyncInFlightDeduper()

_VL_CONCURRENCY = AdaptiveConcurrencyLimiter(
    initial_limit=_env_int("MODULE2_VL_CONCURRENCY_INITIAL", 8),
    min_limit=_env_int("MODULE2_VL_CONCURRENCY_MIN", 2),
    max_limit=_env_int("MODULE2_VL_CONCURRENCY_MAX", 60),
    increase_step=_env_int("MODULE2_VL_CONCURRENCY_INCREASE_STEP", 1),
    decrease_factor=_env_float("MODULE2_VL_CONCURRENCY_DECREASE_FACTOR", 0.5),
    window_size=_env_int("MODULE2_VL_CONCURRENCY_WINDOW_SIZE", 20),
)


def _extract_usage_from_response(response: Any) -> Dict[str, int]:
    """方法说明：_extract_usage_from_response 工具方法。
    执行步骤：
    1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
    2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
    3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
    usage = None
    if isinstance(response, dict):
        usage = response.get("usage")
    else:
        usage = getattr(response, "usage", None)

    prompt_tokens = 0
    completion_tokens = 0
    total_tokens = 0

    def _as_int(val: Any, default: int = 0) -> int:
        try:
            return int(val)
        except Exception:
            return int(default)

    if usage is not None:
        if isinstance(usage, dict):
            prompt_tokens = _as_int(usage.get("prompt_tokens", 0))
            completion_tokens = _as_int(usage.get("completion_tokens", 0))
            total_tokens = _as_int(usage.get("total_tokens", prompt_tokens + completion_tokens))
        else:
            prompt_tokens = _as_int(getattr(usage, "prompt_tokens", 0))
            completion_tokens = _as_int(getattr(usage, "completion_tokens", 0))
            total_tokens = _as_int(getattr(usage, "total_tokens", prompt_tokens + completion_tokens))

    return {
        "prompt_tokens": max(0, prompt_tokens),
        "completion_tokens": max(0, completion_tokens),
        "total_tokens": max(0, total_tokens),
    }


async def _call_vl_api_once(
    *,
    client: Any,
    model: str,
    messages: Any,
    max_tokens: int,
    temperature: float,
    response_format: Optional[Dict[str, Any]] = None,
) -> Tuple[str, Optional[str], Dict[str, int], str]:
    """方法说明：_call_vl_api_once 工具方法。
    执行步骤：
    1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
    2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
    3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
    kwargs: Dict[str, Any] = {
        "model": model,
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": temperature,
    }
    if response_format is not None:
        kwargs["response_format"] = response_format

    response = await client.chat.completions.create(**kwargs)
    content = response.choices[0].message.content
    finish_reason = getattr(response.choices[0], "finish_reason", None)
    usage = _extract_usage_from_response(response)
    model_name = str(getattr(response, "model", model) or model)
    return content, finish_reason, usage, model_name


async def vl_chat_completion(
    *,
    client: Any,
    model: str,
    messages: Any,
    max_tokens: int,
    temperature: float,
    response_format: Optional[Dict[str, Any]] = None,
    cache_key: Optional[str] = None,
) -> VLChatResult:
    """
    作用：统一 VL ChatCompletion 调用，附带缓存、去重与并发控制。
    为什么：VL 调用成本高且波动大，需统一治理减少重复调用。
    权衡：缓存使用 cache_key 控制，调用方需保证 key 稳定与正确性。
    """
    if cache_key and _VL_CACHE_ENABLED:
        cached = await _VL_CACHE.get(cache_key)
        if cached is not None:
            cache_metrics.hit("module2.vl.result_cache")
            return VLChatResult(
                content=str(cached.content or ""),
                finish_reason=cached.finish_reason,
                usage=dict(cached.usage or {}),
                model=str(cached.model or model),
            )
        cache_metrics.miss("module2.vl.result_cache")

    async def _do_request() -> VLChatResult:
        acquired = 0
        try:
            acquired = await _VL_CONCURRENCY.acquire(1)
            content, finish_reason, usage, model_name = await _call_vl_api_once(
                client=client,
                model=model,
                messages=messages,
                max_tokens=max_tokens,
                temperature=temperature,
                response_format=response_format,
            )
            await _VL_CONCURRENCY.record_success()

            if cache_key and _VL_CACHE_ENABLED:
                now = _VL_CACHE.now()
                entry = _VLCacheEntry(
                    content=content,
                    finish_reason=finish_reason,
                    usage=usage,
                    model=model_name,
                    created_at=now,
                    expires_at=now + float(_VL_CACHE.ttl_seconds()),
                )
                await _VL_CACHE.set(cache_key, entry)

            return VLChatResult(
                content=content,
                finish_reason=finish_reason,
                usage=usage,
                model=model_name,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            err_str = str(exc)
            is_rate_limit = "429" in err_str or "rate" in err_str.lower()
            await _VL_CONCURRENCY.record_failure(is_rate_limit=is_rate_limit)
            raise
        finally:
            if acquired:
                await _VL_CONCURRENCY.release(acquired)

    async def _do_hedged_request() -> VLChatResult:
        return await _run_hedged_async_request(
            request_name="vl_chat_completion",
            enabled=_VL_HEDGE_ENABLED,
            delay_ms=_VL_HEDGE_DELAY_MS,
            primary_factory=_do_request,
            secondary_factory=_do_request,
        )

    if cache_key and _VL_INFLIGHT_DEDUP_ENABLED:
        return await _VL_DEDUPER.run(cache_key, _do_hedged_request)
    return await _do_hedged_request()


async def vl_chat_completions(
    *,
    requests: list[Dict[str, Any]],
    max_inflight: Optional[int] = None,
) -> list[VLChatResult]:
    """
    作用：批量执行 VL ChatCompletion，保持输入顺序返回结果。
    为什么：上层批量视频分析需要一个统一批量入口，避免各处重复实现并发控制。
    权衡：采用“并发单请求”而不是“单请求多视频”，兼容现有网关与缓存语义。
    """
    if not requests:
        return []

    try:
        resolved_inflight = int(max_inflight) if max_inflight is not None else int(_VL_BATCH_MAX_INFLIGHT)
    except Exception:
        resolved_inflight = int(_VL_BATCH_MAX_INFLIGHT)
    resolved_inflight = max(1, min(resolved_inflight, len(requests)))

    semaphore = asyncio.Semaphore(resolved_inflight)
    ordered_results: list[Optional[VLChatResult]] = [None] * len(requests)

    async def _run_single(index: int, payload: Dict[str, Any]) -> None:
        async with semaphore:
            if not isinstance(payload, dict):
                raise ValueError(f"vl_chat_completions request[{index}] must be dict")
            ordered_results[index] = await vl_chat_completion(
                client=payload["client"],
                model=str(payload["model"]),
                messages=payload["messages"],
                max_tokens=int(payload["max_tokens"]),
                temperature=float(payload["temperature"]),
                response_format=payload.get("response_format"),
                cache_key=payload.get("cache_key"),
            )

    await asyncio.gather(*[_run_single(index, payload) for index, payload in enumerate(requests)])

    finalized_results: list[VLChatResult] = []
    for index, item in enumerate(ordered_results):
        if item is None:
            raise RuntimeError(f"vl_chat_completions result missing at index {index}")
        finalized_results.append(item)
    return finalized_results
