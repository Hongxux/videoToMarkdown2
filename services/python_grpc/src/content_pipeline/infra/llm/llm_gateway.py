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
import json
import logging
import math
import os
import random
import time
import traceback
from collections import deque
from dataclasses import dataclass
import threading
from typing import Any, Awaitable, Callable, Deque, Dict, Optional, Tuple, TypeVar

import httpx

from services.python_grpc.src.content_pipeline.infra.runtime import cache_metrics
from services.python_grpc.src.common.utils.hash_policy import fast_digest_text
from services.python_grpc.src.common.utils.runtime_llm_context import (
    build_restored_llm_response,
    build_runtime_llm_request_payload,
    current_runtime_llm_context,
    dump_runtime_json_text,
)
from services.python_grpc.src.common.utils.deepseek_model_router import resolve_deepseek_model
from services.python_grpc.src.content_pipeline.infra.llm.llm_client import (
    LLMClient,
    AdaptiveConcurrencyLimiter,
    _AsyncLRUTTLCache,
    _AsyncInFlightDeduper,
)
from services.python_grpc.src.content_pipeline.infra.llm.deepseek_audit import append_deepseek_call_record
from services.python_grpc.src.content_pipeline.infra.llm.vision_ai_audit import append_vision_ai_call_record
from services.python_grpc.src.content_pipeline.infra.llm.token_costing import normalize_usage_payload
from services.python_grpc.src.content_pipeline.infra.llm.vision_ai_client import (
    VisionAIClient,
    VisionAIConfig,
    _VISION_BG_LOOP,
    ensure_sync_bridge_not_in_running_loop,
    get_vision_ai_client,
)
from services.python_grpc.src.content_pipeline.rate_limiter import VLRateLimiter

logger = logging.getLogger(__name__)
_HedgeResultT = TypeVar("_HedgeResultT")
_VISION_INTERNAL_RESPONSE_METADATA_KEY = "__llm_response_metadata"


def _truncate_text(value: str, max_chars: int = 8000) -> str:
    text = str(value or "")
    if max_chars > 0 and len(text) > max_chars:
        return text[:max_chars] + "\n...[TRUNCATED]"
    return text


def _build_retry_failure_record(
    *,
    request_name: str,
    provider_label: str,
    attempt_no: int,
    retry_kind: str,
    wait_seconds: float,
    retryable: bool,
    error: Exception,
) -> Dict[str, Any]:
    return {
        "request_name": str(request_name or ""),
        "provider": str(provider_label or ""),
        "attempt": max(1, int(attempt_no or 1)),
        "retry_kind": str(retry_kind or ""),
        "retryable": bool(retryable),
        "wait_seconds": float(wait_seconds or 0.0),
        "error_type": error.__class__.__name__,
        "error_message": str(error or ""),
        "stack_trace": _truncate_text(
            "".join(traceback.format_exception(type(error), error, error.__traceback__)),
        ),
        "recorded_at_ms": int(time.time() * 1000),
    }


def _normalize_previous_failures(records: Any) -> list[Dict[str, Any]]:
    normalized: list[Dict[str, Any]] = []
    for record in list(records or []):
        if not isinstance(record, dict):
            continue
        normalized.append(dict(record))
    return normalized


def _attach_fallback_metadata(
    metadata: Any,
    *,
    fallback_payload: Optional[Dict[str, Any]] = None,
) -> Any:
    normalized_fallback = dict(fallback_payload or {})
    is_fallback = bool(normalized_fallback.get("is_fallback"))
    previous_failures = _normalize_previous_failures(normalized_fallback.get("previous_failures"))
    if metadata is None:
        return metadata
    try:
        setattr(metadata, "is_fallback", is_fallback)
        setattr(metadata, "fallback", normalized_fallback if is_fallback else {})
        setattr(metadata, "previous_failures", previous_failures)
        propagated_scope_refs = normalized_fallback.get("propagated_scope_refs", [])
        if isinstance(propagated_scope_refs, list):
            setattr(metadata, "propagated_scope_refs", list(propagated_scope_refs))
    except Exception:
        pass
    return metadata


def _build_runtime_response_metadata(
    *,
    metadata: Any,
    requested_model: str,
    fallback_payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    normalized_fallback = dict(fallback_payload or {})
    is_fallback = bool(normalized_fallback.get("is_fallback"))
    return {
        "prompt_tokens": int(getattr(metadata, "prompt_tokens", 0) or 0),
        "completion_tokens": int(getattr(metadata, "completion_tokens", 0) or 0),
        "total_tokens": int(getattr(metadata, "total_tokens", 0) or 0),
        "model": str(getattr(metadata, "model", requested_model) or requested_model),
        "latency_ms": float(getattr(metadata, "latency_ms", 0.0) or 0.0),
        "raw_response": getattr(metadata, "raw_response", None),
        "cache_hit": bool(getattr(metadata, "cache_hit", False)),
        "usage_details": normalize_usage_payload(getattr(metadata, "usage_details", None)),
        "is_fallback": is_fallback,
        "fallback": normalized_fallback if is_fallback else {},
        "previous_failures": _normalize_previous_failures(normalized_fallback.get("previous_failures")),
        "propagated_scope_refs": (
            list(normalized_fallback.get("propagated_scope_refs", []) or [])
            if is_fallback
            else []
        ),
    }


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


_LLM_HEDGE_ENABLED = _env_bool("MODULE2_LLM_HEDGE_ENABLED", False)
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
_DEEPSEEK_QWEN_FALLBACK_ENABLED = _env_bool("MODULE2_DEEPSEEK_QWEN_FALLBACK_ENABLED", True)
_DEEPSEEK_QWEN_FALLBACK_BASE_URL = (
    str(
        os.getenv(
            "MODULE2_DEEPSEEK_QWEN_FALLBACK_BASE_URL",
            "https://dashscope.aliyuncs.com/compatible-mode/v1",
        )
        or ""
    ).strip()
    or "https://dashscope.aliyuncs.com/compatible-mode/v1"
)
_DEEPSEEK_QWEN_FALLBACK_MODEL = (
    str(os.getenv("MODULE2_DEEPSEEK_QWEN_FALLBACK_MODEL", "qwen-plus") or "").strip()
    or "qwen-plus"
)
_DEEPSEEK_QWEN_FALLBACK_API_KEY_ENV = (
    str(os.getenv("MODULE2_DEEPSEEK_QWEN_FALLBACK_API_KEY_ENV", "DASHSCOPE_API_KEY") or "").strip()
    or "DASHSCOPE_API_KEY"
)
_DEEPSEEK_QWEN_FALLBACK_API_KEY = str(
    os.getenv("MODULE2_DEEPSEEK_QWEN_FALLBACK_API_KEY", "") or ""
).strip()
_DEEPSEEK_PROVIDER_MAX_RETRIES = 4
_DEEPSEEK_PROVIDER_INITIAL_BACKOFF_SEC = 2.0
_DEEPSEEK_PROVIDER_MAX_BACKOFF_SEC = 16.0
_POOL_RETRY_ATTEMPTS = 5
_POOL_RETRY_JITTER_RATIO = 0.3
_DEEPSEEK_FAST_FALLBACK_LOCK = threading.Lock()
_DEEPSEEK_FAST_FALLBACK_OPEN = False
_DEEPSEEK_RUNTIME_PROVIDER_ROUTE_NAME = "deepseek_primary"

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


def _is_deepseek_fast_fallback_open() -> bool:
    with _DEEPSEEK_FAST_FALLBACK_LOCK:
        return bool(_DEEPSEEK_FAST_FALLBACK_OPEN)


def _trip_deepseek_fast_fallback() -> bool:
    global _DEEPSEEK_FAST_FALLBACK_OPEN
    with _DEEPSEEK_FAST_FALLBACK_LOCK:
        if _DEEPSEEK_FAST_FALLBACK_OPEN:
            return False
        _DEEPSEEK_FAST_FALLBACK_OPEN = True
        return True


def _reset_deepseek_fast_fallback() -> bool:
    global _DEEPSEEK_FAST_FALLBACK_OPEN
    with _DEEPSEEK_FAST_FALLBACK_LOCK:
        was_open = _DEEPSEEK_FAST_FALLBACK_OPEN
        _DEEPSEEK_FAST_FALLBACK_OPEN = False
        return bool(was_open)


def _get_runtime_deepseek_provider_route(runtime_context: Any) -> Dict[str, Any]:
    if runtime_context is None or not hasattr(runtime_context, "get_llm_provider_route"):
        return {}
    try:
        route_snapshot = runtime_context.get_llm_provider_route(_DEEPSEEK_RUNTIME_PROVIDER_ROUTE_NAME)
    except Exception as exc:
        logger.debug("runtime deepseek provider route lookup failed: %s", exc)
        return {}
    return dict(route_snapshot or {})


def _is_runtime_deepseek_routed_to_qwen(route_snapshot: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(route_snapshot, dict):
        return False
    return str(route_snapshot.get("provider", "") or "").strip().lower() == "qwen"


def _pin_runtime_deepseek_to_qwen(
    *,
    runtime_context: Any,
    gateway_name: str,
    requested_model: str,
    source_error: Exception,
    previous_failures: Optional[list[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    if runtime_context is None or not hasattr(runtime_context, "pin_llm_provider_route"):
        return {}

    existing_route = _get_runtime_deepseek_provider_route(runtime_context)
    route_snapshot = runtime_context.pin_llm_provider_route(
        route_name=_DEEPSEEK_RUNTIME_PROVIDER_ROUTE_NAME,
        provider="qwen",
        source_provider="deepseek",
        reason=str(source_error or ""),
        extra={
            "gateway": str(gateway_name or ""),
            "requested_model": str(requested_model or ""),
            "target_provider": f"Qwen/{_DEEPSEEK_QWEN_FALLBACK_MODEL}",
            "target_base_url": str(_DEEPSEEK_QWEN_FALLBACK_BASE_URL or ""),
            "previous_failures": _normalize_previous_failures(previous_failures),
        },
    )
    if not _is_runtime_deepseek_routed_to_qwen(existing_route):
        logger.warning(
            "[LLM-alt-provider] Current task pinned to Qwen after DeepSeek failure. task_id=%s, stage=%s, gateway=%s, target=%s, error=%s",
            str(getattr(runtime_context, "task_id", "") or ""),
            str(getattr(runtime_context, "stage", "") or ""),
            gateway_name,
            route_snapshot.get("target_provider", f"Qwen/{_DEEPSEEK_QWEN_FALLBACK_MODEL}"),
            source_error,
        )
    return dict(route_snapshot or {})


def _build_provider_switch_payload(
    *,
    fallback_label: str,
    fallback_reason: str,
    source_provider: str,
    target_provider: str,
    previous_failures: Optional[list[Dict[str, Any]]] = None,
    fast_fallback_mode: bool = False,
    task_provider_locked: bool = False,
    route_snapshot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "is_fallback": False,
        "fallback_kind": "provider_switch",
        "fallback_label": str(fallback_label or ""),
        "fallback_reason": str(fallback_reason or ""),
        "source_provider": str(source_provider or ""),
        "target_provider": str(target_provider or ""),
        "fast_fallback_mode": bool(fast_fallback_mode),
        "task_provider_locked": bool(task_provider_locked),
        "previous_failures": _normalize_previous_failures(previous_failures),
    }
    if isinstance(route_snapshot, dict) and route_snapshot:
        payload["task_provider_route"] = {
            "route_name": str(route_snapshot.get("route_name", "") or ""),
            "provider": str(route_snapshot.get("provider", "") or ""),
            "gateway": str(route_snapshot.get("gateway", "") or ""),
            "pinned_at_ms": int(route_snapshot.get("pinned_at_ms", 0) or 0),
            "updated_at_ms": int(route_snapshot.get("updated_at_ms", 0) or 0),
        }
    return payload


def _iter_exception_chain(exc: BaseException, max_depth: int = 6) -> list[BaseException]:
    """遍历异常链，补足被包装的连接池异常。"""
    chain: list[BaseException] = []
    seen: set[int] = set()
    current: Optional[BaseException] = exc
    depth = 0
    while current is not None and depth < max(1, int(max_depth)):
        current_id = id(current)
        if current_id in seen:
            break
        seen.add(current_id)
        chain.append(current)
        current = current.__cause__ or current.__context__
        depth += 1
    return chain


def _is_connection_pool_exhausted_error(exc: BaseException) -> bool:
    """识别连接池资源不足导致的异常。"""
    for current in _iter_exception_chain(exc):
        if isinstance(current, httpx.PoolTimeout):
            return True
        text = str(current).lower()
        if "pool timeout" in text or "connection pool" in text or "no available connections" in text:
            return True
    return False


def _compute_pool_backoff_seconds(retry_index: int) -> float:
    base_seconds = _DEEPSEEK_PROVIDER_INITIAL_BACKOFF_SEC * (2 ** max(0, int(retry_index)))
    base_seconds = min(_DEEPSEEK_PROVIDER_MAX_BACKOFF_SEC, float(base_seconds))
    jitter = base_seconds * float(_POOL_RETRY_JITTER_RATIO)
    if jitter > 0:
        base_seconds += random.uniform(0.0, jitter)
    return max(0.0, float(base_seconds))


def _compute_provider_retry_backoff_seconds(retry_index: int) -> float:
    bounded_retry_index = max(0, int(retry_index))
    wait_seconds = _DEEPSEEK_PROVIDER_INITIAL_BACKOFF_SEC * (2 ** bounded_retry_index)
    return min(_DEEPSEEK_PROVIDER_MAX_BACKOFF_SEC, float(wait_seconds))


async def _run_provider_with_retry(
    *,
    request_name: str,
    provider_label: str,
    max_retries: int,
    attempt_factory: Callable[[], Awaitable[_HedgeResultT]],
    attempt_history: Optional[list[Dict[str, Any]]] = None,
) -> _HedgeResultT:
    total_attempts = max(1, int(max_retries) + 1)
    last_exc: Optional[Exception] = None
    provider_retry_index = 0
    pool_retry_index = 0
    call_attempt = 0
    while True:
        call_attempt += 1
        try:
            return await attempt_factory()
        except Exception as exc:
            last_exc = exc
            if _is_connection_pool_exhausted_error(exc) and pool_retry_index < int(_POOL_RETRY_ATTEMPTS):
                wait_seconds = _compute_pool_backoff_seconds(pool_retry_index)
                pool_retry_index += 1
                if attempt_history is not None:
                    attempt_history.append(
                        _build_retry_failure_record(
                            request_name=request_name,
                            provider_label=provider_label,
                            attempt_no=call_attempt,
                            retry_kind="pool_retry",
                            wait_seconds=wait_seconds,
                            retryable=True,
                            error=exc,
                        )
                    )
                logger.warning(
                    "[LLM-pool] %s pool exhausted, retry %s/%s scheduled. provider=%s, wait=%.1fs, error=%s",
                    request_name,
                    pool_retry_index,
                    _POOL_RETRY_ATTEMPTS,
                    provider_label,
                    wait_seconds,
                    exc,
                )
                await asyncio.sleep(wait_seconds)
                continue
            if provider_retry_index >= total_attempts - 1:
                if attempt_history is not None:
                    attempt_history.append(
                        _build_retry_failure_record(
                            request_name=request_name,
                            provider_label=provider_label,
                            attempt_no=call_attempt,
                            retry_kind="exhausted",
                            wait_seconds=0.0,
                            retryable=False,
                            error=exc,
                        )
                    )
                raise
            retry_no = provider_retry_index + 1
            wait_seconds = _compute_provider_retry_backoff_seconds(provider_retry_index)
            provider_retry_index += 1
            if attempt_history is not None:
                attempt_history.append(
                    _build_retry_failure_record(
                        request_name=request_name,
                        provider_label=provider_label,
                        attempt_no=call_attempt,
                        retry_kind="provider_retry",
                        wait_seconds=wait_seconds,
                        retryable=True,
                        error=exc,
                    )
                )
            logger.warning(
                "[LLM-retry] %s failed, retry %s/%s scheduled. provider=%s, wait=%.1fs, error=%s",
                request_name,
                retry_no,
                max_retries,
                provider_label,
                wait_seconds,
                exc,
            )
            await asyncio.sleep(wait_seconds)
    raise last_exc if last_exc is not None else RuntimeError(f"{request_name} retry finished without result")


def _resolve_unwrapped_async_method(bound_method: Any) -> Optional[Callable[..., Awaitable[Any]]]:
    wrapped = getattr(bound_method, "__wrapped__", None)
    if wrapped is not None:
        return wrapped
    method_func = getattr(bound_method, "__func__", None)
    if method_func is not None:
        wrapped = getattr(method_func, "__wrapped__", None)
        if wrapped is not None:
            return wrapped
    return None


async def _call_client_complete_text_raw(client: Any, **kwargs: Any) -> Tuple[str, Any, Any]:
    bound_method = getattr(client, "complete_text")
    unwrapped_method = _resolve_unwrapped_async_method(bound_method)
    if unwrapped_method is not None:
        return await unwrapped_method(client, **kwargs)
    return await bound_method(**kwargs)


async def _call_client_complete_json_raw(client: Any, **kwargs: Any) -> Tuple[Dict[str, Any], Any, Any]:
    bound_method = getattr(client, "complete_json")
    unwrapped_method = _resolve_unwrapped_async_method(bound_method)
    if unwrapped_method is not None:
        return await unwrapped_method(client, **kwargs)
    return await bound_method(**kwargs)


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
            return await _call_client_complete_text_raw(
                client,
                prompt=prompt,
                system_message=system_message,
                need_logprobs=need_logprobs,
                disable_inflight_dedup=True,
            )
        except TypeError:
            pass
    return await _call_client_complete_text_raw(
        client,
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
            return await _call_client_complete_json_raw(
                client,
                prompt=prompt,
                system_message=system_message,
                need_logprobs=need_logprobs,
                max_tokens=max_tokens,
                disable_inflight_dedup=True,
            )
        except TypeError:
            pass
    return await _call_client_complete_json_raw(
        client,
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
    return fast_digest_text(value)


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


def _resolve_qwen_fallback_api_key() -> Tuple[str, str]:
    explicit_api_key = str(_DEEPSEEK_QWEN_FALLBACK_API_KEY or "").strip()
    if explicit_api_key:
        return explicit_api_key, "MODULE2_DEEPSEEK_QWEN_FALLBACK_API_KEY"

    api_key_env = str(_DEEPSEEK_QWEN_FALLBACK_API_KEY_ENV or "").strip() or "DASHSCOPE_API_KEY"
    env_api_key = str(os.getenv(api_key_env, "") or "").strip()
    return env_api_key, api_key_env


def _build_qwen_fallback_client(
    *,
    temperature: float,
    enable_logprobs: Optional[bool],
    cache_enabled: Optional[bool],
    inflight_dedup_enabled: Optional[bool],
) -> Optional[LLMClient]:
    if not _DEEPSEEK_QWEN_FALLBACK_ENABLED:
        return None

    fallback_api_key, api_key_source = _resolve_qwen_fallback_api_key()
    if not fallback_api_key:
        logger.warning(
            "[LLM-alt-provider] Cannot switch to Qwen alternate provider because API key is missing. source=%s",
            api_key_source,
        )
        return None

    return get_deepseek_client(
        api_key=fallback_api_key,
        base_url=_DEEPSEEK_QWEN_FALLBACK_BASE_URL,
        model=_DEEPSEEK_QWEN_FALLBACK_MODEL,
        temperature=temperature,
        enable_logprobs=enable_logprobs,
        cache_enabled=cache_enabled,
        inflight_dedup_enabled=inflight_dedup_enabled,
    )


async def _call_qwen_text_provider(
    *,
    prompt: str,
    system_message: Optional[str],
    need_logprobs: bool,
    temperature: float,
    enable_logprobs: Optional[bool],
    cache_enabled: Optional[bool],
    inflight_dedup_enabled: Optional[bool],
    request_name: str,
) -> Optional[Tuple[Tuple[str, Any, Any], list[Dict[str, Any]]]]:
    fallback_client = _build_qwen_fallback_client(
        temperature=temperature,
        enable_logprobs=enable_logprobs,
        cache_enabled=cache_enabled,
        inflight_dedup_enabled=inflight_dedup_enabled,
    )
    if fallback_client is None:
        return None

    fallback_attempt_history: list[Dict[str, Any]] = []
    result = await _run_provider_with_retry(
        request_name=request_name,
        provider_label=f"Qwen/{_DEEPSEEK_QWEN_FALLBACK_MODEL}",
        max_retries=_DEEPSEEK_PROVIDER_MAX_RETRIES,
        attempt_factory=lambda: _call_deepseek_text_once(
            client=fallback_client,
            prompt=prompt,
            system_message=system_message,
            need_logprobs=need_logprobs,
            disable_inflight_dedup=False,
        ),
        attempt_history=fallback_attempt_history,
    )
    return result, fallback_attempt_history


async def _fallback_to_qwen_text(
    *,
    prompt: str,
    system_message: Optional[str],
    need_logprobs: bool,
    temperature: float,
    enable_logprobs: Optional[bool],
    cache_enabled: Optional[bool],
    inflight_dedup_enabled: Optional[bool],
    source_error: Exception,
) -> Optional[Tuple[Tuple[str, Any, Any], list[Dict[str, Any]]]]:
    logger.warning(
        "[LLM-alt-provider] DeepSeek text call failed; switching to Qwen alternate provider. model=%s, base_url=%s, error=%s",
        _DEEPSEEK_QWEN_FALLBACK_MODEL,
        _DEEPSEEK_QWEN_FALLBACK_BASE_URL,
        source_error,
    )
    try:
        fallback_output = await _call_qwen_text_provider(
            prompt=prompt,
            system_message=system_message,
            need_logprobs=need_logprobs,
            temperature=temperature,
            enable_logprobs=enable_logprobs,
            cache_enabled=cache_enabled,
            inflight_dedup_enabled=inflight_dedup_enabled,
            request_name="qwen_fallback_text",
        )
        if fallback_output is None:
            return None
        result, fallback_attempt_history = fallback_output
        logger.warning(
            "[LLM-alt-provider] DeepSeek text call succeeded via Qwen alternate provider. model=%s",
            _DEEPSEEK_QWEN_FALLBACK_MODEL,
        )
        return result, fallback_attempt_history
    except Exception as fallback_exc:
        logger.error(
            "[LLM-alt-provider] DeepSeek text alternate-provider switch to Qwen failed. model=%s, original_error=%s, fallback_error=%s",
            _DEEPSEEK_QWEN_FALLBACK_MODEL,
            source_error,
            fallback_exc,
        )
        raise


async def _call_qwen_json_provider(
    *,
    prompt: str,
    system_message: Optional[str],
    need_logprobs: bool,
    max_tokens: Optional[int],
    temperature: float,
    enable_logprobs: Optional[bool],
    cache_enabled: Optional[bool],
    inflight_dedup_enabled: Optional[bool],
    request_name: str,
) -> Optional[Tuple[Tuple[Dict[str, Any], Any, Any], list[Dict[str, Any]]]]:
    fallback_client = _build_qwen_fallback_client(
        temperature=temperature,
        enable_logprobs=enable_logprobs,
        cache_enabled=cache_enabled,
        inflight_dedup_enabled=inflight_dedup_enabled,
    )
    if fallback_client is None:
        return None

    fallback_attempt_history: list[Dict[str, Any]] = []
    result = await _run_provider_with_retry(
        request_name=request_name,
        provider_label=f"Qwen/{_DEEPSEEK_QWEN_FALLBACK_MODEL}",
        max_retries=_DEEPSEEK_PROVIDER_MAX_RETRIES,
        attempt_factory=lambda: _call_deepseek_json_once(
            client=fallback_client,
            prompt=prompt,
            system_message=system_message,
            need_logprobs=need_logprobs,
            max_tokens=max_tokens,
            disable_inflight_dedup=False,
        ),
        attempt_history=fallback_attempt_history,
    )
    return result, fallback_attempt_history


async def _fallback_to_qwen_json(
    *,
    prompt: str,
    system_message: Optional[str],
    need_logprobs: bool,
    max_tokens: Optional[int],
    temperature: float,
    enable_logprobs: Optional[bool],
    cache_enabled: Optional[bool],
    inflight_dedup_enabled: Optional[bool],
    source_error: Exception,
) -> Optional[Tuple[Tuple[Dict[str, Any], Any, Any], list[Dict[str, Any]]]]:
    logger.warning(
        "[LLM-alt-provider] DeepSeek JSON call failed; switching to Qwen alternate provider. model=%s, base_url=%s, error=%s",
        _DEEPSEEK_QWEN_FALLBACK_MODEL,
        _DEEPSEEK_QWEN_FALLBACK_BASE_URL,
        source_error,
    )
    try:
        fallback_output = await _call_qwen_json_provider(
            prompt=prompt,
            system_message=system_message,
            need_logprobs=need_logprobs,
            max_tokens=max_tokens,
            temperature=temperature,
            enable_logprobs=enable_logprobs,
            cache_enabled=cache_enabled,
            inflight_dedup_enabled=inflight_dedup_enabled,
            request_name="qwen_fallback_json",
        )
        if fallback_output is None:
            return None
        result, fallback_attempt_history = fallback_output
        logger.warning(
            "[LLM-alt-provider] DeepSeek JSON call succeeded via Qwen alternate provider. model=%s",
            _DEEPSEEK_QWEN_FALLBACK_MODEL,
        )
        return result, fallback_attempt_history
    except Exception as fallback_exc:
        logger.error(
            "[LLM-alt-provider] DeepSeek JSON alternate-provider switch to Qwen failed. model=%s, original_error=%s, fallback_error=%s",
            _DEEPSEEK_QWEN_FALLBACK_MODEL,
            source_error,
            fallback_exc,
        )
        raise


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
    runtime_identity: Optional[Dict[str, Any]] = None,
) -> Tuple[str, Any, Any]:
    """
    作用：统一调用 DeepSeek 文本输出。
    为什么：把重试/缓存/并发治理沉到统一入口。
    权衡：保留 client 注入通道，便于测试或自定义模型。
    """
    resolved_model = resolve_deepseek_model(model, default_model="deepseek-chat")
    runtime_context = current_runtime_llm_context()
    request_payload = build_runtime_llm_request_payload(
        model=resolved_model,
        prompt=prompt,
        system_prompt=system_message or "",
        kwargs={
            "need_logprobs": bool(need_logprobs),
            "temperature": float(temperature),
            "hedge_context": dict(hedge_context or {}),
            "gateway": "deepseek_complete_text",
        },
    )
    if runtime_context is not None:
        restored = runtime_context.load_committed_call(
            provider="deepseek",
            request_name="deepseek_complete_text",
            request_payload=request_payload,
            runtime_identity=runtime_identity,
        )
        if restored is not None:
            restored_metadata = dict(restored.get("response_metadata", {}) or {})
            return (
                str(restored.get("response_text", "") or ""),
                build_restored_llm_response(
                    response_text=str(restored.get("response_text", "") or ""),
                    response_metadata=restored_metadata,
                ),
                None,
            )
    runtime_route_snapshot = _get_runtime_deepseek_provider_route(runtime_context)
    output_text = ""
    metadata = None
    logprobs = None
    error_text = ""
    fallback_payload: Dict[str, Any] = {}
    if _is_runtime_deepseek_routed_to_qwen(runtime_route_snapshot):
        try:
            routed_output = await _call_qwen_text_provider(
                prompt=prompt,
                system_message=system_message,
                need_logprobs=need_logprobs,
                temperature=temperature,
                enable_logprobs=enable_logprobs,
                cache_enabled=cache_enabled,
                inflight_dedup_enabled=inflight_dedup_enabled,
                request_name="qwen_task_routed_text",
            )
            if routed_output is None:
                raise RuntimeError("Qwen alternate provider is unavailable for task-routed text request")
            (output_text, metadata, logprobs), fallback_attempt_history = routed_output
            fallback_payload = _build_provider_switch_payload(
                fallback_label="deepseek_task_route_to_qwen_text",
                fallback_reason=str(runtime_route_snapshot.get("reason", "") or "task routed to qwen"),
                source_provider="DeepSeek",
                target_provider=f"Qwen/{_DEEPSEEK_QWEN_FALLBACK_MODEL}",
                previous_failures=list(runtime_route_snapshot.get("previous_failures", []) or [])
                + list(fallback_attempt_history or []),
                task_provider_locked=True,
                route_snapshot=runtime_route_snapshot,
            )
            metadata = _attach_fallback_metadata(
                metadata,
                fallback_payload=fallback_payload,
            )
            if runtime_context is not None:
                runtime_context.persist_success(
                    provider="deepseek",
                    request_name="deepseek_complete_text",
                    request_payload=request_payload,
                    response_text=output_text,
                    response_metadata=_build_runtime_response_metadata(
                        metadata=metadata,
                        requested_model=resolved_model,
                        fallback_payload=fallback_payload,
                    ),
                    runtime_identity=runtime_identity,
                )
            return output_text, metadata, logprobs
        except Exception as exc:
            error_text = str(exc)
            if runtime_context is not None:
                runtime_context.persist_failure(
                    provider="deepseek",
                    request_name="deepseek_complete_text",
                    request_payload=request_payload,
                    error=exc,
                    runtime_identity=runtime_identity,
                )
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
                        "fallback": fallback_payload,
                    },
                )
            except Exception as audit_exc:
                logger.warning(f"DeepSeek audit append failed: {audit_exc}")
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
    primary_attempt_history: list[Dict[str, Any]] = []
    delay_ms = _resolve_deepseek_hedge_delay_ms(
        request_name="deepseek_complete_text",
        prompt=prompt,
        system_message=system_message,
        max_tokens=None,
        hedge_context=hedge_context,
    )
    deepseek_fast_fallback_open = _is_deepseek_fast_fallback_open()
    deepseek_max_retries = 0 if deepseek_fast_fallback_open else _DEEPSEEK_PROVIDER_MAX_RETRIES
    try:
        output_text, metadata, logprobs = await _run_provider_with_retry(
            request_name="deepseek_complete_text",
            provider_label="DeepSeek",
            max_retries=deepseek_max_retries,
            attempt_factory=lambda: _run_hedged_async_request(
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
            ),
            attempt_history=primary_attempt_history,
        )
        if deepseek_fast_fallback_open and _reset_deepseek_fast_fallback():
            logger.warning("[LLM-alt-provider] DeepSeek recovered; alternate-provider fast switch mode closed.")
        if runtime_context is not None:
            runtime_context.persist_success(
                provider="deepseek",
                request_name="deepseek_complete_text",
                request_payload=request_payload,
                response_text=output_text,
                response_metadata=_build_runtime_response_metadata(
                    metadata=metadata,
                    requested_model=resolved_model,
                    fallback_payload=fallback_payload,
                ),
                runtime_identity=runtime_identity,
            )
        return output_text, metadata, logprobs
    except Exception as exc:
        error_text = str(exc)
        if not deepseek_fast_fallback_open and _trip_deepseek_fast_fallback():
            logger.warning(
                "[LLM-alt-provider] DeepSeek exhausted retries; alternate-provider fast switch mode opened. retries=%s, error=%s",
                _DEEPSEEK_PROVIDER_MAX_RETRIES,
                exc,
            )
        runtime_route_snapshot = _pin_runtime_deepseek_to_qwen(
            runtime_context=runtime_context,
            gateway_name="deepseek_complete_text",
            requested_model=resolved_model,
            source_error=exc,
            previous_failures=primary_attempt_history,
        )
        fallback_output = await _fallback_to_qwen_text(
            prompt=prompt,
            system_message=system_message,
            need_logprobs=need_logprobs,
            temperature=temperature,
            enable_logprobs=enable_logprobs,
            cache_enabled=cache_enabled,
            inflight_dedup_enabled=inflight_dedup_enabled,
            source_error=exc,
        )
        if fallback_output is not None:
            (output_text, metadata, logprobs), fallback_attempt_history = fallback_output
            fallback_previous_failures = _normalize_previous_failures(
                runtime_route_snapshot.get("previous_failures", []),
            )
            if not fallback_previous_failures:
                fallback_previous_failures = list(primary_attempt_history)
            fallback_previous_failures.extend(list(fallback_attempt_history or []))
            fallback_payload = _build_provider_switch_payload(
                fallback_label="deepseek_to_qwen_text",
                fallback_reason=str(exc or ""),
                source_provider="DeepSeek",
                target_provider=f"Qwen/{_DEEPSEEK_QWEN_FALLBACK_MODEL}",
                previous_failures=fallback_previous_failures,
                fast_fallback_mode=bool(deepseek_fast_fallback_open),
                task_provider_locked=_is_runtime_deepseek_routed_to_qwen(runtime_route_snapshot),
                route_snapshot=runtime_route_snapshot,
            )
            metadata = _attach_fallback_metadata(
                metadata,
                fallback_payload=fallback_payload,
            )
            error_text = ""
            if runtime_context is not None:
                runtime_context.persist_success(
                    provider="deepseek",
                    request_name="deepseek_complete_text",
                    request_payload=request_payload,
                    response_text=output_text,
                    response_metadata=_build_runtime_response_metadata(
                        metadata=metadata,
                        requested_model=resolved_model,
                        fallback_payload=fallback_payload,
                    ),
                    runtime_identity=runtime_identity,
                )
            return output_text, metadata, logprobs
        if runtime_context is not None:
            runtime_context.persist_failure(
                provider="deepseek",
                request_name="deepseek_complete_text",
                request_payload=request_payload,
                error=exc,
                runtime_identity=runtime_identity,
            )
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
                    "fallback": fallback_payload,
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
    runtime_identity: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, Any], Any, Any]:
    """
    作用：统一调用 DeepSeek JSON 输出。
    为什么：集中控制 JSON 解析、缓存与并发策略。
    权衡：允许 max_tokens 透传，但需注意缓存 key 维度变化。
    """
    resolved_model = resolve_deepseek_model(model, default_model="deepseek-chat")
    runtime_context = current_runtime_llm_context()
    request_payload = build_runtime_llm_request_payload(
        model=resolved_model,
        prompt=prompt,
        system_prompt=system_message or "",
        kwargs={
            "need_logprobs": bool(need_logprobs),
            "max_tokens": max_tokens,
            "temperature": float(temperature),
            "hedge_context": dict(hedge_context or {}),
            "gateway": "deepseek_complete_json",
        },
    )
    if runtime_context is not None:
        restored = runtime_context.load_committed_call(
            provider="deepseek",
            request_name="deepseek_complete_json",
            request_payload=request_payload,
            runtime_identity=runtime_identity,
        )
        if restored is not None:
            restored_metadata = dict(restored.get("response_metadata", {}) or {})
            restored_text = str(restored.get("response_text", "") or "")
            return (
                json.loads(restored_text),
                build_restored_llm_response(
                    response_text=restored_text,
                    response_metadata=restored_metadata,
                ),
                None,
            )
    runtime_route_snapshot = _get_runtime_deepseek_provider_route(runtime_context)
    metadata = None
    result_json: Dict[str, Any] = {}
    logprobs = None
    error_text = ""
    fallback_payload: Dict[str, Any] = {}
    if _is_runtime_deepseek_routed_to_qwen(runtime_route_snapshot):
        try:
            routed_output = await _call_qwen_json_provider(
                prompt=prompt,
                system_message=system_message,
                need_logprobs=need_logprobs,
                max_tokens=max_tokens,
                temperature=temperature,
                enable_logprobs=enable_logprobs,
                cache_enabled=cache_enabled,
                inflight_dedup_enabled=inflight_dedup_enabled,
                request_name="qwen_task_routed_json",
            )
            if routed_output is None:
                raise RuntimeError("Qwen alternate provider is unavailable for task-routed json request")
            (result_json, metadata, logprobs), fallback_attempt_history = routed_output
            fallback_payload = _build_provider_switch_payload(
                fallback_label="deepseek_task_route_to_qwen_json",
                fallback_reason=str(runtime_route_snapshot.get("reason", "") or "task routed to qwen"),
                source_provider="DeepSeek",
                target_provider=f"Qwen/{_DEEPSEEK_QWEN_FALLBACK_MODEL}",
                previous_failures=list(runtime_route_snapshot.get("previous_failures", []) or [])
                + list(fallback_attempt_history or []),
                task_provider_locked=True,
                route_snapshot=runtime_route_snapshot,
            )
            metadata = _attach_fallback_metadata(
                metadata,
                fallback_payload=fallback_payload,
            )
            if runtime_context is not None:
                runtime_context.persist_success(
                    provider="deepseek",
                    request_name="deepseek_complete_json",
                    request_payload=request_payload,
                    response_text=dump_runtime_json_text(result_json),
                    response_metadata=_build_runtime_response_metadata(
                        metadata=metadata,
                        requested_model=resolved_model,
                        fallback_payload=fallback_payload,
                    ),
                    runtime_identity=runtime_identity,
                )
            return result_json, metadata, logprobs
        except Exception as exc:
            error_text = str(exc)
            if runtime_context is not None:
                runtime_context.persist_failure(
                    provider="deepseek",
                    request_name="deepseek_complete_json",
                    request_payload=request_payload,
                    error=exc,
                    runtime_identity=runtime_identity,
                )
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
                    output_text=dump_runtime_json_text(result_json) if result_json else "",
                    metadata=metadata,
                    error=error_text,
                    extra={
                        "gateway": "deepseek_complete_json",
                        "fallback": fallback_payload,
                    },
                )
            except Exception as audit_exc:
                logger.warning(f"DeepSeek JSON audit append failed: {audit_exc}")
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
    primary_attempt_history: list[Dict[str, Any]] = []
    delay_ms = _resolve_deepseek_hedge_delay_ms(
        request_name="deepseek_complete_json",
        prompt=prompt,
        system_message=system_message,
        max_tokens=max_tokens,
        hedge_context=hedge_context,
    )
    deepseek_fast_fallback_open = _is_deepseek_fast_fallback_open()
    deepseek_max_retries = 0 if deepseek_fast_fallback_open else _DEEPSEEK_PROVIDER_MAX_RETRIES
    try:
        result_json, metadata, logprobs = await _run_provider_with_retry(
            request_name="deepseek_complete_json",
            provider_label="DeepSeek",
            max_retries=deepseek_max_retries,
            attempt_factory=lambda: _run_hedged_async_request(
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
            ),
            attempt_history=primary_attempt_history,
        )
        if deepseek_fast_fallback_open and _reset_deepseek_fast_fallback():
            logger.warning("[LLM-alt-provider] DeepSeek JSON recovered; alternate-provider fast switch mode closed.")
        if runtime_context is not None:
            runtime_context.persist_success(
                provider="deepseek",
                request_name="deepseek_complete_json",
                request_payload=request_payload,
                response_text=dump_runtime_json_text(result_json),
                response_metadata=_build_runtime_response_metadata(
                    metadata=metadata,
                    requested_model=resolved_model,
                    fallback_payload=fallback_payload,
                ),
                runtime_identity=runtime_identity,
            )
        return result_json, metadata, logprobs
    except Exception as exc:
        error_text = str(exc)
        if not deepseek_fast_fallback_open and _trip_deepseek_fast_fallback():
            logger.warning(
                "[LLM-alt-provider] DeepSeek JSON exhausted retries; alternate-provider fast switch mode opened. retries=%s, error=%s",
                _DEEPSEEK_PROVIDER_MAX_RETRIES,
                exc,
            )
        runtime_route_snapshot = _pin_runtime_deepseek_to_qwen(
            runtime_context=runtime_context,
            gateway_name="deepseek_complete_json",
            requested_model=resolved_model,
            source_error=exc,
            previous_failures=primary_attempt_history,
        )
        fallback_json = await _fallback_to_qwen_json(
            prompt=prompt,
            system_message=system_message,
            need_logprobs=need_logprobs,
            max_tokens=max_tokens,
            temperature=temperature,
            enable_logprobs=enable_logprobs,
            cache_enabled=cache_enabled,
            inflight_dedup_enabled=inflight_dedup_enabled,
            source_error=exc,
        )
        if fallback_json is not None:
            (result_json, metadata, logprobs), fallback_attempt_history = fallback_json
            fallback_previous_failures = _normalize_previous_failures(
                runtime_route_snapshot.get("previous_failures", []),
            )
            if not fallback_previous_failures:
                fallback_previous_failures = list(primary_attempt_history)
            fallback_previous_failures.extend(list(fallback_attempt_history or []))
            fallback_payload = _build_provider_switch_payload(
                fallback_label="deepseek_to_qwen_json",
                fallback_reason=str(exc or ""),
                source_provider="DeepSeek",
                target_provider=f"Qwen/{_DEEPSEEK_QWEN_FALLBACK_MODEL}",
                previous_failures=fallback_previous_failures,
                fast_fallback_mode=bool(deepseek_fast_fallback_open),
                task_provider_locked=_is_runtime_deepseek_routed_to_qwen(runtime_route_snapshot),
                route_snapshot=runtime_route_snapshot,
            )
            metadata = _attach_fallback_metadata(
                metadata,
                fallback_payload=fallback_payload,
            )
            error_text = ""
            if runtime_context is not None:
                runtime_context.persist_success(
                    provider="deepseek",
                    request_name="deepseek_complete_json",
                    request_payload=request_payload,
                    response_text=dump_runtime_json_text(result_json),
                    response_metadata=_build_runtime_response_metadata(
                        metadata=metadata,
                        requested_model=resolved_model,
                        fallback_payload=fallback_payload,
                    ),
                    runtime_identity=runtime_identity,
                )
            return result_json, metadata, logprobs
        if runtime_context is not None:
            runtime_context.persist_failure(
                provider="deepseek",
                request_name="deepseek_complete_json",
                request_payload=request_payload,
                error=exc,
                runtime_identity=runtime_identity,
            )
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
                output_text=dump_runtime_json_text(result_json) if result_json else "",
                metadata=metadata,
                error=error_text,
                extra={
                    "gateway": "deepseek_complete_json",
                    "fallback": fallback_payload,
                },
            )
        except Exception as audit_exc:
            logger.warning(f"DeepSeek JSON audit append failed: {audit_exc}")


# =============================================================================
# Vision AI 统一入口
# =============================================================================


def _extract_vision_response_metadata(payload: Any) -> Dict[str, Any]:
    if isinstance(payload, dict):
        raw_metadata = payload.get(_VISION_INTERNAL_RESPONSE_METADATA_KEY, {})
        if isinstance(raw_metadata, dict):
            metadata = dict(raw_metadata)
            usage_details = metadata.get("usage_details")
            if usage_details is not None:
                metadata["usage_details"] = normalize_usage_payload(usage_details)
            return metadata
        return {}
    if isinstance(payload, list):
        for item in payload:
            metadata = _extract_vision_response_metadata(item)
            if metadata:
                return metadata
    return {}


def _strip_vision_response_metadata(payload: Any) -> Any:
    if isinstance(payload, dict):
        cleaned = dict(payload)
        cleaned.pop(_VISION_INTERNAL_RESPONSE_METADATA_KEY, None)
        return cleaned
    if isinstance(payload, list):
        return [_strip_vision_response_metadata(item) for item in payload]
    return payload


def _build_vision_runtime_metadata(
    *,
    request_payload: Dict[str, Any],
    response_metadata: Dict[str, Any],
) -> Dict[str, Any]:
    usage_details = normalize_usage_payload(response_metadata.get("usage_details"))
    return {
        "model": str(response_metadata.get("model") or request_payload.get("model", "") or ""),
        "prompt_tokens": int(response_metadata.get("prompt_tokens", 0) or 0),
        "completion_tokens": int(response_metadata.get("completion_tokens", 0) or 0),
        "total_tokens": int(response_metadata.get("total_tokens", 0) or 0),
        "latency_ms": float(response_metadata.get("latency_ms", 0.0) or 0.0),
        "cache_hit": bool(response_metadata.get("cache_hit", False)),
        "usage_details": usage_details,
    }


async def vision_validate_image(
    *,
    image_path: str,
    prompt: str = "",
    system_prompt: Optional[str] = None,
    skip_duplicate_check: bool = False,
    client: Optional[VisionAIClient] = None,
    config: Optional[VisionAIConfig] = None,
    runtime_identity: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    作用：统一 Vision AI 异步调用入口。
    为什么：避免业务模块直接操作 VisionAIClient，便于统一策略演进。
    权衡：保留 client 注入，避免重复初始化与便于测试。
    """
    if client is None:
        client = get_vision_ai_client(config)
    runtime_context = current_runtime_llm_context()
    request_payload = {
        "model": str(getattr(getattr(client, "config", None), "model", "") or ""),
        "image_path": str(image_path or ""),
        "prompt": str(prompt or ""),
        "system_prompt": str(system_prompt or ""),
        "skip_duplicate_check": bool(skip_duplicate_check),
        "gateway": "vision_validate_image",
    }
    if runtime_context is not None:
        restored = runtime_context.load_committed_call(
            provider="vision_ai",
            request_name="vision_validate_image",
            request_payload=request_payload,
            runtime_identity=runtime_identity,
        )
        if restored is not None:
            return json.loads(str(restored.get("response_text", "{}") or "{}"))
    try:
        raw_result = await _run_hedged_async_request(
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
        response_metadata = _extract_vision_response_metadata(raw_result)
        result = _strip_vision_response_metadata(raw_result)
        if runtime_context is not None:
            runtime_context.persist_success(
                provider="vision_ai",
                request_name="vision_validate_image",
                request_payload=request_payload,
                response_text=dump_runtime_json_text(result),
                response_metadata=_build_vision_runtime_metadata(
                    request_payload=request_payload,
                    response_metadata=response_metadata,
                ),
                runtime_identity=runtime_identity,
            )
        append_vision_ai_call_record(
            request_name="vision_validate_image",
            request_payload=request_payload,
            response_payload=result,
            response_metadata=response_metadata,
            extra={"gateway": "vision_validate_image"},
        )
        return result
    except Exception as error:
        if runtime_context is not None:
            runtime_context.persist_failure(
                provider="vision_ai",
                request_name="vision_validate_image",
                request_payload=request_payload,
                error=error,
                runtime_identity=runtime_identity,
            )
        append_vision_ai_call_record(
            request_name="vision_validate_image",
            request_payload=request_payload,
            response_payload=None,
            response_metadata=None,
            error=str(error),
            extra={"gateway": "vision_validate_image"},
        )
        raise


async def vision_validate_images(
    *,
    image_paths: list[str],
    prompt: str = "",
    system_prompt: Optional[str] = None,
    skip_duplicate_check: bool = False,
    max_batch_size: Optional[int] = None,
    client: Optional[VisionAIClient] = None,
    config: Optional[VisionAIConfig] = None,
    runtime_identity: Optional[Dict[str, Any]] = None,
) -> list[Dict[str, Any]]:
    """
    作用：统一 Vision AI 批量异步调用入口。
    为什么：集中管理批量参数与回退逻辑，避免业务层直接操作客户端细节。
    """
    if client is None:
        client = get_vision_ai_client(config)
    runtime_context = current_runtime_llm_context()
    request_payload = {
        "model": str(getattr(getattr(client, "config", None), "model", "") or ""),
        "image_paths": list(image_paths or []),
        "prompt": str(prompt or ""),
        "system_prompt": str(system_prompt or ""),
        "skip_duplicate_check": bool(skip_duplicate_check),
        "max_batch_size": max_batch_size,
        "gateway": "vision_validate_images",
    }
    if runtime_context is not None:
        restored = runtime_context.load_committed_call(
            provider="vision_ai",
            request_name="vision_validate_images",
            request_payload=request_payload,
            runtime_identity=runtime_identity,
        )
        if restored is not None:
            return json.loads(str(restored.get("response_text", "[]") or "[]"))
    try:
        raw_result = await _run_hedged_async_request(
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
        response_metadata = _extract_vision_response_metadata(raw_result)
        result = _strip_vision_response_metadata(raw_result)
        if runtime_context is not None:
            runtime_context.persist_success(
                provider="vision_ai",
                request_name="vision_validate_images",
                request_payload=request_payload,
                response_text=dump_runtime_json_text(result),
                response_metadata=_build_vision_runtime_metadata(
                    request_payload=request_payload,
                    response_metadata=response_metadata,
                ),
                runtime_identity=runtime_identity,
            )
        append_vision_ai_call_record(
            request_name="vision_validate_images",
            request_payload=request_payload,
            response_payload=result,
            response_metadata=response_metadata,
            extra={"gateway": "vision_validate_images"},
        )
        return result
    except Exception as error:
        if runtime_context is not None:
            runtime_context.persist_failure(
                provider="vision_ai",
                request_name="vision_validate_images",
                request_payload=request_payload,
                error=error,
                runtime_identity=runtime_identity,
            )
        append_vision_ai_call_record(
            request_name="vision_validate_images",
            request_payload=request_payload,
            response_payload=None,
            response_metadata=None,
            error=str(error),
            extra={"gateway": "vision_validate_images"},
        )
        raise


def vision_validate_image_sync(
    *,
    image_path: str,
    prompt: str = "",
    system_prompt: Optional[str] = None,
    skip_duplicate_check: bool = False,
    client: Optional[VisionAIClient] = None,
    config: Optional[VisionAIConfig] = None,
    timeout: Optional[float] = None,
    runtime_identity: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    ensure_sync_bridge_not_in_running_loop("llm_gateway.vision_validate_image_sync")
    """
    作用：统一 Vision AI 同步调用入口（复用后台事件循环）。
    为什么：同步路径更常见于旧代码，统一入口便于后续收敛。
    权衡：同步调用占用线程，但对外 API 行为保持不变。
    """
    if client is None:
        client = get_vision_ai_client(config)
    runtime_context = current_runtime_llm_context()
    request_payload = {
        "model": str(getattr(getattr(client, "config", None), "model", "") or ""),
        "image_path": str(image_path or ""),
        "prompt": str(prompt or ""),
        "system_prompt": str(system_prompt or ""),
        "skip_duplicate_check": bool(skip_duplicate_check),
        "gateway": "vision_validate_image_sync",
    }
    if runtime_context is not None:
        restored = runtime_context.load_committed_call(
            provider="vision_ai",
            request_name="vision_validate_image_sync",
            request_payload=request_payload,
            runtime_identity=runtime_identity,
        )
        if restored is not None:
            return json.loads(str(restored.get("response_text", "{}") or "{}"))
    try:
        raw_result = _VISION_BG_LOOP.submit(
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
        response_metadata = _extract_vision_response_metadata(raw_result)
        result = _strip_vision_response_metadata(raw_result)
        if runtime_context is not None:
            runtime_context.persist_success(
                provider="vision_ai",
                request_name="vision_validate_image_sync",
                request_payload=request_payload,
                response_text=dump_runtime_json_text(result),
                response_metadata=_build_vision_runtime_metadata(
                    request_payload=request_payload,
                    response_metadata=response_metadata,
                ),
                runtime_identity=runtime_identity,
            )
        append_vision_ai_call_record(
            request_name="vision_validate_image_sync",
            request_payload=request_payload,
            response_payload=result,
            response_metadata=response_metadata,
            extra={"gateway": "vision_validate_image_sync"},
        )
        return result
    except Exception as error:
        if runtime_context is not None:
            runtime_context.persist_failure(
                provider="vision_ai",
                request_name="vision_validate_image_sync",
                request_payload=request_payload,
                error=error,
                runtime_identity=runtime_identity,
            )
        append_vision_ai_call_record(
            request_name="vision_validate_image_sync",
            request_payload=request_payload,
            response_payload=None,
            response_metadata=None,
            error=str(error),
            extra={"gateway": "vision_validate_image_sync"},
        )
        raise


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
    runtime_identity: Optional[Dict[str, Any]] = None,
) -> list[Dict[str, Any]]:
    ensure_sync_bridge_not_in_running_loop("llm_gateway.vision_validate_images_sync")
    """
    作用：统一 Vision AI 批量同步调用入口（复用后台事件循环）。
    为什么：兼容同步调用方，减少业务层对异步模型的耦合。
    """
    if client is None:
        client = get_vision_ai_client(config)
    runtime_context = current_runtime_llm_context()
    request_payload = {
        "model": str(getattr(getattr(client, "config", None), "model", "") or ""),
        "image_paths": list(image_paths or []),
        "prompt": str(prompt or ""),
        "system_prompt": str(system_prompt or ""),
        "skip_duplicate_check": bool(skip_duplicate_check),
        "max_batch_size": max_batch_size,
        "gateway": "vision_validate_images_sync",
    }
    if runtime_context is not None:
        restored = runtime_context.load_committed_call(
            provider="vision_ai",
            request_name="vision_validate_images_sync",
            request_payload=request_payload,
            runtime_identity=runtime_identity,
        )
        if restored is not None:
            return json.loads(str(restored.get("response_text", "[]") or "[]"))
    try:
        raw_result = _VISION_BG_LOOP.submit(
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
        response_metadata = _extract_vision_response_metadata(raw_result)
        result = _strip_vision_response_metadata(raw_result)
        if runtime_context is not None:
            runtime_context.persist_success(
                provider="vision_ai",
                request_name="vision_validate_images_sync",
                request_payload=request_payload,
                response_text=dump_runtime_json_text(result),
                response_metadata=_build_vision_runtime_metadata(
                    request_payload=request_payload,
                    response_metadata=response_metadata,
                ),
                runtime_identity=runtime_identity,
            )
        append_vision_ai_call_record(
            request_name="vision_validate_images_sync",
            request_payload=request_payload,
            response_payload=result,
            response_metadata=response_metadata,
            extra={"gateway": "vision_validate_images_sync"},
        )
        return result
    except Exception as error:
        if runtime_context is not None:
            runtime_context.persist_failure(
                provider="vision_ai",
                request_name="vision_validate_images_sync",
                request_payload=request_payload,
                error=error,
                runtime_identity=runtime_identity,
            )
        append_vision_ai_call_record(
            request_name="vision_validate_images_sync",
            request_payload=request_payload,
            response_payload=None,
            response_metadata=None,
            error=str(error),
            extra={"gateway": "vision_validate_images_sync"},
        )
        raise


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
    usage: Dict[str, Any]
    model: str
    cache_hit: bool = False


@dataclass
class _VLCacheEntry:
    """类说明：_VLCacheEntry 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    content: str
    finish_reason: Optional[str]
    usage: Dict[str, Any]
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
_VL_RATE_LIMITER = VLRateLimiter(
    rpm_limit=_env_int("VL_RATE_LIMIT_RPM", 1200),
    tpm_limit=_env_int("VL_RATE_LIMIT_TPM", 1_000_000),
)
_VL_EST_TEXT_CHARS_PER_TOKEN = max(1, _env_int("VL_EST_TEXT_CHARS_PER_TOKEN", 4))
_VL_EST_TOKENS_PER_MEDIA = max(1, _env_int("VL_EST_TOKENS_PER_MEDIA", 1024))
_VL_EST_COMPLETION_RATIO = max(0.0, min(1.0, _env_float("VL_EST_COMPLETION_RATIO", 0.4)))


def _extract_usage_from_response(response: Any) -> Dict[str, Any]:
    """方法说明：_extract_usage_from_response 工具方法。
    执行步骤：
    1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
    2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
    3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
    usage = response.get("usage") if isinstance(response, dict) else getattr(response, "usage", None)
    return normalize_usage_payload(usage)


def _estimate_vl_request_tokens(messages: Any, max_tokens: int) -> int:
    text_chars = 0
    media_items = 0

    def _consume_content(content: Any) -> None:
        nonlocal text_chars, media_items
        if isinstance(content, str):
            text_chars += len(content)
            return
        if isinstance(content, list):
            for item in content:
                if isinstance(item, str):
                    text_chars += len(item)
                    continue
                if not isinstance(item, dict):
                    text_chars += len(str(item))
                    continue
                item_type = str(item.get("type", "") or "").strip().lower()
                if item_type in {"text", "input_text"}:
                    text_chars += len(str(item.get("text", "") or ""))
                    continue
                if item_type in {"image_url", "input_image", "video_url", "input_video"}:
                    media_items += 1
                    continue
                text_chars += len(json.dumps(item, ensure_ascii=False))
            return
        if isinstance(content, dict):
            text_chars += len(json.dumps(content, ensure_ascii=False))
            return
        text_chars += len(str(content))

    if isinstance(messages, list):
        for message in messages:
            if isinstance(message, dict):
                text_chars += len(str(message.get("role", "") or ""))
                _consume_content(message.get("content"))
            else:
                _consume_content(message)
    else:
        _consume_content(messages)

    prompt_tokens = max(1, int(math.ceil(float(text_chars) / float(_VL_EST_TEXT_CHARS_PER_TOKEN))))
    media_tokens = max(0, int(media_items)) * int(_VL_EST_TOKENS_PER_MEDIA)
    completion_budget = max(0, int(max_tokens))
    completion_est = int(round(float(completion_budget) * float(_VL_EST_COMPLETION_RATIO)))
    if completion_budget > 0:
        completion_est = max(1, completion_est)
    return max(1, prompt_tokens + media_tokens + completion_est)


async def _call_vl_api_once(
    *,
    client: Any,
    model: str,
    messages: Any,
    max_tokens: int,
    temperature: float,
    response_format: Optional[Dict[str, Any]] = None,
    timeout: Optional[float] = None,
) -> Tuple[str, Optional[str], Dict[str, Any], str]:
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
    if timeout is not None:
        try:
            resolved_timeout = float(timeout)
        except Exception:
            resolved_timeout = 0.0
        if resolved_timeout > 0:
            kwargs["timeout"] = resolved_timeout

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
    timeout: Optional[float] = None,
    hedge_delay_ms: Optional[int] = None,
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
                cache_hit=True,
            )
        cache_metrics.miss("module2.vl.result_cache")

    async def _do_request() -> VLChatResult:
        acquired = 0
        estimated_tokens = _estimate_vl_request_tokens(messages, max_tokens)
        try:
            rpm_wait_sec, tpm_wait_sec = await _VL_RATE_LIMITER.acquire(estimated_tokens)
            if rpm_wait_sec > 0.0 or tpm_wait_sec > 0.0:
                logger.info(
                    "VL rate limiter waited before request: rpm_wait=%.3fs tpm_wait=%.3fs estimated_tokens=%s",
                    rpm_wait_sec,
                    tpm_wait_sec,
                    estimated_tokens,
                )
            acquired = await _VL_CONCURRENCY.acquire(1)
            content, finish_reason, usage, model_name = await _call_vl_api_once(
                client=client,
                model=model,
                messages=messages,
                max_tokens=max_tokens,
                temperature=temperature,
                response_format=response_format,
                timeout=timeout,
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
                cache_hit=False,
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
        resolved_hedge_delay_ms = _VL_HEDGE_DELAY_MS
        if hedge_delay_ms is not None:
            try:
                resolved_hedge_delay_ms = max(0, int(hedge_delay_ms))
            except Exception:
                resolved_hedge_delay_ms = _VL_HEDGE_DELAY_MS
        return await _run_hedged_async_request(
            request_name="vl_chat_completion",
            enabled=_VL_HEDGE_ENABLED,
            delay_ms=resolved_hedge_delay_ms,
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
                timeout=payload.get("timeout"),
                hedge_delay_ms=payload.get("hedge_delay_ms"),
            )

    await asyncio.gather(*[_run_single(index, payload) for index, payload in enumerate(requests)])

    finalized_results: list[VLChatResult] = []
    for index, item in enumerate(ordered_results):
        if item is None:
            raise RuntimeError(f"vl_chat_completions result missing at index {index}")
        finalized_results.append(item)
    return finalized_results
