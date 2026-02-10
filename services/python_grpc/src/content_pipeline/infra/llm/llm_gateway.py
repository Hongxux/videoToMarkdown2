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
import hashlib
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple
import threading

from services.python_grpc.src.content_pipeline.infra.runtime import cache_metrics
from services.python_grpc.src.content_pipeline.infra.llm.llm_client import (
    LLMClient,
    AdaptiveConcurrencyLimiter,
    _AsyncLRUTTLCache,
    _AsyncInFlightDeduper,
)
from services.python_grpc.src.content_pipeline.infra.llm.deepseek_audit import append_deepseek_call_record
from services.python_grpc.src.content_pipeline.infra.llm.vision_ai_client import VisionAIClient, VisionAIConfig, get_vision_ai_client

logger = logging.getLogger(__name__)


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
    key = _build_deepseek_client_key(api_key or "", base_url, model, temperature)
    with _DEEPSEEK_LOCK:
        client = _DEEPSEEK_CLIENTS.get(key)
        if client is not None:
            return client
        client = LLMClient(
            api_key=api_key,
            base_url=base_url,
            model=model,
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
    if client is None:
        client = get_deepseek_client(
            api_key=api_key,
            base_url=base_url,
            model=model,
            temperature=temperature,
            enable_logprobs=enable_logprobs,
            cache_enabled=cache_enabled,
            inflight_dedup_enabled=inflight_dedup_enabled,
        )
    output_text = ""
    metadata = None
    logprobs = None
    error_text = ""
    try:
        output_text, metadata, logprobs = await client.complete_text(
            prompt=prompt,
            system_message=system_message,
            need_logprobs=need_logprobs,
        )
        return output_text, metadata, logprobs
    except Exception as exc:
        error_text = str(exc)
        raise
    finally:
        try:
            append_deepseek_call_record(
                prompt=prompt,
                system_message=str(system_message or ""),
                model=model,
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
    if client is None:
        client = get_deepseek_client(
            api_key=api_key,
            base_url=base_url,
            model=model,
            temperature=temperature,
            enable_logprobs=enable_logprobs,
            cache_enabled=cache_enabled,
            inflight_dedup_enabled=inflight_dedup_enabled,
        )
    return await client.complete_json(
        prompt=prompt,
        system_message=system_message,
        need_logprobs=need_logprobs,
        max_tokens=max_tokens,
    )


# =============================================================================
# Vision AI 统一入口
# =============================================================================


async def vision_validate_image(
    *,
    image_path: str,
    prompt: str,
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
    return await client.validate_image(
        image_path=image_path,
        prompt=prompt,
        skip_duplicate_check=skip_duplicate_check,
    )


def vision_validate_image_sync(
    *,
    image_path: str,
    prompt: str,
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
    return client.validate_image_sync(
        image_path=image_path,
        prompt=prompt,
        skip_duplicate_check=skip_duplicate_check,
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
        except Exception as exc:
            err_str = str(exc)
            is_rate_limit = "429" in err_str or "rate" in err_str.lower()
            await _VL_CONCURRENCY.record_failure(is_rate_limit=is_rate_limit)
            raise
        finally:
            if acquired:
                await _VL_CONCURRENCY.release(acquired)

    if cache_key and _VL_INFLIGHT_DEDUP_ENABLED:
        return await _VL_DEDUPER.run(cache_key, _do_request)
    return await _do_request()
