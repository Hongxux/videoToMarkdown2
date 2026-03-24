"""DeepSeek 客户端实现（含重试、缓存、并发与去重优化）。"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import logging
import os
import random
import re
import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple, TypeVar

import httpx

from .client import LLMClient, LLMConfig, LLMResponse
from .error_utils import format_provider_error
from services.python_grpc.src.common.utils.hash_policy import fast_hasher
from services.python_grpc.src.common.utils.runtime_llm_context import (
    build_runtime_llm_request_payload,
    current_runtime_llm_context,
    dump_runtime_json_text,
)
from services.python_grpc.src.common.utils.deepseek_model_router import resolve_deepseek_model
from services.python_grpc.src.content_pipeline.common.utils import json_payload_repair

logger = logging.getLogger(__name__)
_T = TypeVar("_T")


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    value = str(raw).strip().lower()
    if value in {"1", "true", "yes", "y", "on"}:
        return True
    if value in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return int(default)
    try:
        return int(str(raw).strip())
    except Exception:
        return int(default)


def _supports_http2_transport() -> bool:
    """优先走自动探测；也允许通过环境变量强制开关。"""
    if os.getenv("TRANSCRIPT_LLM_HTTP2_ENABLED") is not None:
        return _env_bool("TRANSCRIPT_LLM_HTTP2_ENABLED", True)
    return importlib.util.find_spec("h2") is not None


_CACHE_ENABLED = _env_bool("TRANSCRIPT_LLM_CACHE_ENABLED", True)
_CACHE_MAX_ITEMS = max(1, _env_int("TRANSCRIPT_LLM_CACHE_MAX_ITEMS", 512))
_CACHE_TTL_SECONDS = max(1, _env_int("TRANSCRIPT_LLM_CACHE_TTL_SECONDS", 1800))
_CACHE_MAX_PROMPT_CHARS = max(0, _env_int("TRANSCRIPT_LLM_CACHE_MAX_PROMPT_CHARS", 20000))
_CACHE_MAX_RESPONSE_CHARS = max(0, _env_int("TRANSCRIPT_LLM_CACHE_MAX_RESPONSE_CHARS", 20000))

_INFLIGHT_DEDUP_ENABLED = _env_bool("TRANSCRIPT_LLM_INFLIGHT_DEDUP_ENABLED", True)
_MAX_CONCURRENCY = max(1, _env_int("TRANSCRIPT_LLM_MAX_CONCURRENCY", 10))

_RETRY_ATTEMPTS = max(1, _env_int("TRANSCRIPT_LLM_RETRY_ATTEMPTS", 3))
_RETRY_BACKOFF_MIN_MS = max(50, _env_int("TRANSCRIPT_LLM_RETRY_BACKOFF_MIN_MS", 300))
_RETRY_BACKOFF_MAX_MS = max(
    _RETRY_BACKOFF_MIN_MS,
    _env_int("TRANSCRIPT_LLM_RETRY_BACKOFF_MAX_MS", 3000),
)


@dataclass
class _CacheEntry:
    content: str
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    model: str
    raw_response: Optional[Dict[str, Any]]
    expires_at: float


class _AsyncLRUTTLCache:
    """轻量异步 LRU+TTL 缓存，仅用于单进程内复用。"""

    def __init__(self, max_items: int, ttl_seconds: int):
        self._max_items = max(1, int(max_items))
        self._ttl_seconds = max(1, int(ttl_seconds))
        self._items: "OrderedDict[str, _CacheEntry]" = OrderedDict()
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Optional[_CacheEntry]:
        now = time.time()
        async with self._lock:
            entry = self._items.get(key)
            if entry is None:
                return None
            if entry.expires_at <= now:
                self._items.pop(key, None)
                return None
            self._items.move_to_end(key, last=True)
            return entry

    async def set(self, key: str, entry: _CacheEntry) -> None:
        async with self._lock:
            self._items[key] = entry
            self._items.move_to_end(key, last=True)
            while len(self._items) > self._max_items:
                self._items.popitem(last=False)

    def ttl_seconds(self) -> int:
        return self._ttl_seconds


class _AsyncInFlightDeduper:
    """对相同 key 的并发请求做 singleflight 合并。"""

    def __init__(self):
        self._lock = asyncio.Lock()
        self._inflight: Dict[str, asyncio.Future] = {}

    @staticmethod
    def _drain_future_exception(fut: asyncio.Future) -> None:
        """避免 leader 失败且无 follower 时触发 Future 未读取异常告警。"""
        try:
            fut.exception()
        except asyncio.CancelledError:
            return
        except Exception:
            return

    async def run(self, key: str, fn: Callable[[], Awaitable[_T]]) -> _T:
        loop = asyncio.get_running_loop()
        async with self._lock:
            fut = self._inflight.get(key)
            if fut is None:
                fut = loop.create_future()
                fut.add_done_callback(self._drain_future_exception)
                self._inflight[key] = fut
                leader = True
            else:
                leader = False

        if not leader:
            return await fut

        try:
            result = await fn()
            fut.set_result(result)
            return result
        except Exception as error:
            fut.set_exception(error)
            raise
        finally:
            async with self._lock:
                self._inflight.pop(key, None)


class DeepSeekClient(LLMClient):
    def __init__(self, config: LLMConfig):
        config.model = resolve_deepseek_model(config.model, default_model="deepseek-chat")
        super().__init__(config)
        self._client: Optional[httpx.AsyncClient] = None
        self._client_lock = asyncio.Lock()
        self._client_loop_id: Optional[int] = None
        self._http2_enabled = _supports_http2_transport()

        self._request_semaphore = asyncio.Semaphore(_MAX_CONCURRENCY)
        self._cache = _AsyncLRUTTLCache(_CACHE_MAX_ITEMS, _CACHE_TTL_SECONDS)
        self._deduper = _AsyncInFlightDeduper()

    async def _get_client(self) -> httpx.AsyncClient:
        loop_id = id(asyncio.get_running_loop())
        if self._client is not None and self._client_loop_id == loop_id:
            return self._client

        async with self._client_lock:
            if self._client is not None and self._client_loop_id == loop_id:
                return self._client

            # 同一 client 若跨 event loop 复用，会导致底层连接对象异常；这里直接重建。
            if self._client is not None and self._client_loop_id != loop_id:
                self._client = None
                self._client_loop_id = None

            max_connections = max(20, _MAX_CONCURRENCY * 2)
            max_keepalive = max(10, _MAX_CONCURRENCY)
            timeout = max(1.0, float(self.config.timeout))
            connect_timeout = min(10.0, timeout)

            client_kwargs = {
                "base_url": self.config.base_url,
                "headers": {
                    "Authorization": f"Bearer {self.config.api_key}",
                    "Content-Type": "application/json",
                    "Accept-Encoding": "gzip, br",
                },
                "timeout": httpx.Timeout(timeout, connect=connect_timeout),
                "limits": httpx.Limits(
                    max_connections=max_connections,
                    max_keepalive_connections=max_keepalive,
                    keepalive_expiry=30.0,
                ),
                "http2": self._http2_enabled,
            }

            try:
                self._client = httpx.AsyncClient(**client_kwargs)
            except Exception as error:
                error_text = str(error).lower()
                if self._http2_enabled and "h2" in error_text:
                    logger.warning("DeepSeek HTTP/2 unavailable, fallback to HTTP/1.1")
                    self._http2_enabled = False
                    client_kwargs["http2"] = False
                    self._client = httpx.AsyncClient(**client_kwargs)
                else:
                    raise

            self._client_loop_id = loop_id
            return self._client

    async def close(self):
        if self._client is not None:
            await self._client.aclose()
            self._client = None
            self._client_loop_id = None

    def _is_retryable_http_status(self, status_code: int) -> bool:
        return int(status_code) in {408, 409, 425, 429, 500, 502, 503, 504}

    def _retry_delay_seconds(self, attempt_index: int) -> float:
        base_ms = min(_RETRY_BACKOFF_MAX_MS, _RETRY_BACKOFF_MIN_MS * (2 ** attempt_index))
        jitter_ms = int(base_ms * 0.2)
        if jitter_ms > 0:
            base_ms += random.randint(0, jitter_ms)
        return max(0.05, float(base_ms) / 1000.0)

    async def _post_with_retry(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        last_error: Optional[Exception] = None
        for attempt in range(_RETRY_ATTEMPTS):
            try:
                client = await self._get_client()
                response = await client.post("/chat/completions", json=payload)
                response.raise_for_status()
                data = response.json()
                if not isinstance(data, dict):
                    raise RuntimeError("DeepSeek response JSON root must be an object")
                return data
            except httpx.HTTPStatusError as error:
                status_code = int(getattr(error.response, "status_code", 0))
                retryable = self._is_retryable_http_status(status_code)
                last_error = error
                if (not retryable) or attempt >= _RETRY_ATTEMPTS - 1:
                    raise
                await asyncio.sleep(self._retry_delay_seconds(attempt))
            except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError) as error:
                last_error = error
                if attempt >= _RETRY_ATTEMPTS - 1:
                    raise
                await asyncio.sleep(self._retry_delay_seconds(attempt))

        if last_error is not None:
            raise last_error
        raise RuntimeError("DeepSeek request failed with unknown error")

    def _cacheable(self, prompt: str, system_prompt: Optional[str]) -> bool:
        if not _CACHE_ENABLED:
            return False
        total_chars = len(prompt or "") + len(system_prompt or "")
        return total_chars <= _CACHE_MAX_PROMPT_CHARS

    def _response_cacheable(self, content: str) -> bool:
        return isinstance(content, str) and len(content) <= _CACHE_MAX_RESPONSE_CHARS

    def _kwargs_signature(self, kwargs: Dict[str, Any]) -> str:
        try:
            return json.dumps(kwargs, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        except Exception:
            return repr(kwargs)

    def _build_cache_key(
        self,
        *,
        kind: str,
        prompt: str,
        system_prompt: Optional[str],
        temperature: float,
        kwargs: Dict[str, Any],
    ) -> str:
        hasher = fast_hasher()
        hasher.update(str(kind).encode("utf-8"))
        hasher.update(b"\0")
        hasher.update(str(self.config.base_url).encode("utf-8"))
        hasher.update(b"\0")
        hasher.update(str(self.config.model).encode("utf-8"))
        hasher.update(b"\0")
        hasher.update(str(float(temperature)).encode("utf-8"))
        hasher.update(b"\0")
        hasher.update(self._kwargs_signature(kwargs).encode("utf-8"))
        hasher.update(b"\0")
        hasher.update((system_prompt or "").encode("utf-8"))
        hasher.update(b"\0")
        hasher.update((prompt or "").encode("utf-8"))
        return hasher.hexdigest()

    def _cache_entry_to_response(self, entry: _CacheEntry) -> LLMResponse:
        return LLMResponse(
            content=entry.content,
            prompt_tokens=int(entry.prompt_tokens),
            completion_tokens=int(entry.completion_tokens),
            total_tokens=int(entry.total_tokens),
            model=str(entry.model or self.config.model),
            latency_ms=0.0,
            raw_response=dict(entry.raw_response) if isinstance(entry.raw_response, dict) else entry.raw_response,
        )

    async def complete(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        temperature: Optional[float] = None,
        **kwargs,
    ) -> LLMResponse:
        skip_runtime_capture = bool(kwargs.pop("__runtime_skip_capture__", False))
        runtime_identity_override = kwargs.pop("__runtime_identity__", None)
        runtime_metadata = kwargs.pop("__runtime_metadata__", None)
        effective_temperature = (
            float(temperature) if temperature is not None else float(self.config.temperature)
        )
        runtime_context = None if skip_runtime_capture else current_runtime_llm_context()
        request_payload = build_runtime_llm_request_payload(
            model=self.config.model,
            prompt=prompt,
            system_prompt=system_prompt or "",
            kwargs={
                "temperature": effective_temperature,
                "max_tokens": self.config.max_tokens,
                **kwargs,
            },
        )
        runtime_identity = {"step_name": "complete_text", "request_name": "complete_text"}
        if isinstance(runtime_identity_override, dict):
            runtime_identity.update(runtime_identity_override)
        restored = None
        if runtime_context is not None:
            restored = runtime_context.load_committed_call(
                provider="deepseek",
                request_name="complete_text",
                request_payload=request_payload,
                runtime_identity=runtime_identity,
            )
        if restored is not None:
            restored_metadata = dict(restored.get("response_metadata", {}) or {})
            return LLMResponse(
                content=str(restored.get("response_text", "") or ""),
                prompt_tokens=int(restored_metadata.get("prompt_tokens", 0) or 0),
                completion_tokens=int(restored_metadata.get("completion_tokens", 0) or 0),
                total_tokens=int(restored_metadata.get("total_tokens", 0) or 0),
                model=str(restored_metadata.get("model", self.config.model) or self.config.model),
                latency_ms=float(restored_metadata.get("latency_ms", 0.0) or 0.0),
                raw_response=restored_metadata.get("raw_response"),
            )
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        payload = {
            "model": self.config.model,
            "messages": messages,
            "temperature": effective_temperature,
            "max_tokens": self.config.max_tokens,
            **kwargs,
        }

        cache_key: Optional[str] = None
        if self._cacheable(prompt, system_prompt):
            cache_key = self._build_cache_key(
                kind="text",
                prompt=prompt,
                system_prompt=system_prompt,
                temperature=effective_temperature,
                kwargs={k: v for k, v in kwargs.items()},
            )
            cached = await self._cache.get(cache_key)
            if cached is not None:
                return self._cache_entry_to_response(cached)

        async def _do_request() -> LLMResponse:
            async with self._request_semaphore:
                start_time = datetime.now()
                try:
                    data = await self._post_with_retry(payload)
                except Exception as error:
                    raise RuntimeError(
                        f"DeepSeek API error: {format_provider_error(error, base_url=self.config.base_url, model=self.config.model, timeout=self.config.timeout)}"
                    ) from error

                try:
                    content = str(data["choices"][0]["message"]["content"])
                except Exception as error:
                    raise RuntimeError(f"DeepSeek API error: invalid response schema: {error}") from error

                usage = data.get("usage", {}) if isinstance(data, dict) else {}
                prompt_tokens = int(usage.get("prompt_tokens", 0) or 0)
                completion_tokens = int(usage.get("completion_tokens", 0) or 0)
                total_tokens = int(
                    usage.get("total_tokens", prompt_tokens + completion_tokens) or 0
                )
                model = str(data.get("model") or self.config.model)

                latency_ms = (datetime.now() - start_time).total_seconds() * 1000

                self._last_prompt = prompt
                self._last_response = content
                self._last_token_count = total_tokens

                response_obj = LLMResponse(
                    content=content,
                    prompt_tokens=prompt_tokens,
                    completion_tokens=completion_tokens,
                    total_tokens=total_tokens,
                    model=model,
                    latency_ms=latency_ms,
                    raw_response=data,
                )

                if cache_key and self._response_cacheable(content):
                    now = time.time()
                    await self._cache.set(
                        cache_key,
                        _CacheEntry(
                            content=content,
                            prompt_tokens=prompt_tokens,
                            completion_tokens=completion_tokens,
                            total_tokens=total_tokens,
                            model=model,
                            raw_response=data,
                            expires_at=now + float(self._cache.ttl_seconds()),
                        ),
                    )
                if runtime_context is not None:
                    runtime_context.persist_success(
                        provider="deepseek",
                        request_name="complete_text",
                        request_payload=request_payload,
                        response_text=content,
                        response_metadata={
                            "prompt_tokens": prompt_tokens,
                            "completion_tokens": completion_tokens,
                            "total_tokens": total_tokens,
                            "model": model,
                            "latency_ms": latency_ms,
                            "raw_response": data,
                        },
                        runtime_identity=runtime_identity,
                        metadata=runtime_metadata if isinstance(runtime_metadata, dict) else None,
                    )
                return response_obj

        if cache_key and _INFLIGHT_DEDUP_ENABLED:
            try:
                return await self._deduper.run(cache_key, _do_request)
            except Exception as error:
                if runtime_context is not None:
                    runtime_context.persist_failure(
                        provider="deepseek",
                        request_name="complete_text",
                        request_payload=request_payload,
                        error=error,
                        runtime_identity=runtime_identity,
                        metadata=runtime_metadata if isinstance(runtime_metadata, dict) else None,
                    )
                raise
        try:
            return await _do_request()
        except Exception as error:
            if runtime_context is not None:
                runtime_context.persist_failure(
                    provider="deepseek",
                    request_name="complete_text",
                    request_payload=request_payload,
                    error=error,
                    runtime_identity=runtime_identity,
                    metadata=runtime_metadata if isinstance(runtime_metadata, dict) else None,
                )
            raise

    async def complete_json(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        **kwargs,
    ) -> Tuple[Dict, LLMResponse]:
        json_system = (system_prompt or "") + "\n\n请确保输出为有效的 JSON 格式。"
        call_kwargs = dict(kwargs)
        runtime_identity_override = call_kwargs.pop("__runtime_identity__", None)
        runtime_metadata = call_kwargs.pop("__runtime_metadata__", None)
        call_kwargs.setdefault("response_format", {"type": "json_object"})
        call_kwargs["__runtime_skip_capture__"] = True
        runtime_context = current_runtime_llm_context()
        request_payload = build_runtime_llm_request_payload(
            model=self.config.model,
            prompt=prompt,
            system_prompt=json_system,
            kwargs=call_kwargs,
        )
        runtime_identity = {"step_name": "complete_json", "request_name": "complete_json"}
        if isinstance(runtime_identity_override, dict):
            runtime_identity.update(runtime_identity_override)
        if runtime_context is not None:
            restored = runtime_context.load_committed_call(
                provider="deepseek",
                request_name="complete_json",
                request_payload=request_payload,
                runtime_identity=runtime_identity,
            )
            if restored is not None:
                response_text = str(restored.get("response_text", "") or "")
                parsed = self._load_json_with_repair(response_text)
                if not isinstance(parsed, dict):
                    raise ValueError("restored JSON root must be an object")
                restored_metadata = dict(restored.get("response_metadata", {}) or {})
                response = LLMResponse(
                    content=response_text,
                    prompt_tokens=int(restored_metadata.get("prompt_tokens", 0) or 0),
                    completion_tokens=int(restored_metadata.get("completion_tokens", 0) or 0),
                    total_tokens=int(restored_metadata.get("total_tokens", 0) or 0),
                    model=str(restored_metadata.get("model", self.config.model) or self.config.model),
                    latency_ms=float(restored_metadata.get("latency_ms", 0.0) or 0.0),
                    raw_response=restored_metadata.get("raw_response"),
                )
                runtime_context.emit_llm_call_event(
                    provider="deepseek",
                    request_name="complete_json",
                    runtime_identity=runtime_identity,
                    metadata=runtime_metadata if isinstance(runtime_metadata, dict) else None,
                    runtime_restored=True,
                )
                return parsed, response

        prompt_to_send = prompt
        last_decode_error: Optional[json.JSONDecodeError] = None
        last_content = ""

        for attempt in range(2):
            response = await self._complete_for_json(
                prompt=prompt_to_send,
                system_prompt=json_system,
                kwargs=call_kwargs,
            )
            content = self._extract_json_content(response.content)
            last_content = content

            try:
                parsed = self._load_json_with_repair(content)
                if not isinstance(parsed, dict):
                    raise ValueError("JSON root must be an object")
                if runtime_context is not None:
                    runtime_context.persist_success(
                        provider="deepseek",
                        request_name="complete_json",
                        request_payload=request_payload,
                        response_text=dump_runtime_json_text(parsed),
                        response_metadata={
                            "prompt_tokens": response.prompt_tokens,
                            "completion_tokens": response.completion_tokens,
                            "total_tokens": response.total_tokens,
                            "model": response.model,
                            "latency_ms": response.latency_ms,
                            "raw_response": response.raw_response,
                        },
                        runtime_identity=runtime_identity,
                        metadata=runtime_metadata if isinstance(runtime_metadata, dict) else None,
                    )
                    runtime_context.emit_llm_call_event(
                        provider="deepseek",
                        request_name="complete_json",
                        runtime_identity=runtime_identity,
                        metadata=runtime_metadata if isinstance(runtime_metadata, dict) else None,
                        runtime_restored=False,
                    )
                return parsed, response
            except json.JSONDecodeError as error:
                last_decode_error = error
                if attempt == 0:
                    prompt_to_send = (
                        f"{prompt}\n\n"
                        "上一轮输出不是有效 JSON，请重新输出完整且可解析的 JSON。"
                        "不要输出解释文本，也不要使用 Markdown code block。"
                    )

        error_text = str(last_decode_error) if last_decode_error else "unknown parse error"
        parse_error = ValueError(f"Failed to parse JSON response: {error_text}\nContent: {last_content[:500]}")
        if runtime_context is not None:
            runtime_context.persist_failure(
                provider="deepseek",
                request_name="complete_json",
                request_payload=request_payload,
                error=parse_error,
                runtime_identity=runtime_identity,
                metadata=runtime_metadata if isinstance(runtime_metadata, dict) else None,
            )
        raise parse_error

    async def _complete_for_json(
        self,
        prompt: str,
        system_prompt: str,
        kwargs: Dict[str, Any],
    ) -> LLMResponse:
        try:
            return await self.complete(
                prompt=prompt,
                system_prompt=system_prompt,
                **kwargs,
            )
        except RuntimeError as error:
            error_text = str(error).lower()
            if "response_format" in error_text and ("400" in error_text or "invalid" in error_text):
                fallback_kwargs = dict(kwargs)
                fallback_kwargs.pop("response_format", None)
                return await self.complete(
                    prompt=prompt,
                    system_prompt=system_prompt,
                    **fallback_kwargs,
                )
            raise

    def _extract_json_content(self, content: str) -> str:
        text = content.strip()
        if "```json" in text:
            start = text.find("```json") + 7
            end = text.find("```", start)
            if end != -1:
                text = text[start:end].strip()
        elif "```" in text:
            start = text.find("```") + 3
            end = text.find("```", start)
            if end != -1:
                text = text[start:end].strip()

        object_start = text.find("{")
        array_start = text.find("[")
        starts = [position for position in [object_start, array_start] if position != -1]
        if not starts:
            return text

        start = min(starts)
        end = max(text.rfind("}"), text.rfind("]"))
        if end > start:
            return text[start : end + 1].strip()
        return text[start:].strip()

    def _load_json_with_repair(self, content: str) -> Any:
        parsed, last_error = json_payload_repair.parse_json_payload(
            content,
            extra_repairers=[self._repair_json],
        )
        if parsed is not None:
            return parsed

        if isinstance(last_error, json.JSONDecodeError):
            raise last_error
        if last_error is not None:
            raise json.JSONDecodeError(str(last_error), content, 0)
        raise json.JSONDecodeError("Invalid JSON", content, 0)

    def _repair_json(self, content: str, error: Optional[json.JSONDecodeError] = None) -> str:
        repaired = str(content or "").strip()
        repaired = re.sub(r"}\s*{", "},{", repaired)
        repaired = re.sub(r"]\s*\[", "],[", repaired)
        repaired = re.sub(
            r'("(?:(?:\\.)|[^"\\])*")\s+(?="[^"\\]+"\s*:)',
            r"\1, ",
            repaired,
        )

        if error is not None and "Expecting ',' delimiter" in error.msg and 0 <= error.pos < len(repaired):
            token = repaired[error.pos]
            if token in {'"', "{", "["}:
                repaired = repaired[: error.pos] + "," + repaired[error.pos :]

        repaired = json_payload_repair.normalize_jsonish_text(repaired)
        repaired = json_payload_repair.repair_unclosed_json(repaired)
        return repaired

    async def complete_batch(
        self,
        prompts: list[str],
        system_prompt: Optional[str] = None,
        max_concurrency: int = 5,
    ) -> list[LLMResponse]:
        semaphore = asyncio.Semaphore(max(1, int(max_concurrency)))

        async def limited_complete(single_prompt: str) -> LLMResponse:
            async with semaphore:
                return await self.complete(single_prompt, system_prompt)

        tasks = [limited_complete(single_prompt) for single_prompt in prompts]
        return await asyncio.gather(*tasks)
