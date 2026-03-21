"""
运行态 LLM 调用上下文：
1) 用 ContextVar 把“当前阶段是否开启 llm_call 粒度恢复”从业务代码中剥离出来。
2) 对外只暴露“激活上下文 / 读取已提交调用 / 记录成功或失败调用”三类能力。
3) chunk_id 默认按输入指纹稳定生成，必要时允许上层透传更可读的标识。
"""

from __future__ import annotations

import json
import logging
import re
import time
from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass
from threading import Lock
from types import SimpleNamespace
from typing import Any, Callable, Dict, Iterator, Optional

from services.python_grpc.src.common.utils.runtime_recovery_store import (
    RuntimeRecoveryStore,
    build_runtime_payload_fingerprint,
)

logger = logging.getLogger(__name__)


_ACTIVE_RUNTIME_LLM_CONTEXT: ContextVar[Optional["RuntimeLLMContext"]] = ContextVar(
    "ACTIVE_RUNTIME_LLM_CONTEXT",
    default=None,
)

LLMEventEmitter = Callable[[Dict[str, Any]], None]


def _stable_json_text(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _sanitize_scope_id(value: str, *, fallback: str, max_length: int = 32) -> str:
    normalized = re.sub(r"[^0-9A-Za-z._-]+", "_", str(value or "").strip())
    normalized = normalized.strip("._")
    if max_length > 0 and len(normalized) > max_length:
        normalized = normalized[:max_length].rstrip("._")
    return normalized or fallback


@dataclass
class RuntimeLLMContext:
    stage: str
    output_dir: str
    task_id: str = ""
    storage_key: str = ""
    normalized_video_key: str = ""
    storage_backend: str = "sqlite"
    llm_event_emitter: Optional[LLMEventEmitter] = None

    def __post_init__(self) -> None:
        self.store = RuntimeRecoveryStore(
            output_dir=self.output_dir,
            task_id=self.task_id,
            storage_key=self.storage_key,
            normalized_video_key=self.normalized_video_key,
        )
        self._prefetched_llm_cache: Dict[tuple[str, str, str, str], Dict[str, Any]] = {}
        self._stage_scope_dispatch_summary: Dict[str, Any] = {}
        # 为什么这样做：任务级 provider 路由属于当前任务的运行时决策，放在同一个上下文里最容易保证并发子调用看到一致状态。
        self._llm_provider_route_lock = Lock()
        self._llm_provider_routes: Dict[str, Dict[str, Any]] = {}
        self._prime_stage_entry_dispatch()

    def get_llm_provider_route(self, route_name: str) -> Dict[str, Any]:
        self._ensure_llm_provider_route_state()
        normalized_route_name = str(route_name or "").strip()
        if not normalized_route_name:
            return {}
        with self._llm_provider_route_lock:
            current = self._llm_provider_routes.get(normalized_route_name)
            return dict(current or {})

    def _ensure_llm_provider_route_state(self) -> None:
        if not hasattr(self, "_llm_provider_route_lock"):
            self._llm_provider_route_lock = Lock()
        if not hasattr(self, "_llm_provider_routes"):
            self._llm_provider_routes = {}

    def pin_llm_provider_route(
        self,
        *,
        route_name: str,
        provider: str,
        source_provider: str = "",
        reason: str = "",
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        self._ensure_llm_provider_route_state()
        normalized_route_name = str(route_name or "").strip()
        normalized_provider = str(provider or "").strip()
        if not normalized_route_name or not normalized_provider:
            return {}

        extra_payload = dict(extra or {})
        previous_failures = extra_payload.pop("previous_failures", None)
        now_ms = int(time.time() * 1000)

        with self._llm_provider_route_lock:
            current = dict(self._llm_provider_routes.get(normalized_route_name, {}) or {})
            merged_previous_failures: list[Dict[str, Any]] = [
                dict(item)
                for item in list(current.get("previous_failures", []) or [])
                if isinstance(item, dict)
            ]
            if isinstance(previous_failures, list):
                merged_previous_failures.extend(
                    dict(item)
                    for item in previous_failures
                    if isinstance(item, dict)
                )

            route_payload: Dict[str, Any] = {
                "route_name": normalized_route_name,
                "provider": normalized_provider,
                "source_provider": str(
                    source_provider
                    or current.get("source_provider", "")
                    or ""
                ).strip(),
                "reason": str(reason or current.get("reason", "") or "").strip(),
                "pinned_at_ms": int(current.get("pinned_at_ms", now_ms) or now_ms),
                "updated_at_ms": now_ms,
                "previous_failures": merged_previous_failures,
            }
            route_payload.update(extra_payload)
            self._llm_provider_routes[normalized_route_name] = dict(route_payload)
            return dict(route_payload)

    def _prime_stage_entry_dispatch(self) -> None:
        try:
            prefetch_result = self.store.prefetch_restorable_llm_scope_cache(stage=self.stage)
            self._prefetched_llm_cache = dict(prefetch_result.get("cache", {}) or {})
            self._stage_scope_dispatch_summary = dict(prefetch_result.get("summary", {}) or {})
        except Exception as error:
            logger.warning("RuntimeLLMContext stage prefetch failed: stage=%s error=%s", self.stage, error)
            self._prefetched_llm_cache = {}
            self._stage_scope_dispatch_summary = {}

    def _build_scope(
        self,
        *,
        provider: str,
        request_name: str,
        request_payload: Dict[str, Any],
        runtime_identity: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, str]:
        identity = dict(runtime_identity or {})
        fingerprint_payload = {
            "provider": str(provider or "").strip(),
            "request_name": str(request_name or "").strip(),
            "request": request_payload,
        }
        input_fingerprint = build_runtime_payload_fingerprint(fingerprint_payload)
        logical_unit = (
            identity.get("chunk_id")
            or identity.get("unit_id")
            or identity.get("window_id")
            or identity.get("batch_id")
            or identity.get("request_id")
            or f"{request_name}_{input_fingerprint[:12]}"
        )
        step_name = str(identity.get("step_name", request_name) or request_name).strip() or request_name
        unit_id = str(identity.get("unit_id", logical_unit) or logical_unit).strip() or logical_unit
        fallback_scope = _sanitize_scope_id(
            str(request_name or "request"),
            fallback="request",
            max_length=16,
        )
        chunk_id = _sanitize_scope_id(
            str(logical_unit),
            fallback=f"{fallback_scope}_{input_fingerprint[:8]}",
            max_length=32,
        )
        llm_call_id = _sanitize_scope_id(
            str(
                identity.get("llm_call_id")
                or self.store.build_llm_call_id(
                    step_name=step_name,
                    unit_id=unit_id,
                    input_fingerprint=input_fingerprint,
                )
            ),
            fallback=f"{provider}_{request_name}",
            max_length=96,
        )
        return {
            "input_fingerprint": input_fingerprint,
            "chunk_id": chunk_id,
            "llm_call_id": llm_call_id,
        }

    def _plan_scope(
        self,
        *,
        provider: str,
        request_name: str,
        request_payload: Dict[str, Any],
        runtime_identity: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, str]:
        scope = self._build_scope(
            provider=provider,
            request_name=request_name,
            request_payload=request_payload,
            runtime_identity=runtime_identity,
        )
        identity = dict(runtime_identity or {})
        metadata_payload = {
            "provider": str(provider or "").strip(),
            "request_name": str(request_name or "").strip(),
            "storage_backend": str(self.storage_backend or "sqlite").strip().lower() or "sqlite",
        }
        if isinstance(metadata, dict):
            metadata_payload.update(metadata)
        for field_name in (
            "stage_step",
            "step_name",
            "scope_variant",
            "unit_id",
            "window_id",
            "window_index",
            "batch_id",
            "segment_id",
            "segment_index",
            "total_segments",
            "analysis_mode",
            "clip_id",
            "screenshot_id",
            "semantic_unit_id",
            "section_id",
            "wave_id",
            "substage_name",
            "substage_scope_ref",
            "dependency_fingerprints",
            "request_scope_ids",
        ):
            field_value = identity.get(field_name)
            if field_value in (None, "", [], {}):
                continue
            metadata_payload.setdefault(field_name, field_value)
        if not str(metadata_payload.get("stage_step", "") or "").strip():
            metadata_payload["stage_step"] = str(
                metadata_payload.get("step_name")
                or identity.get("step_name")
                or request_name
            ).strip()
        self.store.plan_llm_call_scope(
            stage=self.stage,
            chunk_id=scope["chunk_id"],
            llm_call_id=scope["llm_call_id"],
            input_fingerprint=scope["input_fingerprint"],
            request_payload=self._prepare_request_payload_for_persistence(
                request_payload,
                metadata=metadata_payload,
                runtime_identity=identity,
            ),
            metadata=metadata_payload,
            scope_variant=str(metadata_payload.get("scope_variant", "") or ""),
            dependency_fingerprints=metadata_payload.get("dependency_fingerprints"),
            extra_payload={
                "provider": metadata_payload.get("provider", ""),
                "request_name": metadata_payload.get("request_name", ""),
                "stage_step": metadata_payload.get("stage_step", ""),
                "unit_id": metadata_payload.get("unit_id", identity.get("unit_id", "")),
                "scope_variant": metadata_payload.get("scope_variant", ""),
            },
        )
        return scope

    def load_committed_call(
        self,
        *,
        provider: str,
        request_name: str,
        request_payload: Dict[str, Any],
        runtime_identity: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        scope = self._plan_scope(
            provider=provider,
            request_name=request_name,
            request_payload=request_payload,
            runtime_identity=runtime_identity,
        )
        cache_key = self.store.build_llm_restore_cache_key(
            stage=self.stage,
            chunk_id=scope["chunk_id"],
            llm_call_id=scope["llm_call_id"],
            input_fingerprint=scope["input_fingerprint"],
        )
        cached_restored = self._prefetched_llm_cache.get(cache_key)
        if isinstance(cached_restored, dict):
            restored = dict(cached_restored)
            restored["runtime_scope"] = scope
            return restored
        restored = self.store.load_committed_llm_response(
            stage=self.stage,
            chunk_id=scope["chunk_id"],
            llm_call_id=scope["llm_call_id"],
            input_fingerprint=scope["input_fingerprint"],
        )
        if restored is None:
            return None
        self._prefetched_llm_cache[cache_key] = dict(restored)
        restored["runtime_scope"] = scope
        return restored

    def build_scope_descriptor(
        self,
        *,
        provider: str,
        request_name: str,
        request_payload: Dict[str, Any],
        runtime_identity: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, str]:
        scope = self._plan_scope(
            provider=provider,
            request_name=request_name,
            request_payload=request_payload,
            runtime_identity=runtime_identity,
        )
        scope_ref = self.store.build_scope_ref(
            stage=self.stage,
            scope_type="llm_call",
            scope_id=scope["llm_call_id"],
        )
        return {
            "input_fingerprint": str(scope["input_fingerprint"] or ""),
            "chunk_id": str(scope["chunk_id"] or ""),
            "llm_call_id": str(scope["llm_call_id"] or ""),
            "scope_ref": str(scope_ref or ""),
        }

    @staticmethod
    def _prepare_request_payload_for_persistence(
        request_payload: Dict[str, Any],
        *,
        metadata: Optional[Dict[str, Any]] = None,
        runtime_identity: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        normalized_request_payload = dict(request_payload or {})
        existing_scope_ids = normalized_request_payload.get("request_scope_ids")
        if isinstance(existing_scope_ids, list) and existing_scope_ids:
            return normalized_request_payload
        for candidate_source in (metadata, runtime_identity):
            if not isinstance(candidate_source, dict):
                continue
            candidate_scope_ids = candidate_source.get("request_scope_ids")
            if not isinstance(candidate_scope_ids, list) or not candidate_scope_ids:
                continue
            normalized_request_payload["request_scope_ids"] = [
                str(item or "").strip()
                for item in candidate_scope_ids
                if str(item or "").strip()
            ]
            if normalized_request_payload["request_scope_ids"]:
                return normalized_request_payload
        return normalized_request_payload

    def persist_success(
        self,
        *,
        provider: str,
        request_name: str,
        request_payload: Dict[str, Any],
        response_text: str,
        response_metadata: Optional[Dict[str, Any]] = None,
        runtime_identity: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        scope = self._plan_scope(
            provider=provider,
            request_name=request_name,
            request_payload=request_payload,
            runtime_identity=runtime_identity,
            metadata=metadata,
        )
        merged_metadata = {
            "provider": str(provider or "").strip(),
            "request_name": str(request_name or "").strip(),
        }
        if str(self.storage_backend or "").strip():
            merged_metadata.setdefault("storage_backend", str(self.storage_backend).strip().lower())
        if isinstance(metadata, dict):
            merged_metadata.update(metadata)
        persist_request_payload = self._prepare_request_payload_for_persistence(
            request_payload,
            metadata=merged_metadata,
            runtime_identity=runtime_identity,
        )
        return self.store.persist_observed_llm_interaction(
            stage=self.stage,
            chunk_id=scope["chunk_id"],
            llm_call_id=scope["llm_call_id"],
            input_fingerprint=scope["input_fingerprint"],
            request_payload=persist_request_payload,
            response_text=str(response_text or ""),
            response_metadata=response_metadata or {},
            metadata=merged_metadata,
        )

    def persist_failure(
        self,
        *,
        provider: str,
        request_name: str,
        request_payload: Dict[str, Any],
        error: Exception,
        runtime_identity: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        scope = self._plan_scope(
            provider=provider,
            request_name=request_name,
            request_payload=request_payload,
            runtime_identity=runtime_identity,
            metadata=metadata,
        )
        merged_metadata = {
            "provider": str(provider or "").strip(),
            "request_name": str(request_name or "").strip(),
        }
        if str(self.storage_backend or "").strip():
            merged_metadata.setdefault("storage_backend", str(self.storage_backend).strip().lower())
        if isinstance(metadata, dict):
            merged_metadata.update(metadata)
        persist_request_payload = self._prepare_request_payload_for_persistence(
            request_payload,
            metadata=merged_metadata,
            runtime_identity=runtime_identity,
        )
        return self.store.persist_observed_llm_interaction(
            stage=self.stage,
            chunk_id=scope["chunk_id"],
            llm_call_id=scope["llm_call_id"],
            input_fingerprint=scope["input_fingerprint"],
            request_payload=persist_request_payload,
            error=error,
            metadata=merged_metadata,
        )

    def emit_llm_call_event(
        self,
        *,
        provider: str,
        request_name: str,
        runtime_identity: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        runtime_restored: bool = False,
    ) -> None:
        if self.llm_event_emitter is None:
            return

        identity = dict(runtime_identity or {})
        metadata_payload = dict(metadata or {})
        stage_step = str(
            metadata_payload.get("stage_step")
            or identity.get("stage_step")
            or identity.get("step_name")
            or request_name
        ).strip()
        stage_prefix = f"{self.stage}_"
        if stage_step.startswith(stage_prefix):
            stage_step = stage_step[len(stage_prefix) :]

        unit_id = str(
            metadata_payload.get("unit_id")
            or identity.get("unit_id")
            or identity.get("chunk_id")
            or identity.get("window_id")
            or identity.get("batch_id")
            or ""
        ).strip()
        llm_call_id = str(identity.get("llm_call_id") or "").strip()
        checkpoint_parts = [stage_step or request_name, "llm_call"]
        if unit_id:
            checkpoint_parts.append(unit_id)
        elif llm_call_id:
            checkpoint_parts.append(llm_call_id)

        event: Dict[str, Any] = {
            "event": "llm_call_completed",
            "stage": str(self.stage or "").strip() or "unknown_stage",
            "status": "running",
            "signal_type": "hard",
            "step_name": stage_step or request_name,
            "stage_step": stage_step or request_name,
            "checkpoint": ".".join(part for part in checkpoint_parts if str(part or "").strip()),
            "provider": str(provider or "").strip(),
            "request_name": str(request_name or "").strip(),
            "runtime_restored": bool(runtime_restored),
            "timestamp_ms": int(time.time() * 1000),
        }
        for field_name in ("scope_variant", "unit_id"):
            field_value = metadata_payload.get(field_name)
            if field_value in (None, "", []):
                continue
            event[field_name] = field_value
        if llm_call_id:
            event["llm_call_id"] = llm_call_id

        try:
            self.llm_event_emitter(dict(event))
        except Exception as error:
            logger.warning(
                "RuntimeLLMContext llm event emit failed: stage=%s step=%s error=%s",
                self.stage,
                stage_step or request_name,
                error,
            )


def current_runtime_llm_context() -> Optional[RuntimeLLMContext]:
    return _ACTIVE_RUNTIME_LLM_CONTEXT.get()


@contextmanager
def activate_runtime_llm_context(
    *,
    stage: str,
    output_dir: str,
    task_id: str = "",
    storage_key: str = "",
    normalized_video_key: str = "",
    storage_backend: str = "sqlite",
    llm_event_emitter: Optional[LLMEventEmitter] = None,
) -> Iterator[RuntimeLLMContext]:
    context = RuntimeLLMContext(
        stage=stage,
        output_dir=output_dir,
        task_id=task_id,
        storage_key=storage_key,
        normalized_video_key=normalized_video_key,
        storage_backend=storage_backend,
        llm_event_emitter=llm_event_emitter,
    )
    token: Token = _ACTIVE_RUNTIME_LLM_CONTEXT.set(context)
    try:
        yield context
    finally:
        _ACTIVE_RUNTIME_LLM_CONTEXT.reset(token)


def build_restored_llm_response(
    *,
    response_text: str,
    response_metadata: Optional[Dict[str, Any]] = None,
):
    metadata = dict(response_metadata or {})
    fallback_payload = metadata.get("fallback")
    previous_failures = metadata.get("previous_failures", [])
    propagated_scope_refs = metadata.get("propagated_scope_refs", [])
    if not isinstance(previous_failures, list):
        previous_failures = []
    if not isinstance(propagated_scope_refs, list):
        propagated_scope_refs = []
    return SimpleNamespace(
        content=str(response_text or ""),
        prompt_tokens=int(metadata.get("prompt_tokens", 0) or 0),
        completion_tokens=int(metadata.get("completion_tokens", 0) or 0),
        total_tokens=int(metadata.get("total_tokens", 0) or 0),
        model=str(metadata.get("model", "") or ""),
        latency_ms=float(metadata.get("latency_ms", 0.0) or 0.0),
        raw_response=metadata.get("raw_response"),
        cache_hit=bool(metadata.get("cache_hit", True)),
        is_fallback=bool(metadata.get("is_fallback", fallback_payload)),
        fallback=fallback_payload if isinstance(fallback_payload, dict) else {},
        previous_failures=list(previous_failures),
        propagated_scope_refs=[
            str(item or "").strip()
            for item in propagated_scope_refs
            if str(item or "").strip()
        ],
    )


def build_runtime_llm_request_payload(
    *,
    model: str,
    prompt: str,
    system_prompt: str,
    kwargs: Optional[Dict[str, Any]] = None,
    extra_payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    payload = {
        "model": str(model or "").strip(),
        "prompt": str(prompt or ""),
        "system_prompt": str(system_prompt or ""),
        "kwargs": dict(kwargs or {}),
    }
    if isinstance(extra_payload, dict):
        payload.update(extra_payload)
    return payload


def dump_runtime_json_text(payload: Any) -> str:
    return _stable_json_text(payload)
