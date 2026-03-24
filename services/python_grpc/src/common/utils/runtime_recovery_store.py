"""
运行态恢复存储：
1) 本地文件是安全提交真源。
2) Redis 仅作为可选热状态镜像。
3) Phase2B LLM 调用与 Phase2A chunk 共享同一套 manifest/commit 协议。
"""

from __future__ import annotations

from collections import deque
import copy
import json
import logging
import os
import re
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from services.python_grpc.src.common.utils.async_disk_writer import enqueue_json_write, flush_async_json_writes
from services.python_grpc.src.common.utils.hash_policy import (
    sha256_bytes as _policy_sha256_bytes,
    sha256_text as _policy_sha256_text,
    stable_json_dumps as _policy_stable_json_dumps,
)
from services.python_grpc.src.common.utils.runtime_recovery_sqlite import RuntimeRecoverySqliteIndex

logger = logging.getLogger(__name__)

_ATOMIC_WRITE_RETRY_COUNT = max(1, int(os.getenv("TASK_RUNTIME_ATOMIC_WRITE_RETRY_COUNT", "4") or 4))
_ATOMIC_WRITE_RETRY_MS = max(1, int(os.getenv("TASK_RUNTIME_ATOMIC_WRITE_RETRY_MS", "25") or 25))

STATUS_PLANNED = "PLANNED"
STATUS_RUNNING = "RUNNING"
STATUS_SUCCESS = "SUCCESS"
STATUS_MANUAL_NEEDED = "MANUAL_NEEDED"
STATUS_ERROR = "ERROR"
STATUS_FAILED = "FAILED"
STATUS_EXECUTING = "EXECUTING"
STATUS_LOCAL_WRITING = "LOCAL_WRITING"
STATUS_LOCAL_COMMITTED = "LOCAL_COMMITTED"
STATUS_COMPLETED = "COMPLETED"
STATUS_AUTO_RETRY_WAIT = "AUTO_RETRY_WAIT"
STATUS_MANUAL_RETRY_REQUIRED = "MANUAL_RETRY_REQUIRED"
STATUS_FATAL = "FATAL"
STATUS_STALE = "STALE"

ERROR_AUTO_RETRYABLE = "AUTO_RETRYABLE"
ERROR_MANUAL_RETRY_REQUIRED = "MANUAL_RETRY_REQUIRED"
ERROR_FATAL_NON_RETRYABLE = "FATAL_NON_RETRYABLE"
ERROR_UNKNOWN = "UNKNOWN"
SCOPE_STATUS_DIRTY = "DIRTY"
SCOPE_TYPE_SUBSTAGE = "substage"

_REDIS_IMPORT_ERROR_LOGGED = False
_DEFAULT_STORAGE_SUCCESS_RETENTION_HOURS = 72
_SCOPE_GRAPH_SCHEMA_VERSION = "runtime_scope_graph_v1"
_REUSABLE_SCOPE_STATUSES = {STATUS_LOCAL_COMMITTED, STATUS_COMPLETED, STATUS_SUCCESS}
_RESUME_INDEX_SCHEMA_VERSION = "runtime_resume_index_v2"
_LLM_LOOKUP_INDEX_SCHEMA_VERSION = "runtime_llm_lookup_v1"
_LLM_ATTEMPT_INDEX_SCHEMA_VERSION = "runtime_llm_attempt_index_v1"
_CHUNK_LOOKUP_INDEX_SCHEMA_VERSION = "runtime_chunk_lookup_v1"
_BLOCKING_STAGE_STATUSES = {STATUS_MANUAL_RETRY_REQUIRED, STATUS_FATAL}
_SCOPE_PLAN_CONTEXT_KEYS = (
    "scope_variant",
    "chunk_id",
    "llm_call_id",
    "provider",
    "request_name",
    "stage_step",
    "unit_id",
    "work_unit_kind",
    "artifact_name",
    "projection_name",
    "section_id",
    "clip_id",
    "screenshot_id",
    "semantic_unit_id",
    "analysis_mode",
    "dependency_fingerprints",
    "substage_scope_ref",
    "substage_name",
    "wave_id",
    "segment_id",
    "segment_index",
    "total_segments",
    "segment_start_sec",
    "segment_end_sec",
    "segment_duration_sec",
    "language",
    "window_id",
    "window_index",
    "batch_id",
)


def _copy_scope_context_value(value: Any) -> Any:
    if isinstance(value, dict):
        return copy.deepcopy(value)
    if isinstance(value, list):
        return copy.deepcopy(value)
    return value


def _normalize_runtime_status(status: str) -> str:
    normalized = str(status or "").strip().upper()
    if not normalized:
        return ""
    if normalized in {STATUS_PLANNED, "PLANNING", "RETRYING", "RETRING"}:
        return STATUS_PLANNED
    if normalized in {STATUS_RUNNING, "RUNING", STATUS_EXECUTING, STATUS_LOCAL_WRITING}:
        return STATUS_RUNNING
    if normalized in {STATUS_SUCCESS, STATUS_COMPLETED, STATUS_LOCAL_COMMITTED}:
        return STATUS_SUCCESS
    if normalized in {STATUS_MANUAL_NEEDED, STATUS_MANUAL_RETRY_REQUIRED, "MANUL_NEEDED"}:
        return STATUS_MANUAL_NEEDED
    if normalized in {STATUS_ERROR, STATUS_AUTO_RETRY_WAIT}:
        return STATUS_ERROR
    if normalized in {STATUS_FAILED, STATUS_FATAL, "FAIL"}:
        return STATUS_FAILED
    return normalized


def _normalize_scope_current_status(status: str) -> str:
    return _normalize_runtime_status(status)


def _derive_scope_failure_status(error_class: str) -> str:
    normalized_error_class = str(error_class or "").strip()
    if normalized_error_class == ERROR_AUTO_RETRYABLE:
        return STATUS_ERROR
    if normalized_error_class == ERROR_FATAL_NON_RETRYABLE:
        return STATUS_FAILED
    return STATUS_MANUAL_NEEDED


def _build_scope_plan_context(
    *,
    stage: str,
    scope_type: str,
    scope_id: str,
    input_fingerprint: str = "",
    metadata: Optional[Dict[str, Any]] = None,
    request_payload: Optional[Dict[str, Any]] = None,
    existing_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    context: Dict[str, Any] = {}
    if isinstance(existing_context, dict):
        context.update({key: _copy_scope_context_value(value) for key, value in existing_context.items()})
    if stage:
        context["stage"] = str(stage or "")
    if scope_type:
        context["scope_type"] = str(scope_type or "")
    if scope_id:
        context["scope_id"] = str(scope_id or "")
    if input_fingerprint:
        context["input_fingerprint"] = str(input_fingerprint or "")
    metadata_payload = dict(metadata or {})
    for nested_key in ("plan_context", "recovery_context", "runtime_identity"):
        nested_payload = metadata_payload.get(nested_key)
        if not isinstance(nested_payload, dict):
            continue
        for field_name, field_value in nested_payload.items():
            if field_value in (None, "", [], {}):
                continue
            context[str(field_name or "")] = _copy_scope_context_value(field_value)
    for field_name in _SCOPE_PLAN_CONTEXT_KEYS:
        field_value = metadata_payload.get(field_name)
        if field_value in (None, "", [], {}):
            continue
        context[field_name] = _copy_scope_context_value(field_value)
    request_scope_ids = []
    if isinstance(request_payload, dict):
        for nested_key in ("plan_context", "recovery_context", "runtime_identity", "request_identity"):
            nested_payload = request_payload.get(nested_key)
            if not isinstance(nested_payload, dict):
                continue
            for field_name, field_value in nested_payload.items():
                if field_value in (None, "", [], {}):
                    continue
                context[str(field_name or "")] = _copy_scope_context_value(field_value)
        raw_scope_ids = request_payload.get("request_scope_ids")
        if isinstance(raw_scope_ids, list):
            request_scope_ids = [
                str(item or "").strip()
                for item in raw_scope_ids
                if str(item or "").strip()
            ]
    if request_scope_ids:
        context["request_scope_ids"] = request_scope_ids
    return context


def _collect_runtime_resource_snapshot(extra_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    snapshot: Dict[str, Any] = {
        "recorded_at_ms": _now_ms(),
        "pid": os.getpid(),
    }
    try:
        import psutil  # type: ignore

        process = psutil.Process(os.getpid())
        mem_info = process.memory_info()
        snapshot.update(
            {
                "rss_mb": round(float(mem_info.rss) / (1024.0 * 1024.0), 2),
                "vms_mb": round(float(mem_info.vms) / (1024.0 * 1024.0), 2),
                "thread_count": int(process.num_threads()),
            }
        )
    except Exception:
        pass
    if isinstance(extra_payload, dict):
        for key, value in extra_payload.items():
            if value in (None, "", [], {}):
                continue
            snapshot[str(key or "")] = _copy_scope_context_value(value)
    return snapshot


def _read_env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name, str(default)) or "").strip()
    try:
        return int(raw)
    except Exception:
        return int(default)


def _read_env_bool(name: str, default: bool) -> bool:
    raw = str(os.getenv(name, "1" if default else "0") or "").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return bool(default)


def _stable_json_dumps(payload: Any) -> str:
    return _policy_stable_json_dumps(payload)


def _sha256_text(value: str) -> str:
    return _policy_sha256_text(value)


def _sha256_bytes(payload: bytes) -> str:
    return _policy_sha256_bytes(payload)


def _sanitize_runtime_segment(value: str, *, fallback: str, max_length: int = 48) -> str:
    normalized = re.sub(r"[^0-9A-Za-z._-]+", "_", str(value or "").strip())
    normalized = normalized.strip("._-")
    if not normalized:
        normalized = str(fallback or "unknown").strip() or "unknown"
    if max_length > 0 and len(normalized) > max_length:
        normalized = normalized[:max_length].rstrip("._-") or normalized[:max_length]
    return normalized or (str(fallback or "unknown").strip() or "unknown")


def build_runtime_payload_fingerprint(payload: Any, *, extra: Optional[Dict[str, Any]] = None) -> str:
    if not isinstance(extra, dict) or not extra:
        return _sha256_text(_stable_json_dumps(payload))
    # 兼容旧调用链：当调用方显式传 extra 时，将其与主 payload 一起纳入稳定指纹，
    # 避免不同阶段/上下文在复用同一 payload 结构时发生误碰撞。
    fingerprint_payload = {
        "payload": payload,
        "extra": extra,
    }
    return _sha256_text(_stable_json_dumps(fingerprint_payload))


def _resolve_chunk_storage_backend(metadata: Optional[Dict[str, Any]] = None) -> str:
    if not isinstance(metadata, dict):
        return "sqlite"
    normalized = str(metadata.get("storage_backend", "") or "").strip().lower()
    if normalized in {"sqlite", "hybrid"}:
        return normalized
    return "sqlite"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _fsync_parent_dir(path: Path) -> None:
    """尽力同步父目录元数据，降低断电后 rename 丢失风险。"""
    try:
        parent_dir = str(path.resolve().parent)
    except Exception:
        parent_dir = str(path.parent)
    if not parent_dir:
        return
    try:
        dir_flags = getattr(os, "O_RDONLY", 0)
        if hasattr(os, "O_BINARY"):
            dir_flags |= getattr(os, "O_BINARY", 0)
        dir_fd = os.open(parent_dir, dir_flags)
    except Exception:
        return
    try:
        os.fsync(dir_fd)
    except Exception:
        pass
    finally:
        try:
            os.close(dir_fd)
        except Exception:
            pass


def _remove_file_quietly(path: Path) -> None:
    try:
        Path(path).unlink(missing_ok=True)
    except Exception:
        pass


def _is_retryable_replace_error(error: Exception) -> bool:
    if os.name != "nt":
        return False
    if isinstance(error, PermissionError):
        return True
    winerror = getattr(error, "winerror", None)
    return winerror in {5, 32}


def _replace_atomic_target(tmp_path: Path, target_path: Path) -> None:
    last_error: Optional[Exception] = None
    for attempt in range(_ATOMIC_WRITE_RETRY_COUNT):
        try:
            os.replace(tmp_path, target_path)
            return
        except Exception as error:
            last_error = error
            if attempt + 1 >= _ATOMIC_WRITE_RETRY_COUNT or not _is_retryable_replace_error(error):
                break
            time.sleep((_ATOMIC_WRITE_RETRY_MS * (attempt + 1)) / 1000.0)
    _remove_file_quietly(tmp_path)
    if last_error is not None:
        raise last_error


def _write_json_atomic_sync(path: Path, payload: Any) -> None:
    target_path = Path(path)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = target_path.with_name(f"{target_path.name}.t{time.time_ns():x}")
    with open(tmp_path, "w", encoding="utf-8") as output_stream:
        json.dump(payload, output_stream, ensure_ascii=False, indent=2, default=str)
        output_stream.flush()
        os.fsync(output_stream.fileno())
    _replace_atomic_target(tmp_path, target_path)
    _fsync_parent_dir(target_path)


def _read_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        with open(path, "r", encoding="utf-8") as input_stream:
            payload = json.load(input_stream)
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def _append_jsonl_sync(path: Path, payload: Dict[str, Any]) -> None:
    target_path = Path(path)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with open(target_path, "a", encoding="utf-8") as output_stream:
        output_stream.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")
        output_stream.flush()
        os.fsync(output_stream.fileno())
    _fsync_parent_dir(target_path)


def _build_file_signature(path_text: str) -> Dict[str, Any]:
    safe_path = str(path_text or "").strip()
    if not safe_path:
        return {"path": "", "exists": False}
    try:
        path = Path(safe_path)
        stat_result = path.stat()
        return {
            "path": str(path.resolve()),
            "exists": True,
            "size": int(stat_result.st_size),
            "mtime_ns": int(getattr(stat_result, "st_mtime_ns", int(stat_result.st_mtime * 1_000_000_000))),
        }
    except Exception:
        return {"path": safe_path, "exists": False}


def _build_runtime_retry_semantics(error_class: str, error_message: str) -> Dict[str, str]:
    """把错误类别进一步翻译为更明确的重试动作语义。"""
    normalized_class = str(error_class or "").strip() or ERROR_UNKNOWN
    lowered = str(error_message or "").lower()

    if normalized_class == ERROR_AUTO_RETRYABLE:
        return {
            "retry_strategy": "AUTO_RETRY",
            "operator_action": "WAIT_AUTO_RETRY",
            "action_hint": "系统会按退避策略自动重试；若持续失败再人工介入。",
        }

    if normalized_class == ERROR_FATAL_NON_RETRYABLE:
        return {
            "retry_strategy": "NO_RETRY",
            "operator_action": "CHECK_INPUT_OR_CONTRACT",
            "action_hint": "当前错误被判定为不可自动重试，请先修正输入、协议或业务约束后再重新提交。",
        }

    if any(token in lowered for token in ("quota", "balance", "insufficient credits")):
        return {
            "retry_strategy": "MANUAL_RETRY_AFTER_REPAIR",
            "operator_action": "RESTORE_QUOTA_OR_BALANCE",
            "action_hint": "请先补足额度或余额，再从当前失败点发起人工重试。",
        }

    if any(token in lowered for token in ("out of memory", "cannot allocate memory", "memoryerror", "oom")):
        return {
            "retry_strategy": "MANUAL_RETRY_AFTER_REPAIR",
            "operator_action": "REDUCE_CONCURRENCY_OR_CHUNK",
            "action_hint": "请先降低并发、缩小 chunk 或扩容内存，再从当前失败点人工重试。",
        }

    if any(token in lowered for token in ("disk full", "no space left")):
        return {
            "retry_strategy": "MANUAL_RETRY_AFTER_REPAIR",
            "operator_action": "FREE_DISK_SPACE",
            "action_hint": "请先释放磁盘空间，再从当前失败点人工重试。",
        }

    if any(token in lowered for token in ("unauthorized", "forbidden", "invalid api key", "authentication", "credential")):
        return {
            "retry_strategy": "MANUAL_RETRY_AFTER_REPAIR",
            "operator_action": "REFRESH_CREDENTIALS",
            "action_hint": "请先修复凭证或鉴权配置，再从当前失败点人工重试。",
        }

    if normalized_class == ERROR_UNKNOWN:
        return {
            "retry_strategy": "MANUAL_REVIEW_REQUIRED",
            "operator_action": "INSPECT_AND_CLASSIFY",
            "action_hint": "系统未能自动判定修复动作，请先查看 error_message 与输入快照，再决定是否人工重试。",
        }

    return {
        "retry_strategy": "MANUAL_RETRY_AFTER_REPAIR",
        "operator_action": "REPAIR_DEPENDENCY_AND_RETRY",
        "action_hint": "请先修复外部依赖或运行环境，再从当前失败点人工重试。",
    }


def classify_runtime_error(error: Exception) -> Dict[str, str]:
    """统一错误分类，便于自动重试与人工干预。"""
    text = str(error or "").strip()
    lowered = text.lower()
    provider_code = ""
    http_status = ""

    for attr_name in ("status_code", "http_status", "code"):
        raw_value = getattr(error, attr_name, "")
        if raw_value not in (None, ""):
            provider_code = str(raw_value)
            break

    response = getattr(error, "response", None)
    if response is not None:
        status_value = getattr(response, "status_code", None)
        if status_value not in (None, ""):
            http_status = str(status_value)
            if not provider_code:
                provider_code = http_status

    if http_status in {"408", "409", "425", "429", "500", "502", "503", "504"}:
        error_class = ERROR_AUTO_RETRYABLE
    elif any(token in lowered for token in ("timed out", "timeout", "connection reset", "temporarily unavailable", "dns")):
        error_class = ERROR_AUTO_RETRYABLE
    elif any(token in lowered for token in ("rate limit", "too many requests")):
        error_class = ERROR_AUTO_RETRYABLE
    elif any(token in lowered for token in ("quota", "balance", "insufficient credits")):
        error_class = ERROR_MANUAL_RETRY_REQUIRED
    elif any(token in lowered for token in ("out of memory", "cannot allocate memory", "memoryerror", "oom", "disk full", "no space left")):
        error_class = ERROR_MANUAL_RETRY_REQUIRED
    elif any(token in lowered for token in ("unauthorized", "forbidden", "invalid api key", "authentication", "credential")):
        error_class = ERROR_MANUAL_RETRY_REQUIRED
    elif any(token in lowered for token in ("invalid argument", "unsupported", "schema", "malformed", "not found")):
        error_class = ERROR_FATAL_NON_RETRYABLE
    else:
        error_class = ERROR_UNKNOWN

    return {
        "error_class": error_class,
        "error_code": provider_code or getattr(error, "__class__", type(error)).__name__,
        "error_message": text,
        **_build_runtime_retry_semantics(error_class, text),
    }


def split_text_parts_by_bytes(text: str, max_part_bytes: int) -> Tuple[List[Dict[str, Any]], str]:
    """按 UTF-8 字节阈值切分文本，保留字符/字节边界元数据。"""
    normalized = str(text or "")
    safe_limit = max(512, int(max_part_bytes or 0))
    parts: List[Dict[str, Any]] = []
    current_chars: List[str] = []
    current_start_char = 0
    current_start_byte = 0
    current_bytes = 0
    consumed_chars = 0
    consumed_bytes = 0

    def _flush_current() -> None:
        nonlocal current_chars, current_start_char, current_start_byte, current_bytes
        if not current_chars:
            return
        part_text = "".join(current_chars)
        encoded = part_text.encode("utf-8")
        part_index = len(parts)
        parts.append(
            {
                "part_index": part_index,
                "char_start": current_start_char,
                "char_end": current_start_char + len(part_text),
                "byte_start": current_start_byte,
                "byte_end": current_start_byte + len(encoded),
                "payload_chars": len(part_text),
                "payload_bytes": len(encoded),
                "payload_hash": _sha256_bytes(encoded),
                "content": part_text,
            }
        )
        current_chars = []
        current_start_char = consumed_chars
        current_start_byte = consumed_bytes
        current_bytes = 0

    for char_value in normalized:
        encoded_char = char_value.encode("utf-8")
        if current_chars and current_bytes + len(encoded_char) > safe_limit:
            _flush_current()
        if not current_chars:
            current_start_char = consumed_chars
            current_start_byte = consumed_bytes
        current_chars.append(char_value)
        current_bytes += len(encoded_char)
        consumed_chars += 1
        consumed_bytes += len(encoded_char)

    _flush_current()
    if not parts:
        parts.append(
            {
                "part_index": 0,
                "char_start": 0,
                "char_end": 0,
                "byte_start": 0,
                "byte_end": 0,
                "payload_chars": 0,
                "payload_bytes": 0,
                "payload_hash": _sha256_text(""),
                "content": "",
            }
        )
    return parts, _sha256_text(normalized)


def build_screenshot_chunk_fingerprint(
    *,
    video_path: str,
    mode: str,
    chunk: Dict[str, Any],
) -> str:
    windows = []
    for raw_window in list(chunk.get("windows", []) or []):
        if not isinstance(raw_window, dict):
            continue
        raw_req = raw_window.get("req", {})
        if not isinstance(raw_req, dict):
            raw_req = {}
        windows.append(
            {
                "screenshot_id": str(raw_req.get("screenshot_id", "") or ""),
                "semantic_unit_id": str(raw_req.get("semantic_unit_id", "") or ""),
                "timestamp_sec": float(
                    raw_req.get("_original_timestamp", raw_req.get("timestamp_sec", 0.0)) or 0.0
                ),
                "label": str(raw_req.get("label", "") or ""),
                "analysis_mode": str(raw_req.get("analysis_mode", "") or ""),
                "knowledge_type": str(raw_req.get("knowledge_type", "") or ""),
                "prefetch_profile": str(raw_window.get("prefetch_profile", "") or ""),
            }
        )
    payload = {
        "video": _build_file_signature(video_path),
        "mode": str(mode or ""),
        "chunk": {
            "union_start": float(chunk.get("union_start", 0.0) or 0.0),
            "union_end": float(chunk.get("union_end", 0.0) or 0.0),
            "prefetch_profile": str(chunk.get("prefetch_profile", "") or ""),
            "prefetch_sample_rate": int(chunk.get("prefetch_sample_rate", 0) or 0),
            "prefetch_target_height": int(chunk.get("prefetch_target_height", 0) or 0),
            "max_chunk_span_seconds": float(chunk.get("max_chunk_span_seconds", 0.0) or 0.0),
            "windows": windows,
        },
    }
    return _sha256_text(_stable_json_dumps(payload))


def build_llm_input_fingerprint(
    *,
    step_name: str,
    unit_id: str,
    model: str,
    system_prompt: str,
    user_prompt: str,
) -> str:
    return _sha256_text(
        _stable_json_dumps(
            {
                "step_name": str(step_name or ""),
                "unit_id": str(unit_id or ""),
                "model": str(model or ""),
                "system_prompt": str(system_prompt or ""),
                "user_prompt": str(user_prompt or ""),
            }
        )
    )


@dataclass
class RuntimeAttemptHandle:
    stage: str
    chunk_id: str
    llm_call_id: str
    attempt: int
    attempt_dir: Path
    manifest_path: Path
    request_path: Path
    response_parts_dir: Path
    input_fingerprint: str
    scope_key: str
    storage_backend: str = "hybrid"
    request_payload_cache: Optional[Dict[str, Any]] = None
    manifest_payload_cache: Optional[Dict[str, Any]] = None


class _RuntimeRedisMirror:
    """可选 Redis 热状态镜像；失败时自动降级为本地真源模式。"""

    def __init__(self) -> None:
        self._enabled = str(os.getenv("TASK_RUNTIME_REDIS_ENABLED", "") or "").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        self._url = str(os.getenv("TASK_RUNTIME_REDIS_URL", "") or "").strip()
        self._prefix = str(os.getenv("TASK_RUNTIME_REDIS_PREFIX", "rt") or "rt").strip() or "rt"
        self._client = None
        self._logged_disabled = False

    def _get_client(self):
        global _REDIS_IMPORT_ERROR_LOGGED
        if not self._enabled or not self._url:
            return None
        if self._client is not None:
            return self._client
        try:
            import redis
        except Exception as error:
            if not _REDIS_IMPORT_ERROR_LOGGED:
                logger.warning("Redis 运行态镜像未启用：缺少 redis 依赖，错误=%s", error)
                _REDIS_IMPORT_ERROR_LOGGED = True
            return None
        try:
            self._client = redis.Redis.from_url(self._url, decode_responses=True)
        except Exception as error:
            if not self._logged_disabled:
                logger.warning("Redis 运行态镜像初始化失败：%s", error)
                self._logged_disabled = True
            return None
        return self._client

    def _key(self, suffix: str) -> str:
        return f"{self._prefix}:{suffix}"

    def hset(self, suffix: str, mapping: Dict[str, Any]) -> None:
        client = self._get_client()
        if client is None or not mapping:
            return
        try:
            normalized = {str(k): "" if v is None else str(v) for k, v in mapping.items()}
            client.hset(self._key(suffix), mapping=normalized)
        except Exception as error:
            logger.warning("Redis hset failed: key=%s error=%s", suffix, error)

    def xadd(self, suffix: str, payload: Dict[str, Any], maxlen: int = 2048) -> None:
        client = self._get_client()
        if client is None or not payload:
            return
        try:
            normalized = {str(k): "" if v is None else str(v) for k, v in payload.items()}
            client.xadd(self._key(suffix), normalized, maxlen=maxlen, approximate=True)
        except Exception as error:
            logger.warning("Redis xadd failed: key=%s error=%s", suffix, error)

    def unlink(self, *suffixes: str) -> None:
        client = self._get_client()
        normalized_keys = [self._key(str(suffix).strip()) for suffix in suffixes if str(suffix or "").strip()]
        if client is None or not normalized_keys:
            return
        try:
            client.unlink(*normalized_keys)
        except Exception as error:
            logger.warning("Redis unlink failed: keys=%s error=%s", normalized_keys, error)


class RuntimeRecoveryStore:
    """封装本地真源 + 可选 Redis 热状态镜像。"""

    def __init__(
        self,
        *,
        output_dir: str,
        task_id: str = "",
        storage_key: str = "",
        normalized_video_key: str = "",
    ) -> None:
        resolved_output_dir = str(output_dir or "").strip()
        if not resolved_output_dir:
            raise ValueError("output_dir is required")
        output_path = Path(resolved_output_dir).resolve()
        self.output_dir = output_path
        self.task_id = str(task_id or output_path.name or "unknown_task").strip() or "unknown_task"
        self.storage_key = str(storage_key or output_path.name or self.task_id or "unknown_storage").strip() or "unknown_storage"
        self.normalized_video_key = str(normalized_video_key or "").strip()
        self.runtime_root = output_path / "intermediates" / "rt"
        self.runtime_root.mkdir(parents=True, exist_ok=True)
        self.runtime_state_db_path = self._resolve_runtime_state_db_path()
        self._redis = _RuntimeRedisMirror()
        self._sqlite_index: Optional[RuntimeRecoverySqliteIndex] = None
        try:
            self._sqlite_index = RuntimeRecoverySqliteIndex.shared(db_path=str(self.runtime_state_db_path))
        except Exception as error:
            logger.warning("Runtime recovery SQLite mirror init failed: %s", error)
        self._storage_success_retention_ms = max(
            0,
            _read_env_int("TASK_RUNTIME_STORAGE_SUCCESS_RETENTION_HOURS", _DEFAULT_STORAGE_SUCCESS_RETENTION_HOURS),
        ) * 3600 * 1000
        self._write_task_meta_file = _read_env_bool("TASK_RUNTIME_WRITE_TASK_META_FILE", True)
        self._write_stage_file_mirrors = _read_env_bool("TASK_RUNTIME_WRITE_STAGE_FILE_MIRRORS", False)
        self.update_task_meta()
        self.sync_scope_hints_from_scope_graph()

    def _task_scope_key(self, *parts: str) -> str:
        segments = ["task", self.task_id]
        for part in parts:
            normalized = str(part or "").strip()
            if normalized:
                segments.append(normalized)
        return ":".join(segments)

    def _storage_scope_key(self, *parts: str) -> str:
        segments = ["storage", self.storage_key]
        for part in parts:
            normalized = str(part or "").strip()
            if normalized:
                segments.append(normalized)
        return ":".join(segments)

    def _task_meta_path(self) -> Path:
        return self.runtime_root / "task_meta.json"

    def _resolve_runtime_state_db_path(self) -> Path:
        configured = str(os.getenv("TASK_RUNTIME_SQLITE_DB_PATH", "") or "").strip()
        if configured:
            return Path(configured).expanduser().resolve()
        return (self.runtime_root / "runtime_state.db").resolve()

    def close(self) -> int:
        """
        做什么：释放当前任务 runtime store 持有的 SQLite 共享索引连接。
        为什么：Windows 上删除任务目录前若仍保留 WAL 句柄，会导致 runtime_state.db-wal 无法删除。
        权衡：连接会在下次访问时按需重建，删除链路优先于连接复用收益。
        """
        if self._sqlite_index is None:
            return 0
        released = 1 if RuntimeRecoverySqliteIndex.release_shared(db_path=str(self.runtime_state_db_path)) else 0
        self._sqlite_index = None
        return released

    def _resume_index_path(self) -> Path:
        return self.runtime_root / "resume_index.json"

    def _should_write_stage_file_mirrors(self) -> bool:
        return bool(self._write_stage_file_mirrors)

    def _should_write_task_meta_file(self) -> bool:
        return bool(self._write_task_meta_file)

    def _stage_journal_path(self, stage: str) -> Path:
        return self.stage_dir(stage) / "stage_journal.jsonl"

    def _stage_outputs_manifest_path(self, stage: str) -> Path:
        return self.stage_dir(stage) / "outputs_manifest.json"

    def _rt_fallback_log_path(self) -> Path:
        return self.runtime_root / "fallback_records.jsonl"

    def _rt_error_log_path(self) -> Path:
        return self.runtime_root / "error_records.jsonl"

    def _rt_manual_retry_log_path(self) -> Path:
        return self.runtime_root / "manual_retry_required_records.jsonl"

    def _runtime_index_dir(self, kind: str) -> Path:
        alias_map = {
            "llm": "l",
            "llm_attempt": "la",
            "chunk": "c",
        }
        safe_kind = alias_map.get(str(kind or "").strip(), _sanitize_runtime_segment(kind, fallback="index", max_length=8).lower())
        path = self.runtime_root / "index" / safe_kind
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _llm_lookup_index_path(
        self,
        *,
        stage: str,
        chunk_id: str,
        llm_call_id: str,
        input_fingerprint: str,
    ) -> Path:
        digest = build_runtime_payload_fingerprint(
            {
                "stage": str(stage or ""),
                "chunk_id": str(chunk_id or ""),
                "llm_call_id": str(llm_call_id or ""),
                "input_fingerprint": str(input_fingerprint or ""),
            }
        )[:24]
        return self._runtime_index_dir("llm") / f"{digest}.json"

    def _llm_attempt_index_path(
        self,
        *,
        stage: str,
        chunk_id: str,
        llm_call_id: str,
    ) -> Path:
        digest = build_runtime_payload_fingerprint(
            {
                "stage": str(stage or ""),
                "chunk_id": str(chunk_id or ""),
                "llm_call_id": str(llm_call_id or ""),
            }
        )[:24]
        return self._runtime_index_dir("llm_attempt") / f"{digest}.json"

    def _chunk_lookup_index_path(
        self,
        *,
        stage: str,
        chunk_id: str,
        input_fingerprint: str,
    ) -> Path:
        digest = build_runtime_payload_fingerprint(
            {
                "stage": str(stage or ""),
                "chunk_id": str(chunk_id or ""),
                "input_fingerprint": str(input_fingerprint or ""),
            }
        )[:24]
        return self._runtime_index_dir("chunk") / f"{digest}.json"

    def _empty_resume_index_payload(self) -> Dict[str, Any]:
        return {
            "schema_version": _RESUME_INDEX_SCHEMA_VERSION,
            "task_id": self.task_id,
            "updated_at_ms": 0,
            "hint_stage": "",
            "hint_status": "",
            "hint_checkpoint": "",
            "hint_stage_state_path": "",
            "recovery_anchor": {},
            "stage_graph_version": "video_pipeline_v2",
            "owner": "python",
        }

    def _load_stage_snapshots_from_sqlite(self) -> Dict[str, Dict[str, Any]]:
        if self._sqlite_index is None:
            return {}
        try:
            rows = self._sqlite_index.list_stage_snapshots(output_dir=str(self.output_dir), limit=32)
        except Exception as error:
            logger.warning("Runtime recovery SQLite stage snapshot load failed: %s", error)
            return {}
        snapshots: Dict[str, Dict[str, Any]] = {}
        for row in list(rows or []):
            if not isinstance(row, dict):
                continue
            stage_name = str(row.get("stage", "") or "").strip()
            if not stage_name:
                continue
            snapshots[stage_name] = dict(row)
        return snapshots

    def _read_resume_index_from_sqlite(self) -> Optional[Dict[str, Any]]:
        stages_payload = self._load_stage_snapshots_from_sqlite()
        if not stages_payload:
            return None
        latest_stage_state, latest_blocking_stage_state = self._rebuild_resume_stage_views(stages_payload)
        anchor = latest_blocking_stage_state or latest_stage_state
        if not isinstance(anchor, dict):
            return None
        stage_state_path = str(
            anchor.get("stage_state_path", "")
            or anchor.get("local_stage_state_path", "")
            or self._stage_state_file_path(str(anchor.get("stage", "") or "")).resolve()
        )
        return {
            **self._empty_resume_index_payload(),
            "task_id": str(anchor.get("task_id", self.task_id) or self.task_id),
            "updated_at_ms": int(anchor.get("updated_at_ms", 0) or 0),
            "hint_stage": str(anchor.get("stage", "") or ""),
            "hint_status": str(anchor.get("status", "") or ""),
            "hint_checkpoint": str(anchor.get("checkpoint", "") or ""),
            "hint_stage_state_path": stage_state_path,
            "recovery_anchor": {
                "resume_from_stage": str(anchor.get("stage", "") or ""),
                "reason": "sqlite_stage_snapshot_projection",
            },
            "owner": str(anchor.get("stage_owner", "python") or "python"),
        }

    def _read_resume_index(self) -> Dict[str, Any]:
        sqlite_payload = self._read_resume_index_from_sqlite()
        if isinstance(sqlite_payload, dict):
            return sqlite_payload
        payload = _read_json(self._resume_index_path())
        if not isinstance(payload, dict):
            return self._empty_resume_index_payload()
        if str(payload.get("schema_version", "") or "") == _RESUME_INDEX_SCHEMA_VERSION:
            normalized = self._empty_resume_index_payload()
            normalized.update(
                {
                    "task_id": str(payload.get("task_id", self.task_id) or self.task_id),
                    "updated_at_ms": int(payload.get("updated_at_ms", 0) or 0),
                    "hint_stage": str(payload.get("hint_stage", "") or ""),
                    "hint_status": str(payload.get("hint_status", "") or ""),
                    "hint_checkpoint": str(payload.get("hint_checkpoint", "") or ""),
                    "hint_stage_state_path": str(payload.get("hint_stage_state_path", "") or ""),
                    "stage_graph_version": str(payload.get("stage_graph_version", "video_pipeline_v2") or "video_pipeline_v2"),
                    "owner": str(payload.get("owner", "python") or "python"),
                }
            )
            if isinstance(payload.get("recovery_anchor"), dict):
                normalized["recovery_anchor"] = dict(payload.get("recovery_anchor") or {})
            return normalized
        legacy_candidates = [
            payload.get("latest_blocking_stage_state"),
            payload.get("latest_stage_state"),
            payload.get("previous_blocking_stage_state"),
            payload.get("previous_stage_state"),
        ]
        for candidate in legacy_candidates:
            if not isinstance(candidate, dict):
                continue
            return {
                **self._empty_resume_index_payload(),
                "task_id": self.task_id,
                "updated_at_ms": int(candidate.get("updated_at_ms", 0) or 0),
                "hint_stage": str(candidate.get("stage", "") or ""),
                "hint_status": str(candidate.get("status", "") or ""),
                "hint_checkpoint": str(candidate.get("checkpoint", "") or ""),
                "hint_stage_state_path": str(
                    candidate.get("stage_state_path", "") or candidate.get("local_stage_state_path", "") or ""
                ),
                "recovery_anchor": {
                    "resume_from_stage": str(candidate.get("stage", "") or ""),
                    "reason": "legacy_resume_index_projection",
                },
                "stage_graph_version": "video_pipeline_v2",
                "owner": "python",
            }
        return self._empty_resume_index_payload()

    def _write_resume_index(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        normalized = self._empty_resume_index_payload()
        normalized.update(
            {
                "task_id": str((payload or {}).get("task_id", self.task_id) or self.task_id),
                "updated_at_ms": int((payload or {}).get("updated_at_ms", 0) or 0),
                "hint_stage": str((payload or {}).get("hint_stage", "") or ""),
                "hint_status": str((payload or {}).get("hint_status", "") or ""),
                "hint_checkpoint": str((payload or {}).get("hint_checkpoint", "") or ""),
                "hint_stage_state_path": str((payload or {}).get("hint_stage_state_path", "") or ""),
                "stage_graph_version": str((payload or {}).get("stage_graph_version", "video_pipeline_v2") or "video_pipeline_v2"),
                "owner": str((payload or {}).get("owner", "python") or "python"),
            }
        )
        if isinstance((payload or {}).get("recovery_anchor"), dict):
            normalized["recovery_anchor"] = dict((payload or {}).get("recovery_anchor") or {})
        _write_json_atomic_sync(self._resume_index_path(), normalized)
        return normalized

    @staticmethod
    def _snapshot_without_nested_previous(payload: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not isinstance(payload, dict):
            return None
        snapshot = {
            key: value
            for key, value in dict(payload).items()
            if key not in {"schema_version", "previous_record"}
        }
        return snapshot or None

    @classmethod
    def _attach_previous_record(
        cls,
        *,
        current_payload: Dict[str, Any],
        existing_payload: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        normalized = dict(current_payload or {})
        previous_record = cls._snapshot_without_nested_previous(existing_payload)
        if previous_record is not None:
            normalized["previous_record"] = previous_record
        else:
            normalized.pop("previous_record", None)
        return normalized

    @staticmethod
    def _payload_candidates_with_previous(payload: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not isinstance(payload, dict):
            return []
        candidates: List[Dict[str, Any]] = []
        current_payload = {
            key: value
            for key, value in dict(payload).items()
            if key not in {"schema_version", "previous_record"}
        }
        if current_payload:
            candidates.append(current_payload)
        previous_record = payload.get("previous_record")
        if isinstance(previous_record, dict) and previous_record:
            candidates.append(dict(previous_record))
        return candidates

    def _rebuild_resume_stage_views(self, stages_payload: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        latest_stage_state: Optional[Dict[str, Any]] = None
        latest_blocking_stage_state: Optional[Dict[str, Any]] = None
        for raw_stage_payload in list((stages_payload or {}).values()):
            if not isinstance(raw_stage_payload, dict):
                continue
            stage_payload = dict(raw_stage_payload)
            updated_at_ms = int(stage_payload.get("updated_at_ms", 0) or 0)
            if latest_stage_state is None or updated_at_ms >= int(latest_stage_state.get("updated_at_ms", 0) or 0):
                latest_stage_state = stage_payload
            if str(stage_payload.get("status", "") or "") not in _BLOCKING_STAGE_STATUSES:
                continue
            if latest_blocking_stage_state is None or updated_at_ms >= int(latest_blocking_stage_state.get("updated_at_ms", 0) or 0):
                latest_blocking_stage_state = stage_payload
        return latest_stage_state, latest_blocking_stage_state

    def _update_resume_task_meta(self) -> Dict[str, Any]:
        resume_payload = self._read_resume_index()
        return self._write_resume_index(resume_payload)

    def _build_stage_resume_summary(
        self,
        *,
        stage: str,
        stage_path: Path,
        state_payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        summary = {
            "stage": str(stage or state_payload.get("stage", "") or ""),
            "status": str(state_payload.get("status", "") or ""),
            "checkpoint": str(state_payload.get("checkpoint", "") or ""),
            "updated_at_ms": int(state_payload.get("updated_at_ms", _now_ms()) or _now_ms()),
            "stage_state_path": str(stage_path),
        }
        for field_name in (
            "completed",
            "pending",
            "retry_mode",
            "required_action",
            "retry_entry_point",
            "retry_strategy",
            "resume_from_step",
            "resume_entry_step",
            "recovery_plan_mode",
            "recovery_plan_digest",
            "dirty_scope_count",
            "invalidated_descendants_count",
            "failed_scope_kind",
            "failed_scope_id",
            "failed_scope_ref",
            "operator_action",
            "action_hint",
            "error_class",
            "error_code",
            "error_message",
        ):
            field_value = state_payload.get(field_name)
            if field_value in (None, "", []):
                continue
            summary[field_name] = field_value
        return summary

    def _update_resume_stage_index(
        self,
        *,
        stage: str,
        stage_path: Path,
        state_payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        stage_summary = self._build_stage_resume_summary(stage=stage, stage_path=stage_path, state_payload=state_payload)
        return self._write_resume_index(
            {
                "task_id": self.task_id,
                "updated_at_ms": int(stage_summary.get("updated_at_ms", _now_ms()) or _now_ms()),
                "hint_stage": str(stage_summary.get("stage", stage) or stage),
                "hint_status": str(stage_summary.get("status", "") or ""),
                "hint_checkpoint": str(stage_summary.get("checkpoint", "") or ""),
                "hint_stage_state_path": str(stage_summary.get("stage_state_path", "") or ""),
                "recovery_anchor": {
                    "resume_from_stage": str(stage_summary.get("stage", stage) or stage),
                    "reason": "latest_stage_checkpoint",
                },
                "stage_graph_version": "video_pipeline_v2",
                "owner": "python",
            }
        )

    @staticmethod
    def _read_indexed_payload(path: Path, schema_version: str) -> Optional[Dict[str, Any]]:
        payload = _read_json(path)
        if not isinstance(payload, dict):
            return None
        if str(payload.get("schema_version", "") or "") != schema_version:
            return None
        return payload

    @staticmethod
    def _normalize_index_payload(payload: Optional[Dict[str, Any]], *, expected_pairs: Dict[str, str]) -> Optional[Dict[str, Any]]:
        if not isinstance(payload, dict):
            return None
        for field_name, expected_value in expected_pairs.items():
            if str(payload.get(field_name, "") or "") != str(expected_value or ""):
                return None
        return payload

    @classmethod
    def _matching_index_payload_candidates(
        cls,
        payload: Optional[Dict[str, Any]],
        *,
        expected_pairs: Dict[str, str],
    ) -> List[Dict[str, Any]]:
        matched: List[Dict[str, Any]] = []
        for candidate in cls._payload_candidates_with_previous(payload):
            normalized_candidate = cls._normalize_index_payload(candidate, expected_pairs=expected_pairs)
            if normalized_candidate is None:
                continue
            matched.append(dict(normalized_candidate))
        return matched

    def _write_llm_lookup_index(
        self,
        *,
        stage: str,
        chunk_id: str,
        llm_call_id: str,
        input_fingerprint: str,
        attempt: int,
        attempt_dir: Path,
        response_hash: str,
        committed_parts: int,
    ) -> Dict[str, Any]:
        index_path = self._llm_lookup_index_path(
            stage=stage,
            chunk_id=chunk_id,
            llm_call_id=llm_call_id,
            input_fingerprint=input_fingerprint,
        )
        existing_payload = self._read_indexed_payload(index_path, _LLM_LOOKUP_INDEX_SCHEMA_VERSION)
        payload = {
            "schema_version": _LLM_LOOKUP_INDEX_SCHEMA_VERSION,
            "stage": str(stage or ""),
            "chunk_id": str(chunk_id or ""),
            "llm_call_id": str(llm_call_id or ""),
            "input_fingerprint": str(input_fingerprint or ""),
            "attempt": int(attempt or 0),
            "attempt_dir": str(attempt_dir),
            "response_hash": str(response_hash or ""),
            "committed_parts": int(committed_parts or 0),
        }
        payload = self._attach_previous_record(current_payload=payload, existing_payload=existing_payload)
        _write_json_atomic_sync(index_path, payload)
        return payload

    def _write_llm_attempt_index(
        self,
        *,
        stage: str,
        chunk_id: str,
        llm_call_id: str,
        latest_attempt: int,
        attempt_dir: Path,
    ) -> Dict[str, Any]:
        index_path = self._llm_attempt_index_path(stage=stage, chunk_id=chunk_id, llm_call_id=llm_call_id)
        existing_payload = self._read_indexed_payload(index_path, _LLM_ATTEMPT_INDEX_SCHEMA_VERSION)
        payload = {
            "schema_version": _LLM_ATTEMPT_INDEX_SCHEMA_VERSION,
            "stage": str(stage or ""),
            "chunk_id": str(chunk_id or ""),
            "llm_call_id": str(llm_call_id or ""),
            "latest_attempt": int(latest_attempt or 0),
            "attempt_dir": str(attempt_dir),
        }
        payload = self._attach_previous_record(current_payload=payload, existing_payload=existing_payload)
        _write_json_atomic_sync(index_path, payload)
        return payload

    def _write_chunk_lookup_index(
        self,
        *,
        stage: str,
        chunk_id: str,
        input_fingerprint: str,
        chunk_dir: Path,
        attempt: int,
        result_hash: str,
    ) -> Dict[str, Any]:
        index_path = self._chunk_lookup_index_path(stage=stage, chunk_id=chunk_id, input_fingerprint=input_fingerprint)
        existing_payload = self._read_indexed_payload(index_path, _CHUNK_LOOKUP_INDEX_SCHEMA_VERSION)
        payload = {
            "schema_version": _CHUNK_LOOKUP_INDEX_SCHEMA_VERSION,
            "stage": str(stage or ""),
            "chunk_id": str(chunk_id or ""),
            "input_fingerprint": str(input_fingerprint or ""),
            "chunk_dir": str(chunk_dir),
            "attempt": int(attempt or 0),
            "result_hash": str(result_hash or ""),
        }
        payload = self._attach_previous_record(current_payload=payload, existing_payload=existing_payload)
        _write_json_atomic_sync(index_path, payload)
        return payload

    def _task_stage_redis_key(self, stage: str) -> str:
        return self._task_scope_key("stage", stage)

    def _task_events_redis_key(self) -> str:
        return self._task_scope_key("events")

    def _task_meta_redis_key(self) -> str:
        return self._task_scope_key("meta")

    def _storage_llm_redis_key(self, *, stage: str, chunk_id: str, llm_call_id: str, attempt: int) -> str:
        return self._storage_scope_key(stage, "llm", chunk_id, llm_call_id, f"a{int(attempt or 0):03d}")

    def _storage_chunk_redis_key(self, *, stage: str, chunk_id: str) -> str:
        return self._storage_scope_key(stage, "chunk", chunk_id)

    def update_task_meta(
        self,
        *,
        output_dir: Optional[str] = None,
        storage_key: Optional[str] = None,
        normalized_video_key: Optional[str] = None,
        extra_payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if output_dir is not None:
            normalized_output_dir = str(output_dir or "").strip()
            if normalized_output_dir:
                self.output_dir = Path(normalized_output_dir).resolve()
        if storage_key is not None:
            normalized_storage_key = str(storage_key or "").strip()
            if normalized_storage_key:
                self.storage_key = normalized_storage_key
        if normalized_video_key is not None:
            self.normalized_video_key = str(normalized_video_key or "").strip()

        payload: Dict[str, Any] = {
            "schema_version": "runtime_task_meta_v1",
            "task_id": self.task_id,
            "storage_key": self.storage_key,
            "output_dir": str(self.output_dir),
            "normalized_video_key": self.normalized_video_key,
            "updated_at_ms": _now_ms(),
        }
        if isinstance(extra_payload, dict):
            payload.update(extra_payload)
        if self._should_write_task_meta_file():
            _write_json_atomic_sync(self._task_meta_path(), payload)
        self._update_resume_task_meta()
        self._redis.hset(self._task_meta_redis_key(), payload)
        return payload

    def build_llm_call_id(self, *, step_name: str, unit_id: str, input_fingerprint: str) -> str:
        step_slug = _sanitize_runtime_segment(step_name, fallback="call", max_length=12).lower()
        unit_slug = _sanitize_runtime_segment(unit_id, fallback="unit", max_length=12).lower()
        if unit_slug == step_slug or unit_slug.startswith(f"{step_slug}_") or unit_slug.startswith(f"{step_slug}."):
            unit_slug = ""
        suffix = _sha256_text(f"{step_name}|{unit_id}|{input_fingerprint}")[:8]
        if unit_slug:
            return f"{step_slug}.{unit_slug}.h{suffix}"
        return f"{step_slug}.h{suffix}"

    def build_chunk_id(self, *, chunk_index: int, prefix: str = "c") -> str:
        safe_prefix = str(prefix or "c").strip() or "c"
        return f"{safe_prefix}{int(chunk_index) + 1:06d}"

    def build_projection_chunk_id(self, *, projection_name: str) -> str:
        safe_name = _sanitize_runtime_segment(projection_name, fallback="projection", max_length=48).lower()
        return f"proj.{safe_name}"

    def stage_dir(self, stage: str, *, ensure_exists: bool = False) -> Path:
        safe_stage = str(stage or "unknown").strip() or "unknown"
        path = self.runtime_root / "stage" / safe_stage
        if ensure_exists:
            path.mkdir(parents=True, exist_ok=True)
        return path

    def _legacy_stage_dir(self, stage: str) -> Path:
        safe_stage = str(stage or "unknown").strip() or "unknown"
        return self.runtime_root / "s" / safe_stage

    def chunk_dir(self, *, stage: str, chunk_id: str, ensure_exists: bool = False) -> Path:
        safe_chunk_id = str(chunk_id or "unknown_chunk").strip() or "unknown_chunk"
        path = self.stage_dir(stage, ensure_exists=ensure_exists) / "chunk" / safe_chunk_id
        if ensure_exists:
            path.mkdir(parents=True, exist_ok=True)
        return path

    def _legacy_chunk_dir(self, *, stage: str, chunk_id: str) -> Path:
        safe_chunk_id = str(chunk_id or "unknown_chunk").strip() or "unknown_chunk"
        return self._legacy_stage_dir(stage) / "c" / safe_chunk_id

    def _llm_calls_dir(self, *, stage: str, chunk_id: str, ensure_exists: bool = False) -> Path:
        path = self.chunk_dir(stage=stage, chunk_id=chunk_id, ensure_exists=ensure_exists) / "call"
        if ensure_exists:
            path.mkdir(parents=True, exist_ok=True)
        return path

    def _legacy_llm_root(self, *, stage: str, chunk_id: str, llm_call_id: str) -> Path:
        return self._legacy_chunk_dir(stage=stage, chunk_id=chunk_id) / "l" / str(llm_call_id or "")

    def build_scope_ref(
        self,
        *,
        stage: str,
        scope_type: str,
        scope_id: str,
        scope_variant: str = "",
    ) -> str:
        safe_stage = _sanitize_runtime_segment(stage, fallback="stage", max_length=32).lower()
        safe_type = _sanitize_runtime_segment(scope_type, fallback="scope", max_length=24).lower()
        safe_id = _sanitize_runtime_segment(scope_id, fallback="unknown", max_length=96)
        scope_ref = f"{safe_stage}/{safe_type}/{safe_id}"
        safe_variant = _sanitize_runtime_segment(scope_variant, fallback="", max_length=32).lower()
        if safe_variant:
            scope_ref = f"{scope_ref}@{safe_variant}"
        return scope_ref

    def build_substage_scope_id(
        self,
        *,
        substage_name: str,
        wave_id: str = "wave_0001",
    ) -> str:
        safe_substage = _sanitize_runtime_segment(substage_name, fallback="substage", max_length=64).lower()
        safe_wave = _sanitize_runtime_segment(wave_id, fallback="wave_0001", max_length=32).lower()
        return f"{safe_substage}.{safe_wave}"

    def build_substage_scope_ref(
        self,
        *,
        stage: str,
        substage_name: str,
        wave_id: str = "wave_0001",
        scope_variant: str = "",
    ) -> str:
        return self.build_scope_ref(
            stage=stage,
            scope_type=SCOPE_TYPE_SUBSTAGE,
            scope_id=self.build_substage_scope_id(substage_name=substage_name, wave_id=wave_id),
            scope_variant=scope_variant,
        )

    def transition_scope_node(
        self,
        *,
        stage: str,
        scope_type: str,
        scope_id: str,
        status: str,
        scope_ref: str = "",
        scope_variant: str = "",
        input_fingerprint: str = "",
        local_path: str = "",
        dependency_fingerprints: Optional[Dict[str, str]] = None,
        chunk_id: str = "",
        unit_id: str = "",
        stage_step: str = "",
        plan_context: Optional[Dict[str, Any]] = None,
        resource_snapshot: Optional[Dict[str, Any]] = None,
        attempt_count: Optional[int] = None,
        result_hash: Optional[str] = None,
        retry_mode: Optional[str] = None,
        retry_entry_point: Optional[str] = None,
        required_action: Optional[str] = None,
        error_class: Optional[str] = None,
        error_code: Optional[str] = None,
        error_message: Optional[str] = None,
        extra_payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        normalized_stage = str(stage or "").strip()
        normalized_scope_type = str(scope_type or "").strip()
        normalized_scope_id = str(scope_id or "").strip()
        normalized_status = _normalize_scope_current_status(status)
        normalized_scope_ref = str(scope_ref or "").strip() or self.build_scope_ref(
            stage=normalized_stage,
            scope_type=normalized_scope_type,
            scope_id=normalized_scope_id,
            scope_variant=scope_variant,
        )
        existing_payload = self.load_scope_node(normalized_scope_ref)
        node_payload: Dict[str, Any] = dict(existing_payload or {})
        if isinstance(extra_payload, dict):
            node_payload.update(extra_payload)
        node_payload.update(
            {
                "scope_ref": normalized_scope_ref,
                "stage": normalized_stage or str(node_payload.get("stage", "") or ""),
                "scope_type": normalized_scope_type or str(node_payload.get("scope_type", "") or ""),
                "scope_id": normalized_scope_id or str(node_payload.get("scope_id", "") or ""),
                "scope_variant": str(scope_variant or node_payload.get("scope_variant", "") or ""),
                "status": normalized_status or str(node_payload.get("status", "") or ""),
                "updated_at_ms": _now_ms(),
            }
        )
        if input_fingerprint not in (None, ""):
            node_payload["input_fingerprint"] = str(input_fingerprint or "")
        if local_path not in (None, ""):
            node_payload["local_path"] = str(local_path or "")
        if chunk_id not in (None, ""):
            node_payload["chunk_id"] = str(chunk_id or "")
        if unit_id not in (None, ""):
            node_payload["unit_id"] = str(unit_id or "")
        if stage_step not in (None, ""):
            node_payload["stage_step"] = str(stage_step or "")
        if isinstance(plan_context, dict):
            node_payload["plan_context"] = dict(plan_context)
        if isinstance(resource_snapshot, dict):
            node_payload["resource_snapshot"] = dict(resource_snapshot)
        if attempt_count is not None:
            node_payload["attempt_count"] = max(0, int(attempt_count or 0))
        if result_hash is not None:
            node_payload["result_hash"] = str(result_hash or "")

        if normalized_status in {STATUS_PLANNED, STATUS_RUNNING, STATUS_SUCCESS}:
            for field_name in (
                "retry_mode",
                "retry_entry_point",
                "required_action",
                "error_class",
                "error_code",
                "error_message",
            ):
                node_payload[field_name] = ""
        if retry_mode is not None:
            node_payload["retry_mode"] = str(retry_mode or "")
        if retry_entry_point is not None:
            node_payload["retry_entry_point"] = str(retry_entry_point or "")
        if required_action is not None:
            node_payload["required_action"] = str(required_action or "")
        if error_class is not None:
            node_payload["error_class"] = str(error_class or "")
        if error_code is not None:
            node_payload["error_code"] = str(error_code or "")
        if error_message is not None:
            node_payload["error_message"] = str(error_message or "")

        return self.upsert_scope_node(
            scope_ref=normalized_scope_ref,
            stage=str(node_payload.get("stage", "") or ""),
            scope_type=str(node_payload.get("scope_type", "") or ""),
            scope_id=str(node_payload.get("scope_id", "") or ""),
            scope_variant=str(node_payload.get("scope_variant", "") or ""),
            status=str(node_payload.get("status", "") or ""),
            input_fingerprint=str(node_payload.get("input_fingerprint", "") or ""),
            local_path=str(node_payload.get("local_path", "") or ""),
            dependency_fingerprints=(
                dependency_fingerprints
                if dependency_fingerprints is not None
                else node_payload.get("dependency_fingerprints")
            ),
            extra_payload=node_payload,
        )

    def plan_substage_scope(
        self,
        *,
        stage: str,
        substage_name: str,
        wave_id: str = "wave_0001",
        scope_variant: str = "",
        input_fingerprint: str = "",
        local_path: str = "",
        dependency_fingerprints: Optional[Dict[str, str]] = None,
        plan_context: Optional[Dict[str, Any]] = None,
        attempt_count: int = 0,
        extra_payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        normalized_substage_name = str(substage_name or "").strip()
        normalized_wave_id = str(wave_id or "").strip() or "wave_0001"
        merged_plan_context: Dict[str, Any] = {
            "substage_name": normalized_substage_name,
            "wave_id": normalized_wave_id,
        }
        if isinstance(plan_context, dict):
            merged_plan_context.update(plan_context)
        merged_payload = dict(extra_payload or {})
        merged_payload.setdefault("substage_name", normalized_substage_name)
        merged_payload.setdefault("wave_id", normalized_wave_id)
        return self.transition_scope_node(
            scope_ref=self.build_substage_scope_ref(
                stage=stage,
                substage_name=normalized_substage_name,
                wave_id=normalized_wave_id,
                scope_variant=scope_variant,
            ),
            stage=stage,
            scope_type=SCOPE_TYPE_SUBSTAGE,
            scope_id=self.build_substage_scope_id(
                substage_name=normalized_substage_name,
                wave_id=normalized_wave_id,
            ),
            scope_variant=scope_variant,
            status=STATUS_PLANNED,
            input_fingerprint=input_fingerprint,
            local_path=local_path,
            dependency_fingerprints=dependency_fingerprints,
            stage_step=normalized_substage_name,
            plan_context=merged_plan_context,
            attempt_count=max(0, int(attempt_count or 0)),
            extra_payload=merged_payload,
        )

    def plan_llm_call_scope(
        self,
        *,
        stage: str,
        chunk_id: str,
        llm_call_id: str,
        input_fingerprint: str,
        request_payload: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        scope_variant: str = "",
        local_path: str = "",
        dependency_fingerprints: Optional[Dict[str, str]] = None,
        attempt_count: int = 0,
        extra_payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        metadata_payload = dict(metadata or {})
        existing_scope = self.load_scope_node(
            self.build_scope_ref(
                stage=stage,
                scope_type="llm_call",
                scope_id=llm_call_id,
                scope_variant=scope_variant,
            )
        )
        existing_status = _normalize_runtime_status((existing_scope or {}).get("status", ""))
        if existing_status and existing_status != STATUS_PLANNED:
            return dict(existing_scope or {})
        merged_plan_context = _build_scope_plan_context(
            stage=stage,
            scope_type="llm_call",
            scope_id=llm_call_id,
            input_fingerprint=input_fingerprint,
            metadata=metadata_payload,
            request_payload=request_payload,
            existing_context=(existing_scope or {}).get("plan_context"),
        )
        merged_plan_context["chunk_id"] = str(chunk_id or "")
        merged_plan_context["llm_call_id"] = str(llm_call_id or "")
        merged_plan_context["planned_attempt"] = max(1, int(attempt_count or 0) + 1)
        merged_payload = dict(extra_payload or {})
        merged_payload.setdefault("chunk_id", str(chunk_id or ""))
        for field_name in ("provider", "request_name", "stage_step", "scope_variant", "unit_id"):
            field_value = metadata_payload.get(field_name)
            if field_value in (None, "", []):
                continue
            merged_payload.setdefault(field_name, field_value)
        return self.transition_scope_node(
            scope_ref=self.build_scope_ref(
                stage=stage,
                scope_type="llm_call",
                scope_id=llm_call_id,
                scope_variant=scope_variant,
            ),
            stage=stage,
            scope_type="llm_call",
            scope_id=llm_call_id,
            scope_variant=scope_variant,
            status=STATUS_PLANNED,
            input_fingerprint=input_fingerprint,
            local_path=local_path,
            dependency_fingerprints=dependency_fingerprints or metadata_payload.get("dependency_fingerprints"),
            chunk_id=str(chunk_id or ""),
            unit_id=str(metadata_payload.get("unit_id", "") or ""),
            stage_step=str(
                metadata_payload.get("stage_step", metadata_payload.get("step_name", metadata_payload.get("request_name", "")))
                or ""
            ),
            plan_context=merged_plan_context,
            attempt_count=max(0, int(attempt_count or 0)),
            extra_payload=merged_payload,
        )

    def plan_chunk_scope(
        self,
        *,
        stage: str,
        chunk_id: str,
        input_fingerprint: str,
        metadata: Optional[Dict[str, Any]] = None,
        scope_variant: str = "",
        local_path: str = "",
        dependency_fingerprints: Optional[Dict[str, str]] = None,
        attempt_count: int = 0,
        extra_payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        metadata_payload = dict(metadata or {})
        existing_scope = self.load_scope_node(
            self.build_scope_ref(
                stage=stage,
                scope_type="chunk",
                scope_id=chunk_id,
                scope_variant=scope_variant,
            )
        )
        existing_status = _normalize_runtime_status((existing_scope or {}).get("status", ""))
        if existing_status and existing_status != STATUS_PLANNED:
            return dict(existing_scope or {})
        merged_plan_context = _build_scope_plan_context(
            stage=stage,
            scope_type="chunk",
            scope_id=chunk_id,
            input_fingerprint=input_fingerprint,
            metadata=metadata_payload,
            existing_context=(existing_scope or {}).get("plan_context"),
        )
        merged_plan_context["chunk_id"] = str(chunk_id or "")
        merged_plan_context["planned_attempt"] = max(1, int(attempt_count or 0) + 1)
        merged_payload = dict(extra_payload or {})
        merged_payload.setdefault("chunk_id", str(chunk_id or ""))
        for field_name in ("scope_variant", "unit_id", "stage_step"):
            field_value = metadata_payload.get(field_name)
            if field_value in (None, "", []):
                continue
            merged_payload.setdefault(field_name, field_value)
        return self.transition_scope_node(
            scope_ref=self.build_scope_ref(
                stage=stage,
                scope_type="chunk",
                scope_id=chunk_id,
                scope_variant=scope_variant,
            ),
            stage=stage,
            scope_type="chunk",
            scope_id=chunk_id,
            scope_variant=scope_variant,
            status=STATUS_PLANNED,
            input_fingerprint=input_fingerprint,
            local_path=local_path,
            dependency_fingerprints=dependency_fingerprints or metadata_payload.get("dependency_fingerprints"),
            chunk_id=str(chunk_id or ""),
            unit_id=str(metadata_payload.get("unit_id", "") or ""),
            stage_step=str(metadata_payload.get("stage_step", metadata_payload.get("step_name", "")) or ""),
            plan_context=merged_plan_context,
            attempt_count=max(0, int(attempt_count or 0)),
            extra_payload=merged_payload,
        )

    def requeue_scope_node(
        self,
        scope_ref: str,
        *,
        reason: str = "",
        plan_context: Optional[Dict[str, Any]] = None,
        resource_snapshot: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        normalized_scope_ref = str(scope_ref or "").strip()
        if not normalized_scope_ref:
            return None
        existing_payload = self.load_scope_node(normalized_scope_ref)
        if not isinstance(existing_payload, dict):
            return None
        merged_plan_context = dict(existing_payload.get("plan_context", {}) or {})
        if isinstance(plan_context, dict):
            merged_plan_context.update(plan_context)
        if reason:
            merged_plan_context["requeue_reason"] = str(reason or "")
        merged_resource_snapshot = dict(existing_payload.get("resource_snapshot", {}) or {})
        if isinstance(resource_snapshot, dict):
            merged_resource_snapshot.update(resource_snapshot)
        if reason:
            merged_resource_snapshot["last_requeue_reason"] = str(reason or "")
        return self.transition_scope_node(
            scope_ref=normalized_scope_ref,
            stage=str(existing_payload.get("stage", "") or ""),
            scope_type=str(existing_payload.get("scope_type", "") or ""),
            scope_id=str(existing_payload.get("scope_id", "") or ""),
            scope_variant=str(existing_payload.get("scope_variant", "") or ""),
            status=STATUS_PLANNED,
            input_fingerprint=str(existing_payload.get("input_fingerprint", "") or ""),
            local_path=str(existing_payload.get("local_path", "") or ""),
            dependency_fingerprints=existing_payload.get("dependency_fingerprints"),
            chunk_id=str(existing_payload.get("chunk_id", "") or ""),
            unit_id=str(existing_payload.get("unit_id", "") or ""),
            stage_step=str(existing_payload.get("stage_step", "") or ""),
            plan_context=merged_plan_context,
            resource_snapshot=merged_resource_snapshot,
            attempt_count=int(existing_payload.get("attempt_count", 0) or 0),
            result_hash=str(existing_payload.get("result_hash", "") or ""),
            extra_payload={
                "requeued_from_status": str(existing_payload.get("status", "") or ""),
                "requeued_at_ms": _now_ms(),
            },
        )

    def reset_running_scopes_to_planned(
        self,
        *,
        stage: str = "",
        scope_type: str = "",
        reason: str = "runtime_interrupted",
    ) -> List[str]:
        affected_scope_refs: List[str] = []
        for node_payload in self.list_scope_nodes(stage=stage, scope_type=scope_type):
            if not isinstance(node_payload, dict):
                continue
            status_value = str(node_payload.get("status", "") or "").strip().upper()
            if _normalize_runtime_status(status_value) != STATUS_RUNNING:
                continue
            updated = self.requeue_scope_node(
                str(node_payload.get("scope_ref", "") or ""),
                reason=reason,
                resource_snapshot={
                    "interrupted_status": status_value,
                    "interrupted_at_ms": _now_ms(),
                },
            )
            if isinstance(updated, dict):
                affected_scope_refs.append(str(updated.get("scope_ref", "") or ""))
        return affected_scope_refs

    def _scope_graph_path(self) -> Path:
        return self.runtime_root / "scope_graph.json"

    def _stage_state_file_path(self, stage: str) -> Path:
        return self.stage_dir(stage) / "stage_state.json"

    def load_stage_snapshot(self, *, stage: str) -> Optional[Dict[str, Any]]:
        normalized_stage = str(stage or "").strip()
        if not normalized_stage:
            return None
        payload: Optional[Dict[str, Any]] = None
        if self._sqlite_index is not None:
            try:
                payload = self._sqlite_index.load_stage_snapshot(
                    output_dir=str(self.output_dir),
                    stage=normalized_stage,
                )
            except Exception as error:
                logger.warning("Runtime recovery SQLite stage snapshot load failed: %s", error)
        if not isinstance(payload, dict):
            payload = _read_json(self._stage_state_file_path(normalized_stage))
        if not isinstance(payload, dict):
            payload = _read_json(self._legacy_stage_dir(normalized_stage) / "stage_state.json")
        if not isinstance(payload, dict):
            return None
        return dict(payload)

    def _load_stage_retry_context(self, stage: str) -> Dict[str, Any]:
        payload = self.load_stage_snapshot(stage=stage)
        if not isinstance(payload, dict):
            return {}
        return {
            "status": str(payload.get("status", "") or ""),
            "retry_mode": str(payload.get("retry_mode", payload.get("retryMode", "")) or ""),
            "retry_entry_point": str(payload.get("retry_entry_point", payload.get("retryEntryPoint", "")) or ""),
            "required_action": str(payload.get("required_action", payload.get("requiredAction", "")) or ""),
            "error_class": str(payload.get("error_class", "") or ""),
            "error_code": str(payload.get("error_code", "") or ""),
            "error_message": str(payload.get("error_message", "") or ""),
        }

    def _sync_scope_hint_from_node(
        self,
        node_payload: Optional[Dict[str, Any]],
        *,
        stage_retry_context: Optional[Dict[str, Any]] = None,
    ) -> None:
        if self._sqlite_index is None or not isinstance(node_payload, dict):
            return
        scope_type = str(node_payload.get("scope_type", "") or "").strip()
        if scope_type not in {"llm_call", "chunk"}:
            return
        stage_name = str(node_payload.get("stage", "") or "").strip()
        if not stage_name:
            return
        retry_context = dict(stage_retry_context or {})
        if not retry_context:
            retry_context = self._load_stage_retry_context(stage_name)
        scope_status = str(node_payload.get("status", "") or "").strip()
        dirty_reason = str(node_payload.get("dirty_reason", "") or "").strip()
        lowered_dirty_reason = dirty_reason.lower()
        retry_mode = str(node_payload.get("retry_mode", node_payload.get("retryMode", "")) or "").strip()
        if not retry_mode and scope_status == SCOPE_STATUS_DIRTY:
            if "fallback" in lowered_dirty_reason:
                retry_mode = "fallback"
            else:
                retry_mode = str(retry_context.get("retry_mode", "") or "").strip()
        retry_entry_point = str(
            node_payload.get("retry_entry_point", node_payload.get("retryEntryPoint", "")) or ""
        ).strip()
        if not retry_entry_point and scope_status == SCOPE_STATUS_DIRTY:
            retry_entry_point = str(retry_context.get("retry_entry_point", "") or "").strip()
            if not retry_entry_point and "fallback" in lowered_dirty_reason:
                retry_entry_point = "fallback_repair:from_scope_hint"
        required_action = str(
            node_payload.get("required_action", node_payload.get("requiredAction", "")) or ""
        ).strip()
        if not required_action and scope_status == SCOPE_STATUS_DIRTY:
            required_action = str(retry_context.get("required_action", "") or "").strip()
        error_class = str(node_payload.get("error_class", "") or "").strip()
        error_code = str(node_payload.get("error_code", "") or "").strip()
        error_message = str(node_payload.get("error_message", "") or "").strip()
        if scope_status == SCOPE_STATUS_DIRTY and not error_class:
            error_class = str(retry_context.get("error_class", "") or "").strip()
            error_code = str(retry_context.get("error_code", "") or "").strip()
            error_message = str(retry_context.get("error_message", "") or "").strip()
        chunk_id = ""
        llm_call_id = ""
        scope_id = str(node_payload.get("scope_id", "") or "").strip()
        if scope_type == "chunk":
            chunk_id = scope_id
        elif scope_type == "llm_call":
            llm_call_id = scope_id
            chunk_id = str(node_payload.get("chunk_id", "") or "").strip()
        latest_attempt = 0
        try:
            latest_attempt = int(node_payload.get("attempt", 0) or 0)
        except Exception:
            latest_attempt = 0
        self._sqlite_index.upsert_scope_hint(
            output_dir=str(self.output_dir),
            task_id=self.task_id,
            storage_key=self.storage_key,
            normalized_video_key=self.normalized_video_key,
            stage=stage_name,
            scope_type=scope_type,
            scope_id=scope_id,
            scope_ref=str(node_payload.get("scope_ref", "") or ""),
            scope_variant=str(node_payload.get("scope_variant", "") or ""),
            chunk_id=chunk_id,
            llm_call_id=llm_call_id,
            unit_id=str(node_payload.get("unit_id", "") or ""),
            stage_step=str(node_payload.get("stage_step", "") or ""),
            status=scope_status,
            input_fingerprint=str(node_payload.get("input_fingerprint", "") or ""),
            dependency_fingerprints=node_payload.get("dependency_fingerprints"),
            depends_on=list(node_payload.get("depends_on", []) or []),
            dirty_reason=dirty_reason,
            retry_mode=retry_mode,
            retry_entry_point=retry_entry_point,
            required_action=required_action,
            error_class=error_class,
            error_code=error_code,
            error_message=error_message,
            latest_attempt=latest_attempt,
            local_path=str(node_payload.get("local_path", "") or ""),
            source_kind="scope_graph",
            updated_at_ms=int(node_payload.get("updated_at_ms", _now_ms()) or _now_ms()),
        )

    def sync_scope_hints_from_scope_graph(self, *, stage: str = "") -> int:
        if self._sqlite_index is None:
            return 0
        normalized_stage = str(stage or "").strip()
        synced_count = 0
        stage_context_cache: Dict[str, Dict[str, Any]] = {}
        for node_payload in self._sqlite_index.list_scope_nodes(
            output_dir=str(self.output_dir),
            stage=normalized_stage,
            limit=50000,
        ):
            if not isinstance(node_payload, dict):
                continue
            stage_name = str(node_payload.get("stage", "") or "").strip()
            if stage_name not in stage_context_cache:
                stage_context_cache[stage_name] = self._load_stage_retry_context(stage_name)
            self._sync_scope_hint_from_node(
                node_payload,
                stage_retry_context=stage_context_cache.get(stage_name, {}),
            )
            if str(node_payload.get("scope_type", "") or "").strip() in {"llm_call", "chunk"}:
                synced_count += 1
        return synced_count

    def _empty_scope_graph_payload(self) -> Dict[str, Any]:
        return {
            "schema_version": _SCOPE_GRAPH_SCHEMA_VERSION,
            "task_id": self.task_id,
            "storage_key": self.storage_key,
            "nodes": {},
            "updated_at_ms": _now_ms(),
        }

    def _read_scope_graph(self) -> Dict[str, Any]:
        payload = _read_json(self._scope_graph_path())
        if not isinstance(payload, dict):
            return self._empty_scope_graph_payload()
        if payload.get("schema_version") != _SCOPE_GRAPH_SCHEMA_VERSION:
            return self._empty_scope_graph_payload()
        nodes = payload.get("nodes")
        if not isinstance(nodes, dict):
            payload["nodes"] = {}
        return payload

    def _write_scope_graph(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        normalized = dict(payload or {})
        normalized["schema_version"] = _SCOPE_GRAPH_SCHEMA_VERSION
        normalized["task_id"] = self.task_id
        normalized["storage_key"] = self.storage_key
        normalized["updated_at_ms"] = _now_ms()
        nodes = normalized.get("nodes")
        if not isinstance(nodes, dict):
            normalized["nodes"] = {}
        _write_json_atomic_sync(self._scope_graph_path(), normalized)
        return normalized

    @staticmethod
    def _normalize_dependency_fingerprints(payload: Any) -> Dict[str, str]:
        normalized: Dict[str, str] = {}
        if not isinstance(payload, dict):
            return normalized
        for raw_ref, raw_fingerprint in payload.items():
            scope_ref = str(raw_ref or "").strip()
            fingerprint = str(raw_fingerprint or "").strip()
            if not scope_ref or not fingerprint:
                continue
            normalized[scope_ref] = fingerprint
        return dict(sorted(normalized.items()))

    def load_scope_node(self, scope_ref: str) -> Optional[Dict[str, Any]]:
        normalized_scope_ref = str(scope_ref or "").strip()
        if not normalized_scope_ref:
            return None
        if self._sqlite_index is not None:
            try:
                payload = self._sqlite_index.load_scope_node(
                    output_dir=str(self.output_dir),
                    scope_ref=normalized_scope_ref,
                )
                return dict(payload) if isinstance(payload, dict) else None
            except Exception as error:
                logger.warning("Runtime recovery SQLite scope node load failed: %s", error)
        graph_payload = self._read_scope_graph()
        nodes = graph_payload.get("nodes", {})
        node_payload = nodes.get(normalized_scope_ref)
        return dict(node_payload) if isinstance(node_payload, dict) else None

    def list_scope_nodes(
        self,
        *,
        stage: str = "",
        scope_type: str = "",
    ) -> List[Dict[str, Any]]:
        normalized_stage = str(stage or "").strip()
        normalized_scope_type = str(scope_type or "").strip()
        if self._sqlite_index is not None:
            try:
                return self._sqlite_index.list_scope_nodes(
                    output_dir=str(self.output_dir),
                    stage=normalized_stage,
                    scope_type=normalized_scope_type,
                    limit=50000,
                )
            except Exception as error:
                logger.warning("Runtime recovery SQLite scope node list failed: %s", error)
        graph_payload = self._read_scope_graph()
        nodes = graph_payload.get("nodes", {})
        collected: List[Dict[str, Any]] = []
        for scope_ref, node_payload in list(nodes.items()):
            if not isinstance(node_payload, dict):
                continue
            if normalized_stage and str(node_payload.get("stage", "") or "").strip() != normalized_stage:
                continue
            if normalized_scope_type and str(node_payload.get("scope_type", "") or "").strip() != normalized_scope_type:
                continue
            normalized_payload = dict(node_payload)
            normalized_payload.setdefault("scope_ref", str(scope_ref or ""))
            collected.append(normalized_payload)
        collected.sort(
            key=lambda item: (
                str(item.get("stage", "") or ""),
                str(item.get("scope_type", "") or ""),
                str(item.get("scope_id", "") or ""),
                int(item.get("updated_at_ms", 0) or 0),
            )
        )
        return collected

    def upsert_scope_node(
        self,
        *,
        scope_ref: str,
        stage: str,
        scope_type: str,
        scope_id: str,
        status: str,
        input_fingerprint: str = "",
        local_path: str = "",
        scope_variant: str = "",
        dependency_fingerprints: Optional[Dict[str, str]] = None,
        extra_payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        normalized_scope_ref = str(scope_ref or "").strip()
        if not normalized_scope_ref:
            raise ValueError("scope_ref must not be empty")

        previous_payload = self.load_scope_node(normalized_scope_ref)
        if not isinstance(previous_payload, dict):
            previous_payload = {}

        node_payload: Dict[str, Any] = dict(previous_payload)
        node_payload.update(
            {
                "scope_ref": normalized_scope_ref,
                "task_id": self.task_id,
                "storage_key": self.storage_key,
                "stage": str(stage or node_payload.get("stage", "") or ""),
                "scope_type": str(scope_type or node_payload.get("scope_type", "") or ""),
                "scope_id": str(scope_id or node_payload.get("scope_id", "") or ""),
                "scope_variant": str(scope_variant or node_payload.get("scope_variant", "") or ""),
                "status": _normalize_scope_current_status(status or node_payload.get("status", "") or ""),
                "updated_at_ms": _now_ms(),
            }
        )
        if input_fingerprint not in (None, ""):
            node_payload["input_fingerprint"] = str(input_fingerprint or "")
        if local_path not in (None, ""):
            node_payload["local_path"] = str(local_path or "")

        normalized_dependencies = self._normalize_dependency_fingerprints(
            dependency_fingerprints if dependency_fingerprints is not None else node_payload.get("dependency_fingerprints")
        )
        node_payload["dependency_fingerprints"] = normalized_dependencies
        node_payload["depends_on"] = sorted(normalized_dependencies.keys())

        if isinstance(extra_payload, dict):
            node_payload.update(extra_payload)

        if str(status or "").strip().upper() != SCOPE_STATUS_DIRTY:
            node_payload.pop("dirty_reason", None)
            node_payload.pop("dirty_at_ms", None)

        if self._sqlite_index is not None:
            try:
                self._sqlite_index.upsert_scope_node(
                    output_dir=str(self.output_dir),
                    task_id=self.task_id,
                    storage_key=self.storage_key,
                    normalized_video_key=self.normalized_video_key,
                    scope_ref=normalized_scope_ref,
                    stage=str(node_payload.get("stage", "") or ""),
                    scope_type=str(node_payload.get("scope_type", "") or ""),
                    scope_id=str(node_payload.get("scope_id", "") or ""),
                    scope_variant=str(node_payload.get("scope_variant", "") or ""),
                    status=str(node_payload.get("status", "") or ""),
                    input_fingerprint=str(node_payload.get("input_fingerprint", "") or ""),
                    local_path=str(node_payload.get("local_path", "") or ""),
                    updated_at_ms=int(node_payload.get("updated_at_ms", _now_ms()) or _now_ms()),
                    dependency_fingerprints=node_payload.get("dependency_fingerprints"),
                    depends_on=node_payload.get("depends_on"),
                    payload=node_payload,
                )
            except Exception as error:
                logger.warning("Runtime recovery SQLite scope node upsert failed: %s", error)
                graph_payload = self._read_scope_graph()
                nodes = graph_payload.setdefault("nodes", {})
                nodes[normalized_scope_ref] = node_payload
                self._write_scope_graph(graph_payload)
        else:
            graph_payload = self._read_scope_graph()
            nodes = graph_payload.setdefault("nodes", {})
            nodes[normalized_scope_ref] = node_payload
            self._write_scope_graph(graph_payload)
        self._sync_scope_hint_from_node(node_payload)
        return dict(node_payload)

    def build_dirty_scope_plan(self, seed_scope_refs: List[str]) -> Dict[str, Any]:
        normalized_seeds = []
        seen_seeds = set()
        for item in list(seed_scope_refs or []):
            scope_ref = str(item or "").strip()
            if not scope_ref or scope_ref in seen_seeds:
                continue
            seen_seeds.add(scope_ref)
            normalized_seeds.append(scope_ref)

        node_rows = self.list_scope_nodes()
        nodes = {
            str(node_payload.get("scope_ref", "") or "").strip(): node_payload
            for node_payload in node_rows
            if isinstance(node_payload, dict) and str(node_payload.get("scope_ref", "") or "").strip()
        }
        reverse_edges: Dict[str, List[str]] = {}
        for scope_ref, node_payload in list(nodes.items()):
            if not isinstance(node_payload, dict):
                continue
            for dep_ref in list(node_payload.get("depends_on", []) or []):
                normalized_dep_ref = str(dep_ref or "").strip()
                if not normalized_dep_ref:
                    continue
                reverse_edges.setdefault(normalized_dep_ref, []).append(str(scope_ref))

        queue: deque[str] = deque(normalized_seeds)
        visited: List[str] = []
        visited_set = set()
        while queue:
            current_scope_ref = str(queue.popleft() or "").strip()
            if not current_scope_ref or current_scope_ref in visited_set:
                continue
            visited_set.add(current_scope_ref)
            visited.append(current_scope_ref)
            for downstream_scope_ref in list(reverse_edges.get(current_scope_ref, []) or []):
                normalized_downstream_scope_ref = str(downstream_scope_ref or "").strip()
                if not normalized_downstream_scope_ref or normalized_downstream_scope_ref in visited_set:
                    continue
                queue.append(normalized_downstream_scope_ref)

        dirty_scope_refs_by_stage: Dict[str, List[str]] = {}
        for scope_ref in visited:
            node_payload = nodes.get(scope_ref, {})
            stage_name = str(
                isinstance(node_payload, dict) and node_payload.get("stage", "") or "unknown_stage"
            ).strip() or "unknown_stage"
            dirty_scope_refs_by_stage.setdefault(stage_name, []).append(scope_ref)

        for stage_name in list(dirty_scope_refs_by_stage.keys()):
            dirty_scope_refs_by_stage[stage_name] = sorted(dirty_scope_refs_by_stage[stage_name])

        return {
            "seed_scope_refs": normalized_seeds,
            "dirty_scope_refs": visited,
            "dirty_scope_refs_by_stage": dirty_scope_refs_by_stage,
            "dirty_scope_count": len(visited),
            "plan_digest": build_runtime_payload_fingerprint(
                {
                    "seed_scope_refs": normalized_seeds,
                    "dirty_scope_refs": visited,
                }
            ),
        }

    def mark_scope_dirty(
        self,
        scope_ref: str,
        *,
        reason: str = "",
        include_descendants: bool = True,
    ) -> Dict[str, Any]:
        normalized_scope_ref = str(scope_ref or "").strip()
        if not normalized_scope_ref:
            return self.build_dirty_scope_plan([])

        dirty_plan = self.build_dirty_scope_plan([normalized_scope_ref] if include_descendants else [])
        dirty_scope_refs = dirty_plan.get("dirty_scope_refs", []) if include_descendants else [normalized_scope_ref]
        if not include_descendants:
            dirty_plan = {
                "seed_scope_refs": [normalized_scope_ref],
                "dirty_scope_refs": [normalized_scope_ref],
                "dirty_scope_refs_by_stage": {},
                "dirty_scope_count": 1,
                "plan_digest": build_runtime_payload_fingerprint([normalized_scope_ref]),
            }

        updated_at_ms = _now_ms()
        dirty_nodes: Dict[str, Dict[str, Any]] = {}
        for dirty_scope_ref in list(dirty_scope_refs or []):
            node_payload = self.load_scope_node(dirty_scope_ref)
            if not isinstance(node_payload, dict):
                continue
            node_payload["status"] = SCOPE_STATUS_DIRTY
            node_payload["dirty_reason"] = str(reason or "")
            node_payload["dirty_at_ms"] = updated_at_ms
            node_payload["updated_at_ms"] = updated_at_ms
            dirty_nodes[dirty_scope_ref] = node_payload
            self.upsert_scope_node(
                scope_ref=dirty_scope_ref,
                stage=str(node_payload.get("stage", "") or ""),
                scope_type=str(node_payload.get("scope_type", "") or ""),
                scope_id=str(node_payload.get("scope_id", "") or ""),
                scope_variant=str(node_payload.get("scope_variant", "") or ""),
                status=SCOPE_STATUS_DIRTY,
                input_fingerprint=str(node_payload.get("input_fingerprint", "") or ""),
                local_path=str(node_payload.get("local_path", "") or ""),
                dependency_fingerprints=node_payload.get("dependency_fingerprints"),
                extra_payload=node_payload,
            )
        stage_context_cache: Dict[str, Dict[str, Any]] = {}
        for dirty_scope_ref in list(dirty_scope_refs or []):
            node_payload = dirty_nodes.get(dirty_scope_ref)
            if not isinstance(node_payload, dict):
                continue
            stage_name = str(node_payload.get("stage", "") or "").strip()
            if stage_name not in stage_context_cache:
                stage_context_cache[stage_name] = self._load_stage_retry_context(stage_name)
            self._sync_scope_hint_from_node(
                node_payload,
                stage_retry_context=stage_context_cache.get(stage_name, {}),
            )
        dirty_plan["dirty_reason"] = str(reason or "")
        return dirty_plan

    def plan_scope_reuse(
        self,
        *,
        scope_ref: str,
        expected_input_fingerprint: str,
        current_dependency_fingerprints: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        normalized_scope_ref = str(scope_ref or "").strip()
        node_payload = self.load_scope_node(normalized_scope_ref)
        normalized_dependencies = self._normalize_dependency_fingerprints(current_dependency_fingerprints)

        base_plan: Dict[str, Any] = {
            "scope_ref": normalized_scope_ref,
            "expected_input_fingerprint": str(expected_input_fingerprint or ""),
            "current_dependency_fingerprints": normalized_dependencies,
            "can_restore": False,
            "reason": "missing_scope_node",
            "dirty_scope_refs": [],
            "dirty_scope_count": 0,
            "plan_digest": build_runtime_payload_fingerprint(
                {
                    "scope_ref": normalized_scope_ref,
                    "expected_input_fingerprint": str(expected_input_fingerprint or ""),
                    "current_dependency_fingerprints": normalized_dependencies,
                }
            ),
        }
        if not isinstance(node_payload, dict):
            return base_plan

        status_value = str(node_payload.get("status", "") or "").strip()
        if status_value == SCOPE_STATUS_DIRTY:
            dirty_plan = self.build_dirty_scope_plan([normalized_scope_ref])
            base_plan.update(
                {
                    "reason": "scope_marked_dirty",
                    "dirty_scope_refs": dirty_plan.get("dirty_scope_refs", []),
                    "dirty_scope_count": int(dirty_plan.get("dirty_scope_count", 0) or 0),
                    "plan_digest": str(dirty_plan.get("plan_digest", base_plan["plan_digest"])),
                }
            )
            return base_plan

        if status_value not in _REUSABLE_SCOPE_STATUSES:
            base_plan["reason"] = f"scope_status_{status_value or 'unknown'}"
            return base_plan

        recorded_input_fingerprint = str(node_payload.get("input_fingerprint", "") or "")
        if recorded_input_fingerprint != str(expected_input_fingerprint or ""):
            dirty_plan = self.mark_scope_dirty(
                normalized_scope_ref,
                reason="input_fingerprint_changed",
                include_descendants=True,
            )
            base_plan.update(
                {
                    "reason": "input_fingerprint_changed",
                    "dirty_scope_refs": dirty_plan.get("dirty_scope_refs", []),
                    "dirty_scope_count": int(dirty_plan.get("dirty_scope_count", 0) or 0),
                    "plan_digest": str(dirty_plan.get("plan_digest", base_plan["plan_digest"])),
                }
            )
            return base_plan

        recorded_dependencies = self._normalize_dependency_fingerprints(node_payload.get("dependency_fingerprints"))
        upstream_dirty_refs: List[str] = []
        for dep_scope_ref in list(sorted(set(recorded_dependencies.keys()) | set(normalized_dependencies.keys()))):
            recorded_dep_fingerprint = str(recorded_dependencies.get(dep_scope_ref, "") or "")
            current_dep_fingerprint = str(normalized_dependencies.get(dep_scope_ref, "") or "")
            dep_node_payload = self.load_scope_node(dep_scope_ref)
            if isinstance(dep_node_payload, dict) and str(dep_node_payload.get("status", "") or "") == SCOPE_STATUS_DIRTY:
                upstream_dirty_refs.append(dep_scope_ref)
                continue
            if not current_dep_fingerprint and isinstance(dep_node_payload, dict):
                current_dep_fingerprint = str(dep_node_payload.get("input_fingerprint", "") or "")
            if recorded_dep_fingerprint != current_dep_fingerprint:
                upstream_dirty_refs.append(dep_scope_ref)

        if upstream_dirty_refs:
            dirty_plan = self.mark_scope_dirty(
                normalized_scope_ref,
                reason=f"upstream_scope_dirty:{'|'.join(upstream_dirty_refs[:3])}",
                include_descendants=True,
            )
            base_plan.update(
                {
                    "reason": "upstream_scope_dirty",
                    "dirty_scope_refs": dirty_plan.get("dirty_scope_refs", []),
                    "dirty_scope_count": int(dirty_plan.get("dirty_scope_count", 0) or 0),
                    "upstream_dirty_scope_refs": upstream_dirty_refs,
                    "plan_digest": str(dirty_plan.get("plan_digest", base_plan["plan_digest"])),
                }
            )
            return base_plan

        base_plan["can_restore"] = True
        base_plan["reason"] = "restore_allowed"
        return base_plan

    @staticmethod
    def _parse_attempt_dir_name(dir_name: str, *, llm_call_id: str = "") -> Optional[int]:
        normalized = str(dir_name or "").strip()
        if not normalized:
            return None
        lowered = normalized.lower()
        if lowered.startswith("a"):
            try:
                return int(lowered[1:])
            except Exception:
                return None
        call_prefix = f"{str(llm_call_id or '').strip()}.a"
        if llm_call_id and normalized.startswith(call_prefix):
            try:
                return int(normalized[len(call_prefix):])
            except Exception:
                return None
        marker_index = lowered.rfind(".a")
        if marker_index >= 0:
            try:
                return int(lowered[marker_index + 2 :])
            except Exception:
                return None
        marker_index = lowered.rfind("--a")
        if marker_index >= 0:
            try:
                return int(lowered[marker_index + 3 :])
            except Exception:
                return None
        return None

    def _collect_exact_attempt_dirs(self, *, stage: str, chunk_id: str, llm_call_id: str) -> List[Path]:
        attempt_dirs: List[Path] = []
        calls_dir = self.chunk_dir(stage=stage, chunk_id=chunk_id) / "call"
        if calls_dir.exists():
            prefix = f"{llm_call_id}.a"
            for child in calls_dir.iterdir():
                if child.is_dir() and child.name.startswith(prefix):
                    attempt_dirs.append(child)
        legacy_root = self._legacy_llm_root(stage=stage, chunk_id=chunk_id, llm_call_id=llm_call_id)
        if legacy_root.exists():
            for child in legacy_root.iterdir():
                if child.is_dir() and self._parse_attempt_dir_name(child.name, llm_call_id=llm_call_id) is not None:
                    attempt_dirs.append(child)
        attempt_dirs.sort(
            key=lambda item: self._parse_attempt_dir_name(item.name, llm_call_id=llm_call_id) or 0,
            reverse=True,
        )
        return attempt_dirs

    def _collect_fallback_attempt_dirs(self, *, stage: str, chunk_id: str) -> List[Path]:
        attempt_dirs: List[Path] = []
        seen_paths: set[str] = set()

        calls_dir = self.chunk_dir(stage=stage, chunk_id=chunk_id) / "call"
        if calls_dir.exists():
            for child in calls_dir.iterdir():
                if not child.is_dir():
                    continue
                if self._parse_attempt_dir_name(child.name) is None:
                    continue
                child_key = str(child.resolve())
                if child_key in seen_paths:
                    continue
                seen_paths.add(child_key)
                attempt_dirs.append(child)

        legacy_calls_root = self._legacy_chunk_dir(stage=stage, chunk_id=chunk_id) / "l"
        if legacy_calls_root.exists():
            for llm_root in legacy_calls_root.iterdir():
                if not llm_root.is_dir():
                    continue
                for child in llm_root.iterdir():
                    if not child.is_dir():
                        continue
                    if self._parse_attempt_dir_name(child.name) is None:
                        continue
                    child_key = str(child.resolve())
                    if child_key in seen_paths:
                        continue
                    seen_paths.add(child_key)
                    attempt_dirs.append(child)

        attempt_dirs.sort(
            key=lambda item: self._parse_attempt_dir_name(item.name) or 0,
            reverse=True,
        )
        return attempt_dirs

    def _load_llm_lookup_payload(
        self,
        *,
        stage: str,
        chunk_id: str,
        llm_call_id: str,
        input_fingerprint: str,
    ) -> Optional[Dict[str, Any]]:
        candidates = self._matching_index_payload_candidates(
            self._read_indexed_payload(
                self._llm_lookup_index_path(
                    stage=stage,
                    chunk_id=chunk_id,
                    llm_call_id=llm_call_id,
                    input_fingerprint=input_fingerprint,
                ),
                _LLM_LOOKUP_INDEX_SCHEMA_VERSION,
            ),
            expected_pairs={
                "stage": str(stage or ""),
                "chunk_id": str(chunk_id or ""),
                "llm_call_id": str(llm_call_id or ""),
                "input_fingerprint": str(input_fingerprint or ""),
            },
        )
        return candidates[0] if candidates else None

    def _load_llm_attempt_payload(
        self,
        *,
        stage: str,
        chunk_id: str,
        llm_call_id: str,
    ) -> Optional[Dict[str, Any]]:
        candidates = self._matching_index_payload_candidates(
            self._read_indexed_payload(
                self._llm_attempt_index_path(stage=stage, chunk_id=chunk_id, llm_call_id=llm_call_id),
                _LLM_ATTEMPT_INDEX_SCHEMA_VERSION,
            ),
            expected_pairs={
                "stage": str(stage or ""),
                "chunk_id": str(chunk_id or ""),
                "llm_call_id": str(llm_call_id or ""),
            },
        )
        return candidates[0] if candidates else None

    def _load_indexed_llm_attempt_dir(
        self,
        *,
        stage: str,
        chunk_id: str,
        llm_call_id: str,
        input_fingerprint: str,
    ) -> Optional[Path]:
        candidates = self._matching_index_payload_candidates(
            self._read_indexed_payload(
                self._llm_lookup_index_path(
                    stage=stage,
                    chunk_id=chunk_id,
                    llm_call_id=llm_call_id,
                    input_fingerprint=input_fingerprint,
                ),
                _LLM_LOOKUP_INDEX_SCHEMA_VERSION,
            ),
            expected_pairs={
                "stage": str(stage or ""),
                "chunk_id": str(chunk_id or ""),
                "llm_call_id": str(llm_call_id or ""),
                "input_fingerprint": str(input_fingerprint or ""),
            },
        )
        for payload in candidates:
            attempt_dir = Path(str(payload.get("attempt_dir", "") or "").strip())
            if attempt_dir.exists():
                return attempt_dir
        return None

    def _attempt_dir_from_hint_root(self, *, root_dir: Path, llm_call_id: str, attempt: int) -> Path:
        safe_attempt = max(1, int(attempt or 1))
        if str(root_dir.name or "").strip().lower() == "call":
            return root_dir / f"{llm_call_id}.a{safe_attempt:03d}"
        return root_dir / f"a{safe_attempt:03d}"

    def _bounded_hint_attempt_dirs(
        self,
        *,
        stage: str,
        chunk_id: str,
        llm_call_id: str,
        input_fingerprint: str,
        window_size: int = 6,
    ) -> List[Path]:
        lookup_payload = self._load_llm_lookup_payload(
            stage=stage,
            chunk_id=chunk_id,
            llm_call_id=llm_call_id,
            input_fingerprint=input_fingerprint,
        )
        attempt_payload = self._load_llm_attempt_payload(
            stage=stage,
            chunk_id=chunk_id,
            llm_call_id=llm_call_id,
        )
        candidates: List[Path] = []
        seen_paths: set[str] = set()

        def _append_candidate(path: Optional[Path]) -> None:
            if path is None:
                return
            normalized_path = str(path)
            if not normalized_path or normalized_path in seen_paths:
                return
            seen_paths.add(normalized_path)
            candidates.append(path)

        lookup_attempt_dir = None
        if isinstance(lookup_payload, dict):
            lookup_attempt_dir = Path(str(lookup_payload.get("attempt_dir", "") or "").strip())
            _append_candidate(lookup_attempt_dir)

        latest_attempt = 0
        if isinstance(lookup_payload, dict):
            latest_attempt = max(latest_attempt, int(lookup_payload.get("attempt", 0) or 0))
        if isinstance(attempt_payload, dict):
            latest_attempt = max(latest_attempt, int(attempt_payload.get("latest_attempt", 0) or 0))

        hint_roots: List[Path] = []
        if isinstance(lookup_payload, dict):
            raw_attempt_dir = str(lookup_payload.get("attempt_dir", "") or "").strip()
            if raw_attempt_dir:
                hint_roots.append(Path(raw_attempt_dir).parent)
        if isinstance(attempt_payload, dict):
            raw_attempt_dir = str(attempt_payload.get("attempt_dir", "") or "").strip()
            if raw_attempt_dir:
                hint_roots.append(Path(raw_attempt_dir).parent)

        safe_window = max(1, int(window_size or 1))
        if latest_attempt > 0:
            lower_bound = max(1, latest_attempt - safe_window + 1)
            for attempt in range(latest_attempt, lower_bound - 1, -1):
                for root_dir in hint_roots:
                    _append_candidate(
                        self._attempt_dir_from_hint_root(root_dir=root_dir, llm_call_id=llm_call_id, attempt=attempt)
                    )

        return candidates

    def _load_indexed_chunk_dir(
        self,
        *,
        stage: str,
        chunk_id: str,
        input_fingerprint: str,
    ) -> Optional[Path]:
        candidates = self._matching_index_payload_candidates(
            self._read_indexed_payload(
                self._chunk_lookup_index_path(stage=stage, chunk_id=chunk_id, input_fingerprint=input_fingerprint),
                _CHUNK_LOOKUP_INDEX_SCHEMA_VERSION,
            ),
            expected_pairs={
                "stage": str(stage or ""),
                "chunk_id": str(chunk_id or ""),
                "input_fingerprint": str(input_fingerprint or ""),
            },
        )
        for payload in candidates:
            chunk_dir = Path(str(payload.get("chunk_dir", "") or "").strip())
            if chunk_dir.exists():
                return chunk_dir
        return None

    def _part_paths(self, attempt_dir: Path) -> List[Path]:
        new_parts = sorted(attempt_dir.glob("part_*.json"))
        if new_parts:
            return new_parts
        legacy_dir = attempt_dir / "p"
        if legacy_dir.exists():
            return sorted(legacy_dir.glob("p*.json"))
        return []

    def _attempt_status_payload(
        self,
        *,
        attempt_dir: Path,
        llm_call_id: str,
    ) -> Optional[Dict[str, Any]]:
        attempt_number = self._parse_attempt_dir_name(attempt_dir.name, llm_call_id=llm_call_id)
        if attempt_number is None:
            return None
        commit_payload = _read_json(attempt_dir / "commit.json")
        manifest_payload = _read_json(attempt_dir / "manifest.json") or {}
        if commit_payload:
            status = str(commit_payload.get("status", "") or "")
            committed_at_ms = int(commit_payload.get("committed_at_ms", 0) or 0)
            if _normalize_runtime_status(status) == STATUS_SUCCESS:
                return {
                    "kind": "success",
                    "attempt": attempt_number,
                    "timestamp_ms": committed_at_ms or int(manifest_payload.get("updated_at_ms", 0) or 0),
                    "attempt_dir": attempt_dir,
                }
        status = str(manifest_payload.get("status", "") or "")
        if _normalize_runtime_status(status) in {STATUS_ERROR, STATUS_MANUAL_NEEDED, STATUS_FAILED}:
            return {
                "kind": "failure",
                "attempt": attempt_number,
                "timestamp_ms": int(manifest_payload.get("updated_at_ms", 0) or 0),
                "attempt_dir": attempt_dir,
            }
        return None

    def _delete_attempt_dir(self, attempt_dir: Path) -> None:
        try:
            shutil.rmtree(attempt_dir, ignore_errors=True)
        except Exception as error:
            logger.warning("Runtime attempt cleanup failed: dir=%s err=%s", attempt_dir, error)

    def _prune_llm_attempt_history(self, *, stage: str, chunk_id: str, llm_call_id: str) -> None:
        attempt_dirs = self._collect_exact_attempt_dirs(stage=stage, chunk_id=chunk_id, llm_call_id=llm_call_id)
        if len(attempt_dirs) <= 1:
            return

        records: List[Dict[str, Any]] = []
        for attempt_dir in attempt_dirs:
            record = self._attempt_status_payload(attempt_dir=attempt_dir, llm_call_id=llm_call_id)
            if record is not None:
                records.append(record)
        if len(records) <= 1:
            return

        latest_success_attempt = max(
            (record["attempt"] for record in records if record["kind"] == "success"),
            default=None,
        )
        latest_failure_attempt = max(
            (record["attempt"] for record in records if record["kind"] == "failure"),
            default=None,
        )
        now_ms = _now_ms()

        for record in records:
            attempt_number = int(record.get("attempt", 0) or 0)
            record_kind = str(record.get("kind", "") or "")
            timestamp_ms = int(record.get("timestamp_ms", 0) or 0)
            should_keep = False
            if record_kind == "success" and latest_success_attempt is not None and attempt_number == latest_success_attempt:
                should_keep = True
            elif record_kind == "failure" and latest_failure_attempt is not None and attempt_number == latest_failure_attempt:
                should_keep = True
            elif record_kind == "success" and timestamp_ms > 0 and now_ms - timestamp_ms < self._storage_success_retention_ms:
                should_keep = True

            if should_keep:
                continue

            self._redis.unlink(
                self._storage_llm_redis_key(
                    stage=stage,
                    chunk_id=chunk_id,
                    llm_call_id=llm_call_id,
                    attempt=attempt_number,
                )
            )
            self._delete_attempt_dir(Path(record["attempt_dir"]))

    def update_stage_state(
        self,
        *,
        stage: str,
        status: str,
        payload: Optional[Dict[str, Any]] = None,
    ) -> None:
        stage_path = self.stage_dir(stage) / "stage_state.json"
        state_payload = {
            "schema_version": "runtime_stage_state_v1",
            "task_id": self.task_id,
            "stage": str(stage or ""),
            "stage_owner": "python",
            "status": str(status or ""),
            "updated_at_ms": _now_ms(),
            "output_dir": str(self.output_dir),
            "local_stage_state_path": str(stage_path),
        }
        if isinstance(payload, dict):
            state_payload.update(payload)
        if self._sqlite_index is not None:
            self._sqlite_index.upsert_stage_snapshot(
                output_dir=str(self.output_dir),
                task_id=self.task_id,
                stage=str(stage or ""),
                stage_owner=str(state_payload.get("stage_owner", "python") or "python"),
                status=str(state_payload.get("status", "") or ""),
                checkpoint=str(state_payload.get("checkpoint", "") or ""),
                completed=int(state_payload.get("completed", 0) or 0),
                pending=int(state_payload.get("pending", 0) or 0),
                updated_at_ms=int(state_payload.get("updated_at_ms", _now_ms()) or _now_ms()),
                stage_state_path=str(stage_path),
                payload=state_payload,
            )
        if self._should_write_stage_file_mirrors():
            _write_json_atomic_sync(stage_path, state_payload)
        self._update_resume_stage_index(stage=stage, stage_path=stage_path, state_payload=state_payload)
        redis_stage_payload: Dict[str, Any] = {
            "status": str(state_payload.get("status", "") or ""),
            "checkpoint": str(state_payload.get("checkpoint", "") or ""),
            "completed": int(state_payload.get("completed", 0) or 0),
            "pending": int(state_payload.get("pending", 0) or 0),
            "updated_at_ms": int(state_payload.get("updated_at_ms", _now_ms()) or _now_ms()),
            "local_stage_state_path": str(stage_path),
        }
        for field_name in (
            "error_class",
            "error_code",
            "error_message",
            "retry_mode",
            "required_action",
            "retry_entry_point",
            "retry_strategy",
            "operator_action",
            "action_hint",
        ):
            field_value = state_payload.get(field_name)
            if field_value not in (None, "", []):
                redis_stage_payload[field_name] = field_value
        self._redis.hset(self._task_stage_redis_key(stage), redis_stage_payload)
        normalized_status = _normalize_runtime_status(state_payload.get("status", ""))
        if normalized_status in {STATUS_ERROR, STATUS_MANUAL_NEEDED, STATUS_FAILED}:
            error_record = {
                "schema_version": "runtime_error_record_v1",
                "record_type": "stage_error",
                "task_id": self.task_id,
                "stage": str(stage or ""),
                "status": normalized_status,
                "checkpoint": str(state_payload.get("checkpoint", "") or ""),
                "updated_at_ms": int(state_payload.get("updated_at_ms", _now_ms()) or _now_ms()),
                "error_class": str(state_payload.get("error_class", "") or ""),
                "error_code": str(state_payload.get("error_code", "") or ""),
                "error_message": str(state_payload.get("error_message", "") or ""),
                "retry_mode": str(state_payload.get("retry_mode", "") or ""),
                "required_action": str(state_payload.get("required_action", "") or ""),
                "retry_entry_point": str(state_payload.get("retry_entry_point", "") or ""),
                "retry_strategy": str(state_payload.get("retry_strategy", "") or ""),
                "operator_action": str(state_payload.get("operator_action", "") or ""),
                "action_hint": str(state_payload.get("action_hint", "") or ""),
                "local_stage_state_path": str(stage_path),
                "output_dir": str(self.output_dir),
                "source": "python",
            }
            self.append_rt_error_record(error_record)
            if normalized_status == STATUS_MANUAL_NEEDED:
                manual_retry_record = dict(error_record)
                manual_retry_record["schema_version"] = "runtime_manual_retry_record_v1"
                manual_retry_record["record_type"] = "stage_manual_retry_required"
                self.append_rt_manual_retry_record(manual_retry_record)
        self.sync_scope_hints_from_scope_graph(stage=stage)

    def append_rt_fallback_record(self, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        record = dict(payload or {})
        record.setdefault("schema_version", "runtime_fallback_record_v1")
        record.setdefault("task_id", self.task_id)
        record.setdefault("updated_at_ms", _now_ms())
        _append_jsonl_sync(self._rt_fallback_log_path(), record)
        return record

    def append_rt_error_record(self, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        record = dict(payload or {})
        record.setdefault("schema_version", "runtime_error_record_v1")
        record.setdefault("task_id", self.task_id)
        record.setdefault("updated_at_ms", _now_ms())
        _append_jsonl_sync(self._rt_error_log_path(), record)
        return record

    def append_rt_manual_retry_record(self, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        record = dict(payload or {})
        record.setdefault("schema_version", "runtime_manual_retry_record_v1")
        record.setdefault("task_id", self.task_id)
        record.setdefault("updated_at_ms", _now_ms())
        _append_jsonl_sync(self._rt_manual_retry_log_path(), record)
        return record

    def append_stage_journal_event(
        self,
        *,
        stage: str,
        event: str,
        checkpoint: str = "",
        status: str = "",
        payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        journal_payload: Dict[str, Any] = {
            "schema_version": "runtime_stage_journal_event_v1",
            "task_id": self.task_id,
            "stage": str(stage or ""),
            "event": str(event or ""),
            "checkpoint": str(checkpoint or ""),
            "status": str(status or ""),
            "updated_at_ms": _now_ms(),
        }
        if isinstance(payload, dict):
            journal_payload.update(payload)
        return journal_payload

    def write_stage_outputs_manifest(
        self,
        *,
        stage: str,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        manifest_payload: Dict[str, Any] = {
            "schema_version": "runtime_stage_outputs_manifest_v1",
            "task_id": self.task_id,
            "stage": str(stage or ""),
            "updated_at_ms": _now_ms(),
        }
        if isinstance(payload, dict):
            manifest_payload.update(payload)
        return manifest_payload

    def append_event(
        self,
        *,
        scope_type: str,
        scope_id: str,
        status: str,
        stage: str,
        chunk_id: str = "",
        llm_call_id: str = "",
        attempt: int = 0,
        error_class: str = "",
        error_code: str = "",
        local_path: str = "",
        message: str = "",
    ) -> None:
        event_payload = {
            "ts_ms": _now_ms(),
            "task_id": self.task_id,
            "stage": str(stage or ""),
            "chunk_id": str(chunk_id or ""),
            "llm_call_id": str(llm_call_id or ""),
            "scope_type": str(scope_type or ""),
            "scope_id": str(scope_id or ""),
            "status": str(status or ""),
            "attempt": int(attempt or 0),
            "error_class": str(error_class or ""),
            "error_code": str(error_code or ""),
            "local_path": str(local_path or ""),
            "message": str(message or ""),
        }
        self._redis.xadd(self._task_events_redis_key(), event_payload)

    def begin_llm_attempt(
        self,
        *,
        stage: str,
        chunk_id: str,
        llm_call_id: str,
        input_fingerprint: str,
        request_payload: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> RuntimeAttemptHandle:
        metadata_payload = dict(metadata or {})
        storage_backend = _resolve_chunk_storage_backend(metadata_payload)
        if storage_backend == "sqlite" and self._sqlite_index is None:
            raise RuntimeError(
                f"SQLite authoritative llm runtime requires runtime_recovery_sqlite for stage={stage}, llm_call_id={llm_call_id}"
            )
        calls_dir = (
            self._llm_calls_dir(stage=stage, chunk_id=chunk_id, ensure_exists=True)
            if storage_backend != "sqlite"
            else None
        )
        latest_attempt = 0
        if self._sqlite_index is not None:
            try:
                latest_attempt_payload = self._sqlite_index.load_latest_llm_attempt(
                    output_dir=str(self.output_dir),
                    stage=stage,
                    chunk_id=chunk_id,
                    llm_call_id=llm_call_id,
                )
                if isinstance(latest_attempt_payload, dict):
                    latest_attempt = max(latest_attempt, int(latest_attempt_payload.get("attempt", 0) or 0))
            except Exception as error:
                logger.warning("Runtime recovery SQLite latest llm attempt load failed: %s", error)
        if latest_attempt <= 0 and storage_backend != "sqlite":
            attempt_payload = self._read_indexed_payload(
                self._llm_attempt_index_path(stage=stage, chunk_id=chunk_id, llm_call_id=llm_call_id),
                _LLM_ATTEMPT_INDEX_SCHEMA_VERSION,
            )
            if isinstance(attempt_payload, dict):
                latest_attempt = max(latest_attempt, int(attempt_payload.get("latest_attempt", 0) or 0))
        if latest_attempt <= 0 and storage_backend != "sqlite":
            for child in self._collect_exact_attempt_dirs(stage=stage, chunk_id=chunk_id, llm_call_id=llm_call_id):
                parsed_attempt = self._parse_attempt_dir_name(child.name, llm_call_id=llm_call_id)
                if parsed_attempt is None:
                    continue
                latest_attempt = max(latest_attempt, parsed_attempt)
        attempt = max(1, latest_attempt + 1)
        if storage_backend == "sqlite":
            attempt_dir = (
                self.output_dir
                / "intermediates"
                / "rt"
                / "sqlite_llm"
                / str(stage or "unknown")
                / str(chunk_id or "unknown_chunk")
                / f"{llm_call_id}.a{attempt:03d}"
            )
        else:
            attempt_dir = calls_dir / f"{llm_call_id}.a{attempt:03d}"
            while attempt_dir.exists():
                attempt += 1
                attempt_dir = calls_dir / f"{llm_call_id}.a{attempt:03d}"
            attempt_dir.mkdir(parents=True, exist_ok=True)
        manifest_payload = {
            "schema_version": "runtime_llm_manifest_v1",
            "task_id": self.task_id,
            "stage": str(stage or ""),
            "chunk_id": str(chunk_id or ""),
            "llm_call_id": str(llm_call_id or ""),
            "attempt": attempt,
            "status": STATUS_RUNNING,
            "input_fingerprint": str(input_fingerprint or ""),
            "created_at_ms": _now_ms(),
            "updated_at_ms": _now_ms(),
        }
        if metadata_payload:
            manifest_payload.update(metadata_payload)
        if not str(manifest_payload.get("stage_step", "") or "").strip():
            manifest_payload["stage_step"] = str(manifest_payload.get("step_name", "") or "").strip()
        if not str(manifest_payload.get("request_name", "") or "").strip():
            manifest_payload["request_name"] = str(
                manifest_payload.get("stage_step", manifest_payload.get("step_name", "")) or ""
            ).strip()
        request_path = attempt_dir / "request.json"
        manifest_path = attempt_dir / "manifest.json"
        if storage_backend != "sqlite":
            _write_json_atomic_sync(manifest_path, manifest_payload)
            _write_json_atomic_sync(request_path, request_payload)
        local_attempt_dir = "" if storage_backend == "sqlite" else str(attempt_dir)
        manifest_path_text = "" if storage_backend == "sqlite" else str(manifest_path)
        self.plan_llm_call_scope(
            stage=stage,
            chunk_id=chunk_id,
            llm_call_id=llm_call_id,
            input_fingerprint=input_fingerprint,
            request_payload=request_payload,
            metadata=manifest_payload,
            scope_variant=str(manifest_payload.get("scope_variant", "") or ""),
            local_path=local_attempt_dir,
            dependency_fingerprints=manifest_payload.get("dependency_fingerprints"),
            attempt_count=max(0, attempt - 1),
            extra_payload={
                "chunk_id": chunk_id,
                "provider": manifest_payload.get("provider", ""),
                "request_name": manifest_payload.get("request_name", ""),
                "stage_step": manifest_payload.get("stage_step", ""),
                "scope_variant": manifest_payload.get("scope_variant", ""),
                "unit_id": manifest_payload.get("unit_id", ""),
            },
        )
        self._redis.hset(
            self._storage_llm_redis_key(stage=stage, chunk_id=chunk_id, llm_call_id=llm_call_id, attempt=attempt),
            {
                "status": STATUS_RUNNING,
                "task_id": self.task_id,
                "storage_key": self.storage_key,
                "stage": stage,
                "chunk_id": chunk_id,
                "llm_call_id": llm_call_id,
                "attempt": attempt,
                "input_fingerprint": input_fingerprint,
                "local_attempt_dir": local_attempt_dir,
                "updated_at_ms": _now_ms(),
            },
        )
        self.append_event(
            scope_type="llm_call",
            scope_id=llm_call_id,
            status=STATUS_RUNNING,
            stage=stage,
            chunk_id=chunk_id,
            llm_call_id=llm_call_id,
            attempt=attempt,
            local_path=local_attempt_dir,
            message="llm attempt started",
        )
        scope_variant = str(manifest_payload.get("scope_variant", "") or "")
        llm_scope_extra_payload = {
            "chunk_id": chunk_id,
            "attempt": attempt,
        }
        for field_name in ("provider", "request_name", "stage_step", "scope_variant", "unit_id"):
            field_value = manifest_payload.get(field_name)
            if field_value in (None, "", []):
                continue
            llm_scope_extra_payload[field_name] = field_value
        existing_llm_scope = self.load_scope_node(
            self.build_scope_ref(
                stage=stage,
                scope_type="llm_call",
                scope_id=llm_call_id,
                scope_variant=scope_variant,
            )
        )
        self.transition_scope_node(
            scope_ref=self.build_scope_ref(
                stage=stage,
                scope_type="llm_call",
                scope_id=llm_call_id,
                scope_variant=scope_variant,
            ),
            stage=stage,
            scope_type="llm_call",
            scope_id=llm_call_id,
            scope_variant=scope_variant,
            status=STATUS_RUNNING,
            input_fingerprint=input_fingerprint,
            local_path=local_attempt_dir,
            dependency_fingerprints=manifest_payload.get("dependency_fingerprints"),
            plan_context=_build_scope_plan_context(
                stage=stage,
                scope_type="llm_call",
                scope_id=llm_call_id,
                input_fingerprint=input_fingerprint,
                metadata=manifest_payload,
                request_payload=request_payload,
                existing_context=(existing_llm_scope or {}).get("plan_context"),
            ),
            attempt_count=int(attempt or 1),
            extra_payload=llm_scope_extra_payload,
        )
        if self._sqlite_index is not None:
            try:
                self._sqlite_index.record_llm_attempt_started(
                    output_dir=str(self.output_dir),
                    task_id=self.task_id,
                    storage_key=self.storage_key,
                    normalized_video_key=self.normalized_video_key,
                    stage=stage,
                    chunk_id=chunk_id,
                    llm_call_id=llm_call_id,
                    input_fingerprint=input_fingerprint,
                    attempt=attempt,
                    request_payload=request_payload,
                    manifest_payload=manifest_payload,
                    attempt_dir=local_attempt_dir,
                    manifest_path=manifest_path_text,
                )
            except Exception as error:
                logger.warning("Runtime recovery SQLite llm start mirror failed: %s", error)
        return RuntimeAttemptHandle(
            stage=stage,
            chunk_id=chunk_id,
            llm_call_id=llm_call_id,
            attempt=attempt,
            attempt_dir=attempt_dir,
            manifest_path=manifest_path,
            request_path=request_path,
            response_parts_dir=attempt_dir,
            input_fingerprint=input_fingerprint,
            scope_key=str(attempt_dir.resolve()) if storage_backend != "sqlite" else f"sqlite-llm:{stage}:{chunk_id}:{llm_call_id}:{attempt}",
            storage_backend=storage_backend,
            request_payload_cache=dict(request_payload or {}),
            manifest_payload_cache=dict(manifest_payload),
        )

    def load_committed_llm_response(
        self,
        *,
        stage: str,
        chunk_id: str,
        llm_call_id: str,
        input_fingerprint: str,
    ) -> Optional[Dict[str, Any]]:
        if self._sqlite_index is None:
            return None
        try:
            return self._sqlite_index.load_latest_committed_llm(
                output_dir=str(self.output_dir),
                stage=stage,
                chunk_id=chunk_id,
                llm_call_id=llm_call_id,
                input_fingerprint=input_fingerprint,
            )
        except Exception as error:
            logger.warning("Runtime recovery SQLite llm load failed: %s", error)
            return None

    def commit_llm_attempt(
        self,
        *,
        handle: RuntimeAttemptHandle,
        response_text: str,
        response_metadata: Optional[Dict[str, Any]] = None,
        max_part_bytes: int = 262144,
    ) -> Dict[str, Any]:
        storage_backend = str(getattr(handle, "storage_backend", "hybrid") or "hybrid").strip().lower() or "hybrid"
        parts, response_hash = split_text_parts_by_bytes(response_text, max_part_bytes=max_part_bytes)
        if storage_backend != "sqlite":
            try:
                for part in parts:
                    part_index = int(part.get("part_index", 0) or 0)
                    part_payload = {
                        "schema_version": "runtime_llm_part_v1",
                        "task_id": self.task_id,
                        "stage": handle.stage,
                        "chunk_id": handle.chunk_id,
                        "llm_call_id": handle.llm_call_id,
                        "attempt": handle.attempt,
                        **part,
                    }
                    enqueue_json_write(
                        str(handle.response_parts_dir / f"part_{part_index + 1:04d}.json"),
                        part_payload,
                        ensure_ascii=False,
                        indent=2,
                        scope_key=handle.scope_key,
                    )
                flushed = flush_async_json_writes(timeout_sec=30.0, scope_key=handle.scope_key)
                if not flushed:
                    raise TimeoutError(f"llm response parts flush timeout: {handle.scope_key}")
            except Exception as enqueue_error:
                logger.warning("LLM response async persist unavailable, fallback to sync write: %s", enqueue_error)
                for part in parts:
                    part_index = int(part.get("part_index", 0) or 0)
                    part_payload = {
                        "schema_version": "runtime_llm_part_v1",
                        "task_id": self.task_id,
                        "stage": handle.stage,
                        "chunk_id": handle.chunk_id,
                        "llm_call_id": handle.llm_call_id,
                        "attempt": handle.attempt,
                        **part,
                    }
                    _write_json_atomic_sync(handle.response_parts_dir / f"part_{part_index + 1:04d}.json", part_payload)
            manifest_payload = _read_json(handle.manifest_path) or {}
        else:
            manifest_payload = dict(getattr(handle, "manifest_payload_cache", {}) or {})
        manifest_payload.update(
            {
                "status": STATUS_SUCCESS,
                "updated_at_ms": _now_ms(),
                "response_hash": response_hash,
                "response_chars": len(str(response_text or "")),
                "response_parts": [
                    {
                        key: value
                        for key, value in part.items()
                        if key != "content"
                    }
                    for part in parts
                ],
                "committed_parts": len(parts),
            }
        )
        if isinstance(response_metadata, dict):
            manifest_payload["response_metadata"] = response_metadata
        if storage_backend != "sqlite":
            _write_json_atomic_sync(handle.manifest_path, manifest_payload)
        commit_payload = {
            "schema_version": "runtime_llm_commit_v1",
            "task_id": self.task_id,
            "stage": handle.stage,
            "chunk_id": handle.chunk_id,
            "llm_call_id": handle.llm_call_id,
            "attempt": handle.attempt,
            "status": STATUS_SUCCESS,
            "input_fingerprint": handle.input_fingerprint,
            "manifest_hash": _sha256_text(_stable_json_dumps(manifest_payload)),
            "response_hash": response_hash,
            "committed_parts": len(parts),
            "final_bytes": len(str(response_text or "").encode("utf-8")),
            "committed_at_ms": _now_ms(),
            "cleanup_after_ms": _now_ms() + self._storage_success_retention_ms,
        }
        local_attempt_dir = "" if storage_backend == "sqlite" else str(handle.attempt_dir)
        manifest_path_text = "" if storage_backend == "sqlite" else str(handle.manifest_path)
        commit_path_text = "" if storage_backend == "sqlite" else str(handle.attempt_dir / "commit.json")
        if storage_backend != "sqlite":
            _write_json_atomic_sync(handle.attempt_dir / "commit.json", commit_payload)
        self._redis.hset(
            self._storage_llm_redis_key(
                stage=handle.stage,
                chunk_id=handle.chunk_id,
                llm_call_id=handle.llm_call_id,
                attempt=handle.attempt,
            ),
            {
                "status": STATUS_SUCCESS,
                "response_hash": response_hash,
                "committed_parts": len(parts),
                "cleanup_after_ms": int(commit_payload.get("cleanup_after_ms", 0) or 0),
                "updated_at_ms": _now_ms(),
                "local_attempt_dir": local_attempt_dir,
            },
        )
        self.append_event(
            scope_type="llm_call",
            scope_id=handle.llm_call_id,
            status=STATUS_SUCCESS,
            stage=handle.stage,
            chunk_id=handle.chunk_id,
            llm_call_id=handle.llm_call_id,
            attempt=handle.attempt,
            local_path=local_attempt_dir,
            message="llm attempt committed",
        )
        manifest_scope_variant = str(manifest_payload.get("scope_variant", "") or "")
        llm_scope_extra_payload = {
            "chunk_id": handle.chunk_id,
            "attempt": handle.attempt,
        }
        for field_name in ("provider", "request_name", "stage_step", "scope_variant", "unit_id"):
            field_value = manifest_payload.get(field_name)
            if field_value in (None, "", []):
                continue
            llm_scope_extra_payload[field_name] = field_value
        existing_llm_scope = self.load_scope_node(
            self.build_scope_ref(
                stage=handle.stage,
                scope_type="llm_call",
                scope_id=handle.llm_call_id,
                scope_variant=manifest_scope_variant,
            )
        )
        self.transition_scope_node(
            scope_ref=self.build_scope_ref(
                stage=handle.stage,
                scope_type="llm_call",
                scope_id=handle.llm_call_id,
                scope_variant=manifest_scope_variant,
            ),
            stage=handle.stage,
            scope_type="llm_call",
            scope_id=handle.llm_call_id,
            scope_variant=manifest_scope_variant,
            status=STATUS_SUCCESS,
            input_fingerprint=handle.input_fingerprint,
            local_path=local_attempt_dir,
            dependency_fingerprints=manifest_payload.get("dependency_fingerprints"),
            plan_context=_build_scope_plan_context(
                stage=handle.stage,
                scope_type="llm_call",
                scope_id=handle.llm_call_id,
                input_fingerprint=handle.input_fingerprint,
                metadata=manifest_payload,
                request_payload=dict(getattr(handle, "request_payload_cache", {}) or {}),
                existing_context=(existing_llm_scope or {}).get("plan_context"),
            ),
            attempt_count=int(handle.attempt or 1),
            result_hash=str(response_hash or ""),
            extra_payload=llm_scope_extra_payload,
        )
        if self._sqlite_index is not None:
            try:
                self._sqlite_index.record_llm_attempt_committed(
                    output_dir=str(self.output_dir),
                    task_id=self.task_id,
                    storage_key=self.storage_key,
                    normalized_video_key=self.normalized_video_key,
                    stage=handle.stage,
                    chunk_id=handle.chunk_id,
                    llm_call_id=handle.llm_call_id,
                    input_fingerprint=handle.input_fingerprint,
                    attempt=handle.attempt,
                    request_payload=(
                        dict(getattr(handle, "request_payload_cache", {}) or {})
                        if storage_backend == "sqlite"
                        else _read_json(handle.request_path)
                    ),
                    manifest_payload=manifest_payload,
                    commit_payload=commit_payload,
                    response_text=str(response_text or ""),
                    attempt_dir=local_attempt_dir,
                    manifest_path=manifest_path_text,
                    commit_path=commit_path_text,
                )
            except Exception as error:
                logger.warning("Runtime recovery SQLite llm commit mirror failed: %s", error)
        self._prune_llm_attempt_history(
            stage=handle.stage,
            chunk_id=handle.chunk_id,
            llm_call_id=handle.llm_call_id,
        )
        return commit_payload

    def fail_llm_attempt(
        self,
        *,
        handle: RuntimeAttemptHandle,
        error: Exception,
        request_snapshot: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        storage_backend = str(getattr(handle, "storage_backend", "hybrid") or "hybrid").strip().lower() or "hybrid"
        error_info = classify_runtime_error(error)
        error_payload = {
            "schema_version": "runtime_llm_error_v1",
            "task_id": self.task_id,
            "stage": handle.stage,
            "chunk_id": handle.chunk_id,
            "llm_call_id": handle.llm_call_id,
            "attempt": handle.attempt,
            **error_info,
            "recorded_at_ms": _now_ms(),
        }
        if isinstance(request_snapshot, dict):
            error_payload["request_snapshot"] = request_snapshot
        if storage_backend != "sqlite":
            _write_json_atomic_sync(handle.attempt_dir / "error.json", error_payload)
            manifest_payload = _read_json(handle.manifest_path) or {}
        else:
            manifest_payload = dict(getattr(handle, "manifest_payload_cache", {}) or {})
        status_value = _derive_scope_failure_status(error_info.get("error_class", ""))
        manifest_payload.update(
            {
                "status": status_value,
                "updated_at_ms": _now_ms(),
                **error_info,
            }
        )
        if storage_backend != "sqlite":
            _write_json_atomic_sync(handle.manifest_path, manifest_payload)
        status_value = _normalize_runtime_status(manifest_payload.get("status", STATUS_MANUAL_NEEDED))
        local_attempt_dir = "" if storage_backend == "sqlite" else str(handle.attempt_dir)
        manifest_path_text = "" if storage_backend == "sqlite" else str(handle.manifest_path)
        self._redis.hset(
            self._storage_llm_redis_key(
                stage=handle.stage,
                chunk_id=handle.chunk_id,
                llm_call_id=handle.llm_call_id,
                attempt=handle.attempt,
            ),
            {
                "status": status_value,
                "error_class": error_info["error_class"],
                "error_code": error_info["error_code"],
                "error_message": error_info["error_message"],
                "updated_at_ms": _now_ms(),
                "local_attempt_dir": local_attempt_dir,
            },
        )
        self.append_event(
            scope_type="llm_call",
            scope_id=handle.llm_call_id,
            status=status_value,
            stage=handle.stage,
            chunk_id=handle.chunk_id,
            llm_call_id=handle.llm_call_id,
            attempt=handle.attempt,
            error_class=error_info["error_class"],
            error_code=error_info["error_code"],
            local_path=local_attempt_dir,
            message=error_info["error_message"],
        )
        manifest_scope_variant = str(manifest_payload.get("scope_variant", "") or "")
        llm_scope_ref = self.build_scope_ref(
            stage=handle.stage,
            scope_type="llm_call",
            scope_id=handle.llm_call_id,
            scope_variant=manifest_scope_variant,
        )
        error_record = {
            "schema_version": "runtime_error_record_v1",
            "record_type": "llm_attempt_error",
            "task_id": self.task_id,
            "stage": handle.stage,
            "status": status_value,
            "scope_type": "llm_call",
            "scope_id": handle.llm_call_id,
            "scope_ref": llm_scope_ref,
            "chunk_id": handle.chunk_id,
            "llm_call_id": handle.llm_call_id,
            "attempt": int(handle.attempt or 0),
            "input_fingerprint": str(handle.input_fingerprint or ""),
            "error_class": error_info["error_class"],
            "error_code": error_info["error_code"],
            "error_message": error_info["error_message"],
            "retry_strategy": str(error_info.get("retry_strategy", "") or ""),
            "operator_action": str(error_info.get("operator_action", "") or ""),
            "action_hint": str(error_info.get("action_hint", "") or ""),
            "local_path": local_attempt_dir,
            "request_snapshot": dict(request_snapshot or {}) if isinstance(request_snapshot, dict) else {},
            "source": "python",
            "recorded_at_ms": int(error_payload.get("recorded_at_ms", _now_ms()) or _now_ms()),
        }
        self.append_rt_error_record(error_record)
        if status_value == STATUS_MANUAL_NEEDED:
            manual_retry_record = dict(error_record)
            manual_retry_record["schema_version"] = "runtime_manual_retry_record_v1"
            manual_retry_record["record_type"] = "llm_attempt_manual_retry_required"
            self.append_rt_manual_retry_record(manual_retry_record)
        llm_scope_extra_payload = {
            "chunk_id": handle.chunk_id,
            "attempt": handle.attempt,
            "error_class": error_info["error_class"],
            "error_code": error_info["error_code"],
            "error_message": error_info["error_message"],
        }
        for field_name in ("provider", "request_name", "stage_step", "scope_variant", "unit_id"):
            field_value = manifest_payload.get(field_name)
            if field_value in (None, "", []):
                continue
            llm_scope_extra_payload[field_name] = field_value
        existing_llm_scope = self.load_scope_node(llm_scope_ref)
        self.transition_scope_node(
            scope_ref=llm_scope_ref,
            stage=handle.stage,
            scope_type="llm_call",
            scope_id=handle.llm_call_id,
            scope_variant=manifest_scope_variant,
            status=status_value,
            input_fingerprint=handle.input_fingerprint,
            local_path=local_attempt_dir,
            dependency_fingerprints=manifest_payload.get("dependency_fingerprints"),
            plan_context=_build_scope_plan_context(
                stage=handle.stage,
                scope_type="llm_call",
                scope_id=handle.llm_call_id,
                input_fingerprint=handle.input_fingerprint,
                metadata=manifest_payload,
                request_payload=dict(getattr(handle, "request_payload_cache", {}) or {}),
                existing_context=(existing_llm_scope or {}).get("plan_context"),
            ),
            resource_snapshot=_collect_runtime_resource_snapshot(
                {
                    "attempt": int(handle.attempt or 1),
                    "error_class": error_info["error_class"],
                    "error_code": error_info["error_code"],
                    "error_message": error_info["error_message"],
                    "request_snapshot": dict(request_snapshot or {}) if isinstance(request_snapshot, dict) else {},
                }
            ),
            attempt_count=int(handle.attempt or 1),
            retry_mode="manual" if error_info["error_class"] != ERROR_AUTO_RETRYABLE else "auto",
            required_action=str(error_info.get("action_hint", "") or ""),
            error_class=error_info["error_class"],
            error_code=error_info["error_code"],
            error_message=error_info["error_message"],
            extra_payload=llm_scope_extra_payload,
        )
        if self._sqlite_index is not None:
            try:
                self._sqlite_index.record_llm_attempt_failed(
                    output_dir=str(self.output_dir),
                    task_id=self.task_id,
                    storage_key=self.storage_key,
                    normalized_video_key=self.normalized_video_key,
                    stage=handle.stage,
                    chunk_id=handle.chunk_id,
                    llm_call_id=handle.llm_call_id,
                    input_fingerprint=handle.input_fingerprint,
                    attempt=handle.attempt,
                    request_payload=(
                        request_snapshot
                        if isinstance(request_snapshot, dict)
                        else (
                            dict(getattr(handle, "request_payload_cache", {}) or {})
                            if storage_backend == "sqlite"
                            else _read_json(handle.request_path)
                        )
                    ),
                    manifest_payload=manifest_payload,
                    error_payload=error_payload,
                    attempt_dir=local_attempt_dir,
                    manifest_path=manifest_path_text,
                )
            except Exception as error:
                logger.warning("Runtime recovery SQLite llm failure mirror failed: %s", error)
        self._prune_llm_attempt_history(
            stage=handle.stage,
            chunk_id=handle.chunk_id,
            llm_call_id=handle.llm_call_id,
        )
        return error_payload

    def load_committed_chunk_payload(
        self,
        *,
        stage: str,
        chunk_id: str,
        input_fingerprint: str,
    ) -> Optional[Dict[str, Any]]:
        if self._sqlite_index is None:
            return None
        try:
            return self._sqlite_index.load_latest_committed_chunk(
                output_dir=str(self.output_dir),
                stage=stage,
                chunk_id=chunk_id,
                input_fingerprint=input_fingerprint,
            )
        except Exception as error:
            logger.warning("Runtime recovery SQLite chunk load failed: %s", error)
            return None

    def load_latest_committed_chunk_payload(
        self,
        *,
        stage: str,
        chunk_id: str,
    ) -> Optional[Dict[str, Any]]:
        if self._sqlite_index is None:
            return None
        try:
            return self._sqlite_index.load_latest_committed_chunk_by_chunk_id(
                output_dir=str(self.output_dir),
                stage=stage,
                chunk_id=chunk_id,
            )
        except Exception as error:
            logger.warning("Runtime recovery SQLite latest chunk load failed: %s", error)
            return None

    def record_chunk_state(
        self,
        *,
        stage: str,
        chunk_id: str,
        input_fingerprint: str,
        status: str,
        metadata: Optional[Dict[str, Any]] = None,
        attempt: int = 1,
    ) -> Dict[str, Any]:
        metadata_payload = dict(metadata or {})
        storage_backend = _resolve_chunk_storage_backend(metadata_payload)
        if storage_backend == "sqlite" and self._sqlite_index is None:
            raise RuntimeError(
                f"SQLite authoritative chunk state requires runtime_recovery_sqlite for stage={stage}, chunk_id={chunk_id}"
            )
        chunk_dir_text = ""
        chunk_state_path_text = ""
        chunk_state_payload = {
            "schema_version": "runtime_chunk_state_v1",
            "task_id": self.task_id,
            "stage": str(stage or ""),
            "chunk_id": str(chunk_id or ""),
            "attempt": int(attempt or 1),
            "status": _normalize_runtime_status(status),
            "input_fingerprint": str(input_fingerprint or ""),
            "updated_at_ms": _now_ms(),
        }
        if metadata_payload:
            chunk_state_payload.update(metadata_payload)
        if storage_backend != "sqlite":
            chunk_path = self.chunk_dir(stage=stage, chunk_id=chunk_id, ensure_exists=True)
            chunk_state_path = chunk_path / "chunk_state.json"
            _write_json_atomic_sync(chunk_state_path, chunk_state_payload)
            chunk_dir_text = str(chunk_path)
            chunk_state_path_text = str(chunk_state_path)
        self._redis.hset(
            self._storage_chunk_redis_key(stage=stage, chunk_id=chunk_id),
            {
                "status": str(chunk_state_payload.get("status", "") or ""),
                "attempt": int(attempt or 1),
                "input_fingerprint": input_fingerprint,
                "updated_at_ms": _now_ms(),
                "local_chunk_dir": chunk_dir_text,
            },
        )
        self.append_event(
            scope_type="chunk",
            scope_id=chunk_id,
            status=str(chunk_state_payload.get("status", "") or ""),
            stage=stage,
            chunk_id=chunk_id,
            attempt=int(attempt or 1),
            local_path=chunk_dir_text,
            message=f"chunk state updated: {chunk_state_payload.get('status', '')}",
        )
        scope_variant = str(metadata_payload.get("scope_variant", "") or "")
        self.plan_chunk_scope(
            stage=stage,
            chunk_id=chunk_id,
            input_fingerprint=input_fingerprint,
            metadata=chunk_state_payload,
            scope_variant=scope_variant,
            local_path=chunk_dir_text,
            dependency_fingerprints=metadata_payload.get("dependency_fingerprints"),
            attempt_count=max(0, int(attempt or 1) - 1),
            extra_payload={
                "unit_id": metadata_payload.get("unit_id", ""),
                "stage_step": metadata_payload.get("stage_step", metadata_payload.get("step_name", "")),
                "scope_variant": scope_variant,
            },
        )
        chunk_scope_ref = self.build_scope_ref(
            stage=stage,
            scope_type="chunk",
            scope_id=chunk_id,
            scope_variant=scope_variant,
        )
        existing_chunk_scope = self.load_scope_node(chunk_scope_ref)
        if str(chunk_state_payload.get("status", "") or "") != STATUS_PLANNED:
            self.transition_scope_node(
                scope_ref=chunk_scope_ref,
                stage=stage,
                scope_type="chunk",
                scope_id=chunk_id,
                scope_variant=scope_variant,
                status=str(chunk_state_payload.get("status", "") or ""),
                input_fingerprint=input_fingerprint,
                local_path=chunk_dir_text,
                dependency_fingerprints=metadata_payload.get("dependency_fingerprints"),
                plan_context=_build_scope_plan_context(
                    stage=stage,
                    scope_type="chunk",
                    scope_id=chunk_id,
                    input_fingerprint=input_fingerprint,
                    metadata=metadata_payload,
                    existing_context=(existing_chunk_scope or {}).get("plan_context"),
                ),
                attempt_count=int(attempt or 1),
                extra_payload={
                    "attempt": int(attempt or 1),
                },
            )
        if self._sqlite_index is not None:
            try:
                self._sqlite_index.record_chunk_state(
                    output_dir=str(self.output_dir),
                    task_id=self.task_id,
                    storage_key=self.storage_key,
                    normalized_video_key=self.normalized_video_key,
                    stage=stage,
                    chunk_id=chunk_id,
                    input_fingerprint=input_fingerprint,
                    attempt=int(attempt or 1),
                    chunk_state_payload=chunk_state_payload,
                    chunk_dir=chunk_dir_text,
                    chunk_state_path=chunk_state_path_text,
                )
            except Exception as error:
                logger.warning("Runtime recovery SQLite chunk state mirror failed: %s", error)
        return chunk_state_payload

    def commit_chunk_payload(
        self,
        *,
        stage: str,
        chunk_id: str,
        input_fingerprint: str,
        result_payload: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
        attempt: int = 1,
    ) -> Dict[str, Any]:
        metadata_payload = dict(metadata or {})
        storage_backend = _resolve_chunk_storage_backend(metadata_payload)
        if storage_backend == "sqlite" and self._sqlite_index is None:
            raise RuntimeError(
                f"SQLite authoritative chunk commit requires runtime_recovery_sqlite for stage={stage}, chunk_id={chunk_id}"
            )
        chunk_dir_text = ""
        chunk_state_path_text = ""
        commit_path_text = ""
        chunk_state_payload = {
            "schema_version": "runtime_chunk_state_v1",
            "task_id": self.task_id,
            "stage": str(stage or ""),
            "chunk_id": str(chunk_id or ""),
            "attempt": int(attempt or 1),
            "status": STATUS_SUCCESS,
            "input_fingerprint": str(input_fingerprint or ""),
            "result_hash": _sha256_text(_stable_json_dumps(result_payload)),
            "updated_at_ms": _now_ms(),
        }
        if metadata_payload:
            chunk_state_payload.update(metadata_payload)
        commit_payload = {
            "schema_version": "runtime_chunk_commit_v1",
            "task_id": self.task_id,
            "stage": str(stage or ""),
            "chunk_id": str(chunk_id or ""),
            "attempt": int(attempt or 1),
            "status": STATUS_SUCCESS,
            "input_fingerprint": str(input_fingerprint or ""),
            "result_hash": chunk_state_payload["result_hash"],
            "committed_at_ms": _now_ms(),
        }
        if storage_backend != "sqlite":
            chunk_path = self.chunk_dir(stage=stage, chunk_id=chunk_id, ensure_exists=True)
            result_path = chunk_path / "result.json"
            chunk_state_path = chunk_path / "chunk_state.json"
            commit_path = chunk_path / "commit.json"
            _write_json_atomic_sync(result_path, result_payload)
            _write_json_atomic_sync(chunk_state_path, chunk_state_payload)
            _write_json_atomic_sync(commit_path, commit_payload)
            chunk_dir_text = str(chunk_path)
            chunk_state_path_text = str(chunk_state_path)
            commit_path_text = str(commit_path)
        self._redis.hset(
            self._storage_chunk_redis_key(stage=stage, chunk_id=chunk_id),
            {
                "status": STATUS_SUCCESS,
                "attempt": int(attempt or 1),
                "input_fingerprint": input_fingerprint,
                "local_chunk_dir": chunk_dir_text,
                "updated_at_ms": _now_ms(),
            },
        )
        self.append_event(
            scope_type="chunk",
            scope_id=chunk_id,
            status=STATUS_SUCCESS,
            stage=stage,
            chunk_id=chunk_id,
            attempt=int(attempt or 1),
            local_path=chunk_dir_text,
            message="chunk committed",
        )
        scope_variant = str(metadata_payload.get("scope_variant", "") or "")
        self.plan_chunk_scope(
            stage=stage,
            chunk_id=chunk_id,
            input_fingerprint=input_fingerprint,
            metadata=chunk_state_payload,
            scope_variant=scope_variant,
            local_path=chunk_dir_text,
            dependency_fingerprints=metadata_payload.get("dependency_fingerprints"),
            attempt_count=max(0, int(attempt or 1) - 1),
            extra_payload={
                "unit_id": metadata_payload.get("unit_id", ""),
                "stage_step": metadata_payload.get("stage_step", metadata_payload.get("step_name", "")),
                "scope_variant": scope_variant,
            },
        )
        existing_chunk_scope = self.load_scope_node(
            self.build_scope_ref(
                stage=stage,
                scope_type="chunk",
                scope_id=chunk_id,
                scope_variant=scope_variant,
            )
        )
        self.transition_scope_node(
            scope_ref=self.build_scope_ref(
                stage=stage,
                scope_type="chunk",
                scope_id=chunk_id,
                scope_variant=scope_variant,
            ),
            stage=stage,
            scope_type="chunk",
            scope_id=chunk_id,
            scope_variant=scope_variant,
            status=STATUS_SUCCESS,
            input_fingerprint=input_fingerprint,
            local_path=chunk_dir_text,
            dependency_fingerprints=metadata_payload.get("dependency_fingerprints"),
            plan_context=_build_scope_plan_context(
                stage=stage,
                scope_type="chunk",
                scope_id=chunk_id,
                input_fingerprint=input_fingerprint,
                metadata=metadata_payload,
                existing_context=(existing_chunk_scope or {}).get("plan_context"),
            ),
            attempt_count=int(attempt or 1),
            result_hash=str(chunk_state_payload.get("result_hash", "") or ""),
            extra_payload={
                "attempt": int(attempt or 1),
            },
        )
        if self._sqlite_index is not None:
            try:
                self._sqlite_index.record_chunk_committed(
                    output_dir=str(self.output_dir),
                    task_id=self.task_id,
                    storage_key=self.storage_key,
                    normalized_video_key=self.normalized_video_key,
                    stage=stage,
                    chunk_id=chunk_id,
                    input_fingerprint=input_fingerprint,
                    attempt=int(attempt or 1),
                    result_payload=result_payload,
                    chunk_state_payload=chunk_state_payload,
                    commit_payload=commit_payload,
                    chunk_dir=chunk_dir_text,
                    chunk_state_path=chunk_state_path_text,
                    commit_path=commit_path_text,
                )
            except Exception as error:
                logger.warning("Runtime recovery SQLite chunk commit mirror failed: %s", error)
        return commit_payload

    def fail_chunk_payload(
        self,
        *,
        stage: str,
        chunk_id: str,
        input_fingerprint: str,
        error: Exception,
        metadata: Optional[Dict[str, Any]] = None,
        attempt: int = 1,
    ) -> Dict[str, Any]:
        metadata_payload = dict(metadata or {})
        storage_backend = _resolve_chunk_storage_backend(metadata_payload)
        if storage_backend == "sqlite" and self._sqlite_index is None:
            raise RuntimeError(
                f"SQLite authoritative chunk failure requires runtime_recovery_sqlite for stage={stage}, chunk_id={chunk_id}"
            )
        chunk_dir_text = ""
        chunk_state_path_text = ""
        error_info = classify_runtime_error(error)
        status_value = _derive_scope_failure_status(error_info.get("error_class", ""))
        error_payload = {
            "schema_version": "runtime_chunk_error_v1",
            "task_id": self.task_id,
            "stage": str(stage or ""),
            "chunk_id": str(chunk_id or ""),
            "attempt": int(attempt or 1),
            "input_fingerprint": str(input_fingerprint or ""),
            **error_info,
            "recorded_at_ms": _now_ms(),
        }
        if metadata_payload:
            error_payload["metadata"] = metadata_payload
        chunk_state_payload = {
            "schema_version": "runtime_chunk_state_v1",
            "task_id": self.task_id,
            "stage": str(stage or ""),
            "chunk_id": str(chunk_id or ""),
            "attempt": int(attempt or 1),
            "status": status_value,
            "input_fingerprint": str(input_fingerprint or ""),
            "updated_at_ms": _now_ms(),
            **error_info,
        }
        if metadata_payload:
            chunk_state_payload.update(metadata_payload)
        if storage_backend != "sqlite":
            chunk_path = self.chunk_dir(stage=stage, chunk_id=chunk_id, ensure_exists=True)
            error_path = chunk_path / "error.json"
            chunk_state_path = chunk_path / "chunk_state.json"
            _write_json_atomic_sync(error_path, error_payload)
            _write_json_atomic_sync(chunk_state_path, chunk_state_payload)
            chunk_dir_text = str(chunk_path)
            chunk_state_path_text = str(chunk_state_path)
        self.plan_chunk_scope(
            stage=stage,
            chunk_id=chunk_id,
            input_fingerprint=input_fingerprint,
            metadata=chunk_state_payload,
            scope_variant=str(metadata_payload.get("scope_variant", "") or ""),
            local_path=chunk_dir_text,
            dependency_fingerprints=metadata_payload.get("dependency_fingerprints"),
            attempt_count=max(0, int(attempt or 1) - 1),
            extra_payload={
                "unit_id": metadata_payload.get("unit_id", ""),
                "stage_step": metadata_payload.get("stage_step", metadata_payload.get("step_name", "")),
                "scope_variant": str(metadata_payload.get("scope_variant", "") or ""),
            },
        )
        self._redis.hset(
            self._storage_chunk_redis_key(stage=stage, chunk_id=chunk_id),
            {
                "status": status_value,
                "attempt": int(attempt or 1),
                "input_fingerprint": input_fingerprint,
                "error_class": error_info["error_class"],
                "error_code": error_info["error_code"],
                "error_message": error_info["error_message"],
                "updated_at_ms": _now_ms(),
                "local_chunk_dir": chunk_dir_text,
            },
        )
        self.append_event(
            scope_type="chunk",
            scope_id=chunk_id,
            status=status_value,
            stage=stage,
            chunk_id=chunk_id,
            attempt=int(attempt or 1),
            error_class=error_info["error_class"],
            error_code=error_info["error_code"],
            local_path=chunk_dir_text,
            message=error_info["error_message"],
        )
        scope_variant = str(metadata_payload.get("scope_variant", "") or "")
        chunk_scope_ref = self.build_scope_ref(
            stage=stage,
            scope_type="chunk",
            scope_id=chunk_id,
            scope_variant=scope_variant,
        )
        error_record = {
            "schema_version": "runtime_error_record_v1",
            "record_type": "chunk_error",
            "task_id": self.task_id,
            "stage": str(stage or ""),
            "status": status_value,
            "scope_type": "chunk",
            "scope_id": str(chunk_id or ""),
            "scope_ref": chunk_scope_ref,
            "chunk_id": str(chunk_id or ""),
            "attempt": int(attempt or 0),
            "input_fingerprint": str(input_fingerprint or ""),
            "error_class": error_info["error_class"],
            "error_code": error_info["error_code"],
            "error_message": error_info["error_message"],
            "retry_strategy": str(error_info.get("retry_strategy", "") or ""),
            "operator_action": str(error_info.get("operator_action", "") or ""),
            "action_hint": str(error_info.get("action_hint", "") or ""),
            "local_path": chunk_dir_text,
            "metadata": metadata_payload,
            "source": "python",
            "recorded_at_ms": int(error_payload.get("recorded_at_ms", _now_ms()) or _now_ms()),
        }
        self.append_rt_error_record(error_record)
        if status_value == STATUS_MANUAL_NEEDED:
            manual_retry_record = dict(error_record)
            manual_retry_record["schema_version"] = "runtime_manual_retry_record_v1"
            manual_retry_record["record_type"] = "chunk_manual_retry_required"
            self.append_rt_manual_retry_record(manual_retry_record)
        existing_chunk_scope = self.load_scope_node(chunk_scope_ref)
        self.transition_scope_node(
            scope_ref=chunk_scope_ref,
            stage=stage,
            scope_type="chunk",
            scope_id=chunk_id,
            scope_variant=scope_variant,
            status=_derive_scope_failure_status(error_info.get("error_class", "")),
            input_fingerprint=input_fingerprint,
            local_path=chunk_dir_text,
            dependency_fingerprints=metadata_payload.get("dependency_fingerprints"),
            plan_context=_build_scope_plan_context(
                stage=stage,
                scope_type="chunk",
                scope_id=chunk_id,
                input_fingerprint=input_fingerprint,
                metadata=metadata_payload,
                existing_context=(existing_chunk_scope or {}).get("plan_context"),
            ),
            resource_snapshot=_collect_runtime_resource_snapshot(
                {
                    "attempt": int(attempt or 1),
                    "error_class": error_info["error_class"],
                    "error_code": error_info["error_code"],
                    "error_message": error_info["error_message"],
                }
            ),
            attempt_count=int(attempt or 1),
            retry_mode="manual" if error_info["error_class"] != ERROR_AUTO_RETRYABLE else "auto",
            required_action=str(error_info.get("action_hint", "") or ""),
            error_class=error_info["error_class"],
            error_code=error_info["error_code"],
            error_message=error_info["error_message"],
            extra_payload={
                "attempt": int(attempt or 1),
                "error_class": error_info["error_class"],
                "error_code": error_info["error_code"],
                "error_message": error_info["error_message"],
            },
        )
        if self._sqlite_index is not None:
            try:
                self._sqlite_index.record_chunk_failed(
                    output_dir=str(self.output_dir),
                    task_id=self.task_id,
                    storage_key=self.storage_key,
                    normalized_video_key=self.normalized_video_key,
                    stage=stage,
                    chunk_id=chunk_id,
                    input_fingerprint=input_fingerprint,
                    attempt=int(attempt or 1),
                    chunk_state_payload=chunk_state_payload,
                    error_payload=error_payload,
                    chunk_dir=chunk_dir_text,
                    chunk_state_path=chunk_state_path_text,
                )
            except Exception as error:
                logger.warning("Runtime recovery SQLite chunk failure mirror failed: %s", error)
        return error_payload

    def list_sqlite_llm_records(
        self,
        *,
        stage: str = "",
        status: str = "",
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        if self._sqlite_index is None:
            return []
        try:
            return self._sqlite_index.list_llm_records(
                output_dir=str(self.output_dir),
                task_id=self.task_id,
                stage=stage or None,
                status=status or None,
                limit=limit,
            )
        except Exception as error:
            logger.warning("Runtime recovery SQLite llm list failed: %s", error)
            return []

    def list_sqlite_chunk_records(
        self,
        *,
        stage: str = "",
        status: str = "",
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        if self._sqlite_index is None:
            return []
        try:
            return self._sqlite_index.list_chunk_records(
                output_dir=str(self.output_dir),
                task_id=self.task_id,
                stage=stage or None,
                status=status or None,
                limit=limit,
            )
        except Exception as error:
            logger.warning("Runtime recovery SQLite chunk list failed: %s", error)
            return []

    def commit_projection_payload(
        self,
        *,
        stage: str,
        projection_name: str,
        payload: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
        attempt: int = 1,
    ) -> Dict[str, Any]:
        if not isinstance(payload, dict):
            raise ValueError("projection payload must be a dict")
        projection_chunk_id = self.build_projection_chunk_id(projection_name=projection_name)
        projection_input_fingerprint = build_runtime_payload_fingerprint(
            {
                "stage": str(stage or ""),
                "projection_name": str(projection_name or ""),
                "payload": payload,
            }
        )
        metadata_payload = dict(metadata or {})
        metadata_payload.setdefault("storage_backend", "sqlite")
        metadata_payload.setdefault("scope_variant", "projection")
        metadata_payload.setdefault("projection_name", str(projection_name or ""))
        self.commit_chunk_payload(
            stage=stage,
            chunk_id=projection_chunk_id,
            input_fingerprint=projection_input_fingerprint,
            result_payload=payload,
            metadata=metadata_payload,
            attempt=attempt,
        )
        return {
            "chunk_id": projection_chunk_id,
            "input_fingerprint": projection_input_fingerprint,
            "payload": dict(payload),
        }

    def load_projection_payload(
        self,
        *,
        stage: str,
        projection_name: str,
    ) -> Optional[Dict[str, Any]]:
        projection_chunk_id = self.build_projection_chunk_id(projection_name=projection_name)
        if self._sqlite_index is not None:
            try:
                restored = self._sqlite_index.load_latest_committed_chunk_by_chunk_id(
                    output_dir=str(self.output_dir),
                    stage=stage,
                    chunk_id=projection_chunk_id,
                )
                if isinstance(restored, dict):
                    result_payload = restored.get("result_payload")
                    if isinstance(result_payload, dict):
                        return dict(result_payload)
            except Exception as error:
                logger.warning(
                    "Runtime recovery SQLite projection load failed: stage=%s projection=%s err=%s",
                    stage,
                    projection_name,
                    error,
                )
        return None

    def batch_load_committed_llm_responses(
        self,
        requests: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        if self._sqlite_index is None:
            results: List[Dict[str, Any]] = []
            for request in list(requests or []):
                if not isinstance(request, dict):
                    continue
                results.append(
                    {
                        "request": dict(request),
                        "restored": self.load_committed_llm_response(
                            stage=str(request.get("stage", "") or ""),
                            chunk_id=str(request.get("chunk_id", "") or ""),
                            llm_call_id=str(request.get("llm_call_id", "") or ""),
                            input_fingerprint=str(request.get("input_fingerprint", "") or ""),
                        ),
                    }
                )
            return results
        try:
            return self._sqlite_index.batch_load_committed_llm(
                output_dir=str(self.output_dir),
                requests=requests,
            )
        except Exception as error:
            logger.warning("Runtime recovery SQLite llm batch load failed: %s", error)
            return []

    def batch_load_committed_chunk_payloads(
        self,
        requests: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        if self._sqlite_index is None:
            results: List[Dict[str, Any]] = []
            for request in list(requests or []):
                if not isinstance(request, dict):
                    continue
                results.append(
                    {
                        "request": dict(request),
                        "restored": self.load_committed_chunk_payload(
                            stage=str(request.get("stage", "") or ""),
                            chunk_id=str(request.get("chunk_id", "") or ""),
                            input_fingerprint=str(request.get("input_fingerprint", "") or ""),
                        ),
                    }
                )
            return results
        try:
            return self._sqlite_index.batch_load_committed_chunk(
                output_dir=str(self.output_dir),
                requests=requests,
            )
        except Exception as error:
            logger.warning("Runtime recovery SQLite chunk batch load failed: %s", error)
            return []

    @staticmethod
    def build_llm_restore_cache_key(
        *,
        stage: str,
        chunk_id: str,
        llm_call_id: str,
        input_fingerprint: str,
    ) -> Tuple[str, str, str, str]:
        return (
            str(stage or "").strip(),
            str(chunk_id or "").strip(),
            str(llm_call_id or "").strip(),
            str(input_fingerprint or "").strip(),
        )

    @staticmethod
    def build_chunk_restore_cache_key(
        *,
        stage: str,
        chunk_id: str,
        input_fingerprint: str,
    ) -> Tuple[str, str, str]:
        return (
            str(stage or "").strip(),
            str(chunk_id or "").strip(),
            str(input_fingerprint or "").strip(),
        )

    def prefetch_restorable_llm_scope_cache(
        self,
        *,
        stage: str,
        candidate_scope_refs: Optional[List[str]] = None,
        candidate_chunk_ids: Optional[List[str]] = None,
        candidate_llm_call_ids: Optional[List[str]] = None,
        limit: int = 4000,
    ) -> Dict[str, Any]:
        normalized_stage = str(stage or "").strip()
        if not normalized_stage:
            return {"cache": {}, "scope_hints": [], "pending_hints": [], "summary": {"stage": "", "scope_type": "llm_call"}}
        scope_ref_filter = {str(item or "").strip() for item in list(candidate_scope_refs or []) if str(item or "").strip()}
        chunk_filter = {str(item or "").strip() for item in list(candidate_chunk_ids or []) if str(item or "").strip()}
        llm_call_filter = {str(item or "").strip() for item in list(candidate_llm_call_ids or []) if str(item or "").strip()}

        def _match_hint(hint: Dict[str, Any]) -> bool:
            scope_ref = str(hint.get("scope_ref", "") or "").strip()
            chunk_id = str(hint.get("chunk_id", "") or "").strip()
            llm_call_id = str(hint.get("llm_call_id", "") or "").strip()
            if scope_ref_filter and scope_ref not in scope_ref_filter:
                return False
            if chunk_filter and chunk_id not in chunk_filter:
                return False
            if llm_call_filter and llm_call_id not in llm_call_filter:
                return False
            return True

        scope_hints = [
            hint
            for hint in self.list_scope_hints(
                stage=normalized_stage,
                scope_type="llm_call",
                refresh_from_scope_graph=False,
                limit=limit,
            )
            if _match_hint(hint)
        ]
        pending_hints = [
            hint
            for hint in self.list_scope_hints(
                stage=normalized_stage,
                scope_type="llm_call",
                pending_only=True,
                refresh_from_scope_graph=False,
                limit=limit,
            )
            if _match_hint(hint)
        ]
        requests: List[Dict[str, Any]] = []
        for hint in scope_hints:
            if int(hint.get("can_restore", 0) or 0) != 1:
                continue
            chunk_id = str(hint.get("chunk_id", "") or "").strip()
            llm_call_id = str(hint.get("llm_call_id", "") or "").strip()
            input_fingerprint = str(hint.get("input_fingerprint", "") or "").strip()
            if not chunk_id or not llm_call_id or not input_fingerprint:
                continue
            requests.append(
                {
                    "stage": normalized_stage,
                    "chunk_id": chunk_id,
                    "llm_call_id": llm_call_id,
                    "input_fingerprint": input_fingerprint,
                    "scope_ref": str(hint.get("scope_ref", "") or "").strip(),
                }
            )
        cache: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
        for item in self.batch_load_committed_llm_responses(requests):
            request = dict(item.get("request", {}) or {})
            restored = item.get("restored")
            if not isinstance(restored, dict):
                continue
            cache[
                self.build_llm_restore_cache_key(
                    stage=normalized_stage,
                    chunk_id=str(request.get("chunk_id", "") or ""),
                    llm_call_id=str(request.get("llm_call_id", "") or ""),
                    input_fingerprint=str(request.get("input_fingerprint", "") or ""),
                )
            ] = dict(restored)
        return {
            "cache": cache,
            "scope_hints": scope_hints,
            "pending_hints": pending_hints,
            "summary": {
                "stage": normalized_stage,
                "scope_type": "llm_call",
                "hint_count": len(scope_hints),
                "pending_count": len(pending_hints),
                "restorable_hint_count": len(requests),
                "prefetched_restore_count": len(cache),
            },
        }

    def prefetch_restorable_chunk_scope_cache(
        self,
        *,
        stage: str,
        candidate_scope_refs: Optional[List[str]] = None,
        candidate_chunk_ids: Optional[List[str]] = None,
        limit: int = 4000,
    ) -> Dict[str, Any]:
        normalized_stage = str(stage or "").strip()
        if not normalized_stage:
            return {"cache": {}, "scope_hints": [], "pending_hints": [], "summary": {"stage": "", "scope_type": "chunk"}}
        scope_ref_filter = {str(item or "").strip() for item in list(candidate_scope_refs or []) if str(item or "").strip()}
        chunk_filter = {str(item or "").strip() for item in list(candidate_chunk_ids or []) if str(item or "").strip()}

        def _match_hint(hint: Dict[str, Any]) -> bool:
            scope_ref = str(hint.get("scope_ref", "") or "").strip()
            chunk_id = str(hint.get("chunk_id", "") or "").strip()
            if scope_ref_filter and scope_ref not in scope_ref_filter:
                return False
            if chunk_filter and chunk_id not in chunk_filter:
                return False
            return True

        scope_hints = [
            hint
            for hint in self.list_scope_hints(
                stage=normalized_stage,
                scope_type="chunk",
                refresh_from_scope_graph=False,
                limit=limit,
            )
            if _match_hint(hint)
        ]
        pending_hints = [
            hint
            for hint in self.list_scope_hints(
                stage=normalized_stage,
                scope_type="chunk",
                pending_only=True,
                refresh_from_scope_graph=False,
                limit=limit,
            )
            if _match_hint(hint)
        ]
        requests: List[Dict[str, Any]] = []
        for hint in scope_hints:
            if int(hint.get("can_restore", 0) or 0) != 1:
                continue
            chunk_id = str(hint.get("chunk_id", "") or "").strip()
            input_fingerprint = str(hint.get("input_fingerprint", "") or "").strip()
            if not chunk_id or not input_fingerprint:
                continue
            requests.append(
                {
                    "stage": normalized_stage,
                    "chunk_id": chunk_id,
                    "input_fingerprint": input_fingerprint,
                    "scope_ref": str(hint.get("scope_ref", "") or "").strip(),
                }
            )
        cache: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        for item in self.batch_load_committed_chunk_payloads(requests):
            request = dict(item.get("request", {}) or {})
            restored = item.get("restored")
            if not isinstance(restored, dict):
                continue
            cache[
                self.build_chunk_restore_cache_key(
                    stage=normalized_stage,
                    chunk_id=str(request.get("chunk_id", "") or ""),
                    input_fingerprint=str(request.get("input_fingerprint", "") or ""),
                )
            ] = dict(restored)
        return {
            "cache": cache,
            "scope_hints": scope_hints,
            "pending_hints": pending_hints,
            "summary": {
                "stage": normalized_stage,
                "scope_type": "chunk",
                "hint_count": len(scope_hints),
                "pending_count": len(pending_hints),
                "restorable_hint_count": len(requests),
                "prefetched_restore_count": len(cache),
            },
        }

    def list_scope_hints(
        self,
        *,
        stage: str = "",
        scope_type: str = "",
        latest_status: str = "",
        retry_mode: str = "",
        pending_only: bool = False,
        refresh_from_scope_graph: bool = True,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        if self._sqlite_index is None:
            return []
        try:
            if refresh_from_scope_graph:
                self.sync_scope_hints_from_scope_graph(stage=stage or "")
            return self._sqlite_index.list_scope_hints(
                output_dir=str(self.output_dir),
                task_id=self.task_id,
                stage=stage or None,
                scope_type=scope_type or None,
                latest_status=latest_status or None,
                retry_mode=retry_mode or None,
                pending_only=pending_only,
                limit=limit,
            )
        except Exception as error:
            logger.warning("Runtime recovery SQLite scope hint list failed: %s", error)
            return []

    def list_pending_scope_hints(
        self,
        *,
        stage: str = "",
        scope_type: str = "",
        retry_mode: str = "",
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        return self.list_scope_hints(
            stage=stage,
            scope_type=scope_type,
            retry_mode=retry_mode,
            pending_only=True,
            refresh_from_scope_graph=True,
            limit=limit,
        )

    def persist_observed_llm_interaction(
        self,
        *,
        stage: str,
        chunk_id: str,
        llm_call_id: str,
        input_fingerprint: str,
        request_payload: Dict[str, Any],
        response_text: str = "",
        response_metadata: Optional[Dict[str, Any]] = None,
        error: Optional[Exception] = None,
        metadata: Optional[Dict[str, Any]] = None,
        max_part_bytes: int = 262144,
    ) -> Dict[str, Any]:
        if error is None:
            restored = self.load_committed_llm_response(
                stage=stage,
                chunk_id=chunk_id,
                llm_call_id=llm_call_id,
                input_fingerprint=input_fingerprint,
            )
            if restored is not None:
                return dict(restored.get("commit_payload", {}) or {})

        handle = self.begin_llm_attempt(
            stage=stage,
            chunk_id=chunk_id,
            llm_call_id=llm_call_id,
            input_fingerprint=input_fingerprint,
            request_payload=request_payload,
            metadata=metadata,
        )
        if error is None:
            return self.commit_llm_attempt(
                handle=handle,
                response_text=response_text,
                response_metadata=response_metadata,
                max_part_bytes=max_part_bytes,
            )
        return self.fail_llm_attempt(
            handle=handle,
            error=error,
            request_snapshot=request_payload,
        )
