"""
运行态恢复存储：
1) 本地文件是安全提交真源。
2) Redis 仅作为可选热状态镜像。
3) Phase2B LLM 调用与 Phase2A chunk 共享同一套 manifest/commit 协议。
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from services.python_grpc.src.common.utils.async_disk_writer import enqueue_json_write, flush_async_json_writes

logger = logging.getLogger(__name__)

STATUS_PLANNED = "PLANNED"
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

_REDIS_IMPORT_ERROR_LOGGED = False


def _stable_json_dumps(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _sha256_text(value: str) -> str:
    return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


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


def _write_json_atomic_sync(path: Path, payload: Any) -> None:
    target_path = Path(path)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = target_path.with_name(
        f"{target_path.name}.tmp.{os.getpid()}.{time.time_ns()}"
    )
    with open(tmp_path, "w", encoding="utf-8") as output_stream:
        json.dump(payload, output_stream, ensure_ascii=False, indent=2, default=str)
        output_stream.flush()
        os.fsync(output_stream.fileno())
    os.replace(tmp_path, target_path)
    _fsync_parent_dir(target_path)


def _read_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        with open(path, "r", encoding="utf-8") as input_stream:
            payload = json.load(input_stream)
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


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


class RuntimeRecoveryStore:
    """封装本地真源 + 可选 Redis 热状态镜像。"""

    def __init__(self, *, output_dir: str, task_id: str = "") -> None:
        resolved_output_dir = str(output_dir or "").strip()
        if not resolved_output_dir:
            raise ValueError("output_dir is required")
        output_path = Path(resolved_output_dir).resolve()
        self.output_dir = output_path
        self.task_id = str(task_id or output_path.name or "unknown_task").strip() or "unknown_task"
        self.runtime_root = output_path / "intermediates" / "rt"
        self.runtime_root.mkdir(parents=True, exist_ok=True)
        self._redis = _RuntimeRedisMirror()

    def build_llm_call_id(self, *, step_name: str, unit_id: str, input_fingerprint: str) -> str:
        suffix = _sha256_text(f"{step_name}|{unit_id}|{input_fingerprint}")[:16]
        return f"lc_{suffix}"

    def build_chunk_id(self, *, chunk_index: int, prefix: str = "c") -> str:
        safe_prefix = str(prefix or "c").strip() or "c"
        return f"{safe_prefix}{int(chunk_index) + 1:06d}"

    def stage_dir(self, stage: str) -> Path:
        safe_stage = str(stage or "unknown").strip() or "unknown"
        path = self.runtime_root / "s" / safe_stage
        path.mkdir(parents=True, exist_ok=True)
        return path

    def chunk_dir(self, *, stage: str, chunk_id: str) -> Path:
        safe_chunk_id = str(chunk_id or "unknown_chunk").strip() or "unknown_chunk"
        path = self.stage_dir(stage) / "c" / safe_chunk_id
        path.mkdir(parents=True, exist_ok=True)
        return path

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
            "status": str(status or ""),
            "updated_at_ms": _now_ms(),
        }
        if isinstance(payload, dict):
            state_payload.update(payload)
        _write_json_atomic_sync(stage_path, state_payload)
        self._redis.hset(
            f"task:{self.task_id}:stage:{stage}",
            state_payload,
        )

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
        self._redis.xadd(f"task:{self.task_id}:events", event_payload)

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
        llm_root = self.chunk_dir(stage=stage, chunk_id=chunk_id) / "l" / llm_call_id
        llm_root.mkdir(parents=True, exist_ok=True)
        attempt = 1
        for child in llm_root.iterdir():
            if not child.is_dir():
                continue
            child_name = child.name.strip().lower()
            if not child_name.startswith("a"):
                continue
            try:
                attempt = max(attempt, int(child_name[1:]) + 1)
            except Exception:
                continue
        attempt_dir = llm_root / f"a{attempt:03d}"
        attempt_dir.mkdir(parents=True, exist_ok=True)
        response_parts_dir = attempt_dir / "p"
        response_parts_dir.mkdir(parents=True, exist_ok=True)
        manifest_payload = {
            "schema_version": "runtime_llm_manifest_v1",
            "task_id": self.task_id,
            "stage": str(stage or ""),
            "chunk_id": str(chunk_id or ""),
            "llm_call_id": str(llm_call_id or ""),
            "attempt": attempt,
            "status": STATUS_LOCAL_WRITING,
            "input_fingerprint": str(input_fingerprint or ""),
            "created_at_ms": _now_ms(),
            "updated_at_ms": _now_ms(),
        }
        if isinstance(metadata, dict):
            manifest_payload.update(metadata)
        request_path = attempt_dir / "request.json"
        manifest_path = attempt_dir / "manifest.json"
        _write_json_atomic_sync(manifest_path, manifest_payload)
        _write_json_atomic_sync(request_path, request_payload)
        self._redis.hset(
            f"task:{self.task_id}:llm:{stage}:{chunk_id}:{llm_call_id}:a{attempt:03d}",
            {
                "status": STATUS_LOCAL_WRITING,
                "task_id": self.task_id,
                "stage": stage,
                "chunk_id": chunk_id,
                "llm_call_id": llm_call_id,
                "attempt": attempt,
                "input_fingerprint": input_fingerprint,
                "local_attempt_dir": str(attempt_dir),
                "updated_at_ms": _now_ms(),
            },
        )
        self.append_event(
            scope_type="llm_call",
            scope_id=llm_call_id,
            status=STATUS_LOCAL_WRITING,
            stage=stage,
            chunk_id=chunk_id,
            llm_call_id=llm_call_id,
            attempt=attempt,
            local_path=str(attempt_dir),
            message="llm attempt started",
        )
        return RuntimeAttemptHandle(
            stage=stage,
            chunk_id=chunk_id,
            llm_call_id=llm_call_id,
            attempt=attempt,
            attempt_dir=attempt_dir,
            manifest_path=manifest_path,
            request_path=request_path,
            response_parts_dir=response_parts_dir,
            input_fingerprint=input_fingerprint,
            scope_key=str(attempt_dir.resolve()),
        )

    def load_committed_llm_response(
        self,
        *,
        stage: str,
        chunk_id: str,
        llm_call_id: str,
        input_fingerprint: str,
    ) -> Optional[Dict[str, Any]]:
        llm_root = self.chunk_dir(stage=stage, chunk_id=chunk_id) / "l" / llm_call_id
        if not llm_root.exists():
            return None
        attempt_dirs = sorted(
            [child for child in llm_root.iterdir() if child.is_dir() and child.name.lower().startswith("a")],
            key=lambda item: item.name,
            reverse=True,
        )
        for attempt_dir in attempt_dirs:
            commit_payload = _read_json(attempt_dir / "commit.json")
            manifest_payload = _read_json(attempt_dir / "manifest.json")
            if not commit_payload or not manifest_payload:
                continue
            if str(commit_payload.get("status", "") or "") != STATUS_LOCAL_COMMITTED:
                continue
            if str(commit_payload.get("input_fingerprint", "") or "") != str(input_fingerprint or ""):
                continue
            expected_parts = int(commit_payload.get("committed_parts", 0) or 0)
            loaded_parts: List[Tuple[int, str]] = []
            for part_path in sorted((attempt_dir / "p").glob("p*.json")):
                payload = _read_json(part_path)
                if not payload:
                    loaded_parts = []
                    break
                loaded_parts.append(
                    (
                        int(payload.get("part_index", 0) or 0),
                        str(payload.get("content", "") or ""),
                    )
                )
            if expected_parts > 0 and len(loaded_parts) != expected_parts:
                continue
            loaded_parts.sort(key=lambda item: item[0])
            response_text = "".join(content for _, content in loaded_parts)
            response_hash = _sha256_text(response_text)
            if response_hash != str(commit_payload.get("response_hash", "") or ""):
                continue
            return {
                "response_text": response_text,
                "attempt": int(commit_payload.get("attempt", 0) or 0),
                "commit_payload": commit_payload,
                "manifest_payload": manifest_payload,
                "attempt_dir": str(attempt_dir),
            }
        return None

    def commit_llm_attempt(
        self,
        *,
        handle: RuntimeAttemptHandle,
        response_text: str,
        response_metadata: Optional[Dict[str, Any]] = None,
        max_part_bytes: int = 262144,
    ) -> Dict[str, Any]:
        parts, response_hash = split_text_parts_by_bytes(response_text, max_part_bytes=max_part_bytes)
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
                    str(handle.response_parts_dir / f"p{part_index:04d}.json"),
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
                _write_json_atomic_sync(handle.response_parts_dir / f"p{part_index:04d}.json", part_payload)
        manifest_payload = _read_json(handle.manifest_path) or {}
        manifest_payload.update(
            {
                "status": STATUS_LOCAL_COMMITTED,
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
        _write_json_atomic_sync(handle.manifest_path, manifest_payload)
        commit_payload = {
            "schema_version": "runtime_llm_commit_v1",
            "task_id": self.task_id,
            "stage": handle.stage,
            "chunk_id": handle.chunk_id,
            "llm_call_id": handle.llm_call_id,
            "attempt": handle.attempt,
            "status": STATUS_LOCAL_COMMITTED,
            "input_fingerprint": handle.input_fingerprint,
            "manifest_hash": _sha256_text(_stable_json_dumps(manifest_payload)),
            "response_hash": response_hash,
            "committed_parts": len(parts),
            "final_bytes": len(str(response_text or "").encode("utf-8")),
            "committed_at_ms": _now_ms(),
        }
        _write_json_atomic_sync(handle.attempt_dir / "commit.json", commit_payload)
        self._redis.hset(
            f"task:{self.task_id}:llm:{handle.stage}:{handle.chunk_id}:{handle.llm_call_id}:a{handle.attempt:03d}",
            {
                "status": STATUS_LOCAL_COMMITTED,
                "response_hash": response_hash,
                "committed_parts": len(parts),
                "updated_at_ms": _now_ms(),
                "local_attempt_dir": str(handle.attempt_dir),
            },
        )
        self.append_event(
            scope_type="llm_call",
            scope_id=handle.llm_call_id,
            status=STATUS_LOCAL_COMMITTED,
            stage=handle.stage,
            chunk_id=handle.chunk_id,
            llm_call_id=handle.llm_call_id,
            attempt=handle.attempt,
            local_path=str(handle.attempt_dir),
            message="llm attempt committed",
        )
        return commit_payload

    def fail_llm_attempt(
        self,
        *,
        handle: RuntimeAttemptHandle,
        error: Exception,
        request_snapshot: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
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
        _write_json_atomic_sync(handle.attempt_dir / "error.json", error_payload)
        manifest_payload = _read_json(handle.manifest_path) or {}
        manifest_payload.update(
            {
                "status": ERROR_AUTO_RETRYABLE == error_info["error_class"] and STATUS_AUTO_RETRY_WAIT or (
                    ERROR_FATAL_NON_RETRYABLE == error_info["error_class"] and STATUS_FATAL or STATUS_MANUAL_RETRY_REQUIRED
                ),
                "updated_at_ms": _now_ms(),
                **error_info,
            }
        )
        _write_json_atomic_sync(handle.manifest_path, manifest_payload)
        status_value = str(manifest_payload.get("status", STATUS_MANUAL_RETRY_REQUIRED) or STATUS_MANUAL_RETRY_REQUIRED)
        self._redis.hset(
            f"task:{self.task_id}:llm:{handle.stage}:{handle.chunk_id}:{handle.llm_call_id}:a{handle.attempt:03d}",
            {
                "status": status_value,
                "error_class": error_info["error_class"],
                "error_code": error_info["error_code"],
                "error_message": error_info["error_message"],
                "updated_at_ms": _now_ms(),
                "local_attempt_dir": str(handle.attempt_dir),
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
            local_path=str(handle.attempt_dir),
            message=error_info["error_message"],
        )
        return error_payload

    def load_committed_chunk_payload(
        self,
        *,
        stage: str,
        chunk_id: str,
        input_fingerprint: str,
    ) -> Optional[Dict[str, Any]]:
        chunk_path = self.chunk_dir(stage=stage, chunk_id=chunk_id)
        commit_payload = _read_json(chunk_path / "commit.json")
        chunk_state = _read_json(chunk_path / "chunk_state.json")
        result_payload = _read_json(chunk_path / "result.json")
        if not commit_payload or not chunk_state or result_payload is None:
            return None
        if str(commit_payload.get("status", "") or "") != STATUS_LOCAL_COMMITTED:
            return None
        if str(commit_payload.get("input_fingerprint", "") or "") != str(input_fingerprint or ""):
            return None
        return {
            "commit_payload": commit_payload,
            "chunk_state": chunk_state,
            "result_payload": result_payload,
            "chunk_dir": str(chunk_path),
        }

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
        chunk_path = self.chunk_dir(stage=stage, chunk_id=chunk_id)
        result_path = chunk_path / "result.json"
        chunk_state_path = chunk_path / "chunk_state.json"
        _write_json_atomic_sync(result_path, result_payload)
        chunk_state_payload = {
            "schema_version": "runtime_chunk_state_v1",
            "task_id": self.task_id,
            "stage": str(stage or ""),
            "chunk_id": str(chunk_id or ""),
            "attempt": int(attempt or 1),
            "status": STATUS_LOCAL_COMMITTED,
            "input_fingerprint": str(input_fingerprint or ""),
            "result_hash": _sha256_text(_stable_json_dumps(result_payload)),
            "updated_at_ms": _now_ms(),
        }
        if isinstance(metadata, dict):
            chunk_state_payload.update(metadata)
        _write_json_atomic_sync(chunk_state_path, chunk_state_payload)
        commit_payload = {
            "schema_version": "runtime_chunk_commit_v1",
            "task_id": self.task_id,
            "stage": str(stage or ""),
            "chunk_id": str(chunk_id or ""),
            "attempt": int(attempt or 1),
            "status": STATUS_LOCAL_COMMITTED,
            "input_fingerprint": str(input_fingerprint or ""),
            "result_hash": chunk_state_payload["result_hash"],
            "committed_at_ms": _now_ms(),
        }
        _write_json_atomic_sync(chunk_path / "commit.json", commit_payload)
        self._redis.hset(
            f"task:{self.task_id}:chunk:{stage}:{chunk_id}",
            {
                "status": STATUS_LOCAL_COMMITTED,
                "attempt": int(attempt or 1),
                "input_fingerprint": input_fingerprint,
                "local_chunk_dir": str(chunk_path),
                "updated_at_ms": _now_ms(),
            },
        )
        self.append_event(
            scope_type="chunk",
            scope_id=chunk_id,
            status=STATUS_LOCAL_COMMITTED,
            stage=stage,
            chunk_id=chunk_id,
            attempt=int(attempt or 1),
            local_path=str(chunk_path),
            message="chunk committed",
        )
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
        chunk_path = self.chunk_dir(stage=stage, chunk_id=chunk_id)
        error_info = classify_runtime_error(error)
        status_value = (
            STATUS_AUTO_RETRY_WAIT
            if error_info["error_class"] == ERROR_AUTO_RETRYABLE
            else STATUS_FATAL
            if error_info["error_class"] == ERROR_FATAL_NON_RETRYABLE
            else STATUS_MANUAL_RETRY_REQUIRED
        )
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
        if isinstance(metadata, dict):
            error_payload["metadata"] = metadata
        _write_json_atomic_sync(chunk_path / "error.json", error_payload)
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
        if isinstance(metadata, dict):
            chunk_state_payload.update(metadata)
        _write_json_atomic_sync(chunk_path / "chunk_state.json", chunk_state_payload)
        self._redis.hset(
            f"task:{self.task_id}:chunk:{stage}:{chunk_id}",
            {
                "status": status_value,
                "attempt": int(attempt or 1),
                "input_fingerprint": input_fingerprint,
                "error_class": error_info["error_class"],
                "error_code": error_info["error_code"],
                "error_message": error_info["error_message"],
                "updated_at_ms": _now_ms(),
                "local_chunk_dir": str(chunk_path),
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
            local_path=str(chunk_path),
            message=error_info["error_message"],
        )
        return error_payload
