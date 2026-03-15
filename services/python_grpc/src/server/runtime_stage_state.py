"""阶段级运行态状态写入辅助。

职责边界：
- 只负责把 gRPC 阶段 checkpoint 规范化后写入 `RuntimeRecoveryStore`。
- 提供接近 AOP 的阶段会话，把心跳、checkpoint 与错误分类从业务代码里剥离。
- 不依赖 gRPC server、protobuf 或大型业务模块，便于在轻量测试中直接验证。
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Callable, Dict, Optional, Tuple

from services.python_grpc.src.common.utils.runtime_recovery_store import (
    RuntimeRecoveryStore,
    STATUS_AUTO_RETRY_WAIT,
    STATUS_COMPLETED,
    STATUS_EXECUTING,
    STATUS_FATAL,
    STATUS_MANUAL_RETRY_REQUIRED,
    classify_runtime_error,
)

logger = logging.getLogger(__name__)

HeartbeatEmitter = Callable[..., None]
HeartbeatEventEmitter = Callable[[Dict[str, Any]], None]


def resolve_runtime_stage_failure_status(
    *,
    error: Optional[Exception] = None,
    error_message: str = "",
) -> Tuple[str, Dict[str, str]]:
    runtime_error = error
    if runtime_error is None:
        runtime_error = RuntimeError(str(error_message or "runtime stage failed"))
    error_info = classify_runtime_error(runtime_error)
    error_class = str(error_info.get("error_class", "") or "")
    if error_class == "AUTO_RETRYABLE":
        return STATUS_AUTO_RETRY_WAIT, error_info
    if error_class == "FATAL_NON_RETRYABLE":
        return STATUS_FATAL, error_info
    return STATUS_MANUAL_RETRY_REQUIRED, error_info


def build_runtime_retry_guidance(error_info: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    details = dict(error_info or {})
    error_class = str(details.get("error_class", "") or "")
    error_message = str(details.get("error_message", "") or "")
    lowered = error_message.lower()

    if error_class == "AUTO_RETRYABLE":
        return {
            "retry_mode": "auto",
            "retry_recommended": True,
            "required_action": "系统自动退避重试，无需人工介入。",
            "retry_entry_point": "from_last_checkpoint",
        }

    if error_class == "FATAL_NON_RETRYABLE":
        return {
            "retry_mode": "blocked",
            "retry_recommended": False,
            "required_action": "先修复输入数据或代码契约，再重新触发该阶段。",
            "retry_entry_point": "from_operator_restart",
        }

    if any(token in lowered for token in ("quota", "balance", "insufficient credits")):
        required_action = "先补充额度或配额，再从当前阶段/当前 chunk 重试。"
    elif any(token in lowered for token in ("unauthorized", "forbidden", "invalid api key", "authentication", "credential")):
        required_action = "先修复鉴权凭证，再从当前阶段/当前 chunk 重试。"
    elif any(token in lowered for token in ("out of memory", "cannot allocate memory", "memoryerror", "oom")):
        required_action = "先降低并发或缩小 chunk，并确认内存恢复后再重试。"
    elif any(token in lowered for token in ("disk full", "no space left")):
        required_action = "先释放磁盘空间，再从当前阶段/当前 chunk 重试。"
    else:
        required_action = "需要人工检查错误上下文后，从当前阶段/当前 chunk 重试。"

    return {
        "retry_mode": "manual",
        "retry_recommended": True,
        "required_action": required_action,
        "retry_entry_point": "from_last_checkpoint",
    }


def record_runtime_stage_checkpoint(
    *,
    store: RuntimeRecoveryStore,
    output_dir: str,
    stage: str,
    status: str,
    checkpoint: str,
    completed: Any,
    pending: Any,
    error: Optional[Exception] = None,
    error_message: str = "",
    extra_payload: Optional[Dict[str, Any]] = None,
    message: str = "",
) -> None:
    normalized_stage = str(stage or "").strip() or "unknown_stage"
    normalized_checkpoint = str(checkpoint or "unknown").strip() or "unknown"
    normalized_status = str(status or "running").strip().lower() or "running"

    try:
        safe_completed = max(0, int(completed))
    except Exception:
        safe_completed = 0
    try:
        safe_pending = max(0, int(pending))
    except Exception:
        safe_pending = 0

    if normalized_status == "completed":
        runtime_status = STATUS_COMPLETED
        error_info: Dict[str, str] = {}
    elif normalized_status in {"failed", "error"}:
        runtime_status, error_info = resolve_runtime_stage_failure_status(
            error=error,
            error_message=error_message,
        )
    else:
        runtime_status = STATUS_EXECUTING
        error_info = {}

    state_payload: Dict[str, Any] = {
        "checkpoint": normalized_checkpoint,
        "completed": safe_completed,
        "pending": safe_pending,
        "original_status": normalized_status,
        "output_dir": str(output_dir or ""),
        "updated_by": "grpc_service_impl",
    }
    if isinstance(extra_payload, dict):
        state_payload.update(extra_payload)
    if error_info:
        state_payload.update(error_info)
        state_payload.update(build_runtime_retry_guidance(error_info))
    elif error_message:
        state_payload["error_message"] = str(error_message or "")

    try:
        store.update_stage_state(
            stage=normalized_stage,
            status=runtime_status,
            payload=state_payload,
        )
        stage_state_path = store.stage_dir(normalized_stage) / "stage_state.json"
        store.append_event(
            scope_type="stage",
            scope_id=normalized_stage,
            status=runtime_status,
            stage=normalized_stage,
            error_class=str(error_info.get("error_class", "") or ""),
            error_code=str(error_info.get("error_code", "") or ""),
            local_path=str(stage_state_path),
            message=str(message or normalized_checkpoint),
        )
    except Exception as runtime_error:
        logger.warning(
            "Runtime stage checkpoint write failed: stage=%s checkpoint=%s error=%s",
            normalized_stage,
            normalized_checkpoint,
            runtime_error,
        )


class RuntimeStageSession:
    """统一管理阶段级 soft/hard heartbeat 与 runtime checkpoint。"""

    def __init__(
        self,
        *,
        store: Optional[RuntimeRecoveryStore],
        output_dir: str,
        task_id: str,
        stage: str,
        base_payload: Optional[Dict[str, Any]] = None,
        heartbeat_emitter: Optional[HeartbeatEmitter] = None,
        heartbeat_event_emitter: Optional[HeartbeatEventEmitter] = None,
    ) -> None:
        self._store = store
        self._output_dir = str(output_dir or "")
        self._task_id = str(task_id or "")
        self._stage = str(stage or "").strip() or "unknown_stage"
        self._base_payload = dict(base_payload or {})
        self._heartbeat_emitter = heartbeat_emitter
        self._heartbeat_event_emitter = heartbeat_event_emitter
        self._snapshot_lock = threading.Lock()
        self._snapshot: Dict[str, Any] = {
            "status": "running",
            "checkpoint": "unknown",
            "completed": 0,
            "pending": 0,
        }
        self._soft_stop_event: Optional[threading.Event] = None
        self._soft_thread: Optional[threading.Thread] = None

    def __enter__(self) -> "RuntimeStageSession":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.close()
        return False

    def snapshot(self) -> Dict[str, Any]:
        with self._snapshot_lock:
            return dict(self._snapshot)

    def update_base_payload(self, payload: Optional[Dict[str, Any]] = None) -> None:
        if not isinstance(payload, dict):
            return
        self._base_payload.update(payload)

    def bind_heartbeat_emitters(
        self,
        *,
        heartbeat_emitter: Optional[HeartbeatEmitter] = None,
        heartbeat_event_emitter: Optional[HeartbeatEventEmitter] = None,
    ) -> None:
        self._heartbeat_emitter = heartbeat_emitter
        self._heartbeat_event_emitter = heartbeat_event_emitter

    def _normalize_counts(self, completed: Any, pending: Any) -> Tuple[int, int]:
        try:
            safe_completed = max(0, int(completed))
        except Exception:
            safe_completed = 0
        try:
            safe_pending = max(0, int(pending))
        except Exception:
            safe_pending = 0
        return safe_completed, safe_pending

    def _update_snapshot(
        self,
        *,
        status: str,
        checkpoint: str,
        completed: Any,
        pending: Any,
    ) -> Tuple[int, int]:
        safe_completed, safe_pending = self._normalize_counts(completed, pending)
        with self._snapshot_lock:
            self._snapshot["status"] = str(status or "running").strip().lower() or "running"
            self._snapshot["checkpoint"] = str(checkpoint or "unknown").strip() or "unknown"
            self._snapshot["completed"] = safe_completed
            self._snapshot["pending"] = safe_pending
        return safe_completed, safe_pending

    def _build_state_payload(self, extra_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        payload = dict(self._base_payload)
        if isinstance(extra_payload, dict):
            payload.update(extra_payload)
        return payload

    def mark(
        self,
        *,
        status: str,
        checkpoint: str,
        completed: Any,
        pending: Any,
        extra_payload: Optional[Dict[str, Any]] = None,
        extra_watchdog: Optional[Dict[str, Any]] = None,
        error: Optional[Exception] = None,
        error_message: str = "",
        message: str = "",
        signal_type: str = "hard",
        runtime_status: Optional[str] = None,
        watchdog_status: Optional[str] = None,
        watchdog_stage: str = "",
        emit_watchdog: bool = True,
        persist_runtime: bool = True,
    ) -> None:
        normalized_checkpoint = str(checkpoint or "unknown").strip() or "unknown"
        effective_status = str(status or "running").strip().lower() or "running"
        safe_completed, safe_pending = self._update_snapshot(
            status=effective_status,
            checkpoint=normalized_checkpoint,
            completed=completed,
            pending=pending,
        )

        if emit_watchdog and self._heartbeat_emitter is not None:
            heartbeat_kwargs: Dict[str, Any] = {
                "status": str(watchdog_status or effective_status).strip().lower() or effective_status,
                "checkpoint": normalized_checkpoint,
                "completed": safe_completed,
                "pending": safe_pending,
                "signal_type": str(signal_type or "hard").strip().lower() or "hard",
            }
            if watchdog_stage:
                heartbeat_kwargs["stage"] = str(watchdog_stage or "").strip().lower()
            if isinstance(extra_watchdog, dict) and extra_watchdog:
                heartbeat_kwargs["extra"] = extra_watchdog
            self._heartbeat_emitter(**heartbeat_kwargs)

        if not persist_runtime or self._store is None:
            return

        record_runtime_stage_checkpoint(
            store=self._store,
            output_dir=self._output_dir,
            stage=self._stage,
            status=str(runtime_status or effective_status).strip().lower() or effective_status,
            checkpoint=normalized_checkpoint,
            completed=safe_completed,
            pending=safe_pending,
            error=error,
            error_message=error_message,
            extra_payload=self._build_state_payload(extra_payload),
            message=message,
        )

    def mark_from_event(
        self,
        event: Optional[Dict[str, Any]],
        *,
        extra_payload: Optional[Dict[str, Any]] = None,
        extra_watchdog: Optional[Dict[str, Any]] = None,
        default_status: str = "running",
        default_checkpoint: str = "unknown",
        default_pending: int = 0,
        emit_watchdog_event: bool = False,
        runtime_status: Optional[str] = None,
        watchdog_status: Optional[str] = None,
        emit_watchdog: bool = True,
        persist_runtime: bool = True,
    ) -> None:
        if not isinstance(event, dict):
            return

        checkpoint = str(
            event.get("checkpoint")
            or event.get("step_name")
            or event.get("event")
            or default_checkpoint
        ).strip() or default_checkpoint
        completed = event.get("completed", self.snapshot().get("completed", 0))
        pending = event.get("pending", self.snapshot().get("pending", default_pending))

        if emit_watchdog_event and self._heartbeat_event_emitter is not None:
            self._heartbeat_event_emitter(event)
            emit_watchdog = False

        self.mark(
            status=str(event.get("status") or default_status).strip().lower() or default_status,
            checkpoint=checkpoint,
            completed=completed,
            pending=pending,
            error_message=str(event.get("error", "") or ""),
            extra_payload=extra_payload,
            extra_watchdog=extra_watchdog,
            runtime_status=runtime_status,
            watchdog_status=watchdog_status,
            emit_watchdog=emit_watchdog,
            persist_runtime=persist_runtime,
        )

    def mark_failed(
        self,
        *,
        checkpoint: str,
        error: Optional[Exception] = None,
        error_message: str = "",
        extra_payload: Optional[Dict[str, Any]] = None,
        extra_watchdog: Optional[Dict[str, Any]] = None,
        message: str = "",
        runtime_status: Optional[str] = None,
        watchdog_status: Optional[str] = None,
        default_pending: int = 1,
    ) -> None:
        snapshot = self.snapshot()
        failed_completed = snapshot.get("completed", 0)
        failed_pending = max(default_pending, int(snapshot.get("pending", default_pending) or default_pending))
        self.mark(
            status="failed",
            checkpoint=checkpoint,
            completed=failed_completed,
            pending=failed_pending,
            error=error,
            error_message=error_message,
            extra_payload=extra_payload,
            extra_watchdog=extra_watchdog,
            message=message,
            runtime_status=runtime_status,
            watchdog_status=watchdog_status,
        )

    def start_soft_heartbeat_loop(
        self,
        *,
        interval_sec: float,
        thread_name: str,
        default_checkpoint: str,
        default_pending: int,
        watchdog_stage: str = "",
    ) -> None:
        if self._heartbeat_emitter is None:
            return
        if self._soft_thread is not None and self._soft_thread.is_alive():
            return

        stop_event = threading.Event()
        safe_interval = max(1.0, float(interval_sec or 1.0))

        def _run() -> None:
            while not stop_event.wait(safe_interval):
                snapshot = self.snapshot()
                try:
                    self._heartbeat_emitter(
                        status=str(snapshot.get("status") or "running").strip().lower() or "running",
                        checkpoint=str(snapshot.get("checkpoint") or default_checkpoint).strip() or default_checkpoint,
                        completed=int(snapshot.get("completed", 0) or 0),
                        pending=max(0, int(snapshot.get("pending", default_pending) or default_pending)),
                        signal_type="soft",
                        **({"stage": watchdog_stage} if watchdog_stage else {}),
                    )
                except Exception as heartbeat_error:
                    logger.warning(
                        "[%s] Runtime stage soft heartbeat emit failed: stage=%s error=%s",
                        self._task_id,
                        self._stage,
                        heartbeat_error,
                    )

        self._soft_stop_event = stop_event
        self._soft_thread = threading.Thread(
            target=_run,
            name=str(thread_name or f"runtime-stage-soft-{self._task_id}"),
            daemon=True,
        )
        self._soft_thread.start()

    def stop_soft_heartbeat_loop(self, *, timeout_sec: float = 2.0) -> None:
        if self._soft_stop_event is not None:
            self._soft_stop_event.set()
        if self._soft_thread is not None and self._soft_thread.is_alive():
            self._soft_thread.join(timeout=max(0.1, float(timeout_sec or 0.1)))
        self._soft_thread = None
        self._soft_stop_event = None

    def emit_snapshot_watchdog(
        self,
        *,
        signal_type: str = "hard",
        extra_watchdog: Optional[Dict[str, Any]] = None,
        watchdog_stage: str = "",
    ) -> None:
        if self._heartbeat_emitter is None:
            return
        snapshot = self.snapshot()
        heartbeat_kwargs: Dict[str, Any] = {
            "status": str(snapshot.get("status") or "running").strip().lower() or "running",
            "checkpoint": str(snapshot.get("checkpoint") or "unknown").strip() or "unknown",
            "completed": int(snapshot.get("completed", 0) or 0),
            "pending": int(snapshot.get("pending", 0) or 0),
            "signal_type": str(signal_type or "hard").strip().lower() or "hard",
        }
        if watchdog_stage:
            heartbeat_kwargs["stage"] = str(watchdog_stage or "").strip().lower()
        if isinstance(extra_watchdog, dict) and extra_watchdog:
            heartbeat_kwargs["extra"] = extra_watchdog
        self._heartbeat_emitter(**heartbeat_kwargs)

    def close(self, *, timeout_sec: float = 2.0) -> None:
        self.stop_soft_heartbeat_loop(timeout_sec=timeout_sec)
