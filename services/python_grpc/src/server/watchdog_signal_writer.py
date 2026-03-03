import json
import logging
import os
import threading
import time
from collections import deque
from typing import Any, Deque, Dict, List, Optional


def _to_non_negative_int(value: Any, fallback: int = 0) -> int:
    try:
        return max(0, int(value))
    except Exception:
        return max(0, int(fallback))


logger = logging.getLogger(__name__)

_HEARTBEAT_WRITE_RETRY_COUNT = max(
    1,
    _to_non_negative_int(os.getenv("WATCHDOG_HEARTBEAT_WRITE_RETRY_COUNT", 6), 6),
)
_HEARTBEAT_WRITE_RETRY_MS = max(
    5,
    _to_non_negative_int(os.getenv("WATCHDOG_HEARTBEAT_WRITE_RETRY_MS", 25), 25),
)


def _remove_file_quietly(path: str) -> None:
    try:
        os.remove(path)
    except FileNotFoundError:
        return
    except Exception:
        return


def persist_watchdog_payload(path: str, payload: Dict[str, Any]) -> Optional[str]:
    """
    以最佳努力方式持久化 watchdog 心跳文件。
    优先原子替换，失败后重试并最终降级为直接覆盖写入。
    """
    target_path = os.path.abspath(str(path or "").strip())
    if not target_path:
        return "target path is empty"
    parent_dir = os.path.dirname(target_path)
    if parent_dir:
        os.makedirs(parent_dir, exist_ok=True)

    last_error: Optional[Exception] = None
    for attempt in range(_HEARTBEAT_WRITE_RETRY_COUNT):
        tmp_path = (
            f"{target_path}.tmp.{os.getpid()}."
            f"{threading.get_ident()}."
            f"{time.time_ns()}"
        )
        try:
            with open(tmp_path, "w", encoding="utf-8") as file:
                json.dump(payload, file, ensure_ascii=False)
            os.replace(tmp_path, target_path)
            return None
        except Exception as error:
            last_error = error
            _remove_file_quietly(tmp_path)
            if attempt + 1 < _HEARTBEAT_WRITE_RETRY_COUNT:
                delay_sec = (_HEARTBEAT_WRITE_RETRY_MS * (attempt + 1)) / 1000.0
                time.sleep(delay_sec)

    try:
        with open(target_path, "w", encoding="utf-8") as file:
            json.dump(payload, file, ensure_ascii=False)
        return None
    except Exception as fallback_error:
        if last_error is None:
            return str(fallback_error)
        return f"atomic_write_error={last_error}; fallback_write_error={fallback_error}"


class _TaskSignalBuffer:
    def __init__(self, max_events: int) -> None:
        self.events: Deque[Dict[str, Any]] = deque(maxlen=max_events)
        self.next_stream_seq: int = 0


class TaskWatchdogSignalHub:
    """
    任务级看门狗信号总线（进程内）。
    """

    def __init__(self, max_events_per_task: int = 2048) -> None:
        self._max_events_per_task = max(128, int(max_events_per_task))
        self._lock = threading.Lock()
        self._buffers: Dict[str, _TaskSignalBuffer] = {}

    def publish(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(payload, dict):
            return {}
        task_id = str(payload.get("task_id") or "").strip()
        if not task_id:
            return dict(payload)

        stage = str(payload.get("stage") or "unknown").strip().lower() or "unknown"
        status = str(payload.get("status") or "running").strip().lower() or "running"
        checkpoint = str(payload.get("checkpoint") or "unknown").strip() or "unknown"
        signal_type = str(payload.get("signal_type") or "hard").strip().lower() or "hard"
        if signal_type not in {"hard", "soft"}:
            signal_type = "hard"

        event: Dict[str, Any] = dict(payload)
        event["task_id"] = task_id
        event["stage"] = stage
        event["status"] = status
        event["checkpoint"] = checkpoint
        event["signal_type"] = signal_type
        event["completed"] = _to_non_negative_int(event.get("completed"), 0)
        event["pending"] = _to_non_negative_int(event.get("pending"), 0)
        event["updated_at_ms"] = _to_non_negative_int(event.get("updated_at_ms"), int(time.time() * 1000))

        with self._lock:
            buffer = self._buffers.get(task_id)
            if buffer is None:
                buffer = _TaskSignalBuffer(self._max_events_per_task)
                self._buffers[task_id] = buffer
            buffer.next_stream_seq += 1
            event["stream_seq"] = buffer.next_stream_seq
            buffer.events.append(dict(event))
        return event

    def read_since(
        self,
        *,
        task_id: str,
        from_stream_seq: int = 0,
        stage: Optional[str] = None,
        limit: int = 256,
    ) -> List[Dict[str, Any]]:
        safe_task_id = str(task_id or "").strip()
        if not safe_task_id:
            return []
        safe_from = _to_non_negative_int(from_stream_seq, 0)
        safe_stage = str(stage or "").strip().lower()
        safe_limit = max(1, int(limit or 1))

        with self._lock:
            buffer = self._buffers.get(safe_task_id)
            if buffer is None:
                return []
            snapshot = list(buffer.events)

        events: List[Dict[str, Any]] = []
        for event in snapshot:
            stream_seq = _to_non_negative_int(event.get("stream_seq"), 0)
            if stream_seq <= safe_from:
                continue
            if safe_stage and str(event.get("stage") or "").strip().lower() != safe_stage:
                continue
            events.append(dict(event))
            if len(events) >= safe_limit:
                break
        return events


_WATCHDOG_SIGNAL_HUB = TaskWatchdogSignalHub()


def publish_watchdog_signal(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _WATCHDOG_SIGNAL_HUB.publish(payload)


def read_watchdog_signals(
    *,
    task_id: str,
    from_stream_seq: int = 0,
    stage: Optional[str] = None,
    limit: int = 256,
) -> List[Dict[str, Any]]:
    return _WATCHDOG_SIGNAL_HUB.read_since(
        task_id=task_id,
        from_stream_seq=from_stream_seq,
        stage=stage,
        limit=limit,
    )


class TaskWatchdogSignalWriter:
    """
    写入任务级看门狗信号文件（供 Java 文件轮询兜底消费），同时发布到 gRPC 流事件总线。
    """

    HEARTBEAT_FILE = "task_watchdog_heartbeat.json"

    def __init__(
        self,
        *,
        task_id: str,
        output_dir: str,
        stage: str,
        total_steps: int = 1,
    ) -> None:
        self._task_id = str(task_id or "").strip()
        self._output_dir = str(output_dir or "").strip()
        self._stage = str(stage or "unknown").strip().lower() or "unknown"
        self._total_steps = max(1, int(total_steps or 1))
        self._seq = 0
        self._lock = threading.Lock()
        self._path = os.path.join(self._output_dir, "intermediates", self.HEARTBEAT_FILE)
        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        self._last_persist_error_message = ""
        self._last_persist_error_at_ms = 0

    @property
    def path(self) -> str:
        return self._path

    def _log_persist_error(self, *, checkpoint: str, error_message: str) -> None:
        now_ms = int(time.time() * 1000)
        if (
            error_message == self._last_persist_error_message
            and now_ms - self._last_persist_error_at_ms < 10_000
        ):
            return
        self._last_persist_error_message = error_message
        self._last_persist_error_at_ms = now_ms
        logger.warning(
            "[%s] Task watchdog heartbeat persist degraded: stage=%s checkpoint=%s error=%s",
            self._task_id,
            self._stage,
            checkpoint,
            error_message,
        )

    def emit(
        self,
        *,
        status: str,
        checkpoint: str,
        completed: int,
        pending: Optional[int] = None,
        signal_type: str = "hard",
        stage: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        safe_stage = str(stage or self._stage).strip().lower() or self._stage
        safe_status = str(status or "running").strip().lower() or "running"
        safe_checkpoint = str(checkpoint or "unknown").strip() or "unknown"
        safe_completed = max(0, int(completed))
        if pending is None:
            safe_pending = max(0, self._total_steps - safe_completed)
        else:
            safe_pending = max(0, int(pending))
        safe_signal_type = str(signal_type or "hard").strip().lower()
        if safe_signal_type not in {"hard", "soft"}:
            safe_signal_type = "hard"

        payload: Dict[str, Any] = {
            "schema": "task_watchdog.v1",
            "source": "python_task_heartbeat",
            "task_id": self._task_id,
            "stage": safe_stage,
            "status": safe_status,
            "checkpoint": safe_checkpoint,
            "completed": safe_completed,
            "pending": safe_pending,
            "signal_type": safe_signal_type,
            "updated_at_ms": int(time.time() * 1000),
        }
        if isinstance(extra, dict):
            for key, value in extra.items():
                if key in payload:
                    continue
                if isinstance(value, (str, int, float, bool)):
                    payload[key] = value

        with self._lock:
            self._seq += 1
            payload["seq"] = self._seq
            published_payload = publish_watchdog_signal(payload)
            if isinstance(published_payload, dict):
                payload["stream_seq"] = _to_non_negative_int(published_payload.get("stream_seq"), 0)
            persist_error = persist_watchdog_payload(self._path, payload)
            if persist_error:
                self._log_persist_error(
                    checkpoint=safe_checkpoint,
                    error_message=persist_error,
                )
