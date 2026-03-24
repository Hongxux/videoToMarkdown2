"""
异步写盘工具：通过独立进程执行 JSON 落盘，降低主流程阻塞。
"""

from __future__ import annotations

import atexit
import json
import logging
import multiprocessing as mp
import os
import queue
import threading
import time
import uuid
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def _build_tmp_path(target_path: str) -> str:
    return f"{target_path}.t{time.time_ns():x}"


def _fsync_parent_dir(path: str) -> None:
    """尽力同步父目录元数据，降低断电后原子替换丢失风险。"""
    parent_dir = os.path.dirname(os.path.abspath(path))
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


def _write_json_atomic(
    path: str,
    payload: Any,
    *,
    ensure_ascii: bool = False,
    indent: int = 2,
) -> None:
    """原子写入 JSON，避免并发写入时出现半写文件。"""
    target_path = os.path.abspath(path)
    parent_dir = os.path.dirname(target_path)
    if parent_dir:
        os.makedirs(parent_dir, exist_ok=True)
    tmp_path = _build_tmp_path(target_path)
    with open(tmp_path, "w", encoding="utf-8") as output_stream:
        json.dump(payload, output_stream, ensure_ascii=ensure_ascii, indent=indent, default=str)
        output_stream.flush()
        os.fsync(output_stream.fileno())
    os.replace(tmp_path, target_path)
    _fsync_parent_dir(target_path)


def _write_text_atomic(path: str, content: str) -> None:
    """原子写入文本文件。"""
    target_path = os.path.abspath(path)
    parent_dir = os.path.dirname(target_path)
    if parent_dir:
        os.makedirs(parent_dir, exist_ok=True)
    tmp_path = _build_tmp_path(target_path)
    with open(tmp_path, "w", encoding="utf-8") as output_stream:
        output_stream.write(content)
        output_stream.flush()
        os.fsync(output_stream.fileno())
    os.replace(tmp_path, target_path)
    _fsync_parent_dir(target_path)


def _json_writer_worker(task_queue: "mp.Queue", ack_queue: "mp.Queue") -> None:
    """写盘子进程主循环。"""
    while True:
        task = task_queue.get()
        if not isinstance(task, dict):
            continue

        task_type = str(task.get("type", "")).strip()
        if task_type == "stop":
            flush_id = str(task.get("flush_id", "")).strip()
            if flush_id:
                ack_queue.put({"type": "stop", "flush_id": flush_id})
            break

        if task_type not in {"json", "text"}:
            continue

        write_id = str(task.get("write_id", "")).strip()
        scope_key = str(task.get("scope_key", "")).strip()
        try:
            if task_type == "text":
                _write_text_atomic(
                    path=str(task.get("path", "")),
                    content=str(task.get("content", "")),
                )
            else:
                _write_json_atomic(
                    path=str(task.get("path", "")),
                    payload=task.get("payload"),
                    ensure_ascii=bool(task.get("ensure_ascii", False)),
                    indent=int(task.get("indent", 2)),
                )
        except Exception as error:
            logger.warning("Async %s write failed: %s", task_type, error)
        finally:
            if write_id:
                ack_queue.put(
                    {
                        "type": "done",
                        "write_id": write_id,
                        "scope_key": scope_key,
                    }
                )


class AsyncJsonDiskWriter:
    """独立进程 JSON 写盘器。"""

    def __init__(self) -> None:
        def _read_env_int(name: str, default: int, min_value: int) -> int:
            raw = str(os.getenv(name, str(default)) or "").strip()
            try:
                value = int(raw)
            except Exception:
                value = default
            return max(min_value, value)

        task_queue_size = _read_env_int("ASYNC_DISK_WRITER_QUEUE_SIZE", 2048, 128)
        ack_queue_default = max(256, task_queue_size // 4)
        ack_queue_size = _read_env_int("ASYNC_DISK_WRITER_ACK_QUEUE_SIZE", ack_queue_default, 64)

        self._ctx = mp.get_context("spawn")
        self._task_queue: "mp.Queue" = self._ctx.Queue(maxsize=task_queue_size)
        self._ack_queue: "mp.Queue" = self._ctx.Queue(maxsize=ack_queue_size)
        self._process = self._ctx.Process(
            target=_json_writer_worker,
            args=(self._task_queue, self._ack_queue),
            daemon=True,
        )
        self._process.start()
        self._lock = threading.Lock()
        self._pending_condition = threading.Condition(self._lock)
        self._pending_total = 0
        self._pending_by_scope: Dict[str, int] = {}
        self._ack_worker_running = True
        self._ack_worker = threading.Thread(
            target=self._consume_acks,
            name="AsyncDiskWriterAck",
            daemon=True,
        )
        self._ack_worker.start()
        logger.info(
            "Async disk writer process started: pid=%s, task_queue=%s, ack_queue=%s",
            self._process.pid,
            task_queue_size,
            ack_queue_size,
        )

    def _normalize_scope_key(self, path: str, scope_key: str = "") -> str:
        if str(scope_key or "").strip():
            raw_scope = str(scope_key).strip()
            try:
                return os.path.abspath(raw_scope)
            except Exception:
                return raw_scope
        try:
            parent = os.path.dirname(os.path.abspath(path))
            return parent or "__global__"
        except Exception:
            return "__global__"

    def _register_pending(self, scope_key: str) -> None:
        with self._pending_condition:
            self._pending_total += 1
            self._pending_by_scope[scope_key] = self._pending_by_scope.get(scope_key, 0) + 1

    def _mark_done(self, scope_key: str) -> None:
        with self._pending_condition:
            if self._pending_total > 0:
                self._pending_total -= 1
            if scope_key in self._pending_by_scope:
                remaining = self._pending_by_scope.get(scope_key, 0) - 1
                if remaining > 0:
                    self._pending_by_scope[scope_key] = remaining
                else:
                    self._pending_by_scope.pop(scope_key, None)
            self._pending_condition.notify_all()

    def _consume_acks(self) -> None:
        while self._ack_worker_running:
            try:
                ack = self._ack_queue.get(timeout=0.5)
            except queue.Empty:
                continue
            if not isinstance(ack, dict):
                continue
            ack_type = str(ack.get("type", "")).strip()
            if ack_type == "done":
                scope_key = str(ack.get("scope_key", "")).strip() or "__global__"
                self._mark_done(scope_key)

    def enqueue_json(
        self,
        path: str,
        payload: Any,
        *,
        ensure_ascii: bool = False,
        indent: int = 2,
        scope_key: str = "",
    ) -> None:
        """提交 JSON 写盘任务；队列拥堵时回退同步写入，避免数据丢失。"""
        normalized_scope_key = self._normalize_scope_key(path, scope_key)
        write_id = uuid.uuid4().hex
        task = {
            "type": "json",
            "path": os.path.abspath(path),
            "payload": payload,
            "ensure_ascii": ensure_ascii,
            "indent": indent,
            "write_id": write_id,
            "scope_key": normalized_scope_key,
        }
        self._register_pending(normalized_scope_key)
        try:
            self._task_queue.put_nowait(task)
        except queue.Full:
            logger.warning("Async JSON queue full, fallback to sync write: %s", path)
            try:
                _write_json_atomic(path=path, payload=payload, ensure_ascii=ensure_ascii, indent=indent)
            finally:
                self._mark_done(normalized_scope_key)

    def enqueue_text(self, path: str, content: str, *, scope_key: str = "") -> None:
        """提交文本写盘任务；队列拥堵时回退同步写入。"""
        normalized_scope_key = self._normalize_scope_key(path, scope_key)
        write_id = uuid.uuid4().hex
        task = {
            "type": "text",
            "path": os.path.abspath(path),
            "content": str(content),
            "write_id": write_id,
            "scope_key": normalized_scope_key,
        }
        self._register_pending(normalized_scope_key)
        try:
            self._task_queue.put_nowait(task)
        except queue.Full:
            logger.warning("Async text queue full, fallback to sync write: %s", path)
            try:
                _write_text_atomic(path=path, content=str(content))
            finally:
                self._mark_done(normalized_scope_key)

    def flush(self, timeout_sec: float = 30.0, scope_key: str = "") -> bool:
        """等待当前队列中的写盘任务完成（支持按作用域等待）。"""
        normalized_scope_key = str(scope_key or "").strip()
        if normalized_scope_key:
            normalized_scope_key = self._normalize_scope_key(path="", scope_key=normalized_scope_key)
        deadline = time.time() + max(0.1, float(timeout_sec))
        with self._pending_condition:
            while True:
                if normalized_scope_key:
                    pending = self._pending_by_scope.get(normalized_scope_key, 0)
                else:
                    pending = self._pending_total
                if pending <= 0:
                    return True
                remaining = deadline - time.time()
                if remaining <= 0:
                    return False
                self._pending_condition.wait(timeout=min(0.5, remaining))

    def stop(self, timeout_sec: float = 5.0) -> None:
        """停止写盘进程。"""
        if not self._process.is_alive():
            self._ack_worker_running = False
            if self._ack_worker.is_alive():
                self._ack_worker.join(timeout=max(0.2, float(timeout_sec)))
            return
        flush_id = uuid.uuid4().hex
        self._task_queue.put({"type": "stop", "flush_id": flush_id})
        self._process.join(timeout=max(0.2, float(timeout_sec)))
        self._ack_worker_running = False
        if self._ack_worker.is_alive():
            self._ack_worker.join(timeout=max(0.2, float(timeout_sec)))


_WRITER_LOCK = threading.Lock()
_WRITER_SINGLETON: Optional[AsyncJsonDiskWriter] = None
_FIRST_WRITE_LOG_LOCK = threading.Lock()
_FIRST_WRITE_LOGGED = False


def _log_async_write_start_once(task_type: str, path: str) -> None:
    """首次写盘任务触发时输出提醒日志，避免日志噪声。"""
    global _FIRST_WRITE_LOGGED
    if _FIRST_WRITE_LOGGED:
        return
    with _FIRST_WRITE_LOG_LOCK:
        if _FIRST_WRITE_LOGGED:
            return
        logger.info(
            "Async disk write started: first_task_type=%s, path=%s",
            str(task_type or "").strip() or "unknown",
            os.path.abspath(path),
        )
        _FIRST_WRITE_LOGGED = True


def get_async_json_writer() -> AsyncJsonDiskWriter:
    """获取全局异步 JSON 写盘器。"""
    global _WRITER_SINGLETON
    with _WRITER_LOCK:
        if _WRITER_SINGLETON is None:
            _WRITER_SINGLETON = AsyncJsonDiskWriter()
    return _WRITER_SINGLETON


def enqueue_json_write(
    path: str,
    payload: Any,
    *,
    ensure_ascii: bool = False,
    indent: int = 2,
    scope_key: str = "",
) -> None:
    """提交异步 JSON 写盘任务。"""
    _log_async_write_start_once("json", path)
    get_async_json_writer().enqueue_json(
        path,
        payload,
        ensure_ascii=ensure_ascii,
        indent=indent,
        scope_key=scope_key,
    )


def enqueue_text_write(path: str, content: str, *, scope_key: str = "") -> None:
    """提交异步文本写盘任务。"""
    _log_async_write_start_once("text", path)
    get_async_json_writer().enqueue_text(path, content, scope_key=scope_key)


def flush_async_json_writes(timeout_sec: float = 30.0, scope_key: str = "") -> bool:
    """阻塞等待队列落盘完成（支持按作用域等待）。"""
    with _WRITER_LOCK:
        if _WRITER_SINGLETON is None:
            return True
    return get_async_json_writer().flush(timeout_sec=timeout_sec, scope_key=scope_key)


def stop_async_json_writer(timeout_sec: float = 5.0) -> None:
    """停止全局异步写盘进程。"""
    global _WRITER_SINGLETON
    global _FIRST_WRITE_LOGGED
    with _WRITER_LOCK:
        writer = _WRITER_SINGLETON
        _WRITER_SINGLETON = None
    with _FIRST_WRITE_LOG_LOCK:
        _FIRST_WRITE_LOGGED = False
    if writer is not None:
        writer.stop(timeout_sec=timeout_sec)


atexit.register(stop_async_json_writer)
