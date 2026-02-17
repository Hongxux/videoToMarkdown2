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
    tmp_path = f"{target_path}.tmp.{os.getpid()}.{time.time_ns()}"
    with open(tmp_path, "w", encoding="utf-8") as output_stream:
        json.dump(payload, output_stream, ensure_ascii=ensure_ascii, indent=indent, default=str)
    os.replace(tmp_path, target_path)


def _write_text_atomic(path: str, content: str) -> None:
    """原子写入文本文件。"""
    target_path = os.path.abspath(path)
    parent_dir = os.path.dirname(target_path)
    if parent_dir:
        os.makedirs(parent_dir, exist_ok=True)
    tmp_path = f"{target_path}.tmp.{os.getpid()}.{time.time_ns()}"
    with open(tmp_path, "w", encoding="utf-8") as output_stream:
        output_stream.write(content)
    os.replace(tmp_path, target_path)


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
                ack_queue.put(flush_id)
            break

        if task_type == "flush":
            flush_id = str(task.get("flush_id", "")).strip()
            if flush_id:
                ack_queue.put(flush_id)
            continue

        if task_type != "json":
            if task_type != "text":
                continue
            try:
                _write_text_atomic(
                    path=str(task.get("path", "")),
                    content=str(task.get("content", "")),
                )
            except Exception as error:
                logger.warning("Async text write failed: %s", error)
            continue

        try:
            _write_json_atomic(
                path=str(task.get("path", "")),
                payload=task.get("payload"),
                ensure_ascii=bool(task.get("ensure_ascii", False)),
                indent=int(task.get("indent", 2)),
            )
        except Exception as error:
            logger.warning("Async JSON write failed: %s", error)


class AsyncJsonDiskWriter:
    """独立进程 JSON 写盘器。"""

    def __init__(self) -> None:
        self._ctx = mp.get_context("spawn")
        self._task_queue: "mp.Queue" = self._ctx.Queue(maxsize=2048)
        self._ack_queue: "mp.Queue" = self._ctx.Queue(maxsize=256)
        self._process = self._ctx.Process(
            target=_json_writer_worker,
            args=(self._task_queue, self._ack_queue),
            daemon=True,
        )
        self._process.start()
        self._lock = threading.Lock()
        logger.info(
            "Async disk writer process started: pid=%s",
            self._process.pid,
        )

    def enqueue_json(self, path: str, payload: Any, *, ensure_ascii: bool = False, indent: int = 2) -> None:
        """提交 JSON 写盘任务；队列拥堵时回退同步写入，避免数据丢失。"""
        task = {
            "type": "json",
            "path": os.path.abspath(path),
            "payload": payload,
            "ensure_ascii": ensure_ascii,
            "indent": indent,
        }
        try:
            self._task_queue.put_nowait(task)
        except queue.Full:
            logger.warning("Async JSON queue full, fallback to sync write: %s", path)
            _write_json_atomic(path=path, payload=payload, ensure_ascii=ensure_ascii, indent=indent)

    def enqueue_text(self, path: str, content: str) -> None:
        """提交文本写盘任务；队列拥堵时回退同步写入。"""
        task = {
            "type": "text",
            "path": os.path.abspath(path),
            "content": str(content),
        }
        try:
            self._task_queue.put_nowait(task)
        except queue.Full:
            logger.warning("Async text queue full, fallback to sync write: %s", path)
            _write_text_atomic(path=path, content=str(content))

    def flush(self, timeout_sec: float = 30.0) -> bool:
        """等待当前队列中的写盘任务完成。"""
        flush_id = uuid.uuid4().hex
        with self._lock:
            self._task_queue.put({"type": "flush", "flush_id": flush_id})
            deadline = time.time() + max(0.1, float(timeout_sec))
            while time.time() < deadline:
                remaining = max(0.01, deadline - time.time())
                try:
                    ack = self._ack_queue.get(timeout=min(0.5, remaining))
                except queue.Empty:
                    continue
                if ack == flush_id:
                    return True
            return False

    def stop(self, timeout_sec: float = 5.0) -> None:
        """停止写盘进程。"""
        if not self._process.is_alive():
            return
        flush_id = uuid.uuid4().hex
        self._task_queue.put({"type": "stop", "flush_id": flush_id})
        try:
            self._ack_queue.get(timeout=max(0.2, float(timeout_sec)))
        except queue.Empty:
            pass
        self._process.join(timeout=max(0.2, float(timeout_sec)))


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


def enqueue_json_write(path: str, payload: Any, *, ensure_ascii: bool = False, indent: int = 2) -> None:
    """提交异步 JSON 写盘任务。"""
    _log_async_write_start_once("json", path)
    get_async_json_writer().enqueue_json(path, payload, ensure_ascii=ensure_ascii, indent=indent)


def enqueue_text_write(path: str, content: str) -> None:
    """提交异步文本写盘任务。"""
    _log_async_write_start_once("text", path)
    get_async_json_writer().enqueue_text(path, content)


def flush_async_json_writes(timeout_sec: float = 30.0) -> bool:
    """阻塞等待队列落盘完成。"""
    with _WRITER_LOCK:
        if _WRITER_SINGLETON is None:
            return True
    return get_async_json_writer().flush(timeout_sec=timeout_sec)


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
