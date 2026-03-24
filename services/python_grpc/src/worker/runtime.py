"""Worker 子进程执行层。

职责边界：
- 负责单个 worker 进程内的运行与异常兜底。
- 不负责多进程编排、信号治理和生命周期管理。

主要功能：
- `run_worker_process`：在子进程中创建并启动 `VideoWorker`。
"""

from __future__ import annotations

import os
import traceback


def run_worker_process(worker_id: int) -> None:
    """执行单个 worker 进程任务。"""
    print(f"Worker #{worker_id} 启动 (PID: {os.getpid()})")
    try:
        from rabbitmq_worker import VideoWorker

        worker = VideoWorker()
        worker.start()
    except Exception as exc:
        print(f"Worker #{worker_id} 致命错误: {exc}")
        traceback.print_exc()


__all__ = ["run_worker_process"]

