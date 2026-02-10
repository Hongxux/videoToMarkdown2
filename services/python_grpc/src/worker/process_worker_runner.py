"""兼容模块：转发到 `worker_process_runtime`。

职责边界：
- 保留历史导入路径 `services.python_grpc.src.worker.process_worker_runner`。
- 不再承载真实实现。

主要功能：
- 转发 `run_worker_process`。
"""

from .runtime import run_worker_process

__all__ = ["run_worker_process"]
