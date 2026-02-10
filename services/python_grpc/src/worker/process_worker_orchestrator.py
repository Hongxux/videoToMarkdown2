"""兼容模块：转发到 `worker_process_orchestrator`。

职责边界：
- 保留历史导入路径 `services.python_grpc.src.worker.process_worker_orchestrator`。
- 不再承载真实实现。

主要功能：
- 转发 `WorkerOrchestrator` 与 `resolve_worker_count`。
"""

from .orchestrator import WorkerOrchestrator, resolve_worker_count

__all__ = ["WorkerOrchestrator", "resolve_worker_count"]
