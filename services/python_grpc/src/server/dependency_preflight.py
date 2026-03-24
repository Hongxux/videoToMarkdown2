"""兼容模块：转发到 `dependency_check`。

职责边界：
- 保留历史导入路径 `services.python_grpc.src.server.dependency_preflight`。
- 不再承载真实实现。

主要功能：
- 转发 `run_dependency_preflight` 与 `run_dependency_check`。
"""

from .dependency_check import run_dependency_check, run_dependency_preflight

__all__ = ["run_dependency_preflight", "run_dependency_check"]

