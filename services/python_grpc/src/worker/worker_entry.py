"""兼容模块：转发到 `entrypoint`。

职责边界：
- 保留历史导入路径 `services.python_grpc.src.worker.worker_entry`。
- 不再承载真实实现。

主要功能：
- 转发 `main` 与 `build_arg_parser`。
"""

from .entrypoint import build_arg_parser, main

__all__ = ["main", "build_arg_parser"]

