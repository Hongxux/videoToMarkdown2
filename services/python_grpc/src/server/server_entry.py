"""兼容模块：转发到 `entrypoint`。

职责边界：
- 保留历史导入路径 `services.python_grpc.src.server.server_entry`。
- 不再承载真实实现。

主要功能：
- 转发 `main`、`serve`、`run_server`。
"""

from .entrypoint import main, run_server, serve

__all__ = ["main", "serve", "run_server"]

