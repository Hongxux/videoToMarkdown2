"""兼容模块：转发到 `startup_runner`。

职责边界：
- 保留历史导入路径 `services.python_grpc.src.server.runtime_bootstrap`。
- 不再承载真实实现。

主要功能：
- 转发 `run_server`、`configure_logging`、`serve`。
"""

from .startup_runner import configure_logging, run_server, serve

__all__ = ["run_server", "configure_logging", "serve"]

