"""Python gRPC 服务端模块导出。

职责边界：
- 统一暴露服务入口与启动函数。
- 不包含业务逻辑实现。

主要功能：
- 导出 `main`、`serve`、`run_server`。
"""

from .entrypoint import main, serve
from .startup_runner import run_server

__all__ = ["serve", "main", "run_server"]

