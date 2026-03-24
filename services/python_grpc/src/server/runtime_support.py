"""兼容模块：转发到 `runtime_env`。

职责边界：
- 保留历史导入路径 `services.python_grpc.src.server.runtime_support`。
- 不再承载真实实现。

主要功能：
- 统一从 `runtime_env` 转发运行时工具函数。
"""

from .runtime_env import *  # noqa: F401,F403

