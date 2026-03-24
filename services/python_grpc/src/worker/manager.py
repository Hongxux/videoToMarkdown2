"""兼容模块：保留历史 `worker.manager` 导入路径。

职责边界：
- 只做入口转发，不承载编排与执行实现。

主要功能：
- 转发 `main` 到 `entrypoint.main`。
"""

from .entrypoint import main

__all__ = ["main"]

