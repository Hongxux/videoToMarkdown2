"""兼容模块：保留历史 `server.main` 导入路径。

职责边界：
- 只做入口转发，不承载执行细节。

主要功能：
- 转发 `main` 和 `serve` 到 `entrypoint`。
"""

from .entrypoint import main, serve

__all__ = ["serve", "main"]

