"""gRPC 服务入口编排层。

职责边界：
- 仅负责暴露对外入口函数，不承载执行细节。
- 统一 CLI/脚本调用落点。

主要功能：
- 暴露 `main` 作为标准入口。
- 复用 `startup_runner.run_server` 执行真实启动。
"""

from __future__ import annotations

from .startup_runner import run_server, serve


def main() -> None:
    """gRPC 服务入口函数。"""
    run_server()


__all__ = ["main", "serve", "run_server"]

