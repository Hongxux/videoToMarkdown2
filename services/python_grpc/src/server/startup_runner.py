"""gRPC 服务进程启动编排层。
职责边界：只负责进程级启动准备，不承载业务处理逻辑。
主要功能：暴露 `run_server()` 统一启动入口，并转发到 `server.service`。
"""

from __future__ import annotations

import asyncio
import logging
from multiprocessing import freeze_support

from services.python_grpc.src.common.logging import configure_pipeline_logging


async def serve(host: str = "0.0.0.0", port: int = 50051):
    """兼容导出：延迟导入并调用当前服务实现。"""
    from .service import serve as current_serve

    return await current_serve(host=host, port=port)


def configure_logging() -> None:
    """初始化服务日志配置。"""
    configure_pipeline_logging(
        level=logging.INFO,
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        force=True,
    )


def run_server() -> None:
    """启动 gRPC 服务。"""
    freeze_support()
    configure_logging()
    asyncio.run(serve())


__all__ = ["run_server", "configure_logging", "serve"]
