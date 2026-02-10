"""Python gRPC 服务启动入口（薄入口）。"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from multiprocessing import freeze_support
from pathlib import Path


def _bootstrap_repo_root() -> None:
    """确保以脚本方式启动时，仓库根目录位于 `sys.path` 首位。"""
    repo_root = str(Path(__file__).resolve().parents[2])
    if repo_root in sys.path:
        sys.path.remove(repo_root)
    sys.path.insert(0, repo_root)


_bootstrap_repo_root()

from services.python_grpc.src.common.logging import configure_pipeline_logging
from services.python_grpc.src.server.dependency_check import run_dependency_check
from services.python_grpc.src.server import serve
from services.python_grpc.src.server.startup_flags import parse_startup_flags


def main() -> None:
    freeze_support()
    startup_flags = parse_startup_flags(default_debug_imports=False)
    if startup_flags.debug_imports:
        os.environ["GRPC_SERVER_DEBUG_IMPORTS"] = "1"

    if startup_flags.check_deps:
        raise SystemExit(run_dependency_check(debug_imports=startup_flags.debug_imports))

    configure_pipeline_logging(
        level=logging.INFO,
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        force=True,
    )
    asyncio.run(serve())


if __name__ == "__main__":
    main()
