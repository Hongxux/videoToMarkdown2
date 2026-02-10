"""gRPC 服务启动参数工具。"""

from __future__ import annotations

import argparse
from dataclasses import dataclass


@dataclass(frozen=True)
class StartupFlags:
    """启动参数聚合对象。"""

    check_deps: bool
    debug_imports: bool


def build_startup_arg_parser() -> argparse.ArgumentParser:
    """构建启动参数解析器。"""
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument(
        "--check-deps",
        action="store_true",
        help="仅执行依赖预检并退出（用于排查启动卡住或导入失败）",
    )
    parser.add_argument(
        "--debug-imports",
        action="store_true",
        help="输出启动阶段关键 import 日志（也可用 env: GRPC_SERVER_DEBUG_IMPORTS=1）",
    )
    return parser


def parse_startup_flags(default_debug_imports: bool) -> StartupFlags:
    """解析启动参数并合并默认调试开关。"""
    parser = build_startup_arg_parser()
    args, _unknown = parser.parse_known_args()
    return StartupFlags(
        check_deps=bool(args.check_deps),
        debug_imports=bool(default_debug_imports or args.debug_imports),
    )


__all__ = ["StartupFlags", "build_startup_arg_parser", "parse_startup_flags"]

