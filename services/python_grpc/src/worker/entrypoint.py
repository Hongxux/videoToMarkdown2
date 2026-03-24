"""Worker 启动入口编排层。

职责边界：
- 负责参数解析与编排器启动。
- 不承载 worker 子进程执行细节。

主要功能：
- 解析 `--workers` 参数。
- 创建并启动 `WorkerOrchestrator`。
"""

from __future__ import annotations

import argparse

from .orchestrator import WorkerOrchestrator


def build_arg_parser() -> argparse.ArgumentParser:
    """构建命令行参数解析器。"""
    parser = argparse.ArgumentParser(description="VideoToMarkdown Worker Orchestrator")
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Worker 数量（默认 CPU核数-1，且上限4）",
    )
    return parser


def main() -> None:
    """Worker 入口函数。"""
    parser = build_arg_parser()
    args = parser.parse_args()
    orchestrator = WorkerOrchestrator(num_workers=args.workers)
    orchestrator.start()


__all__ = ["main", "build_arg_parser"]
