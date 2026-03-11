"""进程池创建工具。"""

from __future__ import annotations

import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
from typing import Any, Callable, Iterable


def create_spawn_process_pool(
    max_workers: int,
    initializer: Callable[..., Any] | None = None,
    initargs: Iterable[Any] = (),
) -> ProcessPoolExecutor:
    """统一创建 spawn 模式进程池。"""
    ctx = mp.get_context("spawn")
    return ProcessPoolExecutor(
        max_workers=max_workers,
        mp_context=ctx,
        initializer=initializer,
        initargs=tuple(initargs or ()),
    )


__all__ = ["create_spawn_process_pool"]
