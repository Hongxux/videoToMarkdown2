"""
模块说明：Module2 缓存命中率统计统一收敛器。
执行逻辑：
1) 记录命中/未命中计数；
2) 按任务与阶段生成快照；
3) 支持按任务重置。
实现方式：线程锁 + 内存计数器。
核心价值：统一缓存命中率统计口径，便于调优与回放。
"""

from __future__ import annotations

import os
import threading
import time
from typing import Dict, Any, Optional


def _env_truthy(name: str, default: str = "1") -> bool:
    value = os.getenv(name, default).strip().lower()
    return value in {"1", "true", "yes", "y", "on"}


_ENABLED = _env_truthy("MODULE2_CACHE_METRICS_ENABLE", "1")
_RESET_ON_TASK = _env_truthy("MODULE2_CACHE_METRICS_RESET_ON_TASK", "1")
_LOCK = threading.Lock()

_CACHES: Dict[str, Dict[str, int]] = {}
_CONTEXT: Dict[str, Optional[str]] = {"task_id": None, "stage": None}


def _ensure(name: str) -> Dict[str, int]:
    cache = _CACHES.get(name)
    if cache is None:
        cache = {"hits": 0, "misses": 0}
        _CACHES[name] = cache
    return cache


def enabled() -> bool:
    return _ENABLED


def reset_on_task_enabled() -> bool:
    return _ENABLED and _RESET_ON_TASK


def set_context(task_id: Optional[str] = None, stage: Optional[str] = None) -> None:
    if not _ENABLED:
        return
    with _LOCK:
        if task_id:
            _CONTEXT["task_id"] = task_id
        if stage:
            _CONTEXT["stage"] = stage


def reset() -> None:
    if not _ENABLED:
        return
    with _LOCK:
        _CACHES.clear()


def hit(name: str, count: int = 1) -> None:
    if not _ENABLED:
        return
    if not name:
        return
    with _LOCK:
        cache = _ensure(name)
        cache["hits"] += int(count)


def miss(name: str, count: int = 1) -> None:
    if not _ENABLED:
        return
    if not name:
        return
    with _LOCK:
        cache = _ensure(name)
        cache["misses"] += int(count)


def snapshot(task_id: Optional[str] = None, stage: Optional[str] = None) -> Dict[str, Any]:
    if not _ENABLED:
        return {
            "enabled": False,
            "task_id": task_id,
            "stage": stage,
            "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
            "caches": {},
        }
    with _LOCK:
        if task_id:
            _CONTEXT["task_id"] = task_id
        if stage:
            _CONTEXT["stage"] = stage
        caches = {}
        for name, data in _CACHES.items():
            hits = int(data.get("hits", 0))
            misses = int(data.get("misses", 0))
            total = hits + misses
            hit_rate = (hits / total) if total > 0 else 0.0
            caches[name] = {
                "hits": hits,
                "misses": misses,
                "hit_rate": round(hit_rate, 6),
            }
        return {
            "enabled": True,
            "task_id": _CONTEXT.get("task_id"),
            "stage": _CONTEXT.get("stage"),
            "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
            "caches": caches,
        }
