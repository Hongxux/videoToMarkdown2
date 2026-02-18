"""服务启动期运行时环境整理工具。"""

from __future__ import annotations

import os
import site
import sys
from typing import Iterable, List


def _iter_user_site_paths() -> List[str]:
    paths: List[str] = []
    try:
        value = site.getusersitepackages()
    except Exception:
        value = []
    if isinstance(value, str):
        value = [value]
    if isinstance(value, Iterable):
        for raw in value:
            token = str(raw or "").strip()
            if token:
                paths.append(os.path.normcase(os.path.normpath(token)))
    return paths


def sanitize_user_site_packages() -> bool:
    """
    执行逻辑：
    1) 默认关闭 user-site 参与导入（可通过 GRPC_SERVER_ALLOW_USER_SITE=1 显式放开）。
    2) 从 sys.path 中移除 user-site 目录，避免全局 pip 包污染 conda 环境。
    实现方式：读取 site.getusersitepackages + 路径归一化过滤。
    核心价值：稳定依赖解析顺序，避免出现 numpy/protobuf/paddleocr 被用户目录抢占。
    """
    allow_user_site = str(os.getenv("GRPC_SERVER_ALLOW_USER_SITE", "") or "").strip().lower()
    if allow_user_site in {"1", "true", "yes", "y", "on"}:
        return False
    # 仅影响后续子进程；当前进程的 site 初始化已完成，需要同步清理 sys.path。
    os.environ.setdefault("PYTHONNOUSERSITE", "1")
    blocked = set(_iter_user_site_paths())
    if not blocked:
        return False

    original = list(sys.path)
    filtered: List[str] = []
    removed = False
    for entry in original:
        normalized = os.path.normcase(os.path.normpath(str(entry or "").strip()))
        if normalized in blocked:
            removed = True
            continue
        filtered.append(entry)
    if removed:
        sys.path[:] = filtered
    return removed


def safe_print(message: str) -> None:
    """安全打印，避免控制台编码导致启动日志抛错。"""
    try:
        print(message, flush=True)
    except UnicodeEncodeError:
        text = str(message or "").encode("ascii", "backslashreplace").decode("ascii")
        print(text, flush=True)


def log_boot_step(message: str, enabled: bool) -> None:
    """按开关输出启动阶段进度日志。"""
    if enabled:
        safe_print(message)
