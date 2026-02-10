"""gRPC 服务运行时基础能力。
职责边界：提供环境变量、标准输出容错、路径优先级等通用执行能力。
"""

from __future__ import annotations

import os
import sys


def configure_opencv_env() -> None:
    """配置 OpenCV 运行时参数，减少无效 OpenCL 初始化开销。"""
    if not os.getenv("OPENCV_OPENCL_RUNTIME"):
        os.environ["OPENCV_OPENCL_RUNTIME"] = "disabled"


def reconfigure_stdio_errors() -> None:
    """将 stdout/stderr 的编码错误策略设为可容错模式。"""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(errors="backslashreplace")
        except Exception:
            continue


def safe_print(message: str) -> None:
    """安全打印，遇到编码异常时自动转义不可编码字符。"""
    try:
        print(message, flush=True)
    except UnicodeEncodeError:
        escaped = message.encode("ascii", "backslashreplace").decode("ascii")
        print(escaped, flush=True)


def is_truthy_env(name: str) -> bool:
    """将环境变量解析为布尔值。"""
    value = os.getenv(name, "").strip().lower()
    return value in {"1", "true", "yes", "y", "on"}


def prepend_sys_path(path_value: str) -> None:
    """将路径置于 `sys.path` 首位，保证导入优先级可控。"""
    if not path_value:
        return
    normalized = os.path.abspath(path_value)
    if normalized in sys.path:
        sys.path.remove(normalized)
    sys.path.insert(0, normalized)


def log_boot_step(message: str, debug_enabled: bool) -> None:
    """按调试开关输出启动阶段日志。"""
    if debug_enabled:
        safe_print(message)


def boot(message: str, debug_enabled: bool) -> None:
    """兼容别名：保留历史 `boot` 方法名。"""
    log_boot_step(message, debug_enabled)


__all__ = [
    "configure_opencv_env",
    "reconfigure_stdio_errors",
    "safe_print",
    "is_truthy_env",
    "prepend_sys_path",
    "log_boot_step",
    "boot",
]

