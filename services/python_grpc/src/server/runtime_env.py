"""服务启动期运行时环境整理工具。"""

from __future__ import annotations

import os
import inspect
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


def patch_pydantic_generic_origin_compat() -> bool:
    """
    执行逻辑：
    1) 检测 pydantic_core.model_schema 是否支持 generic_origin 参数。
    2) 若不支持，则注入兼容包装函数并忽略该参数，避免版本错配导致启动期崩溃。
    核心价值：将“环境包错配”从硬失败降级为可观测告警，保证服务可启动。
    """
    try:
        from pydantic_core import core_schema
    except Exception:
        return False

    try:
        signature = inspect.signature(core_schema.model_schema)
    except Exception:
        return False

    if "generic_origin" in signature.parameters:
        return False

    original_model_schema = core_schema.model_schema

    def _compat_model_schema(
        cls,
        schema,
        *,
        generic_origin=None,
        **kwargs,
    ):
        _ = generic_origin
        return original_model_schema(cls, schema, **kwargs)

    core_schema.model_schema = _compat_model_schema
    safe_print(
        "[BOOT] Applied pydantic_core compatibility shim: model_schema(generic_origin) -> ignored"
    )
    return True


def patch_protobuf_message_factory_compat() -> bool:
    try:
        from google.protobuf import message_factory
    except Exception:
        return False

    factory_cls = getattr(message_factory, 'MessageFactory', None)
    get_message_class = getattr(message_factory, 'GetMessageClass', None)
    if factory_cls is None or get_message_class is None:
        return False
    if hasattr(factory_cls, 'GetPrototype'):
        return False

    def _compat_get_prototype(self, descriptor):
        return get_message_class(descriptor)

    factory_cls.GetPrototype = _compat_get_prototype
    safe_print(
        '[BOOT] Applied protobuf compatibility shim: MessageFactory.GetPrototype -> GetMessageClass'
    )
    return True


def log_boot_step(message: str, enabled: bool) -> None:
    """按开关输出启动阶段进度日志。"""
    if enabled:
        safe_print(message)
