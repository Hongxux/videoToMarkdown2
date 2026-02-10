"""Dependency preflight checks for Python gRPC server startup."""

from __future__ import annotations

import importlib
import os
import sys
import traceback
from pathlib import Path

from .runtime_env import log_boot_step, safe_print


def _prepare_preflight_paths() -> None:
    """为依赖预检补齐导入路径。

    职责边界：
    - 仅服务于 `--check-deps` 的导入校验。
    - 不改变业务运行时的模块加载策略。
    """
    repo_root = Path(__file__).resolve().parents[4]
    candidate_paths = [
        repo_root,
        repo_root / "contracts" / "gen" / "python",
    ]

    for candidate in candidate_paths:
        path_value = os.fspath(candidate)
        if path_value in sys.path:
            sys.path.remove(path_value)
        sys.path.insert(0, path_value)


def run_dependency_check(debug_imports: bool = False) -> int:
    """Run startup dependency preflight; return 0 for pass and 2 for failure."""
    _prepare_preflight_paths()

    modules_to_check = [
        ("psutil",),
        ("grpc",),
        ("grpc.aio",),
        ("numpy",),
        ("video_processing_pb2",),
        ("video_processing_pb2_grpc",),
        ("services.python_grpc.src.transcript_pipeline.graph",),
        ("services.python_grpc.src.media_engine.knowledge_engine.core.video",),
        ("services.python_grpc.src.media_engine.knowledge_engine.core.transcription",),
        ("services.python_grpc.src.content_pipeline",),
    ]

    missing_modules: set[str] = set()
    import_errors: list[tuple[str, str]] = []

    for (module_name,) in modules_to_check:
        log_boot_step(f"[CHECK] import {module_name}", debug_imports)
        try:
            importlib.import_module(module_name)
        except ModuleNotFoundError as exc:
            missing_modules.add(exc.name or module_name)
        except Exception:
            import_errors.append((module_name, traceback.format_exc()))

    if not missing_modules and not import_errors:
        safe_print("Dependency preflight passed.")
        return 0

    safe_print("Dependency preflight failed.")
    if missing_modules:
        safe_print("Missing modules:")
        for name in sorted(missing_modules):
            safe_print(f"- {name}")

    if import_errors:
        safe_print("Import errors:")
        for module_name, detail in import_errors:
            safe_print(f"- {module_name}")
            safe_print(detail.strip())

    safe_print("Tip: install dependencies, then retry with --check-deps --debug-imports.")
    return 2


def run_dependency_preflight(debug_imports: bool = False) -> int:
    """Compatibility alias."""
    return run_dependency_check(debug_imports)


__all__ = ["run_dependency_check", "run_dependency_preflight"]

