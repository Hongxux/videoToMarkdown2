"""Dependency preflight checks for Python gRPC server startup."""

from __future__ import annotations

import importlib
import inspect
import os
import re
import sys
import traceback
from importlib import metadata as importlib_metadata
from pathlib import Path
from typing import Callable

from .runtime_env import log_boot_step, safe_print

_PREPROCESS_VERSION_SPECS: tuple[tuple[str, tuple[str, ...], str], ...] = (
    ("paddleocr", ("paddleocr",), "2.7.3"),
    ("paddlepaddle", ("paddlepaddle", "paddlepaddle-gpu"), "3.3.0"),
    ("paddlex", ("paddlex",), "3.4.2"),
    ("mediapipe", ("mediapipe",), "0.10.14"),
)


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


def _check_ppstructure_importable() -> tuple[bool, str]:
    """Ensure PP-Structure backend can be imported."""
    try:
        importlib.import_module("paddleocr")
        return True, "ok"
    except Exception as exc:  # pragma: no cover - runtime environment specific
        return False, f"paddleocr import failed: {exc}"


def _check_paddlex_importable() -> tuple[bool, str]:
    """Ensure PaddleX fallback backend can be imported."""
    try:
        importlib.import_module("paddlex")
        return True, "ok"
    except Exception as exc:  # pragma: no cover - runtime environment specific
        return False, f"paddlex import failed: {exc}"


def _check_person_prefilter_backend() -> tuple[bool, str]:
    """
    Ensure person-ratio prefilter backend is usable.
    Code path requires `mediapipe.solutions.selfie_segmentation`.
    """
    try:
        mp = importlib.import_module("mediapipe")
    except Exception as exc:  # pragma: no cover - runtime environment specific
        return False, f"mediapipe import failed: {exc}"

    if not hasattr(mp, "solutions"):
        return False, "mediapipe is installed but missing `mediapipe.solutions` API"

    try:
        selfie = mp.solutions.selfie_segmentation.SelfieSegmentation(model_selection=1)
        close = getattr(selfie, "close", None)
        if callable(close):
            close()
        return True, "ok"
    except Exception as exc:  # pragma: no cover - runtime environment specific
        return False, f"mediapipe selfie_segmentation init failed: {exc}"


def _get_distribution_version(distribution_name: str) -> str | None:
    try:
        return importlib_metadata.version(distribution_name)
    except importlib_metadata.PackageNotFoundError:
        return None
    except Exception:
        return None


def _normalize_version_core(version: str) -> str:
    token = str(version or "").strip()
    if not token:
        return ""
    match = re.match(r"^\d+(?:\.\d+){0,3}", token)
    return match.group(0) if match else token


def _check_preprocess_dependency_versions() -> tuple[bool, str]:
    mismatches: list[str] = []
    for logical_name, candidates, expected_version in _PREPROCESS_VERSION_SPECS:
        installed_from: str | None = None
        installed_version: str | None = None
        for distribution_name in candidates:
            version = _get_distribution_version(distribution_name)
            if version:
                installed_from = distribution_name
                installed_version = version
                break
        if not installed_version:
            mismatches.append(
                f"{logical_name} not installed (expected {expected_version})"
            )
            continue
        installed_core = _normalize_version_core(installed_version)
        if installed_core != expected_version:
            mismatches.append(
                f"{logical_name} version mismatch: installed {installed_version} "
                f"(from {installed_from}), expected {expected_version}"
            )

    if mismatches:
        return False, "; ".join(mismatches)
    return True, "ok"


def _check_pydantic_core_schema_compatibility() -> tuple[bool, str]:
    """
    Validate critical pydantic-core function signatures expected by current pydantic code.
    This catches partially-upgraded/corrupted environments before server startup crashes.
    """
    try:
        from pydantic_core import core_schema
    except Exception as exc:
        return False, f"pydantic_core import failed: {exc}"

    try:
        model_schema_sig = inspect.signature(core_schema.model_schema)
    except Exception as exc:
        return False, f"inspect model_schema failed: {exc}"
    if "generic_origin" not in model_schema_sig.parameters:
        return False, (
            "pydantic_core.model_schema missing parameter 'generic_origin' "
            f"(actual: {model_schema_sig})"
        )

    try:
        with_default_sig = inspect.signature(core_schema.with_default_schema)
    except Exception as exc:
        return False, f"inspect with_default_schema failed: {exc}"
    if "default_factory_takes_data" not in with_default_sig.parameters:
        return False, (
            "pydantic_core.with_default_schema missing parameter 'default_factory_takes_data' "
            f"(actual: {with_default_sig})"
        )

    return True, "ok"


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

    feature_checks: list[tuple[str, Callable[[], tuple[bool, str]]]] = [
        ("pydantic_core_schema_compatibility", _check_pydantic_core_schema_compatibility),
        ("preprocess_dependency_versions", _check_preprocess_dependency_versions),
        ("ppstructure_preprocess", _check_ppstructure_importable),
        ("paddlex_layout_fallback", _check_paddlex_importable),
        ("person_subject_prefilter", _check_person_prefilter_backend),
    ]

    missing_modules: set[str] = set()
    import_errors: list[tuple[str, str]] = []
    feature_failures: list[tuple[str, str]] = []

    for (module_name,) in modules_to_check:
        log_boot_step(f"[CHECK] import {module_name}", debug_imports)
        try:
            importlib.import_module(module_name)
        except ModuleNotFoundError as exc:
            missing_modules.add(exc.name or module_name)
        except Exception:
            import_errors.append((module_name, traceback.format_exc()))

    for feature_name, checker in feature_checks:
        log_boot_step(f"[CHECK] feature {feature_name}", debug_imports)
        ok, detail = checker()
        if not ok:
            feature_failures.append((feature_name, detail))

    if not missing_modules and not import_errors and not feature_failures:
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

    if feature_failures:
        safe_print("Feature readiness failures:")
        for feature_name, detail in feature_failures:
            safe_print(f"- {feature_name}: {detail}")

    safe_print("Tip: install dependencies, then retry with --check-deps --debug-imports.")
    return 2


def run_dependency_preflight(debug_imports: bool = False) -> int:
    """Compatibility alias."""
    return run_dependency_check(debug_imports)


__all__ = ["run_dependency_check", "run_dependency_preflight"]

