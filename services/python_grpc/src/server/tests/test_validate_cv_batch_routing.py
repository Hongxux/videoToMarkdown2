import ast
import asyncio
import gc
import os
import sys
import threading
import time
import traceback
from collections import Counter
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any, Dict, List, Tuple

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[5]))


class _ProtoMessage:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


class _VideoProcessingPb2:
    StableIsland = type("StableIsland", (_ProtoMessage,), {})
    ActionSegment = type("ActionSegment", (_ProtoMessage,), {})
    CVValidationResult = type("CVValidationResult", (_ProtoMessage,), {})
    CVValidationResponse = type("CVValidationResponse", (_ProtoMessage,), {})


class _Logger:
    def info(self, *_args, **_kwargs):
        return None

    def warning(self, *_args, **_kwargs):
        return None

    def error(self, *_args, **_kwargs):
        return None


def _load_validate_cv_batch_impl():
    source_path = Path(__file__).resolve().parents[1] / "grpc_service_impl.py"
    source = source_path.read_text(encoding="utf-8")
    module_ast = ast.parse(source, filename=str(source_path))

    for node in module_ast.body:
        if not isinstance(node, ast.ClassDef) or node.name != "_VideoProcessingServicerCore":
            continue
        for child in node.body:
            if isinstance(child, ast.AsyncFunctionDef) and child.name == "_validation_validate_cv_batch_impl":
                isolated_module = ast.Module(body=[child], type_ignores=[])
                ast.fix_missing_locations(isolated_module)
                globals_dict = {
                    "__name__": "test_validate_cv_batch_routing_exec",
                    "Any": Any,
                    "Counter": Counter,
                    "Dict": Dict,
                    "List": List,
                    "Tuple": Tuple,
                    "_safe_float": lambda value, default=0.0: default if value in (None, "") else float(value),
                    "asyncio": asyncio,
                    "gc": gc,
                    "logger": _Logger(),
                    "os": os,
                    "psutil": SimpleNamespace(
                        virtual_memory=lambda: SimpleNamespace(available=8 * 1024**3)
                    ),
                    "time": time,
                    "traceback": traceback,
                    "video_processing_pb2": _VideoProcessingPb2,
                }
                exec(compile(isolated_module, str(source_path), "exec"), globals_dict)
                return globals_dict["_validation_validate_cv_batch_impl"]
    raise AssertionError("_validation_validate_cv_batch_impl not found in grpc_service_impl.py")


def _build_servicer():
    servicer = SimpleNamespace()
    servicer._task_lock = threading.Lock()
    servicer._active_tasks = 0
    servicer.cv_worker_count = 1
    servicer._increment_tasks = lambda: None
    servicer._get_frame_registry = lambda *_args, **_kwargs: SimpleNamespace(max_frames=80)
    servicer._create_ephemeral_frame_registry = lambda *_args, **_kwargs: None
    servicer._cleanup_ephemeral_frame_registry = lambda *_args, **_kwargs: None
    servicer._batch_read_coarse_frames_to_shm = (
        lambda *_args, **_kwargs: {
            "SUCF01": {
                0.0: {"shm_name": "coarse-0"},
                1.0: {"shm_name": "coarse-1"},
            }
        }
    )
    servicer._get_cv_process_pool = lambda *_args, **_kwargs: None
    return servicer


async def _collect_responses(generator):
    return [item async for item in generator]


def test_validate_cv_batch_routes_coarse_fine_units_without_unbound_analysis_width(
    monkeypatch,
    tmp_path,
):
    validate_cv_batch_impl = _load_validate_cv_batch_impl()
    servicer = _build_servicer()
    captured = {}

    fake_worker = ModuleType("services.python_grpc.src.vision_validation.worker")

    def _fake_run_cv_validation_task(*_args, **_kwargs):
        raise AssertionError("process-unit CV path should not run in coarse-fine routing test")

    def _fake_run_coarse_fine_screenshot_task(**kwargs):
        captured["analysis_max_width"] = kwargs["analysis_max_width"]
        captured["unit_id"] = kwargs["unit_id"]
        return {
            "stable_islands": [{"start_sec": 0.0, "end_sec": 1.0, "mid_sec": 0.5, "duration_sec": 1.0}],
            "action_segments": [],
        }

    fake_worker.run_cv_validation_task = _fake_run_cv_validation_task
    fake_worker.run_coarse_fine_screenshot_task = _fake_run_coarse_fine_screenshot_task
    monkeypatch.setitem(sys.modules, "services.python_grpc.src.vision_validation.worker", fake_worker)
    fake_config_loader = ModuleType("services.python_grpc.src.content_pipeline.infra.runtime.config_loader")
    fake_config_loader.load_module2_config = lambda: {
        "vl_material_generation": {
            "routing": {
                "screenshot_analysis_max_width": 912,
            }
        }
    }
    monkeypatch.setitem(
        sys.modules,
        "services.python_grpc.src.content_pipeline.infra.runtime.config_loader",
        fake_config_loader,
    )

    request = SimpleNamespace(
        task_id="task-cf-routing",
        video_path=str(tmp_path / "video.mp4"),
        semantic_units=[
            SimpleNamespace(
                unit_id="SUCF01",
                start_sec=0.0,
                end_sec=1.0,
                knowledge_type="concrete",
            )
        ],
    )

    responses = asyncio.run(_collect_responses(validate_cv_batch_impl(servicer, request, None)))

    assert len(responses) == 1
    assert responses[0].success is True
    assert getattr(responses[0], "error_msg", "") == ""
    assert len(responses[0].results) == 1
    assert responses[0].results[0].unit_id == "SUCF01"
    assert captured == {
        "analysis_max_width": 912,
        "unit_id": "SUCF01",
    }
