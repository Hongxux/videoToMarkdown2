"""
VL Material Generator - VL 素材生成鍣?

功能锛?
1. 调用 split_video_by_semantic_units.py 切割语义单元视频片段
2. 对每个片段调鐢?VLVideoAnalyzer 进 VL 分析
3. 汇总分析结果生成素材请姹?
4. 优化截图时间点（鍦?±1s 范围内查找最佳帧锛?
5. 失败时自动回退到现鏈?GenerateMaterialRequests 流程

使用方式锛?
    generator = VLMaterialGenerator(config)
    result = await generator.generate(video_path, semantic_units)
"""

import os
import json
import logging
import asyncio
import time
import random
import re
import base64
import inspect
import functools
import threading
import subprocess
import weakref
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass, field
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple, Callable, Awaitable

from services.python_grpc.src.common.utils.numbers import safe_float
from services.python_grpc.src.common.utils.opencv_decode import open_video_capture_with_fallback
from services.python_grpc.src.common.utils.process_pool import create_spawn_process_pool
from services.python_grpc.src.content_pipeline.shared.subtitle.subtitle_repository import SubtitleRepository
from services.python_grpc.src.content_pipeline.infra.runtime.vl_interval_utils import (
    normalize_intervals,
    subtract_intervals,
    build_removed_intervals_from_stable,
)
from services.python_grpc.src.content_pipeline.infra.runtime.vl_prefetch_utils import (
    resolve_max_workers,
    build_screenshot_prefetch_chunks,
    build_task_params_from_ts_map,
    resolve_adaptive_prefetch_step,
)
from services.python_grpc.src.content_pipeline.infra.runtime.vl_ffmpeg_utils import (
    export_clip_asset_with_ffmpeg,
    export_keyframe_with_ffmpeg,
    export_keyframes_with_ffmpeg_batch,
    concat_segments_with_ffmpeg,
)
from services.python_grpc.src.content_pipeline.phase2a.materials.models import VLGenerationResult
from services.python_grpc.src.content_pipeline.phase2a.materials.errors import (
    VLMaterialGeneratorError,
    VLAnalysisError,
    JSONParseError,
)
from services.python_grpc.src.content_pipeline.infra.llm import llm_gateway
from services.python_grpc.src.content_pipeline.infra.llm.token_costing import (
    build_token_cost_estimate,
    get_token_pricing_snapshot,
    normalize_usage_payload,
    summarize_token_cost_records,
)
from services.python_grpc.src.content_pipeline.infra.llm.prompt_loader import get_prompt
from services.python_grpc.src.content_pipeline.infra.llm.prompt_registry import PromptKeys
from services.python_grpc.src.content_pipeline.phase2a.materials.vl_instructional_keyframe_extractor import (
    crop_keyframe_inplace_by_grid_range,
    normalize_bbox_1000,
    save_grid_overlay_image,
    save_top_reason_banner_image,
)
from services.python_grpc.src.content_pipeline.common.utils.id_utils import build_unit_relative_asset_id
from services.python_grpc.src.content_pipeline.common.utils.path_utils import find_repo_root
from services.python_grpc.src.content_pipeline.phase2a.materials.flow_ops import (
    split_video_by_semantic_units,
    find_clip_for_unit,
    optimize_screenshots_batch_mode,
    optimize_screenshots_streaming_pipeline,
)

logger = logging.getLogger(__name__)

_DEFAULT_GRID_SPATIAL_ANCHOR_KEY = "vl.video_analysis.grid_spatial_anchor"
_DEFAULT_VL_ARG_STRUCTURED_SYSTEM_KEY = "deepseek.vl_arg.structured.system"
_DEFAULT_VL_ARG_STRUCTURED_USER_KEY = "deepseek.vl_arg.structured.user"
_PACKAGE_PROMPT_ROOT = (Path(__file__).resolve().parents[2] / "prompts").resolve()
_DEFAULT_GRID_SPATIAL_ANCHOR_REL_PATH = "vl/video_analysis/grid_spatial_anchor.md"
_DEFAULT_VL_ARG_STRUCTURED_SYSTEM_REL_PATH = "deepseek/vl_arg/structured_system.md"
_DEFAULT_VL_ARG_STRUCTURED_USER_REL_PATH = "deepseek/vl_arg/structured_user.md"
_DEFAULT_BEST_FRAME_SELECTION_REL_PATH = "vision_ai/best_frame_selection.md"
_SCREENSHOT_TASK_GATE_ENV = "MODULE2_SCREENSHOT_TASK_MAX_CONCURRENCY"
_SCREENSHOT_TASK_GATE_LOCK = threading.Lock()
_SCREENSHOT_TASK_GATES: "weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Semaphore]" = weakref.WeakKeyDictionary()
_SCREENSHOT_TASK_GATE_LIMITS: "weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, int]" = weakref.WeakKeyDictionary()


def _resolve_screenshot_task_gate_limit(screenshot_config: Dict[str, Any]) -> int:
    raw_limit = screenshot_config.get("task_max_concurrency", os.getenv(_SCREENSHOT_TASK_GATE_ENV, 2))
    try:
        return max(1, int(raw_limit))
    except (TypeError, ValueError):
        return 2


def _resolve_screenshot_task_gate(limit: int) -> asyncio.Semaphore:
    loop = asyncio.get_running_loop()
    with _SCREENSHOT_TASK_GATE_LOCK:
        gate = _SCREENSHOT_TASK_GATES.get(loop)
        current_limit = _SCREENSHOT_TASK_GATE_LIMITS.get(loop)
        if gate is None:
            gate = asyncio.Semaphore(limit)
            _SCREENSHOT_TASK_GATES[loop] = gate
            _SCREENSHOT_TASK_GATE_LIMITS[loop] = limit
            return gate
        if current_limit != limit and getattr(gate, "_value", 0) == int(current_limit or 0):
            gate = asyncio.Semaphore(limit)
            _SCREENSHOT_TASK_GATES[loop] = gate
            _SCREENSHOT_TASK_GATE_LIMITS[loop] = limit
        return gate


def _resolve_prompt_key_attr(attr_name: str, default_key: str) -> str:
    raw_key = getattr(PromptKeys, attr_name, default_key)
    if not isinstance(raw_key, str):
        return default_key
    normalized_key = raw_key.strip()
    return normalized_key if normalized_key else default_key


def _safe_get_prompt(prompt_key: str, *, fallback: str) -> str:
    try:
        return get_prompt(prompt_key, fallback=fallback)
    except KeyError:
        logger.warning(
            "Prompt key is missing in registry, fallback to in-code prompt: key=%s",
            prompt_key,
        )
        return fallback


def _load_package_prompt_default(relative_path: str, *, fallback: str) -> str:
    prompt_path = (_PACKAGE_PROMPT_ROOT / relative_path).resolve()
    try:
        if prompt_path.exists() and prompt_path.is_file():
            return prompt_path.read_text(encoding="utf-8")
    except Exception as error:
        logger.warning(
            "Load package prompt fallback failed: path=%s, error=%s",
            prompt_path,
            error,
        )
    return fallback


def _sanitize_stream_unit_folder_name(text: str) -> str:
    """将语义单元标识清洗为安全目录名，避免跨平台路径字符问题。"""
    cleaned = re.sub(r"[^0-9A-Za-z._-]+", "_", str(text or "").strip())
    cleaned = cleaned.strip("._")
    return cleaned or "UNKNOWN_UNIT"


def _split_single_unit_clip_worker(
    *,
    video_path: str,
    semantic_unit: Dict[str, Any],
    output_dir: str,
    split_pre_cut_config: Dict[str, Any],
) -> Dict[str, Any]:
    """
    在独立进程中对单个语义单元执行切片。
    设计点：
    1) 每个 unit 使用独立输出目录，避免 manifest/中间文件并发写冲突；
    2) 复用既有 split_video_by_semantic_units.py，减少新逻辑分叉；
    3) 返回结构化结果，主流程可按完成顺序流式衔接后续步骤。
    """
    unit_id = str((semantic_unit or {}).get("unit_id", "") or "").strip()
    start_sec = safe_float((semantic_unit or {}).get("start_sec", 0.0), 0.0)
    end_sec = safe_float((semantic_unit or {}).get("end_sec", 0.0), 0.0)
    if not unit_id:
        return {
            "success": False,
            "unit_id": "",
            "error": "missing_unit_id",
        }
    if end_sec <= start_sec:
        return {
            "success": False,
            "unit_id": unit_id,
            "error": f"invalid_time_range:{start_sec:.3f}-{end_sec:.3f}",
        }

    project_root = find_repo_root(__file__)
    script_path = project_root / "tools" / "split_video_by_semantic_units.py"
    if not script_path.exists():
        return {
            "success": False,
            "unit_id": unit_id,
            "error": f"split_script_not_found:{script_path}",
        }

    unit_folder = _sanitize_stream_unit_folder_name(unit_id)
    unit_split_dir = (
        Path(output_dir).resolve()
        / "semantic_unit_clips_vl"
        / "_stream_units"
        / unit_folder
    )
    unit_split_dir.mkdir(parents=True, exist_ok=True)

    semantic_units_json = unit_split_dir / "semantic_units_vl_subset.json"
    semantic_units_json.write_text(
        json.dumps([semantic_unit], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    cmd: List[str] = [
        "python",
        str(script_path),
        "--video",
        str(Path(video_path).resolve()),
        "--semantic-units",
        str(semantic_units_json),
        "--out-dir",
        str(unit_split_dir),
        "--overwrite",
    ]

    split_cfg = split_pre_cut_config if isinstance(split_pre_cut_config, dict) else {}
    if bool(split_cfg.get("enabled", True)):
        large_segment_threshold_sec = max(
            0.0,
            safe_float(split_cfg.get("large_segment_threshold_sec", 120.0), 120.0),
        )
        large_segment_scale_height = max(
            0,
            int(safe_float(split_cfg.get("downscale_height", 480.0), 480.0)),
        )
        large_segment_video_bitrate = (
            str(split_cfg.get("video_bitrate", "500k") or "").strip() or "500k"
        )
        cmd.extend(
            [
                "--large-segment-threshold-sec",
                f"{large_segment_threshold_sec:.3f}",
                "--large-segment-scale-height",
                str(large_segment_scale_height),
                "--large-segment-video-bitrate",
                large_segment_video_bitrate,
            ]
        )
        if bool(split_cfg.get("apply_to_all_units", False)):
            cmd.append("--apply-low-res-to-all-units")

    process = subprocess.run(cmd, capture_output=True, text=True)
    if process.returncode != 0:
        error_text = (process.stderr or process.stdout or "").strip()
        return {
            "success": False,
            "unit_id": unit_id,
            "error": f"split_failed_rc={process.returncode}",
            "stderr": error_text[:500],
            "command": cmd,
        }

    unit_pattern = re.compile(rf"(?:^|_){re.escape(unit_id)}(?:_|$)", re.IGNORECASE)
    clip_candidates = [
        clip_path for clip_path in unit_split_dir.glob("*.mp4")
        if unit_pattern.search(clip_path.stem)
    ]
    if not clip_candidates:
        clip_candidates = list(unit_split_dir.glob("*.mp4"))
    if not clip_candidates:
        return {
            "success": False,
            "unit_id": unit_id,
            "error": "split_succeeded_but_clip_missing",
            "command": cmd,
        }

    clip_candidates.sort(
        key=lambda path: path.stat().st_mtime if path.exists() else 0.0,
        reverse=True,
    )
    selected_clip_path = clip_candidates[0]
    return {
        "success": True,
        "unit_id": unit_id,
        "clip_path": str(selected_clip_path.resolve()),
        "clip_dir": str(unit_split_dir.resolve()),
        "command": cmd,
    }


@dataclass
class LegacyFallbackMaterial:
    """
    legacy-action 分支在 VL 失败时的回退素材容器。
    这样做的价值：
    1) 将回退语义从松散 dict 提升为显式结构，减少主流程分支判断复杂度；
    2) 让“保存回退素材”和“应用回退素材”使用同一契约，避免键名漂移。
    """
    clip_requests: List[Dict[str, Any]] = field(default_factory=list)
    screenshot_requests: List[Dict[str, Any]] = field(default_factory=list)

    def apply_to(
        self,
        *,
        target_clip_requests: List[Dict[str, Any]],
        target_screenshot_requests: List[Dict[str, Any]],
    ) -> None:
        """将回退素材追加到目标聚合列表。"""
        target_clip_requests.extend(list(self.clip_requests or []))
        target_screenshot_requests.extend(list(self.screenshot_requests or []))


@dataclass
class LegacyActionDispatchPlan:
    """
    legacy-action 分支调度准备结果。
    作用：
    1) 聚合“立即可用”的回退素材（无需进入 VL）；
    2) 聚合“需要进入 tutorial_stepwise VL”的任务与 pre-prune 信息；
    3) 聚合 VL 失败后的 fallback 素材映射。
    """
    legacy_unit_count: int = 0
    immediate_clip_requests: List[Dict[str, Any]] = field(default_factory=list)
    immediate_screenshot_requests: List[Dict[str, Any]] = field(default_factory=list)
    vl_unit_tasks: List[Dict[str, Any]] = field(default_factory=list)
    vl_pre_prune_results: List[Dict[str, Any]] = field(default_factory=list)
    fallback_materials: Dict[str, LegacyFallbackMaterial] = field(default_factory=dict)


class VLMaterialGenerator:
    """
    VL 素材生成鍣?
    
    璐熻矗锛?
    1. 视频按语义单元切鍓?
    2. VL 分析每个片段
    3. 截图时间点优鍖?
    4. 失败回退
    """

    _visual_extractor_cache: Dict[str, Any] = {}
    _visual_extractor_cache_lock = threading.Lock()
    
    def __init__(self, config: Dict[str, Any] = None, *, cv_executor: Any = None):
        """
        初始化生成器
        
        Args:
            config: VL 素材生成配置（来鑷?module2_config.yaml锛?
            cv_executor: 可选的外部 Executor（通常涓?python_grpc_server 的全局 CV ProcessPool），用于复用进程池与 initializer銆?
        """
        if config is None:
            from services.python_grpc.src.content_pipeline.infra.runtime.config_loader import load_module2_config
            full_config = load_module2_config()
            config = full_config.get("vl_material_generation", {})
        
        self.config = config
        self.enabled = config.get("enabled", False)
        self.screenshot_config = config.get("screenshot_optimization", {})
        self.fallback_config = config.get("fallback", {})
        self.best_frame_vision_select_enabled = bool(
            self.screenshot_config.get("best_frame_vision_select_enabled", True)
        )
        try:
            self.best_frame_vision_max_candidates = max(
                2,
                int(self.screenshot_config.get("best_frame_vision_max_candidates", 4)),
            )
        except (TypeError, ValueError):
            self.best_frame_vision_max_candidates = 4
        try:
            self.best_frame_vision_timeout_sec = max(
                1.0,
                float(self.screenshot_config.get("best_frame_vision_timeout_sec", 45.0)),
            )
        except (TypeError, ValueError):
            self.best_frame_vision_timeout_sec = 45.0
        try:
            self.best_frame_vision_max_tokens = max(
                8,
                int(self.screenshot_config.get("best_frame_vision_max_tokens", 64)),
            )
        except (TypeError, ValueError):
            self.best_frame_vision_max_tokens = 64
        self._best_frame_selection_prompt = _load_package_prompt_default(
            _DEFAULT_BEST_FRAME_SELECTION_REL_PATH,
            fallback=self._get_default_best_frame_selection_prompt(),
        )

        # VL 前预处理：剔闄?process 单元中的长时闂?stable 片段，降浣?VL 输入冗余
        # 关键取舍锛?
        # 1) 默认仅对 process 单元生效，避免影鍝?abstract/concrete 的语义完整性銆?
        # 2) 采用“stable 核心剔除 + 1s 边缘保留”，在节省成本与保留上下文之间平衡銆?
        # 3) 若预处理失败，自动回退原始片段，保证主流程可用性銆?
        self.pre_vl_pruning_config = config.get("pre_vl_static_pruning", {})

        def _parse_pre_vl_flag(raw_value: Any, default_value: bool) -> bool:
            if isinstance(raw_value, bool):
                return raw_value
            if isinstance(raw_value, (int, float)):
                return bool(raw_value)
            if isinstance(raw_value, str):
                lowered = raw_value.strip().lower()
                if lowered in {"1", "true", "yes", "y", "on"}:
                    return True
                if lowered in {"0", "false", "no", "n", "off"}:
                    return False
            return bool(default_value)

        legacy_global_enabled = _parse_pre_vl_flag(
            self.pre_vl_pruning_config.get("enabled", False),
            False,
        )
        self.pre_vl_only_process = _parse_pre_vl_flag(
            self.pre_vl_pruning_config.get("only_process", True),
            True,
        )
        raw_process_enabled = self.pre_vl_pruning_config.get("process_enabled", None)
        raw_concrete_enabled = self.pre_vl_pruning_config.get("concrete_enabled", None)
        if raw_process_enabled is None:
            self.pre_vl_process_pruning_enabled = bool(legacy_global_enabled)
        else:
            self.pre_vl_process_pruning_enabled = _parse_pre_vl_flag(raw_process_enabled, False)
        if raw_concrete_enabled is None:
            self.pre_vl_concrete_pruning_enabled = bool(
                legacy_global_enabled and not self.pre_vl_only_process
            )
        else:
            self.pre_vl_concrete_pruning_enabled = _parse_pre_vl_flag(raw_concrete_enabled, False)
        self.pre_vl_pruning_enabled = bool(
            self.pre_vl_process_pruning_enabled or self.pre_vl_concrete_pruning_enabled
        )

        self.pre_vl_min_unit_duration_sec = float(self.pre_vl_pruning_config.get("min_unit_duration_sec", 10.0))
        self.pre_vl_keep_edge_sec = float(self.pre_vl_pruning_config.get("keep_edge_sec", 1.0))
        # stable 片段长度必须严格大于该阈值才允许进入剔除流程
        self.pre_vl_min_stable_interval_sec = float(self.pre_vl_pruning_config.get("min_stable_interval_sec", 3.0))
        self.pre_vl_min_cut_span_sec = float(self.pre_vl_pruning_config.get("min_cut_span_sec", 0.8))
        self.pre_vl_min_keep_segment_sec = float(self.pre_vl_pruning_config.get("min_keep_segment_sec", 0.5))
        self.pre_vl_min_removed_ratio = float(self.pre_vl_pruning_config.get("min_removed_ratio", 0.10))
        try:
            legacy_trigger_ratio = float(
                self.pre_vl_pruning_config.get("legacy_action_trigger_ratio", 1.0 / 3.0)
            )
        except (TypeError, ValueError):
            legacy_trigger_ratio = 1.0 / 3.0
        self.pre_vl_legacy_action_trigger_ratio = max(0.0, min(1.0, legacy_trigger_ratio))
        try:
            self.pre_vl_legacy_action_window_sec = max(
                0.0, float(self.pre_vl_pruning_config.get("legacy_action_window_sec", 1.0))
            )
        except (TypeError, ValueError):
            self.pre_vl_legacy_action_window_sec = 1.0
        try:
            self.pre_vl_legacy_action_min_dynamic_sec = max(
                0.0,
                float(self.pre_vl_pruning_config.get("legacy_action_min_dynamic_sec", 0.5)),
            )
        except (TypeError, ValueError):
            self.pre_vl_legacy_action_min_dynamic_sec = 0.5
        self.pre_vl_context_text_max_chars = int(self.pre_vl_pruning_config.get("context_text_max_chars", 800))
        raw_process_stable_detect_enabled = self.pre_vl_pruning_config.get("process_stable_detect_enabled", False)
        if isinstance(raw_process_stable_detect_enabled, bool):
            self.pre_vl_process_stable_detect_enabled = raw_process_stable_detect_enabled
        else:
            self.pre_vl_process_stable_detect_enabled = (
                str(raw_process_stable_detect_enabled or "").strip().lower() in {"1", "true", "yes", "y", "on"}
            )
        self.pre_vl_parallel_mode = str(self.pre_vl_pruning_config.get("parallel_mode", "off") or "off").strip().lower()
        self.pre_vl_parallel_workers = self.pre_vl_pruning_config.get("parallel_workers", "auto")
        try:
            self.pre_vl_parallel_hard_cap = max(1, int(self.pre_vl_pruning_config.get("parallel_hard_cap", 8)))
        except (TypeError, ValueError):
            self.pre_vl_parallel_hard_cap = 8

        # Stable 剔除后，合并前做一次边界纠偏（语义句头 + MSE 终点 + 语流缓冲锛?
        self.pre_vl_boundary_refine_config = config.get("pre_vl_boundary_refine", {})
        self.pre_vl_boundary_refine_enabled = bool(self.pre_vl_boundary_refine_config.get("enabled", True))
        self.pre_vl_pause_threshold_sec = float(self.pre_vl_boundary_refine_config.get("pause_threshold_sec", 0.3))
        self.pre_vl_start_buffer_sec = float(self.pre_vl_boundary_refine_config.get("start_buffer_sec", 0.2))
        self.pre_vl_end_buffer_sec = float(self.pre_vl_boundary_refine_config.get("end_buffer_sec", 0.3))
        self.pre_vl_semantic_search_window_sec = float(self.pre_vl_boundary_refine_config.get("semantic_search_window_sec", 8.0))
        self.pre_vl_mse_scan_after_end_sec = float(self.pre_vl_boundary_refine_config.get("mse_scan_after_end_sec", 3.0))
        self.pre_vl_mse_sample_fps = float(self.pre_vl_boundary_refine_config.get("mse_sample_fps", 2.0))
        self.pre_vl_mse_min_threshold = float(self.pre_vl_boundary_refine_config.get("mse_min_threshold", 64.0))

        # VL 主分析并发配置：以“语义单元”为最小调度粒度。
        # 约束：每个 unit 仅调度一次 analyze_clip，避免调度层重复调用。
        self.vl_analysis_config = config.get("vl_analysis", {}) if isinstance(config.get("vl_analysis", {}), dict) else {}
        self.vl_parallel_workers = self.vl_analysis_config.get("parallel_workers", "auto")
        try:
            self.vl_parallel_hard_cap = max(1, int(self.vl_analysis_config.get("parallel_hard_cap", 32)))
        except (TypeError, ValueError):
            self.vl_parallel_hard_cap = 32
        self.stream_unit_pipeline_enabled = _parse_pre_vl_flag(
            self.vl_analysis_config.get("stream_unit_pipeline_enabled", False),
            False,
        )
        self.stream_split_process_workers = self.vl_analysis_config.get("stream_split_process_workers", "auto")
        try:
            self.stream_split_process_hard_cap = max(
                1,
                int(self.vl_analysis_config.get("stream_split_process_hard_cap", 8)),
            )
        except (TypeError, ValueError):
            self.stream_split_process_hard_cap = 8
        try:
            self.stream_split_backpressure_multiplier = max(
                1,
                int(self.vl_analysis_config.get("stream_split_backpressure_multiplier", 4)),
            )
        except (TypeError, ValueError):
            self.stream_split_backpressure_multiplier = 4
        raw_stream_types = self.vl_analysis_config.get(
            "stream_pipeline_knowledge_types",
            ["process", "concrete"],
        )
        if isinstance(raw_stream_types, str):
            stream_type_candidates = [
                item.strip()
                for item in re.split(r"[,\s]+", raw_stream_types)
                if item and item.strip()
            ]
        elif isinstance(raw_stream_types, (list, tuple, set)):
            stream_type_candidates = [str(item or "").strip() for item in raw_stream_types if str(item or "").strip()]
        else:
            stream_type_candidates = ["process", "concrete"]
        normalized_stream_types: set[str] = set()
        for item in stream_type_candidates:
            normalized = self._normalize_pre_vl_pruning_knowledge_type(item)
            if normalized:
                normalized_stream_types.add(normalized)
        self.stream_pipeline_knowledge_types = normalized_stream_types or {"process", "concrete"}


        # AnalyzeWithVL routing: long clips go through process flow
        self.routing_config = config.get("routing", {}) if isinstance(config.get("routing", {}), dict) else {}
        self.process_duration_threshold_sec = float(self.routing_config.get("process_duration_threshold_sec", 20.0))

        # Tutorial mode: process configuration for step splitting and assets
        self.tutorial_mode_config = config.get("tutorial_mode", {}) if isinstance(config.get("tutorial_mode", {}), dict) else {}
        self.tutorial_mode_enabled = bool(self.tutorial_mode_config.get("enabled", True))
        self.tutorial_min_step_duration_sec = float(self.tutorial_mode_config.get("min_step_duration_sec", 5.0))
        self.tutorial_export_assets = bool(self.tutorial_mode_config.get("export_assets", True))
        self.tutorial_save_step_json = bool(self.tutorial_mode_config.get("save_step_json", True))
        raw_top_reason_banner_enabled = self.tutorial_mode_config.get("top_reason_banner_enabled", True)
        if isinstance(raw_top_reason_banner_enabled, bool):
            self.tutorial_top_reason_banner_enabled = raw_top_reason_banner_enabled
        else:
            self.tutorial_top_reason_banner_enabled = str(raw_top_reason_banner_enabled or "").strip().lower() in {"1", "true", "yes", "y", "on"}
        self.tutorial_assets_root_dir = str(self.tutorial_mode_config.get("assets_root_dir", "vl_tutorial_units") or "vl_tutorial_units")
        self.tutorial_export_from_original_clip_when_prepruned = bool(
            self.tutorial_mode_config.get("export_from_original_clip_when_prepruned", True)
        )
        self.tutorial_asset_export_parallel_workers = self.tutorial_mode_config.get("asset_export_parallel_workers", "auto")
        try:
            self.tutorial_asset_export_parallel_hard_cap = max(
                1,
                int(self.tutorial_mode_config.get("asset_export_parallel_hard_cap", 8)),
            )
        except (TypeError, ValueError):
            self.tutorial_asset_export_parallel_hard_cap = 8
        self.tutorial_keyframe_image_ext = str(self.tutorial_mode_config.get("keyframe_image_ext", "png") or "png").lower()
        if self.tutorial_keyframe_image_ext not in {"png", "jpg", "jpeg"}:
            self.tutorial_keyframe_image_ext = "png"
        try:
            self.tutorial_keyframe_bbox_expand_ratio = max(
                0.0,
                float(self.tutorial_mode_config.get("keyframe_bbox_expand_ratio", 0.15)),
            )
        except (TypeError, ValueError):
            self.tutorial_keyframe_bbox_expand_ratio = 0.15
        try:
            self.tutorial_keyframe_bbox_min_border_span_1000 = max(
                0,
                int(self.tutorial_mode_config.get("keyframe_bbox_min_border_span_1000", 20)),
            )
        except (TypeError, ValueError):
            self.tutorial_keyframe_bbox_min_border_span_1000 = 20
        try:
            self.tutorial_keyframe_iframe_search_window_sec = max(
                0.0,
                float(self.tutorial_mode_config.get("keyframe_iframe_search_window_sec", 0.2)),
            )
        except (TypeError, ValueError):
            self.tutorial_keyframe_iframe_search_window_sec = 0.2
        try:
            self.tutorial_keyframe_iframe_search_before_sec = max(
                0.0,
                float(self.tutorial_mode_config.get("keyframe_iframe_search_before_sec", 0.0)),
            )
        except (TypeError, ValueError):
            self.tutorial_keyframe_iframe_search_before_sec = 0.0
        try:
            self.tutorial_keyframe_iframe_search_after_sec = max(
                0.0,
                float(
                    self.tutorial_mode_config.get(
                        "keyframe_iframe_search_after_sec",
                        self.tutorial_keyframe_iframe_search_window_sec,
                    )
                ),
            )
        except (TypeError, ValueError):
            self.tutorial_keyframe_iframe_search_after_sec = self.tutorial_keyframe_iframe_search_window_sec
        self.tutorial_keyframe_select_sharpest_iframe = bool(
            self.tutorial_mode_config.get("keyframe_select_sharpest_iframe", True)
        )
        self.tutorial_keyframe_halfscreen_crop = bool(
            self.tutorial_mode_config.get("keyframe_halfscreen_crop", True)
        )
        try:
            self.tutorial_keyframe_upscale_factor = max(
                1.0,
                float(self.tutorial_mode_config.get("keyframe_upscale_factor", 3.0)),
            )
        except (TypeError, ValueError):
            self.tutorial_keyframe_upscale_factor = 3.0
        self.tutorial_keyframe_upscale_interpolation = str(
            self.tutorial_mode_config.get("keyframe_upscale_interpolation", "lanczos4") or "lanczos4"
        ).strip()
        try:
            self.tutorial_keyframe_usm_sigma = max(
                0.0,
                float(self.tutorial_mode_config.get("keyframe_usm_sigma", 1.0)),
            )
        except (TypeError, ValueError):
            self.tutorial_keyframe_usm_sigma = 1.0
        try:
            self.tutorial_keyframe_usm_amount = max(
                0.0,
                float(self.tutorial_mode_config.get("keyframe_usm_amount", 1.6)),
            )
        except (TypeError, ValueError):
            self.tutorial_keyframe_usm_amount = 1.6
        try:
            self.tutorial_keyframe_usm_threshold = max(
                0,
                int(self.tutorial_mode_config.get("keyframe_usm_threshold", 0)),
            )
        except (TypeError, ValueError):
            self.tutorial_keyframe_usm_threshold = 0
        self.tutorial_keyframe_draw_bbox_red = bool(
            self.tutorial_mode_config.get("keyframe_draw_bbox_red", True)
        )
        self.tutorial_keyframe_draw_bbox_use_expanded = bool(
            self.tutorial_mode_config.get("keyframe_draw_bbox_use_expanded", False)
        )
        self.tutorial_keyframe_draw_on_original_frame = bool(
            self.tutorial_mode_config.get("keyframe_draw_on_original_frame", True)
        )
        try:
            self.tutorial_keyframe_original_draw_crop_expand_ratio = max(
                0.0,
                float(self.tutorial_mode_config.get("keyframe_original_draw_crop_expand_ratio", 0.30)),
            )
        except (TypeError, ValueError):
            self.tutorial_keyframe_original_draw_crop_expand_ratio = 0.30
        try:
            self.tutorial_keyframe_original_draw_crop_min_border_span_1000 = max(
                0,
                int(self.tutorial_mode_config.get("keyframe_original_draw_crop_min_border_span_1000", 20)),
            )
        except (TypeError, ValueError):
            self.tutorial_keyframe_original_draw_crop_min_border_span_1000 = 20
        try:
            self.tutorial_keyframe_red_box_thickness_ratio = max(
                0.0,
                float(self.tutorial_mode_config.get("keyframe_red_box_thickness_ratio", 0.01)),
            )
        except (TypeError, ValueError):
            self.tutorial_keyframe_red_box_thickness_ratio = 0.01
        self.tutorial_keyframe_skip_post_draw_processing = bool(
            self.tutorial_mode_config.get("keyframe_skip_post_draw_processing", True)
        )
        self.tutorial_grid_anchor_enabled = bool(
            self.tutorial_mode_config.get("grid_anchor_enabled", False)
        )
        try:
            self.tutorial_grid_rows = max(
                2,
                int(self.tutorial_mode_config.get("grid_rows", 20)),
            )
        except (TypeError, ValueError):
            self.tutorial_grid_rows = 20
        try:
            self.tutorial_grid_cols = max(
                2,
                int(self.tutorial_mode_config.get("grid_cols", 20)),
            )
        except (TypeError, ValueError):
            self.tutorial_grid_cols = 20
        try:
            self.tutorial_grid_overlay_alpha = max(
                0.1,
                min(0.8, float(self.tutorial_mode_config.get("grid_overlay_alpha", 0.38))),
            )
        except (TypeError, ValueError):
            self.tutorial_grid_overlay_alpha = 0.38
        try:
            self.tutorial_grid_overlay_line_thickness = max(
                1,
                min(2, int(self.tutorial_mode_config.get("grid_overlay_line_thickness", 1))),
            )
        except (TypeError, ValueError):
            self.tutorial_grid_overlay_line_thickness = 1
        self.tutorial_grid_overlay_variant = str(
            self.tutorial_mode_config.get("grid_overlay_variant", "full") or "full"
        ).strip()
        try:
            self.tutorial_grid_crop_expand_ratio = max(
                0.0,
                float(self.tutorial_mode_config.get("grid_crop_expand_ratio", 0.15)),
            )
        except (TypeError, ValueError):
            self.tutorial_grid_crop_expand_ratio = 0.15
        try:
            self.tutorial_grid_crop_min_border_px = max(
                0,
                int(self.tutorial_mode_config.get("grid_crop_min_border_px", 12)),
            )
        except (TypeError, ValueError):
            self.tutorial_grid_crop_min_border_px = 12
        tutorial_grid_anchor_key = _resolve_prompt_key_attr(
            "VL_VIDEO_ANALYSIS_GRID_SPATIAL_ANCHOR",
            _DEFAULT_GRID_SPATIAL_ANCHOR_KEY,
        )
        self._tutorial_grid_anchor_prompt = _safe_get_prompt(
            tutorial_grid_anchor_key,
            fallback=_load_package_prompt_default(
                _DEFAULT_GRID_SPATIAL_ANCHOR_REL_PATH,
                fallback=self._get_default_grid_spatial_anchor_prompt(),
            ),
        )
        raw_vl_arg_postprocess_config = self.tutorial_mode_config.get("vl_arg_postprocess", {})
        if isinstance(raw_vl_arg_postprocess_config, dict):
            self.vl_arg_postprocess_config = raw_vl_arg_postprocess_config
        else:
            self.vl_arg_postprocess_config = {}
        self.vl_arg_postprocess_enabled = bool(self.vl_arg_postprocess_config.get("enabled", True))
        self.vl_arg_postprocess_concrete_enabled = bool(
            self.vl_arg_postprocess_config.get("concrete_enabled", self.vl_arg_postprocess_enabled)
        )
        self.vl_arg_postprocess_model = str(
            self.vl_arg_postprocess_config.get("model", "deepseek-chat") or "deepseek-chat"
        ).strip() or "deepseek-chat"
        try:
            self.vl_arg_postprocess_max_main_operation_chars = max(
                200,
                int(self.vl_arg_postprocess_config.get("max_main_operation_chars", 4000)),
            )
        except (TypeError, ValueError):
            self.vl_arg_postprocess_max_main_operation_chars = 4000
        try:
            self.vl_arg_postprocess_max_main_content_chars = max(
                400,
                int(self.vl_arg_postprocess_config.get("max_main_content_chars", 8000)),
            )
        except (TypeError, ValueError):
            self.vl_arg_postprocess_max_main_content_chars = 8000
        try:
            self.vl_arg_postprocess_max_context_chars = max(
                400,
                int(self.vl_arg_postprocess_config.get("max_context_chars", 8000)),
            )
        except (TypeError, ValueError):
            self.vl_arg_postprocess_max_context_chars = 8000
        try:
            self.vl_arg_postprocess_max_subtitle_lines = max(
                8,
                int(self.vl_arg_postprocess_config.get("max_subtitle_lines", 120)),
            )
        except (TypeError, ValueError):
            self.vl_arg_postprocess_max_subtitle_lines = 120
        try:
            self.vl_arg_postprocess_retry_max_attempts = max(
                1,
                int(self.vl_arg_postprocess_config.get("retry_max_attempts", 3)),
            )
        except (TypeError, ValueError):
            self.vl_arg_postprocess_retry_max_attempts = 3
        try:
            self.vl_arg_postprocess_retry_initial_backoff_sec = max(
                0.1,
                float(self.vl_arg_postprocess_config.get("retry_initial_backoff_sec", 1.0)),
            )
        except (TypeError, ValueError):
            self.vl_arg_postprocess_retry_initial_backoff_sec = 1.0
        try:
            self.vl_arg_postprocess_retry_backoff_multiplier = max(
                1.0,
                float(self.vl_arg_postprocess_config.get("retry_backoff_multiplier", 2.0)),
            )
        except (TypeError, ValueError):
            self.vl_arg_postprocess_retry_backoff_multiplier = 2.0
        try:
            self.vl_arg_postprocess_retry_max_backoff_sec = max(
                self.vl_arg_postprocess_retry_initial_backoff_sec,
                float(self.vl_arg_postprocess_config.get("retry_max_backoff_sec", 8.0)),
            )
        except (TypeError, ValueError):
            self.vl_arg_postprocess_retry_max_backoff_sec = 8.0
        try:
            self.vl_arg_postprocess_retry_jitter_sec = max(
                0.0,
                float(self.vl_arg_postprocess_config.get("retry_jitter_sec", 0.2)),
            )
        except (TypeError, ValueError):
            self.vl_arg_postprocess_retry_jitter_sec = 0.2
        vl_arg_structured_system_key = _resolve_prompt_key_attr(
            "DEEPSEEK_VL_ARG_STRUCTURED_SYSTEM",
            _DEFAULT_VL_ARG_STRUCTURED_SYSTEM_KEY,
        )
        self._vl_arg_structured_system_prompt = _safe_get_prompt(
            vl_arg_structured_system_key,
            fallback=_load_package_prompt_default(
                _DEFAULT_VL_ARG_STRUCTURED_SYSTEM_REL_PATH,
                fallback=self._get_default_vl_arg_structured_system_prompt(),
            ),
        )
        vl_arg_structured_user_key = _resolve_prompt_key_attr(
            "DEEPSEEK_VL_ARG_STRUCTURED_USER",
            _DEFAULT_VL_ARG_STRUCTURED_USER_KEY,
        )
        self._vl_arg_structured_user_prompt_template = _safe_get_prompt(
            vl_arg_structured_user_key,
            fallback=_load_package_prompt_default(
                _DEFAULT_VL_ARG_STRUCTURED_USER_REL_PATH,
                fallback=self._get_default_vl_arg_structured_user_prompt_template(),
            ),
        )

        # Control whether multi-step clip requests are merged
        self.merge_multistep_clip_requests = bool(config.get("merge_multistep_clip_requests", False))
        self._subtitle_repo_cache: Dict[str, SubtitleRepository] = {}
        self._current_subtitle_output_dir: str = ""

        # 可选复鐢?gRPC 侧的 ProcessPool（避免额澶?spawn 多套进程池）
        self._cv_executor = cv_executor
        
        # 延迟初始化分析器（避免不使用时加载）
        self._analyzer = None
        
        logger.info(f"VLMaterialGenerator 初始化完鎴? enabled={self.enabled}")

    def _get_cached_visual_extractor(self, video_path: str):
        """
        获取或创建按 video_path 复用鐨?VisualFeatureExtractor銆?

        目的：减灏?screenshot optimization 热路径中的重复构建銆?
        """
        use_cache = bool(self.screenshot_config.get("reuse_visual_extractor", True))
        if not use_cache:
            from services.python_grpc.src.content_pipeline.phase2a.vision.visual_feature_extractor import VisualFeatureExtractor
            return VisualFeatureExtractor(video_path)

        with self._visual_extractor_cache_lock:
            extractor = self._visual_extractor_cache.get(video_path)
            if extractor is None:
                from services.python_grpc.src.content_pipeline.phase2a.vision.visual_feature_extractor import VisualFeatureExtractor
                extractor = VisualFeatureExtractor(video_path)
                self._visual_extractor_cache[video_path] = extractor
            return extractor
    
    @property
    def analyzer(self):
        """延迟初始化 VL 分析器。"""
        if self._analyzer is None:
            from services.python_grpc.src.content_pipeline.phase2a.materials.vl_video_analyzer import VLVideoAnalyzer
            self._analyzer = VLVideoAnalyzer(self.config)
        return self._analyzer

    async def close(self):
        """显式释放 VL 分析器资源。"""
        if self._analyzer is None:
            return
        try:
            await self._analyzer.close()
        finally:
            self._analyzer = None
    
    def is_enabled(self) -> bool:
        """检查是否启鐢?VL 素材生成"""
        return self.enabled
    
    def _get_cache_path(self, video_path: str, output_dir: str = None) -> Path:
        """获取VL结果缓存文件路径"""
        if output_dir:
            cache_dir = Path(output_dir)
        else:
            cache_dir = Path(video_path).parent
        
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cache_dir / "vl_analysis_cache.json"
    
    def _save_vl_results(
        self,
        cache_path: Path,
        analysis_results: List[Any],
        task_metadata: List[Dict[str, Any]],
        screenshot_requests: List[Dict[str, Any]],
        clip_requests: List[Dict[str, Any]]
    ) -> None:
        """保存VL分析结果到JSON文件"""
        try:
            # 序列化分析结鏋?
            serialized_results = self._serialize_unit_analysis_outputs(
                analysis_results=analysis_results,
                task_metadata=task_metadata,
            )
            
            cache_data = {
                "version": "1.0",
                "timestamp": str(Path(cache_path).stat().st_mtime) if cache_path.exists() else "",
                "analysis_results": serialized_results,
                "aggregated_screenshots": screenshot_requests,
                "aggregated_clips": clip_requests,
                "total_units": len(analysis_results),
                "successful_units": sum(1 for r in serialized_results if r.get("success", False))
            }
            
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"鉁?VL 分析结果已保存到缓存: {cache_path}")
            logger.info(f"   - 总单元数: {cache_data['total_units']}")
            logger.info(f"   - 成功单元: {cache_data['successful_units']}")
            logger.info(f"   - 截图请求: {len(screenshot_requests)}")
            logger.info(f"   - 视频片段: {len(clip_requests)}")
            
        except Exception as e:
            logger.warning(f"保存VL结果缓存失败: {e}")

    def _build_phase2a_token_cost_records(
        self,
        *,
        unit_analysis_outputs: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """从 Phase2A 单元输出中抽取 canonical token/cost 审计记录。"""

        records: List[Dict[str, Any]] = []
        for unit_output in unit_analysis_outputs or []:
            if not isinstance(unit_output, dict):
                continue
            unit_id = str(unit_output.get("unit_id", "") or "").strip()
            analysis_mode = str(unit_output.get("analysis_mode", "") or "").strip().lower() or "default"
            interactions = unit_output.get("raw_llm_interactions", []) or []
            if not isinstance(interactions, list):
                continue
            for index, interaction in enumerate(interactions, start=1):
                if not isinstance(interaction, dict):
                    continue
                request = interaction.get("request", {}) if isinstance(interaction.get("request", {}), dict) else {}
                response = interaction.get("response", {}) if isinstance(interaction.get("response", {}), dict) else {}
                model_name = str(
                    request.get("model", response.get("model", "")) or response.get("model", request.get("model", ""))
                ).strip()
                token_usage = normalize_usage_payload(response.get("usage", {}))
                cost_estimate = build_token_cost_estimate(
                    usage=token_usage,
                    model=model_name,
                    timestamp_utc=interaction.get("timestamp_utc", ""),
                    local_cache_hit=bool(response.get("cache_hit", False)),
                )
                records.append(
                    {
                        "record_index": len(records) + 1,
                        "unit_id": unit_id,
                        "analysis_mode": analysis_mode,
                        "interaction_index": index,
                        "stage": str(interaction.get("stage", "") or ""),
                        "attempt": int(safe_float(interaction.get("attempt", index), float(index))),
                        "success": bool(interaction.get("success", False)),
                        "timestamp_utc": str(interaction.get("timestamp_utc", "") or ""),
                        "model": model_name,
                        "provider": str(cost_estimate.get("provider", "") or ""),
                        "token_usage": token_usage,
                        "cost_estimate": cost_estimate,
                        "request": {
                            "temperature": request.get("temperature"),
                            "max_tokens": request.get("max_tokens"),
                            "analysis_mode": request.get("analysis_mode"),
                            "video_path": request.get("video_path"),
                            "timeout_sec": request.get("timeout_sec"),
                            "hedge_delay_ms": request.get("hedge_delay_ms"),
                            "offline_task_enabled": request.get("offline_task_enabled"),
                        },
                        "response": {
                            "finish_reason": response.get("finish_reason"),
                            "cache_hit": bool(response.get("cache_hit", False)),
                            "error": str(interaction.get("error", "") or interaction.get("error_raw", "") or ""),
                        },
                    }
                )
        return records

    def _write_phase2a_token_cost_audit(
        self,
        *,
        output_dir: str,
        token_stats: Dict[str, Any],
        unit_analysis_outputs: List[Dict[str, Any]],
        video_path: str,
    ) -> str:
        """把 Phase2A 的 token/cost 审计统一落盘到 intermediates。"""

        output_text = str(output_dir or "").strip()
        if not output_text:
            return ""

        try:
            audit_records = self._build_phase2a_token_cost_records(
                unit_analysis_outputs=unit_analysis_outputs,
            )
            summary = summarize_token_cost_records(audit_records)
            payload = {
                "version": "1.0",
                "scene": "phase2a_vl",
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "video_path": str(video_path or ""),
                "pricing_snapshot": get_token_pricing_snapshot(),
                "task_token_stats": dict(token_stats or {}),
                "summary": summary,
                "records": audit_records,
            }
            audit_path = Path(output_text) / "intermediates" / "phase2a_token_cost_audit.json"
            audit_path.parent.mkdir(parents=True, exist_ok=True)
            with open(audit_path, "w", encoding="utf-8") as file_obj:
                json.dump(payload, file_obj, ensure_ascii=False, indent=2)
            logger.info(
                "[VL-TokenAudit] phase2a token cost audit saved: path=%s, records=%s, priced=%s",
                audit_path,
                int(summary.get("total_records", 0) or 0),
                int(summary.get("priced_records", 0) or 0),
            )
            return str(audit_path)
        except Exception as exc:
            logger.warning("[VL-TokenAudit] failed to write phase2a token cost audit: %s", exc)
            return ""

    def _serialize_unit_analysis_outputs(
        self,
        *,
        analysis_results: List[Any],
        task_metadata: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        serialized_results: List[Dict[str, Any]] = []
        for idx, result in enumerate(analysis_results):
            meta = task_metadata[idx] if idx < len(task_metadata) else {}
            unit_id = str(meta.get("unit_id", f"task_{idx}") or f"task_{idx}")
            if isinstance(result, Exception):
                serialized_results.append(
                    {
                        "unit_id": unit_id,
                        "success": False,
                        "error": str(result),
                        "analysis_mode": str(meta.get("analysis_mode", "") or "").strip().lower() or "default",
                        "raw_response_json": [],
                        "raw_llm_interactions": [],
                        "clip_requests": [],
                        "screenshot_requests": [],
                        "metadata": meta,
                    }
                )
                continue
            serialized_results.append(
                {
                    "unit_id": unit_id,
                    "success": bool(getattr(result, "success", False)),
                    "error_msg": str(getattr(result, "error_msg", "") or ""),
                    "analysis_mode": str(getattr(result, "analysis_mode", "") or "").strip().lower() or "default",
                    "raw_response_json": getattr(result, "raw_response_json", []) or [],
                    "raw_llm_interactions": getattr(result, "raw_llm_interactions", []) or [],
                    "clip_requests": getattr(result, "clip_requests", []) or [],
                    "screenshot_requests": getattr(result, "screenshot_requests", []) or [],
                    "metadata": meta,
                }
            )
        return serialized_results

    def _load_vl_results(self, cache_path: Path) -> Optional[Dict[str, Any]]:
        """从JSON文件加载VL分析结果"""
        try:
            if not cache_path.exists():
                return None
            
            with open(cache_path, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            logger.info(f"鉁?从缓存加载VL分析结果: {cache_path}")
            logger.info(f"   - 缓存版本: {cache_data.get('version', 'unknown')}")
            logger.info(f"   - 总单元数: {cache_data.get('total_units', 0)}")
            logger.info(f"   - 成功单元: {cache_data.get('successful_units', 0)}")
            logger.info(f"   - 截图请求: {len(cache_data.get('aggregated_screenshots', []))}")
            logger.info(f"   - 视频片段: {len(cache_data.get('aggregated_clips', []))}")
            
            return cache_data
            
        except Exception as e:
            logger.warning(f"加载VL结果缓存失败: {e}")
            return None

    def _resolve_pre_vl_parallel_workers(self, unit_count: int) -> int:
        """
        解析 VL 前预处理并发度銆?        优先级：
        1) 复用注入鐨?CV 进程姹?max_workers锛?        2) 使用 pre_vl_static_pruning.parallel_workers（auto/整数）；
        3) 最终受 parallel_hard_cap 与任务数双重限制銆?        """
        if unit_count <= 0:
            return 1

        if self._cv_executor is not None:
            injected_workers = getattr(self._cv_executor, "_max_workers", None)
            if isinstance(injected_workers, int) and injected_workers > 0:
                return max(1, min(injected_workers, unit_count))

        raw_value = self.pre_vl_parallel_workers
        desired_workers = 1
        if isinstance(raw_value, int):
            desired_workers = raw_value
        else:
            config_value = str(raw_value).strip().lower()
            if config_value in {"", "auto"}:
                desired_workers = max(1, (os.cpu_count() or 2) - 1)
            else:
                try:
                    desired_workers = int(config_value)
                except (TypeError, ValueError):
                    desired_workers = 1

        return max(1, min(desired_workers, self.pre_vl_parallel_hard_cap, unit_count))

    def _resolve_vl_parallel_workers(self, unit_count: int) -> int:
        """
        解析 VL 主分析并发度（语义单元级）銆?        规则锛?        1) `parallel_workers=auto` 默认鎸?unit_count 全量并发锛?        2) 可通过整数显式限制并发锛?        3) 最终受 `parallel_hard_cap` 与任务数双重限制銆?        """
        if unit_count <= 0:
            return 1

        raw_value = self.vl_parallel_workers
        cpu_count = max(1, int(os.cpu_count() or 4))
        desired_workers = max(2, min(4, max(1, cpu_count // 2)))
        if isinstance(raw_value, int):
            desired_workers = raw_value
        else:
            config_value = str(raw_value).strip().lower()
            if config_value not in {"", "auto"}:
                try:
                    desired_workers = int(config_value)
                except (TypeError, ValueError):
                    desired_workers = max(2, min(4, max(1, cpu_count // 2)))

        return max(1, min(desired_workers, self.vl_parallel_hard_cap, unit_count))

    def _resolve_stream_split_process_workers(self, unit_count: int) -> int:
        """解析“单元切片独立进程池”的并发度。"""
        if unit_count <= 0:
            return 1

        raw_value = self.stream_split_process_workers
        desired_workers = 1
        if isinstance(raw_value, int):
            desired_workers = raw_value
        else:
            config_value = str(raw_value or "").strip().lower()
            if config_value in {"", "auto"}:
                cpu_count = max(1, int(os.cpu_count() or 4))
                desired_workers = max(1, min(2, max(1, cpu_count // 4)))
            else:
                try:
                    desired_workers = int(config_value)
                except (TypeError, ValueError):
                    desired_workers = 1

        return max(1, min(desired_workers, self.stream_split_process_hard_cap, unit_count))

    def _should_stream_pipeline_for_unit(self, semantic_unit: Dict[str, Any]) -> bool:
        """判断语义单元是否进入“流式切片+分析”链路。"""
        knowledge_type = self._normalize_pre_vl_pruning_knowledge_type(
            semantic_unit.get("knowledge_type", "")
        )
        return knowledge_type in set(self.stream_pipeline_knowledge_types or set())

    def _create_stream_split_executor(self, *, worker_count: int) -> ThreadPoolExecutor:
        """
        创建 unit 流式切片执行器。

        切片阶段的实际重活已经在 `_split_single_unit_clip_worker` 内部通过独立
        `python + ffmpeg` 子进程完成；外层再套一层 `ProcessPoolExecutor` 会让
        Windows `spawn` 反复重导 `vl_material_generator.py` 整个大模块，显著放大
        原生依赖初始化与进程级崩溃风险。这里改为线程池，既保留并发切片能力，
        又避免重复 spawn 大进程。
        """
        return ThreadPoolExecutor(max_workers=max(1, int(worker_count or 1)))

    def _build_vl_unit_task_blueprints(
        self,
        semantic_units: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        构建待分析 unit 的基础任务（不含 clip_path）。
        目的：将“任务属性推导”与“视频切片来源”解耦，便于复用到批量与流式链路。
        """
        unit_tasks: List[Dict[str, Any]] = []
        for semantic_unit in semantic_units or []:
            if not isinstance(semantic_unit, dict):
                continue
            unit_id = str(semantic_unit.get("unit_id", "") or "").strip()
            if not unit_id:
                continue

            start_sec = float(semantic_unit.get("start_sec", 0) or 0)
            end_sec = float(semantic_unit.get("end_sec", 0) or 0)
            duration = max(0.0, end_sec - start_sec)

            extra_prompt = None
            analysis_mode = "default"
            mode_override = str(
                semantic_unit.get("_vl_analysis_mode_override", semantic_unit.get("vl_analysis_mode_override", ""))
                or ""
            ).strip().lower()
            if mode_override in {"concrete", "concrete_focus"}:
                analysis_mode = "concrete"
            elif mode_override in {"tutorial", "tutorial_stepwise"}:
                analysis_mode = "tutorial_stepwise"
            elif self._is_tutorial_process_unit(semantic_unit, duration):
                analysis_mode = "tutorial_stepwise"
                extra_prompt = self._build_tutorial_extra_prompt()

            unit_tasks.append(
                {
                    "semantic_unit": semantic_unit,
                    "analysis_mode": analysis_mode,
                    "extra_prompt": extra_prompt,
                    "unit_id": unit_id,
                    "start_sec": start_sec,
                    "end_sec": end_sec,
                    "duration": duration,
                }
            )
        return unit_tasks

    def _normalize_bool_flag(self, value: Any) -> bool:
        """将多种布尔表示统一归一为 bool。"""
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return value != 0
        text = str(value or "").strip().lower()
        return text in {"1", "true", "yes", "y", "on", "是", "是的"}

    def _normalize_should_type(self, value: Any) -> str:
        """归一 should_type 路由覆盖类型。"""
        text = str(value or "").strip().lower()
        if text in {"abstract", "抽象", "讲解", "explanation"}:
            return "abstract"
        if text in {"concrete", "具象", "具体", "实例"}:
            return "concrete"
        return ""

    def _normalize_pre_vl_pruning_knowledge_type(self, value: Any) -> str:
        text = str(value or "").strip().lower()
        if text in {"process", "process_short", "process_long"}:
            return "process"
        if self._normalize_should_type(text) == "concrete":
            return "concrete"
        return text

    def _is_pre_vl_pruning_enabled_for_knowledge_type(self, knowledge_type: Any) -> bool:
        normalized = self._normalize_pre_vl_pruning_knowledge_type(knowledge_type)
        if normalized == "process":
            return bool(self.pre_vl_process_pruning_enabled)
        if normalized == "concrete":
            return bool(self.pre_vl_concrete_pruning_enabled)
        return bool(self.pre_vl_pruning_enabled)

    def _analysis_result_has_no_needed_video(self, analysis_result: Any) -> bool:
        """判断 VL 结果是否标记“该语义单元不需要视频表达”."""
        parsed_items = getattr(analysis_result, "analysis_results", []) or []
        for item in parsed_items:
            if self._normalize_bool_flag(getattr(item, "no_needed_video", False)):
                return True

        raw_items = getattr(analysis_result, "raw_response_json", []) or []
        for item in raw_items:
            if not isinstance(item, dict):
                continue
            raw_flag = item.get("no_needed_video", item.get("no_need_video", item.get("video_not_needed")))
            if self._normalize_bool_flag(raw_flag):
                return True
        return False

    def _analysis_result_should_type_override(self, analysis_result: Any) -> str:
        """提取 VL 结果的 should_type 路由覆盖（abstract 优先于 concrete）。"""
        parsed_items = getattr(analysis_result, "analysis_results", []) or []
        has_concrete = False
        for item in parsed_items:
            normalized = self._normalize_should_type(getattr(item, "should_type", ""))
            if normalized == "abstract":
                return "abstract"
            if normalized == "concrete":
                has_concrete = True

        raw_items = getattr(analysis_result, "raw_response_json", []) or []
        for item in raw_items:
            if not isinstance(item, dict):
                continue
            normalized = self._normalize_should_type(
                item.get("should_type", item.get("target_type", item.get("suggested_type")))
            )
            if normalized == "abstract":
                return "abstract"
            if normalized == "concrete":
                has_concrete = True

        return "concrete" if has_concrete else ""

    def _mark_semantic_unit_knowledge_type(
        self,
        semantic_unit: Dict[str, Any],
        *,
        knowledge_type: str,
        reason: str,
        no_needed_video: bool = False,
    ) -> None:
        """将语义单元回写为指定知识类型，并记录 VL 路由来源。"""
        if not isinstance(semantic_unit, dict):
            return
        normalized_type = self._normalize_should_type(knowledge_type) or str(knowledge_type or "").strip().lower()
        if normalized_type not in {"abstract", "concrete", "process"}:
            return
        semantic_unit["knowledge_type"] = normalized_type
        semantic_unit["_vl_route_override"] = normalized_type
        semantic_unit["_vl_route_reason"] = str(reason or "").strip() or "vl_route_override"
        semantic_unit["_vl_no_needed_video"] = bool(no_needed_video)
        if no_needed_video:
            semantic_unit["_vl_no_needed_video_reason"] = "vl_no_needed_video_true"

    @staticmethod
    def _merge_token_usage(*token_usages: Any) -> Dict[str, int]:
        prompt_tokens = 0
        completion_tokens = 0
        total_tokens = 0
        for usage in token_usages:
            if not isinstance(usage, dict):
                continue
            prompt_value = int(safe_float(usage.get("prompt_tokens", 0), 0.0))
            completion_value = int(safe_float(usage.get("completion_tokens", 0), 0.0))
            total_value = int(
                safe_float(
                    usage.get("total_tokens", prompt_value + completion_value),
                    float(prompt_value + completion_value),
                )
            )
            prompt_tokens += max(0, prompt_value)
            completion_tokens += max(0, completion_value)
            total_tokens += max(0, total_value)
        return {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": total_tokens,
        }

    async def _rerun_should_type_concrete_with_concrete_mode(
        self,
        *,
        analysis_result: Any,
        meta: Dict[str, Any],
    ) -> tuple[Any, bool]:
        if not bool(getattr(analysis_result, "success", False)):
            return analysis_result, False
        if self._analysis_result_has_no_needed_video(analysis_result):
            return analysis_result, False
        if self._analysis_result_should_type_override(analysis_result) != "concrete":
            return analysis_result, False

        current_mode = str(
            meta.get("analysis_mode", getattr(analysis_result, "analysis_mode", "default")) or "default"
        ).strip().lower()
        if current_mode == "concrete":
            return analysis_result, False

        unit_id = str(meta.get("unit_id", "") or "").strip() or "UNKNOWN"
        clip_path = str(meta.get("vl_clip_path", meta.get("clip_path", "")) or "").strip()
        if not clip_path:
            logger.warning(
                "[VL] unit=%s should_type=concrete but no clip path found, skip concrete rerun",
                unit_id,
            )
            return analysis_result, False

        extra_prompt_raw = meta.get("extra_prompt")
        extra_prompt: Optional[str]
        if isinstance(extra_prompt_raw, str):
            extra_prompt = extra_prompt_raw.strip() or None
        else:
            extra_prompt = None

        logger.info(
            "[VL] unit=%s should_type=concrete detected, rerun with concrete mode",
            unit_id,
        )
        try:
            concrete_result = await self.analyzer.analyze_clip(
                clip_path=clip_path,
                semantic_unit_start_sec=safe_float(meta.get("start_sec", 0.0), 0.0),
                semantic_unit_id=unit_id,
                extra_prompt=extra_prompt,
                analysis_mode="concrete",
            )
        except Exception as error:
            logger.warning(
                "[VL] unit=%s concrete rerun failed with exception, keep original result: %s",
                unit_id,
                error,
            )
            return analysis_result, False

        if not bool(getattr(concrete_result, "success", False)):
            logger.warning(
                "[VL] unit=%s concrete rerun failed, keep original result: %s",
                unit_id,
                str(getattr(concrete_result, "error_msg", "") or "").strip(),
            )
            return analysis_result, False

        merged_token_usage = self._merge_token_usage(
            getattr(analysis_result, "token_usage", {}) or {},
            getattr(concrete_result, "token_usage", {}) or {},
        )
        old_interactions = getattr(analysis_result, "raw_llm_interactions", []) or []
        new_interactions = getattr(concrete_result, "raw_llm_interactions", []) or []
        merged_interactions: List[Any] = []
        if isinstance(old_interactions, list):
            merged_interactions.extend(list(old_interactions))
        if isinstance(new_interactions, list):
            merged_interactions.extend(list(new_interactions))
        if not merged_interactions and isinstance(new_interactions, list):
            merged_interactions = list(new_interactions)

        try:
            analysis_result.success = bool(getattr(concrete_result, "success", False))
            analysis_result.error_msg = str(getattr(concrete_result, "error_msg", "") or "")
            analysis_result.analysis_results = list(getattr(concrete_result, "analysis_results", []) or [])
            analysis_result.clip_requests = list(getattr(concrete_result, "clip_requests", []) or [])
            analysis_result.screenshot_requests = list(getattr(concrete_result, "screenshot_requests", []) or [])
            analysis_result.token_usage = merged_token_usage
            analysis_result.analysis_mode = "concrete"
            analysis_result.raw_response_json = list(getattr(concrete_result, "raw_response_json", []) or [])
            analysis_result.raw_llm_interactions = merged_interactions
        except Exception:
            concrete_result.token_usage = merged_token_usage
            concrete_result.analysis_mode = "concrete"
            concrete_result.raw_llm_interactions = merged_interactions
            analysis_result = concrete_result

        meta["analysis_mode"] = "concrete"

        semantic_unit = meta.get("semantic_unit", {})
        if isinstance(semantic_unit, dict):
            semantic_unit["_vl_analysis_mode_override"] = "concrete"
            self._mark_semantic_unit_knowledge_type(
                semantic_unit,
                knowledge_type="concrete",
                reason="vl_should_type_concrete_reanalyze",
                no_needed_video=False,
            )

        logger.info(
            "[VL] unit=%s concrete rerun success: clips=%s, screenshots=%s",
            unit_id,
            len(list(getattr(analysis_result, "clip_requests", []) or [])),
            len(list(getattr(analysis_result, "screenshot_requests", []) or [])),
        )
        return analysis_result, True

    def _resolve_vl_dispatch_lane(
        self,
        *,
        task_input: Dict[str, Any],
        task_meta: Dict[str, Any],
    ) -> str:
        mode = str(task_input.get("analysis_mode", "") or "").strip().lower()
        semantic_unit = task_meta.get("semantic_unit", {}) if isinstance(task_meta, dict) else {}
        knowledge_type = ""
        if isinstance(semantic_unit, dict):
            knowledge_type = str(semantic_unit.get("knowledge_type", "") or "").strip().lower()

        if mode == "concrete" or knowledge_type == "concrete":
            return "concrete"
        if mode == "tutorial_stepwise" or knowledge_type == "process":
            return "process"
        return "default"

    def _build_vl_dispatch_order(
        self,
        *,
        task_inputs: List[Dict[str, Any]],
        task_metadata: List[Dict[str, Any]],
    ) -> List[int]:
        if not task_inputs:
            return []

        lane_queues: Dict[str, deque] = {
            "concrete": deque(),
            "process": deque(),
            "default": deque(),
        }
        for index, task_input in enumerate(task_inputs):
            task_meta = task_metadata[index] if index < len(task_metadata) else {}
            lane = self._resolve_vl_dispatch_lane(task_input=task_input, task_meta=task_meta)
            lane_queues.setdefault(lane, deque()).append(index)

        lane_order = ["concrete", "process", "default"]
        lane_order.extend(
            sorted([lane for lane in lane_queues.keys() if lane not in {"concrete", "process", "default"}])
        )

        dispatch_order: List[int] = []
        while True:
            has_item = False
            for lane in lane_order:
                queue = lane_queues.get(lane)
                if not queue:
                    continue
                dispatch_order.append(int(queue.popleft()))
                has_item = True
            if not has_item:
                break
        return dispatch_order

    async def _analyze_unit_tasks_in_parallel(
        self,
        *,
        unit_tasks: List[Dict[str, Any]],
        pre_prune_results: List[Dict[str, Any]],
        on_result: Optional[Callable[[int, Dict[str, Any], Any], Awaitable[None] | None]] = None,
    ) -> Tuple[List[Any], List[Dict[str, Any]], int]:
        """
        按语义单元并行执琛?VL 分析銆?
        返回锛?        - analysis_results: 涓?task_metadata 索引对齐的分析结果（含异常对象）
        - task_metadata: 每个 unit 的上下文元数鎹?        - pruned_units: 命中 pre-prune 的单元数
        """
        if not unit_tasks:
            return [], [], 0

        task_inputs: List[Dict[str, Any]] = []
        task_metadata: List[Dict[str, Any]] = []
        pruned_units = 0

        for index, unit_task in enumerate(unit_tasks):
            pre_prune_info = (
                pre_prune_results[index]
                if index < len(pre_prune_results)
                else self._build_default_pre_prune_info(
                    semantic_unit=unit_task.get("semantic_unit", {}),
                    clip_path=str(unit_task.get("clip_path", "") or ""),
                )
            )
            unit_id = unit_task["unit_id"]
            start_sec = unit_task["start_sec"]
            end_sec = unit_task["end_sec"]
            duration = unit_task["duration"]
            clip_path = unit_task["clip_path"]
            analysis_mode = unit_task["analysis_mode"]
            extra_prompt = unit_task["extra_prompt"]

            clip_path_for_vl = self._resolve_vl_analysis_clip_path(
                original_clip_path=clip_path,
                preferred_clip_path=pre_prune_info.get("clip_path_for_vl", clip_path),
            )

            if pre_prune_info.get("applied"):
                pruned_units += 1

            pre_context_prompt = str(pre_prune_info.get("pre_context_prompt", "") or "").strip()
            if pre_context_prompt:
                if extra_prompt:
                    extra_prompt = extra_prompt + "\n\n" + pre_context_prompt
                else:
                    extra_prompt = pre_context_prompt

            task_inputs.append(
                {
                    "clip_path": clip_path_for_vl,
                    "semantic_unit_start_sec": start_sec,
                    "semantic_unit_id": unit_id,
                    "extra_prompt": extra_prompt,
                    "analysis_mode": analysis_mode,
                }
            )
            task_metadata.append(
                {
                    "unit_id": unit_id,
                    "semantic_unit": unit_task.get("semantic_unit", {}),
                    "start_sec": start_sec,
                    "end_sec": end_sec,
                    "unit_duration": duration,
                    "clip_path": clip_path,
                    "vl_clip_path": clip_path_for_vl,
                    "pre_prune": pre_prune_info,
                    "analysis_mode": analysis_mode,
                    "extra_prompt": extra_prompt,
                }
            )

        worker_count = self._resolve_vl_parallel_workers(len(task_inputs))
        dispatch_order = self._build_vl_dispatch_order(
            task_inputs=task_inputs,
            task_metadata=task_metadata,
        )
        dispatch_inputs = [task_inputs[index] for index in dispatch_order]
        logger.info(
            f"[VL-UnitParallel] start: units={len(task_inputs)}, workers={worker_count}, policy=one-unit-one-api"
        )
        if dispatch_order:
            lane_counts: Dict[str, int] = {"concrete": 0, "process": 0, "default": 0}
            for index in dispatch_order:
                lane = self._resolve_vl_dispatch_lane(
                    task_input=task_inputs[index],
                    task_meta=task_metadata[index] if index < len(task_metadata) else {},
                )
                lane_counts[lane] = int(lane_counts.get(lane, 0)) + 1
            logger.info(
                "[VL-UnitParallel] dispatch lanes: concrete=%s, process=%s, default=%s",
                lane_counts.get("concrete", 0),
                lane_counts.get("process", 0),
                lane_counts.get("default", 0),
            )

        analysis_results: List[Any] = [RuntimeError("vl_dispatch_result_missing") for _ in task_inputs]
        streamed_indexes: set[int] = set()
        pending_callback_tasks: set[asyncio.Task] = set()
        callback_semaphore = asyncio.Semaphore(max(1, worker_count))

        async def _invoke_on_result_callback(original_index: int, result_item: Any) -> None:
            if on_result is None:
                return
            async with callback_semaphore:
                try:
                    callback_result = on_result(
                        original_index,
                        task_metadata[original_index] if original_index < len(task_metadata) else {},
                        result_item,
                    )
                    if asyncio.isfuture(callback_result) or asyncio.iscoroutine(callback_result):
                        await callback_result
                except Exception as callback_error:
                    logger.warning(
                        "[VL-UnitParallel] on_result callback failed: index=%s, unit=%s, err=%s",
                        original_index,
                        (
                            task_metadata[original_index].get("unit_id", "")
                            if original_index < len(task_metadata)
                            else ""
                        ),
                        callback_error,
                    )

        async def _emit_stream_result(original_index: int, result_item: Any) -> None:
            analysis_results[original_index] = result_item
            if on_result is None:
                return
            if original_index in streamed_indexes:
                return
            streamed_indexes.add(original_index)
            callback_task = asyncio.create_task(
                _invoke_on_result_callback(original_index, result_item)
            )
            pending_callback_tasks.add(callback_task)
            callback_task.add_done_callback(lambda finished: pending_callback_tasks.discard(finished))

        batch_runner = getattr(self.analyzer, "analyze_clips_batch", None)
        if callable(batch_runner):
            async def _dispatch_result_callback(dispatch_index: int, result_item: Any) -> None:
                if dispatch_index >= len(dispatch_order):
                    return
                original_index = dispatch_order[dispatch_index]
                await _emit_stream_result(original_index, result_item)

            batch_params: Dict[str, Any] = {
                "tasks": dispatch_inputs,
                "max_inflight": worker_count,
                "return_exceptions": True,
            }
            try:
                if "result_callback" in inspect.signature(batch_runner).parameters:
                    batch_params["result_callback"] = _dispatch_result_callback
            except Exception:
                pass

            dispatch_results = await batch_runner(**batch_params)
            if not isinstance(dispatch_results, list):
                raise RuntimeError("analyze_clips_batch returned non-list result")
            if len(dispatch_results) != len(dispatch_inputs):
                logger.warning(
                    "[VL-UnitParallel] batch result size mismatch: expected=%s, actual=%s",
                    len(dispatch_inputs),
                    len(dispatch_results),
                )
                if len(dispatch_results) < len(dispatch_inputs):
                    dispatch_results = list(dispatch_results) + [
                        RuntimeError("vl_batch_result_missing")
                    ] * (len(dispatch_inputs) - len(dispatch_results))
                else:
                    dispatch_results = list(dispatch_results)[: len(dispatch_inputs)]

            for dispatch_index, result_item in enumerate(dispatch_results):
                if dispatch_index >= len(dispatch_order):
                    break
                original_index = dispatch_order[dispatch_index]
                await _emit_stream_result(original_index, result_item)
        else:
            semaphore = asyncio.Semaphore(worker_count)

            async def _run_single(dispatch_index: int, task_input: Dict[str, Any]) -> Tuple[int, Any]:
                async with semaphore:
                    try:
                        result_item = await self.analyzer.analyze_clip(
                            clip_path=task_input["clip_path"],
                            semantic_unit_start_sec=task_input["semantic_unit_start_sec"],
                            semantic_unit_id=task_input["semantic_unit_id"],
                            extra_prompt=task_input.get("extra_prompt"),
                            analysis_mode=task_input.get("analysis_mode", "default"),
                        )
                    except Exception as exc:
                        result_item = exc
                return dispatch_index, result_item

            pending_dispatch_tasks = [
                asyncio.create_task(_run_single(dispatch_index, task_input))
                for dispatch_index, task_input in enumerate(dispatch_inputs)
            ]
            try:
                for done_task in asyncio.as_completed(pending_dispatch_tasks):
                    dispatch_index, result_item = await done_task
                    if dispatch_index >= len(dispatch_order):
                        continue
                    original_index = dispatch_order[dispatch_index]
                    await _emit_stream_result(original_index, result_item)
            finally:
                for pending_task in pending_dispatch_tasks:
                    if not pending_task.done():
                        pending_task.cancel()
        if pending_callback_tasks:
            await asyncio.gather(*list(pending_callback_tasks), return_exceptions=True)
        logger.info(
            f"[VL-UnitParallel] done: dispatched={len(task_inputs)}, results={len(analysis_results)}"
        )
        return analysis_results, task_metadata, pruned_units

    def _resolve_vl_analysis_clip_path(
        self,
        *,
        original_clip_path: str,
        preferred_clip_path: Optional[str] = None,
    ) -> str:
        """
        Resolve VL analysis input clip path with "pruned-first, original-fallback" policy.
        """
        original_text = str(original_clip_path or "").strip()
        preferred_text = str(preferred_clip_path or "").strip()
        if not original_text and not preferred_text:
            return ""

        preferred_path = Path(preferred_text) if preferred_text else None
        original_path = Path(original_text) if original_text else None

        def _exists(path_obj: Optional[Path]) -> bool:
            if path_obj is None:
                return False
            try:
                return path_obj.exists()
            except Exception:
                return False

        def _is_pruned_path(path_obj: Optional[Path]) -> bool:
            if path_obj is None:
                return False
            return "vl_pruned_clips" in {str(part).lower() for part in path_obj.parts}

        # 1) Explicitly provided pruned path wins.
        if _is_pruned_path(preferred_path) and _exists(preferred_path):
            return str(preferred_path)

        # 2) If a pruned sibling exists, prefer it over semantic_unit_clips_vl original clip.
        if original_path is not None:
            pruned_dir = original_path.parent / "vl_pruned_clips"
            if pruned_dir.exists():
                stem = original_path.stem
                pruned_candidates: List[Path] = [
                    pruned_dir / f"{stem}_pruned.mp4",
                    pruned_dir / f"{stem}_legacy_action_pruned.mp4",
                ]
                pruned_candidates.extend(sorted(pruned_dir.glob(f"{stem}*_pruned.mp4")))
                seen: set[str] = set()
                for candidate in pruned_candidates:
                    key = str(candidate).lower()
                    if key in seen:
                        continue
                    seen.add(key)
                    if _exists(candidate):
                        return str(candidate)

        # 3) Fallback to preferred then original.
        if _exists(preferred_path):
            return str(preferred_path)
        if _exists(original_path):
            return str(original_path)

        # Keep compatibility when all candidates are missing: preserve preferred input if present.
        return preferred_text or original_text

    def _should_use_pre_vl_process_mode(self, worker_count: int) -> bool:
        """
        判定 VL 前预处理是否启用多进程稳定段检测銆?        规则锛?        1) worker_count<=1 时不并栾?        2) parallel_mode=async/off/disabled 时关闭；
        3) parallel_mode=process 时强制开启；
        4) parallel_mode=auto 时仅在注鍏?cv_executor 时开启銆?        """
        if not self.pre_vl_process_stable_detect_enabled:
            return False

        if worker_count <= 1:
            return False

        mode = self.pre_vl_parallel_mode
        if mode in {"", "async", "off", "disabled", "false", "0"}:
            return False
        if mode == "process":
            return True
        if mode == "auto":
            return self._cv_executor is not None

        logger.warning(f"[VL-PrePrune] unknown parallel_mode={mode}, fallback to async mode")
        return False

    def _resolve_tutorial_asset_export_workers(self, step_count: int) -> int:
        """
        解析教程模式资产导出并发度銆?        """
        if step_count <= 0:
            return 1

        raw_value = self.tutorial_asset_export_parallel_workers
        desired_workers = 1
        if isinstance(raw_value, int):
            desired_workers = raw_value
        else:
            config_value = str(raw_value).strip().lower()
            if config_value in {"", "auto"}:
                desired_workers = max(1, min(4, os.cpu_count() or 2))
            else:
                try:
                    desired_workers = int(config_value)
                except (TypeError, ValueError):
                    desired_workers = 1

        return max(1, min(desired_workers, self.tutorial_asset_export_parallel_hard_cap, step_count))

    async def _detect_stable_islands_for_units_via_process_pool(
        self,
        *,
        unit_tasks: List[Dict[str, Any]],
        worker_count: int,
    ) -> List[Optional[List[Tuple[float, float]]]]:
        """
        使用进程池并行检娴?stable 区间銆?        返回涓?unit_tasks 一一对齐锛?        - List[Tuple[float,float]]: 成功结果（可为空列表锛?        - None: 任务异常，调用方可回退默认结果
        """
        if not unit_tasks:
            return []

        detect_t0 = time.perf_counter()

        from services.python_grpc.src.vision_validation.worker import (
            init_cv_worker,
            run_detect_stable_islands_task,
        )

        loop = asyncio.get_running_loop()
        executor = self._cv_executor
        created_executor = False
        if executor is None:
            executor = create_spawn_process_pool(max_workers=worker_count, initializer=init_cv_worker)
            created_executor = True

        try:
            futures = []
            for task in unit_tasks:
                semantic_unit = task.get("semantic_unit", {})
                unit_id = str(task.get("unit_id", semantic_unit.get("unit_id", "")) or "")
                clip_path = str(task.get("clip_path", "") or "")
                start_sec = safe_float(semantic_unit.get("start_sec", 0.0), 0.0)
                end_sec = safe_float(semantic_unit.get("end_sec", start_sec), start_sec)
                if end_sec < start_sec:
                    end_sec = start_sec
                duration_sec = max(0.0, end_sec - start_sec)

                futures.append(
                    loop.run_in_executor(
                        executor,
                        functools.partial(
                            run_detect_stable_islands_task,
                            clip_path=clip_path,
                            unit_id=unit_id,
                            duration_sec=duration_sec,
                        ),
                    )
                )

            raw_results = await asyncio.gather(*futures, return_exceptions=True)

            normalized_results: List[Optional[List[Tuple[float, float]]]] = []
            failed_count = 0
            for index, raw in enumerate(raw_results):
                if isinstance(raw, Exception):
                    task = unit_tasks[index]
                    semantic_unit = task.get("semantic_unit", {})
                    logger.warning(
                        f"[VL-PrePrune] process stable detect failed: unit={semantic_unit.get('unit_id', '')}, error={raw}"
                    )
                    normalized_results.append(None)
                    failed_count += 1
                    continue

                intervals: List[Tuple[float, float]] = []
                for seg in raw or []:
                    if not isinstance(seg, (tuple, list)) or len(seg) != 2:
                        continue
                    try:
                        seg_start = float(seg[0])
                        seg_end = float(seg[1])
                    except (TypeError, ValueError):
                        continue
                    intervals.append((seg_start, seg_end))
                normalized_results.append(self._normalize_intervals(intervals))

            detect_ms = (time.perf_counter() - detect_t0) * 1000.0
            logger.info(
                f"[VL-PrePrune] process stable detect done: units={len(unit_tasks)}, failed={failed_count}, ms={detect_ms:.1f}"
            )
            return normalized_results
        finally:
            if created_executor:
                executor.shutdown(wait=True)

    def _build_default_pre_prune_info(self, *, semantic_unit: Dict[str, Any], clip_path: str) -> Dict[str, Any]:
        """构建预处理失败或跳过时的默认返回。"""
        start_sec = safe_float(semantic_unit.get("start_sec", 0.0), 0.0)
        end_sec = safe_float(semantic_unit.get("end_sec", start_sec), start_sec)
        if end_sec < start_sec:
            end_sec = start_sec
        duration_sec = max(0.0, end_sec - start_sec)
        return {
            "applied": False,
            "materialized": False,
            "clip_path_for_vl": clip_path,
            "kept_segments": [(0.0, duration_sec)] if duration_sec > 0 else [],
            "removed_segments": [],
            "stable_intervals_raw": [],
            "pre_context_prompt": "",
        }

    def _parse_interval_pairs(self, raw_intervals: Any) -> List[Tuple[float, float]]:
        """
        将外部传入的区间列表统一解析涓?[(start_sec, end_sec)]銆?
        为什么要做：
        - `_routing_pre_prune` 来自上游路由阶段，字段类型可能是 list/tuple/dict 混合銆?        - 在复用前做一次结构归一化，可避免后续时间映射出现隐式类型错误銆?        """
        if not isinstance(raw_intervals, list):
            return []

        parsed: List[Tuple[float, float]] = []
        for item in raw_intervals:
            start_sec: Optional[float] = None
            end_sec: Optional[float] = None

            if isinstance(item, (list, tuple)) and len(item) >= 2:
                try:
                    start_sec = float(item[0])
                    end_sec = float(item[1])
                except (TypeError, ValueError):
                    start_sec, end_sec = None, None
            elif isinstance(item, dict):
                raw_start = item.get("start_sec", item.get("start"))
                raw_end = item.get("end_sec", item.get("end"))
                try:
                    start_sec = float(raw_start)
                    end_sec = float(raw_end)
                except (TypeError, ValueError):
                    start_sec, end_sec = None, None

            if start_sec is None or end_sec is None:
                continue
            if end_sec <= start_sec:
                continue
            parsed.append((start_sec, end_sec))

        return self._normalize_intervals(parsed)

    def _build_reusable_routing_pre_prune_info(
        self,
        *,
        semantic_unit: Dict[str, Any],
        clip_path: str,
    ) -> Optional[Dict[str, Any]]:
        """
        尝试复用路由阶段写入 `_routing_pre_prune` 的预处理结果銆?
        复用判定原则锛?        1) 必须是合法字典结构；
        2) 鑻?`applied=true`，则预裁剪片段路径必须存在，涓?kept_segments 合法锛?        3) 鑻?`applied=false`，统一回落为默认结果（原片段）以保证语义一致銆?        """
        routing_info = semantic_unit.get("_routing_pre_prune")
        if not isinstance(routing_info, dict) or not routing_info:
            return None

        default_result = self._build_default_pre_prune_info(
            semantic_unit=semantic_unit,
            clip_path=clip_path,
        )

        applied = bool(routing_info.get("applied", False))
        if not applied:
            return default_result

        # routing 侧可能只做“区间预处理”用于分流，不落盘 pruned clip。
        # 默认该结果不能直接复用到 VL 分析阶段（避免误把“未裁剪片段”当作 pruned 输入）。
        # 但若路由已明确标记本单元强制走 legacy-action 分支，则允许复用该预处理信息，
        # 以确保 short process 也能稳定触发静态主导降级逻辑。
        force_legacy_action = bool(semantic_unit.get("_routing_force_legacy_action", False))
        materialized = bool(routing_info.get("materialized", True))
        if not materialized and not force_legacy_action:
            return None

        clip_path_for_vl = str(routing_info.get("clip_path_for_vl", "") or "").strip()
        if not clip_path_for_vl:
            return None
        if not Path(clip_path_for_vl).exists():
            return None

        kept_segments = self._parse_interval_pairs(routing_info.get("kept_segments"))
        if not kept_segments:
            return None

        removed_segments = self._parse_interval_pairs(routing_info.get("removed_segments"))
        pre_context_prompt = str(routing_info.get("pre_context_prompt", "") or "").strip()

        return {
            "applied": True,
            "materialized": materialized,
            "clip_path_for_vl": clip_path_for_vl,
            "kept_segments": kept_segments,
            "removed_segments": removed_segments,
            "stable_intervals_raw": self._parse_interval_pairs(routing_info.get("stable_intervals_raw")),
            "pre_context_prompt": pre_context_prompt,
        }

    async def _resolve_pre_prune_results_for_unit_tasks(
        self,
        *,
        clips_dir: str,
        unit_tasks: List[Dict[str, Any]],
        force_preprocess: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        解析 unit_tasks 的预处理结果：优先复用路由结果，不可复用时回退重算銆?
        价值：
        - 避免鍦?AnalyzeWithVL 中重复执行稳定岛检测与 FFmpeg 预裁剪銆?        - 保持“复用失败自动回退”语义，不影响既有正确性銆?        """
        if not unit_tasks:
            return []

        reused_or_missing: List[Optional[Dict[str, Any]]] = []
        pending_tasks: List[Dict[str, Any]] = []
        for task in unit_tasks:
            semantic_unit = task.get("semantic_unit", {})
            clip_path = str(task.get("clip_path", "") or "")
            reused_info = self._build_reusable_routing_pre_prune_info(
                semantic_unit=semantic_unit,
                clip_path=clip_path,
            )
            reused_or_missing.append(reused_info)
            if reused_info is None:
                pending_tasks.append(task)

        recomputed_results: List[Dict[str, Any]] = []
        if pending_tasks:
            recomputed_results = await self._prepare_pruned_clips_for_units(
                clips_dir=clips_dir,
                unit_tasks=pending_tasks,
                force_preprocess=force_preprocess,
            )

        merged_results: List[Dict[str, Any]] = []
        recompute_index = 0
        reused_count = 0
        for idx, reused_info in enumerate(reused_or_missing):
            if reused_info is not None:
                merged_results.append(reused_info)
                reused_count += 1
                continue

            if recompute_index < len(recomputed_results):
                merged_results.append(recomputed_results[recompute_index])
                recompute_index += 1
                continue

            fallback_task = unit_tasks[idx]
            merged_results.append(
                self._build_default_pre_prune_info(
                    semantic_unit=fallback_task.get("semantic_unit", {}),
                    clip_path=str(fallback_task.get("clip_path", "") or ""),
                )
            )

        logger.info(
            f"[VL-PrePrune] routing reuse: total={len(unit_tasks)}, reused={reused_count}, recomputed={len(unit_tasks) - reused_count}"
        )
        return merged_results

    async def _prepare_pruned_clips_for_units(
        self,
        *,
        clips_dir: str,
        unit_tasks: List[Dict[str, Any]],
        force_preprocess: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        并发执 VL 前预处理銆?        输入 unit_tasks 每项包含锛?        - semantic_unit: 语义单元字典
        - clip_path: 对应单元切片路径
        """
        if not unit_tasks:
            return []

        preprocess_t0 = time.perf_counter()
        worker_count = self._resolve_pre_vl_parallel_workers(len(unit_tasks))
        if worker_count <= 1:
            results: List[Dict[str, Any]] = []
            for task in unit_tasks:
                results.append(
                    await self._prepare_pruned_clip_for_vl(
                        clips_dir=clips_dir,
                        semantic_unit=task.get("semantic_unit", {}),
                        original_clip_path=str(task.get("clip_path", "") or ""),
                        force_preprocess=force_preprocess,
                    )
                )
            elapsed_ms = (time.perf_counter() - preprocess_t0) * 1000.0
            applied_count = sum(1 for item in results if bool((item or {}).get("applied", False)))
            logger.info(
                f"[VL-PrePrune] preprocess done: units={len(unit_tasks)}, applied={applied_count}, workers=1, mode=serial, ms={elapsed_ms:.1f}"
            )
            return results

        process_mode = self._should_use_pre_vl_process_mode(worker_count)
        logger.info(
            f"[VL-PrePrune] parallel preprocess start: units={len(unit_tasks)}, workers={worker_count}, "
            f"mode={'process' if process_mode else 'async'}"
        )

        stable_overrides: List[Optional[List[Tuple[float, float]]]] = [None for _ in unit_tasks]
        if process_mode:
            eligible_indexes: List[int] = []
            eligible_tasks: List[Dict[str, Any]] = []
            for index, task in enumerate(unit_tasks):
                semantic_unit = task.get("semantic_unit", {}) if isinstance(task, dict) else {}
                if self._is_pre_vl_pruning_enabled_for_knowledge_type(
                    (semantic_unit or {}).get("knowledge_type", "")
                ):
                    eligible_indexes.append(index)
                    eligible_tasks.append(task)
            if eligible_tasks:
                detected_overrides = await self._detect_stable_islands_for_units_via_process_pool(
                    unit_tasks=eligible_tasks,
                    worker_count=worker_count,
                )
                for rel_index, unit_index in enumerate(eligible_indexes):
                    if rel_index < len(detected_overrides):
                        stable_overrides[unit_index] = detected_overrides[rel_index]

        semaphore = asyncio.Semaphore(worker_count)

        async def _run_single(task: Dict[str, Any], stable_intervals_override: Optional[List[Tuple[float, float]]]) -> Dict[str, Any]:
            semantic_unit = task.get("semantic_unit", {})
            clip_path = str(task.get("clip_path", "") or "")
            if not self._is_pre_vl_pruning_enabled_for_knowledge_type(
                (semantic_unit or {}).get("knowledge_type", "")
            ):
                return self._build_default_pre_prune_info(
                    semantic_unit=semantic_unit,
                    clip_path=clip_path,
                )
            if process_mode and stable_intervals_override is None:
                return self._build_default_pre_prune_info(
                    semantic_unit=semantic_unit,
                    clip_path=clip_path,
                )
            async with semaphore:
                return await self._prepare_pruned_clip_for_vl(
                    clips_dir=clips_dir,
                    semantic_unit=semantic_unit,
                    original_clip_path=clip_path,
                    force_preprocess=force_preprocess,
                    stable_intervals_override=stable_intervals_override,
                )

        raw_results = await asyncio.gather(
            *[
                _run_single(task, stable_intervals_override=stable_overrides[index])
                for index, task in enumerate(unit_tasks)
            ],
            return_exceptions=True,
        )

        normalized_results: List[Dict[str, Any]] = []
        for index, raw in enumerate(raw_results):
            if isinstance(raw, Exception):
                task = unit_tasks[index]
                semantic_unit = task.get("semantic_unit", {})
                clip_path = str(task.get("clip_path", "") or "")
                logger.warning(
                    f"[VL-PrePrune] 并发任务异常: unit={semantic_unit.get('unit_id', '')}, error={raw}"
                )
                normalized_results.append(
                    self._build_default_pre_prune_info(
                        semantic_unit=semantic_unit,
                        clip_path=clip_path,
                    )
                )
            else:
                normalized_results.append(raw)

        elapsed_ms = (time.perf_counter() - preprocess_t0) * 1000.0
        applied_count = sum(1 for item in normalized_results if bool((item or {}).get("applied", False)))
        logger.info(
            f"[VL-PrePrune] preprocess done: units={len(unit_tasks)}, applied={applied_count}, workers={worker_count}, "
            f"mode={'process' if process_mode else 'async'}, ms={elapsed_ms:.1f}"
        )
        return normalized_results

    def _should_merge_multistep_unit(self, unit: Dict[str, Any]) -> bool:
        """
        Whether to merge multi-step clips back into one clip (legacy compatibility).
        """
        knowledge_type = (unit.get("knowledge_type", "") or "").lower()
        return knowledge_type == "process" and bool(unit.get("mult_steps", False))

    def _collect_segments_from_clip(self, clip: Dict[str, Any]) -> List[Dict[str, float]]:
        """
        浠?clip 请求中抽鍙?segments；若未显式提供，则回退鍒?start/end銆?
        """
        segments: List[Dict[str, float]] = []
        raw_segments = clip.get("segments") if isinstance(clip, dict) else None
        if raw_segments:
            for seg in raw_segments:
                start_sec = float(seg.get("start_sec", seg.get("start", 0.0)))
                end_sec = float(seg.get("end_sec", seg.get("end", 0.0)))
                if end_sec > start_sec:
                    segments.append({"start_sec": start_sec, "end_sec": end_sec})
        else:
            start_sec = float(clip.get("start_sec", 0.0))
            end_sec = float(clip.get("end_sec", 0.0))
            if end_sec > start_sec:
                segments.append({"start_sec": start_sec, "end_sec": end_sec})
        return segments

    def _merge_multistep_clip_requests(
        self,
        semantic_units: List[Dict[str, Any]],
        clip_requests: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        灏?process>10s + mult_steps=true 的多涓?clip 请求合并为单一拼接片段銆?
        """
        if not semantic_units:
            return clip_requests

        unit_map = {u.get("unit_id", ""): u for u in semantic_units}
        merge_unit_ids = {u.get("unit_id", "") for u in semantic_units if self._should_merge_multistep_unit(u)}
        if not merge_unit_ids:
            return clip_requests

        grouped: Dict[str, List[Dict[str, Any]]] = {uid: [] for uid in merge_unit_ids}
        remaining: List[Dict[str, Any]] = []
        for clip in clip_requests:
            if str(clip.get("analysis_mode", "")).strip().lower() == "legacy_action_units":
                remaining.append(clip)
                continue
            unit_id = clip.get("semantic_unit_id", "")
            if unit_id in merge_unit_ids:
                grouped.setdefault(unit_id, []).append(clip)
            else:
                remaining.append(clip)

        merged = list(remaining)
        for unit_id in merge_unit_ids:
            unit = unit_map.get(unit_id, {})
            clips = grouped.get(unit_id, [])
            segments: List[Dict[str, float]] = []
            knowledge_type = ""
            for clip in clips:
                if not knowledge_type:
                    knowledge_type = clip.get("knowledge_type", "")
                segments.extend(self._collect_segments_from_clip(clip))

            if not segments:
                start_sec = float(unit.get("start_sec", 0.0))
                end_sec = float(unit.get("end_sec", start_sec))
                if end_sec < start_sec:
                    end_sec = start_sec
                segments = [{"start_sec": start_sec, "end_sec": end_sec}]
                if not knowledge_type:
                    knowledge_type = unit.get("knowledge_type", "")

            segments.sort(key=lambda s: s["start_sec"])
            start_sec = min(seg["start_sec"] for seg in segments)
            end_sec = max(seg["end_sec"] for seg in segments)
            merged_clip_stem = f"{unit_id}_clip_vl_merged"
            merged.append({
                "clip_id": f"{unit_id}/{merged_clip_stem}",
                "start_sec": start_sec,
                "end_sec": end_sec,
                "knowledge_type": knowledge_type,
                "semantic_unit_id": unit_id,
                "segments": segments
            })
            logger.info(
                f"VL 多段拼接合并: unit={unit_id}, segments={len(segments)}, "
                f"range=[{start_sec:.2f}-{end_sec:.2f}]"
            )

        return merged


    def _is_tutorial_process_unit(self, semantic_unit: Dict[str, Any], duration_sec: float) -> bool:
        """Decide whether this semantic unit should use tutorial-stepwise VL mode."""
        if not self.tutorial_mode_enabled:
            return False
        knowledge_type = self._normalize_pre_vl_pruning_knowledge_type(
            semantic_unit.get("knowledge_type", "")
        )
        return (
            knowledge_type == "process"
            and bool(semantic_unit.get("mult_steps", False))
        )

    def _build_tutorial_extra_prompt(self) -> str:
        """Prompt for long multi-step process units in tutorial mode."""
        return (
            "Focus on creating a 1-on-1 operational tutorial instead of generic understanding. "
            "Split the clip into complete steps. Keep explanation, execution, and result of the same step together. "
            "Remove thinking time such as mouse wandering, hesitation, and idle waiting with no new information. "
            "Each step must be at least 5 seconds; merge overly short steps with adjacent ones. "
            "For each step, output step_description, required main_operation, optional main_action/precautions/"
            "step_summary/operation_guidance, "
            "and instructional_keyframes (objects with timestamp_sec, optional frame_reason, optional bbox) "
            "as true instructional keyframes "
            "(prefer final state or just-before-submit moment). "
            "Optional fields can be omitted or returned as empty values when unnecessary."
        )

    @staticmethod
    def _get_default_vl_arg_structured_system_prompt() -> str:
        return _load_package_prompt_default(
            _DEFAULT_VL_ARG_STRUCTURED_SYSTEM_REL_PATH,
            fallback=(
                "You are a tutorial step enhancement assistant.\n"
                "Enhance main_operation using subtitle context.\n"
                "Return Markdown only, do not return JSON."
            ),
        )

    @staticmethod
    def _get_default_vl_arg_structured_user_prompt_template() -> str:
        return _load_package_prompt_default(
            _DEFAULT_VL_ARG_STRUCTURED_USER_REL_PATH,
            fallback=(
                "[Original main_operation]\n"
                "{{main_operation}}\n\n"
                "[Subtitle context]\n"
                "{{subtitle_context}}\n\n"
                "Return structured Markdown only."
            ),
        )

    @staticmethod
    def _normalize_main_operation_markdown(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return ""
            try:
                parsed = json.loads(text)
            except Exception:
                parsed = None
            if isinstance(parsed, list):
                return "\n".join(
                    [str(item or "").strip() for item in parsed if str(item or "").strip()]
                ).strip()
            return text
        if isinstance(value, (list, tuple, set)):
            return "\n".join(
                [str(item or "").strip() for item in value if str(item or "").strip()]
            ).strip()
        return str(value).strip()

    @staticmethod
    def _strip_markdown_fence(text: str) -> str:
        raw = str(text or "").strip()
        if not raw:
            return ""
        fenced = re.match(r"^```(?:markdown|md)?\s*([\s\S]*?)\s*```$", raw, flags=re.IGNORECASE)
        if fenced:
            return str(fenced.group(1) or "").strip()
        return raw

    def _render_vl_arg_structured_user_prompt(
        self,
        *,
        main_operation: str,
        subtitle_context: str,
    ) -> str:
        template = str(getattr(self, "_vl_arg_structured_user_prompt_template", "") or "").strip()
        if not template:
            template = self._get_default_vl_arg_structured_user_prompt_template()
        return (
            template.replace("{{main_operation}}", main_operation)
            .replace("{{subtitle_context}}", subtitle_context)
            .replace("{main_operation}", main_operation)
            .replace("{subtitle_context}", subtitle_context)
        )

    @staticmethod
    def _build_vl_arg_batch_main_operation_text(step_payloads: List[Dict[str, Any]]) -> str:
        blocks: List[str] = []
        for payload in step_payloads:
            step_id = int(safe_float(payload.get("step_id", 0), 0.0))
            if step_id <= 0:
                continue
            main_operation = str(payload.get("main_operation", "") or "").strip()
            if not main_operation:
                continue
            blocks.append(f"[STEP_ID={step_id}]\n{main_operation}")
        return "\n\n".join(blocks).strip()

    @staticmethod
    def _get_default_vl_arg_batch_contract_prompt() -> str:
        return (
            "【批量输出契约】\n"
            "你收到的是同一语义单元内多个步骤的 main_operation。\n"
            "请严格按以下结构返回每个步骤的增强结果：\n"
            "[STEP_ID=<整数>]\n"
            "<该步骤增强后的 Markdown>\n\n"
            "约束：\n"
            "1) 每个 STEP_ID 仅输出一次；\n"
            "2) 仅输出上述结构，不要输出 JSON；\n"
            "3) 不要输出代码块围栏。"
        )

    def _render_vl_arg_batch_prompt(
        self,
        *,
        step_payloads: List[Dict[str, Any]],
        subtitle_context: str,
    ) -> str:
        step_text = self._build_vl_arg_batch_main_operation_text(step_payloads)
        if not step_text:
            return ""
        base_prompt = self._render_vl_arg_structured_user_prompt(
            main_operation=step_text,
            subtitle_context=subtitle_context,
        )
        step_ids = [
            int(safe_float(item.get("step_id", 0), 0.0))
            for item in step_payloads
            if int(safe_float(item.get("step_id", 0), 0.0)) > 0
        ]
        valid_step_ids = ", ".join([str(step_id) for step_id in step_ids])
        return (
            f"{base_prompt}\n\n"
            f"{self._get_default_vl_arg_batch_contract_prompt()}\n"
            f"仅允许输出以下 STEP_ID: {valid_step_ids}"
        ).strip()

    def _parse_vl_arg_batch_response(
        self,
        *,
        text: str,
        expected_step_ids: List[int],
    ) -> Dict[int, str]:
        cleaned_text = self._strip_markdown_fence(text)
        if not cleaned_text:
            return {}
        expected_ids = {int(item) for item in expected_step_ids if int(item) > 0}
        if not expected_ids:
            return {}

        step_marker_pattern = re.compile(r"^\[STEP_ID\s*=\s*(\d+)\]\s*$", re.IGNORECASE | re.MULTILINE)
        markers = list(step_marker_pattern.finditer(cleaned_text))
        if not markers:
            if len(expected_ids) == 1:
                only_step_id = next(iter(expected_ids))
                return {only_step_id: cleaned_text.strip()}
            return {}

        parsed_by_step: Dict[int, str] = {}
        for index, marker in enumerate(markers):
            step_id = int(marker.group(1))
            if step_id not in expected_ids:
                continue
            start_idx = marker.end()
            end_idx = markers[index + 1].start() if index + 1 < len(markers) else len(cleaned_text)
            body = cleaned_text[start_idx:end_idx].strip()
            if body:
                parsed_by_step[step_id] = body
        return parsed_by_step

    def _build_vl_arg_subtitle_context(
        self,
        *,
        semantic_unit: Dict[str, Any],
        output_dir: str,
    ) -> str:
        unit_id = str(semantic_unit.get("unit_id", "") or "").strip() or "UNKNOWN"
        unit_start_sec = safe_float(semantic_unit.get("start_sec", 0.0), 0.0)
        unit_end_sec = safe_float(semantic_unit.get("end_sec", unit_start_sec), unit_start_sec)
        if unit_end_sec < unit_start_sec:
            unit_end_sec = unit_start_sec

        topic = str(semantic_unit.get("knowledge_topic", "") or "").strip()
        full_text = str(semantic_unit.get("full_text", "") or "").strip()
        text = str(semantic_unit.get("text", "") or "").strip()
        context_lines: List[str] = []
        if topic:
            context_lines.append(f"语义单元主题：{topic}")
        if full_text or text:
            context_lines.append(f"语义单元文本：{full_text or text}")

        subtitle_lines: List[str] = []
        try:
            all_subtitles = self._load_subtitles_for_output_dir(output_dir)
            relative_subtitles = self._build_unit_relative_subtitles(
                all_subtitles,
                unit_start_sec,
                unit_end_sec,
            )
        except Exception as error:
            logger.warning(
                "[VL-Arg] load subtitles failed: unit=%s, output_dir=%s, error=%s",
                unit_id,
                output_dir,
                error,
            )
            relative_subtitles = []

        max_lines = max(1, int(self.vl_arg_postprocess_max_subtitle_lines))
        for sub in relative_subtitles[:max_lines]:
            sub_text = str(sub.get("text", "") or "").strip()
            if not sub_text:
                continue
            sub_start = safe_float(sub.get("start_sec", 0.0), 0.0)
            sub_end = safe_float(sub.get("end_sec", sub_start), sub_start)
            subtitle_lines.append(f"[{sub_start:.2f}s-{sub_end:.2f}s] {sub_text}")

        if subtitle_lines:
            context_lines.append("语义单元字幕：\n" + "\n".join(subtitle_lines))
        else:
            context_lines.append("语义单元字幕：无")

        context = "\n".join([line for line in context_lines if line])
        max_chars = max(400, int(self.vl_arg_postprocess_max_context_chars))
        if len(context) > max_chars:
            context = context[:max_chars].rstrip() + "..."
        return context

    @staticmethod
    def _is_retryable_network_error(error: Exception) -> bool:
        if isinstance(error, (asyncio.TimeoutError, TimeoutError, ConnectionError, OSError)):
            return True
        message = str(error or "").strip().lower()
        if not message:
            return False
        retryable_tokens = (
            "connection error",
            "all connection attempts failed",
            "connecterror",
            "connection reset",
            "timed out",
            "timeout",
            "temporarily unavailable",
        )
        return any(token in message for token in retryable_tokens)

    async def _call_deepseek_complete_text_with_backoff(
        self,
        *,
        prompt: str,
        system_message: str,
        model: str,
        hedge_context: Dict[str, Any],
        retry_label: str,
    ) -> Tuple[str, Any, Any]:
        attempts = max(1, int(self.vl_arg_postprocess_retry_max_attempts))
        for attempt in range(1, attempts + 1):
            try:
                return await llm_gateway.deepseek_complete_text(
                    prompt=prompt,
                    system_message=system_message,
                    model=model,
                    hedge_context=hedge_context,
                )
            except Exception as error:
                is_retryable = self._is_retryable_network_error(error)
                if attempt >= attempts or not is_retryable:
                    raise
                exp_delay = float(self.vl_arg_postprocess_retry_initial_backoff_sec) * (
                    float(self.vl_arg_postprocess_retry_backoff_multiplier) ** float(attempt - 1)
                )
                capped_delay = min(float(self.vl_arg_postprocess_retry_max_backoff_sec), exp_delay)
                jitter = random.uniform(0.0, float(self.vl_arg_postprocess_retry_jitter_sec))
                sleep_sec = capped_delay + jitter
                logger.warning(
                    "[VL-Arg] %s retry scheduled: attempt=%s/%s, sleep_sec=%.2f, error=%s",
                    retry_label,
                    attempt + 1,
                    attempts,
                    sleep_sec,
                    error,
                )
                await asyncio.sleep(sleep_sec)

    async def _postprocess_unit_main_operations(
        self,
        *,
        analysis_result: Any,
        semantic_unit: Dict[str, Any],
        output_dir: str,
    ) -> None:
        if not self.vl_arg_postprocess_enabled:
            return
        if not isinstance(semantic_unit, dict):
            return
        if str(getattr(analysis_result, "analysis_mode", "") or "").strip().lower() != "tutorial_stepwise":
            return

        raw_steps = getattr(analysis_result, "raw_response_json", []) or []
        if not isinstance(raw_steps, list) or not raw_steps:
            return

        subtitle_context = self._build_vl_arg_subtitle_context(
            semantic_unit=semantic_unit,
            output_dir=output_dir,
        )
        if not subtitle_context:
            return

        unit_id = str(semantic_unit.get("unit_id", "") or "").strip() or "UNKNOWN"
        raw_steps_by_id: Dict[int, Dict[str, Any]] = {}
        for index, raw_step in enumerate(raw_steps, start=1):
            if not isinstance(raw_step, dict):
                continue
            step_id = int(safe_float(raw_step.get("step_id", index), float(index)))
            if step_id <= 0:
                step_id = index
            raw_steps_by_id[step_id] = raw_step

        parsed_steps_by_id: Dict[int, Any] = {}
        for index, parsed_step in enumerate(getattr(analysis_result, "analysis_results", []) or [], start=1):
            step_id = int(safe_float(getattr(parsed_step, "step_id", index), float(index)))
            if step_id <= 0:
                step_id = index
            parsed_steps_by_id[step_id] = parsed_step

        clips_by_step: Dict[int, List[Dict[str, Any]]] = {}
        for clip in getattr(analysis_result, "clip_requests", []) or []:
            if not isinstance(clip, dict):
                continue
            step_id = int(safe_float(clip.get("step_id", 0), 0.0))
            if step_id <= 0:
                continue
            clips_by_step.setdefault(step_id, []).append(clip)

        step_ids = sorted(set(raw_steps_by_id.keys()) | set(parsed_steps_by_id.keys()) | set(clips_by_step.keys()))
        if not step_ids:
            return

        step_payloads: List[Dict[str, Any]] = []
        for step_id in step_ids:
            raw_step = raw_steps_by_id.get(step_id)
            parsed_step = parsed_steps_by_id.get(step_id)

            raw_main_operation: Any = None
            if isinstance(raw_step, dict):
                raw_main_operation = raw_step.get("main_operation")
                if raw_main_operation is None:
                    raw_main_operation = raw_step.get("main_operations")
            if raw_main_operation is None and parsed_step is not None:
                raw_main_operation = getattr(parsed_step, "main_operation", None)
            if raw_main_operation is None and clips_by_step.get(step_id):
                raw_main_operation = clips_by_step[step_id][0].get("main_operation")

            main_operation = self._normalize_main_operation_markdown(raw_main_operation)
            if not main_operation:
                continue
            if len(main_operation) > self.vl_arg_postprocess_max_main_operation_chars:
                main_operation = (
                    main_operation[: self.vl_arg_postprocess_max_main_operation_chars].rstrip() + "..."
                )

            step_payloads.append(
                {
                    "step_id": step_id,
                    "main_operation": main_operation,
                    "raw_step": raw_step,
                    "parsed_step": parsed_step,
                    "clips": clips_by_step.get(step_id, []),
                }
            )
        if not step_payloads:
            return

        prompt = self._render_vl_arg_batch_prompt(
            step_payloads=step_payloads,
            subtitle_context=subtitle_context,
        )
        if not prompt:
            return

        step_ids_in_batch = [int(item.get("step_id", 0)) for item in step_payloads]
        try:
            enhanced_text, _metadata, _logprobs = await self._call_deepseek_complete_text_with_backoff(
                prompt=prompt,
                system_message=self._vl_arg_structured_system_prompt,
                model=self.vl_arg_postprocess_model,
                hedge_context={
                    "semantic_unit_id": unit_id,
                    "step_ids": step_ids_in_batch,
                    "stage": "vl_arg_main_operation_postprocess_batch",
                },
                retry_label="main_operation batch postprocess",
            )
        except Exception as error:
            logger.warning(
                "[VL-Arg] batch postprocess failed: unit=%s, step_ids=%s, error=%s",
                unit_id,
                step_ids_in_batch,
                error,
            )
            return

        enhanced_by_step = self._parse_vl_arg_batch_response(
            text=str(enhanced_text or ""),
            expected_step_ids=step_ids_in_batch,
        )
        if not enhanced_by_step:
            logger.warning(
                "[VL-Arg] batch postprocess parse failed: unit=%s, step_ids=%s, response_preview=%s",
                unit_id,
                step_ids_in_batch,
                str(enhanced_text or "")[:240],
            )
            return

        updated_steps = 0
        for payload in step_payloads:
            step_id = int(payload.get("step_id", 0))
            if step_id <= 0:
                continue
            raw_step = payload.get("raw_step")
            parsed_step = payload.get("parsed_step")
            step_clips = payload.get("clips") or []
            normalized_enhanced = self._strip_markdown_fence(
                str(enhanced_by_step.get(step_id, "") or "")
            )
            if not normalized_enhanced:
                continue
            if isinstance(raw_step, dict):
                raw_step["main_operation"] = normalized_enhanced
                raw_step["main_operations"] = normalized_enhanced
            if parsed_step is not None:
                try:
                    parsed_step.main_operation = [normalized_enhanced]
                except Exception as error:
                    logger.warning(
                        "[VL-Arg] write parsed step failed: unit=%s, step_id=%s, error=%s",
                        unit_id,
                        step_id,
                        error,
                    )
            for clip in step_clips:
                clip["main_operation"] = [normalized_enhanced]
                clip["main_operations"] = normalized_enhanced

            updated_steps += 1

        if updated_steps > 0:
            logger.info(
                "[VL-Arg] main_operation postprocess done: unit=%s, updated_steps=%s",
                unit_id,
                updated_steps,
            )

    @staticmethod
    def _build_vl_arg_batch_main_content_text(segment_payloads: List[Dict[str, Any]]) -> str:
        blocks: List[str] = []
        for payload in segment_payloads:
            segment_id = int(safe_float(payload.get("segment_id", 0), 0.0))
            if segment_id <= 0:
                continue
            main_content = str(payload.get("main_content", "") or "").strip()
            if not main_content:
                continue
            blocks.append(f"[SEGMENT_ID={segment_id}]\n{main_content}")
        return "\n\n".join(blocks).strip()

    def _render_vl_arg_concrete_batch_prompt(
        self,
        *,
        segment_payloads: List[Dict[str, Any]],
        subtitle_context: str,
    ) -> str:
        segment_text = self._build_vl_arg_batch_main_content_text(segment_payloads)
        if not segment_text:
            return ""
        segment_ids = [
            int(safe_float(item.get("segment_id", 0), 0.0))
            for item in segment_payloads
            if int(safe_float(item.get("segment_id", 0), 0.0)) > 0
        ]
        valid_segment_ids = ", ".join([str(segment_id) for segment_id in segment_ids])
        return (
            "你会收到同一语义单元内多个片段的 main_content 草稿，请基于字幕上下文做增量补全。\n"
            "要求：保持中文 Markdown 结构，不要删除或改写 [KEYFRAME_N] 占位符含义。\n\n"
            f"[字幕上下文]\n{subtitle_context}\n\n"
            f"[待补全 main_content]\n{segment_text}\n\n"
            "请只输出 JSON 数组，每项格式为 "
            '{"segment_id": <int>, "main_content": "<string>"}'
            "。\n"
            "只允许输出以下 segment_id: "
            f"{valid_segment_ids}"
        ).strip()

    @staticmethod
    def _parse_vl_arg_concrete_batch_response(
        *,
        text: str,
        expected_segment_ids: List[int],
    ) -> Dict[int, str]:
        expected_ids = {int(item) for item in expected_segment_ids if int(item) > 0}
        if not expected_ids:
            return {}
        raw = str(text or "").strip()
        if not raw:
            return {}
        fenced = re.search(r"```(?:json)?\s*([\s\S]*?)```", raw, flags=re.IGNORECASE)
        if fenced:
            raw = str(fenced.group(1) or "").strip()
        candidates: List[str] = [raw]
        array_match = re.search(r"\[[\s\S]*\]", raw)
        if array_match:
            candidates.append(str(array_match.group(0) or "").strip())

        parsed_items: List[Any] = []
        for candidate in candidates:
            if not candidate:
                continue
            try:
                parsed = json.loads(candidate)
            except Exception:
                continue
            if isinstance(parsed, list):
                parsed_items = parsed
                break
            if isinstance(parsed, dict):
                for key in ("segments", "items", "results"):
                    value = parsed.get(key)
                    if isinstance(value, list):
                        parsed_items = value
                        break
                if parsed_items:
                    break

        parsed_by_segment: Dict[int, str] = {}
        for item in parsed_items:
            if not isinstance(item, dict):
                continue
            segment_id = int(
                safe_float(
                    item.get("segment_id", item.get("id", 0)),
                    0.0,
                )
            )
            if segment_id not in expected_ids:
                continue
            main_content = str(item.get("main_content", "") or "").strip()
            if not main_content:
                continue
            parsed_by_segment[segment_id] = main_content
        return parsed_by_segment

    async def _postprocess_unit_main_content(
        self,
        *,
        analysis_result: Any,
        semantic_unit: Dict[str, Any],
        output_dir: str,
    ) -> None:
        if not self.vl_arg_postprocess_concrete_enabled:
            return
        if not isinstance(semantic_unit, dict):
            return
        if str(getattr(analysis_result, "analysis_mode", "") or "").strip().lower() != "concrete":
            return

        raw_segments = getattr(analysis_result, "raw_response_json", []) or []
        if not isinstance(raw_segments, list) or not raw_segments:
            return

        subtitle_context = self._build_vl_arg_subtitle_context(
            semantic_unit=semantic_unit,
            output_dir=output_dir,
        )
        if not subtitle_context:
            return

        unit_id = str(semantic_unit.get("unit_id", "") or "").strip() or "UNKNOWN"
        segment_payloads: List[Dict[str, Any]] = []
        for index, raw_segment in enumerate(raw_segments, start=1):
            if not isinstance(raw_segment, dict):
                continue
            segment_id = int(
                safe_float(
                    raw_segment.get("segment_id", raw_segment.get("id", index)),
                    float(index),
                )
            )
            if segment_id <= 0:
                segment_id = index
            main_content = self._strip_markdown_fence(
                str(raw_segment.get("main_content", "") or "")
            )
            main_content = main_content.strip()
            if not main_content:
                continue
            if len(main_content) > self.vl_arg_postprocess_max_main_content_chars:
                main_content = (
                    main_content[: self.vl_arg_postprocess_max_main_content_chars].rstrip() + "..."
                )
            segment_payloads.append(
                {
                    "segment_id": segment_id,
                    "main_content": main_content,
                    "raw_segment": raw_segment,
                }
            )
        if not segment_payloads:
            return

        prompt = self._render_vl_arg_concrete_batch_prompt(
            segment_payloads=segment_payloads,
            subtitle_context=subtitle_context,
        )
        if not prompt:
            return

        segment_ids = [int(item.get("segment_id", 0)) for item in segment_payloads]
        try:
            enhanced_text, _metadata, _logprobs = await self._call_deepseek_complete_text_with_backoff(
                prompt=prompt,
                system_message=(
                    "你是视频图文讲义补全助手。"
                    "请基于字幕上下文增量补全 main_content，并保持 KEYFRAME 占位符映射一致。"
                    "只返回 JSON 数组。"
                ),
                model=self.vl_arg_postprocess_model,
                hedge_context={
                    "semantic_unit_id": unit_id,
                    "segment_ids": segment_ids,
                    "stage": "vl_arg_main_content_postprocess_batch",
                },
                retry_label="main_content batch postprocess",
            )
        except Exception as error:
            logger.warning(
                "[VL-Arg] concrete main_content postprocess failed: unit=%s, segment_ids=%s, error=%s",
                unit_id,
                segment_ids,
                error,
            )
            return

        enhanced_by_segment = self._parse_vl_arg_concrete_batch_response(
            text=str(enhanced_text or ""),
            expected_segment_ids=segment_ids,
        )
        if not enhanced_by_segment:
            logger.warning(
                "[VL-Arg] concrete main_content parse failed: unit=%s, segment_ids=%s, response_preview=%s",
                unit_id,
                segment_ids,
                str(enhanced_text or "")[:240],
            )
            return

        updated_segments = 0
        for payload in segment_payloads:
            segment_id = int(payload.get("segment_id", 0))
            if segment_id <= 0:
                continue
            raw_segment = payload.get("raw_segment")
            normalized_enhanced = self._strip_markdown_fence(
                str(enhanced_by_segment.get(segment_id, "") or "")
            ).strip()
            if not normalized_enhanced:
                continue
            if isinstance(raw_segment, dict):
                raw_segment["main_content"] = normalized_enhanced
            updated_segments += 1

        if updated_segments > 0:
            logger.info(
                "[VL-Arg] concrete main_content postprocess done: unit=%s, updated_segments=%s",
                unit_id,
                updated_segments,
            )

    @staticmethod
    def _get_default_grid_spatial_anchor_prompt() -> str:
        return _load_package_prompt_default(
            _DEFAULT_GRID_SPATIAL_ANCHOR_REL_PATH,
            fallback=(
                "You are a visual anchoring assistant.\n"
                "Locate the target area on this grid-overlaid image.\n"
                "Return only JSON: "
                '{"visual_verification":"...", "grid_start":"C4", "grid_end":"E7"}'
            ),
        )

    @staticmethod
    def _get_default_best_frame_selection_prompt() -> str:
        return (
            "You are an expert screenshot selector.\n"
            "You will receive a frame_reason and multiple candidate screenshots.\n"
            "Choose exactly one best image that matches the frame_reason and is clear/stable.\n"
            "Output only image_n (for example image_2) or image_none."
        )

    def _render_grid_anchor_prompt(self) -> str:
        template = str(getattr(self, "_tutorial_grid_anchor_prompt", "") or "").strip()
        if not template:
            template = self._get_default_grid_spatial_anchor_prompt()
        return template

    @staticmethod
    def _extract_json_object_candidate(text: str) -> str:
        raw = str(text or "").strip()
        if not raw:
            return raw
        code_block = re.search(r"```(?:json)?\\s*([\\s\\S]*?)```", raw, flags=re.IGNORECASE)
        if code_block:
            raw = str(code_block.group(1) or "").strip()
        start = raw.find("{")
        if start < 0:
            return raw
        in_str = False
        escape = False
        depth = 0
        for idx, ch in enumerate(raw[start:], start=start):
            if in_str:
                if escape:
                    escape = False
                elif ch == "\\":
                    escape = True
                elif ch == "\"":
                    in_str = False
                continue
            if ch == "\"":
                in_str = True
                continue
            if ch == "{":
                depth += 1
                continue
            if ch == "}":
                depth -= 1
                if depth == 0:
                    return raw[start : idx + 1]
        return raw

    def _parse_grid_anchor_payload(self, payload: Any) -> Dict[str, str]:
        if isinstance(payload, dict):
            raw_dict = payload
        else:
            raw_dict = {}
        if "grid_start" in raw_dict and "grid_end" in raw_dict:
            return {
                "grid_start": str(raw_dict.get("grid_start", "") or "").strip().upper(),
                "grid_end": str(raw_dict.get("grid_end", "") or "").strip().upper(),
                "visual_verification": str(raw_dict.get("visual_verification", "") or "").strip(),
            }

        raw_text = str(raw_dict.get("raw_response", payload) or "").strip()
        if not raw_text:
            return {"grid_start": "", "grid_end": "", "visual_verification": ""}
        candidate = self._extract_json_object_candidate(raw_text)
        try:
            obj = json.loads(candidate)
            if isinstance(obj, dict):
                return {
                    "grid_start": str(obj.get("grid_start", "") or "").strip().upper(),
                    "grid_end": str(obj.get("grid_end", "") or "").strip().upper(),
                    "visual_verification": str(obj.get("visual_verification", "") or "").strip(),
                }
        except Exception:
            pass
        return {"grid_start": "", "grid_end": "", "visual_verification": ""}

    async def _apply_grid_anchor_crop_for_keyframe(
        self,
        *,
        keyframe_path: Path,
    ) -> Dict[str, Any]:
        if not self.tutorial_grid_anchor_enabled:
            return {"grid_anchor_status": "disabled"}

        overlay_path = keyframe_path.parent / f"{keyframe_path.stem}_grid_overlay{keyframe_path.suffix}"
        overlay_ok = save_grid_overlay_image(
            source_image_path=keyframe_path,
            output_image_path=overlay_path,
            grid_rows=self.tutorial_grid_rows,
            grid_cols=self.tutorial_grid_cols,
            alpha=self.tutorial_grid_overlay_alpha,
            line_thickness=self.tutorial_grid_overlay_line_thickness,
            variant=self.tutorial_grid_overlay_variant,
        )
        if not overlay_ok:
            return {"grid_anchor_status": "overlay_failed"}

        prompt = self._render_grid_anchor_prompt()
        request_audit: Dict[str, Any] = {
            "model": "vision_ai",
            "image_path": str(overlay_path),
            "prompt": prompt,
        }
        try:
            payload = await llm_gateway.vision_validate_image(
                image_path=str(overlay_path),
                prompt=prompt,
                skip_duplicate_check=True,
            )
        except Exception as error:
            return {
                "grid_anchor_status": "vision_failed",
                "grid_anchor_error": str(error),
                "grid_overlay_file": overlay_path.name,
                "grid_anchor_llm_interaction": {
                    "stage": "grid_spatial_anchor",
                    "success": False,
                    "request": request_audit,
                    "error": str(error),
                },
            }

        parsed = self._parse_grid_anchor_payload(payload)
        grid_start = str(parsed.get("grid_start", "") or "").strip().upper()
        grid_end = str(parsed.get("grid_end", "") or "").strip().upper()
        if not grid_start or not grid_end:
            return {
                "grid_anchor_status": "invalid_grid_response",
                "grid_overlay_file": overlay_path.name,
                "grid_anchor_llm_interaction": {
                    "stage": "grid_spatial_anchor",
                    "success": False,
                    "request": request_audit,
                    "response": payload if isinstance(payload, dict) else {"raw_response": str(payload)},
                    "error": "invalid_grid_response",
                },
            }

        crop_meta = crop_keyframe_inplace_by_grid_range(
            image_path=keyframe_path,
            grid_start=grid_start,
            grid_end=grid_end,
            grid_rows=self.tutorial_grid_rows,
            grid_cols=self.tutorial_grid_cols,
            expand_ratio=self.tutorial_grid_crop_expand_ratio,
            min_border_px=self.tutorial_grid_crop_min_border_px,
        )
        if crop_meta is None:
            return {
                "grid_anchor_status": "crop_failed",
                "grid_start": grid_start,
                "grid_end": grid_end,
                "grid_overlay_file": overlay_path.name,
                "grid_anchor_llm_interaction": {
                    "stage": "grid_spatial_anchor",
                    "success": False,
                    "request": request_audit,
                    "response": payload if isinstance(payload, dict) else {"raw_response": str(payload)},
                    "parsed": parsed,
                    "error": "crop_failed",
                },
            }

        result: Dict[str, Any] = {
            "grid_anchor_status": "ok",
            "grid_start": grid_start,
            "grid_end": grid_end,
            "grid_overlay_file": overlay_path.name,
            "crop_x0": int(crop_meta.get("crop_x0", 0)),
            "crop_y0": int(crop_meta.get("crop_y0", 0)),
            "crop_x1": int(crop_meta.get("crop_x1", 0)),
            "crop_y1": int(crop_meta.get("crop_y1", 0)),
            "grid_anchor_llm_interaction": {
                "stage": "grid_spatial_anchor",
                "success": True,
                "request": request_audit,
                "response": payload if isinstance(payload, dict) else {"raw_response": str(payload)},
                "parsed": parsed,
            },
        }
        visual_verification = str(parsed.get("visual_verification", "") or "").strip()
        if visual_verification:
            result["visual_verification"] = visual_verification
        return result

    def _slugify_action_brief(self, text_value: str, max_len: int = 48) -> str:
        """将步骤描述转换为稳定文件名片段。"""
        raw = str(text_value or "").strip().lower()
        raw = re.sub(r"[^a-z0-9]+", "_", raw)
        raw = re.sub(r"_+", "_", raw).strip("_")
        if not raw:
            return "action"
        if len(raw) > max_len:
            return raw[:max_len].rstrip("_") or "action"
        return raw

    @staticmethod
    def _normalize_cv_candidate_screenshots(
        raw_candidates: Any,
        *,
        fallback_timestamp: float,
        fallback_score: float = 0.0,
    ) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        if isinstance(raw_candidates, list):
            for item in raw_candidates:
                if not isinstance(item, dict):
                    continue
                try:
                    timestamp_sec = float(item.get("timestamp_sec", fallback_timestamp))
                except (TypeError, ValueError):
                    continue
                score = safe_float(
                    item.get("score", item.get("quality_score", fallback_score)),
                    fallback_score,
                )
                island_index = int(safe_float(item.get("island_index", 0), 0.0))
                island_start = safe_float(item.get("island_start", timestamp_sec), timestamp_sec)
                island_end = safe_float(item.get("island_end", timestamp_sec), timestamp_sec)
                normalized.append(
                    {
                        "timestamp_sec": float(timestamp_sec),
                        "score": float(score),
                        "island_index": island_index,
                        "island_start": float(island_start),
                        "island_end": float(island_end),
                    }
                )

        if not normalized:
            normalized = [
                {
                    "timestamp_sec": float(fallback_timestamp),
                    "score": float(fallback_score),
                    "island_index": 0,
                    "island_start": float(fallback_timestamp),
                    "island_end": float(fallback_timestamp),
                }
            ]

        deduped: Dict[float, Dict[str, Any]] = {}
        for item in normalized:
            key = round(float(item.get("timestamp_sec", 0.0)), 3)
            existing = deduped.get(key)
            if existing is None or float(item.get("score", 0.0)) > float(existing.get("score", 0.0)):
                deduped[key] = item
        finalized = list(deduped.values())
        finalized.sort(key=lambda it: float(it.get("score", 0.0)), reverse=True)
        return finalized

    @staticmethod
    def _extract_vl_response_text(raw_content: Any) -> str:
        if isinstance(raw_content, str):
            return raw_content.strip()
        if isinstance(raw_content, list):
            parts: List[str] = []
            for item in raw_content:
                if isinstance(item, str):
                    text = item.strip()
                    if text:
                        parts.append(text)
                    continue
                if isinstance(item, dict):
                    text = str(item.get("text", "") or "").strip()
                    if text:
                        parts.append(text)
                        continue
                    inner = item.get("content")
                    if isinstance(inner, str) and inner.strip():
                        parts.append(inner.strip())
            merged = "\n".join(parts).strip()
            if merged:
                return merged
        return str(raw_content or "").strip()

    @staticmethod
    def _encode_frame_as_jpeg_data_uri(frame: Any, *, jpeg_quality: int = 82) -> Optional[str]:
        try:
            import cv2
        except Exception:
            return None
        if frame is None:
            return None
        quality = max(30, min(95, int(jpeg_quality)))
        ok, encoded = cv2.imencode(".jpg", frame, [int(cv2.IMWRITE_JPEG_QUALITY), quality])
        if not ok:
            return None
        raw = encoded.tobytes()
        if not raw:
            return None
        return f"data:image/jpeg;base64,{base64.b64encode(raw).decode('utf-8')}"

    def _build_candidate_images_for_vision_selection(
        self,
        *,
        video_path: str,
        candidates: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        if not candidates:
            return []

        try:
            import cv2
        except Exception as error:
            logger.warning("[VL] best-frame vision selection skipped: cv2 import failed: %s", error)
            return []

        cap, effective_video_path, _ = self._open_video_capture_with_subset_policy(video_path)
        if cap is None or not cap.isOpened():
            logger.warning(
                "[VL] best-frame vision selection cannot open video: source=%s, effective=%s",
                video_path,
                effective_video_path,
            )
            return []

        prepared: List[Dict[str, Any]] = []
        try:
            fps = float(cap.get(cv2.CAP_PROP_FPS) or 30.0)
            total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
            for candidate in candidates:
                timestamp_sec = safe_float(candidate.get("timestamp_sec", 0.0), 0.0)
                frame_index = int(max(0.0, timestamp_sec) * fps) if fps > 0 else 0
                if total_frames > 0:
                    frame_index = max(0, min(frame_index, total_frames - 1))
                cap.set(cv2.CAP_PROP_POS_FRAMES, frame_index)
                ok, frame = cap.read()
                if not ok or frame is None:
                    continue
                data_uri = self._encode_frame_as_jpeg_data_uri(frame)
                if not data_uri:
                    continue
                prepared.append(
                    {
                        "image_id": f"image_{len(prepared) + 1}",
                        "candidate": dict(candidate),
                        "data_uri": data_uri,
                    }
                )
        finally:
            cap.release()

        return prepared

    @staticmethod
    def _build_best_frame_vision_user_text(
        *,
        request: Dict[str, Any],
        prepared_candidates: List[Dict[str, Any]],
    ) -> str:
        frame_reason = str(request.get("frame_reason", "") or "").strip()
        if not frame_reason:
            frame_reason = str(request.get("label", "") or "").strip()
        if not frame_reason:
            frame_reason = "Select the frame that best represents the current semantic action."

        lines: List[str] = [f"frame_reason: {frame_reason}", "candidate_metadata:"]
        for item in prepared_candidates:
            candidate = item.get("candidate", {})
            lines.append(
                f"{item.get('image_id')}: timestamp_sec={safe_float(candidate.get('timestamp_sec', 0.0), 0.0):.3f}, "
                f"cv_score={safe_float(candidate.get('score', 0.0), 0.0):.4f}, "
                f"island_index={int(safe_float(candidate.get('island_index', 0), 0.0))}"
            )
        lines.append("Return only image_n or image_none.")
        return "\n".join(lines)

    @staticmethod
    def _parse_best_frame_selection_index(raw_text: str, candidate_count: int) -> Optional[int]:
        if candidate_count <= 0:
            return None
        text = str(raw_text or "").strip().lower()
        if not text:
            return None
        if re.search(r"\bimage_none\b", text):
            return None

        match = re.search(r"\bimage[_\s-]*(\d{1,2})\b", text)
        if match:
            index = int(match.group(1))
            if 1 <= index <= candidate_count:
                return index - 1

        number_match = re.search(r"\b(\d{1,2})\b", text)
        if number_match:
            index = int(number_match.group(1))
            if 1 <= index <= candidate_count:
                return index - 1
        return None

    async def _select_best_frame_candidate_with_vision(
        self,
        *,
        video_path: str,
        request: Dict[str, Any],
        candidates: List[Dict[str, Any]],
    ) -> Tuple[Dict[str, Any], str]:
        top_candidate = dict(candidates[0])
        prepared_candidates = self._build_candidate_images_for_vision_selection(
            video_path=video_path,
            candidates=candidates,
        )
        if len(prepared_candidates) <= 1:
            return top_candidate, "fallback_cv_top"

        analyzer = self.analyzer
        client = getattr(analyzer, "client", None)
        model = str(getattr(analyzer, "model", "") or "").strip()
        if client is None or not model:
            return top_candidate, "fallback_cv_top"

        user_text = self._build_best_frame_vision_user_text(
            request=request,
            prepared_candidates=prepared_candidates,
        )
        content_items: List[Dict[str, Any]] = [{"type": "text", "text": user_text}]
        for item in prepared_candidates:
            content_items.append(
                {
                    "type": "image_url",
                    "image_url": {"url": str(item.get("data_uri", "") or "")},
                }
            )

        messages = [
            {
                "role": "system",
                "content": str(self._best_frame_selection_prompt or self._get_default_best_frame_selection_prompt()),
            },
            {
                "role": "user",
                "content": content_items,
            },
        ]

        try:
            result = await llm_gateway.vl_chat_completion(
                client=client,
                model=model,
                messages=messages,
                max_tokens=int(self.best_frame_vision_max_tokens),
                temperature=0.0,
                timeout=float(self.best_frame_vision_timeout_sec),
            )
        except Exception as error:
            logger.warning("[VL] best-frame vision selection failed: %s", error)
            return top_candidate, "fallback_cv_top"

        raw_text = self._extract_vl_response_text(getattr(result, "content", ""))
        selected_index = self._parse_best_frame_selection_index(
            raw_text,
            len(prepared_candidates),
        )
        if selected_index is None:
            logger.warning(
                "[VL] best-frame vision selection returned invalid choice, fallback to CV top: response=%s",
                raw_text[:160],
            )
            return top_candidate, "fallback_cv_top"

        selected_candidate = prepared_candidates[selected_index].get("candidate")
        if not isinstance(selected_candidate, dict):
            return top_candidate, "fallback_cv_top"
        return dict(selected_candidate), "ai"

    async def _apply_best_frame_vision_selection(
        self,
        *,
        video_path: str,
        screenshot_requests: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        if not screenshot_requests:
            return screenshot_requests
        if not self.best_frame_vision_select_enabled:
            return screenshot_requests

        multi_candidate_count = 0
        ai_selected_count = 0
        fallback_selected_count = 0
        max_candidates = max(2, int(self.best_frame_vision_max_candidates))

        for req in screenshot_requests:
            if not isinstance(req, dict):
                continue
            current_ts = safe_float(req.get("timestamp_sec", 0.0), 0.0)
            current_score = safe_float(req.get("_cv_quality_score", req.get("score", 0.0)), 0.0)
            candidates = self._normalize_cv_candidate_screenshots(
                req.get("_cv_candidate_screenshots"),
                fallback_timestamp=current_ts,
                fallback_score=current_score,
            )
            candidates = candidates[:max_candidates]
            if len(candidates) <= 1:
                req["_cv_candidate_screenshots"] = candidates[:1]
                only_candidate = candidates[0]
                req["timestamp_sec"] = float(only_candidate.get("timestamp_sec", current_ts))
                req["_cv_quality_score"] = float(only_candidate.get("score", current_score))
                continue

            multi_candidate_count += 1
            selected_candidate, source = await self._select_best_frame_candidate_with_vision(
                video_path=video_path,
                request=req,
                candidates=candidates,
            )

            selected_ts = safe_float(selected_candidate.get("timestamp_sec", current_ts), current_ts)
            selected_score = safe_float(selected_candidate.get("score", current_score), current_score)
            req["timestamp_sec"] = float(selected_ts)
            req["_cv_quality_score"] = float(selected_score)
            req["_cv_candidate_screenshots"] = [dict(selected_candidate)]
            req["_cv_vision_selection_source"] = source

            if source == "ai":
                ai_selected_count += 1
            else:
                fallback_selected_count += 1

        if multi_candidate_count > 0:
            logger.info(
                "[VL] best-frame vision selection summary: multi=%s, ai=%s, fallback=%s",
                multi_candidate_count,
                ai_selected_count,
                fallback_selected_count,
            )
        return screenshot_requests

    def _build_unit_relative_asset_id(self, semantic_unit_id: str, file_stem: str) -> str:
        """构建 unit_id/file_stem 形式的素材 ID，保持与 VL 产物命名一致。"""
        default_stem = f"{str(semantic_unit_id or '').strip() or 'SU000'}_asset_001"
        return build_unit_relative_asset_id(semantic_unit_id, file_stem, default_stem=default_stem)

    def _build_tutorial_unit_dir(self, output_dir: str, unit_id: str) -> Optional[Path]:
        """构建并确保教程资产输出目录存在。"""
        if not output_dir:
            return None
        safe_unit_id = str(unit_id or "UNKNOWN").strip() or "UNKNOWN"
        base_dir = Path(output_dir) / self.tutorial_assets_root_dir / safe_unit_id
        base_dir.mkdir(parents=True, exist_ok=True)
        return base_dir

    async def _export_clip_asset_with_ffmpeg(
        self,
        video_path: str,
        start_sec: float,
        end_sec: float,
        output_path: Path,
    ) -> bool:
        """调用 FFmpeg 导出教程步骤视频片段。"""
        return await export_clip_asset_with_ffmpeg(
            video_path=video_path,
            start_sec=start_sec,
            end_sec=end_sec,
            output_path=output_path,
            logger=logger,
        )

    async def _export_keyframe_with_ffmpeg(
        self,
        video_path: str,
        timestamp_sec: float,
        output_path: Path,
    ) -> bool:
        """调用 FFmpeg 导出教程关键帧图片。"""
        return await export_keyframe_with_ffmpeg(
            video_path=video_path,
            timestamp_sec=timestamp_sec,
            output_path=output_path,
            logger=logger,
            iframe_search_window_sec=self.tutorial_keyframe_iframe_search_window_sec,
            iframe_search_before_sec=self.tutorial_keyframe_iframe_search_before_sec,
            iframe_search_after_sec=self.tutorial_keyframe_iframe_search_after_sec,
            select_sharpest_iframe=self.tutorial_keyframe_select_sharpest_iframe,
        )

    async def _export_keyframes_with_ffmpeg_batch(
        self,
        video_path: str,
        keyframe_jobs: List[Dict[str, Any]],
    ) -> List[bool]:
        if not keyframe_jobs:
            return []

        keyframes: List[Tuple[float, Path]] = []
        index_map: List[int] = []
        for index, key_job in enumerate(keyframe_jobs):
            output_path = key_job.get("output_path")
            if not output_path:
                continue
            keyframes.append(
                (
                    safe_float(key_job.get("timestamp_sec", 0.0), 0.0),
                    Path(output_path),
                )
            )
            index_map.append(index)

        results = [False for _ in keyframe_jobs]
        if not keyframes:
            return results

        batch_results = await export_keyframes_with_ffmpeg_batch(
            video_path=video_path,
            keyframes=keyframes,
            logger=logger,
        )
        for relative_index, source_index in enumerate(index_map):
            if relative_index < len(batch_results):
                results[source_index] = bool(batch_results[relative_index])
        return results

    async def _save_tutorial_assets_for_unit(
        self,
        video_path: str,
        output_dir: str,
        unit_id: str,
        clip_requests: List[Dict[str, Any]],
        screenshot_requests: List[Dict[str, Any]],
        raw_response_json: List[Dict[str, Any]],
        raw_llm_interactions: Optional[List[Dict[str, Any]]] = None,
        use_analysis_relative_timestamps: bool = False,
        prefer_screenshot_requests_keyframes: bool = False,
    ) -> None:
        """
        Persist tutorial assets per semantic unit:
        - step JSON
        - step clips
        - instructional keyframes
        """
        if not self.tutorial_export_assets:
            return

        unit_dir = self._build_tutorial_unit_dir(output_dir, unit_id)
        if unit_dir is None:
            return

        tutorial_clips = [
            c for c in (clip_requests or [])
            if str(c.get("analysis_mode", "")).strip().lower() == "tutorial_stepwise"
            and str(c.get("semantic_unit_id", "")).strip() == str(unit_id)
        ]
        if not tutorial_clips and not (raw_response_json or []):
            return

        tutorial_screenshots = [
            s for s in (screenshot_requests or [])
            if str(s.get("analysis_mode", "")).strip().lower() == "tutorial_stepwise"
            and str(s.get("semantic_unit_id", "")).strip() == str(unit_id)
        ]
        use_relative_ts = bool(use_analysis_relative_timestamps)
        prefer_screenshot_keyframes = bool(prefer_screenshot_requests_keyframes)

        def _normalize_text_list(value: Any) -> List[str]:
            if value is None:
                return []
            raw_items: List[Any]
            if isinstance(value, (list, tuple, set)):
                raw_items = list(value)
            elif isinstance(value, str):
                text = value.strip()
                if not text:
                    return []
                try:
                    parsed = json.loads(text)
                except Exception:
                    parsed = None
                if isinstance(parsed, list):
                    raw_items = parsed
                else:
                    raw_items = [segment for segment in re.split(r"[\n;；]+", text) if segment and segment.strip()]
            else:
                raw_items = [value]

            normalized: List[str] = []
            seen: set[str] = set()
            for item in raw_items:
                text_item = str(item or "").strip()
                if not text_item:
                    continue
                dedup_key = text_item.lower()
                if dedup_key in seen:
                    continue
                seen.add(dedup_key)
                normalized.append(text_item)
            return normalized

        def _normalize_main_operation(value: Any) -> str:
            if value is None:
                return ""
            if isinstance(value, str):
                text = value.strip()
                if not text:
                    return ""
                try:
                    parsed = json.loads(text)
                except Exception:
                    parsed = None
                if isinstance(parsed, list):
                    return "\n".join(_normalize_text_list(parsed)).strip()
                return text
            if isinstance(value, (list, tuple, set)):
                return "\n".join(_normalize_text_list(list(value))).strip()
            return str(value).strip()

        def _normalize_step_type(value: Any) -> str:
            text = str(value or "").strip().upper()
            if text in {"CONDITIONAL", "CONDITION", "BRANCH"}:
                return "CONDITIONAL"
            if text in {"OPTIONAL", "OPTION"}:
                return "OPTIONAL"
            if text in {"TROUBLESHOOTING", "TROUBLESHOOT", "DEBUG", "ERROR_FIX", "RECOVERY"}:
                return "TROUBLESHOOTING"
            return "MAIN_FLOW"

        def _normalize_instructional_keyframe_objects(
            value: Any,
            *,
            start_sec: float,
            end_sec: float,
        ) -> List[Dict[str, Any]]:
            if not isinstance(value, list):
                return []
            normalized: List[Dict[str, Any]] = []
            for item in value:
                if not isinstance(item, dict):
                    continue
                raw_ts = item.get("timestamp_sec", item.get("timestamp", item.get("ts", None)))
                ts = safe_float(raw_ts, start_sec)
                if ts < start_sec:
                    ts = start_sec
                elif ts > end_sec:
                    ts = end_sec
                entry: Dict[str, Any] = {
                    "timestamp_sec": float(ts),
                    "frame_reason": str(item.get("frame_reason", "") or "").strip(),
                }
                bbox = normalize_bbox_1000(item.get("bbox"))
                if bbox is not None:
                    entry["bbox"] = bbox
                normalized.append(entry)
            return normalized

        def _normalize_instructional_clip_id(value: Any, fallback_index: int) -> str:
            fallback = max(1, int(fallback_index))
            text_value = str(value or "").strip()
            if not text_value:
                return f"CLIP_{fallback}"
            matched = re.search(r"CLIP[_\-\s]*(\d+)", text_value, flags=re.IGNORECASE)
            if matched:
                return f"CLIP_{int(matched.group(1))}"
            if re.fullmatch(r"\d+", text_value):
                return f"CLIP_{int(text_value)}"
            return f"CLIP_{fallback}"

        def _normalize_instructional_clip_objects(
            value: Any,
            *,
            start_sec: float,
            end_sec: float,
        ) -> List[Dict[str, Any]]:
            if not isinstance(value, list):
                return []
            normalized: List[Dict[str, Any]] = []
            max_duration_sec = 5.0
            for index, item in enumerate(value, start=1):
                if not isinstance(item, dict):
                    continue
                clip_start = safe_float(
                    item.get("start_sec", item.get("clip_start_sec", item.get("start", start_sec))),
                    start_sec,
                )
                clip_end = safe_float(
                    item.get("end_sec", item.get("clip_end_sec", item.get("end", clip_start))),
                    clip_start,
                )
                if clip_end < clip_start:
                    clip_start, clip_end = clip_end, clip_start
                clip_start = max(start_sec, min(clip_start, end_sec))
                clip_end = max(start_sec, min(clip_end, end_sec))
                if clip_end - clip_start > max_duration_sec:
                    clip_end = min(end_sec, clip_start + max_duration_sec)
                if clip_end <= clip_start:
                    continue
                normalized.append(
                    {
                        "clip_id": _normalize_instructional_clip_id(
                            item.get("clip_id", item.get("clipId", item.get("id"))),
                            fallback_index=index,
                        ),
                        "start_sec": float(clip_start),
                        "end_sec": float(clip_end),
                        "clip_reason": str(item.get("clip_reason", item.get("reason", "")) or "").strip(),
                    }
                )
            return normalized

        raw_steps_by_id: Dict[int, Dict[str, Any]] = {}
        for raw_step in raw_response_json or []:
            if not isinstance(raw_step, dict):
                continue
            step_key = int(safe_float(raw_step.get("step_id", 0), 0.0))
            if step_key <= 0:
                continue
            raw_steps_by_id[step_key] = raw_step

        screenshots_by_step: Dict[int, List[Dict[str, Any]]] = {}
        for ss in tutorial_screenshots:
            step_id = int(safe_float(ss.get("step_id", 0), 0.0))
            screenshots_by_step.setdefault(step_id, []).append(ss)
        for step_ss in screenshots_by_step.values():
            if use_relative_ts:
                step_ss.sort(
                    key=lambda x: safe_float(
                        x.get(
                            "_analysis_relative_timestamp",
                            x.get("_relative_timestamp", x.get("timestamp_sec", 0.0)),
                        ),
                        0.0,
                    )
                )
            else:
                step_ss.sort(key=lambda x: float(x.get("timestamp_sec", 0.0)))

        primary_clips_by_step: Dict[int, List[Dict[str, Any]]] = {}
        instructional_clips_by_step: Dict[int, List[Dict[str, Any]]] = {}
        for clip in tutorial_clips:
            step_id = int(safe_float(clip.get("step_id", 0), 0.0))
            if step_id <= 0:
                continue
            if str(clip.get("instructional_clip_id", "") or "").strip():
                instructional_clips_by_step.setdefault(step_id, []).append(clip)
            else:
                primary_clips_by_step.setdefault(step_id, []).append(clip)

        def _clip_sort_key(clip_item: Dict[str, Any]) -> float:
            if use_relative_ts:
                return safe_float(
                    clip_item.get(
                        "_analysis_relative_start_sec",
                        clip_item.get("start_sec", 0.0),
                    ),
                    0.0,
                )
            return safe_float(clip_item.get("start_sec", 0.0), 0.0)

        for clip_group in primary_clips_by_step.values():
            clip_group.sort(key=_clip_sort_key)
        for clip_group in instructional_clips_by_step.values():
            clip_group.sort(key=_clip_sort_key)

        step_ids = sorted(
            set(raw_steps_by_id.keys())
            | set(screenshots_by_step.keys())
            | set(primary_clips_by_step.keys())
            | set(instructional_clips_by_step.keys())
        )

        export_workers = self._resolve_tutorial_asset_export_workers(
            max(1, len(step_ids) + sum(len(items) for items in instructional_clips_by_step.values()))
        )
        export_semaphore = asyncio.Semaphore(export_workers)
        logger.info(
            f"[VL-Tutorial] asset export parallel: unit={unit_id}, steps={len(step_ids)}, workers={export_workers}"
        )

        async def _run_limited(awaitable: Any) -> Any:
            async with export_semaphore:
                return await awaitable

        step_jobs: List[Dict[str, Any]] = []
        ext = "jpg" if self.tutorial_keyframe_image_ext == "jpeg" else self.tutorial_keyframe_image_ext
        for idx, step_key in enumerate(step_ids, start=1):
            step_index = step_key if step_key > 0 else idx
            raw_step = raw_steps_by_id.get(step_index, {})
            primary_clips = list(primary_clips_by_step.get(step_index, []) or [])
            primary_clip = primary_clips[0] if primary_clips else {}
            step_instructional_clip_requests = list(instructional_clips_by_step.get(step_index, []) or [])

            step_description = str(
                primary_clip.get("step_description")
                or raw_step.get("step_description")
                or raw_step.get("description")
                or ""
            ).strip()
            step_type = _normalize_step_type(
                primary_clip.get("step_type", raw_step.get("step_type", raw_step.get("stepType", "")))
            )
            main_action = str(
                primary_clip.get("main_action")
                or raw_step.get("main_action")
                or ""
            ).strip()
            raw_main_operation = raw_step.get("main_operation", None)
            if raw_main_operation is None:
                raw_main_operation = raw_step.get("main_operations", None)
            if raw_main_operation is None:
                raw_main_operation = primary_clip.get("main_operation")
            main_operation = _normalize_main_operation(raw_main_operation)
            raw_precautions = primary_clip.get("precautions")
            if raw_precautions is None:
                raw_precautions = raw_step.get("precautions", None)
            if raw_precautions is None:
                raw_precautions = raw_step.get("notes", None)
            precautions = _normalize_text_list(raw_precautions)
            step_summary = str(
                primary_clip.get("step_summary")
                or raw_step.get("step_summary")
                or raw_step.get("summary")
                or ""
            ).strip()
            raw_operation_guidance = primary_clip.get("operation_guidance")
            if raw_operation_guidance is None:
                raw_operation_guidance = raw_step.get("operation_guidance", None)
            if raw_operation_guidance is None:
                raw_operation_guidance = raw_step.get("guidance", None)
            operation_guidance = _normalize_text_list(raw_operation_guidance)
            action_brief = self._slugify_action_brief(
                str(primary_clip.get("action_brief", "") or step_description),
            )
            if action_brief == "action" and step_description:
                action_brief = self._slugify_action_brief(step_description)

            fallback_clip_for_bounds = primary_clip or (step_instructional_clip_requests[0] if step_instructional_clip_requests else {})
            if use_relative_ts:
                start_sec = safe_float(
                    raw_step.get(
                        "clip_start_sec",
                        fallback_clip_for_bounds.get("_analysis_relative_start_sec", fallback_clip_for_bounds.get("start_sec", 0.0)),
                    ),
                    0.0,
                )
                end_sec = safe_float(
                    raw_step.get(
                        "clip_end_sec",
                        fallback_clip_for_bounds.get("_analysis_relative_end_sec", fallback_clip_for_bounds.get("end_sec", start_sec)),
                    ),
                    start_sec,
                )
                start_sec = max(0.0, start_sec)
                end_sec = max(0.0, end_sec)
            else:
                start_sec = safe_float(
                    fallback_clip_for_bounds.get(
                        "start_sec",
                        raw_step.get("clip_start_sec", 0.0),
                    ),
                    0.0,
                )
                end_sec = safe_float(
                    fallback_clip_for_bounds.get(
                        "end_sec",
                        raw_step.get("clip_end_sec", start_sec),
                    ),
                    start_sec,
                )
            if end_sec < start_sec:
                start_sec, end_sec = end_sec, start_sec

            clip_filename = f"{unit_id}_clip_step_{step_index:02d}_{action_brief}.mp4"
            clip_output_path = unit_dir / clip_filename if primary_clip else None

            fallback_keyframes = screenshots_by_step.get(step_index, [])
            if not fallback_keyframes and step_index <= 0:
                fallback_keyframes = screenshots_by_step.get(idx, [])

            def _build_step_keyframes_from_fallback() -> List[Dict[str, Any]]:
                built: List[Dict[str, Any]] = []
                for fallback in fallback_keyframes:
                    fallback_ts = safe_float(
                        (
                            fallback.get(
                                "_analysis_relative_timestamp",
                                fallback.get("_relative_timestamp", fallback.get("timestamp_sec", start_sec)),
                            )
                            if use_relative_ts
                            else fallback.get("timestamp_sec", start_sec)
                        ),
                        start_sec,
                    )
                    if fallback_ts < start_sec:
                        fallback_ts = start_sec
                    elif fallback_ts > end_sec:
                        fallback_ts = end_sec
                    fallback_item: Dict[str, Any] = {
                        "timestamp_sec": float(fallback_ts),
                        "frame_reason": str(fallback.get("frame_reason", "") or "").strip(),
                    }
                    bbox = normalize_bbox_1000(fallback.get("bbox"))
                    if bbox is not None:
                        fallback_item["bbox"] = bbox
                    built.append(fallback_item)
                return built

            step_keyframes: List[Dict[str, Any]] = []
            if prefer_screenshot_keyframes and fallback_keyframes:
                step_keyframes = _build_step_keyframes_from_fallback()
            else:
                step_keyframes = _normalize_instructional_keyframe_objects(
                    raw_step.get("instructional_keyframes", None),
                    start_sec=start_sec,
                    end_sec=end_sec,
                )
                if not step_keyframes:
                    step_keyframes = _build_step_keyframes_from_fallback()

            keyframe_jobs: List[Dict[str, Any]] = []
            for key_idx, step_ss in enumerate(step_keyframes, start=1):
                key_ts = safe_float(step_ss.get("timestamp_sec", start_sec), start_sec)
                if key_idx == 1:
                    key_name = f"{unit_id}_ss_step_{step_index:02d}_key_01_{action_brief}.{ext}"
                else:
                    key_name = f"{unit_id}_ss_step_{step_index:02d}_key_{key_idx:02d}_{action_brief}.{ext}"
                keyframe_jobs.append(
                    {
                        "key_name": key_name,
                        "timestamp_sec": key_ts,
                        "frame_reason": str(step_ss.get("frame_reason", "") or "").strip(),
                        "bbox": normalize_bbox_1000(step_ss.get("bbox")),
                        "output_path": unit_dir / key_name,
                    }
                )

            raw_instructional_clips = _normalize_instructional_clip_objects(
                raw_step.get("instructional_clips", None),
                start_sec=start_sec,
                end_sec=end_sec,
            )
            unmatched_instructional_reqs = list(step_instructional_clip_requests)

            def _pop_instructional_clip_request(clip_token: str) -> Optional[Dict[str, Any]]:
                normalized_token = str(clip_token or "").strip().upper()
                for req_index, req in enumerate(unmatched_instructional_reqs):
                    req_token = str(req.get("instructional_clip_id", "") or "").strip().upper()
                    if normalized_token and req_token == normalized_token:
                        return unmatched_instructional_reqs.pop(req_index)
                if unmatched_instructional_reqs:
                    return unmatched_instructional_reqs.pop(0)
                return None

            instructional_clip_jobs: List[Dict[str, Any]] = []
            for clip_idx, clip_item in enumerate(raw_instructional_clips, start=1):
                clip_token = _normalize_instructional_clip_id(clip_item.get("clip_id"), clip_idx)
                request_meta = _pop_instructional_clip_request(clip_token)
                if use_relative_ts:
                    clip_job_start = safe_float(
                        (request_meta or {}).get("_analysis_relative_start_sec", clip_item.get("start_sec", start_sec)),
                        start_sec,
                    )
                    clip_job_end = safe_float(
                        (request_meta or {}).get("_analysis_relative_end_sec", clip_item.get("end_sec", clip_job_start)),
                        clip_job_start,
                    )
                else:
                    clip_job_start = safe_float(
                        (request_meta or {}).get("start_sec", clip_item.get("start_sec", start_sec)),
                        start_sec,
                    )
                    clip_job_end = safe_float(
                        (request_meta or {}).get("end_sec", clip_item.get("end_sec", clip_job_start)),
                        clip_job_start,
                    )
                if clip_job_end < clip_job_start:
                    clip_job_start, clip_job_end = clip_job_end, clip_job_start
                clip_job_start = max(start_sec, min(clip_job_start, end_sec))
                clip_job_end = max(start_sec, min(clip_job_end, end_sec))
                if clip_job_end <= clip_job_start:
                    continue
                clip_number_match = re.search(r"(\d+)$", clip_token)
                clip_number = int(clip_number_match.group(1)) if clip_number_match else clip_idx
                request_clip_id = str((request_meta or {}).get("clip_id", "") or "").strip()
                clip_job_filename = f"{Path(request_clip_id).name}.mp4" if request_clip_id else (
                    f"{unit_id}_clip_step_{step_index:02d}_clip_{clip_number:02d}_{action_brief}.mp4"
                )
                instructional_clip_jobs.append(
                    {
                        "instructional_clip_id": clip_token,
                        "clip_reason": str(clip_item.get("clip_reason", "") or (request_meta or {}).get("clip_reason", "")).strip(),
                        "start_sec": float(clip_job_start),
                        "end_sec": float(clip_job_end),
                        "clip_filename": clip_job_filename,
                        "output_path": unit_dir / clip_job_filename,
                    }
                )

            for extra_idx, request_meta in enumerate(unmatched_instructional_reqs, start=len(instructional_clip_jobs) + 1):
                clip_token = _normalize_instructional_clip_id(request_meta.get("instructional_clip_id"), extra_idx)
                if use_relative_ts:
                    clip_job_start = safe_float(request_meta.get("_analysis_relative_start_sec", start_sec), start_sec)
                    clip_job_end = safe_float(request_meta.get("_analysis_relative_end_sec", clip_job_start), clip_job_start)
                else:
                    clip_job_start = safe_float(request_meta.get("start_sec", start_sec), start_sec)
                    clip_job_end = safe_float(request_meta.get("end_sec", clip_job_start), clip_job_start)
                if clip_job_end < clip_job_start:
                    clip_job_start, clip_job_end = clip_job_end, clip_job_start
                if clip_job_end <= clip_job_start:
                    continue
                request_clip_id = str(request_meta.get("clip_id", "") or "").strip()
                clip_job_filename = f"{Path(request_clip_id).name}.mp4" if request_clip_id else (
                    f"{unit_id}_clip_step_{step_index:02d}_clip_{extra_idx:02d}_{action_brief}.mp4"
                )
                instructional_clip_jobs.append(
                    {
                        "instructional_clip_id": clip_token,
                        "clip_reason": str(request_meta.get("clip_reason", "") or "").strip(),
                        "start_sec": float(clip_job_start),
                        "end_sec": float(clip_job_end),
                        "clip_filename": clip_job_filename,
                        "output_path": unit_dir / clip_job_filename,
                    }
                )

            step_jobs.append(
                {
                    "step_index": step_index,
                    "step_description": step_description,
                    "step_type": step_type,
                    "main_action": main_action,
                    "main_operation": main_operation,
                    "precautions": precautions,
                    "step_summary": step_summary,
                    "operation_guidance": operation_guidance,
                    "action_brief": action_brief,
                    "start_sec": start_sec,
                    "end_sec": end_sec,
                    "clip_filename": clip_filename,
                    "clip_output_path": clip_output_path,
                    "instructional_clip_jobs": instructional_clip_jobs,
                    "keyframe_jobs": keyframe_jobs,
                }
            )

        async def _export_one_step(job: Dict[str, Any]) -> Dict[str, Any]:
            clip_output_path = job.get("clip_output_path")
            clip_ok = False
            if clip_output_path:
                try:
                    clip_ok = await _run_limited(
                        self._export_clip_asset_with_ffmpeg(
                            video_path=video_path,
                            start_sec=float(job["start_sec"]),
                            end_sec=float(job["end_sec"]),
                            output_path=clip_output_path,
                        )
                    )
                except Exception as error:
                    logger.warning(f"[VL-Tutorial] step clip export exception: unit={unit_id}, step={job.get('step_index')}, err={error}")
                    clip_ok = False

            instructional_clip_jobs = list(job.get("instructional_clip_jobs", []) or [])
            instructional_clip_results: List[Any] = []
            if instructional_clip_jobs:
                clip_tasks = [
                    asyncio.create_task(
                        _run_limited(
                            self._export_clip_asset_with_ffmpeg(
                                video_path=video_path,
                                start_sec=float(clip_job.get("start_sec", 0.0)),
                                end_sec=float(clip_job.get("end_sec", 0.0)),
                                output_path=clip_job["output_path"],
                            )
                        )
                    )
                    for clip_job in instructional_clip_jobs
                ]
                instructional_clip_results = await asyncio.gather(*clip_tasks, return_exceptions=True)

            keyframe_jobs = list(job.get("keyframe_jobs", []) or [])
            keyframe_results: List[Any] = []
            used_batch_export = False
            keyframe_export_t0 = time.perf_counter()
            can_batch_export = (
                len(keyframe_jobs) > 1
                and not bool(self.tutorial_keyframe_select_sharpest_iframe)
                and float(self.tutorial_keyframe_iframe_search_window_sec) <= 0.0
            )
            if keyframe_jobs and can_batch_export:
                try:
                    keyframe_results = await _run_limited(
                        self._export_keyframes_with_ffmpeg_batch(
                            video_path=video_path,
                            keyframe_jobs=keyframe_jobs,
                        )
                    )
                    used_batch_export = len(keyframe_results) == len(keyframe_jobs)
                    if not used_batch_export:
                        logger.warning(
                            "[VL-Tutorial] keyframe batch export size mismatch: unit=%s step=%s jobs=%s results=%s",
                            unit_id,
                            job.get("step_index"),
                            len(keyframe_jobs),
                            len(keyframe_results),
                        )
                        keyframe_results = []
                except Exception as error:
                    logger.warning(
                        "[VL-Tutorial] keyframe batch export exception: unit=%s step=%s err=%s",
                        unit_id,
                        job.get("step_index"),
                        error,
                    )
                    keyframe_results = []

            if keyframe_jobs and not used_batch_export:
                keyframe_tasks: List[asyncio.Task] = []
                for key_job in keyframe_jobs:
                    keyframe_tasks.append(
                        asyncio.create_task(
                            _run_limited(
                                self._export_keyframe_with_ffmpeg(
                                    video_path=video_path,
                                    timestamp_sec=float(key_job.get("timestamp_sec", 0.0)),
                                    output_path=key_job["output_path"],
                                )
                            )
                        )
                    )
                keyframe_results = await asyncio.gather(*keyframe_tasks, return_exceptions=True)

            if keyframe_jobs:
                keyframe_ok_count = sum(1 for item in keyframe_results if not isinstance(item, Exception) and bool(item))
                logger.info(
                    "[VL-Tutorial] keyframe export summary: unit=%s step=%s total=%s ok=%s batch=%s ms=%.1f",
                    unit_id,
                    job.get("step_index"),
                    len(keyframe_jobs),
                    keyframe_ok_count,
                    used_batch_export,
                    (time.perf_counter() - keyframe_export_t0) * 1000.0,
                )

            keyframe_files: List[str] = []
            keyframe_details: List[Dict[str, Any]] = []
            if keyframe_results:
                for key_job, key_result in zip(keyframe_jobs, keyframe_results):
                    if isinstance(key_result, Exception):
                        logger.warning(
                            f"[VL-Tutorial] keyframe export exception: unit={unit_id}, step={job.get('step_index')}, "
                            f"file={key_job.get('key_name')}, err={key_result}"
                        )
                        continue
                    if bool(key_result):
                        key_name = str(key_job.get("key_name", ""))
                        keyframe_post_t0 = time.perf_counter()
                        grid_anchor_meta = {"grid_anchor_status": "disabled_by_schema"}
                        frame_reason = str(key_job.get("frame_reason", "") or "").strip()
                        top_reason_banner_status = "skipped_no_reason"
                        output_path_value = key_job.get("output_path")
                        if frame_reason and output_path_value and not self.tutorial_top_reason_banner_enabled:
                            top_reason_banner_status = "disabled_by_config"
                        elif frame_reason and output_path_value:
                            output_path = Path(output_path_value)
                            banner_ok = save_top_reason_banner_image(
                                source_image_path=output_path,
                                output_image_path=output_path,
                                text=frame_reason,
                            )
                            top_reason_banner_status = "applied" if banner_ok else "failed"
                            if not banner_ok:
                                logger.warning(
                                    "[VL-Tutorial] top reason banner apply failed: unit=%s step=%s file=%s",
                                    unit_id,
                                    job.get("step_index"),
                                    key_name,
                                )
                        keyframe_post_ms = (time.perf_counter() - keyframe_post_t0) * 1000.0
                        if keyframe_post_ms >= 200.0:
                            logger.info(
                                "[VL-Tutorial] keyframe postprocess slow: unit=%s step=%s file=%s ms=%.1f status=%s banner=%s",
                                unit_id,
                                job.get("step_index"),
                                key_name,
                                keyframe_post_ms,
                                str((grid_anchor_meta or {}).get("grid_anchor_status", "")),
                                top_reason_banner_status,
                            )
                        keyframe_files.append(key_name)
                        key_detail: Dict[str, Any] = {
                            "image_file": key_name,
                            "timestamp_sec": float(key_job.get("timestamp_sec", 0.0)),
                            "frame_reason": str(key_job.get("frame_reason", "") or "").strip(),
                            "top_reason_banner_status": top_reason_banner_status,
                        }
                        bbox = normalize_bbox_1000(key_job.get("bbox"))
                        if bbox is not None:
                            key_detail["bbox"] = bbox
                        key_detail.update(dict(grid_anchor_meta or {}))
                        keyframe_details.append(key_detail)

            instructional_clip_files: List[str] = []
            instructional_clip_details: List[Dict[str, Any]] = []
            if instructional_clip_results:
                for clip_job, clip_result in zip(instructional_clip_jobs, instructional_clip_results):
                    if isinstance(clip_result, Exception):
                        logger.warning(
                            f"[VL-Tutorial] instructional clip export exception: unit={unit_id}, step={job.get('step_index')}, "
                            f"file={clip_job.get('clip_filename')}, err={clip_result}"
                        )
                        continue
                    if bool(clip_result):
                        clip_name = str(clip_job.get("clip_filename", "") or "").strip()
                        if not clip_name:
                            continue
                        instructional_clip_files.append(clip_name)
                        instructional_clip_details.append(
                            {
                                "clip_file": clip_name,
                                "instructional_clip_id": str(clip_job.get("instructional_clip_id", "") or "").strip(),
                                "clip_reason": str(clip_job.get("clip_reason", "") or "").strip(),
                                "start_sec": float(clip_job.get("start_sec", 0.0)),
                                "end_sec": float(clip_job.get("end_sec", 0.0)),
                            }
                        )

            return {
                "step_id": int(job["step_index"]),
                "step_description": str(job["step_description"]),
                "step_type": str(job.get("step_type", "MAIN_FLOW") or "MAIN_FLOW"),
                "main_action": str(job.get("main_action", "") or ""),
                "main_operation": str(job.get("main_operation", "") or ""),
                "precautions": list(job.get("precautions", []) or []),
                "step_summary": str(job.get("step_summary", "") or ""),
                "operation_guidance": list(job.get("operation_guidance", []) or []),
                "action_brief": str(job["action_brief"]),
                "clip_start_sec": float(job["start_sec"]),
                "clip_end_sec": float(job["end_sec"]),
                "clip_file": str(job["clip_filename"]) if clip_ok else "",
                "instructional_clips": instructional_clip_files,
                "instructional_clip_details": instructional_clip_details,
                "instructional_keyframes": keyframe_files,
                "instructional_keyframe_details": keyframe_details,
            }

        step_manifest = await asyncio.gather(*[_export_one_step(job) for job in step_jobs])

        if self.tutorial_save_step_json:
            json_payload = {
                "unit_id": unit_id,
                "schema": "tutorial_stepwise_v1",
                "raw_response": raw_response_json or [],
                "llm_interactions": raw_llm_interactions or [],
                "steps": step_manifest,
            }
            json_path = unit_dir / f"{unit_id}_steps.json"
            with open(json_path, "w", encoding="utf-8") as file_obj:
                json.dump(json_payload, file_obj, ensure_ascii=False, indent=2)

    
    def _normalize_intervals(self, intervals: List[Tuple[float, float]], min_duration_sec: float = 1e-6) -> List[Tuple[float, float]]:
        """
        将区间列表排序并合并重叠/相邻区间銆?

        为什么：稳定区间可能来自不同检测片段，先规范化可避免后续剪裁时重复处理銆?
        """
        return normalize_intervals(intervals, min_duration_sec=min_duration_sec)

    def _subtract_intervals(
        self,
        base_interval: Tuple[float, float],
        removed_intervals: List[Tuple[float, float]],
        min_keep_segment_sec: float,
    ) -> List[Tuple[float, float]]:
        """
        鍦?base 区间内扣闄?removed 区间，得到保留区间銆?

        为什么：stable 剔除的本质是区间差集，显式实现便于调试与单元测试验证边界銆?
        """
        return subtract_intervals(
            base_interval,
            removed_intervals,
            min_keep_segment_sec=min_keep_segment_sec,
        )

    def _subtract_removed_from_segments(
        self,
        *,
        segments: List[Tuple[float, float]],
        removed_intervals: List[Tuple[float, float]],
        min_keep_segment_sec: float,
    ) -> List[Tuple[float, float]]:
        """
        Subtract removed intervals from multiple segments and normalize the result.
        """
        normalized_segments = self._normalize_intervals(
            segments,
            min_duration_sec=min_keep_segment_sec,
        )
        normalized_removed = self._normalize_intervals(
            removed_intervals,
            min_duration_sec=1e-6,
        )
        if not normalized_segments or not normalized_removed:
            return normalized_segments

        kept: List[Tuple[float, float]] = []
        for seg_start, seg_end in normalized_segments:
            kept.extend(
                self._subtract_intervals(
                    base_interval=(seg_start, seg_end),
                    removed_intervals=normalized_removed,
                    min_keep_segment_sec=min_keep_segment_sec,
                )
            )
        return self._normalize_intervals(
            kept,
            min_duration_sec=min_keep_segment_sec,
        )

    def _build_pruning_context_prompt(
        self,
        semantic_unit: Dict[str, Any],
        kept_segments: List[Tuple[float, float]],
        removed_segments: List[Tuple[float, float]],
    ) -> str:
        """构建剪枝上下文提示，帮助 VL 理解时间跳转。"""
        knowledge_topic = str(semantic_unit.get("knowledge_topic", "") or "").strip()
        full_text = str(semantic_unit.get("full_text", "") or "").strip()
        text = str(semantic_unit.get("text", "") or "").strip()
        context_text = full_text or text
        if len(context_text) > self.pre_vl_context_text_max_chars:
            context_text = context_text[: self.pre_vl_context_text_max_chars].rstrip() + "..."

        def _fmt_segments(segments: List[Tuple[float, float]]) -> str:
            if not segments:
                return "无"
            return "，".join([f"[{s:.2f}s-{e:.2f}s]" for s, e in segments])

        return (
            "【VL前置上下文说明】\n"
            "当前输入并非完整语义单元视频，而是剪除长时间静态段后的拼接片段。\n"
            f"语义单元主题(knowledge_topic)：{knowledge_topic or '未知'}\n"
            f"语义单元上下文：{context_text or '无'}\n"
            f"保留片段(原始时间轴)：{_fmt_segments(kept_segments)}\n"
            f"已移除静态片段核心区(原始时间轴)：{_fmt_segments(removed_segments)}\n"
            "请基于以上上下文理解时间跳转，不要将拼接处误判为语义突变。"
        )

    def _build_removed_intervals_from_stable(self, stable_intervals: List[Tuple[float, float]]) -> List[Tuple[float, float]]:
        """
        根据 stable 区间构建可剔除的核心区间銆?

        规则锛?
        1) stable 原始时长必须严格大于 `min_stable_interval_sec`（默璁?3s）；
        2) 两侧各保鐣?`keep_edge_sec`，仅剔除中间核心段；
        3) 核心段时长至灏?`min_cut_span_sec`銆?
        """
        return build_removed_intervals_from_stable(
            stable_intervals,
            min_stable_interval_sec=self.pre_vl_min_stable_interval_sec,
            keep_edge_sec=self.pre_vl_keep_edge_sec,
            min_cut_span_sec=self.pre_vl_min_cut_span_sec,
        )

    def _get_subtitle_repo_for_output_dir(self, output_dir: str) -> SubtitleRepository:
        """按输出目录获取并缓存字幕仓库对象。"""
        cache_key = str(Path(output_dir).resolve())
        repository = self._subtitle_repo_cache.get(cache_key)
        if repository is not None:
            return repository

        repository = SubtitleRepository.from_output_dir(output_dir=cache_key)
        self._subtitle_repo_cache[cache_key] = repository
        return repository

    def _load_subtitles_for_output_dir(self, output_dir: str) -> List[Dict[str, Any]]:
        """加载输出目录对应的字幕列表。"""
        repository = self._get_subtitle_repo_for_output_dir(output_dir)
        return repository.list_subtitles()

    def _build_unit_relative_subtitles(
        self,
        subtitles: List[Dict[str, Any]],
        unit_start_sec: float,
        unit_end_sec: float,
    ) -> List[Dict[str, Any]]:
        """将绝对时间字幕映射为单元内相对时间字幕。"""
        output_dir = getattr(self, "_current_subtitle_output_dir", "")
        if output_dir:
            repository = self._get_subtitle_repo_for_output_dir(output_dir)
            return repository.build_relative_subtitles(
                unit_start_sec=unit_start_sec,
                unit_end_sec=unit_end_sec,
            )

        if unit_end_sec <= unit_start_sec:
            return []

        unit_duration = unit_end_sec - unit_start_sec
        result: List[Dict[str, Any]] = []
        for sub in subtitles:
            sub_start = safe_float(sub.get("start_sec", 0.0), 0.0)
            sub_end = safe_float(sub.get("end_sec", 0.0), 0.0)
            if sub_end <= unit_start_sec or sub_start >= unit_end_sec:
                continue

            rel_start = max(0.0, sub_start - unit_start_sec)
            rel_end = min(unit_duration, sub_end - unit_start_sec)
            if rel_end <= rel_start:
                continue

            result.append(
                {
                    "start_sec": rel_start,
                    "end_sec": rel_end,
                    "text": str(sub.get("text", "") or ""),
                }
            )
        return result

    def _split_complete_sentences_by_pause(self, subtitles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        使用停顿阈值切分口语句銆?
        为什么：ASR 常见无标点长流文本，需用停顿模拟“完整语义句”銆?
        """
        if not subtitles:
            return []

        pause_threshold = max(0.0, self.pre_vl_pause_threshold_sec)
        sentences: List[Dict[str, Any]] = []
        current_sentence: Optional[Dict[str, Any]] = None

        for sub in subtitles:
            start_sec = safe_float(sub.get("start_sec", 0.0), 0.0)
            end_sec = safe_float(sub.get("end_sec", 0.0), 0.0)
            text = str(sub.get("text", "") or "")

            if current_sentence is None:
                current_sentence = {
                    "start_sec": start_sec,
                    "end_sec": end_sec,
                    "text": text,
                }
                continue

            pause_gap = start_sec - safe_float(current_sentence.get("end_sec", start_sec), start_sec)
            if pause_gap >= pause_threshold:
                sentences.append(current_sentence)
                current_sentence = {
                    "start_sec": start_sec,
                    "end_sec": end_sec,
                    "text": text,
                }
            else:
                current_sentence["end_sec"] = max(safe_float(current_sentence.get("end_sec", end_sec), end_sec), end_sec)
                current_sentence["text"] = str(current_sentence.get("text", "") or "") + text

        if current_sentence is not None:
            sentences.append(current_sentence)

        return sentences

    def _pick_sentence_for_anchor(
        self,
        sentences: List[Dict[str, Any]],
        anchor_sec: float,
        is_start: bool,
    ) -> Optional[Dict[str, Any]]:
        """
        在语义句列表中为边界锚点挑选最优句銆?
        为什么：优先使用“引导词/确认词”可减少截断句首句尾的概率銆?
        """
        if not sentences:
            return None

        search_window = max(0.0, self.pre_vl_semantic_search_window_sec)
        if is_start:
            keyword_set = {
                "下面", "接下来", "我们来看", "首先", "然后", "先看", "先讲", "next", "first",
            }
        else:
            keyword_set = {
                "好了", "这就是", "总结", "也就是说", "结果是", "完成", "最后", "done", "finally",
            }

        near_sentences: List[Dict[str, Any]] = []
        for sentence in sentences:
            sentence_anchor = safe_float(sentence.get("start_sec" if is_start else "end_sec", anchor_sec), anchor_sec)
            if abs(sentence_anchor - anchor_sec) <= search_window:
                near_sentences.append(sentence)

        if not near_sentences:
            return None

        keyword_sentences = [
            sentence
            for sentence in near_sentences
            if any(keyword in str(sentence.get("text", "") or "") for keyword in keyword_set)
        ]
        candidate_sentences = keyword_sentences if keyword_sentences else near_sentences

        containing_sentences = [
            sentence for sentence in candidate_sentences
            if safe_float(sentence.get("start_sec", 0.0), 0.0) <= anchor_sec <= safe_float(sentence.get("end_sec", 0.0), 0.0)
        ]
        if containing_sentences:
            candidate_sentences = containing_sentences

        candidate_sentences.sort(
            key=lambda sentence: abs(
                safe_float(sentence.get("start_sec" if is_start else "end_sec", anchor_sec), anchor_sec) - anchor_sec
            )
        )
        return candidate_sentences[0]

    def _get_complete_semantic_baseline_for_segment(
        self,
        seg_start_sec: float,
        seg_end_sec: float,
        sentences: List[Dict[str, Any]],
    ) -> Tuple[float, float]:
        """
        给单个待拼接片段计算“完整语义单元基线”銆?
        为什么：在稳定段剔除后，原始 kept 区间常落在句中，直接拼接会造成语义断裂銆?
        """
        if not sentences:
            return seg_start_sec, seg_end_sec

        start_sentence = self._pick_sentence_for_anchor(sentences, seg_start_sec, is_start=True)
        end_sentence = self._pick_sentence_for_anchor(sentences, seg_end_sec, is_start=False)

        final_start = safe_float(start_sentence.get("start_sec", seg_start_sec), seg_start_sec) if start_sentence else seg_start_sec
        final_end = safe_float(end_sentence.get("end_sec", seg_end_sec), seg_end_sec) if end_sentence else seg_end_sec
        if final_end < final_start:
            final_start, final_end = seg_start_sec, seg_end_sec
        return final_start, final_end

    async def _detect_segment_mse_jump_end(
        self,
        clip_path: str,
        semantic_end_sec: float,
        clip_duration_sec: float,
    ) -> float:
        """
        使用 MSE 检测片段结束后的物理跳变点銆?
        为什么：口语句可能先结束、画面后翻页；结束点应覆盖物理动作的完成銆?
        """
        scan_after_end_sec = max(0.0, self.pre_vl_mse_scan_after_end_sec)
        scan_start = max(0.0, semantic_end_sec)
        scan_end = min(max(0.0, clip_duration_sec), semantic_end_sec + scan_after_end_sec)
        if scan_end - scan_start <= 0.2:
            return semantic_end_sec

        try:
            from services.python_grpc.src.content_pipeline.phase2a.vision.visual_feature_extractor import VisualFeatureExtractor

            extractor = VisualFeatureExtractor(clip_path)
            try:
                source_fps = float(getattr(extractor, "fps", 30.0) or 30.0)
                sample_fps = max(0.1, self.pre_vl_mse_sample_fps)
                sample_rate = max(1, int(round(source_fps / sample_fps)))
                frames, timestamps = extractor.extract_frames_fast(
                    scan_start,
                    scan_end,
                    sample_rate=sample_rate,
                    target_height=360,
                    register_to_shm=False,
                )
                if len(frames) < 2 or len(timestamps) < 2:
                    return semantic_end_sec

                mse_list, _ = extractor.calculate_all_diffs(frames)
                if not mse_list:
                    return semantic_end_sec

                mse_threshold = max(1.0, self.pre_vl_mse_min_threshold)
                best_end = semantic_end_sec
                best_mse = mse_threshold
                for index, mse_value in enumerate(mse_list):
                    if mse_value < best_mse:
                        continue
                    timestamp_idx = min(index + 1, len(timestamps) - 1)
                    best_end = max(best_end, safe_float(timestamps[timestamp_idx], semantic_end_sec))
                    best_mse = mse_value

                return min(scan_end, best_end)
            finally:
                try:
                    extractor.cap.release()
                except Exception:
                    pass
        except Exception as error:
            logger.debug(f"[VL-PrePrune] MSE jump detect skipped: {error}")
            return semantic_end_sec

    async def _refine_kept_segments_before_concat(
        self,
        *,
        clips_dir: str,
        semantic_unit: Dict[str, Any],
        original_clip_path: str,
        kept_segments: List[Tuple[float, float]],
    ) -> List[Tuple[float, float]]:
        """
        瀵?stable 剔除后的 kept_segments 做“语涔?物理+语流”三段式边界修正銆?
        为什么：该阶段正处于“剔除后、合并前”的最优切入点，可最大限度避免拼接后半句话问题銆?
        """
        if not self.pre_vl_boundary_refine_enabled:
            return kept_segments
        if not kept_segments:
            return kept_segments

        unit_start_sec = safe_float(semantic_unit.get("start_sec", 0.0), 0.0)
        unit_end_sec = safe_float(semantic_unit.get("end_sec", unit_start_sec), unit_start_sec)
        unit_duration_sec = max(0.0, unit_end_sec - unit_start_sec)
        if unit_duration_sec <= 0.0:
            return kept_segments

        output_dir = str(getattr(self, "_current_subtitle_output_dir", "") or Path(clips_dir).parent)
        self._current_subtitle_output_dir = output_dir
        all_subtitles = self._load_subtitles_for_output_dir(output_dir)
        unit_subtitles = self._build_unit_relative_subtitles(all_subtitles, unit_start_sec, unit_end_sec)
        sentences = self._split_complete_sentences_by_pause(unit_subtitles)

        refined_segments: List[Tuple[float, float]] = []
        ordered_segments = sorted(kept_segments, key=lambda seg: float(seg[0]))
        for raw_start_sec, raw_end_sec in ordered_segments:
            seg_start_sec = max(0.0, min(unit_duration_sec, safe_float(raw_start_sec, 0.0)))
            seg_end_sec = max(0.0, min(unit_duration_sec, safe_float(raw_end_sec, seg_start_sec)))
            if seg_end_sec <= seg_start_sec:
                continue

            # 1) 语义完整性基线：优先锚定完整口语句边鐣?
            sem_start_sec, sem_end_sec = self._get_complete_semantic_baseline_for_segment(
                seg_start_sec,
                seg_end_sec,
                sentences,
            )

            # 2) 物理锚点重标定：起点严守语义句头，终点取 max(语义结束, MSE跳变)
            vis_end_sec = await self._detect_segment_mse_jump_end(
                clip_path=original_clip_path,
                semantic_end_sec=sem_end_sec,
                clip_duration_sec=unit_duration_sec,
            )
            recalibrated_start_sec = sem_start_sec
            recalibrated_end_sec = max(sem_end_sec, vis_end_sec)

            # 3) 口语语流缓冲：起鐐?-0.2s，终鐐?+0.3s
            final_start_sec = max(0.0, recalibrated_start_sec - max(0.0, self.pre_vl_start_buffer_sec))
            final_end_sec = min(unit_duration_sec, recalibrated_end_sec + max(0.0, self.pre_vl_end_buffer_sec))

            if refined_segments and final_start_sec < refined_segments[-1][1]:
                final_start_sec = refined_segments[-1][1]

            if final_end_sec - final_start_sec >= self.pre_vl_min_keep_segment_sec:
                refined_segments.append((final_start_sec, final_end_sec))

        normalized_segments = self._normalize_intervals(
            refined_segments,
            min_duration_sec=self.pre_vl_min_keep_segment_sec,
        )
        if not normalized_segments:
            return kept_segments

        logger.info(
            f"[VL-PrePrune] boundary refine: unit={semantic_unit.get('unit_id', '')}, "
            f"segments {len(kept_segments)} -> {len(normalized_segments)}"
        )
        return normalized_segments

    async def _detect_stable_islands_for_unit(
        self,
        clip_path: str,
        unit_id: str,
    ) -> List[Tuple[float, float]]:
        """
        使用现有 CVKnowledgeValidator 复用 stable 检测链路，仅输出稳定区间銆?

        复用点：动态采样、ROI检测、帧级状态判定、边缘动画检测、连续状态合并銆?
        跳过点：动作单元分类、边界细化、相邻动作合并（通过 stable_only=True 实现）銆?
        """
        def _detect_with_local_validator() -> List[Tuple[float, float]]:
            from services.python_grpc.src.content_pipeline.phase2a.vision.cv_knowledge_validator import CVKnowledgeValidator

            validator = CVKnowledgeValidator(clip_path)
            try:
                duration_sec = max(0.0, safe_float(getattr(validator, "duration_sec", 0.0), 0.0))
                if duration_sec <= 0.0:
                    return []
                stable_islands, _, _ = validator.detect_visual_states(0.0, duration_sec, stable_only=True)
                intervals: List[Tuple[float, float]] = []
                for island in stable_islands:
                    intervals.append((float(island.start_sec), float(island.end_sec)))
                return self._normalize_intervals(intervals)
            finally:
                try:
                    validator.close()
                except Exception:
                    pass

        try:
            if self._cv_executor is not None:
                from services.python_grpc.src.vision_validation.worker import run_detect_stable_islands_task

                loop = asyncio.get_running_loop()
                raw_result = await loop.run_in_executor(
                    self._cv_executor,
                    functools.partial(
                        run_detect_stable_islands_task,
                        clip_path=clip_path,
                        unit_id=unit_id,
                        duration_sec=0.0,
                    ),
                )

                intervals: List[Tuple[float, float]] = []
                for seg in raw_result or []:
                    if not isinstance(seg, (tuple, list)) or len(seg) != 2:
                        continue
                    try:
                        seg_start = float(seg[0])
                        seg_end = float(seg[1])
                    except (TypeError, ValueError):
                        continue
                    if seg_end > seg_start:
                        intervals.append((seg_start, seg_end))

                normalized = self._normalize_intervals(intervals)
                logger.info(f"[VL-PrePrune] unit={unit_id}: stable_islands={len(normalized)} mode=process_pool")
                return normalized

            normalized = await asyncio.to_thread(_detect_with_local_validator)
            logger.info(f"[VL-PrePrune] unit={unit_id}: stable_islands={len(normalized)} mode=local_thread")
            return normalized
        except Exception as error:
            logger.warning(f"[VL-PrePrune] stable detect failed for unit={unit_id}: {error}")
            return []

    async def _detect_transition_segments_for_unit(
        self,
        *,
        clip_path: str,
        unit_id: str,
        duration_sec: float,
    ) -> List[Tuple[float, float]]:
        """
        Detect transition-like action segments and return their intervals.
        Transition classification reuses ActionUnit.classify() logic.
        """
        typed_actions = await self._detect_typed_action_segments_for_unit(
            clip_path=clip_path,
            unit_id=unit_id,
            duration_sec=duration_sec,
        )
        transition_intervals = [
            (float(item["start_sec"]), float(item["end_sec"]))
            for item in typed_actions
            if str(item.get("action_type", "")).strip().lower() == "transition"
        ]
        return self._normalize_intervals(transition_intervals, min_duration_sec=1e-6)

    async def _detect_typed_action_segments_for_unit(
        self,
        *,
        clip_path: str,
        unit_id: str,
        duration_sec: float,
    ) -> List[Dict[str, Any]]:
        """
        Detect action segments with action_type labels.
        action_type is produced by existing ActionUnit.classify() semantics.
        """

        def _parse_typed_action_segments(action_segments: Any) -> List[Dict[str, Any]]:
            parsed: List[Dict[str, Any]] = []
            if not isinstance(action_segments, list):
                return parsed
            for item in action_segments:
                if not isinstance(item, dict):
                    continue
                action_type = str(item.get("action_type", "") or "").strip().lower()
                seg_start = safe_float(item.get("start_sec", 0.0), 0.0)
                seg_end = safe_float(item.get("end_sec", seg_start), seg_start)
                if seg_end > seg_start:
                    dynamic_duration_sec, internal_stable_duration_sec = (
                        self._estimate_dynamic_duration_from_internal_stable(
                            action_start_sec=seg_start,
                            action_end_sec=seg_end,
                            internal_stable_islands=item.get("internal_stable_islands", []),
                        )
                    )
                    parsed.append(
                        {
                            "start_sec": seg_start,
                            "end_sec": seg_end,
                            "action_type": action_type or "unknown",
                            "dynamic_duration_sec": dynamic_duration_sec,
                            "internal_stable_duration_sec": internal_stable_duration_sec,
                        }
                    )
            return parsed

        def _detect_with_local_validator() -> List[Dict[str, Any]]:
            from services.python_grpc.src.content_pipeline.phase2a.vision.cv_knowledge_validator import CVKnowledgeValidator

            validator = CVKnowledgeValidator(clip_path)
            try:
                detected_duration = max(0.0, safe_float(getattr(validator, "duration_sec", 0.0), 0.0))
                fallback_duration = max(0.0, safe_float(duration_sec, 0.0))
                scan_duration = detected_duration if detected_duration > 0.0 else fallback_duration
                if scan_duration <= 0.0:
                    return []

                _, action_units, _ = validator.detect_visual_states(0.0, scan_duration, stable_only=False)
                parsed: List[Dict[str, Any]] = []
                for action in action_units:
                    action_type = str(getattr(action, "action_type", "") or "").strip().lower()
                    if not action_type and hasattr(action, "classify"):
                        try:
                            action_type = str(action.classify() or "").strip().lower()
                        except Exception:
                            action_type = ""
                    seg_start = safe_float(getattr(action, "start_sec", 0.0), 0.0)
                    seg_end = safe_float(getattr(action, "end_sec", seg_start), seg_start)
                    if seg_end > seg_start:
                        dynamic_duration_sec, internal_stable_duration_sec = (
                            self._estimate_dynamic_duration_from_internal_stable(
                                action_start_sec=seg_start,
                                action_end_sec=seg_end,
                                internal_stable_islands=getattr(action, "internal_stable_islands", []),
                            )
                        )
                        parsed.append(
                            {
                                "start_sec": seg_start,
                                "end_sec": seg_end,
                                "action_type": action_type or "unknown",
                                "dynamic_duration_sec": dynamic_duration_sec,
                                "internal_stable_duration_sec": internal_stable_duration_sec,
                            }
                        )
                return parsed
            finally:
                try:
                    validator.close()
                except Exception:
                    pass

        try:
            if self._cv_executor is not None:
                from services.python_grpc.src.vision_validation.worker import run_cv_validation_task

                scan_duration = max(0.0, safe_float(duration_sec, 0.0))
                if scan_duration <= 0.0:
                    return []

                loop = asyncio.get_running_loop()
                raw_result = await loop.run_in_executor(
                    self._cv_executor,
                    functools.partial(
                        run_cv_validation_task,
                        video_path=clip_path,
                        unit_data={
                            "unit_id": unit_id or "unknown",
                            "start_sec": 0.0,
                            "end_sec": scan_duration,
                        },
                        shm_frames=None,
                    ),
                )
                typed_actions = _parse_typed_action_segments((raw_result or {}).get("action_segments"))
                transition_count = sum(
                    1 for item in typed_actions if str(item.get("action_type", "")).strip().lower() == "transition"
                )
                logger.info(
                    f"[VL-PrePrune] unit={unit_id}: typed_actions={len(typed_actions)}, "
                    f"transitions={transition_count} mode=process_pool"
                )
                return typed_actions

            typed_actions = await asyncio.to_thread(_detect_with_local_validator)
            transition_count = sum(
                1 for item in typed_actions if str(item.get("action_type", "")).strip().lower() == "transition"
            )
            logger.info(
                f"[VL-PrePrune] unit={unit_id}: typed_actions={len(typed_actions)}, "
                f"transitions={transition_count} mode=local_thread"
            )
            return typed_actions
        except Exception as error:
            logger.warning(f"[VL-PrePrune] typed action detect failed for unit={unit_id}: {error}")
            return []

    async def _concat_segments_with_ffmpeg(
        self,
        source_clip_path: str,
        output_clip_path: str,
        segments: List[Tuple[float, float]],
    ) -> bool:
        """
        通过 ffmpeg concat demuxer 将多个区段拼接为新片段銆?

        说明：Java 侧最终素材提取已使用相同“分段拼接”思想銆?
        这里鍦?Python 侧前置复用该策略，避免引入新的拼接语义偏差銆?
        """
        return await concat_segments_with_ffmpeg(
            source_clip_path=source_clip_path,
            output_clip_path=output_clip_path,
            segments=segments,
            logger=logger,
        )

    def _map_pruned_relative_to_original(
        self,
        rel_value: float,
        kept_segments: List[Tuple[float, float]],
    ) -> float:
        """
        将“裁剪后片段相对时间”映射回“原始单元相对时间”銆?

        为什么：VL 在裁剪后片段上输出的时间戳，必须还原到原视频时间轴，保证后续截图/切片定位正确銆?
        """
        remaining = max(0.0, float(rel_value))
        for start_sec, end_sec in kept_segments:
            seg_len = max(0.0, end_sec - start_sec)
            if remaining <= seg_len + 1e-6:
                return start_sec + remaining
            remaining -= seg_len
        # 越界兜底：映射到最后一个片段尾閮?
        if kept_segments:
            return kept_segments[-1][1]
        return float(rel_value)

    def _map_pruned_interval_to_original_segments(
        self,
        rel_start: float,
        rel_end: float,
        kept_segments: List[Tuple[float, float]],
    ) -> List[Tuple[float, float]]:
        """
        将“裁剪后片段的相对时间区间”映射回“原始单元相对时间轴”的分段区间銆?

        为什么：褰?clip 区间跨过被剔除的 stable 核心段时，映射后会是多段锛?
        若只回写 start/end 会把中间被剔除段重新纳入，导鑷?Java 侧拼接结果与 VL 观测不一致銆?
        """
        if not kept_segments:
            return []

        start_rel = safe_float(rel_start, 0.0)
        end_rel = safe_float(rel_end, 0.0)
        if end_rel < start_rel:
            start_rel, end_rel = end_rel, start_rel

        mapped_segments: List[Tuple[float, float]] = []
        cursor = 0.0

        for seg_start, seg_end in kept_segments:
            seg_start_f = safe_float(seg_start, 0.0)
            seg_end_f = safe_float(seg_end, seg_start_f)
            seg_len = max(0.0, seg_end_f - seg_start_f)
            if seg_len <= 1e-6:
                continue

            pruned_seg_start = cursor
            pruned_seg_end = cursor + seg_len

            overlap_start = max(start_rel, pruned_seg_start)
            overlap_end = min(end_rel, pruned_seg_end)
            if overlap_end - overlap_start > 1e-6:
                mapped_start = seg_start_f + (overlap_start - pruned_seg_start)
                mapped_end = seg_start_f + (overlap_end - pruned_seg_start)
                mapped_segments.append((mapped_start, mapped_end))

            cursor = pruned_seg_end
            if cursor >= end_rel + 1e-6 and mapped_segments:
                break

        return self._normalize_intervals(mapped_segments)

    def _sum_interval_duration(self, intervals: List[Tuple[float, float]]) -> float:
        """汇总区间总时长（秒）。"""
        total = 0.0
        for start_sec, end_sec in intervals or []:
            total += max(0.0, safe_float(end_sec, 0.0) - safe_float(start_sec, 0.0))
        return total

    def _estimate_dynamic_duration_from_internal_stable(
        self,
        *,
        action_start_sec: float,
        action_end_sec: float,
        internal_stable_islands: Any,
    ) -> Tuple[float, float]:
        """
        估算动作片段的“动态净时长”。

        计算方式：动作总时长 - 与动作重叠的内部稳定岛时长。
        返回：(dynamic_duration_sec, internal_stable_duration_sec)。
        """
        start_sec = safe_float(action_start_sec, 0.0)
        end_sec = safe_float(action_end_sec, start_sec)
        if end_sec <= start_sec:
            return 0.0, 0.0

        action_duration_sec = end_sec - start_sec
        stable_overlaps: List[Tuple[float, float]] = []
        if isinstance(internal_stable_islands, list):
            for island in internal_stable_islands:
                if isinstance(island, dict):
                    island_start = safe_float(island.get("start_sec", 0.0), 0.0)
                    island_end = safe_float(island.get("end_sec", island_start), island_start)
                else:
                    island_start = safe_float(getattr(island, "start_sec", 0.0), 0.0)
                    island_end = safe_float(getattr(island, "end_sec", island_start), island_start)
                if island_end <= island_start:
                    continue
                overlap_start = max(start_sec, island_start)
                overlap_end = min(end_sec, island_end)
                if overlap_end > overlap_start:
                    stable_overlaps.append((overlap_start, overlap_end))

        internal_stable_duration_sec = min(
            action_duration_sec,
            self._sum_interval_duration(self._normalize_intervals(stable_overlaps, min_duration_sec=1e-6))
            if stable_overlaps
            else 0.0,
        )
        dynamic_duration_sec = max(0.0, action_duration_sec - internal_stable_duration_sec)
        return dynamic_duration_sec, internal_stable_duration_sec

    def _compute_pre_prune_kept_ratio(
        self,
        *,
        pre_prune_info: Dict[str, Any],
        raw_duration_sec: float,
    ) -> float:
        """计算预剪枝后保留时长占比。"""
        duration_sec = max(0.0, safe_float(raw_duration_sec, 0.0))
        if duration_sec <= 0.0:
            return 1.0

        kept_segments = self._parse_interval_pairs((pre_prune_info or {}).get("kept_segments"))
        if kept_segments:
            kept_duration = self._sum_interval_duration(kept_segments)
        elif bool((pre_prune_info or {}).get("applied", False)):
            kept_duration = 0.0
        else:
            kept_duration = duration_sec
        ratio = kept_duration / duration_sec if duration_sec > 0 else 1.0
        return max(0.0, min(1.0, ratio))

    def _should_use_stable_action_legacy_branch(
        self,
        *,
        semantic_unit: Dict[str, Any],
        pre_prune_info: Dict[str, Any],
        raw_duration_sec: float,
    ) -> bool:
        """
        命中“静态主导降级”分支：
        1) process + mult_steps；
        2) 已有 stable islands；
        3) 裁剪后保留时长占比低于阈值（默认 1/3）。
        """
        knowledge_type = str(semantic_unit.get("knowledge_type", "") or "").strip().lower()
        if knowledge_type != "process":
            return False
        if not bool(semantic_unit.get("mult_steps", False)):
            return False

        stable_intervals = self._parse_interval_pairs((pre_prune_info or {}).get("stable_intervals_raw"))
        if not stable_intervals:
            return False

        kept_ratio = self._compute_pre_prune_kept_ratio(
            pre_prune_info=pre_prune_info or {},
            raw_duration_sec=raw_duration_sec,
        )
        return kept_ratio < self.pre_vl_legacy_action_trigger_ratio

    async def _build_stable_action_material_requests_for_unit(
        self,
        *,
        clips_dir: str,
        semantic_unit: Dict[str, Any],
        original_clip_path: str,
        pre_prune_info: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        基于 stable islands 反推 action units，并生成 VL 兼容的 clip/screenshot 请求。
        说明：本分支不走语义分类，仅复用 CV 时间边界逻辑。
        """
        unit_id = str(semantic_unit.get("unit_id", "") or "")
        unit_start_sec = safe_float(semantic_unit.get("start_sec", 0.0), 0.0)
        unit_end_sec = safe_float(semantic_unit.get("end_sec", unit_start_sec), unit_start_sec)
        if unit_end_sec < unit_start_sec:
            unit_end_sec = unit_start_sec
        unit_duration_sec = max(0.0, unit_end_sec - unit_start_sec)
        if unit_duration_sec <= 0.0:
            return {"clip_requests": [], "screenshot_requests": [], "action_segments": []}

        kept_segments = self._parse_interval_pairs((pre_prune_info or {}).get("kept_segments"))
        if kept_segments:
            action_segments = self._normalize_intervals(
                kept_segments,
                min_duration_sec=self.pre_vl_min_keep_segment_sec,
            )
        else:
            stable_intervals = self._parse_interval_pairs((pre_prune_info or {}).get("stable_intervals_raw"))
            if not stable_intervals:
                return {"clip_requests": [], "screenshot_requests": [], "action_segments": []}
            action_segments = self._subtract_intervals(
                base_interval=(0.0, unit_duration_sec),
                removed_intervals=stable_intervals,
                min_keep_segment_sec=self.pre_vl_min_keep_segment_sec,
            )
        if not action_segments:
            return {"clip_requests": [], "screenshot_requests": [], "action_segments": []}

        typed_actions = await self._detect_typed_action_segments_for_unit(
            clip_path=original_clip_path,
            unit_id=unit_id,
            duration_sec=unit_duration_sec,
        )
        action_window_sec = max(0.0, self.pre_vl_legacy_action_window_sec)
        normalized_typed_actions: List[Dict[str, Any]] = []
        dropped_short_dynamic_actions = 0
        for item in (typed_actions or []):
            seg_start = safe_float(item.get("start_sec", 0.0), 0.0)
            seg_end = safe_float(item.get("end_sec", seg_start), seg_start)
            if seg_end <= seg_start:
                continue
            dynamic_duration_sec = safe_float(
                item.get("dynamic_duration_sec", seg_end - seg_start),
                seg_end - seg_start,
            )
            if dynamic_duration_sec < self.pre_vl_legacy_action_min_dynamic_sec:
                dropped_short_dynamic_actions += 1
                continue
            normalized_typed_actions.append(
                {
                    "start_sec": seg_start,
                    "end_sec": seg_end,
                    "action_type": str(item.get("action_type", "") or "").strip().lower(),
                    "dynamic_duration_sec": dynamic_duration_sec,
                }
            )

        # Rule: keep a kept_segment only when it overlaps at least one non-transition action.
        # If overlapped actions are transition-only, or no action is detected, drop the segment.
        fully_dropped_by_transition = 0
        fully_dropped_without_non_transition = 0
        dropped_tail_requests: List[Dict[str, Any]] = []
        retained_segments: List[Tuple[float, float]] = []
        for seg_index, (seg_start, seg_end) in enumerate(action_segments, start=1):
            overlapped = []
            for action_item in normalized_typed_actions:
                action_start = float(action_item["start_sec"])
                action_end = float(action_item["end_sec"])
                overlap_start = max(seg_start, action_start)
                overlap_end = min(seg_end, action_end)
                if overlap_end > overlap_start:
                    overlapped.append(action_item)

            has_non_transition = any(
                str(item.get("action_type", "")).strip().lower() != "transition"
                for item in overlapped
            )
            if not has_non_transition:
                if overlapped:
                    fully_dropped_by_transition += 1
                else:
                    fully_dropped_without_non_transition += 1
                tail_abs = max(unit_start_sec, min(unit_end_sec, unit_start_sec + seg_end))
                window_start = max(unit_start_sec, tail_abs - action_window_sec)
                window_end = min(unit_end_sec, tail_abs + action_window_sec)
                if window_end < window_start:
                    window_end = window_start
                drop_ss_stem = f"{unit_id}_ss_vl_action_drop_{seg_index:03d}_tail"
                dropped_tail_requests.append(
                    {
                        "screenshot_id": self._build_unit_relative_asset_id(unit_id, drop_ss_stem),
                        "timestamp_sec": tail_abs,
                        "label": f"action_drop_{seg_index:03d}_tail",
                        "semantic_unit_id": unit_id,
                        "analysis_mode": "legacy_action_units",
                        "action_index": seg_index,
                        "anchor_role": "tail",
                        "_window_start_sec": window_start,
                        "_window_end_sec": window_end,
                    }
                )
                continue
            retained_segments.append((seg_start, seg_end))

        action_segments = retained_segments
        if not action_segments:
            logger.info(
                "[VL-PrePrune] transition filter: unit=%s, dropped_transition_only=%s, "
                "dropped_no_non_transition=%s, dropped_short_dynamic_actions=%s, "
                "no action segments remain, fallback_tail_screenshots=%s",
                unit_id,
                fully_dropped_by_transition,
                fully_dropped_without_non_transition,
                dropped_short_dynamic_actions,
                len(dropped_tail_requests),
            )
            return {"clip_requests": [], "screenshot_requests": dropped_tail_requests, "action_segments": []}

        transition_segments = [
            (float(item["start_sec"]), float(item["end_sec"]))
            for item in normalized_typed_actions
            if str(item.get("action_type", "")).strip().lower() == "transition"
        ]

        if transition_segments:
            previous_count = len(action_segments)
            action_segments = self._subtract_removed_from_segments(
                segments=action_segments,
                removed_intervals=transition_segments,
                min_keep_segment_sec=self.pre_vl_min_keep_segment_sec,
            )
            logger.info(
                "[VL-PrePrune] transition filter: unit=%s, segments %s -> %s, transition=%s, "
                "dropped_transition_only=%s, dropped_no_non_transition=%s, dropped_short_dynamic_actions=%s",
                unit_id,
                previous_count,
                len(action_segments),
                len(transition_segments),
                fully_dropped_by_transition,
                fully_dropped_without_non_transition,
                dropped_short_dynamic_actions,
            )
        if not action_segments:
            return {"clip_requests": [], "screenshot_requests": dropped_tail_requests, "action_segments": []}

        refined_segments = await self._refine_kept_segments_before_concat(
            clips_dir=clips_dir,
            semantic_unit=semantic_unit,
            original_clip_path=original_clip_path,
            kept_segments=action_segments,
        )
        action_segments = self._normalize_intervals(
            refined_segments or action_segments,
            min_duration_sec=self.pre_vl_min_keep_segment_sec,
        )
        if not action_segments:
            return {"clip_requests": [], "screenshot_requests": dropped_tail_requests, "action_segments": []}

        knowledge_type = str(semantic_unit.get("knowledge_type", "") or "").strip() or "process"
        clip_requests: List[Dict[str, Any]] = []
        screenshot_requests: List[Dict[str, Any]] = list(dropped_tail_requests)

        for index, (action_rel_start, action_rel_end) in enumerate(action_segments):
            clip_start_abs = max(unit_start_sec, min(unit_end_sec, unit_start_sec + action_rel_start))
            clip_end_abs = max(unit_start_sec, min(unit_end_sec, unit_start_sec + action_rel_end))
            if clip_end_abs <= clip_start_abs:
                continue

            clip_stem = f"{unit_id}_clip_vl_action_{index + 1:03d}"
            clip_requests.append(
                {
                    "clip_id": self._build_unit_relative_asset_id(unit_id, clip_stem),
                    "start_sec": clip_start_abs,
                    "end_sec": clip_end_abs,
                    "knowledge_type": knowledge_type,
                    "semantic_unit_id": unit_id,
                    "segments": [{"start_sec": clip_start_abs, "end_sec": clip_end_abs}],
                    "analysis_mode": "legacy_action_units",
                    "action_index": index + 1,
                }
            )

            anchors = [
                ("head", clip_start_abs),
                ("tail", clip_end_abs),
            ]
            for role, anchor_abs in anchors:
                window_start = max(unit_start_sec, anchor_abs - action_window_sec)
                window_end = min(unit_end_sec, anchor_abs + action_window_sec)
                if window_end < window_start:
                    window_end = window_start

                ss_stem = f"{unit_id}_ss_vl_action_{index + 1:03d}_{role}"
                screenshot_requests.append(
                    {
                        "screenshot_id": self._build_unit_relative_asset_id(unit_id, ss_stem),
                        "timestamp_sec": anchor_abs,
                        "label": f"action_{index + 1:03d}_{role}",
                        "semantic_unit_id": unit_id,
                        "analysis_mode": "legacy_action_units",
                        "action_index": index + 1,
                        "anchor_role": role,
                        "_window_start_sec": window_start,
                        "_window_end_sec": window_end,
                    }
                )

        logger.info(
            "[VL-PrePrune] legacy-action branch: unit=%s, actions=%s, screenshots=%s",
            unit_id,
            len(clip_requests),
            len(screenshot_requests),
        )
        return {
            "clip_requests": clip_requests,
            "screenshot_requests": screenshot_requests,
            "action_segments": action_segments,
        }

    async def _build_legacy_action_pre_prune_info_for_unit(
        self,
        *,
        clips_dir: str,
        semantic_unit: Dict[str, Any],
        original_clip_path: str,
        action_segments: List[Tuple[float, float]],
        base_pre_prune_info: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        将 legacy-action 提取出的动作区间物化为 pruned clip，并转换为统一 VL 调度可复用的 pre_prune 结构。
        为什么这样做：
        1) 让 legacy-action 分支也走 `analyze_clip -> 统一后处理` 主链路；
        2) 复用已有的时间映射逻辑（pruned 相对时间 -> 原始单元时间）；
        3) 强制使用 tutorial_stepwise 模式时，仍保持与既有 tutorial 分支一致的数据结构。
        """
        unit_id = str(semantic_unit.get("unit_id", "") or "")
        start_sec = safe_float(semantic_unit.get("start_sec", 0.0), 0.0)
        end_sec = safe_float(semantic_unit.get("end_sec", start_sec), start_sec)
        if end_sec < start_sec:
            end_sec = start_sec
        duration_sec = max(0.0, end_sec - start_sec)
        if duration_sec <= 0.0:
            return None

        normalized_segments = self._normalize_intervals(
            action_segments,
            min_duration_sec=self.pre_vl_min_keep_segment_sec,
        )
        if not normalized_segments:
            return None

        removed_segments = self._subtract_intervals(
            base_interval=(0.0, duration_sec),
            removed_intervals=normalized_segments,
            min_keep_segment_sec=1e-6,
        )
        pre_context_prompt = self._build_pruning_context_prompt(
            semantic_unit=semantic_unit,
            kept_segments=normalized_segments,
            removed_segments=removed_segments,
        )

        pruned_dir = Path(clips_dir) / "vl_pruned_clips"
        pruned_name = f"{Path(original_clip_path).stem}_legacy_action_pruned.mp4"
        pruned_clip_path = str(pruned_dir / pruned_name)
        ok = await self._concat_segments_with_ffmpeg(
            source_clip_path=original_clip_path,
            output_clip_path=pruned_clip_path,
            segments=normalized_segments,
        )
        if not ok:
            logger.warning(
                "[VL-PrePrune] legacy-action pruned clip materialize failed: unit=%s, clip=%s",
                unit_id,
                original_clip_path,
            )
            return None

        return {
            "applied": True,
            "materialized": True,
            "clip_path_for_vl": pruned_clip_path,
            "kept_segments": normalized_segments,
            "removed_segments": removed_segments,
            "stable_intervals_raw": self._parse_interval_pairs(
                (base_pre_prune_info or {}).get("stable_intervals_raw")
            ),
            "pre_context_prompt": pre_context_prompt,
        }

    def _split_legacy_action_screenshots(
        self,
        screenshot_requests: List[Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        按用途拆分 legacy 截图：
        - drop-tail：立即保留，参与后续增量去重；
        - non-drop：仅作为 VL 失败时的 fallback。
        """
        drop_tail_requests: List[Dict[str, Any]] = []
        fallback_non_drop_requests: List[Dict[str, Any]] = []
        for request in list(screenshot_requests or []):
            if self._is_legacy_action_drop_tail_screenshot_request(request):
                drop_tail_requests.append(request)
            else:
                fallback_non_drop_requests.append(request)
        return drop_tail_requests, fallback_non_drop_requests

    def _build_legacy_action_tutorial_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """
        将 legacy task 转换为 tutorial_stepwise 调度输入。
        权衡：
        - 统一提示词和后处理链路；
        - 保持原任务字段不变，仅最小覆盖 analysis_mode/extra_prompt。
        """
        tutorial_task = dict(task or {})
        tutorial_task["analysis_mode"] = "tutorial_stepwise"
        tutorial_extra_prompt = self._build_tutorial_extra_prompt()
        existing_extra_prompt = str(tutorial_task.get("extra_prompt", "") or "").strip()
        if existing_extra_prompt and tutorial_extra_prompt not in existing_extra_prompt:
            tutorial_task["extra_prompt"] = existing_extra_prompt + "\n\n" + tutorial_extra_prompt
        elif existing_extra_prompt:
            tutorial_task["extra_prompt"] = existing_extra_prompt
        else:
            tutorial_task["extra_prompt"] = tutorial_extra_prompt
        return tutorial_task

    def _apply_legacy_fallback_material_if_exists(
        self,
        *,
        unit_id: str,
        fallback_store: Dict[str, LegacyFallbackMaterial],
        all_clip_requests: List[Dict[str, Any]],
        all_screenshot_requests: List[Dict[str, Any]],
        reason: str,
    ) -> bool:
        """
        当单元命中 legacy fallback 时，将素材回填到聚合结果并记录日志。
        返回是否实际应用，便于调用方统一控制分支。
        """
        material = fallback_store.get(str(unit_id or ""))
        if not material:
            return False
        material.apply_to(
            target_clip_requests=all_clip_requests,
            target_screenshot_requests=all_screenshot_requests,
        )
        logger.info(
            "[VL-PrePrune] legacy-action fallback applied on %s: unit=%s, clips=%s, screenshots=%s",
            reason,
            unit_id,
            len(list(material.clip_requests or [])),
            len(list(material.screenshot_requests or [])),
        )
        return True

    async def _consume_unit_analysis_result_streaming(
        self,
        *,
        result_index: int,
        analysis_result: Any,
        meta: Dict[str, Any],
        legacy_fallback_materials: Dict[str, LegacyFallbackMaterial],
        all_clip_requests: List[Dict[str, Any]],
        all_screenshot_requests: List[Dict[str, Any]],
        token_stats: Dict[str, Any],
        resolved_output_dir: str,
        original_video_path: str,
    ) -> None:
        unit_id = meta.get("unit_id", f"task_{result_index}")

        if isinstance(analysis_result, Exception):
            logger.warning(f"语义单元 {unit_id} VL 分析异常: {analysis_result}")
            self._apply_legacy_fallback_material_if_exists(
                unit_id=unit_id,
                fallback_store=legacy_fallback_materials,
                all_clip_requests=all_clip_requests,
                all_screenshot_requests=all_screenshot_requests,
                reason="exception",
            )
            return

        if not bool(getattr(analysis_result, "success", False)):
            logger.warning(
                f"语义单元 {unit_id} VL 分析失败: {getattr(analysis_result, 'error_msg', '')}"
            )
            self._apply_legacy_fallback_material_if_exists(
                unit_id=unit_id,
                fallback_store=legacy_fallback_materials,
                all_clip_requests=all_clip_requests,
                all_screenshot_requests=all_screenshot_requests,
                reason="failure",
            )
            return

        current_mode = str(
            meta.get("analysis_mode", getattr(analysis_result, "analysis_mode", "default")) or "default"
        ).strip().lower()
        is_tutorial_stepwise = current_mode == "tutorial_stepwise"

        if not is_tutorial_stepwise:
            analysis_result, concrete_rerun_applied = await self._rerun_should_type_concrete_with_concrete_mode(
                analysis_result=analysis_result,
                meta=meta,
            )
            if concrete_rerun_applied:
                token_stats["should_type_concrete_reanalysis_units"] = int(
                    token_stats.get("should_type_concrete_reanalysis_units", 0)
                ) + 1

        await self._postprocess_unit_main_operations(
            analysis_result=analysis_result,
            semantic_unit=meta.get("semantic_unit", {}),
            output_dir=resolved_output_dir,
        )
        await self._postprocess_unit_main_content(
            analysis_result=analysis_result,
            semantic_unit=meta.get("semantic_unit", {}),
            output_dir=resolved_output_dir,
        )
        semantic_unit = meta.get("semantic_unit", {})
        if not is_tutorial_stepwise:
            should_type_override = self._analysis_result_should_type_override(analysis_result)
            has_no_needed_video = self._analysis_result_has_no_needed_video(analysis_result)
            if has_no_needed_video:
                should_type_override = "abstract"
                token_stats["no_needed_video_units"] += 1

            if should_type_override == "abstract":
                if not has_no_needed_video:
                    token_stats["should_type_abstract_units"] += 1
                self._mark_semantic_unit_knowledge_type(
                    semantic_unit,
                    knowledge_type="abstract",
                    reason="vl_no_needed_video_true" if has_no_needed_video else "vl_should_type_abstract",
                    no_needed_video=has_no_needed_video,
                )
                analysis_result.clip_requests = []
                analysis_result.screenshot_requests = []
                for parsed_item in getattr(analysis_result, "analysis_results", []) or []:
                    try:
                        parsed_item.knowledge_type = "abstract"
                        parsed_item.no_needed_video = has_no_needed_video
                        parsed_item.should_type = "abstract"
                    except Exception:
                        continue
                logger.info(
                    "[VL] unit=%s routed as abstract (no_needed_video/should_type); skip clip/screenshot generation",
                    unit_id,
                )
            elif should_type_override == "concrete":
                token_stats["should_type_concrete_units"] += 1
                self._mark_semantic_unit_knowledge_type(
                    semantic_unit,
                    knowledge_type="concrete",
                    reason="vl_should_type_concrete",
                    no_needed_video=False,
                )
                analysis_result.clip_requests = []
                for parsed_item in getattr(analysis_result, "analysis_results", []) or []:
                    try:
                        parsed_item.knowledge_type = "concrete"
                        parsed_item.no_needed_video = False
                        parsed_item.should_type = "concrete"
                    except Exception:
                        continue
                for ss_item in analysis_result.screenshot_requests:
                    ss_item["knowledge_type"] = "concrete"
                logger.info(
                    "[VL] unit=%s routed as concrete by should_type; skip clip generation and keep screenshots",
                    unit_id,
                )

        self._annotate_screenshot_requests_with_unit_context(
            screenshot_requests=analysis_result.screenshot_requests,
            semantic_unit=semantic_unit,
            analysis_mode=getattr(analysis_result, "analysis_mode", meta.get("analysis_mode", "default")),
        )

        usage = getattr(analysis_result, "token_usage", {}) or {}
        prompt_actual = int(usage.get("prompt_tokens", 0) or 0)
        completion_actual = int(usage.get("completion_tokens", 0) or 0)
        total_actual = int(usage.get("total_tokens", prompt_actual + completion_actual) or 0)
        token_stats["prompt_tokens_actual"] += prompt_actual
        token_stats["completion_tokens_actual"] += completion_actual
        token_stats["total_tokens_actual"] += total_actual

        pre_prune_info = meta.get("pre_prune") or {}
        kept_segments = pre_prune_info.get("kept_segments") or []
        unit_duration = safe_float(meta.get("unit_duration", 0.0), 0.0)
        unit_start_sec = safe_float(meta.get("start_sec", 0.0), 0.0)
        unit_end_sec = safe_float(meta.get("end_sec", unit_start_sec), unit_start_sec)
        if unit_end_sec < unit_start_sec:
            unit_end_sec = unit_start_sec

        if pre_prune_info.get("applied") and kept_segments:
            kept_duration = sum(max(0.0, e - s) for s, e in kept_segments)
            if kept_duration > 1e-6 and unit_duration > kept_duration:
                prompt_per_sec = prompt_actual / kept_duration
                completion_per_sec = completion_actual / kept_duration
                prompt_base = int(round(prompt_per_sec * unit_duration))
                completion_base = int(round(completion_per_sec * unit_duration))
                total_base = prompt_base + completion_base
            else:
                prompt_base = prompt_actual
                completion_base = completion_actual
                total_base = total_actual
        else:
            prompt_base = prompt_actual
            completion_base = completion_actual
            total_base = total_actual

        token_stats["prompt_tokens_baseline_est"] += max(0, prompt_base)
        token_stats["completion_tokens_baseline_est"] += max(0, completion_base)
        token_stats["total_tokens_baseline_est"] += max(0, total_base)

        if pre_prune_info.get("applied") and kept_segments:
            for clip_item in analysis_result.clip_requests:
                rel_start = safe_float(clip_item.get("start_sec", unit_start_sec), unit_start_sec) - unit_start_sec
                rel_end = safe_float(clip_item.get("end_sec", unit_start_sec), unit_start_sec) - unit_start_sec

                mapped_rel_segments = self._map_pruned_interval_to_original_segments(
                    rel_start=rel_start,
                    rel_end=rel_end,
                    kept_segments=kept_segments,
                )
                abs_segments: List[Dict[str, float]] = []
                for seg_rel_start, seg_rel_end in mapped_rel_segments:
                    abs_seg_start = unit_start_sec + seg_rel_start
                    abs_seg_end = unit_start_sec + seg_rel_end
                    abs_seg_start = max(unit_start_sec, min(abs_seg_start, unit_end_sec))
                    abs_seg_end = max(unit_start_sec, min(abs_seg_end, unit_end_sec))
                    if abs_seg_end - abs_seg_start > 1e-6:
                        abs_segments.append({
                            "start_sec": abs_seg_start,
                            "end_sec": abs_seg_end,
                        })

                if abs_segments:
                    abs_start = min(seg["start_sec"] for seg in abs_segments)
                    abs_end = max(seg["end_sec"] for seg in abs_segments)
                else:
                    mapped_rel_start = self._map_pruned_relative_to_original(rel_start, kept_segments)
                    mapped_rel_end = self._map_pruned_relative_to_original(rel_end, kept_segments)
                    abs_start = unit_start_sec + mapped_rel_start
                    abs_end = unit_start_sec + mapped_rel_end
                    abs_start = max(unit_start_sec, min(abs_start, unit_end_sec))
                    abs_end = max(unit_start_sec, min(abs_end, unit_end_sec))

                if abs_end < abs_start:
                    abs_start, abs_end = abs_end, abs_start
                clip_item["start_sec"] = abs_start
                clip_item["end_sec"] = abs_end
                clip_item["segments"] = abs_segments

            for ss_item in analysis_result.screenshot_requests:
                rel_ts = safe_float(ss_item.get("_relative_timestamp", 0.0), 0.0)
                mapped_rel_ts = self._map_pruned_relative_to_original(rel_ts, kept_segments)
                mapped_abs_ts = unit_start_sec + mapped_rel_ts
                mapped_abs_ts = max(unit_start_sec, min(mapped_abs_ts, unit_end_sec))
                ss_item["timestamp_sec"] = mapped_abs_ts
                ss_item["_relative_timestamp"] = mapped_rel_ts
                ss_item["_pre_pruned"] = True
        elif pre_prune_info.get("applied"):
            logger.warning(f"[VL-PrePrune] unit={unit_id} applied but no kept_segments, skip remap")

        for clip_item in analysis_result.clip_requests:
            clip_start = safe_float(clip_item.get("start_sec", unit_start_sec), unit_start_sec)
            clip_end = safe_float(clip_item.get("end_sec", unit_start_sec), unit_start_sec)
            clip_start = max(unit_start_sec, min(clip_start, unit_end_sec))
            clip_end = max(unit_start_sec, min(clip_end, unit_end_sec))
            if clip_end < clip_start:
                clip_start, clip_end = clip_end, clip_start
            clip_item["start_sec"] = clip_start
            clip_item["end_sec"] = clip_end

        for ss_item in analysis_result.screenshot_requests:
            abs_ts = safe_float(ss_item.get("timestamp_sec", unit_start_sec), unit_start_sec)
            abs_ts = max(unit_start_sec, min(abs_ts, unit_end_sec))
            ss_item["timestamp_sec"] = abs_ts

        if str(meta.get("analysis_mode", "")).strip().lower() == "tutorial_stepwise":
            export_from_original = bool(
                self.tutorial_export_from_original_clip_when_prepruned
                and pre_prune_info.get("applied")
                and kept_segments
            )
            if export_from_original:
                tutorial_asset_video_path = str(
                    meta.get("clip_path")
                    or meta.get("vl_clip_path")
                    or original_video_path
                ).strip() or original_video_path
                use_relative_ts_for_export = False
                prefer_screenshot_keyframes = True
            else:
                tutorial_asset_video_path = str(
                    meta.get("vl_clip_path")
                    or meta.get("clip_path")
                    or original_video_path
                ).strip() or original_video_path
                use_relative_ts_for_export = True
                prefer_screenshot_keyframes = False
            await self._save_tutorial_assets_for_unit(
                video_path=tutorial_asset_video_path,
                output_dir=resolved_output_dir,
                unit_id=unit_id,
                clip_requests=analysis_result.clip_requests,
                screenshot_requests=analysis_result.screenshot_requests,
                raw_response_json=getattr(analysis_result, "raw_response_json", []) or [],
                raw_llm_interactions=getattr(analysis_result, "raw_llm_interactions", []) or [],
                use_analysis_relative_timestamps=use_relative_ts_for_export,
                prefer_screenshot_requests_keyframes=prefer_screenshot_keyframes,
            )

        all_clip_requests.extend(analysis_result.clip_requests)
        all_screenshot_requests.extend(analysis_result.screenshot_requests)
        logger.info(
            "[VL-Streaming] unit consumed: unit=%s, idx=%s, total_clips=%s, total_screenshots=%s",
            unit_id,
            result_index,
            len(all_clip_requests),
            len(all_screenshot_requests),
        )

    async def _prepare_legacy_action_dispatch_plan(
        self,
        *,
        clips_dir: str,
        legacy_action_tasks: List[Dict[str, Any]],
    ) -> LegacyActionDispatchPlan:
        """
        将 legacy-action 任务转换为统一调度计划。
        设计取舍：
        - 保持 legacy 物料构建逻辑不变；
        - 将“直接回退输出”和“进入 VL 调度输出”拆分，避免 generate 主流程内联过长。
        """
        plan = LegacyActionDispatchPlan(legacy_unit_count=len(list(legacy_action_tasks or [])))
        if not legacy_action_tasks:
            return plan

        legacy_results = await asyncio.gather(
            *[
                self._build_stable_action_material_requests_for_unit(
                    clips_dir=clips_dir,
                    semantic_unit=item["task"].get("semantic_unit", {}),
                    original_clip_path=str(item["task"].get("clip_path", "") or ""),
                    pre_prune_info=item.get("pre_prune_info", {}),
                )
                for item in legacy_action_tasks
            ],
            return_exceptions=True,
        )
        for item, legacy_result in zip(legacy_action_tasks, legacy_results):
            unit_id = str(item["task"].get("unit_id", "") or "")
            if isinstance(legacy_result, Exception):
                logger.warning(
                    "[VL-PrePrune] legacy-action build failed: unit=%s, error=%s",
                    unit_id,
                    legacy_result,
                )
                continue

            legacy_clip_requests = list(legacy_result.get("clip_requests", []) or [])
            legacy_screenshot_requests = list(legacy_result.get("screenshot_requests", []) or [])
            action_segments = self._parse_interval_pairs(legacy_result.get("action_segments"))
            if not action_segments:
                plan.immediate_clip_requests.extend(legacy_clip_requests)
                plan.immediate_screenshot_requests.extend(legacy_screenshot_requests)
                continue

            legacy_pre_prune_info = await self._build_legacy_action_pre_prune_info_for_unit(
                clips_dir=clips_dir,
                semantic_unit=item["task"].get("semantic_unit", {}),
                original_clip_path=str(item["task"].get("clip_path", "") or ""),
                action_segments=action_segments,
                base_pre_prune_info=item.get("pre_prune_info", {}),
            )
            if not legacy_pre_prune_info:
                # 物化失败时回退 legacy 直接产物，避免素材完全丢失。
                plan.immediate_clip_requests.extend(legacy_clip_requests)
                plan.immediate_screenshot_requests.extend(legacy_screenshot_requests)
                continue

            drop_tail_screenshots, fallback_non_drop_screenshots = self._split_legacy_action_screenshots(
                legacy_screenshot_requests
            )
            plan.immediate_screenshot_requests.extend(drop_tail_screenshots)

            legacy_task = self._build_legacy_action_tutorial_task(item["task"])
            plan.vl_unit_tasks.append(legacy_task)
            plan.vl_pre_prune_results.append(legacy_pre_prune_info)
            plan.fallback_materials[unit_id] = LegacyFallbackMaterial(
                clip_requests=legacy_clip_requests,
                screenshot_requests=fallback_non_drop_screenshots,
            )
        return plan

    async def _prepare_pruned_clip_for_vl(
        self,
        clips_dir: str,
        semantic_unit: Dict[str, Any],
        original_clip_path: str,
        force_preprocess: bool = False,
        stable_intervals_override: Optional[List[Tuple[float, float]]] = None,
    ) -> Dict[str, Any]:
        """
        为单个语义单元生成“VL前静态段剔除”结果銆?

        返回结构锛?
        - applied: 是否实际应用了剔闄?
        - clip_path_for_vl: 传给 VL 的片段路径（可能为原片段锛?
        - kept_segments / removed_segments: 相对原片段时间轴的区闂?
        - pre_context_prompt: 渚?VL 追加的上下文提示
        """
        unit_id = str(semantic_unit.get("unit_id", "") or "")
        start_sec = safe_float(semantic_unit.get("start_sec", 0.0), 0.0)
        end_sec = safe_float(semantic_unit.get("end_sec", 0.0), 0.0)
        duration_sec = max(0.0, end_sec - start_sec)
        knowledge_type = str(semantic_unit.get("knowledge_type", "") or "").strip().lower()
        routing_preprocess_only = bool(semantic_unit.get("_routing_preprocess_only", False))

        default_result = {
            "applied": False,
            "materialized": False,
            "clip_path_for_vl": original_clip_path,
            "kept_segments": [(0.0, duration_sec)] if duration_sec > 0 else [],
            "removed_segments": [],
            "pre_context_prompt": "",
        }

        if not self.pre_vl_pruning_enabled:
            return default_result
        if not self._is_pre_vl_pruning_enabled_for_knowledge_type(knowledge_type):
            return default_result
        if (not force_preprocess) and duration_sec < self.pre_vl_min_unit_duration_sec:
            return default_result

        try:
            if stable_intervals_override is None:
                stable_intervals = await self._detect_stable_islands_for_unit(original_clip_path, unit_id)
            else:
                stable_intervals = self._normalize_intervals(stable_intervals_override)
            if not stable_intervals:
                return default_result

            default_with_raw_stable = dict(default_result)
            default_with_raw_stable["stable_intervals_raw"] = stable_intervals

            # 仅剔除满足时长阈值的 stable 核心段（两侧边缘保留锛?
            removed_intervals = self._build_removed_intervals_from_stable(stable_intervals)
            if not removed_intervals:
                return default_with_raw_stable

            kept_segments = self._subtract_intervals(
                base_interval=(0.0, duration_sec),
                removed_intervals=removed_intervals,
                min_keep_segment_sec=self.pre_vl_min_keep_segment_sec,
            )
            if not kept_segments:
                return default_with_raw_stable

            kept_segments = await self._refine_kept_segments_before_concat(
                clips_dir=clips_dir,
                semantic_unit=semantic_unit,
                original_clip_path=original_clip_path,
                kept_segments=kept_segments,
            )
            if not kept_segments:
                return default_with_raw_stable

            removed_total = sum((e - s) for s, e in removed_intervals)
            removed_ratio = removed_total / duration_sec if duration_sec > 0 else 0.0
            if removed_ratio < self.pre_vl_min_removed_ratio:
                # 剔除收益太小时不处理，避免额外编码开销和潜在语义损澶?
                return default_with_raw_stable

            if routing_preprocess_only:
                # routing 分流阶段只需要 kept_segments 与有效时长，不需要生成 pruned clip 文件。
                context_prompt = self._build_pruning_context_prompt(
                    semantic_unit=semantic_unit,
                    kept_segments=kept_segments,
                    removed_segments=removed_intervals,
                )
                logger.info(
                    f"[VL-PrePrune] routing-only prepared: unit={unit_id}, removed_ratio={removed_ratio:.2%}, "
                    f"stable={len(stable_intervals)}, removed={len(removed_intervals)}, kept={len(kept_segments)}"
                )
                return {
                    "applied": True,
                    "materialized": False,
                    "clip_path_for_vl": original_clip_path,
                    "kept_segments": kept_segments,
                    "removed_segments": removed_intervals,
                    "stable_intervals_raw": stable_intervals,
                    "pre_context_prompt": context_prompt,
                }

            clip_parent_dir = Path(original_clip_path).resolve().parent if str(original_clip_path or "").strip() else Path(clips_dir)
            pruned_dir = clip_parent_dir / "vl_pruned_clips"
            pruned_name = f"{Path(original_clip_path).stem}_pruned.mp4"
            pruned_clip_path = str(pruned_dir / pruned_name)

            ok = await self._concat_segments_with_ffmpeg(
                source_clip_path=original_clip_path,
                output_clip_path=pruned_clip_path,
                segments=kept_segments,
            )
            if not ok:
                return default_with_raw_stable

            context_prompt = self._build_pruning_context_prompt(
                semantic_unit=semantic_unit,
                kept_segments=kept_segments,
                removed_segments=removed_intervals,
            )

            logger.info(
                f"[VL-PrePrune] applied: unit={unit_id}, removed_ratio={removed_ratio:.2%}, "
                f"stable={len(stable_intervals)}, removed={len(removed_intervals)}, kept={len(kept_segments)}"
            )

            return {
                "applied": True,
                "materialized": True,
                "clip_path_for_vl": pruned_clip_path,
                "kept_segments": kept_segments,
                "removed_segments": removed_intervals,
                "stable_intervals_raw": stable_intervals,
                "pre_context_prompt": context_prompt,
            }
        except Exception as error:
            logger.warning(f"[VL-PrePrune] failed for unit={unit_id}: {error}")
            return default_result

    async def preprocess_process_units_for_routing(
        self,
        video_path: str,
        process_units: List[Dict[str, Any]],
        output_dir: str = None,
        force_preprocess: bool = True,
    ) -> Dict[str, Dict[str, Any]]:
        """
        在路由层涓?process 单元执栝处理并返回“有效时长”銆?
        为什么：需要先基于 stable 剔除+边界修正后的真实片段长度，再做短/长分流銆?
        """
        route_map: Dict[str, Dict[str, Any]] = {}
        if not process_units:
            return route_map

        clips_dir = await self._split_video_by_semantic_units(video_path, process_units, output_dir)
        if not clips_dir:
            return route_map

        unit_tasks: List[Dict[str, Any]] = []
        for unit in process_units:
            unit_id = str(unit.get("unit_id", "") or "")
            start_sec = safe_float(unit.get("start_sec", 0.0), 0.0)
            end_sec = safe_float(unit.get("end_sec", start_sec), start_sec)
            if end_sec < start_sec:
                end_sec = start_sec
            raw_duration_sec = max(0.0, end_sec - start_sec)

            entry: Dict[str, Any] = {
                "unit_id": unit_id,
                "raw_duration_sec": raw_duration_sec,
                "effective_duration_sec": raw_duration_sec,
                "preprocess_applied": False,
                "clip_path": "",
                "pre_prune_info": {},
            }

            clip_path = self._find_clip_for_unit(clips_dir, unit_id, start_sec, end_sec)
            if not clip_path:
                route_map[unit_id] = entry
                continue

            entry["clip_path"] = clip_path
            route_map[unit_id] = entry
            unit_tasks.append(
                {
                    # routing 阶段只做分流预处理，不在此阶段 materialize pruned clip 文件。
                    "semantic_unit": {**unit, "_routing_preprocess_only": True},
                    "clip_path": clip_path,
                    "entry": entry,
                }
            )

        if not unit_tasks:
            return route_map

        pre_prune_results = await self._prepare_pruned_clips_for_units(
            clips_dir=clips_dir,
            unit_tasks=unit_tasks,
            force_preprocess=force_preprocess,
        )

        for task, pre_prune_info in zip(unit_tasks, pre_prune_results):
            entry = task.get("entry", {})
            kept_segments = pre_prune_info.get("kept_segments") or []
            kept_duration = sum(max(0.0, float(e) - float(s)) for s, e in kept_segments)

            entry["preprocess_applied"] = bool(pre_prune_info.get("applied", False))
            entry["pre_prune_info"] = pre_prune_info
            if kept_duration > 0.0:
                entry["effective_duration_sec"] = kept_duration

        return route_map

    def _build_single_task_dispatch_payload(
        self,
        *,
        unit_task: Dict[str, Any],
        pre_prune_info: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], Dict[str, Any], bool]:
        """
        为单个 unit 构建 VL 调度输入与元数据。
        返回 (task_input, task_meta, pre_prune_applied)。
        """
        unit_id = str(unit_task.get("unit_id", "") or "")
        start_sec = safe_float(unit_task.get("start_sec", 0.0), 0.0)
        end_sec = safe_float(unit_task.get("end_sec", start_sec), start_sec)
        duration = max(0.0, safe_float(unit_task.get("duration", end_sec - start_sec), end_sec - start_sec))
        clip_path = str(unit_task.get("clip_path", "") or "")
        analysis_mode = str(unit_task.get("analysis_mode", "default") or "default")
        extra_prompt = unit_task.get("extra_prompt")

        clip_path_for_vl = self._resolve_vl_analysis_clip_path(
            original_clip_path=clip_path,
            preferred_clip_path=pre_prune_info.get("clip_path_for_vl", clip_path),
        )

        pre_context_prompt = str(pre_prune_info.get("pre_context_prompt", "") or "").strip()
        if pre_context_prompt:
            if extra_prompt:
                extra_prompt = str(extra_prompt) + "\n\n" + pre_context_prompt
            else:
                extra_prompt = pre_context_prompt

        task_input = {
            "clip_path": clip_path_for_vl,
            "semantic_unit_start_sec": start_sec,
            "semantic_unit_id": unit_id,
            "extra_prompt": extra_prompt,
            "analysis_mode": analysis_mode,
        }
        task_meta = {
            "unit_id": unit_id,
            "semantic_unit": unit_task.get("semantic_unit", {}),
            "start_sec": start_sec,
            "end_sec": end_sec,
            "unit_duration": duration,
            "clip_path": clip_path,
            "vl_clip_path": clip_path_for_vl,
            "pre_prune": pre_prune_info,
            "analysis_mode": analysis_mode,
            "extra_prompt": extra_prompt,
        }
        return task_input, task_meta, bool(pre_prune_info.get("applied"))

    async def _run_stream_unit_pipeline(
        self,
        *,
        video_path: str,
        semantic_units: List[Dict[str, Any]],
        resolved_output_dir: str,
        all_clip_requests: List[Dict[str, Any]],
        all_screenshot_requests: List[Dict[str, Any]],
        token_stats: Dict[str, Any],
    ) -> Tuple[List[Any], List[Dict[str, Any]], Dict[str, LegacyFallbackMaterial], int]:
        """
        单元级流式流水线：
        1) 进程池并发切片（一个进程处理一个 unit 切片任务）；
        2) 单元切完即进入 VL 分析；
        3) 分析结果即刻消费（deepseek 补充、素材导出、截图/片段聚合）。
        """
        unit_blueprints = self._build_vl_unit_task_blueprints(semantic_units)
        if not unit_blueprints:
            return [], [], {}, 0

        split_worker_count = self._resolve_stream_split_process_workers(len(unit_blueprints))
        analysis_worker_count = self._resolve_vl_parallel_workers(len(unit_blueprints))
        backpressure_limit = max(1, analysis_worker_count * int(self.stream_split_backpressure_multiplier))
        analysis_semaphore = asyncio.Semaphore(max(1, analysis_worker_count))
        split_pre_cut_config = (
            self.config.get("semantic_split_pre_cut", {})
            if isinstance(self.config.get("semantic_split_pre_cut", {}), dict)
            else {}
        )

        logger.info(
            "[VL-UnitStream] start: units=%s, split_workers=%s, split_executor=%s, analysis_workers=%s, backpressure=%s",
            len(unit_blueprints),
            split_worker_count,
            "thread",
            analysis_worker_count,
            backpressure_limit,
        )

        analysis_results: List[Any] = []
        task_metadata: List[Dict[str, Any]] = []
        streamed_result_indexes: set[int] = set()
        pending_analysis_tasks: set[asyncio.Task] = set()
        legacy_fallback_materials: Dict[str, LegacyFallbackMaterial] = {}
        pruned_units = 0
        dispatched_vl_units = 0
        legacy_units = 0

        async def _drain_analysis_tasks(wait_all: bool = False) -> None:
            nonlocal pending_analysis_tasks
            if not pending_analysis_tasks:
                return
            return_when = asyncio.ALL_COMPLETED if wait_all else asyncio.FIRST_COMPLETED
            done_tasks, pending_tasks = await asyncio.wait(pending_analysis_tasks, return_when=return_when)
            pending_analysis_tasks = set(pending_tasks)
            for done_task in done_tasks:
                try:
                    await done_task
                except Exception as error:
                    logger.warning("[VL-UnitStream] analysis task failed: %s", error)

        def _schedule_analysis_task(unit_task: Dict[str, Any], pre_prune_info: Dict[str, Any]) -> None:
            nonlocal pruned_units, dispatched_vl_units
            task_input, task_meta, pre_prune_applied = self._build_single_task_dispatch_payload(
                unit_task=unit_task,
                pre_prune_info=pre_prune_info,
            )
            result_index = len(task_metadata)
            task_metadata.append(task_meta)
            analysis_results.append(RuntimeError("vl_stream_result_pending"))
            if pre_prune_applied:
                pruned_units += 1
            dispatched_vl_units += 1

            async def _run_and_consume(
                index_value: int,
                input_value: Dict[str, Any],
                meta_value: Dict[str, Any],
            ) -> None:
                async with analysis_semaphore:
                    try:
                        analyzed_result = await self.analyzer.analyze_clip(
                            clip_path=str(input_value.get("clip_path", "") or ""),
                            semantic_unit_start_sec=float(input_value.get("semantic_unit_start_sec", 0.0) or 0.0),
                            semantic_unit_id=str(input_value.get("semantic_unit_id", "") or ""),
                            extra_prompt=input_value.get("extra_prompt"),
                            analysis_mode=str(input_value.get("analysis_mode", "default") or "default"),
                        )
                    except Exception as error:
                        analyzed_result = error
                analysis_results[index_value] = analyzed_result
                await self._consume_unit_analysis_result_streaming(
                    result_index=index_value,
                    analysis_result=analyzed_result,
                    meta=meta_value,
                    legacy_fallback_materials=legacy_fallback_materials,
                    all_clip_requests=all_clip_requests,
                    all_screenshot_requests=all_screenshot_requests,
                    token_stats=token_stats,
                    resolved_output_dir=resolved_output_dir,
                    original_video_path=video_path,
                )
                streamed_result_indexes.add(index_value)

            task = asyncio.create_task(_run_and_consume(result_index, task_input, task_meta))
            pending_analysis_tasks.add(task)

        loop = asyncio.get_running_loop()
        with self._create_stream_split_executor(worker_count=split_worker_count) as split_executor:
            async def _run_single_split(unit_blueprint: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
                split_payload = await loop.run_in_executor(
                    split_executor,
                    functools.partial(
                        _split_single_unit_clip_worker,
                        video_path=video_path,
                        semantic_unit=unit_blueprint.get("semantic_unit", {}),
                        output_dir=resolved_output_dir,
                        split_pre_cut_config=split_pre_cut_config,
                    ),
                )
                return unit_blueprint, split_payload

            split_tasks = [asyncio.create_task(_run_single_split(blueprint)) for blueprint in unit_blueprints]
            for done_task in asyncio.as_completed(split_tasks):
                try:
                    unit_blueprint, split_payload = await done_task
                except Exception as split_error:
                    logger.warning("[VL-UnitStream] split task crashed: %s", split_error)
                    continue
                unit_id = str(unit_blueprint.get("unit_id", "") or "")
                if not bool(split_payload.get("success", False)):
                    logger.warning(
                        "[VL-UnitStream] split failed: unit=%s, err=%s",
                        unit_id,
                        split_payload.get("error", "unknown"),
                    )
                    continue

                clip_path = str(split_payload.get("clip_path", "") or "").strip()
                clip_dir = str(split_payload.get("clip_dir", "") or "").strip()
                if not clip_path:
                    logger.warning("[VL-UnitStream] split clip missing: unit=%s", unit_id)
                    continue
                if not clip_dir:
                    clip_dir = str(Path(clip_path).parent)

                unit_task = dict(unit_blueprint)
                unit_task["clip_path"] = clip_path
                pre_prune_info = await self._prepare_pruned_clip_for_vl(
                    clips_dir=clip_dir,
                    semantic_unit=unit_task.get("semantic_unit", {}),
                    original_clip_path=clip_path,
                    force_preprocess=False,
                )

                semantic_unit = unit_task.get("semantic_unit", {})
                raw_duration_sec = safe_float(unit_task.get("duration", 0.0), 0.0)
                if self._should_use_stable_action_legacy_branch(
                    semantic_unit=semantic_unit,
                    pre_prune_info=pre_prune_info,
                    raw_duration_sec=raw_duration_sec,
                ):
                    legacy_units += 1
                    legacy_dispatch_plan = await self._prepare_legacy_action_dispatch_plan(
                        clips_dir=clip_dir,
                        legacy_action_tasks=[
                            {
                                "task": unit_task,
                                "pre_prune_info": pre_prune_info,
                            }
                        ],
                    )
                    all_clip_requests.extend(legacy_dispatch_plan.immediate_clip_requests)
                    all_screenshot_requests.extend(legacy_dispatch_plan.immediate_screenshot_requests)
                    for fallback_unit_id, material in (legacy_dispatch_plan.fallback_materials or {}).items():
                        legacy_fallback_materials[fallback_unit_id] = material

                    for queued_index, queued_task in enumerate(legacy_dispatch_plan.vl_unit_tasks):
                        queued_pre_prune = (
                            legacy_dispatch_plan.vl_pre_prune_results[queued_index]
                            if queued_index < len(legacy_dispatch_plan.vl_pre_prune_results)
                            else self._build_default_pre_prune_info(
                                semantic_unit=queued_task.get("semantic_unit", {}),
                                clip_path=str(queued_task.get("clip_path", "") or ""),
                            )
                        )
                        _schedule_analysis_task(queued_task, queued_pre_prune)
                else:
                    _schedule_analysis_task(unit_task, pre_prune_info)

                if len(pending_analysis_tasks) >= backpressure_limit:
                    await _drain_analysis_tasks(wait_all=False)

            await _drain_analysis_tasks(wait_all=True)

        # 理论上每个结果都会在 _run_and_consume 中被消费；这里兜底避免异常导致遗漏。
        for result_index, analyzed_result in enumerate(analysis_results):
            if result_index in streamed_result_indexes:
                continue
            meta = task_metadata[result_index] if result_index < len(task_metadata) else {}
            await self._consume_unit_analysis_result_streaming(
                result_index=result_index,
                analysis_result=analyzed_result,
                meta=meta,
                legacy_fallback_materials=legacy_fallback_materials,
                all_clip_requests=all_clip_requests,
                all_screenshot_requests=all_screenshot_requests,
                token_stats=token_stats,
                resolved_output_dir=resolved_output_dir,
                original_video_path=video_path,
            )
            streamed_result_indexes.add(result_index)

        token_stats["stable_action_legacy_units"] += legacy_units
        token_stats["vl_units"] = int(dispatched_vl_units)
        logger.info(
            "[VL-UnitStream] done: split_units=%s, vl_units=%s, legacy_units=%s, pruned=%s",
            len(unit_blueprints),
            dispatched_vl_units,
            legacy_units,
            pruned_units,
        )
        return analysis_results, task_metadata, legacy_fallback_materials, pruned_units

    async def generate(
        self,
        video_path: str,
        semantic_units: List[Dict[str, Any]],
        output_dir: str = None
    ) -> VLGenerationResult:
        """
        生成素材请求 (并桢寲版鏈?
        
        Args:
            video_path: 原视频路寰?
            semantic_units: 语义单元列表（来鑷?semantic_units_phase2a.json锛?
            output_dir: 输出目录（用于存放切割的视频片段锛?
            
        Returns:
            VLGenerationResult: 生成结果
        """
        result = VLGenerationResult()
        resolved_output_dir = str(output_dir or Path(video_path).parent)
        self._current_subtitle_output_dir = resolved_output_dir
        
        if not self.enabled:
            result.success = False
            result.error_msg = "VL 素材生成功能未启用"
            return result

        def _normalize_unit_knowledge_type(value: Any) -> str:
            text = str(value or "").strip().lower()
            if text in {"abstract", "抽象", "讲解", "explanation"}:
                return "abstract"
            if text in {"concrete", "具象", "具体", "实例"}:
                return "concrete"
            if text in {"process", "过程", "过程性", "process_short", "process_long"}:
                return "process"
            return text

        original_semantic_units = list(semantic_units or [])
        filtered_semantic_units: List[Dict[str, Any]] = []
        skipped_abstract_units = 0
        for unit in original_semantic_units:
            if not isinstance(unit, dict):
                continue
            normalized_type = _normalize_unit_knowledge_type(unit.get("knowledge_type", ""))
            if normalized_type == "abstract":
                skipped_abstract_units += 1
                continue
            if normalized_type in {"concrete", "process"}:
                unit["knowledge_type"] = normalized_type
            filtered_semantic_units.append(unit)
        semantic_units = filtered_semantic_units
        if skipped_abstract_units > 0:
            logger.info(
                "[VL] Skip abstract units before analysis: skipped=%s, remaining=%s",
                skipped_abstract_units,
                len(semantic_units),
            )
        
        # 检查是否有缓存
        cache_path = self._get_cache_path(video_path, resolved_output_dir)
        use_cache = self.config.get("use_cache", True)
        
        # VL分析结果(来自缓存或新分析)
        all_screenshot_requests = []
        all_clip_requests = []
        unit_analysis_outputs: List[Dict[str, Any]] = []

        # 任务绾?token 统计
        token_stats: Dict[str, Any] = {
            "total_units": len(original_semantic_units),
            "skipped_abstract_units": skipped_abstract_units,
            "vl_units": 0,
            "stable_action_legacy_units": 0,
            "pruned_units": 0,
            "no_needed_video_units": 0,
            "should_type_abstract_units": 0,
            "should_type_concrete_units": 0,
            "should_type_concrete_reanalysis_units": 0,
            "prompt_tokens_actual": 0,
            "completion_tokens_actual": 0,
            "total_tokens_actual": 0,
            # 基线定义：若不做前置裁剪，则 pruned 单元鎸?"原片娈?token/绉?* 原始时长" 估算
            # 闈?pruned 单元基线=实际（因为路径一致）
            "prompt_tokens_baseline_est": 0,
            "completion_tokens_baseline_est": 0,
            "total_tokens_baseline_est": 0,
            "saved_tokens_est": 0,
            "saved_ratio_est": 0.0,
        }

        if not semantic_units:
            result.clip_requests = []
            result.screenshot_requests = []
            result.token_stats = token_stats
            self._write_phase2a_token_cost_audit(
                output_dir=resolved_output_dir,
                token_stats=token_stats,
                unit_analysis_outputs=[],
                video_path=video_path,
            )
            result.success = True
            logger.info(
                "[VL] No units left after abstract filtering, skip VL analysis: total=%s, skipped_abstract=%s",
                token_stats.get("total_units", 0),
                token_stats.get("skipped_abstract_units", 0),
            )
            return result
        
        if use_cache:
            cached_data = self._load_vl_results(cache_path)
            if cached_data:
                logger.info("🚀 使用缓存的VL分析结果,跳过VL API调用")
                all_screenshot_requests = cached_data.get("aggregated_screenshots", [])
                all_clip_requests = cached_data.get("aggregated_clips", [])
                unit_analysis_outputs = list(cached_data.get("analysis_results", []) or [])
                if self.merge_multistep_clip_requests:
                    all_clip_requests = self._merge_multistep_clip_requests(semantic_units, all_clip_requests)
                # ⚠️  不直接返鍥?继续执鐲V优化
                logger.info(f"从缓存加杞? screenshots={len(all_screenshot_requests)}, clips={len(all_clip_requests)}")
        
        # 如果没有缓存,执桢畬整的VL分析流程
        if not all_screenshot_requests and not all_clip_requests:
            try:
                all_vl_semantic_units = list(semantic_units)
                aggregated_analysis_results: List[Any] = []
                aggregated_task_metadata: List[Dict[str, Any]] = []
                stream_semantic_units: List[Dict[str, Any]] = []
                legacy_pipeline_units: List[Dict[str, Any]] = list(all_vl_semantic_units)

                use_stream_unit_pipeline = bool(self.stream_unit_pipeline_enabled)
                if use_stream_unit_pipeline:
                    legacy_pipeline_units = []
                    legacy_unit_ids_sample: List[str] = []
                    for unit in all_vl_semantic_units:
                        semantic_unit = unit if isinstance(unit, dict) else {}
                        if self._should_stream_pipeline_for_unit(semantic_unit):
                            stream_semantic_units.append(unit)
                            continue
                        legacy_pipeline_units.append(unit)
                        unit_id = str(semantic_unit.get("unit_id", "") or "")
                        if unit_id and len(legacy_unit_ids_sample) < 3:
                            legacy_unit_ids_sample.append(unit_id)

                    if stream_semantic_units and legacy_pipeline_units:
                        logger.info(
                            "[VL-UnitStream] hybrid mode: stream_units=%s, legacy_units=%s, legacy_sample=%s",
                            len(stream_semantic_units),
                            len(legacy_pipeline_units),
                            legacy_unit_ids_sample,
                        )
                    elif stream_semantic_units:
                        logger.info(
                            "[VL-UnitStream] enabled: process/concrete units will be split+analyzed in streaming mode"
                        )
                    else:
                        logger.info(
                            "[VL-UnitStream] no eligible units for streaming; fallback to legacy pipeline"
                        )

                if stream_semantic_units:
                    analysis_results, task_metadata, _legacy_fallback_materials, pruned_units = await self._run_stream_unit_pipeline(
                        video_path=video_path,
                        semantic_units=stream_semantic_units,
                        resolved_output_dir=resolved_output_dir,
                        all_clip_requests=all_clip_requests,
                        all_screenshot_requests=all_screenshot_requests,
                        token_stats=token_stats,
                    )
                    aggregated_analysis_results.extend(analysis_results)
                    aggregated_task_metadata.extend(task_metadata)
                    token_stats["pruned_units"] += pruned_units

                    if not legacy_pipeline_units:
                        if self.merge_multistep_clip_requests:
                            all_clip_requests = self._merge_multistep_clip_requests(all_vl_semantic_units, all_clip_requests)

                        token_stats["saved_tokens_est"] = max(
                            0,
                            int(token_stats["total_tokens_baseline_est"] - token_stats["total_tokens_actual"]),
                        )
                        if token_stats["total_tokens_baseline_est"] > 0:
                            token_stats["saved_ratio_est"] = float(token_stats["saved_tokens_est"]) / float(
                                token_stats["total_tokens_baseline_est"]
                            )
                        else:
                            token_stats["saved_ratio_est"] = 0.0

                        logger.info(
                            "[VL-Token] units=%s, legacy_action=%s, pruned=%s, actual_total=%s, baseline_est=%s, saved_est=%s, saved_ratio=%.2f%%",
                            token_stats.get("vl_units", 0),
                            token_stats.get("stable_action_legacy_units", 0),
                            token_stats.get("pruned_units", 0),
                            token_stats.get("total_tokens_actual", 0),
                            token_stats.get("total_tokens_baseline_est", 0),
                            token_stats.get("saved_tokens_est", 0),
                            float(token_stats.get("saved_ratio_est", 0.0)) * 100.0,
                        )
                        unit_analysis_outputs = self._serialize_unit_analysis_outputs(
                            analysis_results=aggregated_analysis_results,
                            task_metadata=aggregated_task_metadata,
                        )

                        if self.config.get("save_cache", True):
                            self._save_vl_results(
                                cache_path=cache_path,
                                analysis_results=aggregated_analysis_results,
                                task_metadata=aggregated_task_metadata,
                                screenshot_requests=all_screenshot_requests,
                                clip_requests=all_clip_requests,
                            )

                        if self.screenshot_config.get("enabled", True) and all_screenshot_requests:
                            logger.info(f"开始批閲?CV 优化 {len(all_screenshot_requests)} 个截图请姹?..")
                            all_screenshot_requests = await self._apply_screenshot_optimization_with_bypass(
                                video_path=video_path,
                                screenshot_requests=all_screenshot_requests,
                            )
                        if all_screenshot_requests:
                            all_screenshot_requests = self._dedupe_incremental_legacy_drop_tail_screenshots(
                                video_path=video_path,
                                screenshot_requests=all_screenshot_requests,
                            )

                        result.clip_requests = all_clip_requests
                        result.screenshot_requests = all_screenshot_requests
                        result.token_stats = token_stats
                        result.unit_analysis_outputs = unit_analysis_outputs
                        self._write_phase2a_token_cost_audit(
                            output_dir=resolved_output_dir,
                            token_stats=token_stats,
                            unit_analysis_outputs=unit_analysis_outputs,
                            video_path=video_path,
                        )
                        result.success = True
                        logger.info(
                            "[VL-UnitStream] finished: clips=%s, screenshots=%s",
                            len(result.clip_requests),
                            len(result.screenshot_requests),
                        )
                        return result

                semantic_units = legacy_pipeline_units

                # 1. 切割视频为语义单元片娈?
                logger.info(f"开始切割视棰? {video_path}")
                clips_dir = await self._split_video_by_semantic_units(
                    video_path, 
                    semantic_units,
                    output_dir
                )
                
                if not clips_dir or not Path(clips_dir).exists():
                    raise RuntimeError("视频切割失败或输出目录不存在")
                
                # 2. 🚀 语义单元绾?VL 分析并栾紙每涓?unit 一娆?API锛?
                logger.info(f"开始并琛?VL 分析 {len(semantic_units)} 个语义单鍏?..")

                task_metadata = []  # 保存任务元数据以便后续匹閰?
                unit_tasks: List[Dict[str, Any]] = []
                for su in semantic_units:
                    unit_id = su.get("unit_id", "")
                    start_sec = float(su.get("start_sec", 0))
                    end_sec = float(su.get("end_sec", 0))
                    duration = max(0.0, end_sec - start_sec)
                    extra_prompt = None
                    analysis_mode = "default"
                    mode_override = str(
                        su.get("_vl_analysis_mode_override", su.get("vl_analysis_mode_override", ""))
                        or ""
                    ).strip().lower()
                    if mode_override in {"concrete", "concrete_focus"}:
                        analysis_mode = "concrete"
                    elif mode_override in {"tutorial", "tutorial_stepwise"}:
                        analysis_mode = "tutorial_stepwise"
                    elif self._is_tutorial_process_unit(su, duration):
                        analysis_mode = "tutorial_stepwise"
                        extra_prompt = self._build_tutorial_extra_prompt()
                    
                    # 查找对应的视频片娈?
                    clip_path = self._find_clip_for_unit(clips_dir, unit_id, start_sec, end_sec)
                    
                    if not clip_path:
                        logger.warning(f"未找到语义单鍏?{unit_id} 的视频片段，跳过")
                        continue

                    unit_tasks.append(
                        {
                            "semantic_unit": su,
                            "clip_path": clip_path,
                            "analysis_mode": analysis_mode,
                            "extra_prompt": extra_prompt,
                            "unit_id": unit_id,
                            "start_sec": start_sec,
                            "end_sec": end_sec,
                            "duration": duration,
                        }
                    )

                logger.info(
                    f"[VL-PrePrune] dispatch units={len(unit_tasks)}, skipped={len(semantic_units) - len(unit_tasks)}"
                )

                pre_prune_results = await self._resolve_pre_prune_results_for_unit_tasks(
                    clips_dir=clips_dir,
                    unit_tasks=unit_tasks,
                    force_preprocess=False,
                )

                legacy_action_tasks: List[Dict[str, Any]] = []
                vl_unit_tasks: List[Dict[str, Any]] = []
                vl_pre_prune_results: List[Dict[str, Any]] = []
                for index, task in enumerate(unit_tasks):
                    pre_prune_info = (
                        pre_prune_results[index]
                        if index < len(pre_prune_results)
                        else self._build_default_pre_prune_info(
                            semantic_unit=task.get("semantic_unit", {}),
                            clip_path=str(task.get("clip_path", "") or ""),
                        )
                    )
                    semantic_unit = task.get("semantic_unit", {})
                    raw_duration_sec = safe_float(task.get("duration", 0.0), 0.0)
                    if self._should_use_stable_action_legacy_branch(
                        semantic_unit=semantic_unit,
                        pre_prune_info=pre_prune_info,
                        raw_duration_sec=raw_duration_sec,
                    ):
                        legacy_action_tasks.append(
                            {
                                "task": task,
                                "pre_prune_info": pre_prune_info,
                            }
                        )
                    else:
                        vl_unit_tasks.append(task)
                        vl_pre_prune_results.append(pre_prune_info)

                token_stats["stable_action_legacy_units"] += len(legacy_action_tasks)
                legacy_fallback_materials: Dict[str, LegacyFallbackMaterial] = {}
                if legacy_action_tasks:
                    logger.info(
                        "[VL-PrePrune] legacy-action diversion: units=%s, trigger_ratio<%.3f",
                        len(legacy_action_tasks),
                        float(self.pre_vl_legacy_action_trigger_ratio),
                    )
                    legacy_dispatch_plan = await self._prepare_legacy_action_dispatch_plan(
                        clips_dir=clips_dir,
                        legacy_action_tasks=legacy_action_tasks,
                    )
                    all_clip_requests.extend(legacy_dispatch_plan.immediate_clip_requests)
                    all_screenshot_requests.extend(legacy_dispatch_plan.immediate_screenshot_requests)
                    vl_unit_tasks.extend(legacy_dispatch_plan.vl_unit_tasks)
                    vl_pre_prune_results.extend(legacy_dispatch_plan.vl_pre_prune_results)
                    legacy_fallback_materials = dict(legacy_dispatch_plan.fallback_materials)

                token_stats["vl_units"] += len(vl_unit_tasks)
                if legacy_action_tasks:
                    logger.info(
                        "[VL-PrePrune] legacy-action queued into VL: queued=%s, fallback_only=%s",
                        len(legacy_fallback_materials),
                        max(0, len(legacy_action_tasks) - len(legacy_fallback_materials)),
                    )

                analysis_results: List[Any] = []
                task_metadata = []
                pruned_units = 0
                streamed_result_indexes: set[int] = set()

                async def _on_stream_result(
                    result_index: int,
                    meta: Dict[str, Any],
                    analysis_result: Any,
                ) -> None:
                    if result_index in streamed_result_indexes:
                        return
                    await self._consume_unit_analysis_result_streaming(
                        result_index=result_index,
                        analysis_result=analysis_result,
                        meta=meta,
                        legacy_fallback_materials=legacy_fallback_materials,
                        all_clip_requests=all_clip_requests,
                        all_screenshot_requests=all_screenshot_requests,
                        token_stats=token_stats,
                        resolved_output_dir=resolved_output_dir,
                        original_video_path=video_path,
                    )
                    streamed_result_indexes.add(result_index)

                if vl_unit_tasks:
                    analysis_results, task_metadata, pruned_units = await self._analyze_unit_tasks_in_parallel(
                        unit_tasks=vl_unit_tasks,
                        pre_prune_results=vl_pre_prune_results,
                        on_result=_on_stream_result,
                    )
                token_stats["pruned_units"] += pruned_units
                aggregated_analysis_results.extend(analysis_results)
                aggregated_task_metadata.extend(task_metadata)
                
                # 收集所有成功的分析结果
                for idx, analysis_result in enumerate(analysis_results):
                    if idx in streamed_result_indexes:
                        continue
                    meta = task_metadata[idx] if idx < len(task_metadata) else {}
                    unit_id = meta.get("unit_id", f"task_{idx}")
                    
                    # 处理异常情况
                    if isinstance(analysis_result, Exception):
                        logger.warning(f"语义单元 {unit_id} VL 分析异常: {analysis_result}")
                        self._apply_legacy_fallback_material_if_exists(
                            unit_id=unit_id,
                            fallback_store=legacy_fallback_materials,
                            all_clip_requests=all_clip_requests,
                            all_screenshot_requests=all_screenshot_requests,
                            reason="exception",
                        )
                        continue
                    
                    if not analysis_result.success:
                        logger.warning(f"语义单元 {unit_id} VL 分析失败: {analysis_result.error_msg}")
                        self._apply_legacy_fallback_material_if_exists(
                            unit_id=unit_id,
                            fallback_store=legacy_fallback_materials,
                            all_clip_requests=all_clip_requests,
                            all_screenshot_requests=all_screenshot_requests,
                            reason="failure",
                        )
                        continue

                    await self._consume_unit_analysis_result_streaming(
                        result_index=idx,
                        analysis_result=analysis_result,
                        meta=meta,
                        legacy_fallback_materials=legacy_fallback_materials,
                        all_clip_requests=all_clip_requests,
                        all_screenshot_requests=all_screenshot_requests,
                        token_stats=token_stats,
                        resolved_output_dir=resolved_output_dir,
                        original_video_path=video_path,
                    )
                    streamed_result_indexes.add(idx)
                    continue

                
                if self.merge_multistep_clip_requests:
                    all_clip_requests = self._merge_multistep_clip_requests(all_vl_semantic_units, all_clip_requests)
                logger.info(f"VL 分析汇鎬? clips={len(all_clip_requests)}, screenshots={len(all_screenshot_requests)}")

                token_stats["saved_tokens_est"] = max(
                    0,
                    int(token_stats["total_tokens_baseline_est"] - token_stats["total_tokens_actual"]),
                )
                if token_stats["total_tokens_baseline_est"] > 0:
                    token_stats["saved_ratio_est"] = float(token_stats["saved_tokens_est"]) / float(token_stats["total_tokens_baseline_est"])
                else:
                    token_stats["saved_ratio_est"] = 0.0

                logger.info(
                    "[VL-Token] units=%s, legacy_action=%s, pruned=%s, actual_total=%s, baseline_est=%s, saved_est=%s, saved_ratio=%.2f%%",
                    token_stats.get("vl_units", 0),
                    token_stats.get("stable_action_legacy_units", 0),
                    token_stats.get("pruned_units", 0),
                    token_stats.get("total_tokens_actual", 0),
                    token_stats.get("total_tokens_baseline_est", 0),
                    token_stats.get("saved_tokens_est", 0),
                    float(token_stats.get("saved_ratio_est", 0.0)) * 100.0,
                )
                unit_analysis_outputs = self._serialize_unit_analysis_outputs(
                    analysis_results=aggregated_analysis_results,
                    task_metadata=aggregated_task_metadata,
                )
                
                # 保存VL分析原始结果(CV优化鍓?
                if self.config.get("save_cache", True):
                    self._save_vl_results(
                        cache_path=cache_path,
                        analysis_results=aggregated_analysis_results,
                        task_metadata=aggregated_task_metadata,
                        screenshot_requests=all_screenshot_requests,
                        clip_requests=all_clip_requests
                    )
                
            except Exception as e:
                logger.error(f"VL 分析失败: {e}")
                result.success = False
                result.error_msg = str(e)
                return result
        
        # 3. 🚀 批量 CV 优化截图时间鐐?(无论是否使用缓存,都要执!)
        try:
            if self.screenshot_config.get("enabled", True) and all_screenshot_requests:
                logger.info(f"开始批閲?CV 优化 {len(all_screenshot_requests)} 个截图请姹?..")
                all_screenshot_requests = await self._apply_screenshot_optimization_with_bypass(
                    video_path=video_path,
                    screenshot_requests=all_screenshot_requests,
                )

            if all_screenshot_requests:
                all_screenshot_requests = self._dedupe_incremental_legacy_drop_tail_screenshots(
                    video_path=video_path,
                    screenshot_requests=all_screenshot_requests,
                )
            
            # 汇总最终结鏋?
            result.clip_requests = all_clip_requests
            result.screenshot_requests = all_screenshot_requests
            result.token_stats = token_stats
            result.unit_analysis_outputs = unit_analysis_outputs
            self._write_phase2a_token_cost_audit(
                output_dir=resolved_output_dir,
                token_stats=token_stats,
                unit_analysis_outputs=unit_analysis_outputs,
                video_path=video_path,
            )
            result.success = True
            
            logger.info(
                f"VL 素材生成完成: clips={len(result.clip_requests)}, "
                f"screenshots={len(result.screenshot_requests)}"
            )
            
        except Exception as e:
            logger.error(f"VL 素材生成失败: {e}")
            result.success = False
            result.error_msg = str(e)
            
            # 检查是否需要回退
            if self._should_fallback(e):
                result.used_fallback = True
                result.fallback_reason = str(e)
        
        return result
    
    async def _split_video_by_semantic_units(
        self,
        video_path: str,
        semantic_units: List[Dict[str, Any]],
        output_dir: str = None
    ) -> Optional[str]:
        """Compatibility wrapper that delegates to `flow_ops`."""
        return await split_video_by_semantic_units(self, video_path, semantic_units, output_dir=output_dir)
    
    def _find_clip_for_unit(
        self,
        clips_dir: str,
        unit_id: str,
        start_sec: float,
        end_sec: float
    ) -> Optional[str]:
        """Compatibility wrapper that delegates to `flow_ops`."""
        return find_clip_for_unit(self, clips_dir, unit_id, start_sec, end_sec)
    
    async def _optimize_screenshot_timestamps(
        self,
        video_path: str,
        screenshot_requests: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        浼樺寲鎴浘鏃堕棿閻?

        瀵规瘡涓缓璁殑鎴浘鏃堕棿鎴筹紝閸?卤1s 鑼冨洿鍐呬娇閻?screenshot_selector 鏌ユ壘鏈€浣冲抚

        Args:
            video_path: 鍘熻棰戣矾瀵?
            screenshot_requests: 鎴浘璇锋眰鍒楄〃

        Returns:
            List[Dict]: 浼樺寲鍚庣殑鎴浘璇锋眰
        """
        if not screenshot_requests:
            return []

        time_window_before, time_window_after = self._resolve_screenshot_time_window()
        static_island_threshold_ms = self._resolve_screenshot_static_island_threshold_ms()
        optimized: List[Dict[str, Any]] = []

        try:
            from services.python_grpc.src.content_pipeline.phase2a.vision.screenshot_selector import ScreenshotSelector

            selector = ScreenshotSelector.create_lightweight()

            for req in screenshot_requests:
                original_ts = safe_float(req.get("timestamp_sec", 0.0), 0.0)

                default_start = max(0.0, original_ts - time_window_before)
                default_end = original_ts + time_window_after
                raw_window_start = req.get("_window_start_sec")
                raw_window_end = req.get("_window_end_sec")
                if raw_window_start is None and raw_window_end is None:
                    search_start = default_start
                    search_end = default_end
                else:
                    try:
                        search_start = float(raw_window_start) if raw_window_start is not None else default_start
                    except (TypeError, ValueError):
                        search_start = default_start
                    try:
                        search_end = float(raw_window_end) if raw_window_end is not None else default_end
                    except (TypeError, ValueError):
                        search_end = default_end
                    search_start = max(0.0, search_start)
                    if search_end < search_start:
                        search_end = search_start

                try:
                    best_screenshots = selector.select_screenshots_for_range_sync(
                        video_path=video_path,
                        start_sec=search_start,
                        end_sec=search_end,
                        coarse_fps=2.0,
                        fine_fps=10.0,
                        min_static_island_ms=static_island_threshold_ms,
                    )
                    normalized_candidates = self._normalize_cv_candidate_screenshots(
                        best_screenshots,
                        fallback_timestamp=original_ts,
                        fallback_score=0.0,
                    )
                    best_candidate = normalized_candidates[0]
                    best_ts = safe_float(best_candidate.get("timestamp_sec", original_ts), original_ts)
                    best_score = safe_float(best_candidate.get("score", 0.0), 0.0)

                    req["timestamp_sec"] = float(best_ts)
                    req["_optimized"] = True
                    req["_original_timestamp"] = float(original_ts)
                    req["_cv_quality_score"] = float(best_score)
                    req["_cv_candidate_screenshots"] = normalized_candidates
                    req["_cv_static_island_threshold_ms"] = float(static_island_threshold_ms)
                    logger.debug(
                        f"鎴浘鏃堕棿浼樺寲: {original_ts:.2f}s -> {best_ts:.2f}s "
                        f"(score={best_score:.2f})"
                    )
                except Exception as error:
                    logger.warning(
                        "Screenshot optimization failed, fallback to original timestamp: %s",
                        error,
                    )
                    req["_cv_candidate_screenshots"] = self._normalize_cv_candidate_screenshots(
                        [],
                        fallback_timestamp=original_ts,
                        fallback_score=0.0,
                    )
                    req["_cv_static_island_threshold_ms"] = float(static_island_threshold_ms)

                optimized.append(req)

        except ImportError:
            logger.warning("screenshot_selector is unavailable, skip screenshot optimization")
            return screenshot_requests
        except Exception as error:
            logger.warning(f"鎴浘浼樺寲澶辫触: {error}")
            return screenshot_requests

        return optimized
    async def _optimize_screenshots_parallel(
        self,
        video_path: str,
        screenshot_requests: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        并桎紭化截图时间鐐?(使用 cv_worker 进程姹?+ 共享内存)
        
        支持两种模式:
        - 流式模式 (streaming_pipeline=true): 边预读边提交,IO/Compute 重叠
        - 批量模式 (streaming_pipeline=false): 批量预读后提浜?保持向后兼容
        
        Args:
            video_path: 原视频路寰?
            screenshot_requests: 截图请求列表
            
        Returns:
            List[Dict]: 优化后的截图请求
        """
        if not screenshot_requests:
            return []
        
        # 检查是否启用流式处鐞?(默认启用)
        use_streaming = self.screenshot_config.get("streaming_pipeline", True)
        
        if use_streaming:
            logger.info("🚀 使用流式处理模式 (streaming_pipeline=true)")
            return await self._run_screenshot_optimization_with_task_gate(
                mode="streaming",
                video_path=video_path,
                screenshot_requests=screenshot_requests,
            )
        else:
            logger.info("🚀 使用批量处理模式 (streaming_pipeline=false)")
            return await self._run_screenshot_optimization_with_task_gate(
                mode="batch",
                video_path=video_path,
                screenshot_requests=screenshot_requests,
            )
    
    async def _run_screenshot_optimization_with_task_gate(
        self,
        *,
        mode: str,
        video_path: str,
        screenshot_requests: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """以 task 粒度限制截图优化并发，避免多任务同时占满 SHM。"""
        task_gate_limit = _resolve_screenshot_task_gate_limit(self.screenshot_config)
        task_gate = _resolve_screenshot_task_gate(task_gate_limit)
        logger.info(
            "[VL] screenshot task gate wait: limit=%s, mode=%s, requests=%s, video=%s",
            task_gate_limit,
            mode,
            len(screenshot_requests),
            video_path,
        )
        await task_gate.acquire()
        try:
            logger.info(
                "[VL] screenshot task gate acquired: limit=%s, mode=%s, requests=%s, video=%s",
                task_gate_limit,
                mode,
                len(screenshot_requests),
                video_path,
            )
            if mode == "streaming":
                return await self._optimize_screenshots_streaming_pipeline(video_path, screenshot_requests)
            return await self._optimize_screenshots_batch_mode(video_path, screenshot_requests)
        finally:
            task_gate.release()
            logger.info(
                "[VL] screenshot task gate released: limit=%s, mode=%s, requests=%s, video=%s",
                task_gate_limit,
                mode,
                len(screenshot_requests),
                video_path,
            )

    async def _optimize_screenshots_batch_mode(
        self,
        video_path: str,
        screenshot_requests: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Compatibility wrapper that delegates to `flow_ops`."""
        return await optimize_screenshots_batch_mode(self, video_path, screenshot_requests)

    def _annotate_screenshot_requests_with_unit_context(
        self,
        *,
        screenshot_requests: List[Dict[str, Any]],
        semantic_unit: Dict[str, Any],
        analysis_mode: str,
    ) -> None:
        """补齐截图请求上下文，保留 concrete 向后搜寻 2s 所需的窗口与预取参数。"""
        if not screenshot_requests:
            return

        normalized_mode = str(analysis_mode or "").strip().lower() or "default"
        unit_knowledge_type = self._normalize_should_type(
            (semantic_unit or {}).get("knowledge_type", "")
        )

        for item in screenshot_requests:
            if not isinstance(item, dict):
                continue

            request_mode = str(item.get("analysis_mode", "") or "").strip().lower()
            item["analysis_mode"] = request_mode or normalized_mode

            request_knowledge_type = self._normalize_should_type(item.get("knowledge_type", ""))
            if unit_knowledge_type and not request_knowledge_type:
                item["knowledge_type"] = unit_knowledge_type

            resolved_mode = str(item.get("analysis_mode", "") or "").strip().lower()
            resolved_knowledge_type = self._normalize_should_type(item.get("knowledge_type", ""))
            if resolved_mode == "concrete" or resolved_knowledge_type == "concrete":
                concrete_ts = max(0.0, safe_float(item.get("timestamp_sec", 0.0), 0.0))
                concrete_profile = self._resolve_concrete_forward_search_profile()
                item["_window_start_sec"] = concrete_ts
                item["_window_end_sec"] = concrete_ts + float(concrete_profile["forward_after_seconds"])
                item["_cv_prefetch_profile"] = str(concrete_profile["profile_key"])
                item["_prefetch_chunk_max_span_seconds"] = float(concrete_profile["chunk_max_span_seconds"])
                item["_prefetch_chunk_max_requests"] = int(concrete_profile["chunk_max_requests"])
                item["_prefetch_sample_rate"] = int(concrete_profile["sample_rate"])
                item["_prefetch_target_height"] = int(concrete_profile["target_height"])

    def _should_bypass_screenshot_cv_optimization(self, request: Dict[str, Any]) -> bool:
        """
        仅对显式标记的请求跳过 CV 优化。
        """
        if not isinstance(request, dict):
            return False
        return bool(request.get("_skip_cv_optimization", False))


    async def _apply_screenshot_optimization_with_bypass(
        self,
        *,
        video_path: str,
        screenshot_requests: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        对截图优化增加“显式旁路”策略：
        - 显式标记 `_skip_cv_optimization` 的请求：直接保留当前时间戳；
        - 其他请求：继续走既有 CV 优化链路。
        """
        if not screenshot_requests:
            return []
        if not self.screenshot_config.get("enabled", True):
            return screenshot_requests

        optimize_indices: List[int] = []
        optimize_requests: List[Dict[str, Any]] = []
        bypass_count = 0
        for index, request in enumerate(screenshot_requests):
            if self._should_bypass_screenshot_cv_optimization(request):
                bypass_count += 1
                continue
            optimize_indices.append(index)
            optimize_requests.append(request)

        if not optimize_requests:
            logger.info(
                "[VL] screenshot CV optimize bypassed: explicit_skip=%s, optimize=0",
                bypass_count,
            )
            return screenshot_requests

        logger.info(
            "[VL] screenshot CV optimize start: total=%s, optimize=%s, bypass=%s",
            len(screenshot_requests),
            len(optimize_requests),
            bypass_count,
        )
        optimized_requests = await self._optimize_screenshots_parallel(
            video_path=video_path,
            screenshot_requests=optimize_requests,
        )
        if len(optimized_requests) != len(optimize_indices):
            logger.warning(
                "[VL] screenshot optimize size mismatch: expected=%s, actual=%s; fallback to original for overflow",
                len(optimize_indices),
                len(optimized_requests),
            )

        merged_requests: List[Dict[str, Any]] = list(screenshot_requests)
        optimized_cursor = 0
        for index in optimize_indices:
            if optimized_cursor < len(optimized_requests):
                merged_requests[index] = optimized_requests[optimized_cursor]
                optimized_cursor += 1
            else:
                break

        if self.best_frame_vision_select_enabled:
            merged_requests = await self._apply_best_frame_vision_selection(
                video_path=video_path,
                screenshot_requests=merged_requests,
            )
        return merged_requests

    def _is_legacy_action_drop_tail_screenshot_request(self, request: Dict[str, Any]) -> bool:
        """判断请求是否属于 process 静态主导降级分支的 drop-tail 截图。"""
        analysis_mode = str(request.get("analysis_mode", "") or "").strip().lower()
        if analysis_mode != "legacy_action_units":
            return False
        anchor_role = str(request.get("anchor_role", "") or "").strip().lower()
        if anchor_role != "tail":
            return False
        label = str(request.get("label", "") or "").strip().lower()
        return label.startswith("action_drop_") and label.endswith("_tail")

    def _should_force_inline_transcode_for_vl_subset(self, video_path: str) -> bool:
        """
        判断是否命中 VL 片段子集目录。

        仅当路径位于 `semantic_unit_clips_vl` 时，才强制 AV1 内联转码。
        这样可以把转码影响面限制在待分析片段，不扩散到全量原视频链路。
        """
        normalized_path = str(video_path or "").replace("\\", "/").strip().lower()
        if not normalized_path:
            return False
        parts = [part for part in normalized_path.split("/") if part]
        return "semantic_unit_clips_vl" in parts

    def _open_video_capture_with_subset_policy(self, video_path: str):
        """
        统一封装 OpenCV 打开策略。

        做什么：命中 `semantic_unit_clips_vl` 时强制内联转码，其余路径沿用默认策略。
        为什么：保证“仅子集转码”边界在所有调用点一致。
        """
        force_inline = self._should_force_inline_transcode_for_vl_subset(video_path)
        return open_video_capture_with_fallback(
            video_path,
            logger=logger,
            allow_inline_transcode=True if force_inline else None,
        )

    def _dedupe_incremental_legacy_drop_tail_screenshots(
        self,
        *,
        video_path: str,
        screenshot_requests: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        对 process 降级分支 drop-tail 截图应用“增量截图去重”。
        策略：
        1) 仅在同一 semantic_unit 内比较，避免跨单元误伤；
        2) 复用 worker 里的 OCR/形状签名增量规则；
        3) 仅移除被增量覆盖的旧截图，保留更完整候选。
        """
        if not screenshot_requests:
            return screenshot_requests

        grouped_indices: Dict[str, List[int]] = {}
        for idx, req in enumerate(screenshot_requests):
            if not self._is_legacy_action_drop_tail_screenshot_request(req):
                continue
            unit_id = str(req.get("semantic_unit_id", "") or "")
            group_key = unit_id if unit_id else "__unknown_unit__"
            grouped_indices.setdefault(group_key, []).append(idx)

        active_groups = {k: v for k, v in grouped_indices.items() if len(v) > 1}
        if not active_groups:
            return screenshot_requests

        try:
            import cv2
            from services.python_grpc.src.vision_validation.worker import (
                _extract_ocr_tokens,
                _extract_shape_signature,
                _filter_incremental_screenshots,
            )
        except Exception as import_error:
            logger.warning(
                "[VL-PrePrune] skip legacy drop-tail incremental dedupe, helper import failed: %s",
                import_error,
            )
            return screenshot_requests

        cap, effective_video_path, _ = self._open_video_capture_with_subset_policy(video_path)
        if cap is None or not cap.isOpened():
            logger.warning(
                "[VL-PrePrune] skip legacy drop-tail incremental dedupe, cannot open video: source=%s, effective=%s",
                video_path,
                effective_video_path,
            )
            return screenshot_requests

        keep_indices = set(range(len(screenshot_requests)))
        removed_count = 0

        try:
            fps_val = float(cap.get(cv2.CAP_PROP_FPS) or 30.0)
            for group_key, indices in active_groups.items():
                candidates: List[Dict[str, Any]] = []
                for idx in indices:
                    req = screenshot_requests[idx]
                    timestamp_sec = safe_float(req.get("timestamp_sec", 0.0), 0.0)
                    quality_score = safe_float(
                        req.get("_cv_quality_score", req.get("score", 0.0)),
                        0.0,
                    )
                    frame = None
                    if fps_val > 0.0:
                        frame_idx = int(max(0.0, timestamp_sec) * fps_val)
                        cap.set(cv2.CAP_PROP_POS_FRAMES, frame_idx)
                        ok, sampled = cap.read()
                        if ok and sampled is not None:
                            frame = sampled

                    ocr_tokens = sorted(_extract_ocr_tokens(frame, None)) if frame is not None else []
                    shape_signature = (
                        _extract_shape_signature(frame, None)
                        if frame is not None
                        else {"rect_count": 0, "component_count": 0, "edge_density": 0.0}
                    )
                    candidates.append(
                        {
                            "_idx": idx,
                            "timestamp_sec": timestamp_sec,
                            "score": quality_score,
                            "ocr_tokens": ocr_tokens,
                            "shape_signature": shape_signature,
                        }
                    )

                filtered = _filter_incremental_screenshots(candidates)
                keep_in_group = set()
                for item in filtered:
                    try:
                        keep_in_group.add(int(item.get("_idx")))
                    except (TypeError, ValueError):
                        continue
                if not keep_in_group:
                    latest_idx = max(
                        indices,
                        key=lambda i: safe_float(screenshot_requests[i].get("timestamp_sec", 0.0), 0.0),
                    )
                    keep_in_group = {latest_idx}

                for idx in indices:
                    if idx in keep_in_group:
                        continue
                    if idx in keep_indices:
                        keep_indices.remove(idx)
                        removed_count += 1

                logger.info(
                    "[VL-PrePrune] legacy drop-tail incremental dedupe: unit=%s, before=%s, kept=%s, removed=%s",
                    group_key,
                    len(indices),
                    len(keep_in_group),
                    len(indices) - len(keep_in_group),
                )
        finally:
            cap.release()

        if removed_count <= 0:
            return screenshot_requests

        deduped_requests = [
            req for idx, req in enumerate(screenshot_requests)
            if idx in keep_indices
        ]
        logger.info(
            "[VL-PrePrune] legacy drop-tail incremental dedupe summary: before=%s, after=%s, removed=%s",
            len(screenshot_requests),
            len(deduped_requests),
            removed_count,
        )
        return deduped_requests
    
    def _is_truthy_env(self, name: str, default: str = "0") -> bool:
        """解析环境变量真值。"""
        value = os.getenv(name, default).strip().lower()
        return value in {"1", "true", "yes", "y", "on"}

    def _resolve_screenshot_time_window(self) -> Tuple[float, float]:
        """瑙ｆ瀽鎴浘鏃堕棿绐楀彛锛屽吋瀹硅€佺殑瀵圭О閰嶇疆銆?"""
        legacy_window = safe_float(self.screenshot_config.get("time_window_seconds", 1.0), 1.0)
        if legacy_window < 0.0:
            legacy_window = 0.0
        before = safe_float(
            self.screenshot_config.get("time_window_before_seconds", legacy_window),
            legacy_window,
        )
        after = safe_float(
            self.screenshot_config.get("time_window_after_seconds", legacy_window),
            legacy_window,
        )
        return max(0.0, float(before)), max(0.0, float(after))

    def _resolve_screenshot_static_island_threshold_ms(self) -> float:
        """瑙ｆ瀽闈欐€佸矝鏈€灏忔椂闀块槇鍊硷紝鍚戝悗鍏煎鏃ф牸寮忛厤缃€?"""
        raw_value = self.screenshot_config.get(
            "static_island_min_ms",
            self.screenshot_config.get("static_island_threshold_ms", 200.0),
        )
        threshold_ms = safe_float(raw_value, 200.0)
        return max(0.0, min(5000.0, float(threshold_ms)))

    def _resolve_concrete_forward_search_profile(self) -> Dict[str, Any]:
        """解析 concrete 截图的轻量 forward-search 配置。"""
        _, default_after = self._resolve_screenshot_time_window()
        base_sample_rate = max(1, int(self.screenshot_config.get("prefetch_sample_rate", 2) or 2))
        base_target_height = max(0, int(self.screenshot_config.get("prefetch_target_height", 360) or 360))
        base_chunk_max_requests = max(1, int(self.screenshot_config.get("prefetch_chunk_max_requests", 1000) or 1000))

        forward_after_seconds = max(
            0.0,
            safe_float(
                self.screenshot_config.get("concrete_forward_search_after_seconds", default_after),
                default_after,
            ),
        )
        chunk_max_span_seconds = max(
            0.5,
            safe_float(
                self.screenshot_config.get(
                    "concrete_prefetch_chunk_max_span_seconds",
                    max(2.5, forward_after_seconds + 0.5),
                ),
                max(2.5, forward_after_seconds + 0.5),
            ),
        )
        chunk_max_requests = max(
            1,
            int(
                safe_float(
                    self.screenshot_config.get("concrete_prefetch_chunk_max_requests", min(64, base_chunk_max_requests)),
                    min(64, base_chunk_max_requests),
                )
            ),
        )
        sample_rate = max(
            base_sample_rate,
            int(
                safe_float(
                    self.screenshot_config.get("concrete_prefetch_sample_rate", max(base_sample_rate, 3)),
                    max(base_sample_rate, 3),
                )
            ),
        )
        default_target_height = min(base_target_height, 240) if base_target_height > 0 else 240
        target_height = max(
            0,
            int(
                safe_float(
                    self.screenshot_config.get("concrete_prefetch_target_height", default_target_height),
                    default_target_height,
                )
            ),
        )
        return {
            "forward_after_seconds": forward_after_seconds,
            "chunk_max_span_seconds": chunk_max_span_seconds,
            "chunk_max_requests": chunk_max_requests,
            "sample_rate": sample_rate,
            "target_height": target_height,
            "profile_key": "concrete_forward",
        }

    def _resolve_max_workers(self, request_count: int) -> int:
        """解析截图并发 worker 数量上限。"""
        return resolve_max_workers(
            request_count,
            cv_executor=self._cv_executor,
            screenshot_config=self.screenshot_config,
            hard_cap=6,
        )

    def _build_screenshot_prefetch_chunks(
        self,
        *,
        screenshot_requests: List[Dict[str, Any]],
        max_span_seconds: float,
        max_requests: int,
        time_window: Optional[float] = None,
        time_window_before: Optional[float] = None,
        time_window_after: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        """
        将截图请求按时间聚类为多涓?chunk銆?

        目的锛?
        - 每个 chunk 用一娆?Union 预读覆盖区间，避免对短视频反澶?seek/read锛?
        - 同时把单娆?Union 区间限制鍦?max_span_seconds 内，防止一次预读过大；
        - 涓?double-buffer overlap 预留“chunk 绾?SHM 生命周期”边界，避免璺?chunk 淘汰 unlink銆?

        返回：chunk 列表，每涓?chunk 包含：union_start/union_end/windows銆?
        windows 内结构用于构寤?worker 任务参数銆?
        """
        return build_screenshot_prefetch_chunks(
            screenshot_requests=screenshot_requests,
            max_span_seconds=max_span_seconds,
            max_requests=max_requests,
            time_window=time_window,
            time_window_before=time_window_before,
            time_window_after=time_window_after,
        )

    def _prefetch_union_frames_to_registry_sync(
        self,
        extractor: Any,
        registry_cls: Any,
        union_start: float,
        union_end: float,
        sample_rate: int,
        target_height: int,
    ) -> Tuple[Any, Dict[float, Any], float, float]:
        """
        同步预读 + 写入 chunk 专属 SharedMemory Registry銆?

        注意：此函数会被 asyncio.to_thread 调用，以实现主线程可 drain 已完成的 worker 结果锛?
        形成 IO/Compute 重叠銆?
        """
        # 背景：短窗口锛?5s）走 OpenCV Random Access（多娆?cap.set）会非常慢，导致 worker 长时间空闲銆?
        # 这里改为“单娆?seek + 顺序 read 扫描”，只在命中鐨?target frame 涓?resize + 写入 SHM銆?
        # 这样 prefetch 成本大幅下降，CPU 更能花在 worker 计算上銆?
        import cv2

        video_path = getattr(extractor, "video_path", None) or getattr(extractor, "video", None)
        if not video_path:
            return None, {}, 0.0, 0.0

        t0 = time.perf_counter()
        cap, effective_video_path, _ = self._open_video_capture_with_subset_policy(video_path)
        if cap is None or not cap.isOpened():
            logger.warning(
                "Prefetch union cannot open video: source=%s, effective=%s",
                video_path,
                effective_video_path,
            )
            return None, {}, (time.perf_counter() - t0) * 1000.0, 0.0

        try:
            fps = cap.get(cv2.CAP_PROP_FPS) or float(getattr(extractor, "fps", 30.0) or 30.0)
            total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
            if total_frames <= 0:
                total_frames = int(getattr(extractor, "frame_count", 0) or 0)

            start_frame = int(max(0.0, union_start) * fps)
            end_frame = int(max(0.0, union_end) * fps)
            if total_frames > 0:
                start_frame = max(0, min(start_frame, total_frames - 1))
                end_frame = max(start_frame, min(end_frame, total_frames - 1))

            source_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH) or 0)
            source_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT) or 0)
            effective_height = source_height if source_height > 0 else max(target_height, 1)
            effective_width = source_width if source_width > 0 else max(2, effective_height)
            if target_height > 0 and effective_height > 0 and effective_width > 0:
                effective_width = max(2, int((effective_width / effective_height) * target_height))
                effective_width = max(2, (effective_width // 2) * 2)
                effective_height = target_height
            estimated_frame_bytes = max(1, effective_width * max(1, effective_height) * 3)
            registry_byte_budget = int(
                max(1, getattr(registry_cls, "resolve_default_max_bytes", lambda: 64 * 1024 * 1024)())
            )
            max_frames_per_chunk = int(
                self.screenshot_config.get("prefetch_max_frames_per_chunk", 240)
            )
            byte_safe_frame_cap = max(1, registry_byte_budget // estimated_frame_bytes)
            max_frames_per_chunk = max(1, min(max_frames_per_chunk, byte_safe_frame_cap))
            step = resolve_adaptive_prefetch_step(
                start_frame=start_frame,
                end_frame=end_frame,
                sample_rate=sample_rate,
                max_frames_per_chunk=max_frames_per_chunk,
            )

            sampled_frame_count = int((end_frame - start_frame) // step) + 2

            # 该 chunk 内不允许淘汰：max_frames 覆盖本次候选帧数。
            registry = registry_cls(
                max_frames=max(10, sampled_frame_count + 10),
                max_bytes=registry_byte_budget,
            )

            # Seek once, then sequential scan
            cap.set(cv2.CAP_PROP_POS_FRAMES, start_frame)
            current_idx = start_frame
            next_target_idx = start_frame

            ts_to_shm_ref: Dict[float, Any] = {}
            register_ms = 0.0

            while current_idx <= end_frame:
                ret, frame = cap.read()
                if not ret or frame is None:
                    break

                should_sample = (current_idx == next_target_idx) or (current_idx == end_frame)
                if should_sample:
                    # Downsample to proxy height for memory safety + speed
                    h, w = frame.shape[:2]
                    if h > 0 and w > 0 and target_height > 0:
                        target_w = int((w / h) * target_height)
                        target_w = (target_w // 2) * 2
                        if target_w <= 0:
                            target_w = 2
                        frame = cv2.resize(frame, (target_w, target_height))

                    ts = float(current_idx / fps) if fps > 0 else float(union_start)
                    t_reg0 = time.perf_counter()
                    registry.register_frame(current_idx, frame)
                    shm_ref = registry.get_shm_ref(current_idx)
                    register_ms += (time.perf_counter() - t_reg0) * 1000.0
                    if shm_ref:
                        ts_to_shm_ref[ts] = shm_ref

                    if current_idx >= next_target_idx:
                        next_target_idx = next_target_idx + step

                current_idx += 1

            prefetch_total_ms = (time.perf_counter() - t0) * 1000.0
            prefetch_ms = max(0.0, prefetch_total_ms - register_ms)
            return registry, ts_to_shm_ref, prefetch_ms, register_ms
        finally:
            cap.release()

    def _build_task_params_from_ts_map(
        self,
        *,
        windows: List[Dict[str, Any]],
        ts_to_shm_ref: Dict[float, Any],
        fps: float,
    ) -> List[Dict[str, Any]]:
        """基于预读帧映射构建 worker 任务参数。"""
        return build_task_params_from_ts_map(
            windows=windows,
            ts_to_shm_ref=ts_to_shm_ref,
            fps=fps,
        )

    async def _maybe_warmup_pool(self, *, loop: asyncio.AbstractEventLoop, executor: Any, worker_count: int) -> None:
        """按需执行进程池 warmup。"""
        if not self._is_truthy_env("CV_POOL_WARMUP", "0"):
            return

        warmup_n = int(os.getenv("CV_POOL_WARMUP_N", str(worker_count)))
        warmup_n = max(1, min(warmup_n, max(1, worker_count * 2)))
        try:
            from services.python_grpc.src.vision_validation.worker import warmup_worker
        except Exception as e:
            logger.warning(f"Warmup skipped: cannot import warmup_worker: {e}")
            return

        futures = [loop.run_in_executor(executor, warmup_worker) for _ in range(warmup_n)]
        results = await asyncio.gather(*futures, return_exceptions=True)
        pids = sorted({r for r in results if isinstance(r, int)})
        logger.info(f"🔥 [Warmup] tasks={warmup_n}, unique_pids={pids}")

    def _apply_selection_result(self, *, req: Dict[str, Any], original_ts: float, unit_id: str, result: Any) -> None:
        """
        鐏?worker 杩斿洖缁撴灉鍐欏洖閸?request锛堝師鍦版洿鏂帮級閵?

        绾︽潫锛氫笉鏀瑰彉 screenshot_requests 鐨勯『搴忥紱浠呮洿閺?timestamp_sec 涓庤瘖鏂瓧娈甸妴?
        """
        if isinstance(result, Exception):
            logger.warning(f"CV Worker 寮傚父: {unit_id}: {result}")
            return

        if isinstance(result, dict) and "selected_timestamp" in result:
            selected_ts = safe_float(result.get("selected_timestamp", original_ts), original_ts)
            quality_score = safe_float(result.get("quality_score", 0.0), 0.0)
            req["timestamp_sec"] = float(selected_ts)
            req["_optimized"] = True
            req["_original_timestamp"] = float(original_ts)
            req["_cv_quality_score"] = float(quality_score)
            req["_cv_candidate_screenshots"] = self._normalize_cv_candidate_screenshots(
                result.get("candidate_screenshots"),
                fallback_timestamp=selected_ts,
                fallback_score=quality_score,
            )
            default_threshold_ms = self._resolve_screenshot_static_island_threshold_ms()
            req["_cv_static_island_threshold_ms"] = safe_float(
                result.get("static_island_threshold_ms", default_threshold_ms),
                default_threshold_ms,
            )
            logger.debug(
                f"CV 浼樺寲: {unit_id}: {original_ts:.2f}s 閳?{selected_ts:.2f}s "
                f"(score={quality_score:.3f})"
            )
    async def _optimize_screenshots_streaming_pipeline(
        self,
        video_path: str,
        screenshot_requests: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Compatibility wrapper that delegates to `flow_ops`."""
        return await optimize_screenshots_streaming_pipeline(self, video_path, screenshot_requests)
    
    def _should_fallback(self, error: Exception) -> bool:
        """
        检查是否应该回退到原有流绋?
        
        Args:
            error: 发生的异甯?
            
        Returns:
            bool: 是否应该回退
        """
        if not self.fallback_config.get("enabled", True):
            return False
        
        error_str = str(error).lower()
        
        # JSON 解析错误
        if self.fallback_config.get("on_parse_error", True):
            if "json" in error_str or "parse" in error_str or "decode" in error_str:
                return True
        
        # API 错误
        if self.fallback_config.get("on_api_error", True):
            if "api" in error_str or "request" in error_str or "connection" in error_str:
                return True
            if "401" in error_str or "403" in error_str or "500" in error_str:
                return True
        
        return True  # 默认回退
