"""
VL Material Generator - VL 素材生成器

功能：
1. 调用 split_video_by_semantic_units.py 切割语义单元视频片段
2. 对每个片段调用 VLVideoAnalyzer 进行 VL 分析
3. 汇总分析结果生成素材请求
4. 优化截图时间点（在 ±1s 范围内查找最佳帧）
5. 失败时自动回退到现有 GenerateMaterialRequests 流程

使用方式：
    generator = VLMaterialGenerator(config)
    result = await generator.generate(video_path, semantic_units)
"""

import os
import json
import logging
import asyncio
import time
import re
import functools
import threading
from collections import deque
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

from services.python_grpc.src.common.utils.numbers import safe_float
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
)
from services.python_grpc.src.content_pipeline.infra.runtime.vl_ffmpeg_utils import (
    export_clip_asset_with_ffmpeg,
    export_keyframe_with_ffmpeg,
    concat_segments_with_ffmpeg,
)
from services.python_grpc.src.content_pipeline.phase2a.materials.models import VLGenerationResult
from services.python_grpc.src.content_pipeline.phase2a.materials.errors import (
    VLMaterialGeneratorError,
    VLAnalysisError,
    JSONParseError,
)
from services.python_grpc.src.content_pipeline.common.utils.path_utils import find_repo_root
from services.python_grpc.src.content_pipeline.phase2a.materials.flow_ops import (
    split_video_by_semantic_units,
    find_clip_for_unit,
    optimize_screenshots_batch_mode,
    optimize_screenshots_streaming_pipeline,
)

logger = logging.getLogger(__name__)


class VLMaterialGenerator:
    """
    VL 素材生成器
    
    负责：
    1. 视频按语义单元切割
    2. VL 分析每个片段
    3. 截图时间点优化
    4. 失败回退
    """

    _visual_extractor_cache: Dict[str, Any] = {}
    _visual_extractor_cache_lock = threading.Lock()
    
    def __init__(self, config: Dict[str, Any] = None, *, cv_executor: Any = None):
        """
        初始化生成器
        
        Args:
            config: VL 素材生成配置（来自 module2_config.yaml）
            cv_executor: 可选的外部 Executor（通常为 python_grpc_server 的全局 CV ProcessPool），用于复用进程池与 initializer。
        """
        if config is None:
            from services.python_grpc.src.content_pipeline.infra.runtime.config_loader import load_module2_config
            full_config = load_module2_config()
            config = full_config.get("vl_material_generation", {})
        
        self.config = config
        self.enabled = config.get("enabled", False)
        self.screenshot_config = config.get("screenshot_optimization", {})
        self.fallback_config = config.get("fallback", {})

        # VL 前预处理：剔除 process 单元中的长时间 stable 片段，降低 VL 输入冗余
        # 关键取舍：
        # 1) 默认仅对 process 单元生效，避免影响 abstract/concrete 的语义完整性。
        # 2) 采用“stable 核心剔除 + 1s 边缘保留”，在节省成本与保留上下文之间平衡。
        # 3) 若预处理失败，自动回退原始片段，保证主流程可用性。
        self.pre_vl_pruning_config = config.get("pre_vl_static_pruning", {})
        self.pre_vl_pruning_enabled = bool(self.pre_vl_pruning_config.get("enabled", True))
        self.pre_vl_only_process = bool(self.pre_vl_pruning_config.get("only_process", True))
        self.pre_vl_min_unit_duration_sec = float(self.pre_vl_pruning_config.get("min_unit_duration_sec", 10.0))
        self.pre_vl_keep_edge_sec = float(self.pre_vl_pruning_config.get("keep_edge_sec", 1.0))
        # stable 片段长度必须严格大于该阈值才允许进入剔除流程
        self.pre_vl_min_stable_interval_sec = float(self.pre_vl_pruning_config.get("min_stable_interval_sec", 3.0))
        self.pre_vl_min_cut_span_sec = float(self.pre_vl_pruning_config.get("min_cut_span_sec", 0.8))
        self.pre_vl_min_keep_segment_sec = float(self.pre_vl_pruning_config.get("min_keep_segment_sec", 0.5))
        self.pre_vl_min_removed_ratio = float(self.pre_vl_pruning_config.get("min_removed_ratio", 0.10))
        self.pre_vl_context_text_max_chars = int(self.pre_vl_pruning_config.get("context_text_max_chars", 800))

        # Stable 剔除后，合并前做一次边界纠偏（语义句头 + MSE 终点 + 语流缓冲）
        self.pre_vl_boundary_refine_config = config.get("pre_vl_boundary_refine", {})
        self.pre_vl_boundary_refine_enabled = bool(self.pre_vl_boundary_refine_config.get("enabled", True))
        self.pre_vl_pause_threshold_sec = float(self.pre_vl_boundary_refine_config.get("pause_threshold_sec", 0.3))
        self.pre_vl_start_buffer_sec = float(self.pre_vl_boundary_refine_config.get("start_buffer_sec", 0.2))
        self.pre_vl_end_buffer_sec = float(self.pre_vl_boundary_refine_config.get("end_buffer_sec", 0.3))
        self.pre_vl_semantic_search_window_sec = float(self.pre_vl_boundary_refine_config.get("semantic_search_window_sec", 8.0))
        self.pre_vl_mse_scan_after_end_sec = float(self.pre_vl_boundary_refine_config.get("mse_scan_after_end_sec", 3.0))
        self.pre_vl_mse_sample_fps = float(self.pre_vl_boundary_refine_config.get("mse_sample_fps", 2.0))
        self.pre_vl_mse_min_threshold = float(self.pre_vl_boundary_refine_config.get("mse_min_threshold", 64.0))


        # AnalyzeWithVL routing: long clips go through process flow
        self.routing_config = config.get("routing", {}) if isinstance(config.get("routing", {}), dict) else {}
        self.process_duration_threshold_sec = float(self.routing_config.get("process_duration_threshold_sec", 20.0))

        # Tutorial mode: process configuration for step splitting and assets
        self.tutorial_mode_config = config.get("tutorial_mode", {}) if isinstance(config.get("tutorial_mode", {}), dict) else {}
        self.tutorial_mode_enabled = bool(self.tutorial_mode_config.get("enabled", True))
        self.tutorial_min_step_duration_sec = float(self.tutorial_mode_config.get("min_step_duration_sec", 5.0))
        self.tutorial_export_assets = bool(self.tutorial_mode_config.get("export_assets", True))
        self.tutorial_save_step_json = bool(self.tutorial_mode_config.get("save_step_json", True))
        self.tutorial_assets_root_dir = str(self.tutorial_mode_config.get("assets_root_dir", "vl_tutorial_units") or "vl_tutorial_units")
        self.tutorial_keyframe_image_ext = str(self.tutorial_mode_config.get("keyframe_image_ext", "png") or "png").lower()
        if self.tutorial_keyframe_image_ext not in {"png", "jpg", "jpeg"}:
            self.tutorial_keyframe_image_ext = "png"

        # Control whether multi-step clip requests are merged
        self.merge_multistep_clip_requests = bool(config.get("merge_multistep_clip_requests", False))
        self._subtitle_repo_cache: Dict[str, SubtitleRepository] = {}
        self._current_subtitle_output_dir: str = ""

        # 可选复用 gRPC 侧的 ProcessPool（避免额外 spawn 多套进程池）
        self._cv_executor = cv_executor
        
        # 延迟初始化分析器（避免不使用时加载）
        self._analyzer = None
        
        logger.info(f"VLMaterialGenerator 初始化完成: enabled={self.enabled}")

    def _get_cached_visual_extractor(self, video_path: str):
        """
        获取或创建按 video_path 复用的 VisualFeatureExtractor。

        目的：减少 screenshot optimization 热路径中的重复构建。
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
        """延迟初始化 VL 分析器"""
        if self._analyzer is None:
            from services.python_grpc.src.content_pipeline.phase2a.materials.vl_video_analyzer import VLVideoAnalyzer
            self._analyzer = VLVideoAnalyzer(self.config)
        return self._analyzer
    
    def is_enabled(self) -> bool:
        """检查是否启用 VL 素材生成"""
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
            # 序列化分析结果
            serialized_results = []
            for idx, result in enumerate(analysis_results):
                meta = task_metadata[idx] if idx < len(task_metadata) else {}
                
                if isinstance(result, Exception):
                    serialized_results.append({
                        "unit_id": meta.get("unit_id", f"task_{idx}"),
                        "success": False,
                        "error": str(result),
                        "metadata": meta
                    })
                else:
                    serialized_results.append({
                        "unit_id": meta.get("unit_id", f"task_{idx}"),
                        "success": result.success,
                        "error_msg": result.error_msg if hasattr(result, 'error_msg') else "",
                        "analysis_mode": getattr(result, "analysis_mode", "default"),
                        "raw_response_json": getattr(result, "raw_response_json", []) or [],
                        "clip_requests": result.clip_requests if hasattr(result, 'clip_requests') else [],
                        "screenshot_requests": result.screenshot_requests if hasattr(result, 'screenshot_requests') else [],
                        "metadata": meta
                    })
            
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
            
            logger.info(f"✅ VL 分析结果已保存到缓存: {cache_path}")
            logger.info(f"   - 总单元数: {cache_data['total_units']}")
            logger.info(f"   - 成功单元: {cache_data['successful_units']}")
            logger.info(f"   - 截图请求: {len(screenshot_requests)}")
            logger.info(f"   - 视频片段: {len(clip_requests)}")
            
        except Exception as e:
            logger.warning(f"保存VL结果缓存失败: {e}")
    
    def _load_vl_results(self, cache_path: Path) -> Optional[Dict[str, Any]]:
        """从JSON文件加载VL分析结果"""
        try:
            if not cache_path.exists():
                return None
            
            with open(cache_path, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            logger.info(f"✅ 从缓存加载VL分析结果: {cache_path}")
            logger.info(f"   - 缓存版本: {cache_data.get('version', 'unknown')}")
            logger.info(f"   - 总单元数: {cache_data.get('total_units', 0)}")
            logger.info(f"   - 成功单元: {cache_data.get('successful_units', 0)}")
            logger.info(f"   - 截图请求: {len(cache_data.get('aggregated_screenshots', []))}")
            logger.info(f"   - 视频片段: {len(cache_data.get('aggregated_clips', []))}")
            
            return cache_data
            
        except Exception as e:
            logger.warning(f"加载VL结果缓存失败: {e}")
            return None

    def _should_merge_multistep_unit(self, unit: Dict[str, Any]) -> bool:
        """
        Whether to merge multi-step clips back into one clip (legacy compatibility).
        """
        knowledge_type = (unit.get("knowledge_type", "") or "").lower()
        start_sec = float(unit.get("start_sec", 0.0))
        end_sec = float(unit.get("end_sec", 0.0))
        duration = max(0.0, end_sec - start_sec)
        return knowledge_type == "process" and duration > self.process_duration_threshold_sec and bool(unit.get("mult_steps", False))

    def _collect_segments_from_clip(self, clip: Dict[str, Any]) -> List[Dict[str, float]]:
        """
        从 clip 请求中抽取 segments；若未显式提供，则回退到 start/end。
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
        将 process>10s + mult_steps=true 的多个 clip 请求合并为单一拼接片段。
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
        knowledge_type = str(semantic_unit.get("knowledge_type", "") or "").strip().lower()
        return (
            knowledge_type == "process"
            and bool(semantic_unit.get("mult_steps", False))
            and float(duration_sec) > float(self.process_duration_threshold_sec)
        )

    def _build_tutorial_extra_prompt(self) -> str:
        """Prompt for long multi-step process units in tutorial mode."""
        return (
            "Focus on creating a 1-on-1 operational tutorial instead of generic understanding. "
            "Split the clip into complete steps. Keep explanation, execution, and result of the same step together. "
            "Remove thinking time such as mouse wandering, hesitation, and idle waiting with no new information. "
            "Each step must be at least 5 seconds; merge overly short steps with adjacent ones. "
            "For each step, output step_description and instructional_keyframe_timestamp as true instructional keyframes "
            "(prefer final state or just-before-submit moment)."
        )

    def _slugify_action_brief(self, text_value: str, max_len: int = 48) -> str:
        """方法说明：VLMaterialGenerator._slugify_action_brief 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        raw = str(text_value or "").strip().lower()
        raw = re.sub(r"[^a-z0-9]+", "_", raw)
        raw = re.sub(r"_+", "_", raw).strip("_")
        if not raw:
            return "action"
        if len(raw) > max_len:
            return raw[:max_len].rstrip("_") or "action"
        return raw

    def _build_tutorial_unit_dir(self, output_dir: str, unit_id: str) -> Optional[Path]:
        """方法说明：VLMaterialGenerator._build_tutorial_unit_dir 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
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
        """方法说明：VLMaterialGenerator._export_clip_asset_with_ffmpeg 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
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
        """方法说明：VLMaterialGenerator._export_keyframe_with_ffmpeg 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        return await export_keyframe_with_ffmpeg(
            video_path=video_path,
            timestamp_sec=timestamp_sec,
            output_path=output_path,
            logger=logger,
        )

    async def _save_tutorial_assets_for_unit(
        self,
        video_path: str,
        output_dir: str,
        unit_id: str,
        clip_requests: List[Dict[str, Any]],
        screenshot_requests: List[Dict[str, Any]],
        raw_response_json: List[Dict[str, Any]],
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

        screenshots_by_step: Dict[int, List[Dict[str, Any]]] = {}
        for ss in tutorial_screenshots:
            step_id = int(safe_float(ss.get("step_id", 0), 0.0))
            screenshots_by_step.setdefault(step_id, []).append(ss)
        for step_ss in screenshots_by_step.values():
            step_ss.sort(key=lambda x: float(x.get("timestamp_sec", 0.0)))

        ordered_clips = sorted(
            tutorial_clips,
            key=lambda c: (
                int(safe_float(c.get("step_id", 0), 0.0)),
                float(c.get("start_sec", 0.0)),
            ),
        )

        step_manifest: List[Dict[str, Any]] = []
        for idx, clip in enumerate(ordered_clips, start=1):
            step_id = int(safe_float(clip.get("step_id", idx), float(idx)))
            step_index = step_id if step_id > 0 else idx
            step_description = str(clip.get("step_description", "") or "").strip()
            action_brief = self._slugify_action_brief(
                str(clip.get("action_brief", "") or step_description),
            )
            if action_brief == "action" and step_description:
                action_brief = self._slugify_action_brief(step_description)

            clip_filename = f"{unit_id}_clip_step_{step_index:02d}_{action_brief}.mp4"
            clip_output_path = unit_dir / clip_filename

            start_sec = safe_float(clip.get("start_sec", 0.0), 0.0)
            end_sec = safe_float(clip.get("end_sec", start_sec), start_sec)
            if end_sec < start_sec:
                start_sec, end_sec = end_sec, start_sec

            clip_ok = await self._export_clip_asset_with_ffmpeg(
                video_path=video_path,
                start_sec=start_sec,
                end_sec=end_sec,
                output_path=clip_output_path,
            )

            keyframe_files: List[str] = []
            step_keyframes = screenshots_by_step.get(step_id, [])
            if not step_keyframes and step_id <= 0:
                step_keyframes = screenshots_by_step.get(idx, [])

            for key_idx, step_ss in enumerate(step_keyframes, start=1):
                key_ts = safe_float(step_ss.get("timestamp_sec", start_sec), start_sec)
                ext = "jpg" if self.tutorial_keyframe_image_ext == "jpeg" else self.tutorial_keyframe_image_ext
                if key_idx == 1:
                    key_name = f"{unit_id}_ss_step_{step_index:02d}_key_01_{action_brief}.{ext}"
                else:
                    key_name = f"{unit_id}_ss_step_{step_index:02d}_key_{key_idx:02d}_{action_brief}.{ext}"
                key_path = unit_dir / key_name
                key_ok = await self._export_keyframe_with_ffmpeg(
                    video_path=video_path,
                    timestamp_sec=key_ts,
                    output_path=key_path,
                )
                if key_ok:
                    keyframe_files.append(key_name)

            step_manifest.append({
                "step_id": step_index,
                "step_description": step_description,
                "action_brief": action_brief,
                "clip_start_sec": start_sec,
                "clip_end_sec": end_sec,
                "clip_file": clip_filename if clip_ok else "",
                "instructional_keyframes": keyframe_files,
            })

        if self.tutorial_save_step_json:
            json_payload = {
                "unit_id": unit_id,
                "schema": "tutorial_stepwise_v1",
                "raw_response": raw_response_json or [],
                "steps": step_manifest,
            }
            json_path = unit_dir / f"{unit_id}_steps.json"
            with open(json_path, "w", encoding="utf-8") as file_obj:
                json.dump(json_payload, file_obj, ensure_ascii=False, indent=2)

    
    def _normalize_intervals(self, intervals: List[Tuple[float, float]], min_duration_sec: float = 1e-6) -> List[Tuple[float, float]]:
        """
        将区间列表排序并合并重叠/相邻区间。

        为什么：稳定区间可能来自不同检测片段，先规范化可避免后续剪裁时重复处理。
        """
        return normalize_intervals(intervals, min_duration_sec=min_duration_sec)

    def _subtract_intervals(
        self,
        base_interval: Tuple[float, float],
        removed_intervals: List[Tuple[float, float]],
        min_keep_segment_sec: float,
    ) -> List[Tuple[float, float]]:
        """
        在 base 区间内扣除 removed 区间，得到保留区间。

        为什么：stable 剔除的本质是区间差集，显式实现便于调试与单元测试验证边界。
        """
        return subtract_intervals(
            base_interval,
            removed_intervals,
            min_keep_segment_sec=min_keep_segment_sec,
        )

    def _build_pruning_context_prompt(
        self,
        semantic_unit: Dict[str, Any],
        kept_segments: List[Tuple[float, float]],
        removed_segments: List[Tuple[float, float]],
    ) -> str:
        """
        构造给 VL 的上下文提示词。

        要点：
        1) 明确告知这是“裁剪片段”而非原始完整视频。
        2) 提供完整语义上下文（text/full_text）与标题（knowledge_topic）。
        3) 给出保留/剔除时间段，帮助模型理解时间跳跃，降低误判。
        """
        knowledge_topic = str(semantic_unit.get("knowledge_topic", "") or "").strip()
        full_text = str(semantic_unit.get("full_text", "") or "").strip()
        text = str(semantic_unit.get("text", "") or "").strip()
        context_text = full_text or text
        if len(context_text) > self.pre_vl_context_text_max_chars:
            context_text = context_text[: self.pre_vl_context_text_max_chars].rstrip() + "…"

        def _fmt_segments(segments: List[Tuple[float, float]]) -> str:
            if not segments:
                return "无"
            return "，".join([f"[{s:.2f}s-{e:.2f}s]" for s, e in segments])

        # 注：该补充提示会拼接到原 extra_prompt 后，尽量保持短而信息密度高，控制 token 增量。
        prompt = (
            "【VL前置上下文说明】\n"
            "当前输入并非完整语义单元视频，而是剔除长时间静态段后的拼接片段。\n"
            f"语义单元标题(knowledge_topic)：{knowledge_topic or '未知'}\n"
            f"语义单元完整文本上下文：{context_text or '无'}\n"
            f"保留片段(原始时间轴)：{_fmt_segments(kept_segments)}\n"
            f"已剔除静态片段核心区(原始时间轴)：{_fmt_segments(removed_segments)}\n"
            "请基于上述上下文理解时间跳跃，不要把片段拼接处误判为语义突变；输出仍按原规则返回。"
        )
        return prompt

    def _build_removed_intervals_from_stable(self, stable_intervals: List[Tuple[float, float]]) -> List[Tuple[float, float]]:
        """
        根据 stable 区间构建可剔除的核心区间。

        规则：
        1) stable 原始时长必须严格大于 `min_stable_interval_sec`（默认 3s）；
        2) 两侧各保留 `keep_edge_sec`，仅剔除中间核心段；
        3) 核心段时长至少 `min_cut_span_sec`。
        """
        return build_removed_intervals_from_stable(
            stable_intervals,
            min_stable_interval_sec=self.pre_vl_min_stable_interval_sec,
            keep_edge_sec=self.pre_vl_keep_edge_sec,
            min_cut_span_sec=self.pre_vl_min_cut_span_sec,
        )

    def _get_subtitle_repo_for_output_dir(self, output_dir: str) -> SubtitleRepository:
        """方法说明：VLMaterialGenerator._get_subtitle_repo_for_output_dir 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        cache_key = str(Path(output_dir).resolve())
        repository = self._subtitle_repo_cache.get(cache_key)
        if repository is not None:
            return repository

        repository = SubtitleRepository.from_output_dir(output_dir=cache_key)
        self._subtitle_repo_cache[cache_key] = repository
        return repository

    def _load_subtitles_for_output_dir(self, output_dir: str) -> List[Dict[str, Any]]:
        """方法说明：VLMaterialGenerator._load_subtitles_for_output_dir 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        repository = self._get_subtitle_repo_for_output_dir(output_dir)
        return repository.list_subtitles()

    def _build_unit_relative_subtitles(
        self,
        subtitles: List[Dict[str, Any]],
        unit_start_sec: float,
        unit_end_sec: float,
    ) -> List[Dict[str, Any]]:
        """方法说明：VLMaterialGenerator._build_unit_relative_subtitles 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
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
        使用停顿阈值切分口语句。
        为什么：ASR 常见无标点长流文本，需用停顿模拟“完整语义句”。
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
        在语义句列表中为边界锚点挑选最优句。
        为什么：优先使用“引导词/确认词”可减少截断句首句尾的概率。
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
        给单个待拼接片段计算“完整语义单元基线”。
        为什么：在稳定段剔除后，原始 kept 区间常落在句中，直接拼接会造成语义断裂。
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
        使用 MSE 检测片段结束后的物理跳变点。
        为什么：口语句可能先结束、画面后翻页；结束点应覆盖物理动作的完成。
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
        对 stable 剔除后的 kept_segments 做“语义+物理+语流”三段式边界修正。
        为什么：该阶段正处于“剔除后、合并前”的最优切入点，可最大限度避免拼接后半句话问题。
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

        output_dir = str(Path(clips_dir).parent)
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

            # 1) 语义完整性基线：优先锚定完整口语句边界
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

            # 3) 口语语流缓冲：起点 -0.2s，终点 +0.3s
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
        使用现有 CVKnowledgeValidator 复用 stable 检测链路，仅输出稳定区间。

        复用点：动态采样、ROI检测、帧级状态判定、边缘动画检测、连续状态合并。
        跳过点：动作单元分类、边界细化、相邻动作合并（通过 stable_only=True 实现）。
        """
        from services.python_grpc.src.content_pipeline.phase2a.vision.cv_knowledge_validator import CVKnowledgeValidator

        validator = CVKnowledgeValidator(clip_path)
        try:
            duration_sec = max(0.0, safe_float(getattr(validator, "duration_sec", 0.0), 0.0))
            if duration_sec <= 0.0:
                return []
            stable_islands, _, _ = validator.detect_visual_states(0.0, duration_sec, stable_only=True)
            intervals = []
            for island in stable_islands:
                intervals.append((float(island.start_sec), float(island.end_sec)))
            normalized = self._normalize_intervals(intervals)
            logger.info(f"[VL-PrePrune] unit={unit_id}: stable_islands={len(normalized)}")
            return normalized
        finally:
            try:
                validator.close()
            except Exception:
                pass

    async def _concat_segments_with_ffmpeg(
        self,
        source_clip_path: str,
        output_clip_path: str,
        segments: List[Tuple[float, float]],
    ) -> bool:
        """
        通过 ffmpeg concat demuxer 将多个区段拼接为新片段。

        说明：Java 侧最终素材提取已使用相同“分段拼接”思想。
        这里在 Python 侧前置复用该策略，避免引入新的拼接语义偏差。
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
        将“裁剪后片段相对时间”映射回“原始单元相对时间”。

        为什么：VL 在裁剪后片段上输出的时间戳，必须还原到原视频时间轴，保证后续截图/切片定位正确。
        """
        remaining = max(0.0, float(rel_value))
        for start_sec, end_sec in kept_segments:
            seg_len = max(0.0, end_sec - start_sec)
            if remaining <= seg_len + 1e-6:
                return start_sec + remaining
            remaining -= seg_len
        # 越界兜底：映射到最后一个片段尾部
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
        将“裁剪后片段的相对时间区间”映射回“原始单元相对时间轴”的分段区间。

        为什么：当 clip 区间跨过被剔除的 stable 核心段时，映射后会是多段；
        若只回写 start/end 会把中间被剔除段重新纳入，导致 Java 侧拼接结果与 VL 观测不一致。
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

    async def _prepare_pruned_clip_for_vl(
        self,
        clips_dir: str,
        semantic_unit: Dict[str, Any],
        original_clip_path: str,
        force_preprocess: bool = False,
    ) -> Dict[str, Any]:
        """
        为单个语义单元生成“VL前静态段剔除”结果。

        返回结构：
        - applied: 是否实际应用了剔除
        - clip_path_for_vl: 传给 VL 的片段路径（可能为原片段）
        - kept_segments / removed_segments: 相对原片段时间轴的区间
        - pre_context_prompt: 供 VL 追加的上下文提示
        """
        unit_id = str(semantic_unit.get("unit_id", "") or "")
        start_sec = safe_float(semantic_unit.get("start_sec", 0.0), 0.0)
        end_sec = safe_float(semantic_unit.get("end_sec", 0.0), 0.0)
        duration_sec = max(0.0, end_sec - start_sec)
        knowledge_type = str(semantic_unit.get("knowledge_type", "") or "").strip().lower()

        default_result = {
            "applied": False,
            "clip_path_for_vl": original_clip_path,
            "kept_segments": [(0.0, duration_sec)] if duration_sec > 0 else [],
            "removed_segments": [],
            "pre_context_prompt": "",
        }

        if not self.pre_vl_pruning_enabled:
            return default_result
        if self.pre_vl_only_process and knowledge_type != "process":
            return default_result
        if (not force_preprocess) and duration_sec < self.pre_vl_min_unit_duration_sec:
            return default_result

        try:
            stable_intervals = await self._detect_stable_islands_for_unit(original_clip_path, unit_id)
            if not stable_intervals:
                return default_result

            # 仅剔除满足时长阈值的 stable 核心段（两侧边缘保留）
            removed_intervals = self._build_removed_intervals_from_stable(stable_intervals)
            if not removed_intervals:
                return default_result

            kept_segments = self._subtract_intervals(
                base_interval=(0.0, duration_sec),
                removed_intervals=removed_intervals,
                min_keep_segment_sec=self.pre_vl_min_keep_segment_sec,
            )
            if not kept_segments:
                return default_result

            kept_segments = await self._refine_kept_segments_before_concat(
                clips_dir=clips_dir,
                semantic_unit=semantic_unit,
                original_clip_path=original_clip_path,
                kept_segments=kept_segments,
            )
            if not kept_segments:
                return default_result

            removed_total = sum((e - s) for s, e in removed_intervals)
            removed_ratio = removed_total / duration_sec if duration_sec > 0 else 0.0
            if removed_ratio < self.pre_vl_min_removed_ratio:
                # 剔除收益太小时不处理，避免额外编码开销和潜在语义损失
                return default_result

            pruned_dir = Path(clips_dir) / "vl_pruned_clips"
            pruned_name = f"{Path(original_clip_path).stem}_pruned.mp4"
            pruned_clip_path = str(pruned_dir / pruned_name)

            ok = await self._concat_segments_with_ffmpeg(
                source_clip_path=original_clip_path,
                output_clip_path=pruned_clip_path,
                segments=kept_segments,
            )
            if not ok:
                return default_result

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
                "clip_path_for_vl": pruned_clip_path,
                "kept_segments": kept_segments,
                "removed_segments": removed_intervals,
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
        在路由层为 process 单元执行预处理并返回“有效时长”。
        为什么：需要先基于 stable 剔除+边界修正后的真实片段长度，再做短/长分流。
        """
        route_map: Dict[str, Dict[str, Any]] = {}
        if not process_units:
            return route_map

        clips_dir = await self._split_video_by_semantic_units(video_path, process_units, output_dir)
        if not clips_dir:
            return route_map

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
            pre_prune_info = await self._prepare_pruned_clip_for_vl(
                clips_dir=clips_dir,
                semantic_unit=unit,
                original_clip_path=clip_path,
                force_preprocess=force_preprocess,
            )
            kept_segments = pre_prune_info.get("kept_segments") or []
            kept_duration = sum(max(0.0, float(e) - float(s)) for s, e in kept_segments)

            entry["preprocess_applied"] = bool(pre_prune_info.get("applied", False))
            entry["pre_prune_info"] = pre_prune_info
            if kept_duration > 0.0:
                entry["effective_duration_sec"] = kept_duration

            route_map[unit_id] = entry

        return route_map

    async def generate(
        self,
        video_path: str,
        semantic_units: List[Dict[str, Any]],
        output_dir: str = None
    ) -> VLGenerationResult:
        """
        生成素材请求 (并行化版本)
        
        Args:
            video_path: 原视频路径
            semantic_units: 语义单元列表（来自 semantic_units_phase2a.json）
            output_dir: 输出目录（用于存放切割的视频片段）
            
        Returns:
            VLGenerationResult: 生成结果
        """
        result = VLGenerationResult()
        
        if not self.enabled:
            result.success = False
            result.error_msg = "VL 素材生成未启用"
            return result
        
        # 检查是否有缓存
        cache_path = self._get_cache_path(video_path, output_dir)
        use_cache = self.config.get("use_cache", True)
        
        # VL分析结果(来自缓存或新分析)
        all_screenshot_requests = []
        all_clip_requests = []

        # 任务级 token 统计
        token_stats: Dict[str, Any] = {
            "total_units": len(semantic_units or []),
            "vl_units": 0,
            "pruned_units": 0,
            "prompt_tokens_actual": 0,
            "completion_tokens_actual": 0,
            "total_tokens_actual": 0,
            # 基线定义：若不做前置裁剪，则 pruned 单元按 "原片段 token/秒 * 原始时长" 估算
            # 非 pruned 单元基线=实际（因为路径一致）
            "prompt_tokens_baseline_est": 0,
            "completion_tokens_baseline_est": 0,
            "total_tokens_baseline_est": 0,
            "saved_tokens_est": 0,
            "saved_ratio_est": 0.0,
        }
        
        if use_cache:
            cached_data = self._load_vl_results(cache_path)
            if cached_data:
                logger.info("🚀 使用缓存的VL分析结果,跳过VL API调用")
                all_screenshot_requests = cached_data.get("aggregated_screenshots", [])
                all_clip_requests = cached_data.get("aggregated_clips", [])
                if self.merge_multistep_clip_requests:
                    all_clip_requests = self._merge_multistep_clip_requests(semantic_units, all_clip_requests)
                # ⚠️  不直接返回!继续执行CV优化
                logger.info(f"从缓存加载: screenshots={len(all_screenshot_requests)}, clips={len(all_clip_requests)}")
        
        # 如果没有缓存,执行完整的VL分析流程
        if not all_screenshot_requests and not all_clip_requests:
            try:
                # 1. 切割视频为语义单元片段
                logger.info(f"开始切割视频: {video_path}")
                clips_dir = await self._split_video_by_semantic_units(
                    video_path, 
                    semantic_units,
                    output_dir
                )
                
                if not clips_dir or not Path(clips_dir).exists():
                    raise RuntimeError("视频切割失败或输出目录不存在")
                
                # 2. 🚀 并行 VL 分析 (使用 asyncio.gather)
                logger.info(f"开始并行 VL 分析 {len(semantic_units)} 个语义单元...")
                
                # 构建分析任务列表
                analysis_tasks = []
                task_metadata = []  # 保存任务元数据以便后续匹配
                token_stats["vl_units"] = len(semantic_units)
                
                for su in semantic_units:
                    unit_id = su.get("unit_id", "")
                    start_sec = float(su.get("start_sec", 0))
                    end_sec = float(su.get("end_sec", 0))
                    duration = max(0.0, end_sec - start_sec)
                    extra_prompt = None
                    analysis_mode = "default"
                    if self._is_tutorial_process_unit(su, duration):
                        analysis_mode = "tutorial_stepwise"
                        extra_prompt = self._build_tutorial_extra_prompt()
                    
                    # 查找对应的视频片段
                    clip_path = self._find_clip_for_unit(clips_dir, unit_id, start_sec, end_sec)
                    
                    if not clip_path:
                        logger.warning(f"未找到语义单元 {unit_id} 的视频片段，跳过")
                        continue

                    # VL 前预处理：对 process 单元剔除 stable 核心区间并拼接新片段
                    pre_prune_info = await self._prepare_pruned_clip_for_vl(
                        clips_dir=clips_dir,
                        semantic_unit=su,
                        original_clip_path=clip_path,
                    )
                    clip_path_for_vl = pre_prune_info.get("clip_path_for_vl", clip_path)

                    if pre_prune_info.get("applied"):
                        token_stats["pruned_units"] += 1

                    pre_context_prompt = str(pre_prune_info.get("pre_context_prompt", "") or "").strip()
                    if pre_context_prompt:
                        if extra_prompt:
                            extra_prompt = extra_prompt + "\n\n" + pre_context_prompt
                        else:
                            extra_prompt = pre_context_prompt
                    
                    # 创建异步分析任务
                    task = self.analyzer.analyze_clip(
                        clip_path=clip_path_for_vl,
                        semantic_unit_start_sec=start_sec,
                        semantic_unit_id=unit_id,
                        extra_prompt=extra_prompt,
                        analysis_mode=analysis_mode
                    )
                    analysis_tasks.append(task)
                    task_metadata.append({
                        "unit_id": unit_id,
                        "start_sec": start_sec,
                        "end_sec": end_sec,
                        "unit_duration": duration,
                        "clip_path": clip_path,
                        "vl_clip_path": clip_path_for_vl,
                        "pre_prune": pre_prune_info,
                        "analysis_mode": analysis_mode,
                    })
                
                # 🚀 并行执行所有 VL 分析任务
                logger.info(f"🚀 启动 {len(analysis_tasks)} 个并行 VL 分析任务...")
                analysis_results = await asyncio.gather(*analysis_tasks, return_exceptions=True)
                logger.info(f"✅ 并行 VL 分析完成,共 {len(analysis_results)} 个结果")
                
                # 收集所有成功的分析结果
                for idx, analysis_result in enumerate(analysis_results):
                    meta = task_metadata[idx] if idx < len(task_metadata) else {}
                    unit_id = meta.get("unit_id", f"task_{idx}")
                    
                    # 处理异常情况
                    if isinstance(analysis_result, Exception):
                        logger.warning(f"语义单元 {unit_id} VL 分析异常: {analysis_result}")
                        continue
                    
                    if not analysis_result.success:
                        logger.warning(f"语义单元 {unit_id} VL 分析失败: {analysis_result.error_msg}")
                        continue

                    # 汇总 token 使用与基线估算
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

                    # 估算基线：对 pruned 单元做秒级线性回推（第一性近似），非 pruned 单元基线=实际
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

                    # 若使用了预处理裁剪片段，需要将 VL 相对时间映射回原始时间轴
                    if pre_prune_info.get("applied") and kept_segments:
                        for clip_item in analysis_result.clip_requests:
                            # clip 请求本身就是绝对时间：先转回单元相对时间，再映射回原始单元相对时间，再加单元起点
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
                            # 同时给出 segments，复用 Java 侧拼接逻辑，且仅保留当前 clip 对应的有效子段
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

                    # 统一兜底：无论是否预裁剪，都将时间戳约束在当前语义单元区间内。
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
                        await self._save_tutorial_assets_for_unit(
                            video_path=video_path,
                            output_dir=output_dir or str(Path(video_path).parent),
                            unit_id=unit_id,
                            clip_requests=analysis_result.clip_requests,
                            screenshot_requests=analysis_result.screenshot_requests,
                            raw_response_json=getattr(analysis_result, "raw_response_json", []) or [],
                        )

                    
                    # 收集结果 (暂不优化截图时间点，后续批量处理)
                    all_clip_requests.extend(analysis_result.clip_requests)
                    all_screenshot_requests.extend(analysis_result.screenshot_requests)
                
                if self.merge_multistep_clip_requests:
                    all_clip_requests = self._merge_multistep_clip_requests(semantic_units, all_clip_requests)
                logger.info(f"VL 分析汇总: clips={len(all_clip_requests)}, screenshots={len(all_screenshot_requests)}")

                token_stats["saved_tokens_est"] = max(
                    0,
                    int(token_stats["total_tokens_baseline_est"] - token_stats["total_tokens_actual"]),
                )
                if token_stats["total_tokens_baseline_est"] > 0:
                    token_stats["saved_ratio_est"] = float(token_stats["saved_tokens_est"]) / float(token_stats["total_tokens_baseline_est"])
                else:
                    token_stats["saved_ratio_est"] = 0.0

                logger.info(
                    "[VL-Token] units=%s, pruned=%s, actual_total=%s, baseline_est=%s, saved_est=%s, saved_ratio=%.2f%%",
                    token_stats.get("vl_units", 0),
                    token_stats.get("pruned_units", 0),
                    token_stats.get("total_tokens_actual", 0),
                    token_stats.get("total_tokens_baseline_est", 0),
                    token_stats.get("saved_tokens_est", 0),
                    float(token_stats.get("saved_ratio_est", 0.0)) * 100.0,
                )
                
                # 保存VL分析原始结果(CV优化前)
                if self.config.get("save_cache", True):
                    self._save_vl_results(
                        cache_path=cache_path,
                        analysis_results=analysis_results,
                        task_metadata=task_metadata,
                        screenshot_requests=all_screenshot_requests,
                        clip_requests=all_clip_requests
                    )
                
            except Exception as e:
                logger.error(f"VL 分析失败: {e}")
                result.success = False
                result.error_msg = str(e)
                return result
        
        # 3. 🚀 批量 CV 优化截图时间点 (无论是否使用缓存,都要执行!)
        try:
            if self.screenshot_config.get("enabled", True) and all_screenshot_requests:
                logger.info(f"开始批量 CV 优化 {len(all_screenshot_requests)} 个截图请求...")
                optimized_screenshots = await self._optimize_screenshots_parallel(
                    video_path=video_path,
                    screenshot_requests=all_screenshot_requests
                )
                all_screenshot_requests = optimized_screenshots
            
            # 汇总最终结果
            result.clip_requests = all_clip_requests
            result.screenshot_requests = all_screenshot_requests
            result.token_stats = token_stats
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
        """??? `flow_ops`??????????"""
        return await split_video_by_semantic_units(self, video_path, semantic_units, output_dir=output_dir)
    
    def _find_clip_for_unit(
        self,
        clips_dir: str,
        unit_id: str,
        start_sec: float,
        end_sec: float
    ) -> Optional[str]:
        """??? `flow_ops`??????????"""
        return find_clip_for_unit(self, clips_dir, unit_id, start_sec, end_sec)
    
    async def _optimize_screenshot_timestamps(
        self,
        video_path: str,
        screenshot_requests: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        优化截图时间点
        
        对每个建议的截图时间戳，在 ±1s 范围内使用 screenshot_selector 查找最佳帧
        
        Args:
            video_path: 原视频路径
            screenshot_requests: 截图请求列表
            
        Returns:
            List[Dict]: 优化后的截图请求
        """
        if not screenshot_requests:
            return []
        
        time_window = self.screenshot_config.get("time_window_seconds", 1.0)
        optimized = []
        
        try:
            # 使用 screenshot_selector 的逻辑
            from services.python_grpc.src.content_pipeline.phase2a.vision.screenshot_selector import ScreenshotSelector
            
            selector = ScreenshotSelector.create_lightweight()
            
            for req in screenshot_requests:
                original_ts = req.get("timestamp_sec", 0)
                
                # 计算搜索窗口
                search_start = max(0, original_ts - time_window)
                search_end = original_ts + time_window
                
                try:
                    # 调用截图选择逻辑
                    best_screenshots = selector.select_screenshots_for_range_sync(
                        video_path=video_path,
                        start_sec=search_start,
                        end_sec=search_end,
                        coarse_fps=2.0,
                        fine_fps=10.0
                    )
                    
                    if best_screenshots:
                        # 使用最佳时间戳
                        best_ts = best_screenshots[0].get("timestamp_sec", original_ts)
                        req["timestamp_sec"] = best_ts
                        req["_optimized"] = True
                        req["_original_timestamp"] = original_ts
                        logger.debug(
                            f"截图时间优化: {original_ts:.2f}s -> {best_ts:.2f}s "
                            f"(score={best_screenshots[0].get('score', 0):.2f})"
                        )
                    
                except Exception as e:
                    logger.warning(f"截图优化失败: {e}, 使用原始时间戳")
                
                optimized.append(req)
            
        except ImportError:
            logger.warning("screenshot_selector 不可用，跳过截图优化")
            return screenshot_requests
        except Exception as e:
            logger.warning(f"截图优化失败: {e}")
            return screenshot_requests
        
        return optimized
    
    async def _optimize_screenshots_parallel(
        self,
        video_path: str,
        screenshot_requests: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        并行优化截图时间点 (使用 cv_worker 进程池 + 共享内存)
        
        支持两种模式:
        - 流式模式 (streaming_pipeline=true): 边预读边提交,IO/Compute 重叠
        - 批量模式 (streaming_pipeline=false): 批量预读后提交,保持向后兼容
        
        Args:
            video_path: 原视频路径
            screenshot_requests: 截图请求列表
            
        Returns:
            List[Dict]: 优化后的截图请求
        """
        if not screenshot_requests:
            return []
        
        # 检查是否启用流式处理 (默认启用)
        use_streaming = self.screenshot_config.get("streaming_pipeline", True)
        
        if use_streaming:
            logger.info("🚀 使用流式处理模式 (streaming_pipeline=true)")
            return await self._optimize_screenshots_streaming_pipeline(
                video_path,
                screenshot_requests
            )
        else:
            logger.info("🚀 使用批量处理模式 (streaming_pipeline=false)")
            return await self._optimize_screenshots_batch_mode(
                video_path,
                screenshot_requests
            )
    
    async def _optimize_screenshots_batch_mode(
        self,
        video_path: str,
        screenshot_requests: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """??? `flow_ops`??????????"""
        return await optimize_screenshots_batch_mode(self, video_path, screenshot_requests)
    
    def _is_truthy_env(self, name: str, default: str = "0") -> bool:
        """方法说明：VLMaterialGenerator._is_truthy_env 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        value = os.getenv(name, default).strip().lower()
        return value in {"1", "true", "yes", "y", "on"}

    def _resolve_max_workers(self, request_count: int) -> int:
        """
        解析 max_workers 配置。

        优先级：
        1) 若注入了外部 executor，优先以其 max_workers 为准（保证日志/背压与实际一致）。
        2) 否则读取配置 `screenshot_optimization.max_workers`：'auto' 或整数。

        设计原则：Windows spawn 成本高，默认做安全上限保护（cap=6）。
        """
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
        time_window: float,
        max_span_seconds: float,
        max_requests: int,
    ) -> List[Dict[str, Any]]:
        """
        将截图请求按时间聚类为多个 chunk。

        目的：
        - 每个 chunk 用一次 Union 预读覆盖区间，避免对短视频反复 seek/read；
        - 同时把单次 Union 区间限制在 max_span_seconds 内，防止一次预读过大；
        - 为 double-buffer overlap 预留“chunk 级 SHM 生命周期”边界，避免跨 chunk 淘汰 unlink。

        返回：chunk 列表，每个 chunk 包含：union_start/union_end/windows。
        windows 内结构用于构建 worker 任务参数。
        """
        return build_screenshot_prefetch_chunks(
            screenshot_requests=screenshot_requests,
            time_window=time_window,
            max_span_seconds=max_span_seconds,
            max_requests=max_requests,
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
        同步预读 + 写入 chunk 专属 SharedMemory Registry。

        注意：此函数会被 asyncio.to_thread 调用，以实现主线程可 drain 已完成的 worker 结果，
        形成 IO/Compute 重叠。
        """
        # 背景：短窗口（<5s）走 OpenCV Random Access（多次 cap.set）会非常慢，导致 worker 长时间空闲。
        # 这里改为“单次 seek + 顺序 read 扫描”，只在命中的 target frame 上 resize + 写入 SHM。
        # 这样 prefetch 成本大幅下降，CPU 更能花在 worker 计算上。
        import cv2

        video_path = getattr(extractor, "video_path", None) or getattr(extractor, "video", None)
        if not video_path:
            return None, {}, 0.0, 0.0

        t0 = time.perf_counter()
        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened():
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

            step = max(1, int(sample_rate))
            target_indices = set(range(start_frame, end_frame + 1, step))
            target_indices.add(end_frame)

            # 该 chunk 内不允许淘汰：max_frames 覆盖本次候选帧数
            registry = registry_cls(max_frames=max(10, len(target_indices) + 10))

            # Seek once, then sequential scan
            cap.set(cv2.CAP_PROP_POS_FRAMES, start_frame)
            current_idx = start_frame

            ts_to_shm_ref: Dict[float, Any] = {}
            register_ms = 0.0

            while current_idx <= end_frame:
                ret, frame = cap.read()
                if not ret or frame is None:
                    break

                if current_idx in target_indices:
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
        """方法说明：VLMaterialGenerator._build_task_params_from_ts_map 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        return build_task_params_from_ts_map(
            windows=windows,
            ts_to_shm_ref=ts_to_shm_ref,
            fps=fps,
        )

    async def _maybe_warmup_pool(self, *, loop: asyncio.AbstractEventLoop, executor: Any, worker_count: int) -> None:
        """方法说明：VLMaterialGenerator._maybe_warmup_pool 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
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
        将 worker 返回结果写回到 request（原地更新）。

        约束：不改变 screenshot_requests 的顺序；仅更新 timestamp_sec 与诊断字段。
        """
        if isinstance(result, Exception):
            logger.warning(f"CV Worker 异常: {unit_id}: {result}")
            return

        if isinstance(result, dict) and "selected_timestamp" in result:
            req["timestamp_sec"] = result["selected_timestamp"]
            req["_optimized"] = True
            req["_original_timestamp"] = original_ts
            req["_cv_quality_score"] = result.get("quality_score", 0)
            logger.debug(
                f"CV 优化: {unit_id}: {original_ts:.2f}s → {result['selected_timestamp']:.2f}s "
                f"(score={result.get('quality_score', 0):.3f})"
            )
    
    async def _optimize_screenshots_streaming_pipeline(
        self,
        video_path: str,
        screenshot_requests: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """??? `flow_ops`??????????"""
        return await optimize_screenshots_streaming_pipeline(self, video_path, screenshot_requests)
    
    def _should_fallback(self, error: Exception) -> bool:
        """
        检查是否应该回退到原有流程
        
        Args:
            error: 发生的异常
            
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
