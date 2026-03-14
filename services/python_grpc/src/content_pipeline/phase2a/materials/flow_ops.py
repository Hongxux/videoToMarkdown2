"""VL 语义单元切片与定位辅助逻辑。"""

from __future__ import annotations

import asyncio
import functools
import json
import logging
import os
import re
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List, Optional

from services.python_grpc.src.common.utils.numbers import safe_float
from services.python_grpc.src.content_pipeline.common.utils.path_utils import find_repo_root

logger = logging.getLogger(__name__)


def _sanitize_stream_unit_folder_name(text: str) -> str:
    cleaned = re.sub(r"[^0-9A-Za-z._-]+", "_", str(text or "").strip())
    cleaned = cleaned.strip("._")
    return cleaned or "UNKNOWN_UNIT"


def _resolve_screenshot_time_window(generator) -> tuple[float, float]:
    resolver = getattr(generator, "_resolve_screenshot_time_window", None)
    if callable(resolver):
        try:
            before, after = resolver()
            return max(0.0, float(before)), max(0.0, float(after))
        except Exception:
            pass
    window = safe_float(getattr(generator, "screenshot_config", {}).get("time_window_seconds", 1.0), 1.0)
    window = max(0.0, float(window))
    return window, window


def _resolve_screenshot_static_island_threshold_ms(generator) -> float:
    resolver = getattr(generator, "_resolve_screenshot_static_island_threshold_ms", None)
    if callable(resolver):
        try:
            threshold = float(resolver())
            return max(0.0, min(5000.0, threshold))
        except Exception:
            pass
    return 200.0


def _resolve_prefetch_governor(generator) -> Dict[str, Any]:
    config = getattr(generator, "screenshot_config", {}) or {}

    def _safe_int(name: str, default: int, minimum: int = 1) -> int:
        try:
            return max(minimum, int(config.get(name, default)))
        except (TypeError, ValueError):
            return max(minimum, int(default))

    def _safe_float(name: str, default: float, minimum: float = 0.0) -> float:
        try:
            return max(minimum, float(config.get(name, default)))
        except (TypeError, ValueError):
            return max(minimum, float(default))

    return {
        "monitor_enabled": bool(config.get("prefetch_monitor_enabled", True)),
        "rss_warn_mb": _safe_float("prefetch_monitor_warn_rss_mb", 2048.0, 0.0),
        "prefetch_frames_hard_cap": _safe_int("prefetch_hard_max_frames_per_chunk", 160, 8),
        "prefetch_requests_hard_cap": _safe_int("prefetch_hard_max_requests_per_chunk", 256, 1),
        "prefetch_span_hard_cap": _safe_float("prefetch_hard_max_span_seconds", 8.0, 0.5),
        "max_inflight_hard_cap": _safe_int("max_inflight_hard_cap", 2, 1),
        "overlap_buffers_hard_cap": _safe_int("streaming_overlap_buffers_hard_cap", 1, 1),
    }


def _should_use_threaded_cv_executor(generator, screenshot_requests: List[Dict[str, Any]]) -> bool:
    config = getattr(generator, "screenshot_config", {}) or {}
    override = str(
        config.get("cv_route_executor_mode", os.getenv("MODULE2_CV_ROUTE_EXECUTOR_MODE", "")) or ""
    ).strip().lower()
    if override in {"thread", "threads"}:
        return True
    if override in {"process", "processes"}:
        return False
    if os.name != "nt":
        return False
    for request in screenshot_requests or []:
        if not isinstance(request, dict):
            continue
        profile = str(request.get("_cv_prefetch_profile", "default") or "default").strip().lower()
        if profile == "concrete_forward":
            return True
    return False


def _resolve_cv_route_executor(generator, screenshot_requests: List[Dict[str, Any]], max_workers: int, init_cv_worker):
    from services.python_grpc.src.common.utils.process_pool import create_spawn_process_pool
    if _should_use_threaded_cv_executor(generator, screenshot_requests):
        try:
            init_cv_worker()
        except Exception as error:
            logger.warning("[CV ROUTE] thread-mode init failed: %s", error)
        return ThreadPoolExecutor(max_workers=max_workers), True, "thread"
    executor = getattr(generator, "_cv_executor", None)
    if executor is None:
        return create_spawn_process_pool(max_workers=max_workers, initializer=init_cv_worker), True, "process_local"
    return executor, False, "process_shared"


def _collect_prefetch_runtime_snapshot() -> Dict[str, float]:
    try:
        import psutil

        process = psutil.Process()
        return {
            "rss_mb": float(process.memory_info().rss) / (1024.0 * 1024.0),
            "cpu_percent": float(psutil.cpu_percent(interval=None)),
        }
    except Exception:
        return {}


def _emit_prefetch_monitor(
    *,
    governor: Dict[str, Any],
    mode: str,
    chunk_index: int,
    total_chunks: int,
    profile: str,
    request_count: int,
    span_sec: float,
    sampled_frames: int,
    inflight: int,
    inflight_cap: int,
    shm_frames: Optional[int] = None,
    shm_mb: Optional[float] = None,
    shm_budget_mb: Optional[float] = None,
) -> None:
    if not bool(governor.get("monitor_enabled", False)):
        return

    snapshot = _collect_prefetch_runtime_snapshot()
    rss_mb = snapshot.get("rss_mb")
    cpu_percent = snapshot.get("cpu_percent")
    logger.info(
        "[PrefetchMonitor] mode=%s chunk=%s/%s profile=%s reqs=%s span=%.2fs sampled_frames=%s inflight=%s/%s rss_mb=%s cpu=%s shm_frames=%s shm_mb=%s shm_budget_mb=%s",
        mode,
        chunk_index,
        total_chunks,
        profile,
        request_count,
        span_sec,
        sampled_frames,
        inflight,
        inflight_cap,
        f"{rss_mb:.1f}" if isinstance(rss_mb, (int, float)) else "na",
        f"{cpu_percent:.1f}" if isinstance(cpu_percent, (int, float)) else "na",
        str(int(shm_frames)) if isinstance(shm_frames, int) else "na",
        f"{shm_mb:.2f}" if isinstance(shm_mb, (int, float)) else "na",
        f"{shm_budget_mb:.2f}" if isinstance(shm_budget_mb, (int, float)) else "na",
    )

    rss_warn_mb = float(governor.get("rss_warn_mb", 0.0) or 0.0)
    if rss_warn_mb > 0.0 and isinstance(rss_mb, (int, float)) and rss_mb >= rss_warn_mb:
        logger.warning(
            "[PrefetchMonitor] RSS above warning threshold: mode=%s chunk=%s/%s rss_mb=%.1f threshold_mb=%.1f",
            mode,
            chunk_index,
            total_chunks,
            rss_mb,
            rss_warn_mb,
        )

    prefetch_frames_hard_cap = int(governor.get("prefetch_frames_hard_cap", 0) or 0)
    if prefetch_frames_hard_cap > 0 and sampled_frames >= prefetch_frames_hard_cap:
        logger.warning(
            "[PrefetchMonitor] sampled_frames reached hard cap: mode=%s chunk=%s/%s sampled_frames=%s hard_cap=%s",
            mode,
            chunk_index,
            total_chunks,
            sampled_frames,
            prefetch_frames_hard_cap,
        )

async def split_video_by_semantic_units(
    generator,
    video_path: str,
    semantic_units: List[Dict[str, Any]],
    output_dir: str = None
) -> Optional[str]:
    """
    调用 `split_video_by_semantic_units.py` 生成 VL 语义单元切片。

    约束：
    1) 优先复用 `_stream_units/<unit_id>/` 下已存在的 VL canonical clip。
    2) 只对缺失的语义单元补切，避免重复 ffmpeg 与重复 I/O。
    3) 让后续 assets / analyzer / pre-prune 统一消费同一套 VL 切片布局。
    """
    if output_dir is None:
        output_dir = str(Path(video_path).parent)

    clips_dir = Path(output_dir) / "semantic_unit_clips_vl"
    intermediates_dir = Path(output_dir) / "intermediates"
    intermediates_dir.mkdir(parents=True, exist_ok=True)
    semantic_units_json = intermediates_dir / "semantic_units_vl_subset.json"

    valid_units: List[Dict[str, Any]] = []
    seen_unit_ids = set()
    for unit in semantic_units or []:
        unit_id = str(unit.get("unit_id", "") or "").strip()
        start_sec = safe_float(unit.get("start_sec", 0.0), 0.0)
        end_sec = safe_float(unit.get("end_sec", 0.0), 0.0)
        if not unit_id or end_sec <= start_sec or unit_id in seen_unit_ids:
            continue
        seen_unit_ids.add(unit_id)
        valid_units.append(unit)

    if not valid_units:
        raise ValueError("没有可用于 VL 切片的有效语义单元")

    project_root = find_repo_root(__file__)
    script_path = project_root / "tools" / "split_video_by_semantic_units.py"
    if not script_path.exists():
        raise FileNotFoundError(f"未找到语义单元切片脚本: {script_path}")

    def _find_existing_unit_clip(unit_id: str, start_sec: float, end_sec: float) -> Optional[str]:
        unit_dir = clips_dir / "_stream_units" / _sanitize_stream_unit_folder_name(unit_id)

        def _is_valid_file(path_value: Any) -> Optional[str]:
            if not path_value:
                return None
            candidate = Path(str(path_value))
            if not candidate.is_absolute():
                candidate = (clips_dir / candidate).resolve()
            if candidate.exists() and candidate.is_file():
                return str(candidate)
            return None

        def _iter_manifest_matches(manifest_file: Path) -> List[Dict[str, Any]]:
            if not manifest_file.exists():
                return []
            try:
                with open(manifest_file, "r", encoding="utf-8") as manifest_fp:
                    manifest = json.load(manifest_fp)
            except Exception:
                return []
            matches = [
                item for item in (manifest.get("items", []) or [])
                if isinstance(item, dict)
                and str(item.get("unit_id", "")) == str(unit_id)
                and str(item.get("status", "")) == "success"
            ]
            matches.sort(
                key=lambda item: abs(safe_float(item.get("start_sec", start_sec), start_sec) - start_sec)
                + abs(safe_float(item.get("end_sec", end_sec), end_sec) - end_sec)
            )
            return matches

        for manifest_file in (unit_dir / "manifest.json", clips_dir / "manifest.json"):
            for item in _iter_manifest_matches(manifest_file):
                manifest_out = _is_valid_file(item.get("out_path"))
                if manifest_out:
                    return manifest_out

        unit_pattern = re.compile(rf"(?:^|_){re.escape(unit_id)}(?:_|$)", re.IGNORECASE)
        for clip_file in sorted(unit_dir.glob("*.mp4")):
            if unit_pattern.search(clip_file.stem):
                return str(clip_file)

        for clip_file in sorted(clips_dir.glob("*.mp4")):
            if unit_pattern.search(clip_file.stem):
                return str(clip_file)
        return None

    def _collect_missing_units() -> List[str]:
        missing: List[str] = []
        for su in valid_units:
            unit_id = str(su.get("unit_id", "") or "").strip()
            start_sec = safe_float(su.get("start_sec", 0.0), 0.0)
            end_sec = safe_float(su.get("end_sec", 0.0), 0.0)
            if not unit_id:
                continue
            if not _find_existing_unit_clip(unit_id, start_sec, end_sec):
                missing.append(unit_id)
        return missing

    units_to_split: List[Dict[str, Any]] = list(valid_units)
    missing_units: List[str] = []
    manifest_path = clips_dir / "manifest.json"
    if manifest_path.exists():
        try:
            with open(manifest_path, "r", encoding="utf-8") as manifest_fp:
                manifest = json.load(manifest_fp)
            summary = manifest.get("summary", {})
            if summary.get("success", 0) > 0 and summary.get("failed", 0) == 0:
                missing_units = _collect_missing_units()
                if not missing_units:
                    logger.info(f"复用已有 VL 语义切片目录: {clips_dir}")
                    return str(clips_dir)
                logger.info("manifest 校验发现缺失切片单元: missing=%s", len(missing_units))
        except Exception:
            pass

    if clips_dir.exists() and not missing_units:
        try:
            missing_units = _collect_missing_units()
            if not missing_units:
                logger.info(f"复用已有切片目录（文件探测通过）: {clips_dir}")
                return str(clips_dir)
            logger.warning(
                "检测到缺失语义单元，准备补切 %s/%s (e.g., %s)",
                len(missing_units),
                len(valid_units),
                missing_units[:3],
            )
        except Exception as error:
            logger.warning(f"探测已有语义切片失败: {error}")

    if missing_units and len(missing_units) < len(valid_units):
        missing_set = {str(unit_id) for unit_id in missing_units if str(unit_id)}
        units_to_split = [
            su for su in valid_units
            if str(su.get("unit_id", "") or "").strip() in missing_set
        ]
        logger.info(
            "仅对缺失语义单元执行补切: missing=%s/%s",
            len(units_to_split),
            len(valid_units),
        )

    with open(semantic_units_json, "w", encoding="utf-8") as manifest_fp:
        json.dump(units_to_split, manifest_fp, ensure_ascii=False, indent=2)

    cmd = [
        "python",
        str(script_path),
        "--video", video_path,
        "--semantic-units", str(semantic_units_json),
        "--out-dir", str(clips_dir),
        "--stream-unit-layout",
        "--overwrite",
    ]

    split_pre_cut_config = {}
    if isinstance(getattr(generator, "config", None), dict):
        raw_cfg = generator.config.get("semantic_split_pre_cut", {})
        if isinstance(raw_cfg, dict):
            split_pre_cut_config = raw_cfg

    pre_cut_enabled = bool(split_pre_cut_config.get("enabled", True))
    if pre_cut_enabled:
        large_segment_threshold_sec = max(
            0.0,
            safe_float(split_pre_cut_config.get("large_segment_threshold_sec", 120.0), 120.0),
        )
        large_segment_scale_height = max(
            0,
            int(safe_float(split_pre_cut_config.get("downscale_height", 480.0), 480.0)),
        )
        large_segment_video_bitrate = (
            str(split_pre_cut_config.get("video_bitrate", "500k") or "").strip() or "500k"
        )
        apply_low_res_to_all_units = bool(split_pre_cut_config.get("apply_to_all_units", False))
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
        if apply_low_res_to_all_units:
            cmd.append("--apply-low-res-to-all-units")

    logger.info(f"执行语义单元切片命令: {' '.join(cmd)}")

    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _stdout, stderr = await process.communicate()

        if process.returncode != 0:
            error_msg = stderr.decode("utf-8", errors="ignore")
            raise RuntimeError(f"语义单元切片失败 (code={process.returncode}): {error_msg[:500]}")

        logger.info(f"语义单元切片完成: {clips_dir}")
        return str(clips_dir)
    except Exception as error:
        logger.error(f"执行语义单元切片异常: {error}")
        raise


def find_clip_for_unit(
    generator,
    clips_dir: str,
    unit_id: str,
    start_sec: float,
    end_sec: float
) -> Optional[str]:
    """
    按 unit_id 与时间范围为当前语义单元定位 clip。

    查找顺序：
    1) 顶层 `manifest.json`
    2) `_stream_units/<unit_id>/manifest.json`
    3) `_stream_units/<unit_id>/*.mp4`
    4) 顶层目录下的 `*.mp4` 回退匹配
    """
    _ = generator
    clips_path = Path(clips_dir)
    unit_dir = clips_path / "_stream_units" / _sanitize_stream_unit_folder_name(unit_id)

    def _is_valid_file(path_value: Any) -> Optional[str]:
        if not path_value:
            return None
        candidate = Path(str(path_value))
        if not candidate.is_absolute():
            candidate = (clips_path / candidate).resolve()
        if candidate.exists() and candidate.is_file():
            return str(candidate)
        return None

    def _filename_matches_unit(clip_name: str, target_unit_id: str) -> bool:
        pattern = re.compile(rf"(?:^|_){re.escape(target_unit_id)}(?:_|$)", re.IGNORECASE)
        return bool(pattern.search(Path(clip_name).stem))

    def _close_to_expected(item: Dict[str, Any]) -> float:
        s = safe_float(item.get("start_sec", start_sec), start_sec)
        e = safe_float(item.get("end_sec", end_sec), end_sec)
        return abs(s - start_sec) + abs(e - end_sec)

    def _load_manifest_items(manifest_file: Path) -> List[Dict[str, Any]]:
        if not manifest_file.exists():
            return []
        try:
            with open(manifest_file, "r", encoding="utf-8") as manifest_fp:
                manifest = json.load(manifest_fp)
        except Exception:
            return []
        items = [
            item for item in (manifest.get("items", []) or [])
            if isinstance(item, dict)
            and str(item.get("unit_id", "")) == str(unit_id)
            and str(item.get("status", "")) == "success"
        ]
        items.sort(key=_close_to_expected)
        return items

    for manifest_file in (unit_dir / "manifest.json", clips_path / "manifest.json"):
        for item in _load_manifest_items(manifest_file):
            manifest_out = _is_valid_file(item.get("out_path"))
            if manifest_out:
                return manifest_out

    matched_files: List[Path] = []
    for search_dir in (unit_dir, clips_path):
        for clip_file in search_dir.glob("*.mp4"):
            if _filename_matches_unit(clip_file.name, str(unit_id)):
                matched_files.append(clip_file)
        if matched_files:
            break

    if matched_files:
        if len(matched_files) == 1:
            return str(matched_files[0])

        time_pattern = f"{start_sec:.2f}-{end_sec:.2f}"
        for clip_file in matched_files:
            if time_pattern in clip_file.name:
                return str(clip_file)

        matched_files.sort(key=lambda f: f.name)
        return str(matched_files[0])

    time_pattern = f"{start_sec:.2f}-{end_sec:.2f}"
    for search_dir in (unit_dir, clips_path):
        for clip_file in search_dir.glob("*.mp4"):
            if time_pattern in clip_file.name:
                return str(clip_file)

    return None
async def optimize_screenshots_batch_mode(
    generator,
    video_path: str,
    screenshot_requests: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    批量模式: 批量预读所有帧后再提交任务 (原实现,保持向后兼容)
    
    架构:
    1. 主进程预读所有帧并写入 SharedMemory
    2. 批量提交所有任务到 ProcessPool
    3. Worker 零拷贝读取帧并执行 CV 分析
    
    Args:
        video_path: 原视频路径
        screenshot_requests: 截图请求列表
        
    Returns:
        List[Dict]: 优化后的截图请求
    """
    if not screenshot_requests:
        return []
    
    time_window_before, time_window_after = _resolve_screenshot_time_window(generator)
    static_island_threshold_ms = _resolve_screenshot_static_island_threshold_ms(generator)
    
    try:
        from services.python_grpc.src.content_pipeline.phase2a.vision.visual_feature_extractor import SharedFrameRegistry
        import sys
        import gc
        
        # 尝试导入 cv_worker (位于项目根目录)
        project_root = find_repo_root(__file__)
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))
        
        from services.python_grpc.src.vision_validation.worker import run_screenshot_selection_task, init_cv_worker

        logger.info(f"🚀 [Batch Mode] 初始化并行 CV 优化: {len(screenshot_requests)} 个请求")

        # 初始化帧提取器（主进程负责预读与写入 SHM）
        extractor = generator._get_cached_visual_extractor(video_path)

        # 配置参数
        governor = _resolve_prefetch_governor(generator)
        max_workers = generator._resolve_max_workers(request_count=len(screenshot_requests))
        max_inflight_multiplier = int(generator.screenshot_config.get("max_inflight_multiplier", 2))
        max_inflight = max(1, max_workers * max_inflight_multiplier)
        max_inflight = min(max_inflight, int(governor["max_inflight_hard_cap"]))
        sample_rate = max(1, int(generator.screenshot_config.get("prefetch_sample_rate", 2)))
        target_height = max(0, int(generator.screenshot_config.get("prefetch_target_height", 360)))
        max_prefetch_frames = max(8, int(generator.screenshot_config.get("prefetch_max_frames_per_chunk", 240)))
        max_prefetch_frames = min(max_prefetch_frames, int(governor["prefetch_frames_hard_cap"]))
        chunk_max_span_sec = max(0.5, float(generator.screenshot_config.get("prefetch_union_max_span_seconds", 10.0)))
        chunk_max_span_sec = min(chunk_max_span_sec, float(governor["prefetch_span_hard_cap"]))
        chunk_max_requests = max(1, int(generator.screenshot_config.get("prefetch_chunk_max_requests", 1000)))
        chunk_max_requests = min(chunk_max_requests, int(governor["prefetch_requests_hard_cap"]))

        chunks = generator._build_screenshot_prefetch_chunks(
            screenshot_requests=screenshot_requests,
            max_span_seconds=chunk_max_span_sec,
            max_requests=chunk_max_requests,
            time_window_before=time_window_before,
            time_window_after=time_window_after,
        )

        logger.info(
            f"📦 [Batch Mode] Config: workers={max_workers}, inflight={max_inflight}, "
            f"chunks={len(chunks)}, max_span={chunk_max_span_sec:.2f}s, max_req/chunk={chunk_max_requests}, "
            f"max_prefetch_frames={max_prefetch_frames}"
        )

        executor, created_executor, executor_kind = _resolve_cv_route_executor(
            generator,
            screenshot_requests,
            max_workers,
            init_cv_worker,
        )

        try:
            loop = asyncio.get_running_loop()

            # 可选 Warmup：诊断是否真的分发到多个 Worker
            if executor_kind.startswith("process"):
                await generator._maybe_warmup_pool(loop=loop, executor=executor, worker_count=max_workers)
            logger.info("[CV ROUTE] executor_kind=%s, workers=%s", executor_kind, max_workers)

            submitted_tasks = 0
            completed_tasks = 0

            for chunk_id, chunk in enumerate(chunks):
                chunk_t0 = time.perf_counter()

                chunk_profile = str(chunk.get("prefetch_profile", "default") or "default")
                chunk_sample_rate = max(1, int(chunk.get("prefetch_sample_rate", sample_rate) or sample_rate))
                chunk_target_height = max(0, int(chunk.get("prefetch_target_height", target_height) or target_height))
                registry, ts_to_shm_ref, prefetch_ms, register_ms = await asyncio.to_thread(
                    generator._prefetch_union_frames_to_registry_sync,
                    extractor,
                    SharedFrameRegistry,
                    chunk["union_start"],
                    chunk["union_end"],
                    chunk_sample_rate,
                    chunk_target_height,
                )
                registry_snapshot = getattr(registry, "snapshot", lambda: {})() if registry is not None else {}

                _emit_prefetch_monitor(
                    governor=governor,
                    mode="batch",
                    chunk_index=chunk_id + 1,
                    total_chunks=len(chunks),
                    profile=chunk_profile,
                    request_count=len(chunk["windows"]),
                    span_sec=float(chunk["union_end"] - chunk["union_start"]),
                    sampled_frames=len(ts_to_shm_ref),
                    inflight=0,
                    inflight_cap=max_inflight,
                    shm_frames=int(registry_snapshot.get("frame_count", 0) or 0),
                    shm_mb=float(registry_snapshot.get("current_bytes", 0) or 0) / (1024.0 * 1024.0),
                    shm_budget_mb=float(registry_snapshot.get("max_bytes", 0) or 0) / (1024.0 * 1024.0),
                )

                try:
                    if not ts_to_shm_ref:
                        logger.warning(
                            f"⚠️ [Batch Mode] Chunk {chunk_id + 1}/{len(chunks)} 预读失败，跳过该 chunk 的 CV 优化"
                        )
                        continue

                    task_params = generator._build_task_params_from_ts_map(
                        windows=chunk["windows"],
                        ts_to_shm_ref=ts_to_shm_ref,
                        fps=extractor.fps,
                    )

                    # 提交该 chunk 的所有任务并等待（chunk 级 barrier）
                    futures = []
                    meta = []
                    for p in task_params:
                        if p.get("skip"):
                            continue
                        req = p["req"]
                        original_ts = req.get("timestamp_sec", 0)
                        future = loop.run_in_executor(
                            executor,
                            functools.partial(
                                run_screenshot_selection_task,
                                video_path=video_path,
                                unit_id=p["unit_id"],
                                island_index=p["island_index"],
                                expanded_start=p["expanded_start"],
                                expanded_end=p["expanded_end"],
                                shm_frames=p["shm_frames"],
                                fps=p["fps"],
                                static_island_min_ms=static_island_threshold_ms,
                            ),
                        )
                        futures.append(future)
                        meta.append((req, original_ts, p["unit_id"]))

                    submitted_tasks += len(futures)

                    if futures:
                        results = await asyncio.gather(*futures, return_exceptions=True)
                        for (req, original_ts, unit_id), r in zip(meta, results):
                            completed_tasks += 1
                            generator._apply_selection_result(req=req, original_ts=original_ts, unit_id=unit_id, result=r)

                    gc.collect()

                    chunk_total_ms = (time.perf_counter() - chunk_t0) * 1000.0
                    logger.info(
                        f"✅ [Batch Mode] Chunk {chunk_id + 1}/{len(chunks)} done: "
                        f"reqs={len(chunk['windows'])}, span={chunk['union_end'] - chunk['union_start']:.2f}s, "
                        f"prefetch={prefetch_ms:.1f}ms, register={register_ms:.1f}ms, "
                        f"submitted={len(futures)}, profile={chunk_profile}, total={chunk_total_ms:.1f}ms"
                    )
                finally:
                    # cleanup chunk SHM：确保异常情况下也不会泄漏
                    if registry is not None:
                        try:
                            cleanup_snapshot = getattr(registry, "snapshot", lambda: {})()
                            logger.info(
                                "[SHM ChunkCleanup] mode=batch chunk=%s/%s frames=%s shm_mb=%.2f budget_mb=%.2f",
                                chunk_id + 1,
                                len(chunks),
                                int(cleanup_snapshot.get("frame_count", 0) or 0),
                                float(cleanup_snapshot.get("current_bytes", 0) or 0) / (1024.0 * 1024.0),
                                float(cleanup_snapshot.get("max_bytes", 0) or 0) / (1024.0 * 1024.0),
                            )
                            registry.cleanup()
                        except Exception as e:
                            logger.debug(f"[Batch Mode] Chunk registry cleanup failed: {e}")

            logger.info(
                f"✅ [Batch Mode] Completed: submitted_tasks={submitted_tasks}, completed_tasks={completed_tasks}"
            )
            return screenshot_requests
        finally:
            if created_executor:
                executor.shutdown(wait=True)
        
    except ImportError as e:
        error_msg = f"[CV PARALLEL] cv_worker import failed: {e} (sys.path={sys.path[:3]}...)"
        logger.warning(error_msg)
        print(f"\n{'='*80}", flush=True)
        print(f"[CV PARALLEL] {error_msg}", flush=True)
        print(f"{'='*80}\n", flush=True)
        import traceback
        traceback.print_exc()
        return await generator._optimize_screenshot_timestamps(video_path, screenshot_requests)
    except Exception as e:
        error_msg = f"[CV PARALLEL] optimize failed: {e}"
        logger.error(error_msg)
        print(f"\n{'='*80}", flush=True)
        print(f"[CV PARALLEL] {error_msg}", flush=True)
        print(f"{'='*80}\n", flush=True)
        import traceback
        logger.error(traceback.format_exc())
        traceback.print_exc()
        return await generator._optimize_screenshot_timestamps(video_path, screenshot_requests)


async def optimize_screenshots_streaming_pipeline(
    generator,
    video_path: str,
    screenshot_requests: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    流式处理流水线: 边预读边提交,实现 IO/Compute 重叠
    
    架构 (参考 upgrade-log.md 第119-130行):
    1. 逐个预读帧并写入 SharedMemory
    2. 立即提交任务到 ProcessPool  
    3. 维护全局 pending in-flight 队列
    4. 背压节流: pending 达到上限时 drain_completed
    5. 持续流式返回结果
    
    收益:
    - IO/Compute 重叠 (预读和计算并行)
    - Worker 尽早开始工作 (不等所有预读完成)
    - 降低内存峰值 (不需一次性加载所有帧)
    - 流式输出结果
    
    Args:
        video_path: 原视频路径
        screenshot_requests: 截图请求列表
        
    Returns:
        List[Dict]: 优化后的截图请求
    """
    if not screenshot_requests:
        return []
    
    time_window_before, time_window_after = _resolve_screenshot_time_window(generator)
    static_island_threshold_ms = _resolve_screenshot_static_island_threshold_ms(generator)
    
    try:
        from services.python_grpc.src.content_pipeline.phase2a.vision.visual_feature_extractor import SharedFrameRegistry
        import sys
        import gc
        
        # 导入 cv_worker
        project_root = find_repo_root(__file__)
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))
        
        from services.python_grpc.src.vision_validation.worker import run_screenshot_selection_task, init_cv_worker
        
        logger.info(f"🚀 [Streaming Pipeline] 启动流式处理: {len(screenshot_requests)} 个请求")
        
        # 初始化帧提取器
        extractor = generator._get_cached_visual_extractor(video_path)

        # 配置参数
        governor = _resolve_prefetch_governor(generator)
        max_workers = generator._resolve_max_workers(request_count=len(screenshot_requests))
        max_inflight_multiplier = int(generator.screenshot_config.get("max_inflight_multiplier", 2))
        max_inflight = max(1, max_workers * max_inflight_multiplier)
        max_inflight = min(max_inflight, int(governor["max_inflight_hard_cap"]))
        overlap_buffers = int(generator.screenshot_config.get("streaming_overlap_buffers", 2))
        overlap_buffers = max(1, overlap_buffers)
        overlap_buffers = min(overlap_buffers, int(governor["overlap_buffers_hard_cap"]))

        sample_rate = max(1, int(generator.screenshot_config.get("prefetch_sample_rate", 2)))
        target_height = max(0, int(generator.screenshot_config.get("prefetch_target_height", 360)))
        max_prefetch_frames = max(8, int(generator.screenshot_config.get("prefetch_max_frames_per_chunk", 240)))
        max_prefetch_frames = min(max_prefetch_frames, int(governor["prefetch_frames_hard_cap"]))
        chunk_max_span_sec = max(0.5, float(generator.screenshot_config.get("prefetch_union_max_span_seconds", 10.0)))
        chunk_max_span_sec = min(chunk_max_span_sec, float(governor["prefetch_span_hard_cap"]))
        chunk_max_requests = max(1, int(generator.screenshot_config.get("prefetch_chunk_max_requests", 1000)))
        chunk_max_requests = min(chunk_max_requests, int(governor["prefetch_requests_hard_cap"]))

        chunks = generator._build_screenshot_prefetch_chunks(
            screenshot_requests=screenshot_requests,
            max_span_seconds=chunk_max_span_sec,
            max_requests=chunk_max_requests,
            time_window_before=time_window_before,
            time_window_after=time_window_after,
        )

        logger.info(
            f"📦 [Streaming Pipeline] Config: workers={max_workers}, inflight={max_inflight}, "
            f"overlap_buffers={overlap_buffers}, chunks={len(chunks)}, "
            f"max_span={chunk_max_span_sec:.2f}s, max_req/chunk={chunk_max_requests}, "
            f"max_prefetch_frames={max_prefetch_frames}"
        )

        executor, created_executor, executor_kind = _resolve_cv_route_executor(
            generator,
            screenshot_requests,
            max_workers,
            init_cv_worker,
        )

        try:
            loop = asyncio.get_running_loop()

            # 可选 Warmup：诊断是否真的分发到多个 Worker
            if executor_kind.startswith("process"):
                await generator._maybe_warmup_pool(loop=loop, executor=executor, worker_count=max_workers)
            logger.info("[CV ROUTE] executor_kind=%s, workers=%s", executor_kind, max_workers)

            pending: set = set()
            futures_meta: Dict[asyncio.Future, Dict[str, Any]] = {}
            active_chunks: deque = deque()  # list[dict]

            submitted_tasks = 0
            completed_tasks = 0

            async def cleanup_finished_chunks():
                # 清理已完成的 chunk（必须等待该 chunk 的任务全部完成）
                for _ in range(len(active_chunks)):
                    ctx = active_chunks[0]
                    if ctx.get("closed") and ctx.get("pending", 0) <= 0:
                        active_chunks.popleft()
                        try:
                            cleanup_snapshot = getattr(ctx.get("registry"), "snapshot", lambda: {})()
                            logger.info(
                                "[SHM ChunkCleanup] mode=streaming chunk=%s/%s reason=completed frames=%s shm_mb=%.2f budget_mb=%.2f",
                                int(ctx.get("chunk_id", 0)) + 1,
                                len(chunks),
                                int(cleanup_snapshot.get("frame_count", 0) or 0),
                                float(cleanup_snapshot.get("current_bytes", 0) or 0) / (1024.0 * 1024.0),
                                float(cleanup_snapshot.get("max_bytes", 0) or 0) / (1024.0 * 1024.0),
                            )
                            ctx["registry"].cleanup()
                        except Exception as e:
                            logger.debug(f"[Streaming Pipeline] Chunk registry cleanup failed: {e}")
                    else:
                        active_chunks.rotate(-1)

            async def drain_first_completed():
                nonlocal pending, completed_tasks
                if not pending:
                    return

                done, pending_new = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                pending = set(pending_new)

                for fut in done:
                    completed_tasks += 1
                    meta = futures_meta.pop(fut, None) or {}
                    req = meta.get("req")
                    if req is None:
                        continue
                    chunk_ctx = meta.get("chunk_ctx")
                    original_ts = meta.get("original_ts", 0)
                    unit_id = meta.get("unit_id", "unknown")
                    started_at = meta.get("started_at", None)

                    try:
                        result = fut.result()
                    except Exception as e:
                        result = e
                    generator._apply_selection_result(req=req, original_ts=original_ts, unit_id=unit_id, result=result)

                    if chunk_ctx is not None:
                        chunk_ctx["pending"] -= 1
                        chunk_ctx["completed"] += 1
                        if started_at is not None:
                            chunk_ctx["task_ms_sum"] += (time.perf_counter() - started_at) * 1000.0

                await cleanup_finished_chunks()

            for chunk_id, chunk in enumerate(chunks):
                # overlap buffer 控制：最多保留 overlap_buffers 个 chunk 的 SHM
                while len(active_chunks) >= overlap_buffers:
                    if not pending:
                        ctx = active_chunks.popleft()
                        try:
                            cleanup_snapshot = getattr(ctx.get("registry"), "snapshot", lambda: {})()
                            logger.info(
                                "[SHM ChunkCleanup] mode=streaming chunk=%s/%s reason=overlap_evict frames=%s shm_mb=%.2f budget_mb=%.2f",
                                int(ctx.get("chunk_id", 0)) + 1,
                                len(chunks),
                                int(cleanup_snapshot.get("frame_count", 0) or 0),
                                float(cleanup_snapshot.get("current_bytes", 0) or 0) / (1024.0 * 1024.0),
                                float(cleanup_snapshot.get("max_bytes", 0) or 0) / (1024.0 * 1024.0),
                            )
                            ctx["registry"].cleanup()
                        except Exception:
                            pass
                        continue
                    await drain_first_completed()

                chunk_t0 = time.perf_counter()
                chunk_profile = str(chunk.get("prefetch_profile", "default") or "default")
                chunk_sample_rate = max(1, int(chunk.get("prefetch_sample_rate", sample_rate) or sample_rate))
                chunk_target_height = max(0, int(chunk.get("prefetch_target_height", target_height) or target_height))
                try:
                    registry, ts_to_shm_ref, prefetch_ms, register_ms = await asyncio.to_thread(
                        generator._prefetch_union_frames_to_registry_sync,
                        extractor,
                        SharedFrameRegistry,
                        chunk["union_start"],
                        chunk["union_end"],
                        chunk_sample_rate,
                        chunk_target_height,
                    )
                except Exception as error:
                    if bool(getattr(generator, "screenshot_iframe_fallback_on_oom", False)) and bool(
                        getattr(generator, "_is_memory_pressure_error", lambda _error: False)(error)
                    ):
                        logger.warning(
                            "[Streaming Pipeline] Chunk %s/%s prefetch hit memory pressure, fallback to iframe-only mode: %s",
                            chunk_id + 1,
                            len(chunks),
                            error,
                        )
                        iframe_fallback = getattr(generator, "_optimize_screenshots_by_iframe_only", None)
                        if callable(iframe_fallback):
                            remapped_requests = await iframe_fallback(
                                video_path=video_path,
                                screenshot_requests=[window.get("req", {}) for window in chunk.get("windows", [])],
                                reason="streaming_prefetch_memory_pressure",
                            )
                            for window, updated_request in zip(chunk.get("windows", []), remapped_requests):
                                if not isinstance(window, dict):
                                    continue
                                original_request = window.get("req")
                                if isinstance(original_request, dict) and isinstance(updated_request, dict):
                                    original_request.clear()
                                    original_request.update(updated_request)
                                    window["req"] = original_request
                                else:
                                    window["req"] = updated_request
                        continue
                    raise
                registry_snapshot = getattr(registry, "snapshot", lambda: {})() if registry is not None else {}

                _emit_prefetch_monitor(
                    governor=governor,
                    mode="streaming",
                    chunk_index=chunk_id + 1,
                    total_chunks=len(chunks),
                    profile=chunk_profile,
                    request_count=len(chunk["windows"]),
                    span_sec=float(chunk["union_end"] - chunk["union_start"]),
                    sampled_frames=len(ts_to_shm_ref),
                    inflight=len(pending),
                    inflight_cap=max_inflight,
                    shm_frames=int(registry_snapshot.get("frame_count", 0) or 0),
                    shm_mb=float(registry_snapshot.get("current_bytes", 0) or 0) / (1024.0 * 1024.0),
                    shm_budget_mb=float(registry_snapshot.get("max_bytes", 0) or 0) / (1024.0 * 1024.0),
                )

                if not ts_to_shm_ref:
                    logger.warning(
                        f"⚠️ [Streaming Pipeline] Chunk {chunk_id + 1}/{len(chunks)} 预读失败，跳过该 chunk 的 CV 优化"
                    )
                    continue

                task_params = generator._build_task_params_from_ts_map(
                    windows=chunk["windows"],
                    ts_to_shm_ref=ts_to_shm_ref,
                    fps=extractor.fps,
                )

                chunk_ctx = {
                    "chunk_id": chunk_id,
                    "registry": registry,
                    "submitted": 0,
                    "completed": 0,
                    "pending": 0,
                    "closed": False,
                    "prefetch_ms": prefetch_ms,
                    "register_ms": register_ms,
                    "task_ms_sum": 0.0,
                }
                active_chunks.append(chunk_ctx)

                submitted_in_chunk = 0
                for p in task_params:
                    if p.get("skip"):
                        continue
                    while len(pending) >= max_inflight:
                        await drain_first_completed()

                    req = p["req"]
                    original_ts = req.get("timestamp_sec", 0)
                    started_at = time.perf_counter()
                    fut = loop.run_in_executor(
                        executor,
                        functools.partial(
                            run_screenshot_selection_task,
                            video_path=video_path,
                            unit_id=p["unit_id"],
                            island_index=p["island_index"],
                            expanded_start=p["expanded_start"],
                            expanded_end=p["expanded_end"],
                            shm_frames=p["shm_frames"],
                            fps=p["fps"],
                            static_island_min_ms=static_island_threshold_ms,
                        ),
                    )
                    pending.add(fut)
                    futures_meta[fut] = {
                        "req": req,
                        "original_ts": original_ts,
                        "unit_id": p["unit_id"],
                        "chunk_ctx": chunk_ctx,
                        "started_at": started_at,
                    }
                    submitted_tasks += 1
                    submitted_in_chunk += 1
                    chunk_ctx["submitted"] += 1
                    chunk_ctx["pending"] += 1

                chunk_ctx["closed"] = True

                chunk_total_ms = (time.perf_counter() - chunk_t0) * 1000.0
                logger.info(
                    f"📌 [Streaming Pipeline] Feed chunk {chunk_id + 1}/{len(chunks)}: "
                    f"reqs={len(chunk['windows'])}, span={chunk['union_end'] - chunk['union_start']:.2f}s, "
                    f"prefetch={prefetch_ms:.1f}ms, register={register_ms:.1f}ms, "
                    f"submitted={submitted_in_chunk}, inflight={len(pending)}, profile={chunk_profile}, total={chunk_total_ms:.1f}ms"
                )

                gc.collect()

            while pending:
                await drain_first_completed()

            # 防御性 cleanup
            while active_chunks:
                ctx = active_chunks.popleft()
                try:
                    cleanup_snapshot = getattr(ctx.get("registry"), "snapshot", lambda: {})()
                    logger.info(
                        "[SHM ChunkCleanup] mode=streaming chunk=%s/%s reason=final_drain frames=%s shm_mb=%.2f budget_mb=%.2f",
                        int(ctx.get("chunk_id", 0)) + 1,
                        len(chunks),
                        int(cleanup_snapshot.get("frame_count", 0) or 0),
                        float(cleanup_snapshot.get("current_bytes", 0) or 0) / (1024.0 * 1024.0),
                        float(cleanup_snapshot.get("max_bytes", 0) or 0) / (1024.0 * 1024.0),
                    )
                    ctx["registry"].cleanup()
                except Exception:
                    pass

            logger.info(
                f"✅ [Streaming Pipeline] Completed: submitted_tasks={submitted_tasks}, completed_tasks={completed_tasks}"
            )
            return screenshot_requests
        finally:
            # 异常路径兜底：尽量 drain + cleanup，避免 SHM 泄漏（允许 best-effort 超时）
            try:
                if "pending" in locals() and pending:
                    await asyncio.wait(pending, timeout=5.0)
                if "active_chunks" in locals() and active_chunks:
                    while active_chunks:
                        ctx = active_chunks.popleft()
                        try:
                            cleanup_snapshot = getattr(ctx.get("registry"), "snapshot", lambda: {})()
                            logger.info(
                                "[SHM ChunkCleanup] mode=streaming chunk=%s/%s reason=finally_cleanup frames=%s shm_mb=%.2f budget_mb=%.2f",
                                int(ctx.get("chunk_id", 0)) + 1,
                                len(chunks) if "chunks" in locals() else 0,
                                int(cleanup_snapshot.get("frame_count", 0) or 0),
                                float(cleanup_snapshot.get("current_bytes", 0) or 0) / (1024.0 * 1024.0),
                                float(cleanup_snapshot.get("max_bytes", 0) or 0) / (1024.0 * 1024.0),
                            )
                            ctx["registry"].cleanup()
                        except Exception:
                            pass
            except Exception:
                pass
            if created_executor:
                executor.shutdown(wait=True)
        
    except ImportError as e:
        error_msg = f"[CV STREAMING] cv_worker import failed: {e}"
        logger.warning(error_msg)
        print(f"\n{'='*80}", flush=True)
        print(f"[CV STREAMING] {error_msg}", flush=True)
        print(f"{'='*80}\n", flush=True)
        import traceback
        traceback.print_exc()
        return await generator._optimize_screenshot_timestamps(video_path, screenshot_requests)
    except Exception as e:
        error_msg = f"[CV STREAMING] pipeline failed: {e}"
        logger.error(error_msg)
        print(f"\n{'='*80}", flush=True)
        print(f"[CV STREAMING] {error_msg}", flush=True)
        print(f"{'='*80}\n", flush=True)
        import traceback
        logger.error(traceback.format_exc())
        traceback.print_exc()
        if bool(getattr(generator, "screenshot_iframe_fallback_on_oom", False)) and bool(
            getattr(generator, "_is_memory_pressure_error", lambda _error: False)(e)
        ):
            iframe_fallback = getattr(generator, "_optimize_screenshots_by_iframe_only", None)
            if callable(iframe_fallback):
                logger.warning("[CV STREAMING] memory pressure fallback to iframe-only mode for all requests")
                return await iframe_fallback(
                    video_path=video_path,
                    screenshot_requests=screenshot_requests,
                    reason="streaming_pipeline_memory_pressure",
                )
        return await generator._optimize_screenshot_timestamps(video_path, screenshot_requests)
