"""VL ???????????????"""

from __future__ import annotations

import asyncio
import functools
import json
import logging
import re
import time
from collections import deque
from pathlib import Path
from typing import Any, Dict, List, Optional

from services.python_grpc.src.common.utils.numbers import safe_float
from services.python_grpc.src.content_pipeline.common.utils.path_utils import find_repo_root

logger = logging.getLogger(__name__)

async def split_video_by_semantic_units(
    generator,
    video_path: str,
    semantic_units: List[Dict[str, Any]],
    output_dir: str = None
) -> Optional[str]:
    """
    调用 split_video_by_semantic_units.py 切割视频
    
    Args:
        video_path: 原视频路径
        semantic_units: 语义单元列表
        output_dir: 输出目录
        
    Returns:
        str: 切割后的视频片段目录路径
    """
    # 确定输出目录
    if output_dir is None:
        output_dir = str(Path(video_path).parent)

    # 仅为 VL 目标单元切割，避免复用全量 semantic_units_phase2a.json 导致无效切片。
    clips_dir = Path(output_dir) / "semantic_unit_clips_vl"
    intermediates_dir = Path(output_dir) / "intermediates"
    intermediates_dir.mkdir(parents=True, exist_ok=True)
    semantic_units_json = intermediates_dir / "semantic_units_vl_subset.json"

    # 去重并过滤非法单元，确保后续切割列表与实际 VL 分析候选一致。
    valid_units: List[Dict[str, Any]] = []
    seen_unit_ids = set()
    for unit in semantic_units or []:
        unit_id = str(unit.get("unit_id", "") or "").strip()
        start_sec = safe_float(unit.get("start_sec", 0.0), 0.0)
        end_sec = safe_float(unit.get("end_sec", 0.0), 0.0)
        if not unit_id:
            continue
        if end_sec <= start_sec:
            continue
        if unit_id in seen_unit_ids:
            continue
        seen_unit_ids.add(unit_id)
        valid_units.append(unit)

    if not valid_units:
        raise ValueError("没有可用于 VL 分析的有效语义单元，跳过视频切割")

    # 查找脚本路径
    project_root = find_repo_root(__file__)
    script_path = project_root / "tools" / "split_video_by_semantic_units.py"
    
    if not script_path.exists():
        raise FileNotFoundError(f"视频切割脚本不存在: {script_path}")
    
    def _collect_missing_units(existing_names: List[str]) -> List[str]:
        missing_units: List[str] = []
        for su in valid_units:
            unit_id = str(su.get("unit_id", "") or "").strip()
            if not unit_id:
                continue
            unit_pattern = re.compile(rf"(?:^|_){re.escape(unit_id)}(?:_|$)", re.IGNORECASE)
            if not any(unit_pattern.search(name) for name in existing_names):
                missing_units.append(unit_id)
        return missing_units

    # 检查是否已经切割过（避免重复切割）
    units_to_split: List[Dict[str, Any]] = list(valid_units)
    missing_units: List[str] = []
    manifest_path = clips_dir / "manifest.json"
    if manifest_path.exists():
        try:
            with open(manifest_path, "r", encoding="utf-8") as f:
                manifest = json.load(f)
            # 检查是否切割成功
            summary = manifest.get("summary", {})
            if summary.get("success", 0) > 0 and summary.get("failed", 0) == 0:
                # 只要当前所需 unit 都存在即可复用，不要求目录中仅包含当前子集。
                existing_clips = list(clips_dir.glob("*.mp4"))
                existing_names = [f.name for f in existing_clips]
                missing_units = _collect_missing_units(existing_names)
                if not missing_units:
                    logger.info(f"复用已存在的 VL 目标视频片段: {clips_dir}")
                    return str(clips_dir)
                logger.info(f"manifest 存在但仍需补切片: missing={len(missing_units)}")
            
        except Exception:
            pass
    
    # 2. 备用检查：直接检查是否存在对应的 .mp4 文件
    # 如果 manifest 丢失但文件都在，也可以复用
    if clips_dir.exists() and not missing_units:
        try:
            existing_clips = list(clips_dir.glob("*.mp4"))
            if len(existing_clips) > 0:
                # 检查是否所有 unit_id 都有对应的片段
                existing_names = [f.name for f in existing_clips]
                missing_units = _collect_missing_units(existing_names)
                
                if not missing_units:
                    logger.info(f"复用已存在的视频片段 (文件完整性检查通过): {clips_dir}")
                    return str(clips_dir)
                else:
                    logger.warning(f"无法复用视频片段，缺失: {len(missing_units)}/{len(valid_units)} (e.g., {missing_units[:3]})")
        except Exception as e:
            logger.warning(f"文件完整性检查出错: {e}")

    if missing_units and len(missing_units) < len(valid_units):
        missing_set = {str(unit_id) for unit_id in missing_units if str(unit_id)}
        units_to_split = [
            su for su in valid_units
            if str(su.get("unit_id", "") or "").strip() in missing_set
        ]
        logger.info(
            "检测到增量切片场景，仅切割缺失单元: missing=%s/%s",
            len(units_to_split),
            len(valid_units),
        )

    # 每次按“待分析子集”重写 JSON，确保切割范围与当前 VL 候选严格一致。
    with open(semantic_units_json, "w", encoding="utf-8") as f:
        json.dump(units_to_split, f, ensure_ascii=False, indent=2)
    
    # 执行切割命令
    cmd = [
        "python",
        str(script_path),
        "--video", video_path,
        "--semantic-units", str(semantic_units_json),
        "--out-dir", str(clips_dir),
        "--overwrite"  # 覆盖已存在的文件
    ]

    # 超长片段预切降码率/分辨率：仅用于 VL 预分析输入，最终 assets 仍从原视频按时间戳截取。
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
    
    logger.info(f"执行视频切割: {' '.join(cmd)}")
    
    try:
        # 使用 asyncio 异步执行
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()
        
        if process.returncode != 0:
            error_msg = stderr.decode("utf-8", errors="ignore")
            raise RuntimeError(f"视频切割失败 (code={process.returncode}): {error_msg[:500]}")
        
        logger.info(f"视频切割完成: {clips_dir}")
        return str(clips_dir)
        
    except Exception as e:
        logger.error(f"视频切割执行失败: {e}")
        raise


def find_clip_for_unit(
    generator,
    clips_dir: str,
    unit_id: str,
    start_sec: float,
    end_sec: float
) -> Optional[str]:
    """
    查找语义单元对应的视频片段
    
    Args:
        clips_dir: 视频片段目录
        unit_id: 语义单元 ID
        start_sec: 起始时间
        end_sec: 结束时间
        
    Returns:
        str: 视频片段路径，未找到则返回 None
    """
    clips_path = Path(clips_dir)

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
        # 文件名典型格式：001_SU001_topic_0.00-10.00.mp4
        # 这里要求 unit_id token 边界匹配，避免 SU01 误匹配 SU010。
        pattern = re.compile(rf"(?:^|_){re.escape(target_unit_id)}(?:_|$)", re.IGNORECASE)
        return bool(pattern.search(Path(clip_name).stem))

    def _close_to_expected(item: Dict[str, Any]) -> float:
        s = safe_float(item.get("start_sec", start_sec), start_sec)
        e = safe_float(item.get("end_sec", end_sec), end_sec)
        return abs(s - start_sec) + abs(e - end_sec)

    # 1) 优先通过 manifest 做精确匹配（最可靠）
    manifest_path = clips_path / "manifest.json"
    if manifest_path.exists():
        try:
            with open(manifest_path, "r", encoding="utf-8") as f:
                manifest = json.load(f)
            items = [
                item for item in (manifest.get("items", []) or [])
                if isinstance(item, dict)
                and str(item.get("unit_id", "")) == str(unit_id)
                and str(item.get("status", "")) == "success"
            ]
            items.sort(key=_close_to_expected)
            for item in items:
                manifest_out = _is_valid_file(item.get("out_path"))
                if manifest_out:
                    return manifest_out
        except Exception:
            pass

    # 2) 文件名精确 token 匹配
    matched_files: List[Path] = []
    for clip_file in clips_path.glob("*.mp4"):
        if _filename_matches_unit(clip_file.name, str(unit_id)):
            matched_files.append(clip_file)

    if matched_files:
        if len(matched_files) == 1:
            return str(matched_files[0])

        time_pattern = f"{start_sec:.2f}-{end_sec:.2f}"
        for clip_file in matched_files:
            if time_pattern in clip_file.name:
                return str(clip_file)

        matched_files.sort(key=lambda f: f.name)
        return str(matched_files[0])

    # 3) 时间范围回退匹配
    time_pattern = f"{start_sec:.2f}-{end_sec:.2f}"
    for clip_file in clips_path.glob("*.mp4"):
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
    
    time_window = generator.screenshot_config.get("time_window_seconds", 1.0)
    
    try:
        from concurrent.futures import ProcessPoolExecutor
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
        max_workers = generator._resolve_max_workers(request_count=len(screenshot_requests))
        max_inflight_multiplier = int(generator.screenshot_config.get("max_inflight_multiplier", 2))
        max_inflight = max(1, max_workers * max_inflight_multiplier)
        sample_rate = int(generator.screenshot_config.get("prefetch_sample_rate", 2))
        target_height = int(generator.screenshot_config.get("prefetch_target_height", 360))
        max_prefetch_frames = int(generator.screenshot_config.get("prefetch_max_frames_per_chunk", 240))
        chunk_max_span_sec = float(generator.screenshot_config.get("prefetch_union_max_span_seconds", 10.0))
        chunk_max_requests = int(generator.screenshot_config.get("prefetch_chunk_max_requests", 1000))

        chunks = generator._build_screenshot_prefetch_chunks(
            screenshot_requests=screenshot_requests,
            time_window=time_window,
            max_span_seconds=chunk_max_span_sec,
            max_requests=chunk_max_requests,
        )

        logger.info(
            f"📦 [Batch Mode] Config: workers={max_workers}, inflight={max_inflight}, "
            f"chunks={len(chunks)}, max_span={chunk_max_span_sec:.2f}s, max_req/chunk={chunk_max_requests}, "
            f"max_prefetch_frames={max_prefetch_frames}"
        )

        executor = generator._cv_executor
        created_executor = False
        if executor is None:
            executor = ProcessPoolExecutor(max_workers=max_workers, initializer=init_cv_worker)
            created_executor = True

        try:
            loop = asyncio.get_running_loop()

            # 可选 Warmup：诊断是否真的分发到多个 Worker
            await generator._maybe_warmup_pool(loop=loop, executor=executor, worker_count=max_workers)

            submitted_tasks = 0
            completed_tasks = 0

            for chunk_id, chunk in enumerate(chunks):
                chunk_t0 = time.perf_counter()

                registry, ts_to_shm_ref, prefetch_ms, register_ms = await asyncio.to_thread(
                    generator._prefetch_union_frames_to_registry_sync,
                    extractor,
                    SharedFrameRegistry,
                    chunk["union_start"],
                    chunk["union_end"],
                    sample_rate,
                    target_height,
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
                        f"submitted={len(futures)}, total={chunk_total_ms:.1f}ms"
                    )
                finally:
                    # cleanup chunk SHM：确保异常情况下也不会泄漏
                    if registry is not None:
                        try:
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
    
    time_window = generator.screenshot_config.get("time_window_seconds", 1.0)
    
    try:
        from concurrent.futures import ProcessPoolExecutor
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
        max_workers = generator._resolve_max_workers(request_count=len(screenshot_requests))
        max_inflight_multiplier = int(generator.screenshot_config.get("max_inflight_multiplier", 2))
        max_inflight = max(1, max_workers * max_inflight_multiplier)
        overlap_buffers = int(generator.screenshot_config.get("streaming_overlap_buffers", 2))
        overlap_buffers = max(1, overlap_buffers)

        sample_rate = int(generator.screenshot_config.get("prefetch_sample_rate", 2))
        target_height = int(generator.screenshot_config.get("prefetch_target_height", 360))
        max_prefetch_frames = int(generator.screenshot_config.get("prefetch_max_frames_per_chunk", 240))
        chunk_max_span_sec = float(generator.screenshot_config.get("prefetch_union_max_span_seconds", 10.0))
        chunk_max_requests = int(generator.screenshot_config.get("prefetch_chunk_max_requests", 1000))

        chunks = generator._build_screenshot_prefetch_chunks(
            screenshot_requests=screenshot_requests,
            time_window=time_window,
            max_span_seconds=chunk_max_span_sec,
            max_requests=chunk_max_requests,
        )

        logger.info(
            f"📦 [Streaming Pipeline] Config: workers={max_workers}, inflight={max_inflight}, "
            f"overlap_buffers={overlap_buffers}, chunks={len(chunks)}, "
            f"max_span={chunk_max_span_sec:.2f}s, max_req/chunk={chunk_max_requests}, "
            f"max_prefetch_frames={max_prefetch_frames}"
        )

        executor = generator._cv_executor
        created_executor = False
        if executor is None:
            executor = ProcessPoolExecutor(max_workers=max_workers, initializer=init_cv_worker)
            created_executor = True

        try:
            loop = asyncio.get_running_loop()

            # 可选 Warmup：诊断是否真的分发到多个 Worker
            await generator._maybe_warmup_pool(loop=loop, executor=executor, worker_count=max_workers)

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
                            ctx["registry"].cleanup()
                        except Exception:
                            pass
                        continue
                    await drain_first_completed()

                chunk_t0 = time.perf_counter()
                registry, ts_to_shm_ref, prefetch_ms, register_ms = await asyncio.to_thread(
                    generator._prefetch_union_frames_to_registry_sync,
                    extractor,
                    SharedFrameRegistry,
                    chunk["union_start"],
                    chunk["union_end"],
                    sample_rate,
                    target_height,
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
                    f"submitted={submitted_in_chunk}, inflight={len(pending)}, total={chunk_total_ms:.1f}ms"
                )

                gc.collect()

            while pending:
                await drain_first_completed()

            # 防御性 cleanup
            while active_chunks:
                ctx = active_chunks.popleft()
                try:
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
        return await generator._optimize_screenshot_timestamps(video_path, screenshot_requests)
