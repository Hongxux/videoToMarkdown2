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
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class VLGenerationResult:
    """VL 素材生成结果"""
    success: bool = True
    screenshot_requests: List[Dict[str, Any]] = field(default_factory=list)
    clip_requests: List[Dict[str, Any]] = field(default_factory=list)
    error_msg: str = ""
    used_fallback: bool = False
    fallback_reason: str = ""


class VLMaterialGenerator:
    """
    VL 素材生成器
    
    负责：
    1. 视频按语义单元切割
    2. VL 分析每个片段
    3. 截图时间点优化
    4. 失败回退
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        初始化生成器
        
        Args:
            config: VL 素材生成配置（来自 module2_config.yaml）
        """
        if config is None:
            from .config_loader import load_module2_config
            full_config = load_module2_config()
            config = full_config.get("vl_material_generation", {})
        
        self.config = config
        self.enabled = config.get("enabled", False)
        self.screenshot_config = config.get("screenshot_optimization", {})
        self.fallback_config = config.get("fallback", {})
        
        # 延迟初始化分析器（避免不使用时加载）
        self._analyzer = None
        
        logger.info(f"VLMaterialGenerator 初始化完成: enabled={self.enabled}")
    
    @property
    def analyzer(self):
        """延迟初始化 VL 分析器"""
        if self._analyzer is None:
            from .vl_video_analyzer import VLVideoAnalyzer
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
        
        if use_cache:
            cached_data = self._load_vl_results(cache_path)
            if cached_data:
                logger.info("🚀 使用缓存的VL分析结果,跳过VL API调用")
                result.screenshot_requests = cached_data.get("aggregated_screenshots", [])
                result.clip_requests = cached_data.get("aggregated_clips", [])
                result.success = True
                return result
        
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
            
            for su in semantic_units:
                unit_id = su.get("unit_id", "")
                start_sec = float(su.get("start_sec", 0))
                end_sec = float(su.get("end_sec", 0))
                
                # 查找对应的视频片段
                clip_path = self._find_clip_for_unit(clips_dir, unit_id, start_sec, end_sec)
                
                if not clip_path:
                    logger.warning(f"未找到语义单元 {unit_id} 的视频片段，跳过")
                    continue
                
                # 创建异步分析任务
                task = self.analyzer.analyze_clip(
                    clip_path=clip_path,
                    semantic_unit_start_sec=start_sec,
                    semantic_unit_id=unit_id
                )
                analysis_tasks.append(task)
                task_metadata.append({
                    "unit_id": unit_id,
                    "start_sec": start_sec,
                    "end_sec": end_sec,
                    "clip_path": clip_path
                })
            
            # 并发执行所有 VL 分析任务
            if analysis_tasks:
                logger.info(f"🚀 启动 {len(analysis_tasks)} 个并行 VL 分析任务...")
                analysis_results = await asyncio.gather(*analysis_tasks, return_exceptions=True)
                logger.info(f"✅ 并行 VL 分析完成，共 {len(analysis_results)} 个结果")
            else:
                analysis_results = []
            
            # 收集所有成功的分析结果
            all_screenshot_requests = []
            all_clip_requests = []
            
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
                
                # 收集结果 (暂不优化截图时间点，后续批量处理)
                all_clip_requests.extend(analysis_result.clip_requests)
                all_screenshot_requests.extend(analysis_result.screenshot_requests)
            
            logger.info(f"VL 分析汇总: clips={len(all_clip_requests)}, screenshots={len(all_screenshot_requests)}")
            
            # 保存VL分析原始结果(CV优化前)
            if self.config.get("save_cache", True):
                self._save_vl_results(
                    cache_path=cache_path,
                    analysis_results=analysis_results,
                    task_metadata=task_metadata,
                    screenshot_requests=all_screenshot_requests,
                    clip_requests=all_clip_requests
                )
            
            # 3. 🚀 批量 CV 优化截图时间点
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
        
        clips_dir = Path(output_dir) / "semantic_unit_clips"
        semantic_units_json = Path(output_dir) / "semantic_units_phase2a.json"
        
        # 确保语义单元 JSON 存在
        if not semantic_units_json.exists():
            # 创建临时 JSON 文件
            with open(semantic_units_json, "w", encoding="utf-8") as f:
                json.dump(semantic_units, f, ensure_ascii=False, indent=2)
        
        # 查找脚本路径
        project_root = Path(__file__).resolve().parent.parent.parent
        script_path = project_root / "tools" / "split_video_by_semantic_units.py"
        
        if not script_path.exists():
            raise FileNotFoundError(f"视频切割脚本不存在: {script_path}")
        
        # 检查是否已经切割过（避免重复切割）
        manifest_path = clips_dir / "manifest.json"
        if manifest_path.exists():
            try:
                with open(manifest_path, "r", encoding="utf-8") as f:
                    manifest = json.load(f)
                # 检查是否切割成功
                summary = manifest.get("summary", {})
                if summary.get("success", 0) > 0 and summary.get("failed", 0) == 0:
                    logger.info(f"复用已存在的视频片段: {clips_dir}")
                    return str(clips_dir)
            except Exception:
                pass
        
        # 2. 备用检查：直接检查是否存在对应的 .mp4 文件
        # 如果 manifest 丢失但文件都在，也可以复用
        if clips_dir.exists():
            try:
                existing_clips = list(clips_dir.glob("*.mp4"))
                if len(existing_clips) > 0:
                    # 检查是否所有 unit_id 都有对应的片段
                    missing_units = []
                    for su in semantic_units:
                        unit_id = su.get("unit_id", "")
                        # 检查是否有包含 unit_id 的文件名
                        if not any(unit_id in f.name for f in existing_clips):
                            missing_units.append(unit_id)
                    
                    if not missing_units:
                        logger.info(f"复用已存在的视频片段 (文件完整性检查通过): {clips_dir}")
                        return str(clips_dir)
                    else:
                        logger.warning(f"无法复用视频片段，缺失: {len(missing_units)}/{len(semantic_units)} (e.g., {missing_units[:3]})")
            except Exception as e:
                logger.warning(f"文件完整性检查出错: {e}")
        
        # 执行切割命令
        cmd = [
            "python",
            str(script_path),
            "--video", video_path,
            "--semantic-units", str(semantic_units_json),
            "--out-dir", str(clips_dir),
            "--overwrite"  # 覆盖已存在的文件
        ]
        
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
    
    def _find_clip_for_unit(
        self,
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
        
        # 尝试按 unit_id 匹配
        for clip_file in clips_path.glob("*.mp4"):
            filename = clip_file.name
            # 文件名格式: 001_SU001_topic_0.00-10.00.mp4
            if unit_id in filename:
                return str(clip_file)
        
        # 尝试按时间范围匹配
        time_pattern = f"{start_sec:.2f}-{end_sec:.2f}"
        for clip_file in clips_path.glob("*.mp4"):
            if time_pattern in clip_file.name:
                return str(clip_file)
        
        # 从 manifest.json 查找
        manifest_path = clips_path / "manifest.json"
        if manifest_path.exists():
            try:
                with open(manifest_path, "r", encoding="utf-8") as f:
                    manifest = json.load(f)
                for item in manifest.get("items", []):
                    if item.get("unit_id") == unit_id and item.get("status") == "success":
                        return item.get("out_path")
            except Exception:
                pass
        
        return None
    
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
            from .screenshot_selector import ScreenshotSelector
            
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
        
        架构:
        1. 主进程预读帧并写入 SharedMemory
        2. 提交任务到 ProcessPool
        3. Worker 零拷贝读取帧并执行 CV 分析
        
        Args:
            video_path: 原视频路径
            screenshot_requests: 截图请求列表
            
        Returns:
            List[Dict]: 优化后的截图请求
        """
        if not screenshot_requests:
            return []
        
        time_window = self.screenshot_config.get("time_window_seconds", 1.0)
        
        try:
            from concurrent.futures import ProcessPoolExecutor, as_completed
            from .visual_feature_extractor import VisualFeatureExtractor, get_shared_frame_registry
            import sys
            
            # 尝试导入 cv_worker (位于项目根目录)
            project_root = Path(__file__).resolve().parent.parent.parent
            if str(project_root) not in sys.path:
                sys.path.insert(0, str(project_root))
            
            from cv_worker import run_screenshot_selection_task, init_cv_worker
            
            logger.info(f"🚀 初始化并行 CV 优化: {len(screenshot_requests)} 个请求")
             
            # 初始化帧提取器
            extractor = VisualFeatureExtractor(video_path)
            shm_registry = get_shared_frame_registry()
            
            # 准备任务参数（注意：此阶段是“主进程预读并写入共享内存”，会先串行跑完再启动进程池）
            task_params = self._build_parallel_cv_task_params(
                extractor=extractor,
                shm_registry=shm_registry,
                screenshot_requests=screenshot_requests,
                time_window=time_window,
            )
            
            # 使用进程池并行执行 CV 分析
            max_workers = min(4, len([p for p in task_params if not p.get("skip")]))
            if max_workers == 0:
                logger.warning("所有预读任务失败，跳过 CV 优化")
                return screenshot_requests
            
            logger.info(f"🚀 启动 {max_workers} 个 CV Worker 进程...")
            
            optimized = []
            futures_map = {}
            
            loop = asyncio.get_event_loop()
            
            with ProcessPoolExecutor(max_workers=max_workers, initializer=init_cv_worker) as executor:
                for param in task_params:
                    if param.get("skip"):
                        optimized.append(param["req"])
                        continue
                    
                    future = loop.run_in_executor(
                        executor,
                        run_screenshot_selection_task,
                        video_path,
                        param["unit_id"],
                        param["island_index"],
                        param["expanded_start"],
                        param["expanded_end"],
                        param["shm_frames"],
                        param["fps"]
                    )
                    futures_map[future] = param["req"]
                
                # 等待所有任务完成
                if futures_map:
                    results = await asyncio.gather(*futures_map.keys(), return_exceptions=True)
                    
                    for future, result in zip(futures_map.keys(), results):
                        req = futures_map[future]
                        original_ts = req.get("timestamp_sec", 0)
                        
                        if isinstance(result, Exception):
                            logger.warning(f"CV Worker 异常: {result}")
                            optimized.append(req)
                        elif result and "selected_timestamp" in result:
                            req["timestamp_sec"] = result["selected_timestamp"]
                            req["_optimized"] = True
                            req["_original_timestamp"] = original_ts
                            req["_cv_quality_score"] = result.get("quality_score", 0)
                            optimized.append(req)
                            logger.debug(
                                f"CV 优化: {original_ts:.2f}s -> {result['selected_timestamp']:.2f}s "
                                f"(score={result.get('quality_score', 0):.3f})"
                            )
                        else:
                            optimized.append(req)
            
            logger.info(f"✅ 并行 CV 优化完成: {len(optimized)} 个请求")
            return optimized
            
        except ImportError as e:
            error_msg = f"❌ cv_worker 导入失败: {e} (sys.path={sys.path[:3]}...)"
            logger.warning(error_msg)
            print(f"\n{'='*80}", flush=True)
            print(f"[CV PARALLEL] {error_msg}", flush=True)
            print(f"{'='*80}\n", flush=True)
            import traceback
            traceback.print_exc()
            return await self._optimize_screenshot_timestamps(video_path, screenshot_requests)
        except Exception as e:
            error_msg = f"❌ 并行 CV 优化失败: {e}"
            logger.error(error_msg)
            print(f"\n{'='*80}", flush=True)
            print(f"[CV PARALLEL] {error_msg}", flush=True)
            print(f"{'='*80}\n", flush=True)
            import traceback
            logger.error(traceback.format_exc())
            traceback.print_exc()
            return await self._optimize_screenshot_timestamps(video_path, screenshot_requests)
    
    def _build_parallel_cv_task_params(
        self,
        *,
        extractor: Any,
        shm_registry: Any,
        screenshot_requests: List[Dict[str, Any]],
        time_window: float,
    ) -> List[Dict[str, Any]]:
        """
        构建并行 CV 优化的任务参数。

        为什么要拆出来：
        - 用户常见误解：“已经初始化 ProcessPool 了，为什么还没多进程？”
          实际上进程池启动在后面；本方法负责的“预读帧 -> 写入 SHM”必须在主进程先完成，
          才能让 worker 零拷贝读取并计算。
        - 同时这里是性能瓶颈（尤其短片段 duration<5s 时，extract_frames_fast 会走 OpenCV 随机访问）。

        关键优化：当截图请求很多且窗口重叠明显时，优先做“Union 预读”（一次性预读覆盖范围），
        避免对短视频反复 seek/read 导致看起来“卡在没开多进程”。

        返回结构：每个元素为 dict，包含 req/skip/unit_id/island_index/expanded_start/expanded_end/shm_frames/fps。
        """
        if not screenshot_requests:
            return []

        # Union 预读的启发式参数：默认偏保守，只在“请求多 + 覆盖范围不大”时启用。
        union_min_requests = int(self.screenshot_config.get("prefetch_union_min_requests", 20))
        union_max_span_sec = float(self.screenshot_config.get("prefetch_union_max_span_seconds", 10.0))
        sample_rate = int(self.screenshot_config.get("prefetch_sample_rate", 2))
        target_height = int(self.screenshot_config.get("prefetch_target_height", 360))

        task_params: List[Dict[str, Any]] = []

        # 先计算每个请求的窗口，后续复用（避免重复计算 + 便于 union 策略决策）
        windows: List[Tuple[float, float, str, Dict[str, Any]]] = []
        for idx, req in enumerate(screenshot_requests):
            original_ts = float(req.get("timestamp_sec", 0) or 0)
            unit_id = req.get("unit_id", f"req_{idx}")
            search_start = max(0.0, original_ts - time_window)
            search_end = original_ts + time_window
            windows.append((search_start, search_end, unit_id, req))

        union_start = min(w[0] for w in windows)
        union_end = max(w[1] for w in windows)
        union_span = max(0.0, union_end - union_start)

        use_union_prefetch = (len(screenshot_requests) >= union_min_requests) and (union_span <= union_max_span_sec)

        if use_union_prefetch:
            logger.info(
                f"🧠 [Prefetch] 使用 Union 预读：reqs={len(screenshot_requests)}, "
                f"span={union_span:.2f}s (阈值={union_max_span_sec:.2f}s)"
            )
            frames, timestamps = extractor.extract_frames_fast(
                start_sec=union_start,
                end_sec=union_end,
                sample_rate=sample_rate,
                target_height=target_height,
            )
            if not frames:
                logger.warning(f"Union 预读帧失败: ({union_start:.2f}s-{union_end:.2f}s)，回退到逐请求预读")
                use_union_prefetch = False
            else:
                # 预先把时间戳转换为 shm_ref，后续按窗口筛选即可
                ts_to_shm_ref: Dict[float, Any] = {}
                for ts in timestamps:
                    frame_idx = int(ts * extractor.fps)
                    shm_ref = shm_registry.get_shm_ref(frame_idx)
                    if shm_ref:
                        ts_to_shm_ref[ts] = shm_ref

                for idx, (search_start, search_end, unit_id, req) in enumerate(windows):
                    shm_frames = {
                        ts: shm_ref
                        for ts, shm_ref in ts_to_shm_ref.items()
                        if (search_start <= ts <= search_end)
                    }
                    if not shm_frames:
                        logger.warning(f"Union 预读未覆盖到候选帧: {unit_id} ({search_start:.2f}s-{search_end:.2f}s)")
                        task_params.append({"req": req, "skip": True})
                        continue

                    task_params.append(
                        {
                            "req": req,
                            "skip": False,
                            "unit_id": unit_id,
                            "island_index": idx,
                            "expanded_start": search_start,
                            "expanded_end": search_end,
                            "shm_frames": shm_frames,
                            "fps": extractor.fps,
                        }
                    )

        if not use_union_prefetch:
            logger.info(
                f"🧠 [Prefetch] 使用逐请求预读：reqs={len(screenshot_requests)}, "
                f"union_span={union_span:.2f}s"
            )
            for idx, (search_start, search_end, unit_id, req) in enumerate(windows):
                frames, timestamps = extractor.extract_frames_fast(
                    start_sec=search_start,
                    end_sec=search_end,
                    sample_rate=sample_rate,
                    target_height=target_height,
                )
                if not frames:
                    logger.warning(f"预读帧失败: {unit_id} ({search_start:.2f}s-{search_end:.2f}s)")
                    task_params.append({"req": req, "skip": True})
                    continue

                shm_frames: Dict[float, Any] = {}
                for ts in timestamps:
                    frame_idx = int(ts * extractor.fps)
                    shm_ref = shm_registry.get_shm_ref(frame_idx)
                    if shm_ref:
                        shm_frames[ts] = shm_ref

                task_params.append(
                    {
                        "req": req,
                        "skip": False,
                        "unit_id": unit_id,
                        "island_index": idx,
                        "expanded_start": search_start,
                        "expanded_end": search_end,
                        "shm_frames": shm_frames,
                        "fps": extractor.fps,
                    }
                )

        return task_params

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


class VLMaterialGeneratorError(Exception):
    """VL 素材生成错误"""
    pass


class VLAnalysisError(VLMaterialGeneratorError):
    """VL 分析错误"""
    pass


class JSONParseError(VLMaterialGeneratorError):
    """JSON 解析错误"""
    pass
