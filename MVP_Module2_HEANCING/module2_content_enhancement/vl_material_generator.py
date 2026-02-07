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
import time
import functools
from collections import deque
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
    
    def __init__(self, config: Dict[str, Any] = None, *, cv_executor: Any = None):
        """
        初始化生成器
        
        Args:
            config: VL 素材生成配置（来自 module2_config.yaml）
            cv_executor: 可选的外部 Executor（通常为 python_grpc_server 的全局 CV ProcessPool），用于复用进程池与 initializer。
        """
        if config is None:
            from .config_loader import load_module2_config
            full_config = load_module2_config()
            config = full_config.get("vl_material_generation", {})
        
        self.config = config
        self.enabled = config.get("enabled", False)
        self.screenshot_config = config.get("screenshot_optimization", {})
        self.fallback_config = config.get("fallback", {})

        # 可选复用 gRPC 侧的 ProcessPool（避免额外 spawn 多套进程池）
        self._cv_executor = cv_executor
        
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

    def _should_merge_multistep_unit(self, unit: Dict[str, Any]) -> bool:
        """
        判定是否需要做多段拼接合并（process>10s 且 mult_steps=true）
        """
        knowledge_type = (unit.get("knowledge_type", "") or "").lower()
        start_sec = float(unit.get("start_sec", 0.0))
        end_sec = float(unit.get("end_sec", 0.0))
        duration = max(0.0, end_sec - start_sec)
        return knowledge_type == "process" and duration > 10.0 and bool(unit.get("mult_steps", False))

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
            merged.append({
                "clip_id": f"vl_clip_{unit_id}_merged",
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
        
        if use_cache:
            cached_data = self._load_vl_results(cache_path)
            if cached_data:
                logger.info("🚀 使用缓存的VL分析结果,跳过VL API调用")
                all_screenshot_requests = cached_data.get("aggregated_screenshots", [])
                all_clip_requests = cached_data.get("aggregated_clips", [])
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
                
                for su in semantic_units:
                    unit_id = su.get("unit_id", "")
                    start_sec = float(su.get("start_sec", 0))
                    end_sec = float(su.get("end_sec", 0))
                    duration = max(0.0, end_sec - start_sec)
                    knowledge_type = (su.get("knowledge_type", "") or "").lower()
                    mult_steps = bool(su.get("mult_steps", False))
                    extra_prompt = None
                    if knowledge_type == "process" and duration > 10.0 and mult_steps:
                        extra_prompt = (
                            "该视频片段属于多步骤配置/推演/实操，请你提取的视频片段能剔除冗余部分。"
                            "冗余部分包括但不限于："
                            "知识讲解、解释却没有进行实际操作；"
                            "镜头长时间拍一行命令/JSON/YAML/IP/端口/路径；"
                            "口述逐字念命令、念参数、念配置键值；"
                            "打字过程全程拍摄（无特殊校验/无弹窗，仅普通输入）；"
                            "视频开头/中间长时间口述背景/前置条件/版本要求；"
                            "对着 PPT/纯文本页面念概念、念约束、念依赖清单；"
                            "纯口头梳理流程且无实际操作；"
                            "全程拍摄加载条、安装进度、服务启动等待；"
                            "重复刷新或重复检查且无新信息；"
                            "机械重复话术或无意义过渡；"
                            "画面已显示结果却口述复述。"
                            "截图选择要求：多步骤中的每一步终态截图，以及每一步需要学习者记忆的关键帧截图。"
                            "若冗余较多仍需返回最小可用片段，不要返回空片段。"
                        )
                    
                    # 查找对应的视频片段
                    clip_path = self._find_clip_for_unit(clips_dir, unit_id, start_sec, end_sec)
                    
                    if not clip_path:
                        logger.warning(f"未找到语义单元 {unit_id} 的视频片段，跳过")
                        continue
                    
                    # 创建异步分析任务
                    task = self.analyzer.analyze_clip(
                        clip_path=clip_path,
                        semantic_unit_start_sec=start_sec,
                        semantic_unit_id=unit_id,
                        extra_prompt=extra_prompt
                    )
                    analysis_tasks.append(task)
                    task_metadata.append({
                        "unit_id": unit_id,
                        "start_sec": start_sec,
                        "end_sec": end_sec,
                        "clip_path": clip_path
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
                    
                    # 收集结果 (暂不优化截图时间点，后续批量处理)
                    all_clip_requests.extend(analysis_result.clip_requests)
                    all_screenshot_requests.extend(analysis_result.screenshot_requests)
                
                all_clip_requests = self._merge_multistep_clip_requests(semantic_units, all_clip_requests)
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
            logger.info(f"🚀 使用流式处理模式 (streaming_pipeline=true)")
            return await self._optimize_screenshots_streaming_pipeline(
                video_path,
                screenshot_requests
            )
        else:
            logger.info(f"🚀 使用批量处理模式 (streaming_pipeline=false)")
            return await self._optimize_screenshots_batch_mode(
                video_path,
                screenshot_requests
            )
    
    async def _optimize_screenshots_batch_mode(
        self,
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
        
        time_window = self.screenshot_config.get("time_window_seconds", 1.0)
        
        try:
            from concurrent.futures import ProcessPoolExecutor
            from .visual_feature_extractor import VisualFeatureExtractor, SharedFrameRegistry
            import sys
            import gc
            
            # 尝试导入 cv_worker (位于项目根目录)
            project_root = Path(__file__).resolve().parent.parent.parent
            if str(project_root) not in sys.path:
                sys.path.insert(0, str(project_root))
            
            from cv_worker import run_screenshot_selection_task, init_cv_worker

            logger.info(f"🚀 [Batch Mode] 初始化并行 CV 优化: {len(screenshot_requests)} 个请求")

            # 初始化帧提取器（主进程负责预读与写入 SHM）
            extractor = VisualFeatureExtractor(video_path)

            # 配置参数
            max_workers = self._resolve_max_workers(request_count=len(screenshot_requests))
            max_inflight_multiplier = int(self.screenshot_config.get("max_inflight_multiplier", 2))
            max_inflight = max(1, max_workers * max_inflight_multiplier)
            sample_rate = int(self.screenshot_config.get("prefetch_sample_rate", 2))
            target_height = int(self.screenshot_config.get("prefetch_target_height", 360))
            chunk_max_span_sec = float(self.screenshot_config.get("prefetch_union_max_span_seconds", 10.0))
            chunk_max_requests = int(self.screenshot_config.get("prefetch_chunk_max_requests", 1000))

            chunks = self._build_screenshot_prefetch_chunks(
                screenshot_requests=screenshot_requests,
                time_window=time_window,
                max_span_seconds=chunk_max_span_sec,
                max_requests=chunk_max_requests,
            )

            logger.info(
                f"📦 [Batch Mode] Config: workers={max_workers}, inflight={max_inflight}, "
                f"chunks={len(chunks)}, max_span={chunk_max_span_sec:.2f}s, max_req/chunk={chunk_max_requests}"
            )

            executor = self._cv_executor
            created_executor = False
            if executor is None:
                executor = ProcessPoolExecutor(max_workers=max_workers, initializer=init_cv_worker)
                created_executor = True

            try:
                loop = asyncio.get_running_loop()

                # 可选 Warmup：诊断是否真的分发到多个 Worker
                await self._maybe_warmup_pool(loop=loop, executor=executor, worker_count=max_workers)

                submitted_tasks = 0
                completed_tasks = 0

                for chunk_id, chunk in enumerate(chunks):
                    chunk_t0 = time.perf_counter()

                    registry, ts_to_shm_ref, prefetch_ms, register_ms = await asyncio.to_thread(
                        self._prefetch_union_frames_to_registry_sync,
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

                        task_params = self._build_task_params_from_ts_map(
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
                                self._apply_selection_result(req=req, original_ts=original_ts, unit_id=unit_id, result=r)

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
    
    def _is_truthy_env(self, name: str, default: str = "0") -> bool:
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
        # 1) injected executor 优先
        if self._cv_executor is not None:
            injected_workers = getattr(self._cv_executor, "_max_workers", None)
            if isinstance(injected_workers, int) and injected_workers > 0:
                return max(1, min(injected_workers, request_count))

        # 2) config fallback
        max_workers_config = self.screenshot_config.get("max_workers", "auto")
        hard_cap = 6

        if isinstance(max_workers_config, int):
            desired = max_workers_config
        else:
            config_str = str(max_workers_config).strip().lower()
            if config_str == "auto":
                desired = max(1, (os.cpu_count() or 2) - 1)
            else:
                desired = int(config_str)

        return max(1, min(desired, hard_cap, request_count))

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
        if not screenshot_requests:
            return []

        windows = []
        for idx, req in enumerate(screenshot_requests):
            original_ts = float(req.get("timestamp_sec", 0) or 0.0)
            search_start = max(0.0, original_ts - time_window)
            search_end = original_ts + time_window
            unit_id = (
                req.get("semantic_unit_id")
                or req.get("unit_id")
                or req.get("screenshot_id")
                or f"req_{idx}"
            )
            windows.append(
                {
                    "req": req,
                    "order_idx": idx,
                    "unit_id": unit_id,
                    "island_index": idx,
                    "original_ts": original_ts,
                    "expanded_start": search_start,
                    "expanded_end": search_end,
                }
            )

        windows.sort(key=lambda w: w["original_ts"])

        chunks: List[Dict[str, Any]] = []
        current: List[Dict[str, Any]] = []
        union_start: Optional[float] = None
        union_end: Optional[float] = None

        def flush():
            nonlocal current, union_start, union_end
            if not current:
                return
            chunks.append(
                {
                    "union_start": float(union_start or 0.0),
                    "union_end": float(union_end or 0.0),
                    "windows": current,
                }
            )
            current = []
            union_start = None
            union_end = None

        for w in windows:
            if not current:
                current = [w]
                union_start = w["expanded_start"]
                union_end = w["expanded_end"]
                continue

            candidate_start = min(union_start, w["expanded_start"])  # type: ignore[arg-type]
            candidate_end = max(union_end, w["expanded_end"])  # type: ignore[arg-type]
            candidate_span = candidate_end - candidate_start

            if (len(current) >= max_requests) or (candidate_span > max_span_seconds):
                flush()
                current = [w]
                union_start = w["expanded_start"]
                union_end = w["expanded_end"]
                continue

            current.append(w)
            union_start = candidate_start
            union_end = candidate_end

        flush()
        return chunks

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
        task_params: List[Dict[str, Any]] = []
        for w in windows:
            search_start = float(w["expanded_start"])
            search_end = float(w["expanded_end"])
            shm_frames = {ts: ref for ts, ref in ts_to_shm_ref.items() if (search_start <= ts <= search_end)}
            if not shm_frames:
                task_params.append({"req": w["req"], "skip": True})
                continue
            task_params.append(
                {
                    "req": w["req"],
                    "skip": False,
                    "unit_id": w["unit_id"],
                    "island_index": w["island_index"],
                    "expanded_start": search_start,
                    "expanded_end": search_end,
                    "shm_frames": shm_frames,
                    "fps": fps,
                }
            )
        return task_params

    async def _maybe_warmup_pool(self, *, loop: asyncio.AbstractEventLoop, executor: Any, worker_count: int) -> None:
        if not self._is_truthy_env("CV_POOL_WARMUP", "0"):
            return

        warmup_n = int(os.getenv("CV_POOL_WARMUP_N", str(worker_count)))
        warmup_n = max(1, min(warmup_n, max(1, worker_count * 2)))
        try:
            from cv_worker import warmup_worker
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
        
        time_window = self.screenshot_config.get("time_window_seconds", 1.0)
        
        try:
            from concurrent.futures import ProcessPoolExecutor
            from .visual_feature_extractor import VisualFeatureExtractor, SharedFrameRegistry
            import sys
            import gc
            
            # 导入 cv_worker
            project_root = Path(__file__).resolve().parent.parent.parent
            if str(project_root) not in sys.path:
                sys.path.insert(0, str(project_root))
            
            from cv_worker import run_screenshot_selection_task, init_cv_worker
            
            logger.info(f"🚀 [Streaming Pipeline] 启动流式处理: {len(screenshot_requests)} 个请求")
            
            # 初始化帧提取器
            extractor = VisualFeatureExtractor(video_path)

            # 配置参数
            max_workers = self._resolve_max_workers(request_count=len(screenshot_requests))
            max_inflight_multiplier = int(self.screenshot_config.get("max_inflight_multiplier", 2))
            max_inflight = max(1, max_workers * max_inflight_multiplier)
            overlap_buffers = int(self.screenshot_config.get("streaming_overlap_buffers", 2))
            overlap_buffers = max(1, overlap_buffers)

            sample_rate = int(self.screenshot_config.get("prefetch_sample_rate", 2))
            target_height = int(self.screenshot_config.get("prefetch_target_height", 360))
            chunk_max_span_sec = float(self.screenshot_config.get("prefetch_union_max_span_seconds", 10.0))
            chunk_max_requests = int(self.screenshot_config.get("prefetch_chunk_max_requests", 1000))

            chunks = self._build_screenshot_prefetch_chunks(
                screenshot_requests=screenshot_requests,
                time_window=time_window,
                max_span_seconds=chunk_max_span_sec,
                max_requests=chunk_max_requests,
            )

            logger.info(
                f"📦 [Streaming Pipeline] Config: workers={max_workers}, inflight={max_inflight}, "
                f"overlap_buffers={overlap_buffers}, chunks={len(chunks)}, "
                f"max_span={chunk_max_span_sec:.2f}s, max_req/chunk={chunk_max_requests}"
            )

            executor = self._cv_executor
            created_executor = False
            if executor is None:
                executor = ProcessPoolExecutor(max_workers=max_workers, initializer=init_cv_worker)
                created_executor = True

            try:
                loop = asyncio.get_running_loop()

                # 可选 Warmup：诊断是否真的分发到多个 Worker
                await self._maybe_warmup_pool(loop=loop, executor=executor, worker_count=max_workers)

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
                        self._apply_selection_result(req=req, original_ts=original_ts, unit_id=unit_id, result=result)

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
                        self._prefetch_union_frames_to_registry_sync,
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

                    task_params = self._build_task_params_from_ts_map(
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
            error_msg = f"❌ cv_worker 导入失败 (流式模式): {e}"
            logger.warning(error_msg)
            print(f"\n{'='*80}", flush=True)
            print(f"[CV STREAMING] {error_msg}", flush=True)
            print(f"{'='*80}\n", flush=True)
            import traceback
            traceback.print_exc()
            return await self._optimize_screenshot_timestamps(video_path, screenshot_requests)
        except Exception as e:
            error_msg = f"❌ 流式处理失败: {e}"
            logger.error(error_msg)
            print(f"\n{'='*80}", flush=True)
            print(f"[CV STREAMING] {error_msg}", flush=True)
            print(f"{'='*80}\n", flush=True)
            import traceback
            logger.error(traceback.format_exc())
            traceback.print_exc()
            return await self._optimize_screenshot_timestamps(video_path, screenshot_requests)
    
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
