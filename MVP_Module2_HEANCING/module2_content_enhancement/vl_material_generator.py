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
    
    async def generate(
        self,
        video_path: str,
        semantic_units: List[Dict[str, Any]],
        output_dir: str = None
    ) -> VLGenerationResult:
        """
        生成素材请求
        
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
            
            # 2. 逐个分析语义单元片段
            logger.info(f"开始 VL 分析 {len(semantic_units)} 个语义单元...")
            
            for su in semantic_units:
                unit_id = su.get("unit_id", "")
                start_sec = float(su.get("start_sec", 0))
                
                # 查找对应的视频片段
                clip_path = self._find_clip_for_unit(clips_dir, unit_id, start_sec, su.get("end_sec", 0))
                
                if not clip_path:
                    logger.warning(f"未找到语义单元 {unit_id} 的视频片段，跳过")
                    continue
                
                # 调用 VL 分析
                analysis_result = await self.analyzer.analyze_clip(
                    clip_path=clip_path,
                    semantic_unit_start_sec=start_sec,
                    semantic_unit_id=unit_id
                )
                
                if not analysis_result.success:
                    logger.warning(f"语义单元 {unit_id} VL 分析失败: {analysis_result.error_msg}")
                    continue
                
                # 3. 优化截图时间点
                if self.screenshot_config.get("enabled", True):
                    optimized_screenshots = await self._optimize_screenshot_timestamps(
                        video_path=video_path,
                        screenshot_requests=analysis_result.screenshot_requests
                    )
                    analysis_result.screenshot_requests = optimized_screenshots
                
                # 汇总结果
                result.clip_requests.extend(analysis_result.clip_requests)
                result.screenshot_requests.extend(analysis_result.screenshot_requests)
            
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
