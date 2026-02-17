"""
模块说明：Module2 内容增强中的 cv_knowledge_validator 模块。
执行逻辑：
1) 聚合本模块的类/函数，对外提供核心能力。
2) 通过内部调用与外部依赖完成具体处理。
实现方式：通过模块内函数组合与外部依赖调用实现。
核心价值：统一模块职责边界，降低跨文件耦合成本。
输入：
- 调用方传入的参数与数据路径。
输出：
- 各函数/类返回的结构化结果或副作用。
补充说明：
基于第一性原理的视觉知识验证模块 v2.1：
- 稳定岛检测 (ROI内SSIM≥0.9, ≥500ms)
- 动作单元检测 (ROI内diff_ratio>0.05, ≥300ms)
- 视觉冗余5类细分 (转场/无关动作/空白/装饰/遮挡)
- 视觉知识类型识别 (抽象/具象/过程)
- 跨模态一致性校验
性能优化:
- ROI增量复用缓存
- 帧特征增量缓存
- 轻量冗余初筛
- 状态判定轻量校验
- 动态采样率适配
- 批量增量并行处理
多级采样策略:
- 1fps: ROI检测
- 5fps: 状态判定
- 10fps: 边界精修"""

import cv2
import numpy as np
import logging
import os
import time
import hashlib
import shutil
import subprocess
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

from services.python_grpc.src.content_pipeline.infra.runtime import cache_metrics
from services.python_grpc.src.content_pipeline.infra.runtime.resource_manager import get_resource_manager
from services.python_grpc.src.content_pipeline.infra.runtime.fast_metrics import fast_ssim, fast_diff_ratio
from services.python_grpc.src.content_pipeline.phase2a.vision.cv_models import (
    CVConfig,
    VisualKnowledgeType,
    RedundancyType,
    FrameState,
    StableIsland,
    Modality,
    ActionUnit,
    RedundancySegment,
    VisionStats,
    CVValidationResult,
    ConflictPackage,
    ROICache,
    FrameFeatureCache,
)

from services.python_grpc.src.content_pipeline.phase2a.vision.cv_state_analysis import (
    detect_visual_states,
    merge_state_intervals,
)

logger = logging.getLogger(__name__)

# =============================================================================
# Core Implementation
# =============================================================================

class CVKnowledgeValidator:
    """类说明：CVKnowledgeValidator 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    
    def __init__(self, video_path: str, use_resource_manager: bool = True):
        """
        执行逻辑：
        1) 解析配置或依赖，准备运行环境。
        2) 初始化对象状态、缓存与依赖客户端。
        实现方式：通过内部方法调用/状态更新、OpenCV 图像处理实现。
        核心价值：在初始化阶段固化依赖，保证运行稳定性。
        输入参数：
        - video_path: 文件路径（类型：str）。
        - use_resource_manager: 函数入参（类型：bool）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self.video_path = video_path
        self.source_video_path = video_path
        self.use_resource_manager = use_resource_manager
        self.cap: Optional[cv2.VideoCapture] = None
        self.fps: float = 30.0
        self.frame_count: int = 0
        self.duration_sec: float = 0.0
        self._decode_fallback_path: Optional[str] = None
        
        # 性能优化: 缓存
        self.roi_cache = ROICache()
        self.frame_feat_cache = FrameFeatureCache()
        
        # 上一单元的复杂度 (用于动态采样率)
        self.last_unit_complexity = "medium"
        
        self._init_video()

    @staticmethod
    def _resolve_ffmpeg_bin() -> Optional[str]:
        """
        做什么：解析可用 ffmpeg 可执行路径。
        为什么：OpenCV 在某些编码（如 AV1）上可能 `isOpened=true` 但 `read` 失败。
        权衡：优先 PATH，兼容历史固定路径，避免强依赖单一安装方式。
        """
        candidates = [
            str(os.getenv("FFMPEG_BIN", "") or "").strip(),
            str(os.getenv("FFMPEG_PATH", "") or "").strip(),
            "ffmpeg",
            r"D:\New_ANACONDA\envs\whisper_env\Library\bin\ffmpeg.exe",
        ]
        for candidate in candidates:
            if not candidate:
                continue
            if shutil.which(candidate):
                return candidate
            if Path(candidate).exists():
                return candidate
        return None

    @staticmethod
    def _probe_capture_readable(cap: Any) -> bool:
        """
        做什么：探测 capture 是否可真正解码出帧。
        为什么：仅检查 `isOpened()` 无法覆盖“容器可开但编码不可解码”的场景。
        权衡：初始化多一次轻量 read，换来更稳定的失败前置与回退。
        """
        try:
            if cap is None or not hasattr(cap, "isOpened") or not cap.isOpened():
                return False

            original_pos = None
            if hasattr(cap, "get"):
                try:
                    original_pos = float(cap.get(cv2.CAP_PROP_POS_FRAMES))
                except Exception:
                    original_pos = None

            if hasattr(cap, "set"):
                try:
                    cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                except Exception:
                    pass

            ret, frame = cap.read()
            readable = bool(ret and frame is not None and getattr(frame, "size", 0) > 0)

            if hasattr(cap, "set"):
                try:
                    if isinstance(original_pos, (int, float)) and original_pos >= 0:
                        cap.set(cv2.CAP_PROP_POS_FRAMES, original_pos)
                    else:
                        cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                except Exception:
                    pass
            return readable
        except Exception:
            return False

    def _build_decode_fallback_path(self, source_path: str) -> Path:
        """
        做什么：生成稳定的转码缓存路径（按源路径+mtime+size 指纹）。
        为什么：避免同一源文件被重复转码。
        权衡：会在源目录写入 `_opencv_decode_fallback` 缓存文件。
        """
        source = Path(source_path)
        try:
            stat = source.stat()
            fingerprint = f"{source.resolve()}::{stat.st_size}::{int(stat.st_mtime)}"
        except Exception:
            fingerprint = f"{source}::{time.time_ns()}"
        digest = hashlib.md5(fingerprint.encode("utf-8", errors="ignore")).hexdigest()[:12]
        cache_dir = source.parent / "_opencv_decode_fallback"
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cache_dir / f"{source.stem}_{digest}_h264.mp4"

    def _transcode_to_h264_for_opencv(self, source_path: str) -> Optional[str]:
        """
        做什么：将源视频转码为 H.264，供 OpenCV 稳定读取。
        为什么：兜底 AV1 等在当前运行环境不可读的编码。
        权衡：仅在探测失败时触发，避免影响常规性能。
        """
        ffmpeg_bin = self._resolve_ffmpeg_bin()
        if not ffmpeg_bin:
            logger.warning("ffmpeg not found, cannot apply decode fallback for: %s", source_path)
            return None

        fallback_path = self._build_decode_fallback_path(source_path)
        if fallback_path.exists() and fallback_path.stat().st_size > 0:
            return str(fallback_path)

        command = [
            ffmpeg_bin,
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-i",
            source_path,
            "-map",
            "0:v:0",
            "-map",
            "0:a?",
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "22",
            "-pix_fmt",
            "yuv420p",
            "-c:a",
            "aac",
            "-b:a",
            "96k",
            "-movflags",
            "+faststart",
            str(fallback_path),
        ]
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=300,
            )
        except Exception as exc:
            logger.warning("ffmpeg decode fallback exception for %s: %s", source_path, exc)
            return None

        if result.returncode != 0:
            logger.warning(
                "ffmpeg decode fallback failed for %s: rc=%s, err=%s",
                source_path,
                result.returncode,
                str(result.stderr or "").strip()[:300],
            )
            return None

        if not fallback_path.exists() or fallback_path.stat().st_size <= 0:
            logger.warning("ffmpeg decode fallback generated empty file for %s", source_path)
            return None
        return str(fallback_path)

    def _open_direct_capture_with_decode_fallback(self, source_path: str) -> Tuple[cv2.VideoCapture, str]:
        """
        做什么：直接打开视频；若不可解码则转码后重开。
        为什么：worker direct 模式下没有 ResourceManager 兜底，需要本地自治。
        权衡：仅失败时转码，保证常规路径零额外成本。
        """
        cap = cv2.VideoCapture(source_path)
        if self._probe_capture_readable(cap):
            return cap, source_path
        if cap is not None:
            cap.release()

        fallback_path = self._transcode_to_h264_for_opencv(source_path)
        if not fallback_path:
            raise ValueError(f"Cannot decode video: {source_path}")

        fallback_cap = cv2.VideoCapture(fallback_path)
        if not self._probe_capture_readable(fallback_cap):
            if fallback_cap is not None:
                fallback_cap.release()
            raise ValueError(
                f"Cannot decode video even after ffmpeg fallback: source={source_path}, fallback={fallback_path}"
            )

        self._decode_fallback_path = fallback_path
        logger.warning(
            "Decode fallback applied: source=%s, fallback=%s",
            source_path,
            fallback_path,
        )
        return fallback_cap, fallback_path
    
    def _init_video(self):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、OpenCV 图像处理实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：self.use_resource_manager
        - 条件：not self.cap.isOpened()
        - 条件：self.cap
        依据来源（证据链）：
        - 对象内部状态：self.cap, self.fps, self.target_width, self.use_resource_manager。
        输入参数：
        - 无。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        if self.use_resource_manager:
            try:
                rm = get_resource_manager()
                self.cap = rm.get_video_capture(self.video_path)
                info = rm.get_video_info(self.video_path)
                if self._probe_capture_readable(self.cap):
                    self.fps = info["fps"]
                    self.frame_count = info["frame_count"]
                    self.duration_sec = info["duration"]
                else:
                    logger.warning(
                        "ResourceManager capture decode probe failed, fallback to direct mode: %s",
                        self.video_path,
                    )
                    self.use_resource_manager = False
            except Exception as exc:
                logger.warning(
                    "ResourceManager init failed, fallback to direct mode: %s, err=%s",
                    self.video_path,
                    exc,
                )
                self.use_resource_manager = False

        if not self.use_resource_manager:
            self.cap, effective_path = self._open_direct_capture_with_decode_fallback(self.video_path)
            self.video_path = effective_path
            self.fps = self.cap.get(cv2.CAP_PROP_FPS)
            self.frame_count = int(self.cap.get(cv2.CAP_PROP_FRAME_COUNT))
            self.duration_sec = self.frame_count / self.fps if self.fps > 0 else 0

            logger.info(
                f"Video loaded (Direct): source={self.source_video_path}, "
                f"effective={self.video_path}, FPS={self.fps:.2f}, Duration={self.duration_sec:.2f}s"
            )
        
        # 🚀 动态计算缩放比例 (最大宽度 640)
        # 缩小处理分辨率能大幅降低内存消耗 (1080p -> 640p 内存减少 ~84%)
        width = self.cap.get(cv2.CAP_PROP_FRAME_WIDTH) if self.cap else 1920
        self.target_width = 640
        self.processing_scale = self.target_width / width if width > self.target_width else 1.0
    
    def _resize_frame(self, frame: np.ndarray) -> np.ndarray:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：self.processing_scale < 1.0 and frame is not None
        依据来源（证据链）：
        - 输入参数：frame。
        - 对象内部状态：self.processing_scale。
        输入参数：
        - frame: 函数入参（类型：np.ndarray）。
        输出参数：
        - 函数计算/封装后的结果对象。"""
        if self.processing_scale < 1.0 and frame is not None:
             # Fast resize using INTER_AREA for downsampling or INTER_LINEAR for speed
             # INTER_LINEAR is faster and sufficient for CV metrics
             width = int(frame.shape[1] * self.processing_scale)
             height = int(frame.shape[0] * self.processing_scale)
             return cv2.resize(frame, (width, height), interpolation=cv2.INTER_LINEAR)
        return frame

    
    def close(self):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：self.cap
        - 条件：not self.use_resource_manager
        依据来源（证据链）：
        - 对象内部状态：self.cap, self.use_resource_manager。
        输入参数：
        - 无。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        if self.cap:
            if not self.use_resource_manager:
                self.cap.release()
            self.cap = None
    
    def __enter__(self):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - 无。
        输出参数：
        - 函数计算/封装后的结果对象。"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - exc_type: 函数入参（类型：未标注）。
        - exc_val: 函数入参（类型：未标注）。
        - exc_tb: 函数入参（类型：未标注）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self.close()
    
    # =========================================================================
    # 措施3: 轻量冗余初筛
    # =========================================================================
    
    def _quick_redundancy_check(self, frame: np.ndarray, 
                                 roi: Optional[Tuple[int, int, int, int]] = None) -> Optional[RedundancyType]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：variance <= CVConfig.RED_LOW_VAR_THRESH
        - 条件：len(frame.shape) == 3
        依据来源（证据链）：
        - 输入参数：frame。
        输入参数：
        - frame: 函数入参（类型：np.ndarray）。
        - roi: 函数入参（类型：Optional[Tuple[int, int, int, int]]）。
        输出参数：
        - 函数计算/封装后的结果对象。"""
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY) if len(frame.shape) == 3 else frame
        
        # 仅检查: 亮度方差 (纯黑/纯白屏)
        variance = np.var(gray)
        if variance <= CVConfig.RED_LOW_VAR_THRESH:
            logger.debug(f"Redundancy: LOW_VAR={variance:.1f}")
            return RedundancyType.RED_BLANK
        
        # 不再检查边缘占比 - PPT类视频边缘本来就很少
        # 不再检查ROI有效像素 - 可能误判
        return None

    
    # =========================================================================
    # 措施1: ROI增量复用
    # =========================================================================
    
    def _compute_layout_feature(self, frame: np.ndarray) -> float:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：len(frame.shape) == 3
        依据来源（证据链）：
        - 输入参数：frame。
        输入参数：
        - frame: 函数入参（类型：np.ndarray）。
        输出参数：
        - 数值型计算结果。"""
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY) if len(frame.shape) == 3 else frame
        # 8x8分块灰度均值
        h, w = gray.shape
        block_h, block_w = h // 8, w // 8
        feature = 0.0
        for i in range(8):
            for j in range(8):
                block = gray[i*block_h:(i+1)*block_h, j*block_w:(j+1)*block_w]
                feature += np.mean(block) * (i * 8 + j + 1)
        return feature
    
    def _detect_roi(self, frame: np.ndarray, 
                    use_cache: bool = True) -> Optional[Tuple[int, int, int, int]]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：use_cache
        - 条件：not contours
        - 条件：not all_points
        依据来源（证据链）：
        - 输入参数：frame, use_cache。
        - 对象内部状态：self.roi_cache。
        输入参数：
        - frame: 函数入参（类型：np.ndarray）。
        - use_cache: 函数入参（类型：bool）。
        输出参数：
        - 多值结果元组（各元素含义见实现）。"""
        # 措施1: 尝试复用缓存
        if use_cache:
            layout_feat = self._compute_layout_feature(frame)
            prev_feat = self.roi_cache.get_last_layout_feature()
            if prev_feat and abs(layout_feat - prev_feat) / prev_feat < (1 - CVConfig.ROI_REUSE_SIM_THRESH):
                cached_roi = list(self.roi_cache.cache.values())[-1]['roi'] if self.roi_cache.cache else None
                if cached_roi:
                    cache_metrics.hit("module2.cv_validator.roi_cache")
                    logger.debug("ROI cache hit, reusing previous ROI")
                    return cached_roi
            cache_metrics.miss("module2.cv_validator.roi_cache")
        
        # 新检测ROI
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY) if len(frame.shape) == 3 else frame
        h, w = gray.shape
        
        # 边缘检测
        edges = cv2.Canny(gray, 50, 150)
        contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        
        if not contours:
            # 默认ROI: 中心80%区域
            margin_x, margin_y = int(w * 0.1), int(h * 0.1)
            return (margin_x, margin_y, w - margin_x, h - margin_y)
        
        # 计算包围盒
        all_points = []
        min_area = (h * w) * 0.001  # 最小面积阈值
        for cnt in contours:
            x, y, cw, ch = cv2.boundingRect(cnt)
            if cw * ch > min_area:
                all_points.append((x, y))
                all_points.append((x + cw, y + ch))
        
        if not all_points:
            margin_x, margin_y = int(w * 0.1), int(h * 0.1)
            return (margin_x, margin_y, w - margin_x, h - margin_y)
        
        points = np.array(all_points)
        x1, y1 = np.min(points, axis=0)
        x2, y2 = np.max(points, axis=0)
        
        # 添加边距
        margin = 10
        x1 = max(0, x1 - margin)
        y1 = max(0, y1 - margin)
        x2 = min(w, x2 + margin)
        y2 = min(h, y2 + margin)
        
        roi = (int(x1), int(y1), int(x2), int(y2))
        
        # 缓存ROI
        if use_cache:
            layout_feat = self._compute_layout_feature(frame)
            self.roi_cache.put(0, roi, layout_feat, 0.9)
        
        return roi
    
    # =========================================================================
    # SSIM计算 (ROI内)
    # =========================================================================
    
    def _calculate_ssim_roi(self, frame1: np.ndarray, frame2: np.ndarray,
                             roi: Tuple[int, int, int, int]) -> float:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：roi1.size == 0 or roi2.size == 0
        - 条件：len(frame1.shape) == 3
        - 条件：len(frame2.shape) == 3
        依据来源（证据链）：
        - 输入参数：frame1, frame2。
        输入参数：
        - frame1: 函数入参（类型：np.ndarray）。
        - frame2: 函数入参（类型：np.ndarray）。
        - roi: 函数入参（类型：Tuple[int, int, int, int]）。
        输出参数：
        - 数值型计算结果。"""
        x1, y1, x2, y2 = roi
        
        gray1 = cv2.cvtColor(frame1, cv2.COLOR_BGR2GRAY) if len(frame1.shape) == 3 else frame1
        gray2 = cv2.cvtColor(frame2, cv2.COLOR_BGR2GRAY) if len(frame2.shape) == 3 else frame2
        
        roi1 = gray1[y1:y2, x1:x2]
        roi2 = gray2[y1:y2, x1:x2]
        
        if roi1.size == 0 or roi2.size == 0:
            return 0.0
        
        # 使用 Numba 加速版本 (measures 7/10.2.4)
        return float(fast_ssim(roi1, roi2))
    
    # =========================================================================
    # Diff Ratio计算 (ROI内)
    # =========================================================================
    
    def _calculate_diff_ratio_roi(self, frame1: np.ndarray, frame2: np.ndarray,
                                   roi: Tuple[int, int, int, int]) -> float:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：total_pixels == 0
        - 条件：roi1.size == 0 or roi2.size == 0
        - 条件：not CVConfig.MOTION_FILTER_ENABLED
        依据来源（证据链）：
        - 输入参数：frame1, frame2。
        输入参数：
        - frame1: 函数入参（类型：np.ndarray）。
        - frame2: 函数入参（类型：np.ndarray）。
        - roi: 函数入参（类型：Tuple[int, int, int, int]）。
        输出参数：
        - 数值型计算结果。"""
        x1, y1, x2, y2 = roi
        roi_w = x2 - x1
        roi_h = y2 - y1
        total_pixels = roi_w * roi_h
        
        if total_pixels == 0:
            return 0.0
        
        gray1 = cv2.cvtColor(frame1, cv2.COLOR_BGR2GRAY) if len(frame1.shape) == 3 else frame1
        gray2 = cv2.cvtColor(frame2, cv2.COLOR_BGR2GRAY) if len(frame2.shape) == 3 else frame2
        
        roi1 = gray1[y1:y2, x1:x2]
        roi2 = gray2[y1:y2, x1:x2]
        
        if roi1.size == 0 or roi2.size == 0:
            return 0.0
        
        # ========== V8.0 智能干扰过滤 ==========
        if not CVConfig.MOTION_FILTER_ENABLED:
            # 未启用过滤，使用原始快速版本
            return float(fast_diff_ratio(roi1, roi2, threshold=30))
        
        # Step 1: 计算差分 + 二值化
        diff = cv2.absdiff(roi1, roi2)
        _, thresh = cv2.threshold(diff, 30, 255, cv2.THRESH_BINARY)
        
        # Step 2: 形态学开运算 (过滤鼠标光标和细小噪点)
        kernel_size = CVConfig.MOTION_MORPH_KERNEL_SIZE
        kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (kernel_size, kernel_size))
        cleaned = cv2.morphologyEx(thresh, cv2.MORPH_OPEN, kernel)
        
        # Step 3: 固定ROI排除 (人物区域 - 用户配置)
        if CVConfig.PERSON_EXCLUDE_ROIS:
            for roi_ratio in CVConfig.PERSON_EXCLUDE_ROIS:
                ex_x1 = int(roi_w * roi_ratio[0])
                ex_y1 = int(roi_h * roi_ratio[1])
                ex_x2 = int(roi_w * roi_ratio[2])
                ex_y2 = int(roi_h * roi_ratio[3])
                # 将排除区域置零
                cleaned[ex_y1:ex_y2, ex_x1:ex_x2] = 0
        
        # Step 4: 轮廓分析 + 小面积过滤 (鼠标)
        contours, _ = cv2.findContours(cleaned, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        
        min_area = total_pixels * CVConfig.MOTION_MIN_AREA_RATIO
        valid_change_pixels = 0
        
        for cnt in contours:
            area = cv2.contourArea(cnt)
            # 过滤: 忽略过小区域 (鼠标光标残留)
            if area < min_area:
                continue
            valid_change_pixels += area
        
        return valid_change_pixels / total_pixels
    
    # =========================================================================
    # 采样帧提取
    # =========================================================================
    
    def _sample_frames(self, start_sec: float, end_sec: float, 
                       fps: float) -> List[Tuple[float, np.ndarray]]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：self.use_resource_manager
        - 条件：not raw_frames and self.cap
        - 条件：ret and frame is not None
        依据来源（证据链）：
        - 对象内部状态：self.cap, self.use_resource_manager。
        输入参数：
        - start_sec: 起止时间/区间边界（类型：float）。
        - end_sec: 起止时间/区间边界（类型：float）。
        - fps: 函数入参（类型：float）。
        输出参数：
        - Tuple[float, np.ndarray] 列表（与输入或处理结果一一对应）。"""
        # 统一时间边界，避免上游时间戳漂移导致越界读帧
        sample_fps = fps if isinstance(fps, (int, float)) and fps > 0 else 1.0
        sampling_start = max(0.0, float(start_sec or 0.0))
        sampling_end = max(sampling_start, float(end_sec or sampling_start))
        if self.duration_sec > 0:
            duration = float(self.duration_sec)
            if sampling_start > duration:
                logger.warning(
                    f"Sampling start {sampling_start:.3f}s exceeds video duration {duration:.3f}s, skip"
                )
                return []
            if sampling_end > duration:
                logger.warning(
                    f"Clamp sampling end_sec from {sampling_end:.3f}s to video duration {duration:.3f}s"
                )
                sampling_end = duration

        raw_frames = []
        if self.use_resource_manager:
            # 兼容: 如果 ResourceManager 返回 None 或空，回退到本地 cap
            try:
                raw_frames = get_resource_manager().extract_frames(
                    self.video_path,
                    sampling_start,
                    sampling_end,
                    sample_fps,
                )
            except Exception as e:
                logger.warning(f"ResourceManager extract_frames failed: {e}, using local cap")
                raw_frames = []
        
        # 如果 ResourceManager 未启用或失败，使用本地 cap
        if not raw_frames and self.cap:
             # 回退逻辑 (不推荐)
            interval = 1.0 / sample_fps
            frame_step = 1.0 / self.fps if self.fps > 0 else interval
            eps_sec = max(1e-6, interval * 0.01)
            safe_end = sampling_end

            # 尾帧保护：避免请求 duration 精确边界导致 OpenCV 读帧失败。
            duration = float(self.duration_sec) if self.duration_sec > 0 else 0.0
            if duration > 0:
                tail_guard = max(frame_step * 0.5, eps_sec)
                safe_end = min(safe_end, max(sampling_start, duration - tail_guard))

            sample_count = int(np.floor((safe_end - sampling_start) / interval + eps_sec)) + 1
            for idx in range(max(0, sample_count)):
                # 使用 idx 计算时间点，避免 t += interval 的浮点累积误差。
                t = round(min(sampling_start + idx * interval, safe_end), 6)
                try:
                    if not isinstance(self.cap, cv2.VideoCapture):
                        logger.error(f"self.cap is not cv2.VideoCapture: {type(self.cap)}")
                        break
                    
                    if not self.cap.isOpened():
                        logger.warning(f"self.cap is not opened: {self.video_path}")
                        break

                    self.cap.set(cv2.CAP_PROP_POS_MSEC, t * 1000)
                    ret, frame = self.cap.read()
                    if ret and frame is not None:
                        raw_frames.append((t, frame))
                    else:
                        if duration > 0 and t >= (duration - max(interval, frame_step)):
                            logger.debug(
                                f"Skip unreadable tail frame at {t:.3f}s (duration={duration:.3f}s)"
                            )
                            break
                        logger.warning(f"Failed to read frame at {t:.3f}s")
                except Exception as e:
                    logger.error(f"Error reading frame at {t:.3f}s: {e}, cap={self.cap}")
                    break

        # 🚀 统一缩放帧
        return [(t, self._resize_frame(f)) for t, f in raw_frames]
    
    # =========================================================================
    # 措施4: 状态判定轻量校验
    # =========================================================================
    
    def _light_stable_check(self, frame: np.ndarray, prev_frame: np.ndarray,
                            roi: Tuple[int, int, int, int]) -> bool:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：diff_ratio > CVConfig.TH_DIFF_RATIO
        依据来源（证据链）：
        输入参数：
        - frame: 函数入参（类型：np.ndarray）。
        - prev_frame: 函数入参（类型：np.ndarray）。
        - roi: 函数入参（类型：Tuple[int, int, int, int]）。
        输出参数：
        - 布尔判断结果。"""
        x1, y1, x2, y2 = roi
        w, h = x2 - x1, y2 - y1
        
        # 5个采样点: 4角 + 中心
        points = [
            (x1, y1, x1 + w//4, y1 + h//4),           # 左上
            (x2 - w//4, y1, x2, y1 + h//4),           # 右上
            (x1, y2 - h//4, x1 + w//4, y2),           # 左下
            (x2 - w//4, y2 - h//4, x2, y2),           # 右下
            (x1 + w//4, y1 + h//4, x2 - w//4, y2 - h//4)  # 中心
        ]
        
        for px1, py1, px2, py2 in points:
            diff_ratio = self._calculate_diff_ratio_roi(
                frame, prev_frame, (px1, py1, px2, py2))
            if diff_ratio > CVConfig.TH_DIFF_RATIO:
                return False
        
        return True
    
    # =========================================================================
    # V6.9.4: 边缘差分累积检测 (检测平移类动画)
    # =========================================================================
    

    


    
    def _should_trigger_edge_detection(self, ssim: float, diff_ratio: float) -> bool:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - ssim: 函数入参（类型：float）。
        - diff_ratio: 函数入参（类型：float）。
        输出参数：
        - 布尔判断结果。"""
        ssim_drop = 1.0 - ssim
        return (
            diff_ratio < 0.01 and  # diff_ratio < 1% (等效MSE极低)
            ssim_drop < CVConfig.EDGE_DETECT_TRIGGER_SSIM_DROP
        )

    # =========================================================================
    # V8.0: 动作边界细化 (复用 VideoClipExtractor 核心逻辑)
    # =========================================================================
    
    def _calculate_mse(self, frame1: np.ndarray, frame2: np.ndarray, 
                       roi: Tuple[int, int, int, int] = None) -> float:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：roi
        - 条件：len(f1.shape) == 3
        - 条件：len(f2.shape) == 3
        依据来源（证据链）：
        - 输入参数：roi。
        输入参数：
        - frame1: 函数入参（类型：np.ndarray）。
        - frame2: 函数入参（类型：np.ndarray）。
        - roi: 函数入参（类型：Tuple[int, int, int, int]）。
        输出参数：
        - 数值型计算结果。"""
        if roi:
            x1, y1, x2, y2 = roi
            f1 = frame1[y1:y2, x1:x2]
            f2 = frame2[y1:y2, x1:x2]
        else:
            f1, f2 = frame1, frame2
        
        # 转灰度
        if len(f1.shape) == 3:
            f1 = cv2.cvtColor(f1, cv2.COLOR_BGR2GRAY)
        if len(f2.shape) == 3:
            f2 = cv2.cvtColor(f2, cv2.COLOR_BGR2GRAY)
        
        diff = f1.astype(np.float32) - f2.astype(np.float32)
        mse = np.mean(diff ** 2)
        return mse
    
    def _refine_action_boundaries(self, action: ActionUnit, 
                                  roi: Tuple[int, int, int, int] = None) -> ActionUnit:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：len(frames) < 2
        - 条件：not mse_list
        - 条件：final_end <= final_start
        依据来源（证据链）：
        - 输入参数：action。
        输入参数：
        - action: 函数入参（类型：ActionUnit）。
        - roi: 函数入参（类型：Tuple[int, int, int, int]）。
        输出参数：
        - ActionUnit 对象（包含字段：start_sec, end_sec, avg_diff_ratio, action_type, ssim_drop, is_effective, knowledge_subtype, modality, has_internal_stable, knowledge_type, confidence, internal_stable_islands）。"""
        # 扩展扫描窗 (±2s)
        scan_start = max(0, action.start_sec - 2.0)
        scan_end = min(getattr(self, '_video_duration', action.end_sec + 10), action.end_sec + 2.0)
        
        # 采样帧 (2fps 足够)
        frames = self._sample_frames(scan_start, scan_end, fps=2.0)
        if len(frames) < 2:
            logger.debug(f"Boundary refinement skipped: insufficient frames")
            return action
        
        # 自适应 MSE 阈值
        mse_threshold = 80  # 默认阈值
        
        # 计算帧间 MSE
        mse_list = []
        for i in range(len(frames) - 1):
            _, f1 = frames[i]
            _, f2 = frames[i + 1]
            mse = self._calculate_mse(f1, f2, roi)
            mse_list.append((frames[i][0], mse, frames[i + 1][0]))
        
        if not mse_list:
            return action
        
        # 评分函数 (复用 VideoClipExtractor 逻辑)
        def calculate_anchor_score(mse_val: float, anchor_time: float, target_time: float) -> float:
            """
            执行逻辑：
            1) 准备必要上下文与参数。
            2) 执行核心处理并返回结果。
            实现方式：通过内部函数组合与条件判断实现。
            核心价值：封装逻辑单元，提升复用与可维护性。
            决策逻辑：
            - 条件：time_gap <= 1.5
            依据来源（证据链）：
            输入参数：
            - mse_val: 函数入参（类型：float）。
            - anchor_time: 函数入参（类型：float）。
            - target_time: 函数入参（类型：float）。
            输出参数：
            - 数值型计算结果。"""
            intensity = min(1.0, mse_val / mse_threshold)
            time_gap = abs(anchor_time - target_time)
            zone_weight = 1.0 if time_gap <= 1.5 else 0.7
            temporal_similarity = max(0.0, 1.0 - (time_gap / 5.0))
            return (0.7 * intensity + 0.3 * temporal_similarity) * zone_weight
        
        # 找起始锚点: 原始 start_sec 附近最高得分的 MSE 跳变
        best_start = action.start_sec
        max_start_score = -1.0
        for t, mse, _ in mse_list:
            if mse > mse_threshold:
                score = calculate_anchor_score(mse, t, action.start_sec)
                if score > max_start_score:
                    max_start_score = score
                    best_start = t
        
        # 找终止锚点: 原始 end_sec 附近最高得分的 MSE 跳变
        best_end = action.end_sec
        max_end_score = -1.0
        for t, mse, t_next in reversed(mse_list):
            if mse > mse_threshold * 0.8:  # 终止点阈值稍低
                score = calculate_anchor_score(mse, t_next, action.end_sec)
                if score > max_end_score:
                    max_end_score = score
                    best_end = t_next
        
        # 第一性原理: 起始点严守，终止点取语义终点与物理跳变的并集
        final_start = best_start
        final_end = max(action.end_sec, best_end)
        
        # 边界保护: 避免倒置或过长
        if final_end <= final_start:
            final_end = final_start + max(1.0, action.duration_ms / 1000)
        if final_end > action.end_sec + 3.0:
            final_end = action.end_sec + 0.5  # 适度缓冲
        
        # 更新 ActionUnit
        original_start, original_end = action.start_sec, action.end_sec
        action.start_sec = final_start
        action.end_sec = final_end
        
        if abs(original_start - final_start) > 0.1 or abs(original_end - final_end) > 0.1:
            logger.debug(f"Refined action boundaries: [{original_start:.2f}s-{original_end:.2f}s] → "
                        f"[{final_start:.2f}s-{final_end:.2f}s]")
        
        return action

    # =========================================================================
    # V7.0: 模态分类辅助方法
    # =========================================================================
    
    def _has_internal_stable_islands(self, action: ActionUnit, 
                                     all_stable_islands: List[StableIsland],
                                     min_duration_ms: float = 500.0) -> bool:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：has_internal
        - 条件：stable.start_sec > action.start_sec + 0.1 and stable.end_sec < action.end_sec - 0.1 and (stable.duration_ms >= min_duration_ms)
        依据来源（证据链）：
        - 输入参数：action, min_duration_ms。
        输入参数：
        - action: 函数入参（类型：ActionUnit）。
        - all_stable_islands: 函数入参（类型：List[StableIsland]）。
        - min_duration_ms: 函数入参（类型：float）。
        输出参数：
        - 布尔判断结果。"""
        internal_islands = []
        for stable in all_stable_islands:
            # 检查是否为内部稳定岛 (排除首尾衔接)
            if (stable.start_sec > action.start_sec + 0.1 and  # 排除首部
                stable.end_sec < action.end_sec - 0.1 and      # 排除尾部
                stable.duration_ms >= min_duration_ms):
                internal_islands.append(stable)
        
        has_internal = len(internal_islands) >= 1
        if has_internal:
            logger.debug(f"Found {len(internal_islands)} internal stable islands in action [{action.start_sec:.1f}s-{action.end_sec:.1f}s]")
        
        return has_internal
    
    # =========================================================================
    # V7.2: 呈现型动态检测 (淡入/渐显/弹出)
    # =========================================================================
    
    def _is_presentation_dynamic(self, action: ActionUnit,
                                  frames: List[Tuple[float, np.ndarray]],
                                  roi: Tuple[int, int, int, int],
                                  all_stable_islands: List[StableIsland]) -> bool:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：len(frames) < 3
        - 条件：len(action_frames) < 3
        - 条件：len(action_frames) > 8
        依据来源（证据链）：
        - 输入参数：action, frames, roi。
        - 对象内部状态：self._has_creation_features, self._is_monotonic_smooth。
        输入参数：
        - action: 函数入参（类型：ActionUnit）。
        - frames: 数据列表/集合（类型：List[Tuple[float, np.ndarray]]）。
        - roi: 函数入参（类型：Tuple[int, int, int, int]）。
        - all_stable_islands: 函数入参（类型：List[StableIsland]）。
        输出参数：
        - 布尔判断结果。"""
        if len(frames) < 3:
            return False
        
        x1, y1, x2, y2 = roi
        
        # 提取动作区间内的帧
        action_frames = [(t, f) for t, f in frames 
                         if action.start_sec <= t <= action.end_sec]
        
        if len(action_frames) < 3:
            return False
        
        # 🚀 性能优化: 降采样 (提取动作区间帧后，最多保留 8 帧用于判断)
        if len(action_frames) > 8:
            step = len(action_frames) // 8
            analysis_frames = action_frames[::step][:8]
        else:
            analysis_frames = action_frames

        
        # ============ 规则1: 空间分散度检测 (最快，先做) ============
        # 计算变化像素的空间占比 (呈现型 > 0.75)
        spatial_spread = self._calculate_spatial_spread(analysis_frames, roi)
        if spatial_spread < 0.75:
            logger.debug(f"Presentation check failed: spatial_spread={spatial_spread:.2f} < 0.75")
            return False

        # ============ V7.3 反制层: 创作型强特征检测 (较重，后做) ============
        # 如果检测到创作型特征，直接排除呈现型判定
        # 防止手写/绘制/操作被误判为淡入淡出
        if self._has_creation_features(analysis_frames, roi):
            logger.debug(f"Presentation check blocked: creation features detected")
            return False
        
        # ============ 规则2: diff_ratio单调性检测 ============
        # 呈现型: 单调上升或下降; 创作型: 有波动
        diff_ratios = []
        prev_frame = None
        for t, frame in analysis_frames:
            if prev_frame is not None:
                diff = self._calculate_diff_ratio_roi(prev_frame, frame, roi)
                diff_ratios.append(diff)
            prev_frame = frame
        
        if not self._is_monotonic_smooth(diff_ratios, tolerance=0.02):
            logger.debug(f"Presentation check failed: diff_ratio not monotonic")
            return False
        
        # ============ 规则3: 首尾帧内容IOU检测 ============
        # 呈现型: 首尾内容一致 (仅可见度变化)
        first_frame = analysis_frames[0][1]
        last_frame = analysis_frames[-1][1]
        content_iou = self._calculate_frame_content_iou(first_frame, last_frame, roi)
        
        if content_iou < 0.90:
            logger.debug(f"Presentation check failed: content_iou={content_iou:.2f} < 0.90")
            return False
        
        # ============ 规则4: 动作后稳定岛检测 ============
        # 呈现完成后应立即进入稳定状态
        has_post_stable = False
        for stable in all_stable_islands:
            if stable.start_sec >= action.end_sec - 0.1 and stable.duration_ms >= 500:
                has_post_stable = True
                break
        
        if not has_post_stable:
            logger.debug(f"Presentation check failed: no stable island after action end")
            return False
        
        logger.debug(f"Detected PRESENTATION dynamic: spatial={spatial_spread:.2f}, "
                    f"content_iou={content_iou:.2f}")
        return True
    
    def _calculate_spatial_spread(self, frames: List[Tuple[float, np.ndarray]],
                                   roi: Tuple[int, int, int, int]) -> float:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：len(frames) < 2
        - 条件：roi_area == 0
        - 条件：prev_frame is not None
        依据来源（证据链）：
        - 输入参数：frames。
        输入参数：
        - frames: 数据列表/集合（类型：List[Tuple[float, np.ndarray]]）。
        - roi: 函数入参（类型：Tuple[int, int, int, int]）。
        输出参数：
        - 数值型计算结果。"""
        if len(frames) < 2:
            return 0.0
        
        x1, y1, x2, y2 = roi
        total_changed_pixels = 0
        roi_area = (x2 - x1) * (y2 - y1)
        
        if roi_area == 0:
            return 0.0
        
        prev_frame = None
        for t, frame in frames:
            if prev_frame is not None:
                gray_curr = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY) if len(frame.shape) == 3 else frame
                gray_prev = cv2.cvtColor(prev_frame, cv2.COLOR_BGR2GRAY) if len(prev_frame.shape) == 3 else prev_frame
                
                roi_curr = gray_curr[y1:y2, x1:x2]
                roi_prev = gray_prev[y1:y2, x1:x2]
                
                diff = cv2.absdiff(roi_curr, roi_prev)
                changed = np.sum(diff > 10)  # 阈值10排除噪点
                ratio = changed / roi_area
                total_changed_pixels = max(total_changed_pixels, ratio)
            
            prev_frame = frame
        
        return total_changed_pixels
    
    def _is_monotonic_smooth(self, series: List[float], tolerance: float = 0.02) -> bool:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：len(series) < 2
        - 条件：len(arr) > 0
        依据来源（证据链）：
        - 输入参数：series。
        输入参数：
        - series: 函数入参（类型：List[float]）。
        - tolerance: 函数入参（类型：float）。
        输出参数：
        - 布尔判断结果。"""
        if len(series) < 2:
            return True
        
        # 去极值
        arr = np.array(series)
        arr = np.clip(arr, 0, np.percentile(arr, 95) if len(arr) > 0 else 1)
        
        # 计算一阶差分
        diffs = np.diff(arr)
        
        # 允许小波动
        diffs[np.abs(diffs) <= tolerance] = 0
        
        # 检查单调性
        is_up = np.all(diffs >= 0)
        is_down = np.all(diffs <= 0)
        
        return is_up or is_down
    
    def _calculate_frame_content_iou(self, frame1: np.ndarray, frame2: np.ndarray,
                                      roi: Tuple[int, int, int, int]) -> float:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：len(frame1.shape) == 3
        - 条件：len(frame2.shape) == 3
        - 条件：union > 0
        依据来源（证据链）：
        - 输入参数：frame1, frame2。
        输入参数：
        - frame1: 函数入参（类型：np.ndarray）。
        - frame2: 函数入参（类型：np.ndarray）。
        - roi: 函数入参（类型：Tuple[int, int, int, int]）。
        输出参数：
        - 数值型计算结果。"""
        x1, y1, x2, y2 = roi
        
        gray1 = cv2.cvtColor(frame1, cv2.COLOR_BGR2GRAY) if len(frame1.shape) == 3 else frame1
        gray2 = cv2.cvtColor(frame2, cv2.COLOR_BGR2GRAY) if len(frame2.shape) == 3 else frame2
        
        roi1 = gray1[y1:y2, x1:x2]
        roi2 = gray2[y1:y2, x1:x2]
        
        # 二值化 (低阈值，捕捉内容轮廓)
        _, bin1 = cv2.threshold(roi1, 50, 255, cv2.THRESH_BINARY)
        _, bin2 = cv2.threshold(roi2, 50, 255, cv2.THRESH_BINARY)
        
        # 计算IOU
        intersection = np.logical_and(bin1, bin2).sum()
        union = np.logical_or(bin1, bin2).sum()
        
        return intersection / union if union > 0 else 0.0
    
    
    # =========================================================================
    # V7.3: 创作型强特征检测 (伪阳性规避)
    # =========================================================================
    
    def _has_creation_features(self, frames: List[Tuple[float, np.ndarray]],
                                roi: Tuple[int, int, int, int]) -> bool:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：len(frames) < 3
        - 条件：len(frames) > 10
        - 条件：has_local_increment
        依据来源（证据链）：
        - 输入参数：frames。
        输入参数：
        - frames: 数据列表/集合（类型：List[Tuple[float, np.ndarray]]）。
        - roi: 函数入参（类型：Tuple[int, int, int, int]）。
        输出参数：
        - 布尔判断结果。"""
        if len(frames) < 3:
            return False
        
        x1, y1, x2, y2 = roi
        
        # 🚀 性能优化: 降采样 (最多处理 10 帧)
        if len(frames) > 10:
            step = len(frames) // 10
            frames = frames[::step][:10]
        
        # 特征1: 检测局部集中变化 (创作型特征)
        has_local_increment = self._detect_local_pixel_increment(frames, roi)
        if has_local_increment:
            logger.debug("Creation feature detected: local pixel increment")
            return True
        
        # 特征2: 检测非均匀变化模式 (轨迹/笔迹)
        has_trace_pattern = self._detect_trace_pattern(frames, roi)
        if has_trace_pattern:
            logger.debug("Creation feature detected: trace pattern")
            return True
        
        # 特征3: 检测内容边界扩展
        has_boundary_expansion = self._detect_boundary_expansion(frames, roi)
        if has_boundary_expansion:
            logger.debug("Creation feature detected: boundary expansion")
            return True
        
        return False
    
    def _detect_local_pixel_increment(self, frames: List[Tuple[float, np.ndarray]],
                                       roi: Tuple[int, int, int, int]) -> bool:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：grid_h < 10 or grid_w < 10
        - 条件：total_change == 0
        - 条件：prev_frame is not None
        依据来源（证据链）：
        输入参数：
        - frames: 数据列表/集合（类型：List[Tuple[float, np.ndarray]]）。
        - roi: 函数入参（类型：Tuple[int, int, int, int]）。
        输出参数：
        - 布尔判断结果。"""
        x1, y1, x2, y2 = roi
        grid_h = (y2 - y1) // 4
        grid_w = (x2 - x1) // 4
        
        if grid_h < 10 or grid_w < 10:
            return False
        
        # 统计每个网格的累计变化
        grid_changes = np.zeros((4, 4))
        
        prev_frame = None
        for t, frame in frames:
            if prev_frame is not None:
                gray_curr = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY) if len(frame.shape) == 3 else frame
                gray_prev = cv2.cvtColor(prev_frame, cv2.COLOR_BGR2GRAY) if len(prev_frame.shape) == 3 else prev_frame
                
                for i in range(4):
                    for j in range(4):
                        gy1, gy2 = y1 + i * grid_h, y1 + (i + 1) * grid_h
                        gx1, gx2 = x1 + j * grid_w, x1 + (j + 1) * grid_w
                        
                        grid_curr = gray_curr[gy1:gy2, gx1:gx2]
                        grid_prev = gray_prev[gy1:gy2, gx1:gx2]
                        
                        diff = cv2.absdiff(grid_curr, grid_prev)
                        grid_changes[i, j] += np.sum(diff > 15)
            
            prev_frame = frame
        
        # 检查变化是否集中 (超过50%变化集中在<25%的网格)
        total_change = grid_changes.sum()
        if total_change == 0:
            return False
        
        sorted_changes = np.sort(grid_changes.flatten())[::-1]
        top_4_ratio = sorted_changes[:4].sum() / total_change  # 前4个网格占比
        
        # 如果前4个网格(25%)包含超过60%的变化 → 局部集中 → 创作型
        return top_4_ratio > 0.6
    
    def _detect_trace_pattern(self, frames: List[Tuple[float, np.ndarray]],
                               roi: Tuple[int, int, int, int]) -> bool:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：prev_frame is not None
        - 条件：len(frame.shape) == 3
        - 条件：len(prev_frame.shape) == 3
        依据来源（证据链）：
        输入参数：
        - frames: 数据列表/集合（类型：List[Tuple[float, np.ndarray]]）。
        - roi: 函数入参（类型：Tuple[int, int, int, int]）。
        输出参数：
        - 布尔判断结果。"""
        x1, y1, x2, y2 = roi
        
        trace_count = 0
        prev_frame = None
        
        for t, frame in frames[-5:]:  # 只检查最后5帧 (本来就是采样的，这里取最后5个样本)
            if prev_frame is not None:
                gray_curr = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY) if len(frame.shape) == 3 else frame
                gray_prev = cv2.cvtColor(prev_frame, cv2.COLOR_BGR2GRAY) if len(prev_frame.shape) == 3 else prev_frame
                
                roi_curr = gray_curr[y1:y2, x1:x2]
                roi_prev = gray_prev[y1:y2, x1:x2]
                
                diff = cv2.absdiff(roi_curr, roi_prev)
                _, binary = cv2.threshold(diff, 20, 255, cv2.THRESH_BINARY)
                
                # 寻找连通区域
                contours, _ = cv2.findContours(binary, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
                
                # 检查是否有细长连通区域 (轨迹特征)
                for contour in contours:
                    if len(contour) > 10:
                        # 计算轮廓的细长度 (周长^2 / 面积)
                        area = cv2.contourArea(contour)
                        perimeter = cv2.arcLength(contour, True)
                        if area > 0 and perimeter > 0:
                            circularity = (perimeter ** 2) / (4 * np.pi * area)
                            # 细长轨迹的circularity > 3
                            if circularity > 3:
                                trace_count += 1
            
            prev_frame = frame
        
        # 如果检测到多个轨迹模式 → 创作型
        return trace_count >= 2
    
    def _detect_boundary_expansion(self, frames: List[Tuple[float, np.ndarray]],
                                    roi: Tuple[int, int, int, int]) -> bool:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：len(frames) < 2
        - 条件：first_pixels == 0
        - 条件：len(first_frame.shape) == 3
        依据来源（证据链）：
        - 输入参数：frames。
        输入参数：
        - frames: 数据列表/集合（类型：List[Tuple[float, np.ndarray]]）。
        - roi: 函数入参（类型：Tuple[int, int, int, int]）。
        输出参数：
        - 布尔判断结果。"""
        if len(frames) < 2:
            return False
        
        x1, y1, x2, y2 = roi
        
        first_frame = frames[0][1]
        last_frame = frames[-1][1]
        
        gray_first = cv2.cvtColor(first_frame, cv2.COLOR_BGR2GRAY) if len(first_frame.shape) == 3 else first_frame
        gray_last = cv2.cvtColor(last_frame, cv2.COLOR_BGR2GRAY) if len(last_frame.shape) == 3 else last_frame
        
        roi_first = gray_first[y1:y2, x1:x2]
        roi_last = gray_last[y1:y2, x1:x2]
        
        # 自适应二值化
        _, bin_first = cv2.threshold(roi_first, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        _, bin_last = cv2.threshold(roi_last, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        
        # 计算内容区域 (非零像素)
        first_pixels = np.sum(bin_first > 0)
        last_pixels = np.sum(bin_last > 0)
        
        if first_pixels == 0:
            # 首帧无内容，末帧有内容 → 明显的创作过程
            return last_pixels > (roi_first.shape[0] * roi_first.shape[1] * 0.01)
        
        # 如果末帧内容比首帧多20%以上 → 有新内容添加 → 创作型
        expansion_ratio = (last_pixels - first_pixels) / first_pixels if first_pixels > 0 else 0
        
        return expansion_ratio > 0.2

    def _classify_continuous_type(self, frames: List[Tuple[float, np.ndarray]],
                                   roi: Tuple[int, int, int, int]) -> str:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：len(frames) < 2
        - 条件：has_localized_change and (not has_traced_change)
        - 条件：prev_frame is None
        依据来源（证据链）：
        - 输入参数：frames。
        输入参数：
        - frames: 数据列表/集合（类型：List[Tuple[float, np.ndarray]]）。
        - roi: 函数入参（类型：Tuple[int, int, int, int]）。
        输出参数：
        - 字符串结果。"""
        if len(frames) < 2:
            return "ambiguous"
        
        x1, y1, x2, y2 = roi
        
        # 检测变化模式
        has_localized_change = False  # 局部定点变化 → K4特征
        has_traced_change = False     # 轨迹式变化 → K3特征
        
        prev_frame = None
        change_regions = []
        
        for t, frame in frames[-5:]:  # 只分析最后几帧
            if prev_frame is None:
                prev_frame = frame
                continue
            
            # 计算变化区域
            gray_curr = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY) if len(frame.shape) == 3 else frame
            gray_prev = cv2.cvtColor(prev_frame, cv2.COLOR_BGR2GRAY) if len(prev_frame.shape) == 3 else prev_frame
            
            roi_curr = gray_curr[y1:y2, x1:x2]
            roi_prev = gray_prev[y1:y2, x1:x2]
            
            diff = cv2.absdiff(roi_curr, roi_prev)
            _, thresh = cv2.threshold(diff, 30, 255, cv2.THRESH_BINARY)
            
            # 找到变化区域的轮廓
            contours, _ = cv2.findContours(thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
            
            if contours:
                # 检查变化区域的特征
                total_area = sum(cv2.contourArea(c) for c in contours)
                roi_area = (x2 - x1) * (y2 - y1)
                change_ratio = total_area / roi_area if roi_area > 0 else 0
                
                if len(contours) <= 3 and change_ratio < 0.1:
                    # 少量小区域变化 → 可能是UI点击
                    has_localized_change = True
                elif len(contours) > 5 or change_ratio > 0.15:
                    # 大量分散变化 → 可能是书写轨迹
                    has_traced_change = True
            
            prev_frame = frame
        
        # 决策
        if has_localized_change and not has_traced_change:
            return "operation"   # K4
        elif has_traced_change:
            return "derivation"  # K3
        else:
            return "derivation"  # 默认K3 (保守策略: 视频+截图)
    
    def _extract_key_screenshot_times(self, action: ActionUnit,
                                       diff_ratios: List[Tuple[float, float]] = None) -> List[float]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：diff_ratios
        - 条件：inflection_time is None
        - 条件：action.start_sec < t < action.end_sec and ratio > max_ratio
        依据来源（证据链）：
        - 输入参数：action, diff_ratios。
        输入参数：
        - action: 函数入参（类型：ActionUnit）。
        - diff_ratios: 函数入参（类型：List[Tuple[float, float]]）。
        输出参数：
        - float 列表（与输入或处理结果一一对应）。"""
        start_time = action.start_sec
        end_time = action.end_sec
        
        # 查找视觉拐点
        inflection_time = None
        if diff_ratios:
            # 找diff_ratio的峰值时间
            max_ratio = 0
            for t, ratio in diff_ratios:
                if action.start_sec < t < action.end_sec and ratio > max_ratio:
                    max_ratio = ratio
                    inflection_time = t
        
        # 无明显拐点则用时间中点
        if inflection_time is None:
            inflection_time = (start_time + end_time) / 2
        
        return [start_time, inflection_time, end_time]


    # 核心检测: 稳定岛/动作单元/冗余
    # =========================================================================

    
    def detect_visual_states(self, start_sec: float, end_sec: float,
                              stable_only: bool = False
                              ) -> Tuple[List[StableIsland], List[ActionUnit], List[RedundancySegment]]:
        """??? `cv_state_analysis`??????????"""
        return detect_visual_states(self, start_sec, end_sec, stable_only=stable_only)



    
    def _merge_state_intervals(self, states: List[Tuple[float, FrameState, float]],
                                start_sec: float, end_sec: float,
                                frames: List[Tuple[float, np.ndarray]] = None,
                                roi: Tuple[int, int, int, int] = None,
                                stable_only: bool = False
                                ) -> Tuple[List[StableIsland], List[ActionUnit], List[RedundancySegment]]:
        """??? `cv_state_analysis`??????????"""
        return merge_state_intervals(
            self,
            states,
            start_sec,
            end_sec,
            frames=frames,
            roi=roi,
            stable_only=stable_only,
        )


    # =========================================================================
    # V9.0: 两阶段动作单元合并 (新架构)
    # =========================================================================
    
    def _merge_action_units_stage1(
        self, 
        action_units: List[ActionUnit],
        all_stable_islands: List[StableIsland]
    ) -> Tuple[List[ActionUnit], List[StableIsland]]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：len(action_units) <= 1
        - 条件：len(merged) < len(action_units)
        - 条件：gap < MERGE_GAP_THRESHOLD
        依据来源（证据链）：
        - 输入参数：action_units。
        - 阈值常量：MERGE_GAP_THRESHOLD。
        输入参数：
        - action_units: 函数入参（类型：List[ActionUnit]）。
        - all_stable_islands: 函数入参（类型：List[StableIsland]）。
        输出参数：
        - List[ActionUnit], List[StableIsland] 列表（与输入或处理结果一一对应）。"""
        if len(action_units) <= 1:
            return action_units, []
        
        MERGE_GAP_THRESHOLD = 1.0  # 第一阶段：1秒
        
        merged = []
        crossed_islands = []
        current = action_units[0]
        
        for next_unit in action_units[1:]:
            gap = next_unit.start_sec - current.end_sec
            
            if gap < MERGE_GAP_THRESHOLD:
                # 记录被跨越的稳定岛
                gap_islands = self._get_stable_islands_in_range(
                    current.end_sec, next_unit.start_sec, all_stable_islands
                )
                crossed_islands.extend(gap_islands)
                
                # 合并动作单元
                current = ActionUnit(
                    start_sec=current.start_sec,
                    end_sec=next_unit.end_sec,
                    avg_diff_ratio=max(current.avg_diff_ratio, next_unit.avg_diff_ratio),
                    ssim_drop=max(current.ssim_drop, next_unit.ssim_drop),
                    action_type=current.action_type,
                    is_effective=current.is_effective or next_unit.is_effective,
                    has_internal_stable=current.has_internal_stable or next_unit.has_internal_stable,
                    modality=current.modality,
                    knowledge_subtype=current.knowledge_subtype
                )
                logger.debug(f"Stage1 merge: gap={gap:.2f}s → [{current.start_sec:.1f}s-{current.end_sec:.1f}s]")
            else:
                merged.append(current)
                current = next_unit
        
        merged.append(current)
        
        if len(merged) < len(action_units):
            logger.info(f"Stage1 merge: {len(action_units)} → {len(merged)} actions, "
                       f"crossed {len(crossed_islands)} stable islands")
        
        return merged, crossed_islands
    
    def _merge_action_units_stage2(
        self, 
        action_units: List[ActionUnit],
        all_stable_islands: List[StableIsland],
        semantic_unit_id: str = ""
    ) -> Tuple[List[ActionUnit], List[StableIsland]]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：len(action_units) <= 1
        - 条件：len(merged) < len(action_units)
        - 条件：gap < MERGE_GAP_THRESHOLD and same_type
        依据来源（证据链）：
        - 输入参数：action_units。
        - 阈值常量：MERGE_GAP_THRESHOLD。
        输入参数：
        - action_units: 函数入参（类型：List[ActionUnit]）。
        - all_stable_islands: 函数入参（类型：List[StableIsland]）。
        - semantic_unit_id: 标识符（类型：str）。
        输出参数：
        - List[ActionUnit], List[StableIsland] 列表（与输入或处理结果一一对应）。"""
        if len(action_units) <= 1:
            return action_units, []
        
        MERGE_GAP_THRESHOLD = 5.0  # 第二阶段：5秒
        
        merged = []
        crossed_islands = []
        current = action_units[0]
        
        for next_unit in action_units[1:]:
            gap = next_unit.start_sec - current.end_sec
            
            # 只有 knowledge_type 相同且间隔 < 5s 才合并
            same_type = getattr(current, 'knowledge_type', '') == getattr(next_unit, 'knowledge_type', '')
            
            if gap < MERGE_GAP_THRESHOLD and same_type:
                # 记录被跨越的稳定岛
                gap_islands = self._get_stable_islands_in_range(
                    current.end_sec, next_unit.start_sec, all_stable_islands
                )
                crossed_islands.extend(gap_islands)
                
                # 合并动作单元
                current = ActionUnit(
                    start_sec=current.start_sec,
                    end_sec=next_unit.end_sec,
                    avg_diff_ratio=max(current.avg_diff_ratio, next_unit.avg_diff_ratio),
                    ssim_drop=max(current.ssim_drop, next_unit.ssim_drop),
                    action_type=current.action_type,
                    is_effective=True,
                    has_internal_stable=current.has_internal_stable or next_unit.has_internal_stable,
                    modality=current.modality,
                    knowledge_subtype=current.knowledge_subtype
                )
                # 保留 knowledge_type
                if hasattr(current, 'knowledge_type'):
                    current.knowledge_type = getattr(action_units[0], 'knowledge_type', '')
                    
                logger.debug(f"Stage2 merge [{semantic_unit_id}]: gap={gap:.2f}s, type={getattr(current, 'knowledge_type', 'unknown')} → "
                           f"[{current.start_sec:.1f}s-{current.end_sec:.1f}s]")
            else:
                merged.append(current)
                current = next_unit
        
        merged.append(current)
        
        if len(merged) < len(action_units):
            logger.info(f"Stage2 merge [{semantic_unit_id}]: {len(action_units)} → {len(merged)} actions, "
                       f"crossed {len(crossed_islands)} stable islands")
        
        return merged, crossed_islands
    
    def _get_stable_islands_in_range(
        self, 
        start_sec: float, 
        end_sec: float,
        stable_islands: List[StableIsland]
    ) -> List[StableIsland]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - start_sec: 起止时间/区间边界（类型：float）。
        - end_sec: 起止时间/区间边界（类型：float）。
        - stable_islands: 函数入参（类型：List[StableIsland]）。
        输出参数：
        - StableIsland 列表（与输入或处理结果一一对应）。"""
        return [
            island for island in stable_islands
            if island.end_sec > start_sec and island.start_sec < end_sec
        ]
    
    def _collect_all_stable_islands(
        self,
        action_units: List[ActionUnit],
        external_stable_islands: List[StableIsland],
        crossed_islands_stage1: List[StableIsland],
        crossed_islands_stage2: List[StableIsland]
    ) -> List[StableIsland]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：key not in seen_times
        - 条件：hasattr(unit, 'internal_stable_islands') and unit.internal_stable_islands
        依据来源（证据链）：
        输入参数：
        - action_units: 函数入参（类型：List[ActionUnit]）。
        - external_stable_islands: 函数入参（类型：List[StableIsland]）。
        - crossed_islands_stage1: 函数入参（类型：List[StableIsland]）。
        - crossed_islands_stage2: 函数入参（类型：List[StableIsland]）。
        输出参数：
        - StableIsland 列表（与输入或处理结果一一对应）。
        补充说明：
        收集所有稳定岛用于截图提取：
        1. ActionUnit 内部的稳定岛
        2. ActionUnit 外部的稳定岛
        3. 两次合并中被跨越的稳定岛"""
        all_islands = []
        seen_times = set()  # 去重
        
        def add_island(island: StableIsland):
            """
            执行逻辑：
            1) 准备必要上下文与参数。
            2) 执行核心处理并返回结果。
            实现方式：通过内部函数组合与条件判断实现。
            核心价值：封装逻辑单元，提升复用与可维护性。
            决策逻辑：
            - 条件：key not in seen_times
            依据来源（证据链）：
            输入参数：
            - island: 函数入参（类型：StableIsland）。
            输出参数：
            - 无（仅产生副作用，如日志/写盘/状态更新）。"""
            key = (round(island.start_sec, 2), round(island.end_sec, 2))
            if key not in seen_times:
                seen_times.add(key)
                all_islands.append(island)
        
        # 1. 内部稳定岛（从 ActionUnit 的 internal_stable_islands 属性）
        for unit in action_units:
            if hasattr(unit, 'internal_stable_islands') and unit.internal_stable_islands:
                for island in unit.internal_stable_islands:
                    add_island(island)
        
        # 2. 外部稳定岛
        for island in external_stable_islands:
            add_island(island)
        
        # 3. 被跨越的稳定岛
        for island in crossed_islands_stage1:
            add_island(island)
        for island in crossed_islands_stage2:
            add_island(island)
        
        # 按时间排序
        all_islands.sort(key=lambda x: x.start_sec)
        
        logger.debug(f"Collected {len(all_islands)} stable islands for screenshot extraction")
        return all_islands

    
    # =========================================================================
    # 视觉知识类型识别
    # =========================================================================
    
    def classify_visual_knowledge_type(self, 
                                        stable_islands: List[StableIsland],
                                        action_units: List[ActionUnit],
                                        redundancy_segments: List[RedundancySegment],
                                        total_duration: float
                                        ) -> Tuple[VisualKnowledgeType, VisionStats]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：total_duration <= 0
        - 条件：accounted < total_duration
        - 条件：max_p >= CVConfig.TH_ABSOLUTE_LEAD
        依据来源（证据链）：
        - 输入参数：total_duration。
        输入参数：
        - stable_islands: 函数入参（类型：List[StableIsland]）。
        - action_units: 函数入参（类型：List[ActionUnit]）。
        - redundancy_segments: 函数入参（类型：List[RedundancySegment]）。
        - total_duration: 函数入参（类型：float）。
        输出参数：
        - 多值结果元组（各元素含义见实现）。"""
        if total_duration <= 0:
            return VisualKnowledgeType.MIXED, VisionStats()
        
        # 计算各类区间占比
        stable_duration = sum(s.duration_ms for s in stable_islands) / 1000
        action_duration = sum(a.duration_ms for a in action_units) / 1000
        redundant_duration = sum(r.duration_ms for r in redundancy_segments) / 1000
        
        # 补全到total_duration
        accounted = stable_duration + action_duration + redundant_duration
        if accounted < total_duration:
            # 未分类区间归为抽象 (无明显视觉特征)
            abstract_duration = total_duration - accounted
        else:
            abstract_duration = 0.0
        
        # 三类互斥占比
        s_stable = stable_duration / total_duration
        s_action = action_duration / total_duration
        s_redundant = redundant_duration / total_duration
        
        # 知识类型占比映射
        # 具象 ← 稳定岛
        # 过程 ← 动作单元  
        # 抽象 ← 未分类区间
        p_abstract = abstract_duration / total_duration
        p_concrete = s_stable
        p_process = s_action
        
        stats = VisionStats(
            s_stable=s_stable,
            s_action=s_action,
            s_redundant=s_redundant,
            p_abstract=p_abstract,
            p_concrete=p_concrete,
            p_process=p_process
        )
        
        # 三级阶梯判定
        ratios = {'abstract': p_abstract, 'concrete': p_concrete, 'process': p_process}
        sorted_ratios = sorted(ratios.items(), key=lambda x: x[1], reverse=True)
        max_type, max_p = sorted_ratios[0]
        second_p = sorted_ratios[1][1]
        
        # Step 1: 绝对主导
        if max_p >= CVConfig.TH_ABSOLUTE_LEAD:
            return VisualKnowledgeType(max_type), stats
        
        # Step 2: 相对主导
        if max_p >= CVConfig.TH_RELATIVE_LEAD and (max_p - second_p) >= CVConfig.TH_RELATIVE_DIFF:
            return VisualKnowledgeType(max_type), stats
        
        # Step 3: 混杂
        return VisualKnowledgeType.MIXED, stats
    
    # =========================================================================
    # 批量校验 (措施6 + 方案B)
    # =========================================================================
    
    def validate_batch(self, units: List[Dict[str, Any]]) -> List[CVValidationResult]:
        """
        执行逻辑：
        1) 整理待校验数据。
        2) 按规则逐项校验并返回结果。
        实现方式：通过内部方法调用/状态更新、线程池并发实现。
        核心价值：提前发现数据/状态问题，降低运行风险。
        决策逻辑：
        - 条件：not units
        依据来源（证据链）：
        - 输入参数：units。
        输入参数：
        - units: 函数入参（类型：List[Dict[str, Any]]）。
        输出参数：
        - CVValidationResult 列表（与输入或处理结果一一对应）。"""
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        results: List[CVValidationResult] = []
        if not units:
            return results
            
        # 按 start_sec 排序，尽量保证缓存命中 (虽然并行可能会乱序，但ResourceManager可能有底层优化)
        sorted_units = sorted(units, key=lambda x: x.get("start_sec", 0.0))
        
        # 限制并发数 (避免过多 VideoCapture 竞争或内存爆炸)
        # 建议设置为 CPU 核心数 或 4-8
        max_workers = min(8, os.cpu_count() or 4)
        
        logger.info(f"Starting batch CV validation for {len(units)} units (workers={max_workers})")
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_unit = {
                executor.submit(
                    self.validate_single, 
                    unit.get("unit_id", ""), 
                    unit.get("start_sec", 0.0), 
                    unit.get("end_sec", 0.0),
                    unit.get("knowledge_type", "abstract")
                ): unit for unit in sorted_units
            }
            
            for future in as_completed(future_to_unit):
                unit = future_to_unit[future]
                unit_id = unit.get("unit_id", "")
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logger.error(f"CV validation failed for unit {unit_id}: {e}")
                    # 生成降级结果
                    results.append(CVValidationResult(
                        unit_id=unit_id,
                        timeline=(unit.get("start_sec", 0.0), unit.get("end_sec", 0.0)),
                        vision_stats=VisionStats(),
                        main_vision_type=VisualKnowledgeType.MIXED,
                        abnormal_type="validation_error",
                        abnormal_reason=str(e)
                    ))
        
        # 恢复原始顺序
        results_map = {r.unit_id: r for r in results}
        final_results = [results_map.get(u.get("unit_id", ""), None) for u in units]
        # 过滤 None (理论上不应发生)
        final_results = [r for r in final_results if r]
        
        logger.info(f"Batch CV validation completed in {time.time() - start_time:.2f}s")
        return final_results
    
    def validate_single(self, unit_id: str, start_sec: float, end_sec: float,
                         llm_type: str) -> CVValidationResult:
        """
        执行逻辑：
        1) 整理待校验数据。
        2) 按规则逐项校验并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：提前发现数据/状态问题，降低运行风险。
        决策逻辑：
        - 条件：not type_match
        - 条件：rs.duration_ms > 2000
        - 条件：abs(si.start_sec - start_sec) < epsilon or abs(si.end_sec - end_sec) < epsilon
        依据来源（证据链）：
        - 输入参数：end_sec, start_sec。
        输入参数：
        - unit_id: 标识符（类型：str）。
        - start_sec: 起止时间/区间边界（类型：float）。
        - end_sec: 起止时间/区间边界（类型：float）。
        - llm_type: 函数入参（类型：str）。
        输出参数：
        - CVValidationResult 对象（包含字段：unit_id, timeline, vision_stats, main_vision_type, stable_islands, action_units, redundancy_segments, vision_anchors, timeline_continuous, type_match, vision_unit_complete, abnormal_type, abnormal_timeline, abnormal_reason）。"""
        total_duration = end_sec - start_sec
        
        # 1. 检测三类区间
        stable_islands, action_units, redundancy_segments = \
            self.detect_visual_states(start_sec, end_sec)
        
        # 2. 视觉知识类型识别
        vision_type, stats = self.classify_visual_knowledge_type(
            stable_islands, action_units, redundancy_segments, total_duration)
        
        # 3. 提取视觉锚点
        anchors = []
        for si in stable_islands:
            anchors.extend([si.start_sec, si.end_sec])
        for au in action_units:
            anchors.extend([au.start_sec, au.end_sec])
        anchors = sorted(set(anchors))
        
        # 4. 跨模态一致性校验
        type_match = self._check_type_match(llm_type, vision_type, stats)
        
        # 5. 时序连续性校验 (简化: 检查是否有大段冗余断层)
        timeline_continuous = True
        for rs in redundancy_segments:
            if rs.duration_ms > 2000:  # 2秒以上冗余
                timeline_continuous = False
                break
        
        # 6. 视觉单元完整性校验
        vision_unit_complete = True
        # 检查稳定岛/动作单元是否被边界截断
        epsilon = 0.5  # 500ms容差
        for si in stable_islands:
            if abs(si.start_sec - start_sec) < epsilon or abs(si.end_sec - end_sec) < epsilon:
                if si.duration_ms < CVConfig.TH_STABLE_DURATION_MS * 1.5:
                    vision_unit_complete = False
                    break
        
        # 构建结果
        result = CVValidationResult(
            unit_id=unit_id,
            timeline=(start_sec, end_sec),
            vision_stats=stats,
            main_vision_type=vision_type,
            stable_islands=stable_islands,
            action_units=action_units,
            redundancy_segments=redundancy_segments,
            vision_anchors=anchors,
            timeline_continuous=timeline_continuous,
            type_match=type_match,
            vision_unit_complete=vision_unit_complete
        )
        
        # 异常信息
        if not type_match:
            result.abnormal_type = "类型不匹配"
            result.abnormal_reason = f"LLM={llm_type}, CV={vision_type.value}"
        elif not timeline_continuous:
            result.abnormal_type = "时序割裂"
            result.abnormal_reason = "存在>2s的冗余断层"
        elif not vision_unit_complete:
            result.abnormal_type = "视觉单元不完整"
            result.abnormal_reason = "稳定岛/动作单元被边界截断"
        
        return result
    
    def _check_type_match(self, llm_type: str, vision_type: VisualKnowledgeType,
                           stats: VisionStats) -> bool:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：vision_type == VisualKnowledgeType.MIXED
        - 条件：llm_type == 'abstract'
        - 条件：llm_type == 'concrete'
        依据来源（证据链）：
        - 输入参数：llm_type, vision_type。
        输入参数：
        - llm_type: 函数入参（类型：str）。
        - vision_type: 函数入参（类型：VisualKnowledgeType）。
        - stats: 函数入参（类型：VisionStats）。
        输出参数：
        - 布尔判断结果。"""
        if vision_type == VisualKnowledgeType.MIXED:
            # 联合裁决
            if llm_type == "abstract":
                return (stats.p_abstract + stats.p_concrete) >= CVConfig.TH_ABS_CON_SUM
            elif llm_type == "concrete":
                return stats.s_stable >= CVConfig.TH_STABLE_RATIO
            elif llm_type == "process":
                return stats.s_action >= CVConfig.TH_ACTION_RATIO
            return False
        
        return llm_type == vision_type.value
    
    # =========================================================================
    # 冲突包生成 (用于LLM重判)
    # =========================================================================
    
    def generate_conflict_packages(self, results: List[CVValidationResult]
                                    ) -> List[ConflictPackage]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：result.is_normal
        - 条件：abs(anchor - si.start_sec) < 0.1
        - 条件：abs(anchor - au.start_sec) < 0.1
        依据来源（证据链）：
        输入参数：
        - results: 函数入参（类型：List[CVValidationResult]）。
        输出参数：
        - ConflictPackage 列表（与输入或处理结果一一对应）。"""
        packages: List[ConflictPackage] = []
        
        for result in results:
            if result.is_normal:
                continue
            
            # 提取有效区间
            valid_timelines = []
            for si in result.stable_islands:
                valid_timelines.append((si.start_sec, si.end_sec))
            for au in result.action_units:
                valid_timelines.append((au.start_sec, au.end_sec))
            
            # 锚点类型
            anchor_types = []
            for anchor in result.vision_anchors:
                for si in result.stable_islands:
                    if abs(anchor - si.start_sec) < 0.1:
                        anchor_types.append("稳定岛起始")
                    elif abs(anchor - si.end_sec) < 0.1:
                        anchor_types.append("稳定岛结束")
                for au in result.action_units:
                    if abs(anchor - au.start_sec) < 0.1:
                        anchor_types.append("动作单元起始")
                    elif abs(anchor - au.end_sec) < 0.1:
                        anchor_types.append("动作单元结束")
            
            packages.append(ConflictPackage(
                conflict_unit_id=result.unit_id,
                conflict_timeline=result.timeline,
                vision_anchors=result.vision_anchors,
                vision_anchor_types=anchor_types,
                conflict_reason=result.abnormal_reason or "",
                vision_valid_timelines=valid_timelines
            ))
        
        return packages
    
    # =========================================================================
    # 输出序列化
    # =========================================================================
    
    def to_dict(self, result: CVValidationResult) -> Dict[str, Any]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - result: 函数入参（类型：CVValidationResult）。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        return {
            "unit_id": result.unit_id,
            "timeline": list(result.timeline),
            "vision_stats": {
                "s_stable": result.vision_stats.s_stable,
                "s_action": result.vision_stats.s_action,
                "s_redundant": result.vision_stats.s_redundant,
                "p_abstract": result.vision_stats.p_abstract,
                "p_concrete": result.vision_stats.p_concrete,
                "p_process": result.vision_stats.p_process
            },
            "main_vision_type": result.main_vision_type.value,
            "stable_islands": [
                {"start": s.start_sec, "end": s.end_sec, "ssim": s.avg_ssim}
                for s in result.stable_islands
            ],
            "action_units": [
                {"start": a.start_sec, "end": a.end_sec, "diff_ratio": a.avg_diff_ratio}
                for a in result.action_units
            ],
            "redundancy_segments": [
                {"start": r.start_sec, "end": r.end_sec, 
                 "type": r.redundancy_type.value, "action": r.processing_action}
                for r in result.redundancy_segments
            ],
            "vision_anchors": result.vision_anchors,
            "is_normal": result.is_normal,
            "abnormal_type": result.abnormal_type,
            "abnormal_reason": result.abnormal_reason
        }
