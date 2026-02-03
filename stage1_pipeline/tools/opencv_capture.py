"""
OpenCV 截帧工具
用于 Step 12-15
"""

import cv2
import numpy as np
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass


@dataclass
class FrameResult:
    """截帧结果"""
    frame_id: str
    timestamp: float
    frame_path: str
    width: int
    height: int
    brightness: float
    sharpness: float
    is_valid: bool
    invalid_reason: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None  # Added metadata field


class FrameCapture:
    """
    帧截取器
    
    支持：
    - 精确时间点截帧
    - 多帧采样
    - 图像增强
    - 质量校验
    """
    
    def __init__(
        self,
        video_path: str,
        output_dir: str = "temp_frames"
    ):
        self.video_path = video_path
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self._cap: Optional[cv2.VideoCapture] = None
        self._fps: float = 0
        self._frame_count: int = 0
        self._duration: float = 0
        self._last_frame_hash: Optional[int] = None # 用于检测重复帧
        self._last_frame_path: Optional[str] = None # 用于复用截图文件
        
    def open(self) -> bool:
        """打开视频"""
        self._cap = cv2.VideoCapture(self.video_path)
        if not self._cap.isOpened():
            return False
            
        self._fps = self._cap.get(cv2.CAP_PROP_FPS)
        self._frame_count = int(self._cap.get(cv2.CAP_PROP_FRAME_COUNT))
        self._duration = self._frame_count / self._fps if self._fps > 0 else 0
        
        return True
    
    def close(self):
        """关闭视频"""
        if self._cap:
            self._cap.release()
            self._cap = None
    
    def __enter__(self):
        self.open()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        
    @property
    def duration(self) -> float:
        return self._duration
    
    @property
    def fps(self) -> float:
        return self._fps

    def _seek_to_time(self, target_time: float) -> bool:
        """
        智能寻帧：
        1. 优先使用 grab() 推进（对于正向微调，速度快且稳）
        2. 大跨度或反向寻帧使用 set()
        """
        if not self._cap:
            self.open()
            
        target_ms = target_time * 1000
        pos_ms = self._cap.get(cv2.CAP_PROP_POS_MSEC)
        diff_ms = target_ms - pos_ms
        
        # 帧持续时间
        frame_time_ms = 1000 / self._fps if self._fps > 0 else 33
        
        # 情况A：正向小跨度（30帧以内，约1秒）
        # 使用 grab() 推进，避免 seek 导致的解码器刷新延迟或报错
        if 2 <= diff_ms < (frame_time_ms * 30):
            # 扣除最后一帧交给后面的 read()
            grab_count = int(diff_ms / frame_time_ms)
            for _ in range(max(0, grab_count - 1)):
                if not self._cap.grab():
                    break
            return True
        
        # 情况B：大跨度、反向或静止
        self._cap.set(cv2.CAP_PROP_POS_MSEC, target_ms)
        
        # 校验：部分后端 seek 后立刻 get 会得到旧值，这里做个简单的宽容校验
        actual_ms = self._cap.get(cv2.CAP_PROP_POS_MSEC)
        if abs(actual_ms - target_ms) > 1000: # 偏差 > 1s
            # 备选方案：尝试按帧定位
            frame_num = int(target_time * self._fps)
            self._cap.set(cv2.CAP_PROP_POS_FRAMES, frame_num)
        
        return True
    
    def capture_frame(
        self,
        timestamp: float,
        frame_id: str,
        enhance_params: Optional[Dict[str, Any]] = None
    ) -> FrameResult:
        """
        截取单帧
        
        Args:
            timestamp: 时间戳（秒）
            frame_id: 帧ID
            enhance_params: 增强参数 {"sharpen": bool, "contrast_boost": float, "local_zoom": bool}
        """
        if not self._cap:
            self.open()
        
        # 执行智能定位与重复帧自适应偏移
        max_drift = 0.5  # 最大允许向后偏移 0.5 秒寻找新帧
        drift_step = 0.1 # 每次偏移 0.1 秒
        current_ts = timestamp
        actual_timestamp = timestamp
        is_duplicate = False
        
        while current_ts <= timestamp + max_drift:
            self._seek_to_time(current_ts)
            ret, frame = self._cap.read()
            
            if not ret or frame is None:
                return FrameResult(
                    frame_id=frame_id,
                    timestamp=timestamp,
                    frame_path="",
                    width=0,
                    height=0,
                    brightness=0,
                    sharpness=0,
                    is_valid=False,
                    invalid_reason="Failed to read frame from video",
                    metadata={}
                )
                
            curr_hash = hash(frame.tobytes()[::1000])
            if curr_hash != self._last_frame_hash:
                # 找到新帧，记录哈希并更新结果时间戳
                if current_ts > timestamp:
                    print(f"      [DRIFT] Found new frame at {current_ts:.2f}s (offset +{current_ts - timestamp:.2f}s)")
                self._last_frame_hash = curr_hash
                actual_timestamp = current_ts
                is_duplicate = False
                break
            
            # 如果是重复帧，且还没超过最大漂移量，尝试往后推
            current_ts += drift_step
            # 如果已经到了最大漂移，只能接受当前帧
            if current_ts > timestamp + max_drift:
                actual_timestamp = current_ts - drift_step
                is_duplicate = True
                break

        # 如果是重复帧且已经有之前的路径，直接复用，不写硬盘
        if is_duplicate and self._last_frame_path and Path(self._last_frame_path).exists():
            print(f"      [REUSE] Static frame at {actual_timestamp:.2f}s, reusing previous storage.")
            return FrameResult(
                frame_id=frame_id,
                timestamp=actual_timestamp,
                frame_path=self._last_frame_path,
                width=int(self._cap.get(cv2.CAP_PROP_FRAME_WIDTH)),
                height=int(self._cap.get(cv2.CAP_PROP_FRAME_HEIGHT)),
                brightness=0, # 复用时不再重新计算
                sharpness=0,
                is_valid=True,
                metadata={"from_cache": True}
            )

        # 应用图像增强
        if enhance_params:
            frame = self._apply_enhancement(frame, enhance_params)
        
        # 计算质量指标
        brightness, sharpness = self._calculate_quality(frame)
        
        # 保存帧
        frame_path = self.output_dir / f"{frame_id}.png"
        cv2.imwrite(str(frame_path), frame)
        
        # 更新最后一次唯一的路径
        self._last_frame_path = str(frame_path)
        
        height, width = frame.shape[:2]
        
        return FrameResult(
            frame_id=frame_id,
            timestamp=actual_timestamp,
            frame_path=str(frame_path),
            width=width,
            height=height,
            brightness=brightness,
            sharpness=sharpness,
            is_valid=True
        )
    
    def capture_best_frame(
        self,
        target_time: float,
        frame_id: str,
        enhance_params: Optional[Dict[str, Any]] = None,
        search_window: float = 1.0,
        step: float = 0.1
    ) -> FrameResult:
        """
        智能截帧：在目标时间附近搜索最佳质量帧（最清晰且静止）
        
        Args:
            target_time: 目标时间戳
            frame_id: 帧ID
            enhance_params: 增强参数
            search_window: 搜索窗口大小（秒），也就是 +/- window/2
            step: 搜索步长（秒）
        """
        if not self._cap:
            self.open()
            
        start_time = max(0, target_time - search_window / 2)
        end_time = min(self.duration, target_time + search_window / 2)
        
        # 生成候选时间点
        candidates_times = np.arange(start_time, end_time, step)
        if len(candidates_times) == 0:
            candidates_times = [target_time]
            
        best_frame = None
        best_score = -1.0
        best_timestamp = target_time
        
        prev_frame_gray = None
        current_frame_num = -1
        
        for t in candidates_times:
            # 读取帧
            self._seek_to_time(t)
            
            ret, frame = self._cap.read()
            current_frame_num = int(self._cap.get(cv2.CAP_PROP_POS_FRAMES))
            
            if not ret or frame is None:
                continue
                
            # 计算质量
            gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            laplacian_var = cv2.Laplacian(gray, cv2.CV_64F).var()
            
            # 计算稳定性（与上一帧的差异）
            stability_score = 1.0
            if prev_frame_gray is not None:
                # 简单的像素差检查
                # 使用极小尺寸快速对比
                curr_small = cv2.resize(gray, (32, 32))
                prev_small = cv2.resize(prev_frame_gray, (32, 32))
                diff = cv2.absdiff(curr_small, prev_small)
                mean_diff = np.mean(diff)
                # mean_diff 越小越稳定
                stability_score = max(0, 1 - (mean_diff / 50.0))
            
            prev_frame_gray = gray
            
            # 综合评分：清晰度权重0.6，稳定性权重0.4
            norm_sharpness = min(1.0, laplacian_var / 500.0)
            
            if np.mean(gray) < 10: 
                total_score = 0
            else:
                total_score = norm_sharpness * 0.6 + stability_score * 0.4
            
            if total_score > best_score:
                best_score = total_score
                best_frame = frame.copy()
                best_timestamp = float(t)
                
        # 如果没找到任何帧，回退到原始点
        if best_frame is None:
            return self.capture_frame(target_time, frame_id, enhance_params)
            
        # 对最佳帧进行后续处理
        if enhance_params:
            best_frame = self._apply_enhancement(best_frame, enhance_params)
            
        brightness, sharpness = self._calculate_quality(best_frame)
        
        # 物理复用逻辑：如果最佳帧和上一次抓取完全一样，直接复用文件
        curr_hash = hash(best_frame.tobytes()[::1000])
        if curr_hash == self._last_frame_hash and self._last_frame_path and Path(self._last_frame_path).exists():
            print(f"      [REUSE] Best frame for '{frame_id}' matches previous, skipping write.")
            return FrameResult(
                frame_id=frame_id,
                timestamp=best_timestamp,
                frame_path=self._last_frame_path,
                width=int(self._cap.get(cv2.CAP_PROP_FRAME_WIDTH)),
                height=int(self._cap.get(cv2.CAP_PROP_FRAME_HEIGHT)),
                brightness=brightness,
                sharpness=sharpness,
                is_valid=True,
                metadata={"from_cache": True, "score": best_score}
            )

        # 保存
        frame_path = self.output_dir / f"{frame_id}.png"
        cv2.imwrite(str(frame_path), best_frame)
        
        # 记录哈希和路径供下次复用
        self._last_frame_hash = curr_hash
        self._last_frame_path = str(frame_path)
        
        height, width = best_frame.shape[:2]
        
        return FrameResult(
            frame_id=frame_id,
            timestamp=best_timestamp,
            frame_path=str(frame_path),
            width=width,
            height=height,
            brightness=brightness,
            sharpness=sharpness,
            is_valid=True
        )

    def capture_multiple(
        self,
        instruction: Dict[str, Any]
    ) -> List[FrameResult]:
        """
        根据指令截取多帧
        
        Args:
            instruction: 截帧指令
        """
        results = []
        instruction_id = instruction["instruction_id"]
        params = instruction.get("opencv_params", {})
        times = params.get("primary_times", [])
        enhance = params.get("enhance_params", {})
        
        # 默认启用智能搜索，窗口1.5秒
        search_window = 1.5
        step = 0.2
        
        for i, timestamp in enumerate(times):
            frame_id = f"F_{instruction_id}_{i}"
            # 使用智能截帧
            result = self.capture_best_frame(timestamp, frame_id, enhance, search_window, step)
            results.append(result)
        
        return results
    
    def _apply_enhancement(
        self, 
        frame: np.ndarray, 
        params: Dict[str, Any]
    ) -> np.ndarray:
        """应用图像增强"""
        result = frame.copy()
        
        # 锐化
        if params.get("sharpen", False):
            # 增强锐化力度
            kernel = np.array([
                [0, -1, 0],
                [-1, 5, -1],
                [0, -1, 0]
            ])
            result = cv2.filter2D(result, -1, kernel)
        
        # 对比度增强
        contrast_boost = params.get("contrast_boost", 1.0)
        if contrast_boost != 1.0:
            result = cv2.convertScaleAbs(result, alpha=contrast_boost, beta=0)
        
        # 局部放大（用于符号编号类断层）
        if params.get("local_zoom", False):
            # 放大中心区域
            h, w = result.shape[:2]
            center_x, center_y = w // 2, h // 2
            crop_w, crop_h = w // 2, h // 2
            
            x1 = center_x - crop_w // 2
            y1 = center_y - crop_h // 2
            x2 = center_x + crop_w // 2
            y2 = center_y + crop_h // 2
            
            cropped = result[y1:y2, x1:x2]
            result = cv2.resize(cropped, (w, h), interpolation=cv2.INTER_CUBIC)
        
        return result
    
    def _calculate_quality(self, frame: np.ndarray) -> Tuple[float, float]:
        """计算帧质量指标"""
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        
        # 平均亮度
        brightness = float(np.mean(gray))
        
        # 拉普拉斯方差（清晰度）
        laplacian = cv2.Laplacian(gray, cv2.CV_64F)
        sharpness = float(laplacian.var())
        
        return brightness, sharpness
    
    def validate_frame(
        self,
        frame_path: str,
        thresholds: Optional[Dict[str, float]] = None
    ) -> Tuple[bool, Optional[str]]:
        """
        校验帧质量 (Step 13)
        
        Args:
            frame_path: 帧路径
            thresholds: 阈值 {"min_brightness": 30, "min_sharpness": 100}
            
        Returns:
            (is_valid, invalid_reason)
        """
        thresholds = thresholds or {"min_brightness": 30, "min_sharpness": 100}
        
        frame = cv2.imread(frame_path)
        if frame is None:
            return False, "Cannot read frame"
        
        brightness, sharpness = self._calculate_quality(frame)
        
        if brightness < thresholds["min_brightness"]:
            return False, f"亮度不足 ({brightness:.1f} < {thresholds['min_brightness']})"
        
        if sharpness < thresholds["min_sharpness"]:
            return False, f"过渡模糊 ({sharpness:.1f} < {thresholds['min_sharpness']})"
        
        return True, None
    
    def extract_text_region(
        self,
        frame_path: str,
        region: Optional[Tuple[int, int, int, int]] = None
    ) -> str:
        """
        提取帧中的文字区域（用于 OCR 预处理）
        
        Args:
            frame_path: 帧路径
            region: 区域 (x, y, width, height)，None 表示全图
            
        Returns:
            处理后的图片路径
        """
        frame = cv2.imread(frame_path)
        if frame is None:
            return frame_path
        
        if region:
            x, y, w, h = region
            frame = frame[y:y+h, x:x+w]
        
        # 预处理：灰度 + 二值化
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        _, binary = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        
        # 保存
        output_path = frame_path.replace(".png", "_text.png")
        cv2.imwrite(output_path, binary)
        
        return output_path


class SemanticPeakDetector:
    """
    语义视觉峰值检测器
    用于寻找 "信息量最丰富" 的瞬间
    """
    def __init__(self, cap: cv2.VideoCapture):
        self._cap = cap

    def calculate_frame_metrics(self, frame: np.ndarray) -> Dict[str, float]:
        """计算单帧的视觉信息指标"""
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        
        # 1. 边缘密度 (Canny Edge Density) - 代理: 文本/图表丰富度
        edges = cv2.Canny(gray, 100, 200)
        edge_density = np.sum(edges) / 255.0
        
        # 2. 局部对比度 (Local Contrast) - 代理: 清晰度
        laplacian_var = cv2.Laplacian(gray, cv2.CV_64F).var()
        
        return {
            "edge_density": edge_density,
            "sharpness": laplacian_var
        }

    def detect_peak(self, start_sec: float, end_sec: float, step_sec: float = 0.5) -> Tuple[float, List[Dict[str, float]]]:
        """
        在时间范围内检测信息峰值
        
        策略:
        1. 扫描 [start, end] 区间
        2. 计算每帧的 Edge Density
        3. 寻找局部最大值
        """
        timestamps = np.arange(start_sec, end_sec, step_sec)
        best_time = start_sec
        max_score = -1.0
        
        metrics_history = []
        
        for t in timestamps:
            self._cap.set(cv2.CAP_PROP_POS_MSEC, t * 1000)
            ret, frame = self._cap.read()
            if not ret or frame is None:
                continue
                
            metrics = self.calculate_frame_metrics(frame)
            
            # 综合评分: 边缘密度为主 (0.7), 清晰度为辅 (0.3)
            # 注意: 需要归一化或动态调整权重，这里简单假设
            score = metrics["edge_density"] * 0.7 + (metrics["sharpness"] / 500.0) * 0.3
            
            metrics_history.append((t, score))
            
            if score > max_score:
                max_score = score
                best_time = t
                
        # 简单平滑策略 (可选): 如果后一帧骤降，前一帧可能是峰值
        # 这里暂时直接返回最高分时刻
        
        print(f"      [PEAK] Detected peak at {best_time:.2f}s (Score: {max_score:.2f}) in range [{start_sec:.1f}-{end_sec:.1f}]")
        return float(best_time), metrics_history


def calculate_capture_times(
    strategy_match: Dict[str, Any],
    fault_location: Dict[str, float]
) -> Dict[str, Any]:
    """
    计算截帧时间 (Step 10)
    
    Args:
        strategy_match: 策略匹配结果
        fault_location: 断层位置 {"start_sec": 10.5, "end_sec": 15.2}
        
    Returns:
        {
            "capture_times": [10.5, 11.0, 11.5],
            "fallback_range": {"start_sec": 10.0, "end_sec": 16.0, "step_sec": 0.5},
            "peak_detect_params": { ... } # Optional
        }
    """
    start = fault_location["start_sec"]
    end = fault_location["end_sec"]
    mode = strategy_match.get("mode", "单帧精准")
    count = strategy_match.get("count", 1)
    
    peak_detect_params = None
    
    if mode == "peak_detect":
        # 峰值检测模式: 生成一个宽范围供 Detector 扫描
        # 这里的 preferred_time 只是一个 fallback
        preferred = [(start + end) / 2]
        
        # 定义搜索范围: 目标区间外扩 1s
        search_start = max(0, start - 1.0)
        search_end = end + 1.0
        
        peak_detect_params = {
            "search_range": (search_start, search_end),
            "step_sec": 0.5
        }
    elif mode == "单帧精准":
        preferred = [start + 0.5]
    elif mode == "双帧采样":
        preferred = [start + 0.5, end - 0.5]
    elif mode == "多帧采样":
        if count > 1:
            step = (end - start) / (count + 1)
            preferred = [start + step * (i + 1) for i in range(count)]
        else:
            preferred = [(start + end) / 2]
    else:
        preferred = [(start + end) / 2]
    
    fallback = {
        "start_sec": max(0, start - 1),
        "end_sec": end + 1,
        "step_sec": 0.5
    }
    
    result = {
        "capture_times": preferred,
        "fallback_range": fallback
    }
    
    if peak_detect_params:
        result["peak_detect_params"] = peak_detect_params
        
    return result
