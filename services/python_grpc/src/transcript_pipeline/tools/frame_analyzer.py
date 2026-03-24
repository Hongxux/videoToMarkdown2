"""
模块说明：阶段工具 frame_analyzer 的实现。
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
核心功能：
1. 在候选时间范围内提取关键帧（0.5s间隔）
2. 使用SSIM像素对比检测边界
3. 输出候选边界帧列表"""

import cv2
import numpy as np
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field

try:
    from skimage.metrics import structural_similarity as ssim
    HAS_SKIMAGE = True
except ImportError:
    HAS_SKIMAGE = False

from .opencv_capture import FrameCapture, FrameResult


@dataclass
class BoundaryCandidate:
    """类说明：BoundaryCandidate 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    frame_idx: int
    timestamp: float
    frame_path: str
    score: float  # SSIM分数或稳定帧数
    is_start: bool  # True=开始候选, False=结束候选
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass 
class BoundaryAnalysisResult:
    """类说明：BoundaryAnalysisResult 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    start_candidates: List[BoundaryCandidate]
    end_candidates: List[BoundaryCandidate]
    all_frames: List[Dict[str, Any]]
    search_range: Dict[str, float]


class FrameBoundaryAnalyzer:
    """类说明：FrameBoundaryAnalyzer 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    
    # 检测参数
    FRAME_INTERVAL = 0.5  # 默认降级间隔
    MIN_STEP = 0.6      # 最小自适应步长（提高以避免平移过程过度采样）
    MAX_STEP = 3.0      # 最大自适应步长
    FINE_INTERVAL = 0.2    # 精筛间隔
    START_SSIM_THRESHOLD = 0.9  # 开始点SSIM阈值
    END_SSIM_THRESHOLD = 0.95  # 结束点SSIM阈值（高于此值认为画面稳定）
    DIFF_RATIO_MIN = 0.05  # 最小差异占比
    DIFF_RATIO_MAX = 0.15  # 最大差异占比
    STABLE_FRAME_COUNT = 3  # 结束点需要的连续稳定帧数
    
    def __init__(self, video_path: str, output_dir: str, session_id: str = "default"):
        """
        执行逻辑：
        1) 解析配置或依赖，准备运行环境。
        2) 初始化对象状态、缓存与依赖客户端。
        实现方式：通过内部方法调用/状态更新、文件系统读写实现。
        核心价值：在初始化阶段固化依赖，保证运行稳定性。
        输入参数：
        - video_path: 文件路径（类型：str）。
        - output_dir: 目录路径（类型：str）。
        - session_id: 标识符（类型：str）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self.video_path = video_path
        self.output_dir = Path(output_dir)
        # 为并发任务使用唯一的临时目录
        self.frames_dir = self.output_dir / f"boundary_analysis_{session_id}"
        self.frames_dir.mkdir(parents=True, exist_ok=True)
        
        self.frame_capture = FrameCapture(video_path, str(self.frames_dir))
        self.active_paths = set() # 记录真正有意义的候选帧路径
        
    def analyze_boundary(
        self,
        rough_range: Dict[str, float],
        title: str = "",
        summary: str = "",
        sample_step: Optional[float] = None
    ) -> BoundaryAnalysisResult:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、文件系统读写实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：sample_step
        - 条件：len(frames_data) < 2
        - 条件：additional_frames
        依据来源（证据链）：
        - 输入参数：sample_step。
        - 配置字段：frame_path。
        - 阈值常量：AOI_SSIM_THRESHOLD。
        - 对象内部状态：self.FINE_INTERVAL。
        输入参数：
        - rough_range: 函数入参（类型：Dict[str, float]）。
        - title: 函数入参（类型：str）。
        - summary: 函数入参（类型：str）。
        - sample_step: 函数入参（类型：Optional[float]）。
        输出参数：
        - BoundaryAnalysisResult 对象（包含字段：start_candidates, end_candidates, all_frames, search_range）。"""
        start_sec = rough_range.get("start_sec", 0)
        end_sec = rough_range.get("end_sec", 0)
        
        duration = end_sec - start_sec
        # 如果指定了采样步长，直接使用；否则使用自适应逻辑
        if sample_step:
            adaptive_step = sample_step
        else:
            # 比例自适应：取持续时间的 1/15 作为粗筛步长，保证短动画有足够密度
            adaptive_step = max(self.MIN_STEP, min(self.MAX_STEP, duration / 15.0))
        
        print(f"      [BOUNDARY] Scanning '{title}' ({duration:.1f}s, step: {adaptive_step:.2f}s)")
        print(f"      [BOUNDARY] Search range: {start_sec:.1f}s - {end_sec:.1f}s")
        
        # 1. 粗精定位两步走 (Coarse-to-Fine)
        # 第一阶段：粗定位
        frames_data = self._extract_frames(start_sec, end_sec, interval=adaptive_step)
        
        if len(frames_data) < 2:
             return BoundaryAnalysisResult([], [], frames_data, rough_range)

        # 加载粗定位帧以检测 AOI
        frames_objs = self._load_frames(frames_data)
        additional_frames = []
        
        # 寻找变化剧烈的区域，进行精细化补全
        aoi_count = 0
        AOI_SSIM_THRESHOLD = 0.98  # 只要有微小变化就触发精筛（原为0.9，现提高敏锐度）
        
        for i in range(len(frames_objs) - 1):
            ssim_score, _ = self._compare_frames(frames_objs[i], frames_objs[i+1])
            # 如果两帧之间有变化（SSIM < 0.98），或者前一帧与后一帧差异较大
            if ssim_score < AOI_SSIM_THRESHOLD:
                t_start = frames_data[i]["timestamp"]
                t_end = frames_data[i+1]["timestamp"]
                
                # 只有当间隔大于精筛间隔时才需要补全
                if (t_end - t_start) > self.FINE_INTERVAL:
                    aoi_count += 1
                    print(f"      [FINE] AOI detected at {t_start:.1f}s-{t_end:.1f}s (SSIM: {ssim_score:.3f})")
                    fine_data = self._extract_frames(t_start, t_end, interval=self.FINE_INTERVAL)
                    additional_frames.extend(fine_data)
        
        if additional_frames:
            print(f"      [FINE] Detected {aoi_count} AOI (Area of Interest) regions")
            print(f"      [FINE] Targeted refining: +{len(additional_frames)} frames (step: {self.FINE_INTERVAL}s)")
            # 合并、去重、排序
            ts_map = {fd["timestamp"]: fd for fd in frames_data}
            for fd in additional_frames:
                ts_map[fd["timestamp"]] = fd
            
            frames_data = sorted(ts_map.values(), key=lambda x: x["timestamp"])
            # 更新索引
            for idx, fd in enumerate(frames_data):
                fd["frame_idx"] = idx
            frames = self._load_frames(frames_data)
        else:
            frames = frames_objs

        # 2. 记录所有帧路径（用于后续清理）
        for fd in frames_data:
            if fd.get("frame_path"):
                self.active_paths.add(Path(fd["frame_path"]).resolve())
        
        print(f"      [RESULT] Sampled {len(frames_data)} frames for Vision AI analysis.")
        if frames_data:
            print(f"      [RESULT] Frame range: {frames_data[0]['timestamp']:.1f}s - {frames_data[-1]['timestamp']:.1f}s")
        
        # 返回完整帧序列，不做候选判断
        return BoundaryAnalysisResult(
            start_candidates=[],  # 不再由 SSIM 判断候选
            end_candidates=[],    # 不再由 SSIM 判断候选
            all_frames=frames_data,
            search_range=rough_range
        )
    
    def _extract_frames(
        self, 
        start_sec: float, 
        end_sec: float,
        interval: Optional[float] = None
    ) -> List[Dict[str, Any]]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：interval is None
        - 条件：result.is_valid
        依据来源（证据链）：
        - 输入参数：interval。
        输入参数：
        - start_sec: 起止时间/区间边界（类型：float）。
        - end_sec: 起止时间/区间边界（类型：float）。
        - interval: 函数入参（类型：Optional[float]）。
        输出参数：
        - Dict[str, Any] 列表（与输入或处理结果一一对应）。"""
        if interval is None:
            interval = self.FRAME_INTERVAL
            
        frames_data = []
        
        with self.frame_capture:
            current_time = start_sec
            frame_idx = 0
            
            while current_time <= end_sec:
                frame_id = f"boundary_{frame_idx:04d}"
                result = self.frame_capture.capture_frame(
                    current_time, 
                    frame_id,
                    enhance_params={"sharpen": False}  # 不做增强，保持原始
                )
                
                if result.is_valid:
                    frames_data.append({
                        "frame_idx": frame_idx,
                        "timestamp": result.timestamp, # 使用实际抓取到的时间点（可能已偏移）
                        "frame_path": result.frame_path,
                        "brightness": result.brightness,
                        "sharpness": result.sharpness
                    })
                    
                    # 关键优化：以实际抓取时间点为基准推移，解决冗余
                    # 如果刚才因为重复帧推迟了（如漂移了 0.4s），下一次检查从延迟后的点开始
                    current_time = result.timestamp + interval
                else:
                    current_time += interval
                
                frame_idx += 1
        
        return frames_data
    
    def _load_frames(self, frames_data: List[Dict]) -> List[np.ndarray]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：frame is not None
        依据来源（证据链）：
        输入参数：
        - frames_data: 函数入参（类型：List[Dict]）。
        输出参数：
        - np.ndarray 列表（与输入或处理结果一一对应）。"""
        frames = []
        for fd in frames_data:
            frame = cv2.imread(fd["frame_path"])
            if frame is not None:
                frames.append(frame)
            else:
                frames.append(np.zeros((480, 640, 3), dtype=np.uint8))
        return frames
    
    def _detect_start_candidates(
        self,
        frames_data: List[Dict],
        frames: List[np.ndarray]
    ) -> List[BoundaryCandidate]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：ssim_score < self.START_SSIM_THRESHOLD
        - 条件：self.DIFF_RATIO_MIN <= diff_ratio <= self.DIFF_RATIO_MAX
        依据来源（证据链）：
        - 对象内部状态：self.DIFF_RATIO_MAX, self.DIFF_RATIO_MIN, self.START_SSIM_THRESHOLD。
        输入参数：
        - frames_data: 函数入参（类型：List[Dict]）。
        - frames: 数据列表/集合（类型：List[np.ndarray]）。
        输出参数：
        - BoundaryCandidate 列表（与输入或处理结果一一对应）。
        补充说明：
        判断标准：
        1. 相邻帧SSIM < 0.9（存在显著差异）
        2. 差异区域占比在5%-15%之间"""
        candidates = []
        
        for i in range(len(frames) - 1):
            ssim_score, diff_ratio = self._compare_frames(frames[i], frames[i+1])
            
            # 记录所有帧的SSIM值用于调试
            frames_data[i]["ssim_to_next"] = ssim_score
            frames_data[i]["diff_ratio_to_next"] = diff_ratio
            
            # 检测显著变化
            if ssim_score < self.START_SSIM_THRESHOLD:
                if self.DIFF_RATIO_MIN <= diff_ratio <= self.DIFF_RATIO_MAX:
                    candidates.append(BoundaryCandidate(
                        frame_idx=i + 1,  # 变化后的帧
                        timestamp=frames_data[i + 1]["timestamp"],
                        frame_path=frames_data[i + 1]["frame_path"],
                        score=ssim_score,
                        is_start=True,
                        metadata={
                            "ssim": ssim_score,
                            "diff_ratio": diff_ratio,
                            "prev_frame_idx": i
                        }
                    ))
        
        # 按时间排序（开始点应选最早的有效候选）
        candidates.sort(key=lambda x: x.timestamp)
        
        return candidates
    
    def _detect_end_candidates(
        self,
        frames_data: List[Dict],
        frames: List[np.ndarray]
    ) -> List[BoundaryCandidate]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：ssim_score > self.END_SSIM_THRESHOLD
        - 条件：stable_start_idx < 0
        - 条件：stable_count >= self.STABLE_FRAME_COUNT
        依据来源（证据链）：
        - 对象内部状态：self.END_SSIM_THRESHOLD, self.STABLE_FRAME_COUNT。
        输入参数：
        - frames_data: 函数入参（类型：List[Dict]）。
        - frames: 数据列表/集合（类型：List[np.ndarray]）。
        输出参数：
        - BoundaryCandidate 列表（与输入或处理结果一一对应）。
        补充说明：
        判断标准：
        1. 连续3-5帧SSIM > 0.95（画面稳定）"""
        candidates = []
        stable_count = 0
        stable_start_idx = -1
        
        for i in range(len(frames) - 1):
            ssim_score, _ = self._compare_frames(frames[i], frames[i+1])
            
            if ssim_score > self.END_SSIM_THRESHOLD:
                if stable_start_idx < 0:
                    stable_start_idx = i
                stable_count += 1
                
                if stable_count >= self.STABLE_FRAME_COUNT:
                    # 找到稳定序列，记录第一帧为结束候选
                    candidates.append(BoundaryCandidate(
                        frame_idx=stable_start_idx,
                        timestamp=frames_data[stable_start_idx]["timestamp"],
                        frame_path=frames_data[stable_start_idx]["frame_path"],
                        score=stable_count,
                        is_start=False,
                        metadata={
                            "stable_frames": stable_count,
                            "avg_ssim": ssim_score
                        }
                    ))
                    # 重置计数，查找下一个稳定区间
                    stable_count = 0
                    stable_start_idx = -1
            else:
                stable_count = 0
                stable_start_idx = -1
        
        # 按时间排序（结束点应选最晚的有效候选）
        candidates.sort(key=lambda x: x.timestamp, reverse=True)
        
        return candidates
    
    def _compare_frames(
        self,
        frame1: np.ndarray,
        frame2: np.ndarray
    ) -> Tuple[float, float]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：HAS_SKIMAGE
        依据来源（证据链）：
        - 阈值常量：HAS_SKIMAGE。
        输入参数：
        - frame1: 函数入参（类型：np.ndarray）。
        - frame2: 函数入参（类型：np.ndarray）。
        输出参数：
        - 多值结果元组（各元素含义见实现）。"""
        # 转为灰度图
        gray1 = cv2.cvtColor(frame1, cv2.COLOR_BGR2GRAY)
        gray2 = cv2.cvtColor(frame2, cv2.COLOR_BGR2GRAY)
        
        # 计算SSIM
        if HAS_SKIMAGE:
            ssim_score = ssim(gray1, gray2)
        else:
            # 简化的SSIM替代计算
            ssim_score = self._simple_ssim(gray1, gray2)
        
        # 计算差异占比
        diff = cv2.absdiff(frame1, frame2)
        gray_diff = cv2.cvtColor(diff, cv2.COLOR_BGR2GRAY)
        _, binary = cv2.threshold(gray_diff, 30, 255, cv2.THRESH_BINARY)
        diff_ratio = cv2.countNonZero(binary) / (binary.shape[0] * binary.shape[1])
        
        return ssim_score, diff_ratio
    
    def _simple_ssim(self, img1: np.ndarray, img2: np.ndarray) -> float:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：std1 == 0 or std2 == 0
        - 条件：np.allclose(img1_f, img2_f)
        依据来源（证据链）：
        输入参数：
        - img1: 函数入参（类型：np.ndarray）。
        - img2: 函数入参（类型：np.ndarray）。
        输出参数：
        - 数值型计算结果。"""
        # 使用归一化互相关作为近似
        img1_f = img1.astype(np.float32) / 255.0
        img2_f = img2.astype(np.float32) / 255.0
        
        mean1, mean2 = img1_f.mean(), img2_f.mean()
        std1, std2 = img1_f.std(), img2_f.std()
        
        if std1 == 0 or std2 == 0:
            return 1.0 if np.allclose(img1_f, img2_f) else 0.0
        
        cov = ((img1_f - mean1) * (img2_f - mean2)).mean()
        
        # 简化的SSIM公式
        c1, c2 = 0.01 ** 2, 0.03 ** 2
        ssim_val = ((2 * mean1 * mean2 + c1) * (2 * cov + c2)) / \
                   ((mean1 ** 2 + mean2 ** 2 + c1) * (std1 ** 2 + std2 ** 2 + c2))
        
        return float(ssim_val)
    
    def cleanup(self, force: bool = False):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、文件系统读写实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：not self.frames_dir.exists()
        - 条件：force
        - 条件：Path(file).resolve() not in self.active_paths
        依据来源（证据链）：
        - 输入参数：force。
        - 对象内部状态：self.active_paths, self.frames_dir。
        输入参数：
        - force: 函数入参（类型：bool）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        if not self.frames_dir.exists():
            return
            
        if force:
            import shutil
            shutil.rmtree(self.frames_dir)
        else:
            # 只清理非候选帧，保留现场供监控
            for file in self.frames_dir.glob("*.png"):
                if Path(file).resolve() not in self.active_paths:
                    try:
                        file.unlink()
                    except:
                        pass
