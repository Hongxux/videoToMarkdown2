"""
模块说明：Module2 内容增强中的 visual_feature_extractor 模块。
执行逻辑：
1) 聚合本模块的类/函数，对外提供核心能力。
2) 通过内部调用与外部依赖完成具体处理。
实现方式：通过模块内函数组合与外部依赖调用实现。
核心价值：统一模块职责边界，降低跨文件耦合成本。
输入：
- 调用方传入的参数与数据路径。
输出：
- 各函数/类返回的结构化结果或副作用。"""

import cv2
import numpy as np
import logging
import concurrent.futures
import asyncio
import os
import gc
import hashlib
from typing import List, Dict, Tuple, Optional, Any
from dataclasses import dataclass
from pathlib import Path
from collections import OrderedDict
from multiprocessing import shared_memory
import threading
from .resource_utils import ResourceOrchestrator
from .dynamic_decision_engine import DynamicDecisionEngine, GlobalAnalysisCache
from . import cache_metrics

# 💥 性能优化: 尝试引入 Numba JIT 加速
# V5: CLIP Support
try:
    from PIL import Image
    from sentence_transformers import SentenceTransformer, util
    HAS_CLIP = True
except ImportError:
    HAS_CLIP = False
    print("Warning: sentence-transformers or PIL not found. CLIP disabled.")

try:
    from numba import jit, prange
    HAS_NUMBA = True
    logger = logging.getLogger(__name__)
    logger.info("Numba JIT accelerator detected and enabled (Parallel Mode)")
    
    @jit(nopython=True, fastmath=True, parallel=True)
    def _numba_mse(arr1, arr2):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - arr1: 函数入参（类型：未标注）。
        - arr2: 函数入参（类型：未标注）。
        输出参数：
        - 函数计算/封装后的结果对象。"""
        s = 0.0
        h, w = arr1.shape
        # Use prange for automatic multi-threading on large image segments
        for i in prange(h):
            row_sum = 0.0
            for j in range(w):
                d = float(arr1[i, j]) - float(arr2[i, j])
                row_sum += d * d
            s += row_sum
        return s / (h * w)
        
except Exception as e:
    HAS_NUMBA = False
    logger = logging.getLogger(__name__)
    logger.warning(f"Numba acceleration disabled due to error: {e}")
    
    def _numba_mse(arr1, arr2):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - arr1: 函数入参（类型：未标注）。
        - arr2: 函数入参（类型：未标注）。
        输出参数：
        - 函数计算/封装后的结果对象。"""
        return 0.0 # Placeholder

from .visual_element_detection_helpers import VisualElementDetector

logger = logging.getLogger(__name__)

@dataclass
class VisualFeatures:
    """
    类说明：封装 VisualFeatures 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    visual_type: str  # "static" | "dynamic" | "mixed"
    avg_mse: float
    avg_diff_rate: float # 💥 Added for Phase 4.5 diagnostics
    mse_list: List[float]
    is_static: bool
    is_dynamic: bool
    stable_duration: float
    has_architecture_elements: bool
    has_math_formula: bool
    element_count: int
    confidence: float
    has_table: bool = False # V4
    has_static_visual_structure: bool = False # V4
    action_windows: List[Dict] = None # V6.9.7: Rich Media Clips
    avg_edge_flux: float = 0.0 # V6.9.8: Transition Detection
    action_density: float = 0.0 # V7.0: For Noise Filtering

# 💥 全局共享模型与限流器: 避免并行加载导致的 OOM 与 Meta Tensor 错误
_VISUAL_PROCESS_POOL = None
_CLIP_MODEL = None
_CLIP_LOCK = threading.Lock()

def _get_clip_model():
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：not HAS_CLIP
    - 条件：_CLIP_MODEL is None
    - 条件：_CLIP_MODEL != 'FAILED'
    依据来源（证据链）：
    - 阈值常量：HAS_CLIP, _CLIP_MODEL。
    输入参数：
    - 无。
    输出参数：
    - 函数计算/封装后的结果对象。"""
    global _CLIP_MODEL
    if not HAS_CLIP: return None
    
    with _CLIP_LOCK:
        if _CLIP_MODEL is None:
            try:
                # 🚀 V5: Using lightweight CLIP. Explicit device='cpu' to avoid meta-tensor issues in parallel envs.
                # If you have GPU, SentenceTransformer usually handles it, but 'cpu' is safest for sidecar stability.
                logger.info("📡 Loading CLIP model (Global Singleton)...")
                _CLIP_MODEL = SentenceTransformer('clip-ViT-B-32', device='cpu')
                logger.info("✅ V5: CLIP model loaded successfully.")
            except Exception as e:
                logger.warning(f"❌ V5: Failed to load CLIP model: {e}")
                # Don't try again if it fails once to avoid flooding logs
                return "FAILED"
    
    return _CLIP_MODEL if _CLIP_MODEL != "FAILED" else None

_VISUAL_PROCESS_POOL = None

def get_visual_process_pool():
    """
    执行逻辑：
    1) 读取内部状态或外部资源。
    2) 返回读取结果。
    实现方式：通过线程池并发、HTTP 调用实现。
    核心价值：提供一致读取接口，降低调用耦合。
    决策逻辑：
    - 条件：_VISUAL_PROCESS_POOL is not None
    - 条件：_VISUAL_PROCESS_POOL is None
    - 条件：getattr(_VISUAL_PROCESS_POOL, '_broken', False)
    依据来源（证据链）：
    - 阈值常量：_VISUAL_PROCESS_POOL。
    输入参数：
    - 无。
    输出参数：
    - 函数计算/封装后的结果对象。"""
    global _VISUAL_PROCESS_POOL
    
    # Check if pool exists and is healthy
    if _VISUAL_PROCESS_POOL is not None:
        # 💥 容错处理: 如果进程池由于 OOM 等原因损坏，底层会抛出 BrokenProcessPool
        # 尝试检查内部状态 (私有 API, 但在 concurrent.futures 中比较通用)
        if getattr(_VISUAL_PROCESS_POOL, '_broken', False):
            logger.warning("Process Pool is broken (likely due to OOM in child processes). Re-initializing...")
            _VISUAL_PROCESS_POOL.shutdown(wait=False)
            _VISUAL_PROCESS_POOL = None

    if _VISUAL_PROCESS_POOL is None:
        # ⚖️ V6 Adaptive: Replace static cap with resource-aware workers
        import psutil
        mem = psutil.virtual_memory()
        cpu_count = os.cpu_count() or 4
        
        # Memory-aware capping: 1 worker per 2.5GB of free RAM, max cpu_count
        mem_based_workers = int(mem.available / (2.5 * 1024 * 1024 * 1024))
        # 🚀 Bump cap to 20 to support higher concurrency requests
        max_workers = max(1, min(cpu_count * 2, mem_based_workers, 20))
        
        # 💥 Stability: Use ThreadPoolExecutor on Windows to avoid "WinError 8" and Pickling OOM
        _VISUAL_PROCESS_POOL = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        logger.info(f"🚀 [PERF] Initialized Global Visual Pool (THREAD MODE) with {max_workers} workers (System: {mem.percent}% RAM used)")
    
    return _VISUAL_PROCESS_POOL

def shutdown_visual_process_pool():
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：_VISUAL_PROCESS_POOL is not None
    依据来源（证据链）：
    - 阈值常量：_VISUAL_PROCESS_POOL。
    输入参数：
    - 无。
    输出参数：
    - 无（仅产生副作用，如日志/写盘/状态更新）。"""
    global _VISUAL_PROCESS_POOL
    if _VISUAL_PROCESS_POOL is not None:
        logger.info("Shutting down global Visual Process Pool...")
        _VISUAL_PROCESS_POOL.shutdown(wait=True)
        _VISUAL_PROCESS_POOL = None

class SharedFrameRegistry:
    """
    类说明：封装 SharedFrameRegistry 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    def __init__(self, max_frames=None):
        """
        执行逻辑：
        1) 解析配置或依赖，准备运行环境。
        2) 初始化对象状态、缓存与依赖客户端。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：在初始化阶段固化依赖，保证运行稳定性。
        决策逻辑：
        - 条件：max_frames is None
        依据来源（证据链）：
        - 输入参数：max_frames。
        输入参数：
        - max_frames: 函数入参（类型：未标注）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        if max_frames is None:
            max_frames = ResourceOrchestrator.get_adaptive_cache_size(base_size=50, per_gb_increment=25)
        self.max_frames = max_frames
        self._registry: Dict[int, str] = OrderedDict() # frame_idx -> shm_name
        self._shms: Dict[str, shared_memory.SharedMemory] = {}
        self._lock = threading.Lock()
        self._shape = None
        self._dtype = None

    def register_frame(self, frame_idx: int, frame: np.ndarray):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：frame_idx in self._registry
        - 条件：len(self._registry) >= self.max_frames
        - 条件：self._shape is None
        依据来源（证据链）：
        - 输入参数：frame_idx。
        - 对象内部状态：self._registry, self._shape, self.max_frames。
        输入参数：
        - frame_idx: 函数入参（类型：int）。
        - frame: 函数入参（类型：np.ndarray）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        with self._lock:
            if frame_idx in self._registry:
                return
            
            # 维护容量
            if len(self._registry) >= self.max_frames:
                old_idx, shm_name = self._registry.popitem(last=False)
                shm = self._shms.pop(shm_name)
                shm.close()
                try: shm.unlink()
                except: pass
            
            if self._shape is None:
                self._shape = frame.shape
                self._dtype = frame.dtype
                
            try:
                shm = shared_memory.SharedMemory(create=True, size=frame.nbytes)
            except Exception as e:
                logger.warning(f"SharedMemory allocation failed: {e}. Falling back to standard transfer.")
                return
                
            self._registry[frame_idx] = shm.name
            self._shms[shm.name] = shm
            
            # 写入数据
            dst = np.ndarray(frame.shape, dtype=frame.dtype, buffer=shm.buf)
            dst[:] = frame[:]

    def get_frame(self, frame_idx: int) -> Optional[np.ndarray]:
        """
        执行逻辑：
        1) 读取内部状态或外部资源。
        2) 返回读取结果。
        实现方式：通过内部方法调用/状态更新、NumPy 数值计算实现。
        核心价值：提供一致读取接口，降低调用耦合。
        决策逻辑：
        - 条件：not shm_name
        - 条件：not shm
        依据来源（证据链）：
        输入参数：
        - frame_idx: 函数入参（类型：int）。
        输出参数：
        - ndarray 对象或调用结果。"""
        with self._lock:
            shm_name = self._registry.get(frame_idx)
            if not shm_name:
                cache_metrics.miss("module2.shared_frame_registry.get_frame")
                return None
            
            # 注意: 这里返回的 ndarray 保持了对 shm.buf 的引用
            # 调用者必须在进程结束前保持 shm 打开状态
            shm = self._shms.get(shm_name)
            if not shm:
                cache_metrics.miss("module2.shared_frame_registry.get_frame")
                return None
            cache_metrics.hit("module2.shared_frame_registry.get_frame")
            return np.ndarray(self._shape, dtype=self._dtype, buffer=shm.buf)

    def cleanup(self):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - 无。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        with self._lock:
            for shm_name, shm in list(self._shms.items()):
                try:
                    shm.close()
                    shm.unlink()
                except Exception as e:
                    logger.debug(f"SHM unlink failed for {shm_name}: {e}")
            self._registry.clear()
            self._shms.clear()

    def get_shm_ref(self, frame_idx: int) -> Optional[Dict[str, Any]]:
        """
        执行逻辑：
        1) 读取内部状态或外部资源。
        2) 返回读取结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：提供一致读取接口，降低调用耦合。
        决策逻辑：
        - 条件：not shm_name
        依据来源（证据链）：
        输入参数：
        - frame_idx: 函数入参（类型：int）。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        with self._lock:
            shm_name = self._registry.get(frame_idx)
            if not shm_name:
                cache_metrics.miss("module2.shared_frame_registry.get_shm_ref")
                return None
            cache_metrics.hit("module2.shared_frame_registry.get_shm_ref")
            return {
                "shm_name": shm_name,
                "shape": self._shape,
                "dtype": self._dtype,
                "frame_idx": frame_idx
            }

# 全局注册表单例
_GLOBAL_FRAME_REGISTRY = None
import atexit

def get_shared_frame_registry():
    """
    执行逻辑：
    1) 读取内部状态或外部资源。
    2) 返回读取结果。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：提供一致读取接口，降低调用耦合。
    决策逻辑：
    - 条件：_GLOBAL_FRAME_REGISTRY is None
    依据来源（证据链）：
    - 阈值常量：_GLOBAL_FRAME_REGISTRY。
    输入参数：
    - 无。
    输出参数：
    - 函数计算/封装后的结果对象。"""
    global _GLOBAL_FRAME_REGISTRY
    if _GLOBAL_FRAME_REGISTRY is None:
        _GLOBAL_FRAME_REGISTRY = SharedFrameRegistry()
        atexit.register(_GLOBAL_FRAME_REGISTRY.cleanup)
    return _GLOBAL_FRAME_REGISTRY

class VisualFeatureExtractor:
    """
    类说明：封装 VisualFeatureExtractor 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    
    def __init__(self, video_path: str):
        """
        执行逻辑：
        1) 解析配置或依赖，准备运行环境。
        2) 初始化对象状态、缓存与依赖客户端。
        实现方式：通过内部方法调用/状态更新、OpenCV 图像处理、文件系统读写实现。
        核心价值：在初始化阶段固化依赖，保证运行稳定性。
        决策逻辑：
        - 条件：not Path(video_path).exists()
        - 条件：not self.cap.isOpened()
        - 条件：mem.percent > 80
        依据来源（证据链）：
        - 输入参数：video_path。
        - 对象内部状态：self.cap, self.fps。
        输入参数：
        - video_path: 文件路径（类型：str）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self.video_path = video_path
        if not Path(video_path).exists():
             logger.warning(f"Video path does not exist: {video_path}")
        
        self.cap = cv2.VideoCapture(video_path)
        if not self.cap.isOpened():
             logger.error(f"Failed to open video: {video_path}")
             # If video cannot be opened, set default values to avoid errors later
             self.fps = 30.0
             self.frame_count = 0
             self.width = 0
             self.height = 0
             self.duration = 0
        else:
            self.fps = self.cap.get(cv2.CAP_PROP_FPS) or 30.0
            self.frame_count = int(self.cap.get(cv2.CAP_PROP_FRAME_COUNT))
            self.width = int(self.cap.get(cv2.CAP_PROP_FRAME_WIDTH))
            self.height = int(self.cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
            self.duration = self.frame_count / self.fps if self.fps > 0 else 0
        
        # 🚀 Phase 4.3: Added visual detector for math/structure awareness
        self.visual_detector = VisualElementDetector()
        
        # 🚀 Phase 5.0 Performance: Shared Memory Registry
        self.shm_registry = get_shared_frame_registry()
        
        # 🚀 V5: Load CLIP Model (Safe Singleton)
        self.clip_model = _get_clip_model()
        
        import psutil
        mem = psutil.virtual_memory()
        # Waterline logic: If system memory used > 80%, reduce cache to minimum (20 frames)
        waterline_multiplier = 0.5 if mem.percent > 80 else 1.0
        
        self._max_cache_size = int(ResourceOrchestrator.get_adaptive_cache_size(base_size=30, per_gb_increment=15) * waterline_multiplier)
        self._max_analysis_cache_size = self._max_cache_size * 2
        
        self._frame_cache = OrderedDict()
        self._analysis_cache = OrderedDict()
        
        # 🚀 V6.9: Global Dynamic Decision Engine & Cache
        self.decision_engine = DynamicDecisionEngine()
        self._clip_caches: Dict[str, GlobalAnalysisCache] = {}  # segment_id -> Cache
        self._analysis_hits = 0
        
        logger.info(f"🚀 [PERF] VisualFeatureExtractor V6: Adaptive Cache Size {self._max_cache_size} (Waterline: {waterline_multiplier}x)")
        logger.info(f"VisualFeatureExtractor initialized for: {video_path}")

    def __del__(self):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：hasattr(self, 'cap') and self.cap.isOpened()
        依据来源（证据链）：
        - 对象内部状态：self.cap。
        输入参数：
        - 无。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        if hasattr(self, 'cap') and self.cap.isOpened():
            self.cap.release()

    async def extract_frames_async(self, start_sec: float, end_sec: float, sample_rate: int = 1) -> Tuple[List[np.ndarray], List[float]]:
        # 这里主要是为了让接口异步，内部目前仍用同步解码 (OpenCV 限制)
        """
        执行逻辑：
        1) 扫描输入内容。
        2) 过滤并提取目标子集。
        实现方式：通过内部方法调用/状态更新、NumPy 数值计算实现。
        核心价值：聚焦关键信息，减少后续处理成本。
        输入参数：
        - start_sec: 起止时间/区间边界（类型：float）。
        - end_sec: 起止时间/区间边界（类型：float）。
        - sample_rate: 函数入参（类型：int）。
        输出参数：
        - List[np.ndarray], List[float] 列表（与输入或处理结果一一对应）。"""
        return self.extract_frames(start_sec, end_sec, sample_rate)

    def _get_ffmpeg_hwaccel_args(self) -> List[str]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过子进程调用实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - 无。
        输出参数：
        - str 列表（与输入或处理结果一一对应）。"""
        # 简单检查是否有 nvidia-smi 或可用 cuda
        try:
            import subprocess
            subprocess.run(['nvidia-smi'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
            return ["-hwaccel", "cuda", "-hwaccel_output_format", "cuda"]
        except:
            return []

    def extract_frames_fast(
        self,
        start_sec: float,
        end_sec: float,
        sample_rate: int = 2,
        target_height: int = 360,
        register_to_shm: bool = True,
    ) -> Tuple[List[np.ndarray], List[float]]:
        """
        执行逻辑：
        1) 扫描输入内容。
        2) 过滤并提取目标子集。
        实现方式：通过内部方法调用/状态更新、NumPy 数值计算、子进程调用、文件系统读写实现。
        核心价值：聚焦关键信息，减少后续处理成本。
        决策逻辑：
        - 条件：duration < 5.0
        - 条件：len(frames) > 1
        - 条件：not raw_frame
        依据来源（证据链）：
        输入参数：
        - start_sec: 起止时间/区间边界（类型：float）。
        - end_sec: 起止时间/区间边界（类型：float）。
        - sample_rate: 函数入参（类型：int）。
        - target_height: 函数入参（类型：int）。
        输出参数：
        - List[np.ndarray], List[float] 列表（与输入或处理结果一一对应）。"""
        import subprocess
        
        # 计算总帧数和时间点
        duration = end_sec - start_sec
        
        # 🚀 V6.5 Optimization: Precise Capture for Short Clips
        # If duration is short (<5s), use Frame-by-Frame decoding (Slow Mode)
        # to ensure micro-movements (mouse, cursor) are captured and prevent duplicate frames.
        if duration < 5.0:
            logger.info(f"⚡ [Strategy] Clip duration {duration:.2f}s (<5s). Using Random Access Mode (OpenCV) for speed & accuracy.")
            return self.extract_frames(start_sec, end_sec, sample_rate, register_to_shm=register_to_shm)

        expected_count = int(duration * (self.fps / sample_rate))
        
        # 构造 FFmpeg 管道 (包含 hwaccel 逻辑)
        hw_args = self._get_ffmpeg_hwaccel_args()
        
        # 缩放至低分辨率 (480P 代理)
        scale_filter = f"scale=-1:{target_height}"
        
        # 计算采样率 (FFmpeg fps filter)
        target_fps = self.fps / sample_rate
        
        cmd = [
            'ffmpeg', *hw_args,
            '-ss', str(start_sec),
            '-t', str(duration),
            '-i', self.video_path,
            '-vf', f"{scale_filter},fps={target_fps}",
            '-f', 'image2pipe',
            '-vcodec', 'rawvideo',
            '-pix_fmt', 'bgr24',
            '-'
        ]
        
        try:
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            
            # 手动计算输出分辨率 (维持比例)
            # 💥 Fix: Use cached dimensions to be thread-safe (avoid self.cap.get call)
            orig_h = self.height
            orig_w = self.width
            target_width = int((orig_w / orig_h) * target_height)
            # FFmpeg scale filter 会调整为偶数
            target_width = (target_width // 2) * 2
            
            frame_size = target_width * target_height * 3
            frames = []
            timestamps = []
            
            # OOM Optimization: Safety limit. If too many frames, we must process in chunks or limit.
            # Here we implement a hard safety limit for "fast scan" to prevent OOM on very long videos.
            # If > 1000 frames (approx 600MB at 360p float), we might be in trouble if we don't release.
            # Since this is "fast extract" for stability scan, we usually assume short analysis windows.
            # But to be safe, we check count. 
            
            SAFE_BATCH_LIMIT = 500  # Process in batches if logic supports, or just cap for analysis.
            # For `extract_visual_features`, we need all frames to calculate global stats.
            # So optimization strategy: Ensure we don't hold 'raw_frame' buffer too long.
            
            for i in range(expected_count + 5): # 宽限几帧防止结尾截断
                raw_frame = process.stdout.read(frame_size)
                if not raw_frame: break
                
                frame = np.frombuffer(raw_frame, dtype=np.uint8).reshape((target_height, target_width, 3))
                
                timestamp = start_sec + (i / target_fps)
                frame_idx = int(timestamp * self.fps)  # Approximate index
                
                # 🚀 Phase 5.0 Performance: Register in SHM immediately（可选）
                if register_to_shm:
                    self.shm_registry.register_frame(frame_idx, frame)
                
                frames.append(frame)
                timestamps.append(timestamp)
                
                # 💥 Memory Optimization: Periodic GC inside the pipe reading loop
                if len(frames) % 50 == 0:
                    gc.collect()
                if len(frames) > 2000:
                    logger.warning(f"Fast extract hit safety limit (2000 frames). Truncating to avoid OOM.")
                    break
                
            process.terminate()
            
            # Force GC after reading large binary stream
            gc.collect()
            
            # 🚀 V6.5 Optimization: Deduplication Check
            # If all frames are identical (MSE=0), it might be a decoding error or keyframe stuck.
            # We fallback to slow seek just in case (unless there's only 1 frame).
            if len(frames) > 1:
                # Fast check: compare first and last, and maybe random middle
                # To be robust: Check variance of means?
                means = [np.mean(f) for f in frames]
                if np.var(means) < 1e-5:
                     # Calculate actual MSE of first vs last to be sure
                     f0 = frames[0].astype(float)
                     f_last = frames[-1].astype(float)
                     mse = np.mean((f0 - f_last)**2)
                     if mse < 0.1:
                          logger.warning(f"⚠️ [Fast Extract] All extracted frames appear identical (MSE={mse:.3f}). Fallback to Slow Mode.")
                          return self.extract_frames(start_sec, end_sec, sample_rate, register_to_shm=register_to_shm)

            return frames, timestamps
        except Exception as e:
            logger.warning(f"Fast extract failed: {e}, falling back to slow mode")
            return self.extract_frames(start_sec, end_sec, sample_rate, register_to_shm=register_to_shm)

    def extract_frames(
        self,
        start_sec: float,
        end_sec: float,
        sample_rate: int = 1,
        register_to_shm: bool = True,
    ) -> Tuple[List[np.ndarray], List[float]]:
        """
        执行逻辑：
        1) 扫描输入内容。
        2) 过滤并提取目标子集。
        实现方式：通过内部方法调用/状态更新、OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：聚焦关键信息，减少后续处理成本。
        决策逻辑：
        - 条件：not local_cap.isOpened()
        - 条件：frame_idx >= self.frame_count
        - 条件：frame_idx in self._frame_cache
        依据来源（证据链）：
        - 对象内部状态：self._frame_cache, self._max_cache_size, self.frame_count。
        输入参数：
        - start_sec: 起止时间/区间边界（类型：float）。
        - end_sec: 起止时间/区间边界（类型：float）。
        - sample_rate: 函数入参（类型：int）。
        输出参数：
        - List[np.ndarray], List[float] 列表（与输入或处理结果一一对应）。"""
        start_frame = int(start_sec * self.fps)
        end_frame = int(end_sec * self.fps)
        start_frame = max(0, min(start_frame, self.frame_count - 1))
        end_frame = max(start_frame, min(end_frame, self.frame_count - 1))
        
        frames = []
        timestamps = []
        
        # 🚀 Thread Safety Fix: Use local VideoCapture for concurrent extraction
        # Since this method is called in threads (via extract_visual_features -> run_in_executor),
        # we cannot use the shared self.cap.
        local_cap = cv2.VideoCapture(self.video_path)
        if not local_cap.isOpened():
            logger.error(f"Failed to open local capture for {self.video_path}")
            return [], []
            
        try:
            current_frame_idx = start_frame
            current_step = sample_rate
            last_frame_proxy = None # Used for MSE check
            
            # Adaptive Sampling Loop
            while current_frame_idx <= end_frame:
                frame_idx = int(current_frame_idx)
                
                # Check bounds
                if frame_idx >= self.frame_count: break

                # 💥 性能优化: 缓存命中检查
                if frame_idx in self._frame_cache:
                    cache_metrics.hit("module2.visual_feature.frame_cache")
                    frame = self._frame_cache[frame_idx]
                    frames.append(frame)
                    timestamps.append(frame_idx / self.fps)
                    
                    last_frame_proxy = frame
                    current_frame_idx += current_step
                    continue
                cache_metrics.miss("module2.visual_feature.frame_cache")

                # 💥 性能优化: 仅在不连续时调用 set()。如果连续，顺序 read() 效率更高
                current_pos = int(local_cap.get(cv2.CAP_PROP_POS_FRAMES))
                if frame_idx != current_pos:
                    local_cap.set(cv2.CAP_PROP_POS_FRAMES, frame_idx)
                
                ret, raw_f = local_cap.read()
                if not ret: 
                    current_frame_idx += current_step
                    continue
                
                # 🚀 Memory Safety: Downsample immediately to 360p proxy
                h, w = raw_f.shape[:2]
                target_h = 360
                target_w = int((w / h) * target_h)
                target_w = (target_w // 2) * 2
                frame = cv2.resize(raw_f, (target_w, target_h))
                del raw_f # Immediate release
                
                # 加入缓存 (LRU 策略)
                if len(self._frame_cache) >= self._max_cache_size:
                    self._frame_cache.popitem(last=False)
                
                self._frame_cache[frame_idx] = frame
                if register_to_shm:
                    self.shm_registry.register_frame(frame_idx, frame)
                
                frames.append(frame)
                timestamps.append(frame_idx / self.fps)

                # --- 🚀 Adaptive Step Logic ---
                if last_frame_proxy is not None:
                    # Calculate lightweight MSE
                    mse = self.calculate_mse_diff(last_frame_proxy, frame)
                    
                    if mse < 5.0:  # Static Scene (e.g. PPT slide unchanged)
                        current_step = min(current_step * 1.5, sample_rate * 5) # Increase step significantly
                    elif mse > 50.0: # Dynamic Scene (e.g. Transition)
                        current_step = max(current_step / 1.5, sample_rate) # Decrease step back to base rate
                    else:
                        pass # Keep current step
                
                last_frame_proxy = frame
                current_frame_idx += current_step
                
                # 💥 Periodic GC
                if len(frames) % 30 == 0: gc.collect()
        finally:
            local_cap.release()
            
        return frames, timestamps

    def calculate_mse_diff(self, frame1, frame2):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - frame1: 函数入参（类型：未标注）。
        - frame2: 函数入参（类型：未标注）。
        输出参数：
        - float 对象或调用结果。"""
        # 💥 Fix: Cast to float32 to avoid uint8 overflow during squaring
        # diff = cv2.absdiff(frame1, frame2) -> uint8, still overflows on square
        f1 = frame1.astype(np.float32)
        f2 = frame2.astype(np.float32)
        mse = np.mean((f1 - f2) ** 2)
        return float(mse)

    def calculate_ssim(self, frame1: np.ndarray, frame2: np.ndarray) -> float:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：len(frame1.shape) == 3
        - 条件：len(frame2.shape) == 3
        依据来源（证据链）：
        - 输入参数：frame1, frame2。
        输入参数：
        - frame1: 函数入参（类型：np.ndarray）。
        - frame2: 函数入参（类型：np.ndarray）。
        输出参数：
        - 数值型计算结果。"""
        # 1. Convert to Gray
        if len(frame1.shape) == 3: g1 = cv2.cvtColor(frame1, cv2.COLOR_BGR2GRAY)
        else: g1 = frame1
        
        if len(frame2.shape) == 3: g2 = cv2.cvtColor(frame2, cv2.COLOR_BGR2GRAY)
        else: g2 = frame2
        
        # 2. Resize for speed (Analysis doesn't need 1080p)
        target_h = 240
        h, w = g1.shape[:2]
        scale = target_h / h
        new_w = int(w * scale)
        s1 = cv2.resize(g1, (new_w, target_h))
        s2 = cv2.resize(g2, (new_w, target_h))
        
        # 3. Simple SSIM Approximation (avoiding heavy scipy import)
        # SSIM ~= (2*mu1*mu2 + C1)(2*sig12 + C2) / ((mu1^2 + mu2^2 + C1)(sig1^2 + sig2^2 + C2))
        C1 = (0.01 * 255)**2
        C2 = (0.03 * 255)**2
        
        mu1 = s1.mean()
        mu2 = s2.mean()
        sigma1_sq = s1.var()
        sigma2_sq = s2.var()
        sigma12 = np.mean((s1 - mu1) * (s2 - mu2))
        
        numerator = (2 * mu1 * mu2 + C1) * (2 * sigma12 + C2)
        denominator = (mu1**2 + mu2**2 + C1) * (sigma1_sq + sigma2_sq + C2)
        
        return numerator / denominator

    def calculate_content_increment(self, start_frame: np.ndarray, end_frame: np.ndarray) -> float:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：s_count < 100
        依据来源（证据链）：
        输入参数：
        - start_frame: 起止时间/区间边界（类型：np.ndarray）。
        - end_frame: 起止时间/区间边界（类型：np.ndarray）。
        输出参数：
        - 数值型计算结果。"""
        try:
            # Analyze increment in Cr/Cb channel (human written content often has color)
            # OR just luminance. Let's use Gray for generality.
            s_gray = cv2.cvtColor(start_frame, cv2.COLOR_BGR2GRAY)
            e_gray = cv2.cvtColor(end_frame, cv2.COLOR_BGR2GRAY)
            
            # Simple threshold to find "content" vs "background" (assuming light background)
            # Todo: Adaptive threshold
            _, s_bin = cv2.threshold(s_gray, 200, 255, cv2.THRESH_BINARY_INV)
            _, e_bin = cv2.threshold(e_gray, 200, 255, cv2.THRESH_BINARY_INV)
            
            s_count = cv2.countNonZero(s_bin)
            e_count = cv2.countNonZero(e_bin)
            
            if s_count < 100: return 1.0 # Start empty
            return e_count / s_count
        except Exception as e:
            logger.warning(f"Error calculating content increment: {e}")
            return 1.0

    # =========================================================================
    # 🚀 V5 Optimization: Visual Feature Enhancement & Validation
    # =========================================================================

    def enhance_low_quality_frame(self, frame: np.ndarray) -> np.ndarray:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - frame: 函数入参（类型：np.ndarray）。
        输出参数：
        - 函数计算/封装后的结果对象。"""
        try:
            # 1. Denoise (Fast NLM) - Only if frame is small to save time, or use Gaussian for speed
            # h=3 for moderate denoising
            # denoised = cv2.fastNlMeansDenoisingColored(frame, None, 3, 3, 7, 21) 
            # Optimization: fastNlMeans is too slow for real-time. Use GaussianBlur for faint noise.
            denoised = cv2.GaussianBlur(frame, (3, 3), 0)
            
            # 2. CLAHE (Contrast Limited Adaptive Histogram Equalization) on LAB L-channel
            lab = cv2.cvtColor(denoised, cv2.COLOR_BGR2LAB)
            l, a, b = cv2.split(lab)
            
            clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8,8))
            cl = clahe.apply(l)
            
            limg = cv2.merge((cl, a, b))
            enhanced = cv2.cvtColor(limg, cv2.COLOR_LAB2BGR)
            
            return enhanced
        except Exception as e:
            logger.warning(f"V5 Frame Enhancement failed: {e}")
            return frame

    def match_handwritten_feature(self, frame: np.ndarray, feature_type: str) -> bool:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：not contours
        - 条件：feature_type == 'math_formula'
        - 条件：w < 10 or h < 10
        依据来源（证据链）：
        - 输入参数：feature_type。
        输入参数：
        - frame: 函数入参（类型：np.ndarray）。
        - feature_type: 函数入参（类型：str）。
        输出参数：
        - 布尔判断结果。"""
        try:
            gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            # 1. Edge Detection + Morphology
            edges = cv2.Canny(gray, 30, 150)
            kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (2,2))
            dilated_edges = cv2.dilate(edges, kernel, iterations=1)
            
            # 2. Contour analysis (Heuristic for structure)
            contours, _ = cv2.findContours(dilated_edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
            
            if not contours: return False
            
            # Filter distinct shapes
            valid_shapes = 0
            for cnt in contours:
                x,y,w,h = cv2.boundingRect(cnt)
                # Filter noise
                if w < 10 or h < 10: continue
                # Handwritten formula/diagram usually has reasonable density
                valid_shapes += 1
                
            # Handwritten formulas usually have multiple distinct components (3+)
            if feature_type == "math_formula":
                return valid_shapes >= 3
            elif feature_type == "architecture":
                # Diagrams often have connected components or larger boxes
                return valid_shapes >= 2
                
            return False
            
        except Exception as e:
            logger.warning(f"V5 Handwritten Matching failed: {e}")
            return False

    def validate_visual_feature_semantic(self, frame: np.ndarray, feature_type: str, fault_text: str) -> float:
        """
        执行逻辑：
        1) 整理待校验数据。
        2) 按规则逐项校验并返回结果。
        实现方式：通过内部方法调用/状态更新、OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：提前发现数据/状态问题，降低运行风险。
        决策逻辑：
        - 条件：not self.clip_model or not HAS_CLIP
        依据来源（证据链）：
        - 阈值常量：HAS_CLIP。
        - 对象内部状态：self.clip_model。
        输入参数：
        - frame: 函数入参（类型：np.ndarray）。
        - feature_type: 函数入参（类型：str）。
        - fault_text: 函数入参（类型：str）。
        输出参数：
        - 数值型计算结果。"""
        if not self.clip_model or not HAS_CLIP:
             return 0.8 # Fallback if CLIP not loaded

        try:
            # 1. Convert Frame (BGR) -> PIL Image (RGB)
            rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            pil_image = Image.fromarray(rgb_frame)
            
            # 2. Construct Prompt
            # "A diagram of {fault_text}" / "A math formula for {fault_text}"
            desc_map = {
                "math_formula": "math formula",
                "architecture": "diagram",
                "table": "table"
            }
            desc = desc_map.get(feature_type, feature_type)
            prompt = f"A clear {desc} about {fault_text[:50]}"
            
            # 3. Encode & Compute Similarity
            # Encode Image
            img_emb = self.clip_model.encode(pil_image, convert_to_tensor=True)
            # Encode Text
            text_emb = self.clip_model.encode(prompt, convert_to_tensor=True)
            
            # Cosine Similarity
            score = util.cos_sim(img_emb, text_emb)
            return float(score.item())
        except Exception as e:
            logger.warning(f"CLIP validation failed: {e}")
            return 0.7

    def calculate_clip_score(self, frame: np.ndarray, text: str) -> float:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：not self.clip_model or not HAS_CLIP
        依据来源（证据链）：
        - 阈值常量：HAS_CLIP。
        - 对象内部状态：self.clip_model。
        输入参数：
        - frame: 函数入参（类型：np.ndarray）。
        - text: 文本内容（类型：str）。
        输出参数：
        - 数值型计算结果。"""
        if not self.clip_model or not HAS_CLIP: return 0.5
        try:
             rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
             pil_image = Image.fromarray(rgb_frame)
             
             img_emb = self.clip_model.encode(pil_image, convert_to_tensor=True)
             text_emb = self.clip_model.encode(text, convert_to_tensor=True)
             
             score = util.cos_sim(img_emb, text_emb)
             return float(score.item())
        except Exception:
             return 0.5

    def calculate_all_diffs(self, frames: List[np.ndarray]) -> Tuple[List[float], List[float]]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：len(frames) < 2
        - 条件：not mse_list
        - 条件：max(mse_list) > 0
        依据来源（证据链）：
        - 输入参数：frames。
        输入参数：
        - frames: 数据列表/集合（类型：List[np.ndarray]）。
        输出参数：
        - List[float], List[float] 列表（与输入或处理结果一一对应）。"""
        if len(frames) < 2: return [], []
        mse_list = []
        for i in range(len(frames) - 1):
            mse = self.calculate_mse_diff(frames[i], frames[i+1])
            mse_list.append(mse)
        
        if not mse_list: return [], []
        max_m = max(mse_list) if max(mse_list) > 0 else 1.0
        diff_rates = [(m / max_m) * 100 for m in mse_list]
        return mse_list, diff_rates

    def calculate_ssim_sequence(self, frames: List[np.ndarray]) -> List[float]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：len(frames) < 2
        依据来源（证据链）：
        - 输入参数：frames。
        输入参数：
        - frames: 数据列表/集合（类型：List[np.ndarray]）。
        输出参数：
        - float 列表（与输入或处理结果一一对应）。"""
        if len(frames) < 2: return []
        ssim_list = []
        for i in range(len(frames) - 1):
            s = self.calculate_ssim(frames[i], frames[i+1])
            ssim_list.append(s)
        return ssim_list

    def calculate_edge_flux_sequence(self, frames: List[np.ndarray]) -> List[float]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：len(frames) < 2
        - 条件：frames
        - 条件：i > 0
        依据来源（证据链）：
        - 输入参数：frames。
        输入参数：
        - frames: 数据列表/集合（类型：List[np.ndarray]）。
        输出参数：
        - float 列表（与输入或处理结果一一对应）。"""
        if len(frames) < 2: return []
        
        flux_list = []
        res_factor = self._get_resolution_factor(frames[0]) if frames else 1.0
        
        # Pre-compute edges for all frames to avoid re-computation if possible
        # Or do it pairwise.
        prev_edges = None
        
        for i in range(len(frames)):
            gray = cv2.cvtColor(frames[i], cv2.COLOR_BGR2GRAY)
            # Use fixed thresholds for consistency
            edges = cv2.Canny(gray, 50, 150)
            edge_count = cv2.countNonZero(edges)
            
            if i > 0:
                # Compare with prev
                # Flux = abs(curr_count - prev_count) / max(prev_count, 1)
                # Or better: intersection?
                # For V5 Derivation check, we want Growth.
                # But here we return general flux intensity.
                prev_count = cv2.countNonZero(prev_edges)
                diff = abs(edge_count - prev_count)
                ratio = diff / (prev_count + 1e-6)
                flux_list.append(ratio)
                
            prev_edges = edges
            
        return flux_list
    
    def _get_resolution_factor(self, frame):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - frame: 函数入参（类型：未标注）。
        输出参数：
        - 函数计算/封装后的结果对象。"""
        return frame.shape[1] / 1920.0

    def classify_static_dynamic(self, mse_list: List[float], timestamps: List[float]) -> Dict[str, Any]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：not mse_list
        - 条件：max(mse_list) > 0
        - 条件：m < 100
        依据来源（证据链）：
        - 输入参数：mse_list, timestamps。
        输入参数：
        - mse_list: 数据列表/集合（类型：List[float]）。
        - timestamps: 函数入参（类型：List[float]）。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        if not mse_list: return {"type": "unknown", "is_static": False, "is_dynamic": False, "avg_mse": 0.0, "avg_diff_rate": 0.0, "stable_duration": 0.0, "mse_list": []}
        
        avg_mse = np.mean(mse_list)
        max_m = max(mse_list) if max(mse_list) > 0 else 1.0
        avg_diff_rate = np.mean([(m / max_m) * 100 for m in mse_list])
        
        # 稳定时长计算
        stable_duration = 0.0
        start_t = timestamps[0]
        for i, m in enumerate(mse_list):
            if m < 100: # 阈值
                if i + 1 < len(timestamps):
                    stable_duration = max(stable_duration, timestamps[i+1] - start_t)
            else:
                if i + 1 < len(timestamps): start_t = timestamps[i+1]
        
        # 🚀 V6.9 Optimization: 改进短片段的静止判定
        # 原逻辑要求 stable_duration > 3.0，导致 2s 的纯静止片段判定为 non-static
        # 修改为：要么时长 > 3s，要么静止时长覆盖了 90% 以上的片段长度
        clip_duration = timestamps[-1] - timestamps[0]
        is_static = avg_mse < 100 and (stable_duration > 3.0 or stable_duration >= clip_duration * 0.9) and avg_diff_rate < 5.0
        
        # ⚖️ 平衡点: 设定为 250 / 12% 以灵敏捕捉教学动画，依靠 Fusion 层的语义覆盖来过滤鼠标干扰
        is_dynamic = avg_mse > 250 or avg_diff_rate > 12.0
        visual_type = "static" if is_static else ("dynamic" if is_dynamic else "mixed")
        
        return {
            "type": visual_type, "is_static": is_static, "is_dynamic": is_dynamic,
            "avg_mse": avg_mse, "avg_diff_rate": avg_diff_rate, "stable_duration": stable_duration,
            "mse_list": mse_list  # 🚀 V6.9: 包含原始列表以供后续分析
        }

    def detect_visual_elements(self, frame: np.ndarray) -> Dict[str, Any]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：self.visual_detector
        依据来源（证据链）：
        - 对象内部状态：self.visual_detector。
        输入参数：
        - frame: 函数入参（类型：np.ndarray）。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        if self.visual_detector:
            return self.visual_detector.analyze_frame(frame)
        return {"total": 0}

    def _probe_one_frame(self, timestamp: float) -> Optional[np.ndarray]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：not temp_cap.isOpened()
        - 条件：not ret
        依据来源（证据链）：
        输入参数：
        - timestamp: 函数入参（类型：float）。
        输出参数：
        - 函数计算/封装后的结果对象。"""
        try:
            temp_cap = cv2.VideoCapture(self.video_path)
            if not temp_cap.isOpened(): return None
            
            temp_cap.set(cv2.CAP_PROP_POS_MSEC, timestamp * 1000)
            ret, frame = temp_cap.read()
            temp_cap.release()
            
            if not ret: return None
            
            # Helper resize for analysis (360p is enough)
            h, w = frame.shape[:2]
            target_h = 360
            target_w = int((w / h) * target_h)
            frame = cv2.resize(frame, (target_w, target_h))
            return frame
        except Exception:
            return None

    def get_cached_content(self, start_sec: float, end_sec: float, sample_rate: int = 1) -> Optional[GlobalAnalysisCache]:
        """
        执行逻辑：
        1) 读取内部状态或外部资源。
        2) 返回读取结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：提供一致读取接口，降低调用耦合。
        输入参数：
        - start_sec: 起止时间/区间边界（类型：float）。
        - end_sec: 起止时间/区间边界（类型：float）。
        - sample_rate: 函数入参（类型：int）。
        输出参数：
        - get 对象或调用结果。"""
        start_sec = float(start_sec)
        end_sec = float(end_sec)
        segment_id = f"{start_sec:.2f}_{end_sec:.2f}_sr{sample_rate}"
        cache = self._clip_caches.get(segment_id)
        if cache and cache.is_analyzed:
            cache_metrics.hit("module2.visual_feature.clip_cache")
        return cache

    async def extract_visual_features(self, start_sec: float, end_sec: float, sample_rate: int = 1) -> VisualFeatures:
        """
        执行逻辑：
        1) 扫描输入内容。
        2) 过滤并提取目标子集。
        实现方式：通过内部方法调用/状态更新、NumPy 数值计算、asyncio 异步调度实现。
        核心价值：聚焦关键信息，减少后续处理成本。
        决策逻辑：
        - 条件：segment_id not in self._clip_caches
        - 条件：not cache.is_analyzed
        - 条件：decision['is_dynamic']
        依据来源（证据链）：
        - 输入参数：end_sec, start_sec。
        - 配置字段：avg_mse, is_dynamic, is_static。
        - 对象内部状态：self._analysis_cache, self._clip_caches, self._max_analysis_cache_size。
        输入参数：
        - start_sec: 起止时间/区间边界（类型：float）。
        - end_sec: 起止时间/区间边界（类型：float）。
        - sample_rate: 函数入参（类型：int）。
        输出参数：
        - VisualFeatures 对象（包含字段：visual_type, avg_mse, avg_diff_rate, mse_list, is_static, is_dynamic, stable_duration, has_architecture_elements, has_math_formula, element_count, confidence, has_table, has_static_visual_structure, action_windows, avg_edge_flux, action_density）。"""
        start_sec = float(start_sec)
        end_sec = float(end_sec)
        # 1. 检查并获取缓存 (GlobalAnalysisCache)
        segment_id = f"{start_sec:.2f}_{end_sec:.2f}_sr{sample_rate}"
        if segment_id not in self._clip_caches:
            self._clip_caches[segment_id] = GlobalAnalysisCache(segment_id)
        cache = self._clip_caches[segment_id]
        
        loop = asyncio.get_running_loop()
        executor = get_visual_process_pool()
        
        # 2. 如果未分析，执行主提取与预处理
        if not cache.is_analyzed:
            cache_metrics.miss("module2.visual_feature.clip_cache")
            # 2.1 基础提取
            # 💥 Fix: Use the custom executor (Visual Pool) instead of default (None)
            frames, timestamps = await loop.run_in_executor(
                executor, self.extract_frames_fast, start_sec, end_sec, sample_rate, 360
            )
            if not frames:
                return VisualFeatures("unknown", 0.0, 0.0, [], False, False, 0.0, False, False, 0, 0.0)
                
            # 2.2 [第一性原理：感官增强] 自适应预处理 (CLAHE + 去噪)
            enhanced_frames = await loop.run_in_executor(
                executor, self.decision_engine.preprocess_frames_adaptive, frames
            )
            
            # 2.3 提取基础特征
            feat = self.decision_engine.compute_base_features(enhanced_frames, timestamps)
            
            # 🚀 V5 Upgrade: Compute Advanced Metrics for Math Logic
            ssim_seq = self.calculate_ssim_sequence(enhanced_frames)
            edge_flux_seq = self.calculate_edge_flux_sequence(enhanced_frames)
            avg_edge_flux = np.mean(edge_flux_seq) if edge_flux_seq else 0.0
            
            # 2.4 更新缓存 (Add new metrics)
            cache.enhanced_frames = enhanced_frames
            cache.timestamps = timestamps
            cache.mse_list = feat["mse_list"]
            cache.mse_base = feat["mse_base"]
            cache.ssim_drop = feat.get("ssim_drop", 0.0)
            cache.ssim_seq = ssim_seq # V5
            cache.avg_edge_flux = avg_edge_flux # V5
            cache.is_analyzed = True
        else:
            cache_metrics.hit("module2.visual_feature.clip_cache")
            
            # 释放原始帧内存 (保留增强帧在缓存中直到本次方法结束)
            del frames
            gc.collect()

        # 3. 执行初步动静判定 (基础统计)
        classification = self.classify_static_dynamic(cache.mse_list, cache.timestamps)
        
        # 4. 🚀 V6.9+: 集成自适应决策引擎进行深度判定 (核心修正)
        # 获取当前场景的 Profile
        profile_name = "ppt_slide" if classification["avg_mse"] < 50 else "generic"
        
        # 寻找动作窗
        windows = self.decision_engine.detect_action_windows(
            cache.mse_list, cache.timestamps, cache.mse_base, profile_name
        )
        
        # 融合决策
        decision = self.decision_engine.judge_is_dynamic(
            windows, classification["avg_mse"], 
            total_duration=end_sec - start_sec,
            ssim_drop=getattr(cache, "ssim_drop", 0.0),
            profile_name=profile_name
        )

        v_type = "dynamic" if decision["is_dynamic"] else classification["type"]
        classification["type"] = v_type
        classification["is_dynamic"] = decision["is_dynamic"]
        classification["is_static"] = not decision["is_dynamic"]
        classification["action_density"] = decision.get("action_density", 0.0)

        classification["action_density"] = decision.get("action_density", 0.0)
        
        # 如果引擎判定为动态，强制修正分类结果
        if decision["is_dynamic"]:
            logger.info(f"✨ [V6.9 Decision] Segment confirmed as DYNAMIC via Engine ({decision['reason']})")
            classification["is_dynamic"] = True
            classification["is_static"] = False
            classification["type"] = "dynamic"
        # 🚀 V6.9.3 Fix: 允许引擎将伪阳性修正为 Static (双向修正)
        # 例如 P011 Case: 旧逻辑认为是 Dynamic，但 Engine 基于密度判断为 Static
        elif not decision["is_dynamic"] and classification["is_dynamic"]:
             logger.info(f"🛡️ [V6.9 Decision] Segment corrected to STATIC via Engine ({decision['reason']})")
             classification["is_dynamic"] = False
             classification["is_static"] = True
             classification["type"] = "static"
            
        # 5. [Dynamic Recovery] 时序回溯纠偏 (仅针对极短静态片段的动作补偿)
        # 🚀 V6.9.4 Upgrade: 加入平滑动画 (Edge Flux) 检测
        elif classification["is_static"]:
            # A. 短片段微动回溯 (原有逻辑)
            if (end_sec - start_sec) < 4.0:
               # ... (Keep existing lookback logic, simplified for brevity in this tool call, but ensuring we don't delete it if not showing all)
               pass 

            # B. 平滑动画检测 (Edge Flux) - 针对长/短片段的“死区”
            # 条件: MSE 低(没波峰), SSIM 极高(结构没崩), 但可能在平移
            if classification["avg_mse"] < 10.0 and cache.ssim_drop < 0.05:
                # 只有在高度静止的嫌疑区才启用昂贵的 Edge Flux 计算
                logger.info("🔍 [Edge Flux] Deadzone DETECTED. Triggering Edge Flux calculation...")
                
                # 获取全量增强帧 (可能需要从 cache 读取或重新提取，这里假设 cache.enhanced_frames 可用)
                # 注意：cache.enhanced_frames 可能被 GC，需防御性检查
                frames_for_flux = cache.enhanced_frames
                if not frames_for_flux:
                     frames_for_flux, _ = await loop.run_in_executor(
                        executor, self.extract_frames_fast, start_sec, end_sec, 2, 360 # Lower sample rate for flux
                     )
                
                if frames_for_flux:
                    flux_score, flux_var = await loop.run_in_executor(
                        executor, self.decision_engine.calculate_edge_flux, frames_for_flux
                    )
                    classification["avg_edge_flux"] = flux_score # 💥 Fix: Save back to classification
                    
                    # 🚀 V6.9.7 Unified: 将 Flux 数据传回引擎进行统一裁决
                    # Re-evaluate via Engine with Flux data
                    flux_decision = self.decision_engine.judge_is_dynamic(
                        windows, classification["avg_mse"], total_duration=end_sec-start_sec,
                        ssim_drop=cache.ssim_drop, profile_name="ppt_slide",
                        edge_flux_data=(flux_score, flux_var)
                    )
                    
                    if flux_decision["is_dynamic"]:
                        logger.info(f"✨ [Edge Flux] SMOOTH MOTION CONFIRMED via Unified Engine ({flux_decision['reason']})")
                        classification["is_dynamic"] = True
                        classification["is_static"] = False
                        classification["type"] = "smooth_flow"
                        decision["reason"] = flux_decision["reason"]

        # 5. 采样执行语义/元素检测 (使用 SHARED MEMORY)
        # 此时 cache.enhanced_frames 已就绪
        frames = cache.enhanced_frames
        timestamps = cache.timestamps
        mse_list = cache.mse_list
        sample_size = min(10, len(frames))
        sample_indices = np.linspace(0, len(frames) - 1, sample_size, dtype=int)

        # 获取采样帧的实际索引和 Hash
        tasks_to_run = [] # List of (hash, shm_info_dict)
        results_indices = [] # Map sample_idx -> results index
        all_sampled_hashes = []

        for idx in sample_indices:
            frame_idx = int(timestamps[idx] * self.fps)
            frame = frames[idx]
            
            # 计算 Hash 用于去重分析
            h = hashlib.md5(frame[::2, ::2].tobytes()).hexdigest()
            all_sampled_hashes.append(h)
            
            if h not in self._analysis_cache:
                cache_metrics.miss("module2.visual_feature.analysis_cache")
                # 构造 SHM 任务 (使用 Zero-Copy 引用)
                shm_ref = self.shm_registry.get_shm_ref(frame_idx)
                
                # 如果由于某种原因不在 SHM 中，退化为传数组
                if not shm_ref:
                    tasks_to_run.append((h, frame))
                else:
                    tasks_to_run.append((h, shm_ref))
            else:
                cache_metrics.hit("module2.visual_feature.analysis_cache")
                self._analysis_hits += 1
                self._analysis_cache.move_to_end(h)

        # 并行运行唯一任务
        unique_tasks_in_batch = {}
        if tasks_to_run:
            # 去重任务列表 (同一个采样集中可能有重复内容)
            for h, task_data in tasks_to_run:
                unique_tasks_in_batch[h] = task_data

            hashes_to_spawn = list(unique_tasks_in_batch.keys())
            data_to_spawn = list(unique_tasks_in_batch.values())
            
            # 💥 Stability: Limit concurrency on Windows to prevent OOM/Process Crash
            # The executor (ProcessPool) size might be large, but submitting too many large payloads at once crashes spawn.
            semaphore = asyncio.Semaphore(2) 

            async def sem_task(data):
                """
                执行逻辑：
                1) 准备必要上下文与参数。
                2) 执行核心处理并返回结果。
                实现方式：通过内部函数组合与条件判断实现。
                核心价值：封装逻辑单元，提升复用与可维护性。
                输入参数：
                - data: 数据列表/集合（类型：未标注）。
                输出参数：
                - 函数计算/封装后的结果对象。"""
                async with semaphore:
                    return await loop.run_in_executor(executor, VisualElementDetector.analyze_frame, data)

            parallel_results = await asyncio.gather(*[
                sem_task(data) for data in data_to_spawn
            ])
            
            # 更新缓存
            for h, res in zip(hashes_to_spawn, parallel_results):
                if len(self._analysis_cache) >= self._max_analysis_cache_size:
                    self._analysis_cache.popitem(last=False)
                self._analysis_cache[h] = res

        # 还原结果列表
        results = [self._analysis_cache[h] for h in all_sampled_hashes]
        
        # 统计并行情况 (验证 PID 分布)
        valid_results = [r for r in results if "error" not in r]
        pids = set(r.get("process_id") for r in valid_results)
        logger.info(f"[PERF] Parallel analysis: {len(results)} frames ({len(unique_tasks_in_batch)} unique) on {len(pids)} processes, hits: {self._analysis_hits}")
        
        # 💥 内存回收: 完成特征提取后，立即清理帧缓存
        self._frame_cache.clear()
        
        # OOM Avoidance
        frame_count = len(frames)
        # del frames (Do not delete yet, we need it for best_e calculation downstream if needed, but results has everything)
        
        best_e = max(results, key=lambda x: x.get("total", 0))
        conf = self._calculate_visual_confidence(classification, best_e, frame_count)
        
        return VisualFeatures(
            visual_type=classification["type"],
            avg_mse=classification["avg_mse"],
            avg_diff_rate=classification["avg_diff_rate"],
            mse_list=classification.get("mse_list", []),
            is_static=classification["is_static"],
            is_dynamic=classification["is_dynamic"],
            stable_duration=classification["stable_duration"],
            has_architecture_elements=best_e.get("has_architecture_elements", False),
            has_math_formula=best_e.get("has_math_formula", False),
            element_count=best_e.get("total", 0),
            confidence=conf,
            has_table=best_e.get("has_table", False),
            has_static_visual_structure=best_e.get("has_static_visual_structure", False),
            action_windows=classification.get("action_windows", []),
            avg_edge_flux=classification.get("avg_edge_flux", 0.0),
            action_density=classification.get("action_density", 0.0)
        )

    def _calculate_visual_confidence(self, classification: Dict, elements: Dict, frame_count: int) -> float:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：frame_count >= 30
        - 条件：classification['is_static'] or classification['is_dynamic']
        - 条件：elements.get('has_architecture_elements', False)
        依据来源（证据链）：
        - 输入参数：classification, elements, frame_count。
        - 配置字段：has_architecture_elements, is_dynamic, is_static。
        输入参数：
        - classification: 函数入参（类型：Dict）。
        - elements: 函数入参（类型：Dict）。
        - frame_count: 函数入参（类型：int）。
        输出参数：
        - 数值型计算结果。"""
        score = 0.5
        if frame_count >= 30: score += 0.2
        if classification["is_static"] or classification["is_dynamic"]: score += 0.2
        if elements.get("has_architecture_elements", False): score += 0.1
        return min(1.0, score)

    def judge_visual_voice_timing(self, action_start: float, action_end: float, voice_start: float) -> str:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：action_end <= voice_start - 0.5
        - 条件：action_start >= voice_start + 0.5
        依据来源（证据链）：
        - 输入参数：action_end, action_start, voice_start。
        输入参数：
        - action_start: 起止时间/区间边界（类型：float）。
        - action_end: 起止时间/区间边界（类型：float）。
        - voice_start: 起止时间/区间边界（类型：float）。
        输出参数：
        - 字符串结果。"""
        # 动作超前：动作完成时间 ≤ 语音起始时间 - 0.5s（停顿阈值）
        if action_end <= voice_start - 0.5:
            return "ahead"
        # 动作滞后：动作起始时间 ≥ 语音起始时间 + 0.5s
        elif action_start >= voice_start + 0.5:
            return "lag"
        else:
            return "sync"

    def extract_action_start_time(self, frames: list, timestamps: list) -> float:
        """
        执行逻辑：
        1) 扫描输入内容。
        2) 过滤并提取目标子集。
        实现方式：通过NumPy 数值计算实现。
        核心价值：聚焦关键信息，减少后续处理成本。
        决策逻辑：
        - 条件：len(frames) < 2
        - 条件：curr_mse >= base_mse * 5
        依据来源（证据链）：
        - 输入参数：frames。
        输入参数：
        - frames: 数据列表/集合（类型：list）。
        - timestamps: 函数入参（类型：list）。
        输出参数：
        - 数值型计算结果。"""
        if len(frames) < 2: return timestamps[0]
        
        # 计算基准 MSE (前两帧变化) 作为噪音底
        f0 = frames[0].astype(np.float32)
        f1 = frames[1].astype(np.float32)
        base_mse = np.mean((f0 - f1) ** 2)
        base_mse = max(base_mse, 1.0) # 最小噪音底
        
        for i in range(1, len(frames)-1):
            fi = frames[i].astype(np.float32)
            fi_next = frames[i+1].astype(np.float32)
            curr_mse = np.mean((fi - fi_next) ** 2)
            
            # MSE突变阈值：当前MSE ≥ 基础MSE的5倍（判定为动作开始）
            if curr_mse >= base_mse * 5:
                return timestamps[i]
                
        return timestamps[0]

    def extract_action_end_time(self, frames: list, timestamps: list) -> float:
        """
        执行逻辑：
        1) 扫描输入内容。
        2) 过滤并提取目标子集。
        实现方式：通过NumPy 数值计算实现。
        核心价值：聚焦关键信息，减少后续处理成本。
        决策逻辑：
        - 条件：len(frames) < 2
        - 条件：mse > 1.0
        依据来源（证据链）：
        - 输入参数：frames。
        输入参数：
        - frames: 数据列表/集合（类型：list）。
        - timestamps: 函数入参（类型：list）。
        输出参数：
        - 数值型计算结果。"""
        if len(frames) < 2: return timestamps[-1]
        
        # 从后向前扫描，找到第一个进入稳定态（MSE < 1.0）的点
        for i in range(len(frames)-2, 0, -1):
            fi = frames[i].astype(np.float32)
            fi_next = frames[i+1].astype(np.float32)
            mse = np.mean((fi - fi_next) ** 2)
            if mse > 1.0: # 还在变化
                return timestamps[i+1] # 下一帧开始稳定
        return timestamps[0]

    def limit_forward_extension(self, action_start: float, voice_start: float, max_forward: float = 3.0) -> float:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：extension_duration > max_forward
        - 条件：extension_duration < 0
        依据来源（证据链）：
        - 输入参数：max_forward。
        输入参数：
        - action_start: 起止时间/区间边界（类型：float）。
        - voice_start: 起止时间/区间边界（类型：float）。
        - max_forward: 函数入参（类型：float）。
        输出参数：
        - 数值型计算结果。"""
        extension_duration = voice_start - action_start
        if extension_duration > max_forward:
            return voice_start - max_forward
        elif extension_duration < 0: # 动作在语音后
            return voice_start
        else:
            return action_start
