"""
模块说明：Module2 内容增强中的 resource_manager 模块。
执行逻辑：
1) 聚合本模块的类/函数，对外提供核心能力。
2) 通过内部调用与外部依赖完成具体处理。
实现方式：通过模块内函数组合与外部依赖调用实现。
核心价值：统一模块职责边界，降低跨文件耦合成本。
输入：
- 调用方传入的参数与数据路径。
输出：
- 各函数/类返回的结构化结果或副作用。"""

import os
import cv2
import atexit
import logging
import threading
from typing import Dict, Optional
from concurrent.futures import ThreadPoolExecutor
from services.python_grpc.src.content_pipeline.infra.runtime import cache_metrics

logger = logging.getLogger(__name__)


class ResourceManager:
    """类说明：ResourceManager 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    
    _instance: Optional["ResourceManager"] = None
    _lock = threading.Lock()
    
    # 默认配置
    DEFAULT_IO_WORKERS = 4      # IO线程数
    
    def __new__(cls):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：cls._instance is None
        依据来源（证据链）：
        输入参数：
        - 无。
        输出参数：
        - 函数计算/封装后的结果对象。"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """
        执行逻辑：
        1) 解析配置或依赖，准备运行环境。
        2) 初始化对象状态、缓存与依赖客户端。
        实现方式：通过内部方法调用/状态更新、OpenCV 图像处理、线程池并发实现。
        核心价值：在初始化阶段固化依赖，保证运行稳定性。
        决策逻辑：
        - 条件：self._initialized
        依据来源（证据链）：
        - 对象内部状态：self._initialized。
        输入参数：
        - 无。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        if self._initialized:
            return
        
        self._initialized = True
        self._io_executor: Optional[ThreadPoolExecutor] = None
        self._video_captures: Dict[str, cv2.VideoCapture] = {}
        self._video_locks: Dict[str, threading.Lock] = {}
        
        # 注册退出时清理
        atexit.register(self.shutdown)
        
        logger.info("ResourceManager initialized (singleton)")
    
    # ==========================================================================
    # IO 线程池 (ffmpeg, 文件读写)
    # ==========================================================================
    
    def get_io_executor(self, max_workers: int = None) -> ThreadPoolExecutor:
        """
        执行逻辑：
        1) 读取内部状态或外部资源。
        2) 返回读取结果。
        实现方式：通过内部方法调用/状态更新、线程池并发实现。
        核心价值：提供一致读取接口，降低调用耦合。
        决策逻辑：
        - 条件：self._io_executor is None
        依据来源（证据链）：
        - 对象内部状态：self._io_executor。
        输入参数：
        - max_workers: 函数入参（类型：int）。
        输出参数：
        - 函数计算/封装后的结果对象。"""
        if self._io_executor is None:
            workers = max_workers or self.DEFAULT_IO_WORKERS
            self._io_executor = ThreadPoolExecutor(
                max_workers=workers,
                thread_name_prefix="io_worker"
            )
            logger.info(f"Created IO ThreadPoolExecutor with {workers} workers")
        return self._io_executor
    
    # ==========================================================================
    # 视频资源管理 (复用 VideoCapture)
    # ==========================================================================
    
    def get_video_capture(self, video_path: str) -> cv2.VideoCapture:
        """
        执行逻辑：
        1) 读取内部状态或外部资源。
        2) 返回读取结果。
        实现方式：通过内部方法调用/状态更新、OpenCV 图像处理、文件系统读写实现。
        核心价值：提供一致读取接口，降低调用耦合。
        决策逻辑：
        - 条件：abs_path not in self._video_captures
        - 条件：not cap.isOpened()
        依据来源（证据链）：
        - 对象内部状态：self._video_captures。
        输入参数：
        - video_path: 文件路径（类型：str）。
        输出参数：
        - 函数计算/封装后的结果对象。"""
        abs_path = os.path.abspath(video_path)
        
        if abs_path in self._video_captures:
            cache_metrics.hit("module2.resource.video_capture")
            return self._video_captures[abs_path]

        cache_metrics.miss("module2.resource.video_capture")
        cap = cv2.VideoCapture(abs_path)
        if not cap.isOpened():
            raise RuntimeError(f"Cannot open video: {video_path}")
        self._video_captures[abs_path] = cap
        self._video_locks[abs_path] = threading.Lock()
        logger.debug(f"Opened video: {abs_path}")
        
        return self._video_captures[abs_path]
    
    def get_video_lock(self, video_path: str) -> threading.Lock:
        """
        执行逻辑：
        1) 读取内部状态或外部资源。
        2) 返回读取结果。
        实现方式：通过内部方法调用/状态更新、文件系统读写实现。
        核心价值：提供一致读取接口，降低调用耦合。
        决策逻辑：
        - 条件：abs_path not in self._video_locks
        依据来源（证据链）：
        - 对象内部状态：self._video_locks。
        输入参数：
        - video_path: 文件路径（类型：str）。
        输出参数：
        - 函数计算/封装后的结果对象。"""
        abs_path = os.path.abspath(video_path)
        if abs_path not in self._video_locks:
            self._video_locks[abs_path] = threading.Lock()
        return self._video_locks[abs_path]
    
    def get_video_info(self, video_path: str) -> Dict:
        """
        执行逻辑：
        1) 读取内部状态或外部资源。
        2) 返回读取结果。
        实现方式：通过内部方法调用/状态更新、OpenCV 图像处理实现。
        核心价值：提供一致读取接口，降低调用耦合。
        输入参数：
        - video_path: 文件路径（类型：str）。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        cap = self.get_video_capture(video_path)
        with self.get_video_lock(video_path):
            return {
                "fps": cap.get(cv2.CAP_PROP_FPS),
                "frame_count": int(cap.get(cv2.CAP_PROP_FRAME_COUNT)),
                "width": int(cap.get(cv2.CAP_PROP_FRAME_WIDTH)),
                "height": int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT)),
                "duration": cap.get(cv2.CAP_PROP_FRAME_COUNT) / max(1, cap.get(cv2.CAP_PROP_FPS))
            }
    
    def extract_frames(self, video_path: str, start_sec: float, end_sec: float, 
                       fps: float) -> list:
        """
        执行逻辑：
        1) 扫描输入内容。
        2) 过滤并提取目标子集。
        实现方式：通过内部方法调用/状态更新、OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：聚焦关键信息，减少后续处理成本。
        决策逻辑：
        - 条件：target_pos_msec >= current_pos_msec and target_pos_msec - current_pos_msec < 2000
        - 条件：ret and frame is not None
        依据来源（证据链）：
        输入参数：
        - video_path: 文件路径（类型：str）。
        - start_sec: 起止时间/区间边界（类型：float）。
        - end_sec: 起止时间/区间边界（类型：float）。
        - fps: 函数入参（类型：float）。
        输出参数：
        - 列表结果（与输入或处理结果一一对应）。"""
        import numpy as np
        
        cap = self.get_video_capture(video_path)
        lock = self.get_video_lock(video_path)
        
        video_fps = self.get_video_info(video_path)["fps"]
        interval_sec = 1.0 / fps
        
        frames = []
        t = start_sec
        
        with lock:
            # 1. 初始 Seek (仅限第一次)
            cap.set(cv2.CAP_PROP_POS_MSEC, t * 1000)
            
            last_pos_msec = t * 1000
            
            while t <= end_sec:
                # 检查当前位置，如果偏差太远则重新 Seek，否则顺序读取
                current_pos_msec = cap.get(cv2.CAP_PROP_POS_MSEC)
                target_pos_msec = t * 1000
                
                # 容忍度: 如果目标位置在当前位置之后 2 秒内，采用顺序读取(更快)
                if target_pos_msec >= current_pos_msec and (target_pos_msec - current_pos_msec) < 2000:
                    # 顺序读取并跳过中间帧
                    skip_frames = int((target_pos_msec - current_pos_msec) * video_fps / 1000.0)
                    for _ in range(skip_frames - 1):
                        cap.grab() # 只抓取不解码，速度快
                    
                    ret, frame = cap.read()
                else:
                    # 距离太远，重新 Seek
                    cap.set(cv2.CAP_PROP_POS_MSEC, target_pos_msec)
                    ret, frame = cap.read()
                
                if ret and frame is not None:
                    frames.append((t, frame.copy())) # copy 避免引用问题
                
                t += interval_sec
                
        return frames
    
    # ==========================================================================
    # 资源清理
    # ==========================================================================
    
    def shutdown(self):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：self._io_executor
        依据来源（证据链）：
        - 对象内部状态：self._io_executor。
        输入参数：
        - 无。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        logger.info("Shutting down ResourceManager...")
        
        # 关闭线程池
        if self._io_executor:
            self._io_executor.shutdown(wait=True)
            self._io_executor = None
            logger.debug("IO executor shutdown")
        
        # 关闭进程池
        
        # 释放所有视频资源
        for path, cap in list(self._video_captures.items()):
            cap.release()
        self._video_captures.clear()
        self._video_locks.clear()
        logger.debug("All video captures released")
        
        logger.info("ResourceManager shutdown complete")


# =============================================================================
# 便捷函数
# =============================================================================

def get_resource_manager() -> ResourceManager:
    """
    执行逻辑：
    1) 读取内部状态或外部资源。
    2) 返回读取结果。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：提供一致读取接口，降低调用耦合。
    输入参数：
    - 无。
    输出参数：
    - ResourceManager 对象或调用结果。"""
    return ResourceManager()


def get_io_executor() -> ThreadPoolExecutor:
    """
    执行逻辑：
    1) 读取内部状态或外部资源。
    2) 返回读取结果。
    实现方式：通过线程池并发实现。
    核心价值：提供一致读取接口，降低调用耦合。
    输入参数：
    - 无。
    输出参数：
    - get_io_executor 对象或调用结果。"""
    return get_resource_manager().get_io_executor()
