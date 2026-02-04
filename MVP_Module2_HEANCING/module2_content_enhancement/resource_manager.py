"""
全局资源管理器 - 单例模式管理共享资源

提供:
- ThreadPoolExecutor: IO密集型操作 (ffmpeg调用、文件读写)
- VideoCapture: 视频资源复用
"""

import os
import cv2
import atexit
import logging
import threading
from typing import Dict, Optional
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)


class ResourceManager:
    """
    全局资源管理器 (单例模式)
    
    用法:
        from .resource_manager import get_resource_manager
        rm = get_resource_manager()
        executor = rm.get_io_executor()
    """
    
    _instance: Optional["ResourceManager"] = None
    _lock = threading.Lock()
    
    # 默认配置
    DEFAULT_IO_WORKERS = 4      # IO线程数
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
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
        """获取 IO 线程池 (懒加载)"""
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
        获取视频捕获对象 (复用已打开的)
        
        注意: 调用者需要使用 get_video_lock() 保护多线程访问
        """
        abs_path = os.path.abspath(video_path)
        
        if abs_path not in self._video_captures:
            cap = cv2.VideoCapture(abs_path)
            if not cap.isOpened():
                raise RuntimeError(f"Cannot open video: {video_path}")
            self._video_captures[abs_path] = cap
            self._video_locks[abs_path] = threading.Lock()
            logger.debug(f"Opened video: {abs_path}")
        
        return self._video_captures[abs_path]
    
    def get_video_lock(self, video_path: str) -> threading.Lock:
        """获取视频资源的锁 (用于多线程安全访问)"""
        abs_path = os.path.abspath(video_path)
        if abs_path not in self._video_locks:
            self._video_locks[abs_path] = threading.Lock()
        return self._video_locks[abs_path]
    
    def get_video_info(self, video_path: str) -> Dict:
        """获取视频信息 (缓存)"""
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
        🚀 优化版采样帧提取
        
        策略 (First Principle):
        1. 顺序读取优于随机尋道 (OpenCV .set() 极慢)。
        2. 若采样点密集且有序，seek一次后逐帧读取/跳帧。
        """
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
        """关闭所有资源"""
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
    """获取全局资源管理器实例"""
    return ResourceManager()


def get_io_executor() -> ThreadPoolExecutor:
    """快捷方式: 获取 IO 线程池"""
    return get_resource_manager().get_io_executor()
