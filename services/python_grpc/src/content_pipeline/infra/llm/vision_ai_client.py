"""
模块说明：Module2 内容增强中的 vision_ai_client 模块。
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
import json
import asyncio
import hashlib
import fnmatch
import logging
import time
import threading
import importlib.util
import numpy as np
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass, field
import httpx
from services.python_grpc.src.content_pipeline.infra.runtime import cache_metrics

logger = logging.getLogger(__name__)


def _supports_http2_transport() -> bool:
    """
    做什么：判断当前环境是否支持 HTTP/2 传输。
    为什么：未安装 h2 时直接启用 http2=True 会在客户端初始化阶段失败。
    权衡：默认自动探测，也允许通过 MODULE2_HTTP2_ENABLED 强制开关。
    """
    raw = os.getenv("MODULE2_HTTP2_ENABLED")
    if raw is not None:
        val = str(raw).strip().lower()
        if val in {"1", "true", "yes", "y", "on"}:
            return True
        if val in {"0", "false", "no", "n", "off"}:
            return False
    return importlib.util.find_spec("h2") is not None


# =============================================================================
# 感知哈希 (Perceptual Hash) 实现
# =============================================================================

class PerceptualHasher:
    """类说明：PerceptualHasher 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    
    HASH_SIZE = 8  # 8x8 = 64 bits
    
    @staticmethod
    def compute_dhash(image: np.ndarray) -> str:
        """
        执行逻辑：
        1) 准备输入数据。
        2) 执行计算并返回结果。
        实现方式：通过OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：提供量化结果，为上游决策提供依据。
        决策逻辑：
        - 条件：len(image.shape) == 3
        依据来源（证据链）：
        - 输入参数：image。
        输入参数：
        - image: 函数入参（类型：np.ndarray）。
        输出参数：
        - 字符串结果。"""
        # 转灰度
        if len(image.shape) == 3:
            gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        else:
            gray = image
        
        # 缩放到 (HASH_SIZE+1, HASH_SIZE)
        resized = cv2.resize(gray, (PerceptualHasher.HASH_SIZE + 1, PerceptualHasher.HASH_SIZE))
        
        # 计算水平差异
        diff = resized[:, 1:] > resized[:, :-1]
        
        # 转换为整数
        hash_value = 0
        for bit in diff.flatten():
            hash_value = (hash_value << 1) | int(bit)
        
        return f"{hash_value:016x}"
    
    @staticmethod
    def compute_similarity(hash1: str, hash2: str) -> float:
        """
        执行逻辑：
        1) 准备输入数据。
        2) 执行计算并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：提供量化结果，为上游决策提供依据。
        决策逻辑：
        - 条件：len(hash1) != len(hash2)
        依据来源（证据链）：
        - 输入参数：hash1, hash2。
        输入参数：
        - hash1: 函数入参（类型：str）。
        - hash2: 函数入参（类型：str）。
        输出参数：
        - 数值型计算结果。"""
        if len(hash1) != len(hash2):
            return 0.0
        
        # 转换为整数
        int1 = int(hash1, 16)
        int2 = int(hash2, 16)
        
        # 计算汉明距离
        xor = int1 ^ int2
        hamming_distance = bin(xor).count('1')
        
        # 相似度 = 1 - (距离 / 总位数)
        total_bits = len(hash1) * 4  # 每个十六进制字符 = 4 bits
        return 1.0 - (hamming_distance / total_bits)
    
    @staticmethod
    def compute_from_file(image_path: str) -> Optional[str]:
        """
        执行逻辑：
        1) 准备输入数据。
        2) 执行计算并返回结果。
        实现方式：通过OpenCV 图像处理实现。
        核心价值：提供量化结果，为上游决策提供依据。
        决策逻辑：
        - 条件：img is None
        依据来源（证据链）：
        输入参数：
        - image_path: 文件路径（类型：str）。
        输出参数：
        - compute_dhash 对象或调用结果。"""
        try:
            img = cv2.imread(image_path)
            if img is None:
                return None
            return PerceptualHasher.compute_dhash(img)
        except Exception as e:
            logger.warning(f"Failed to compute hash for {image_path}: {e}")
            return None


# =============================================================================
# 哈希缓存管理器
# =============================================================================

class HashCacheManager:
    """类说明：HashCacheManager 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    
    def __init__(self, similarity_threshold: float = 0.95):
        """
        执行逻辑：
        1) 解析配置或依赖，准备运行环境。
        2) 初始化对象状态、缓存与依赖客户端。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：在初始化阶段固化依赖，保证运行稳定性。
        输入参数：
        - similarity_threshold: 阈值（类型：float）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self.threshold = similarity_threshold
        self._cache: Dict[str, Dict[str, Any]] = {}  # hash -> {path, result}
        self._path_to_hash: Dict[str, str] = {}  # path -> hash
    
    def check_duplicate_with_mode(
        self,
        image_path: str,
    ) -> Tuple[bool, Optional[Dict[str, Any]], bool]:
        """
        ????????????????????????
        - ?????? `is_exact_duplicate=True` ?? hash ????????
        """
        current_hash = PerceptualHasher.compute_from_file(image_path)
        if current_hash is None:
            return False, None, False

        # ????
        if current_hash in self._cache:
            cache_metrics.hit("module2.vision_ai.hash_cache")
            logger.info(f"Exact hash match for {Path(image_path).name}, reusing cached result")
            return True, self._cache[current_hash].get("result"), True

        # ????
        for cached_hash, cached_data in self._cache.items():
            similarity = PerceptualHasher.compute_similarity(current_hash, cached_hash)
            if similarity >= self.threshold:
                cache_metrics.hit("module2.vision_ai.hash_cache")
                logger.info(
                    f"Similar frame detected: {Path(image_path).name} ~ {cached_data.get('path', 'unknown')} "
                    f"(similarity={similarity:.1%})"
                )
                return True, cached_data.get("result"), False

        # ???????? hash??????
        self._cache[current_hash] = {"path": image_path, "result": None}
        self._path_to_hash[image_path] = current_hash
        cache_metrics.miss("module2.vision_ai.hash_cache")
        return False, None, False

    def check_duplicate(self, image_path: str) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """????????????????????"""
        is_duplicate, cached_result, _is_exact_duplicate = self.check_duplicate_with_mode(image_path)
        return is_duplicate, cached_result

    def store_result(self, image_path: str, result: Dict[str, Any]):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：image_path in self._path_to_hash
        - 条件：hash_key in self._cache
        依据来源（证据链）：
        - 输入参数：image_path。
        - 对象内部状态：self._cache, self._path_to_hash。
        输入参数：
        - image_path: 文件路径（类型：str）。
        - result: 函数入参（类型：Dict[str, Any]）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        if image_path in self._path_to_hash:
            hash_key = self._path_to_hash[image_path]
            if hash_key in self._cache:
                self._cache[hash_key]["result"] = result

    def load_results(self, cached_items: Dict[str, Dict[str, Any]]):
        """
        执行逻辑：
        1) 校验输入路径与参数。
        2) 读取并解析为结构化对象。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：将外部数据转为内部结构，统一输入口径。
        决策逻辑：
        - 条件：not cached_items
        - 条件：hash_key in self._cache
        依据来源（证据链）：
        - 输入参数：cached_items。
        - 对象内部状态：self._cache。
        输入参数：
        - cached_items: 函数入参（类型：Dict[str, Dict[str, Any]]）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        if not cached_items:
            return
        for hash_key, item in cached_items.items():
            if hash_key in self._cache:
                continue
            self._cache[hash_key] = {
                "path": item.get("path", ""),
                "result": item.get("result")
            }

    def export_results(self, include_empty: bool = False) -> Dict[str, Dict[str, Any]]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：not include_empty and item.get('result') is None
        依据来源（证据链）：
        - 输入参数：include_empty。
        - 配置字段：result。
        输入参数：
        - include_empty: 函数入参（类型：bool）。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        exported = {}
        for hash_key, item in self._cache.items():
            if not include_empty and item.get("result") is None:
                continue
            exported[hash_key] = {
                "path": item.get("path", ""),
                "result": item.get("result")
            }
        return exported
    
    def get_stats(self) -> Dict[str, Any]:
        """
        执行逻辑：
        1) 读取内部状态或外部资源。
        2) 返回读取结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：提供一致读取接口，降低调用耦合。
        输入参数：
        - 无。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        return {
            "cached_hashes": len(self._cache),
            "threshold": f"{self.threshold:.0%}"
        }


# =============================================================================
# 自适应并发控制器 (复用 llm_client.py 的 AIMD 逻辑)
# =============================================================================

class VisionAIConcurrencyLimiter:
    """类说明：VisionAIConcurrencyLimiter 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    
    def __init__(
        self,
        initial_limit: int = 5,
        min_limit: int = 5,
        max_limit: int = 60,
        decrease_factor: float = 0.5
    ):
        """
        执行逻辑：
        1) 解析配置或依赖，准备运行环境。
        2) 初始化对象状态、缓存与依赖客户端。
        实现方式：通过内部方法调用/状态更新、asyncio 异步调度实现。
        核心价值：在初始化阶段固化依赖，保证运行稳定性。
        输入参数：
        - initial_limit: 函数入参（类型：int）。
        - min_limit: 函数入参（类型：int）。
        - max_limit: 函数入参（类型：int）。
        - decrease_factor: 函数入参（类型：float）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self.current_limit = initial_limit
        self.min_limit = min_limit
        self.max_limit = max_limit
        self.decrease_factor = decrease_factor
        
        self._semaphore = asyncio.Semaphore(initial_limit)
        self._lock = asyncio.Lock()
        self._results: List[bool] = []
        self._window_size = 10
        
        logger.info(f"VisionAIConcurrencyLimiter initialized: limit={initial_limit}, range=[{min_limit}, {max_limit}]")
    
    async def acquire(self):
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
        await self._semaphore.acquire()
    
    def release(self):
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
        self._semaphore.release()
    
    async def record_success(self):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、asyncio 异步调度实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：len(self._results) > self._window_size
        - 条件：len(self._results) >= self._window_size
        - 条件：success_rate > 0.9 and self.current_limit < self.max_limit
        依据来源（证据链）：
        - 对象内部状态：self._results, self._window_size, self.current_limit, self.max_limit。
        输入参数：
        - 无。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        async with self._lock:
            self._results.append(True)
            if len(self._results) > self._window_size:
                self._results.pop(0)
            
            if len(self._results) >= self._window_size:
                success_rate = sum(self._results) / len(self._results)
                if success_rate > 0.9 and self.current_limit < self.max_limit:
                    old = self.current_limit
                    self.current_limit = min(self.current_limit + 1, self.max_limit)
                    self._semaphore = asyncio.Semaphore(self.current_limit)
                    logger.debug(f"Vision concurrency ↑ {old} → {self.current_limit}")
    
    async def record_failure(self, is_rate_limit: bool = False):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、asyncio 异步调度实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：len(self._results) > self._window_size
        - 条件：is_rate_limit
        - 条件：old != self.current_limit
        依据来源（证据链）：
        - 输入参数：is_rate_limit。
        - 对象内部状态：self._results, self._window_size, self.current_limit。
        输入参数：
        - is_rate_limit: 开关/状态（类型：bool）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        async with self._lock:
            self._results.append(False)
            if len(self._results) > self._window_size:
                self._results.pop(0)
            
            old = self.current_limit
            if is_rate_limit:
                self.current_limit = max(int(self.current_limit * self.decrease_factor), self.min_limit)
            else:
                self.current_limit = max(self.current_limit - 1, self.min_limit)
            
            if old != self.current_limit:
                self._semaphore = asyncio.Semaphore(self.current_limit)
                logger.warning(f"Vision concurrency ↓ {old} → {self.current_limit} (rate_limit={is_rate_limit})")


# =============================================================================
# Vision AI 客户端
# =============================================================================

# =============================================================================
# Vision AI rate limiter (token bucket: init=30, refill=1 token/s)
# =============================================================================

class VisionAIRateLimiter:
    """
    Apply token-bucket throttling for Vision API calls.
    Burst capacity is 30 and refill rate is 1 token per second.
    """

    def __init__(
        self,
        rate_per_minute: int = 60,
        initial_tokens: int = 30,
        refill_tokens_per_second: float = 1.0,
    ):
        self.rate_per_minute = int(rate_per_minute)
        self._capacity = float(max(1, int(initial_tokens)))
        self._refill_per_second = float(refill_tokens_per_second)
        if self._refill_per_second <= 0 and self.rate_per_minute > 0:
            self._refill_per_second = float(self.rate_per_minute) / 60.0
        self._refill_per_second = max(0.0, self._refill_per_second)
        self._tokens = float(self._capacity)
        self._last_refill_time = time.monotonic()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._lock: Optional[asyncio.Lock] = None

    def _ensure_loop(self) -> bool:
        """Ensure a valid asyncio lock bound to the current running loop."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return False
        if self._loop is None or self._loop.is_closed() or self._loop != loop or self._lock is None:
            self._loop = loop
            self._lock = asyncio.Lock()
        return True

    async def acquire(self) -> float:
        """
        Return wait time in seconds before one token can be consumed.
        """
        if self._refill_per_second <= 0:
            return 0.0
        if not self._ensure_loop():
            raise RuntimeError("No running event loop for VisionAIRateLimiter")
        async with self._lock:
            now = time.monotonic()
            elapsed = max(0.0, now - self._last_refill_time)
            if elapsed > 0:
                self._tokens = min(self._capacity, self._tokens + elapsed * self._refill_per_second)
                self._last_refill_time = now

            total_wait_sec = 0.0
            while self._tokens < 1.0:
                need = 1.0 - self._tokens
                wait_sec = need / self._refill_per_second
                await asyncio.sleep(wait_sec)
                total_wait_sec += wait_sec

                now = time.monotonic()
                elapsed = max(0.0, now - self._last_refill_time)
                if elapsed > 0:
                    self._tokens = min(self._capacity, self._tokens + elapsed * self._refill_per_second)
                    self._last_refill_time = now

            self._tokens = max(0.0, self._tokens - 1.0)
            return total_wait_sec


# =============================================================================
# Vision AI 后台事件循环（同步调用桥接）
# =============================================================================

class VisionAIBackgroundLoop:
    """
    作用：提供单一后台事件循环，避免每次同步调用都创建/销毁 loop。
    """

    def __init__(self):
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._ready = threading.Event()

    def _run(self):
        """方法说明：VisionAIBackgroundLoop._run 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._ready.set()
        loop.run_forever()

    def get_loop(self) -> asyncio.AbstractEventLoop:
        """方法说明：VisionAIBackgroundLoop.get_loop 核心方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        if self._loop and self._thread and self._thread.is_alive():
            return self._loop
        with self._lock:
            if self._loop and self._thread and self._thread.is_alive():
                return self._loop
            self._ready.clear()
            self._thread = threading.Thread(
                target=self._run,
                name="VisionAIBackgroundLoop",
                daemon=True
            )
            self._thread.start()
            self._ready.wait(timeout=5)
            if not self._loop:
                raise RuntimeError("VisionAIBackgroundLoop failed to start")
        return self._loop

    def submit(self, coro, timeout: Optional[float] = None):
        """方法说明：VisionAIBackgroundLoop.submit 核心方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        loop = self.get_loop()
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        return future.result(timeout=timeout)


_VISION_BG_LOOP = VisionAIBackgroundLoop()


@dataclass
class VisionAIConfig:
    """类说明：VisionAIConfig 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    enabled: bool = False
    bearer_token: str = ""
    base_url: str = "https://qianfan.baidubce.com/v2/chat/completions"
    model: str = "ernie-4.5-turbo-vl-32k"
    temperature: float = 0.3
    timeout: float = 60.0
    rate_limit_per_minute: int = 60
    
    # 重复帧检测
    duplicate_detection_enabled: bool = True
    similarity_threshold: float = 0.95
    # 批量调用配置（默认关闭，保持旧行为不变）
    batch_enabled: bool = False
    batch_max_size: int = 4
    batch_flush_ms: int = 20
    batch_max_inflight_batches: int = 2
    # 人物主体预过滤（MediaPipe Selfie Segmentation）
    person_subject_filter_enabled: bool = True
    person_mask_area_threshold: float = 0.3
    person_mask_binary_threshold: float = 0.5
    person_mask_high_conf_threshold: float = 0.8
    person_mask_high_conf_min_area: float = 0.08
    person_prefilter_force_include_patterns: List[str] = field(default_factory=list)
    person_model_selection: int = 1


class VisionAIClient:
    """类说明：VisionAIClient 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    
    def __init__(self, config: Optional[VisionAIConfig] = None):
        """
        执行逻辑：
        1) 解析配置或依赖，准备运行环境。
        2) 初始化对象状态、缓存与依赖客户端。
        实现方式：通过内部方法调用/状态更新、HTTP 调用实现。
        核心价值：在初始化阶段固化依赖，保证运行稳定性。
        决策逻辑：
        - 条件：self.config.duplicate_detection_enabled
        依据来源（证据链）：
        - 对象内部状态：self.config。
        输入参数：
        - config: 配置对象/字典（类型：Optional[VisionAIConfig]）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self.config = config or VisionAIConfig()
        
        # HTTP 客户端 (连接池 + HTTP/2)
        self._http_client: Optional[httpx.AsyncClient] = None
        self._client_loop = None
        
        # 并发控制
        self._concurrency_limiter = VisionAIConcurrencyLimiter()
        self._rate_limiter = VisionAIRateLimiter(self.config.rate_limit_per_minute)
        
        # 重复帧检测
        self._hash_cache = HashCacheManager(
            similarity_threshold=self.config.similarity_threshold
        ) if self.config.duplicate_detection_enabled else None

        # 人物主体预过滤（懒加载 MediaPipe，避免未安装时报错）
        self._person_segmenter = None
        self._person_segmenter_init_error: Optional[str] = None
        self._person_segmenter_lock = threading.Lock()
        
        # 统计
        self._stats = {
            "total_requests": 0,
            "cache_hits": 0,
            "duplicate_skips": 0,
            "duplicate_deleted_files": 0,
            "api_calls": 0,
            "api_batch_calls": 0,
            "api_batch_items_total": 0,
            "api_wait_ms_total": 0.0,
            "api_wait_count": 0,
            "api_rate_wait_ms_total": 0.0,
            "api_rate_wait_count": 0,
            "batch_fallback_to_single_calls": 0,
            "person_subject_skips": 0,
        }

    def _get_person_segmenter(self):
        """
        获取 MediaPipe SelfieSegmentation 实例（懒加载）。
        返回 None 表示不可用（未安装/初始化失败），调用方需自动降级。
        """
        if self._person_segmenter is not None:
            return self._person_segmenter
        if self._person_segmenter_init_error:
            return None
        with self._person_segmenter_lock:
            if self._person_segmenter is not None:
                return self._person_segmenter
            if self._person_segmenter_init_error:
                return None
            try:
                mp_spec = importlib.util.find_spec("mediapipe")
                if mp_spec is None:
                    self._person_segmenter_init_error = "mediapipe_not_installed"
                    logger.warning("Vision person prefilter disabled: mediapipe is not installed")
                    return None
                import mediapipe as mp  # type: ignore

                model_selection = int(getattr(self.config, "person_model_selection", 1) or 1)
                model_selection = 0 if model_selection <= 0 else 1
                self._person_segmenter = mp.solutions.selfie_segmentation.SelfieSegmentation(
                    model_selection=model_selection
                )
                return self._person_segmenter
            except Exception as exc:
                self._person_segmenter_init_error = str(exc)
                logger.warning(f"Vision person prefilter disabled: MediaPipe init failed: {exc}")
                return None

    def _estimate_person_mask_signals(self, image_path: str) -> Optional[Dict[str, float]]:
        """
        基于 MediaPipe 分割结果估算人物相关信号。
        返回 None 表示无法估算（文件不可读/模型不可用/推理失败）。
        """
        segmenter = self._get_person_segmenter()
        if segmenter is None:
            return None
        frame_bgr = cv2.imread(image_path)
        if frame_bgr is None or frame_bgr.size == 0:
            return None
        try:
            frame_rgb = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2RGB)
            segmentation = segmenter.process(frame_rgb)
            mask = getattr(segmentation, "segmentation_mask", None)
            if mask is None:
                return None
            binary_threshold = float(getattr(self.config, "person_mask_binary_threshold", 0.5) or 0.5)
            binary_threshold = max(0.0, min(1.0, binary_threshold))
            high_conf_threshold = float(getattr(self.config, "person_mask_high_conf_threshold", 0.8) or 0.8)
            high_conf_threshold = max(0.0, min(1.0, high_conf_threshold))
            return {
                "mask_ratio": float(np.mean(mask > binary_threshold)),
                "high_conf_ratio": float(np.mean(mask > high_conf_threshold)),
            }
        except Exception as exc:
            logger.warning(f"Vision person prefilter inference failed for {Path(image_path).name}: {exc}")
            return None

    def _estimate_person_mask_ratio(self, image_path: str) -> Optional[float]:
        """向后兼容：仅返回人物掩膜占比。"""
        signals = self._estimate_person_mask_signals(image_path)
        if not signals:
            return None
        return float(signals.get("mask_ratio", 0.0))

    def _build_person_subject_skip_result(self, mask_ratio: float, high_conf_ratio: float) -> Dict[str, Any]:
        """构造“人物主体”过滤结果，统一返回 false。"""
        return {
            "has_concrete_knowledge": False,
            "confidence": 1.0,
            "concrete_type": "person_subject",
            "reason": (
                "人物主体占比过高，预过滤跳过 Vision"
                f"（mask_ratio={mask_ratio:.4f}, high_conf_ratio={high_conf_ratio:.4f}）"
            ),
            "should_include": False,
            "img_description": "人物主体截图，已在预处理阶段过滤",
            "person_mask_ratio": float(mask_ratio),
            "person_high_conf_ratio": float(high_conf_ratio),
            "prefilter_source": "mediapipe_selfie_segmentation",
        }

    def _is_person_prefilter_force_included(self, image_path: str) -> bool:
        """命中白名单模式时，跳过人物预过滤并放行到 Vision。"""
        patterns = getattr(self.config, "person_prefilter_force_include_patterns", None)
        if not patterns:
            return False
        path_obj = Path(image_path)
        full_path = str(path_obj)
        file_name = path_obj.name
        try:
            iterable = patterns if isinstance(patterns, (list, tuple, set)) else [patterns]
            for raw_pattern in iterable:
                pattern = str(raw_pattern or "").strip()
                if not pattern:
                    continue
                if fnmatch.fnmatch(file_name, pattern) or fnmatch.fnmatch(full_path, pattern):
                    return True
        except Exception as exc:
            logger.warning(f"Person prefilter force-include pattern parse failed: {exc}")
            return False
        return False

    def _run_person_subject_prefilter(self, image_path: str) -> Optional[Dict[str, Any]]:
        """在调用 Vision API 前执行人物主体过滤。"""
        if not bool(getattr(self.config, "person_subject_filter_enabled", True)):
            return None
        if self._is_person_prefilter_force_included(image_path):
            logger.info(
                f"Person-subject prefilter bypassed by force-include pattern: {Path(image_path).name}"
            )
            return None
        signals = self._estimate_person_mask_signals(image_path)
        if signals is None:
            return None
        mask_ratio = float(signals.get("mask_ratio", 0.0))
        high_conf_ratio = float(signals.get("high_conf_ratio", 0.0))
        area_threshold = float(getattr(self.config, "person_mask_area_threshold", 0.3) or 0.3)
        area_threshold = max(0.0, min(1.0, area_threshold))
        high_conf_min_area = float(getattr(self.config, "person_mask_high_conf_min_area", 0.08) or 0.08)
        high_conf_min_area = max(0.0, min(1.0, high_conf_min_area))
        if mask_ratio > area_threshold and high_conf_ratio >= high_conf_min_area:
            return self._build_person_subject_skip_result(mask_ratio, high_conf_ratio)
        if mask_ratio > area_threshold and high_conf_ratio < high_conf_min_area:
            logger.info(
                f"Person-subject prefilter guard passed-through: {Path(image_path).name}, "
                f"mask_ratio={mask_ratio:.4f}, high_conf_ratio={high_conf_ratio:.4f}, "
                f"min_high_conf_area={high_conf_min_area:.4f}"
            )
        return None
    
    async def _get_client(self) -> httpx.AsyncClient:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、HTTP 调用实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：self._http_client is None
        依据来源（证据链）：
        - 对象内部状态：self._http_client。
        输入参数：
        - 无。
        输出参数：
        - 函数计算/封装后的结果对象。"""
        loop = asyncio.get_running_loop()
        need_reset = (
            self._http_client is None or
            self._http_client.is_closed or
            self._client_loop is None or
            self._client_loop.is_closed() or
            self._client_loop != loop
        )
        if need_reset:
            if self._http_client and not self._http_client.is_closed:
                await self._safe_close_client()
            http2_enabled = _supports_http2_transport()
            client_kwargs = {
                "limits": httpx.Limits(
                    max_connections=20,
                    max_keepalive_connections=10,
                    keepalive_expiry=30.0
                ),
                "timeout": httpx.Timeout(self.config.timeout, connect=10.0),
                "http2": http2_enabled,
            }
            try:
                self._http_client = httpx.AsyncClient(**client_kwargs)
            except Exception as e:
                error_text = str(e)
                if http2_enabled and "h2" in error_text:
                    logger.warning("VisionAI HTTP/2 unavailable (missing h2), fallback to HTTP/1.1")
                    client_kwargs["http2"] = False
                    self._http_client = httpx.AsyncClient(**client_kwargs)
                    http2_enabled = False
                else:
                    raise
            self._client_loop = loop
            logger.info(f"VisionAI HTTP client initialized: pool=20, http2={http2_enabled}")
        return self._http_client

    async def _safe_close_client(self):
        """方法说明：VisionAIClient._safe_close_client 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        if not self._http_client:
            return
        if self._client_loop and self._client_loop.is_closed():
            logger.warning("VisionAI HTTP client close skipped: event loop is closed")
            self._http_client = None
            self._client_loop = None
            return
        try:
            await self._http_client.aclose()
        except Exception as e:
            logger.warning(f"VisionAI HTTP client close failed: {e}")
        finally:
            self._http_client = None
            self._client_loop = None

    def _delete_duplicate_image_file(self, image_path: str):
        """命中可复用重复结果后，最佳努力删除重复图片文件。"""
        if not image_path:
            return
        try:
            if os.path.exists(image_path):
                os.remove(image_path)
                self._stats["duplicate_deleted_files"] += 1
                logger.info(f"Duplicate frame file deleted: {Path(image_path).name}")
        except Exception as e:
            logger.warning(f"Failed to delete duplicate frame file: {image_path}, err={e}")
    
    async def validate_image(
        self,
        image_path: str,
        prompt: str = "",
        system_prompt: Optional[str] = None,
        skip_duplicate_check: bool = False
    ) -> Dict[str, Any]:
        """
        执行逻辑：
        1) 整理待校验数据。
        2) 按规则逐项校验并返回结果。
        实现方式：通过内部方法调用/状态更新、HTTP 调用、文件系统读写实现。
        核心价值：提前发现数据/状态问题，降低运行风险。
        决策逻辑：
        - 条件：not skip_duplicate_check and self._hash_cache
        - 条件：not self.config.enabled or not self.config.bearer_token
        - 条件：self._hash_cache
        依据来源（证据链）：
        - 输入参数：skip_duplicate_check。
        - 对象内部状态：self._hash_cache, self.config。
        输入参数：
        - image_path: 文件路径（类型：str）。
        - prompt: 文本内容（类型：str）。
        - skip_duplicate_check: 函数入参（类型：bool）。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        self._stats["total_requests"] += 1
        
        # Step 1: 重复帧检测
        if not skip_duplicate_check and self._hash_cache:
            is_exact_duplicate = False
            if hasattr(self._hash_cache, "check_duplicate_with_mode"):
                is_duplicate, cached_result, is_exact_duplicate = self._hash_cache.check_duplicate_with_mode(image_path)
            else:
                is_duplicate, cached_result = self._hash_cache.check_duplicate(image_path)
            if is_duplicate and cached_result:
                self._stats["duplicate_skips"] += 1
                if is_exact_duplicate:
                    self._delete_duplicate_image_file(image_path)
                logger.info(f"Duplicate frame skipped: {Path(image_path).name}")
                return cached_result

        # Step 2: 人物主体预过滤（命中后直接判定 false，并跳过 Vision API）
        prefilter_result = self._run_person_subject_prefilter(image_path)
        if prefilter_result is not None:
            self._stats["person_subject_skips"] += 1
            if self._hash_cache:
                self._hash_cache.store_result(image_path, prefilter_result)
            logger.info(
                f"Person-subject prefilter skipped Vision API: {Path(image_path).name}, "
                f"mask_ratio={float(prefilter_result.get('person_mask_ratio', 0.0)):.4f}, "
                f"high_conf_ratio={float(prefilter_result.get('person_high_conf_ratio', 0.0)):.4f}"
            )
            return prefilter_result

        # Step 3: API 调用
        if not self.config.enabled or not self.config.bearer_token:
            return {"error": "Vision AI not configured", "should_include": True}
        
        result = await self._call_vision_api(image_path, prompt, system_prompt)
        
        # Step 4: 缓存结果
        if self._hash_cache:
            self._hash_cache.store_result(image_path, result)
        
        return result

    def _build_messages(
        self,
        *,
        image_payloads: List[str],
        prompt: str = "",
        system_prompt: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """统一构建 Vision 请求消息，支持单图/多图。"""
        messages: List[Dict[str, Any]] = []
        normalized_system_prompt = (system_prompt or "").strip()
        normalized_prompt = (prompt or "").strip()

        if normalized_system_prompt:
            messages.append({"role": "system", "content": normalized_system_prompt})

        user_content: List[Dict[str, Any]] = []
        if normalized_prompt:
            user_content.append({"type": "text", "text": normalized_prompt})
        for encoded_image in image_payloads:
            user_content.append(
                {
                    "type": "image_url",
                    "image_url": {"url": f"data:image/jpeg;base64,{encoded_image}"},
                }
            )
        messages.append({"role": "user", "content": user_content})
        return messages

    def _parse_vision_content(self, content: Any) -> Any:
        """解析 Vision 模型 content 字段，兼容 dict/list/markdown-json/plain。"""
        if isinstance(content, (dict, list)):
            return content
        text = str(content or "").strip()
        if not text:
            return {}
        # 兼容 ```json ... ``` 包裹
        clean = text.replace("```json", "").replace("```", "").strip()
        try:
            return json.loads(clean)
        except Exception:
            return {"raw_response": text, "should_include": True}

    def _normalize_batch_result(
        self,
        parsed: Any,
        expected_count: int,
    ) -> List[Dict[str, Any]]:
        """
        将模型返回统一为与输入等长的结果列表。
        优先支持 list，其次支持 {results/items/...} 容器。
        """
        if expected_count <= 0:
            return []

        if isinstance(parsed, list):
            normalized = [item if isinstance(item, dict) else {"raw_response": str(item)} for item in parsed]
            if len(normalized) >= expected_count:
                return normalized[:expected_count]
            normalized.extend(
                [{"error": "batch_result_missing", "should_include": True}]
                * (expected_count - len(normalized))
            )
            return normalized

        if isinstance(parsed, dict):
            for key in ("results", "items", "outputs", "data"):
                candidate = parsed.get(key)
                if isinstance(candidate, list):
                    return self._normalize_batch_result(candidate, expected_count)
            if expected_count == 1:
                return [parsed]
            # 仅返回单对象时，扩展到每个输入，保证流程可继续
            return [dict(parsed) for _ in range(expected_count)]

        return [{"error": "invalid_batch_response", "should_include": True} for _ in range(expected_count)]

    async def validate_images_batch(
        self,
        image_paths: List[str],
        prompt: str = "",
        system_prompt: Optional[str] = None,
        skip_duplicate_check: bool = False,
        max_batch_size: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        批量校验图片。默认关闭批量能力，保持原单图路径行为不变。
        批量失败时自动回退到单图调用，避免影响主链路可用性。
        """
        if not image_paths:
            return []

        configured_batch_size = int(max_batch_size or self.config.batch_max_size or 1)
        effective_batch_size = max(1, configured_batch_size)
        if not self.config.batch_enabled or effective_batch_size <= 1:
            ordered_results: List[Optional[Dict[str, Any]]] = [None] * len(image_paths)
            for idx in range(len(image_paths) - 1, -1, -1):
                path = image_paths[idx]
                result = await self.validate_image(
                    image_path=path,
                    prompt=prompt,
                    system_prompt=system_prompt,
                    skip_duplicate_check=skip_duplicate_check,
                )
                ordered_results[idx] = result
            return [item or {"error": "empty_result", "should_include": True} for item in ordered_results]

        self._stats["total_requests"] += len(image_paths)

        indexed_paths = list(enumerate(image_paths))
        pending: List[Tuple[int, str]] = []
        ordered_results: List[Optional[Dict[str, Any]]] = [None] * len(indexed_paths)

        for idx, image_path in reversed(indexed_paths):
            if not skip_duplicate_check and self._hash_cache:
                is_exact_duplicate = False
                if hasattr(self._hash_cache, "check_duplicate_with_mode"):
                    is_duplicate, cached_result, is_exact_duplicate = self._hash_cache.check_duplicate_with_mode(
                        image_path
                    )
                else:
                    is_duplicate, cached_result = self._hash_cache.check_duplicate(image_path)
                if is_duplicate and cached_result:
                    self._stats["duplicate_skips"] += 1
                    if is_exact_duplicate:
                        self._delete_duplicate_image_file(image_path)
                    ordered_results[idx] = cached_result
                    continue
            prefilter_result = self._run_person_subject_prefilter(image_path)
            if prefilter_result is not None:
                self._stats["person_subject_skips"] += 1
                if self._hash_cache:
                    self._hash_cache.store_result(image_path, prefilter_result)
                ordered_results[idx] = prefilter_result
                logger.info(
                    f"Person-subject prefilter skipped Vision API (batch): {Path(image_path).name}, "
                    f"mask_ratio={float(prefilter_result.get('person_mask_ratio', 0.0)):.4f}, "
                    f"high_conf_ratio={float(prefilter_result.get('person_high_conf_ratio', 0.0)):.4f}"
                )
                continue
            pending.append((idx, image_path))

        if not pending:
            return [item or {"error": "empty_result", "should_include": True} for item in ordered_results]

        batches: List[List[Tuple[int, str]]] = []
        for i in range(0, len(pending), effective_batch_size):
            batches.append(pending[i : i + effective_batch_size])

        batch_sem = asyncio.Semaphore(max(1, int(self.config.batch_max_inflight_batches or 1)))

        async def _run_one_batch(batch_items: List[Tuple[int, str]]) -> None:
            async with batch_sem:
                batch_paths = [path for _, path in batch_items]
                batch_results = await self._call_vision_api_batch(
                    image_paths=batch_paths,
                    prompt=prompt,
                    system_prompt=system_prompt,
                )

                # 批量返回不匹配时，降级为逐图调用，保证结果可用。
                if len(batch_results) != len(batch_items):
                    self._stats["batch_fallback_to_single_calls"] += len(batch_items)
                    batch_results = await asyncio.gather(
                        *[
                            self._call_vision_api(
                                image_path=path,
                                prompt=prompt,
                                system_prompt=system_prompt,
                            )
                            for _, path in batch_items
                        ]
                    )

                for (idx, path), result in zip(batch_items, batch_results):
                    normalized = result if isinstance(result, dict) else {"raw_response": str(result)}
                    if self._hash_cache:
                        self._hash_cache.store_result(path, normalized)
                    ordered_results[idx] = normalized

        await asyncio.gather(*[_run_one_batch(batch) for batch in batches])
        return [item or {"error": "empty_result", "should_include": True} for item in ordered_results]

    async def _call_vision_api_batch(
        self,
        image_paths: List[str],
        prompt: str = "",
        system_prompt: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """使用单次请求提交多张图片，返回与输入等长的结果列表。"""
        if not image_paths:
            return []

        import base64

        encoded_images: List[str] = []
        for image_path in image_paths:
            try:
                with open(image_path, "rb") as file_obj:
                    encoded_images.append(base64.b64encode(file_obj.read()).decode("utf-8"))
            except Exception as exc:
                logger.error(f"Failed to read image {image_path}: {exc}")
                return [
                    {
                        "error": f"read_image_failed: {exc}",
                        "should_include": True,
                    }
                    for _ in image_paths
                ]

        rate_wait_sec = await self._rate_limiter.acquire()
        rate_wait_ms = rate_wait_sec * 1000.0
        self._stats["api_rate_wait_ms_total"] += rate_wait_ms
        self._stats["api_rate_wait_count"] += 1

        wait_start = time.perf_counter()
        await self._concurrency_limiter.acquire()
        wait_ms = (time.perf_counter() - wait_start) * 1000.0
        self._stats["api_wait_ms_total"] += wait_ms
        self._stats["api_wait_count"] += 1

        try:
            client = await self._get_client()
            payload = {
                "model": self.config.model,
                "messages": self._build_messages(
                    image_payloads=encoded_images,
                    prompt=prompt,
                    system_prompt=system_prompt,
                ),
                "temperature": self.config.temperature,
            }
            headers = {
                "Authorization": f"Bearer {self.config.bearer_token}",
                "Content-Type": "application/json",
            }

            req_start = time.perf_counter()
            response = await client.post(
                self.config.base_url,
                json=payload,
                headers=headers,
            )
            http_ms = (time.perf_counter() - req_start) * 1000.0
            logger.info(
                f"Vision batch timing: size={len(image_paths)}, rate_wait={rate_wait_ms:.1f}ms, "
                f"wait={wait_ms:.1f}ms, http={http_ms:.1f}ms, status={response.status_code}"
            )

            self._stats["api_calls"] += 1
            self._stats["api_batch_calls"] += 1
            self._stats["api_batch_items_total"] += len(image_paths)

            if response.status_code == 200:
                await self._concurrency_limiter.record_success()
                data = response.json()
                content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                parsed = self._parse_vision_content(content)
                return self._normalize_batch_result(parsed, len(image_paths))

            if response.status_code == 429:
                await self._concurrency_limiter.record_failure(is_rate_limit=True)
                return [{"error": "rate_limited", "should_include": True} for _ in image_paths]

            await self._concurrency_limiter.record_failure(is_rate_limit=False)
            logger.error(f"Vision batch API error: {response.status_code} - {response.text}")
            return [{"error": f"API error {response.status_code}", "should_include": True} for _ in image_paths]

        except Exception as exc:
            await self._concurrency_limiter.record_failure(is_rate_limit=False)
            logger.error(f"Vision batch API call failed: {exc}")
            return [{"error": str(exc), "should_include": True} for _ in image_paths]
        finally:
            self._concurrency_limiter.release()
    
    async def _call_vision_api(
        self,
        image_path: str,
        prompt: str = "",
        system_prompt: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化、文件系统读写实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：response.status_code == 200
        - 条件：response.status_code == 429
        依据来源（证据链）：
        输入参数：
        - image_path: 文件路径（类型：str）。
        - prompt: 文本内容（类型：str）。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        import base64
        
        # 读取并编码图片
        try:
            with open(image_path, "rb") as f:
                image_data = base64.b64encode(f.read()).decode("utf-8")
        except Exception as e:
            logger.error(f"Failed to read image {image_path}: {e}")
            return {"error": str(e), "should_include": True}
        
        # 获取并发许可（统计等待耗时）
        rate_wait_sec = await self._rate_limiter.acquire()
        rate_wait_ms = rate_wait_sec * 1000.0
        self._stats["api_rate_wait_ms_total"] += rate_wait_ms
        self._stats["api_rate_wait_count"] += 1

        wait_start = time.perf_counter()
        await self._concurrency_limiter.acquire()
        wait_ms = (time.perf_counter() - wait_start) * 1000.0
        self._stats["api_wait_ms_total"] += wait_ms
        self._stats["api_wait_count"] += 1
        
        try:
            client = await self._get_client()

            payload = {
                "model": self.config.model,
                "messages": self._build_messages(
                    image_payloads=[image_data],
                    prompt=prompt,
                    system_prompt=system_prompt,
                ),
                "temperature": self.config.temperature
            }
            
            headers = {
                "Authorization": f"Bearer {self.config.bearer_token}",
                "Content-Type": "application/json"
            }
            
            # 发送请求（统计 API 等待耗时）
            req_start = time.perf_counter()
            response = await client.post(
                self.config.base_url,
                json=payload,
                headers=headers
            )
            http_ms = (time.perf_counter() - req_start) * 1000.0
            avg_wait = self._stats["api_wait_ms_total"] / max(1, self._stats["api_wait_count"])
            avg_rate_wait = self._stats["api_rate_wait_ms_total"] / max(1, self._stats["api_rate_wait_count"])
            logger.info(
                f"Vision API timing: rate_wait={rate_wait_ms:.1f}ms, wait={wait_ms:.1f}ms, http={http_ms:.1f}ms, "
                f"avg_wait={avg_wait:.1f}ms, avg_rate_wait={avg_rate_wait:.1f}ms, "
                f"calls={self._stats['api_wait_count']}, status={response.status_code}"
            )
            
            self._stats["api_calls"] += 1
            
            if response.status_code == 200:
                await self._concurrency_limiter.record_success()
                data = response.json()
                content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                parsed = self._parse_vision_content(content)
                if isinstance(parsed, dict):
                    return parsed
                # 非 dict 返回降级为 raw，保证兼容原单图接口
                return {"raw_response": str(parsed), "should_include": True}
            
            elif response.status_code == 429:
                await self._concurrency_limiter.record_failure(is_rate_limit=True)
                logger.warning(f"Vision API rate limited (429)")
                return {"error": "rate_limited", "should_include": True}
            
            else:
                await self._concurrency_limiter.record_failure(is_rate_limit=False)
                logger.error(f"Vision API error: {response.status_code} - {response.text}")
                return {"error": f"API error {response.status_code}", "should_include": True}
        
        except Exception as e:
            await self._concurrency_limiter.record_failure(is_rate_limit=False)
            http_ms = -1.0
            if "req_start" in locals():
                http_ms = (time.perf_counter() - req_start) * 1000.0
            avg_wait = self._stats["api_wait_ms_total"] / max(1, self._stats["api_wait_count"])
            logger.error(
                f"Vision API call failed: {e} (rate_wait={rate_wait_ms:.1f}ms, wait={wait_ms:.1f}ms, http={http_ms:.1f}ms, "
                f"avg_wait={avg_wait:.1f}ms, calls={self._stats['api_wait_count']})"
            )
            return {"error": str(e), "should_include": True}
        
        finally:
            self._concurrency_limiter.release()
    
    def get_stats(self) -> Dict[str, Any]:
        """
        执行逻辑：
        1) 读取内部状态或外部资源。
        2) 返回读取结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：提供一致读取接口，降低调用耦合。
        决策逻辑：
        - 条件：self._hash_cache
        依据来源（证据链）：
        - 对象内部状态：self._hash_cache。
        输入参数：
        - 无。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        stats = self._stats.copy()
        if self._hash_cache:
            stats["hash_cache"] = self._hash_cache.get_stats()
        stats["concurrency"] = {
            "current_limit": self._concurrency_limiter.current_limit
        }
        if self._stats["api_wait_count"] > 0:
            stats["api_wait_avg_ms"] = self._stats["api_wait_ms_total"] / self._stats["api_wait_count"]
        if self._stats["api_rate_wait_count"] > 0:
            stats["api_rate_wait_avg_ms"] = self._stats["api_rate_wait_ms_total"] / self._stats["api_rate_wait_count"]
        return stats

    def validate_image_sync(
        self,
        image_path: str,
        prompt: str = "",
        system_prompt: Optional[str] = None,
        skip_duplicate_check: bool = False,
        timeout: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        作用：在同步上下文中复用后台事件循环执行异步 Vision 调用。
        """
        return _VISION_BG_LOOP.submit(
            self.validate_image(
                image_path,
                prompt,
                system_prompt=system_prompt,
                skip_duplicate_check=skip_duplicate_check,
            ),
            timeout=timeout
        )

    def validate_images_batch_sync(
        self,
        image_paths: List[str],
        prompt: str = "",
        system_prompt: Optional[str] = None,
        skip_duplicate_check: bool = False,
        max_batch_size: Optional[int] = None,
        timeout: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        """在同步上下文中调用批量 Vision 校验。"""
        return _VISION_BG_LOOP.submit(
            self.validate_images_batch(
                image_paths=image_paths,
                prompt=prompt,
                system_prompt=system_prompt,
                skip_duplicate_check=skip_duplicate_check,
                max_batch_size=max_batch_size,
            ),
            timeout=timeout,
        )

    async def close(self):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：self._http_client
        依据来源（证据链）：
        - 对象内部状态：self._http_client。
        输入参数：
        - 无。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        if self._http_client:
            await self._safe_close_client()
        if self._person_segmenter is not None:
            try:
                close_func = getattr(self._person_segmenter, "close", None)
                if callable(close_func):
                    close_func()
            except Exception as exc:
                logger.warning(f"Vision person prefilter close failed: {exc}")
            finally:
                self._person_segmenter = None


# =============================================================================
# 全局单例
# =============================================================================

_global_vision_client: Optional[VisionAIClient] = None

def get_vision_ai_client(config: Optional[VisionAIConfig] = None) -> VisionAIClient:
    """
    执行逻辑：
    1) 读取内部状态或外部资源。
    2) 返回读取结果。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：提供一致读取接口，降低调用耦合。
    决策逻辑：
    - 条件：_global_vision_client is None
    依据来源（证据链）：
    输入参数：
    - config: 配置对象/字典（类型：Optional[VisionAIConfig]）。
    输出参数：
    - 函数计算/封装后的结果对象。"""
    global _global_vision_client
    if _global_vision_client is None:
        _global_vision_client = VisionAIClient(config)
    return _global_vision_client
