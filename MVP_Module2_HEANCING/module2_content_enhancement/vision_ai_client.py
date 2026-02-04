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
import logging
import time
import numpy as np
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass, field
import httpx

logger = logging.getLogger(__name__)


# =============================================================================
# 感知哈希 (Perceptual Hash) 实现
# =============================================================================

class PerceptualHasher:
    """
    类说明：封装 PerceptualHasher 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    
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
    """
    类说明：封装 HashCacheManager 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    
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
    
    def check_duplicate(self, image_path: str) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        执行逻辑：
        1) 整理待校验数据。
        2) 按规则逐项校验并返回结果。
        实现方式：通过内部方法调用/状态更新、文件系统读写实现。
        核心价值：提前发现数据/状态问题，降低运行风险。
        决策逻辑：
        - 条件：current_hash is None
        - 条件：current_hash in self._cache
        - 条件：similarity >= self.threshold
        依据来源（证据链）：
        - 对象内部状态：self._cache, self.threshold。
        输入参数：
        - image_path: 文件路径（类型：str）。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        # 计算当前图片哈希
        current_hash = PerceptualHasher.compute_from_file(image_path)
        if current_hash is None:
            return False, None
        
        # 检查精确匹配
        if current_hash in self._cache:
            logger.info(f"Exact hash match for {Path(image_path).name}, reusing cached result")
            return True, self._cache[current_hash].get("result")
        
        # 检查近似匹配
        for cached_hash, cached_data in self._cache.items():
            similarity = PerceptualHasher.compute_similarity(current_hash, cached_hash)
            if similarity >= self.threshold:
                logger.info(f"Similar frame detected: {Path(image_path).name} ~ {cached_data.get('path', 'unknown')} "
                           f"(similarity={similarity:.1%})")
                return True, cached_data.get("result")
        
        # 缓存当前哈希 (结果稍后填充)
        self._cache[current_hash] = {"path": image_path, "result": None}
        self._path_to_hash[image_path] = current_hash
        
        return False, None
    
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
    """
    类说明：封装 VisionAIConcurrencyLimiter 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    
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

@dataclass
class VisionAIConfig:
    """
    类说明：封装 VisionAIConfig 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    enabled: bool = False
    bearer_token: str = ""
    base_url: str = "https://qianfan.baidubce.com/v2/chat/completions"
    model: str = "ernie-4.5-turbo-vl-32k"
    temperature: float = 0.3
    timeout: float = 60.0
    
    # 重复帧检测
    duplicate_detection_enabled: bool = True
    similarity_threshold: float = 0.95


class VisionAIClient:
    """
    类说明：封装 VisionAIClient 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    
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
        
        # 并发控制
        self._concurrency_limiter = VisionAIConcurrencyLimiter()
        
        # 重复帧检测
        self._hash_cache = HashCacheManager(
            similarity_threshold=self.config.similarity_threshold
        ) if self.config.duplicate_detection_enabled else None
        
        # 统计
        self._stats = {
            "total_requests": 0,
            "cache_hits": 0,
            "duplicate_skips": 0,
            "api_calls": 0,
            "api_wait_ms_total": 0.0,
            "api_wait_count": 0
        }
    
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
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                limits=httpx.Limits(
                    max_connections=20,
                    max_keepalive_connections=10,
                    keepalive_expiry=30.0
                ),
                timeout=httpx.Timeout(self.config.timeout, connect=10.0),
                http2=True
            )
            logger.info("VisionAI HTTP client initialized: pool=20, http2=True")
        return self._http_client
    
    async def validate_image(
        self,
        image_path: str,
        prompt: str,
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
            is_duplicate, cached_result = self._hash_cache.check_duplicate(image_path)
            if is_duplicate and cached_result:
                self._stats["duplicate_skips"] += 1
                logger.info(f"Duplicate frame skipped: {Path(image_path).name}")
                return cached_result
        
        # Step 2: API 调用
        if not self.config.enabled or not self.config.bearer_token:
            return {"error": "Vision AI not configured", "should_include": True}
        
        result = await self._call_vision_api(image_path, prompt)
        
        # Step 3: 缓存结果
        if self._hash_cache:
            self._hash_cache.store_result(image_path, result)
        
        return result
    
    async def _call_vision_api(self, image_path: str, prompt: str) -> Dict[str, Any]:
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
        wait_start = time.perf_counter()
        await self._concurrency_limiter.acquire()
        wait_ms = (time.perf_counter() - wait_start) * 1000.0
        self._stats["api_wait_ms_total"] += wait_ms
        self._stats["api_wait_count"] += 1
        
        try:
            client = await self._get_client()
            
            # 构建请求
            payload = {
                "model": self.config.model,
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": prompt},
                            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{image_data}"}}
                        ]
                    }
                ],
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
            logger.info(
                f"Vision API timing: wait={wait_ms:.1f}ms, http={http_ms:.1f}ms, "
                f"avg_wait={avg_wait:.1f}ms, calls={self._stats['api_wait_count']}, "
                f"status={response.status_code}"
            )
            
            self._stats["api_calls"] += 1
            
            if response.status_code == 200:
                await self._concurrency_limiter.record_success()
                data = response.json()
                content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                
                # 解析 JSON 响应
                try:
                    return json.loads(content)
                except json.JSONDecodeError:
                    return {"raw_response": content, "should_include": True}
            
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
                f"Vision API call failed: {e} (wait={wait_ms:.1f}ms, http={http_ms:.1f}ms, "
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
        return stats
    
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
            await self._http_client.aclose()
            self._http_client = None


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
