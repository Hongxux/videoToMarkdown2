"""
Vision AI Client - 模块化 Vision API 调用

功能:
1. 统一的 Vision AI 接口 (支持 ERNIE Vision, OpenAI Vision 等)
2. 自适应并发控制 (AIMD 算法)
3. 结果缓存 (基于图片感知哈希)
4. 连接池 + HTTP/2 + 压缩
5. 重复帧检测 (pHash)

V1.0 - 2024-02
"""

import os
import cv2
import json
import asyncio
import hashlib
import logging
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
    感知哈希计算器
    
    使用 dHash (差异哈希) 算法:
    - 缩放到 9x8 灰度图
    - 计算相邻像素差异
    - 生成 64-bit 哈希
    
    优点: 快速、对缩放/轻微变化鲁棒
    """
    
    HASH_SIZE = 8  # 8x8 = 64 bits
    
    @staticmethod
    def compute_dhash(image: np.ndarray) -> str:
        """
        计算图像的差异哈希 (dHash)
        
        Args:
            image: BGR 或灰度图像
            
        Returns:
            16 字符十六进制字符串 (64 bits)
        """
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
        计算两个哈希的相似度 (汉明距离)
        
        Returns:
            相似度 0.0 - 1.0 (1.0 = 完全相同)
        """
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
        """从文件计算哈希"""
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
    感知哈希缓存管理器
    
    功能:
    1. 缓存已处理图片的哈希值
    2. 快速检测重复帧 (相似度 > 阈值)
    3. 支持获取最相似的已处理帧
    """
    
    def __init__(self, similarity_threshold: float = 0.95):
        """
        Args:
            similarity_threshold: 相似度阈值，超过则判定为重复帧
        """
        self.threshold = similarity_threshold
        self._cache: Dict[str, Dict[str, Any]] = {}  # hash -> {path, result}
        self._path_to_hash: Dict[str, str] = {}  # path -> hash
    
    def check_duplicate(self, image_path: str) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        检查是否为重复帧
        
        Returns:
            (is_duplicate, cached_result_if_duplicate)
        """
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
        """存储验证结果"""
        if image_path in self._path_to_hash:
            hash_key = self._path_to_hash[image_path]
            if hash_key in self._cache:
                self._cache[hash_key]["result"] = result
    
    def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计"""
        return {
            "cached_hashes": len(self._cache),
            "threshold": f"{self.threshold:.0%}"
        }


# =============================================================================
# 自适应并发控制器 (复用 llm_client.py 的 AIMD 逻辑)
# =============================================================================

class VisionAIConcurrencyLimiter:
    """
    Vision AI 专用自适应并发控制器
    
    Vision API 通常限流更严 (如 10 QPS)，所以默认值更保守
    """
    
    def __init__(
        self,
        initial_limit: int = 5,
        min_limit: int = 5,
        max_limit: int = 60,
        decrease_factor: float = 0.5
    ):
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
        await self._semaphore.acquire()
    
    def release(self):
        self._semaphore.release()
    
    async def record_success(self):
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
    """Vision AI 配置"""
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
    模块化 Vision AI 客户端
    
    功能:
    1. 连接池 + HTTP/2
    2. 自适应并发控制
    3. 重复帧检测 (pHash)
    4. 结果缓存
    """
    
    def __init__(self, config: Optional[VisionAIConfig] = None):
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
            "api_calls": 0
        }
    
    async def _get_client(self) -> httpx.AsyncClient:
        """获取或创建 HTTP 客户端"""
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
        验证图像
        
        Args:
            image_path: 图像路径
            prompt: 验证提示词
            skip_duplicate_check: 是否跳过重复检测
            
        Returns:
            验证结果字典
        """
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
        """调用 Vision API"""
        import base64
        
        # 读取并编码图片
        try:
            with open(image_path, "rb") as f:
                image_data = base64.b64encode(f.read()).decode("utf-8")
        except Exception as e:
            logger.error(f"Failed to read image {image_path}: {e}")
            return {"error": str(e), "should_include": True}
        
        # 获取并发许可
        await self._concurrency_limiter.acquire()
        
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
            
            # 发送请求
            response = await client.post(
                self.config.base_url,
                json=payload,
                headers=headers
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
            logger.error(f"Vision API call failed: {e}")
            return {"error": str(e), "should_include": True}
        
        finally:
            self._concurrency_limiter.release()
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        stats = self._stats.copy()
        if self._hash_cache:
            stats["hash_cache"] = self._hash_cache.get_stats()
        stats["concurrency"] = {
            "current_limit": self._concurrency_limiter.current_limit
        }
        return stats
    
    async def close(self):
        """关闭客户端"""
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None


# =============================================================================
# 全局单例
# =============================================================================

_global_vision_client: Optional[VisionAIClient] = None

def get_vision_ai_client(config: Optional[VisionAIConfig] = None) -> VisionAIClient:
    """获取全局 Vision AI 客户端"""
    global _global_vision_client
    if _global_vision_client is None:
        _global_vision_client = VisionAIClient(config)
    return _global_vision_client
