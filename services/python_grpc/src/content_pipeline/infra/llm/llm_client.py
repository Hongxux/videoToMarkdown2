"""
模块说明：Module2 内容增强中的 llm_client 模块。
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
import json
import time
import math
import hashlib
import asyncio
import logging
import importlib.util
from collections import OrderedDict
from typing import Tuple, Dict, Any, List, Optional, Callable, Awaitable, TypeVar
from dataclasses import dataclass
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log
)
import httpx
import psutil
from services.python_grpc.src.content_pipeline.infra.runtime import cache_metrics
from services.python_grpc.src.common.utils.deepseek_model_router import resolve_deepseek_model

logger = logging.getLogger(__name__)

T = TypeVar("T")


# =============================================================================
# 🚀 LLM Result Cache + In-flight Dedup (参考：LLM调用优化.md「结果缓存」)
# =============================================================================

def _env_bool(name: str, default: bool) -> bool:
    """
    做什么：解析环境变量为 bool。
    为什么：性能开关需要支持不改代码的快速调参（例如线上临时关闭缓存/打开 logprobs）。
    权衡：仅支持常见 true/false 表达，非法值回退默认值。
    """
    raw = os.getenv(name)
    if raw is None:
        return default
    val = str(raw).strip().lower()
    if val in {"1", "true", "yes", "y", "on"}:
        return True
    if val in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _env_int(name: str, default: int) -> int:
    """方法说明：_env_int 工具方法。
    执行步骤：
    1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
    2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
    3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
    raw = os.getenv(name)
    if raw is None:
        return int(default)
    try:
        return int(str(raw).strip())
    except Exception:
        return int(default)


def _supports_http2_transport() -> bool:
    """
    做什么：判断当前运行环境是否可安全启用 HTTP/2。
    为什么：在未安装 `h2` 时传入 `http2=True` 会导致 LLM 请求失败。
    权衡：优先自动探测；也允许通过环境变量 `MODULE2_HTTP2_ENABLED` 强制开关。
    """
    if os.getenv("MODULE2_HTTP2_ENABLED") is not None:
        return _env_bool("MODULE2_HTTP2_ENABLED", True)
    return importlib.util.find_spec("h2") is not None


@dataclass
class _LLMCacheEntry:
    """
    做什么：保存一次 LLM 调用的可复用结果（payload + usage）。
    为什么：重复 prompt 在重跑/并发竞态/批处理时非常常见，缓存可直接降本提速。
    权衡：缓存命中率与内存占用存在 trade-off，需要 TTL 与容量控制。
    """
    payload: Any
    model: str
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    logprobs: Any
    created_at: float
    expires_at: float


class _AsyncLRUTTLCache:
    """
    做什么：异步安全的 LRU + TTL 内存缓存。
    为什么：Module2 以单进程跑批为主，内存缓存收益高且实现轻量。
    权衡：不做跨进程共享；容量与 TTL 由环境变量控制。
    """

    def __init__(self, max_items: int, ttl_seconds: int):
        self._max_items = max(1, int(max_items))
        self._ttl_seconds = max(1, int(ttl_seconds))
        self._items: "OrderedDict[str, _LLMCacheEntry]" = OrderedDict()
        self._lock = asyncio.Lock()
        self._hits = 0
        self._misses = 0

    def now(self) -> float:
        """方法说明：_AsyncLRUTTLCache.now 核心方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        return time.time()

    def ttl_seconds(self) -> int:
        """方法说明：_AsyncLRUTTLCache.ttl_seconds 核心方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        return self._ttl_seconds

    async def get(self, key: str) -> Optional[_LLMCacheEntry]:
        """方法说明：_AsyncLRUTTLCache.get 核心方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        now = self.now()
        async with self._lock:
            entry = self._items.get(key)
            if entry is None:
                self._misses += 1
                cache_metrics.miss("module2.llm.result_cache")
                return None
            if entry.expires_at <= now:
                try:
                    del self._items[key]
                except Exception:
                    pass
                self._misses += 1
                cache_metrics.miss("module2.llm.result_cache")
                return None
            self._items.move_to_end(key, last=True)
            self._hits += 1
            cache_metrics.hit("module2.llm.result_cache")
            return entry

    async def set(self, key: str, entry: _LLMCacheEntry) -> None:
        """方法说明：_AsyncLRUTTLCache.set 核心方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        async with self._lock:
            self._items[key] = entry
            self._items.move_to_end(key, last=True)
            while len(self._items) > self._max_items:
                self._items.popitem(last=False)

    async def stats(self) -> Dict[str, Any]:
        """方法说明：_AsyncLRUTTLCache.stats 核心方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        async with self._lock:
            total = self._hits + self._misses
            hit_rate = (self._hits / total) if total else 0.0
            return {
                "items": len(self._items),
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": f"{hit_rate:.0%}",
                "ttl_seconds": self._ttl_seconds,
                "max_items": self._max_items,
            }


class _AsyncInFlightDeduper:
    """
    做什么：对同一 cache_key 的并发请求进行合并（singleflight）。
    为什么：高并发下“相同 prompt 同时触发”会造成重复调用与排队，去重后可显著提升吞吐。
    权衡：仅在进程内生效；leader 失败会把异常广播给所有等待者。
    """

    def __init__(self):
        self._lock = asyncio.Lock()
        self._inflight: Dict[str, asyncio.Future] = {}

    @staticmethod
    def _drain_future_exception(fut: asyncio.Future) -> None:
        """避免 leader 失败且无 follower 时触发 'Future exception was never retrieved'。"""
        try:
            fut.exception()
        except asyncio.CancelledError:
            return
        except Exception:
            return

    async def run(self, key: str, fn: Callable[[], Awaitable[T]]) -> T:
        """方法说明：_AsyncInFlightDeduper.run 核心方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        loop = asyncio.get_running_loop()
        async with self._lock:
            fut = self._inflight.get(key)
            if fut is None:
                fut = loop.create_future()
                fut.add_done_callback(self._drain_future_exception)
                self._inflight[key] = fut
                leader = True
            else:
                leader = False

        if not leader:
            return await fut

        try:
            result = await fn()
            if not fut.done():
                fut.set_result(result)
            return result
        except Exception as e:
            if not fut.done():
                fut.set_exception(e)
            raise
        finally:
            async with self._lock:
                self._inflight.pop(key, None)


# 全局缓存与去重器（模块内所有 LLMClient 实例共享）
_GLOBAL_CACHE = _AsyncLRUTTLCache(
    max_items=_env_int("MODULE2_LLM_CACHE_MAX_ITEMS", 1024),
    ttl_seconds=_env_int("MODULE2_LLM_CACHE_TTL_SECONDS", 3600),
)
_GLOBAL_DEDUPER = _AsyncInFlightDeduper()


# =============================================================================
# 🚀 Adaptive Concurrency Controller (AIMD Algorithm)
# =============================================================================

class AdaptiveConcurrencyLimiter:
    """类说明：AdaptiveConcurrencyLimiter 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    
    def __init__(
        self,
        initial_limit: int = 10,
        min_limit: int = 2,
        max_limit: int = 100,
        increase_step: int = 1,
        decrease_factor: float = 0.5,
        window_size: int = 20
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
        - increase_step: 函数入参（类型：int）。
        - decrease_factor: 函数入参（类型：float）。
        - window_size: 函数入参（类型：int）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self.current_limit = initial_limit
        self.min_limit = min_limit
        self.max_limit = max_limit
        self.increase_step = increase_step
        self.decrease_factor = decrease_factor
        self._external_cap: Optional[int] = None
        self._effective_limit = initial_limit
        
        # 滑动窗口统计
        self.window_size = window_size
        self.results: List[bool] = []  # True=成功, False=失败
        
        # 并发控制（支持按 token 加权的 permits，避免多次 acquire 造成死锁）
        self._lock = asyncio.Lock()
        self._cond = asyncio.Condition(self._lock)
        self._in_use_permits: int = 0
        
        logger.info(f"AdaptiveConcurrencyLimiter initialized: limit={initial_limit}, range=[{min_limit}, {max_limit}]")
    
    async def acquire(self, permits: int = 1) -> int:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - permits: 需要占用的配额数（类型：int，默认 1）。
        输出参数：
        - 实际占用的配额数（int，用于 release 对齐）。"""
        permits = max(1, int(permits))
        async with self._cond:
            # permits 过大时降级为“独占”，避免永远等待
            if permits > self._effective_limit:
                permits = max(1, self._effective_limit)
            while self._in_use_permits + permits > self._effective_limit:
                await self._cond.wait()
            self._in_use_permits += permits
            return permits
    
    async def release(self, permits: int = 1):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - permits: 释放的配额数（类型：int，默认 1）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        permits = max(1, int(permits))
        async with self._cond:
            self._in_use_permits = max(0, self._in_use_permits - permits)
            self._cond.notify_all()

    async def record_success(self):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：len(self.results) > self.window_size
        - 条件：len(self.results) >= self.window_size
        - 条件：success_rate > 0.9 and self.current_limit < self.max_limit
        依据来源（证据链）：
        - 对象内部状态：self.current_limit, self.max_limit, self.results, self.window_size。
        输入参数：
        - 无。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        async with self._cond:
            self.results.append(True)
            if len(self.results) > self.window_size:
                self.results.pop(0)
            
            # Additive Increase: 成功率 > 90% 且窗口满 -> 增加并发
            if len(self.results) >= self.window_size:
                success_rate = sum(self.results) / len(self.results)
                if success_rate > 0.9 and self.current_limit < self.max_limit:
                    old_limit = self.current_limit
                    self.current_limit = min(self.current_limit + self.increase_step, self.max_limit)
                    self._recompute_effective_limit_locked()
                    logger.debug(f"Concurrency ↑ {old_limit} → {self.current_limit} (success_rate={success_rate:.0%})")
    
    async def record_failure(self, is_rate_limit: bool = False):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：len(self.results) > self.window_size
        - 条件：is_rate_limit
        - 条件：old_limit != self.current_limit
        依据来源（证据链）：
        - 输入参数：is_rate_limit。
        - 对象内部状态：self.current_limit, self.results, self.window_size。
        输入参数：
        - is_rate_limit: 开关/状态（类型：bool）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        async with self._cond:
            self.results.append(False)
            if len(self.results) > self.window_size:
                self.results.pop(0)
            
            # Multiplicative Decrease: 立即降低并发 (尤其是 429 错误)
            old_limit = self.current_limit
            if is_rate_limit:
                # 429 错误: 激进降低
                self.current_limit = max(int(self.current_limit * self.decrease_factor), self.min_limit)
            else:
                # 其他错误: 温和降低
                self.current_limit = max(self.current_limit - 1, self.min_limit)
            
            if old_limit != self.current_limit:
                self._recompute_effective_limit_locked()
                logger.warning(f"Concurrency ↓ {old_limit} → {self.current_limit} (rate_limit={is_rate_limit})")
    
    def _recompute_effective_limit_locked(self) -> Tuple[int, int]:
        """
        做什么：重算有效并发上限（= AIMD 上限 与 外部 cap 的最小值）。
        为什么：外部 cap（资源治理）必须覆盖 AIMD 的增长，避免抖动与雪崩。
        权衡：当 cap 下降时不会强行打断已在执行的请求，只对后续 acquire 生效。
        """
        old_eff = self._effective_limit
        new_eff = self.current_limit
        if self._external_cap is not None:
            new_eff = min(new_eff, self._external_cap)
        new_eff = max(self.min_limit, min(new_eff, self.max_limit))
        if new_eff != old_eff:
            self._effective_limit = new_eff
            # 唤醒等待 acquire 的协程
            self._cond.notify_all()
        return old_eff, new_eff

    async def set_external_cap(self, cap: Optional[int]):
        """
        执行逻辑：
        1) 设置外部并发上限（可由资源或 Token 估算）。
        2) 触发有效并发上限更新。
        实现方式：保存 _external_cap 并重算 effective_limit，唤醒等待中的 acquire。
        核心价值：将资源/请求规模约束统一到并发控制中。
        输入参数：
        - cap: 外部上限（类型：Optional[int]，None 表示解除）。
        输出参数：
        - 无（仅产生副作用，如日志/状态更新）。"""
        async with self._cond:
            if cap is None:
                self._external_cap = None
            else:
                self._external_cap = max(self.min_limit, min(int(cap), self.max_limit))
            self._recompute_effective_limit_locked()

    @property
    def effective_limit(self) -> int:
        """
        对外暴露当前有效并发上限（已叠加 external cap）。
        """
        return int(self._effective_limit)
    
    @property
    def stats(self) -> Dict[str, Any]:
        """
        执行逻辑：
        1) 读取对象内部状态。
        2) 返回属性值。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：对外提供统一读路径，便于维护与扩展。
        决策逻辑：
        - 条件：self.results
        依据来源（证据链）：
        - 对象内部状态：self.results。
        输入参数：
        - 无。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        success_rate = sum(self.results) / len(self.results) if self.results else 0
        return {
            "current_limit": self.current_limit,
            "effective_limit": self._effective_limit,
            "in_use_permits": self._in_use_permits,
            "external_cap": self._external_cap,
            "success_rate": f"{success_rate:.0%}",
            "window_size": len(self.results)
        }


# =============================================================================
# 🚀 Adaptive Connection Pool Manager
# =============================================================================

class AdaptiveConnectionPoolManager:
    """类说明：AdaptiveConnectionPoolManager 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    
    def __init__(self):
        """
        执行逻辑：
        1) 解析配置或依赖，准备运行环境。
        2) 初始化对象状态、缓存与依赖客户端。
        实现方式：通过内部方法调用/状态更新、asyncio 异步调度、HTTP 调用实现。
        核心价值：在初始化阶段固化依赖，保证运行稳定性。
        输入参数：
        - 无。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self._client: Optional[httpx.AsyncClient] = None
        self._last_pool_size: int = 0
        self._rebuild_threshold: float = 0.3  # 30% 变化触发重建
        self._lock = asyncio.Lock()
    
    async def get_client(self, current_limit: int) -> httpx.AsyncClient:
        """
        执行逻辑：
        1) 读取内部状态或外部资源。
        2) 返回读取结果。
        实现方式：通过内部方法调用/状态更新、HTTP 调用实现。
        核心价值：提供一致读取接口，降低调用耦合。
        决策逻辑：
        - 条件：self._client is not None
        - 条件：change_ratio < self._rebuild_threshold
        依据来源（证据链）：
        - 对象内部状态：self._client, self._rebuild_threshold。
        输入参数：
        - current_limit: 函数入参（类型：int）。
        输出参数：
        - 函数计算/封装后的结果对象。"""
        target_pool_size = max(current_limit * 2, 20)  # 至少 20 连接
        
        # 检查是否需要重建
        if self._client is not None:
            change_ratio = abs(target_pool_size - self._last_pool_size) / max(self._last_pool_size, 1)
            if change_ratio < self._rebuild_threshold:
                return self._client  # 变化不大，复用现有池
        
        async with self._lock:
            # Double-check after acquiring lock
            if self._client is not None:
                change_ratio = abs(target_pool_size - self._last_pool_size) / max(self._last_pool_size, 1)
                if change_ratio < self._rebuild_threshold:
                    return self._client
            
            # 关闭旧连接池
            if self._client is not None:
                try:
                    await self._client.aclose()
                    logger.info(f"Connection pool closed (old size={self._last_pool_size})")
                except Exception as e:
                    logger.warning(f"Error closing old pool: {e}")
            
            # 创建新连接池
            max_connections = target_pool_size
            max_keepalive = max(current_limit, 10)
            
            http2_enabled = _supports_http2_transport()
            client_kwargs = {
                "limits": httpx.Limits(
                    max_connections=max_connections,
                    max_keepalive_connections=max_keepalive,
                    keepalive_expiry=30.0
                ),
                "headers": {
                    "Accept-Encoding": "gzip, br",  # 启用压缩
                },
                "timeout": httpx.Timeout(120.0, connect=10.0),
                "http2": http2_enabled,
            }
            try:
                self._client = httpx.AsyncClient(**client_kwargs)
            except Exception as e:
                error_text = str(e)
                if http2_enabled and "h2" in error_text:
                    logger.warning("HTTP/2 unavailable (missing h2), fallback to HTTP/1.1")
                    client_kwargs["http2"] = False
                    self._client = httpx.AsyncClient(**client_kwargs)
                    http2_enabled = False
                else:
                    raise
            self._last_pool_size = target_pool_size
            
            logger.info(f"🚀 Connection pool rebuilt: max_connections={max_connections}, "
                       f"max_keepalive={max_keepalive}, http2={http2_enabled}, compression=gzip+br")
            
            return self._client
    
    def get_client_sync(self) -> Optional[httpx.AsyncClient]:
        """
        执行逻辑：
        1) 读取内部状态或外部资源。
        2) 返回读取结果。
        实现方式：通过内部方法调用/状态更新、HTTP 调用实现。
        核心价值：提供一致读取接口，降低调用耦合。
        输入参数：
        - 无。
        输出参数：
        - 函数计算/封装后的结果对象。"""
        return self._client


# 全局连接池管理器
_global_pool_manager: Optional[AdaptiveConnectionPoolManager] = None

def get_pool_manager() -> AdaptiveConnectionPoolManager:
    """
    执行逻辑：
    1) 读取内部状态或外部资源。
    2) 返回读取结果。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：提供一致读取接口，降低调用耦合。
    决策逻辑：
    - 条件：_global_pool_manager is None
    依据来源（证据链）：
    输入参数：
    - 无。
    输出参数：
    - 函数计算/封装后的结果对象。"""
    global _global_pool_manager
    if _global_pool_manager is None:
        _global_pool_manager = AdaptiveConnectionPoolManager()
    return _global_pool_manager


# 全局自适应并发控制器
_global_concurrency_limiter: Optional[AdaptiveConcurrencyLimiter] = None

def get_concurrency_limiter() -> AdaptiveConcurrencyLimiter:
    """
    执行逻辑：
    1) 读取内部状态或外部资源。
    2) 返回读取结果。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：提供一致读取接口，降低调用耦合。
    决策逻辑：
    - 条件：_global_concurrency_limiter is None
    依据来源（证据链）：
    输入参数：
    - 无。
    输出参数：
    - 函数计算/封装后的结果对象。"""
    global _global_concurrency_limiter
    if _global_concurrency_limiter is None:
        initial_limit = max(1, _env_int("MODULE2_DEEPSEEK_CONCURRENCY_INITIAL", 56))
        min_limit = max(1, _env_int("MODULE2_DEEPSEEK_CONCURRENCY_MIN", 8))
        max_limit = max(min_limit, _env_int("MODULE2_DEEPSEEK_CONCURRENCY_MAX", 64))
        increase_step = max(1, _env_int("MODULE2_DEEPSEEK_CONCURRENCY_INCREASE_STEP", 1))
        window_size = max(1, _env_int("MODULE2_DEEPSEEK_CONCURRENCY_WINDOW_SIZE", 30))
        _global_concurrency_limiter = AdaptiveConcurrencyLimiter(
            initial_limit=initial_limit,
            min_limit=min_limit,
            max_limit=max_limit,
            increase_step=increase_step,
            window_size=window_size,
        )
    return _global_concurrency_limiter


@dataclass
class LLMResponse:
    """类说明：LLMResponse 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    model: str
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    latency_ms: float
    cache_hit: bool = False


class LLMClient:
    """类说明：LLMClient 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    
    def __init__(
        self,
        api_key: str = None,
        base_url: str = "https://api.deepseek.com/v1",
        model: str = "deepseek-chat",
        temperature: float = 0.3,
        enable_logprobs: Optional[bool] = None,
        cache_enabled: Optional[bool] = None,
        inflight_dedup_enabled: Optional[bool] = None
    ):
        """
        执行逻辑：
        1) 解析配置或依赖，准备运行环境。
        2) 初始化对象状态、缓存与依赖客户端。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：在初始化阶段固化依赖，保证运行稳定性。
        决策逻辑：
        - 条件：not self.api_key
        依据来源（证据链）：
        - 对象内部状态：self.api_key。
        输入参数：
        - api_key: 函数入参（类型：str）。
        - base_url: 函数入参（类型：str）。
        - model: 模型/推理配置（类型：str）。
        - temperature: 函数入参（类型：float）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self.api_key = api_key or os.getenv("DEEPSEEK_API_KEY")
        self.base_url = base_url
        self.model = resolve_deepseek_model(model, default_model="deepseek-chat")
        self.temperature = temperature
        # 🚀 性能开关：默认关闭 logprobs（当前调用点普遍未使用，且会增加服务端计算与返回体积）
        self._enable_logprobs = (
            _env_bool("MODULE2_LLM_ENABLE_LOGPROBS", False)
            if enable_logprobs is None
            else bool(enable_logprobs)
        )
        # 🚀 结果缓存：命中时可直接跳过网络调用（参考 LLM调用优化.md）
        self._cache_enabled = (
            _env_bool("MODULE2_LLM_CACHE_ENABLED", True)
            if cache_enabled is None
            else bool(cache_enabled)
        )
        # 🚀 In-flight 去重：并发同 prompt 时只打一次请求
        self._inflight_dedup_enabled = (
            _env_bool("MODULE2_LLM_INFLIGHT_DEDUP_ENABLED", True)
            if inflight_dedup_enabled is None
            else bool(inflight_dedup_enabled)
        )
        # 缓存大小保护（避免超长 prompt/response 占用过多内存）
        self._cache_max_prompt_chars = _env_int("MODULE2_LLM_CACHE_MAX_PROMPT_CHARS", 20000)
        self._cache_max_response_chars = _env_int("MODULE2_LLM_CACHE_MAX_RESPONSE_CHARS", 20000)
        # 资源 cap 采样节流：避免每个请求都触发 psutil 调用带来的额外开销
        self._resource_cap_interval_ms = _env_int("MODULE2_LLM_RESOURCE_CAP_INTERVAL_MS", 500)
        self._last_resource_cap_check_ts = 0.0
        self._last_resource_cap_base_limit: Optional[int] = None
        # 最大请求 token 上限与估算粒度
        self.max_request_tokens = 4000
        self.token_unit = 800
        if not self.api_key:
            raise ValueError("DEEPSEEK_API_KEY not found in environment")
        
        # 💥 稳定性增强: 增加默认超时时间 (120s)
        self.timeout = 120.0
        
        # 🚀 延迟初始化 OpenAI 客户端
        self._openai_client: Optional[Any] = None
        self._pool_manager = get_pool_manager()
        self.concurrency_limiter = get_concurrency_limiter()
    
    async def _ensure_openai_client(self):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：self._openai_client is None
        依据来源（证据链）：
        - 对象内部状态：self._openai_client。
        输入参数：
        - 无。
        输出参数：
        - 函数计算/封装后的结果对象。"""
        if self._openai_client is None:
            from openai import AsyncOpenAI
            # 获取或创建当前循环下的 http_client
            http_client = await self._pool_manager.get_client(self.concurrency_limiter.effective_limit)
            self._openai_client = AsyncOpenAI(
                api_key=self.api_key,
                base_url=self.base_url,
                timeout=self.timeout,
                http_client=http_client
            )
            logger.debug("AsyncOpenAI client initialized lazily in active loop")
        return self._openai_client
    
    async def _refresh_client_if_needed(self):
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
        from openai import AsyncOpenAI
        new_http_client = await self._pool_manager.get_client(self.concurrency_limiter.effective_limit)
        # AsyncOpenAI 不支持运行时更换 http_client，所以这里仅触发池重建
        # 新请求会自动使用更新后的池
    
    def _estimate_tokens(self, prompt: str, system_message: Optional[str] = None) -> int:
        """
        执行逻辑：按字符数/4 估算 token，最少返回 1。
        实现方式：len(prompt) + len(system_message) 的线性估算。
        核心价值：快速估算请求规模以进行并发调度。"""
        base = len(prompt or '') + len(system_message or '')
        return max(1, int(base / 4))

    def _make_cache_key(
        self,
        kind: str,
        prompt: str,
        system_message: Optional[str],
        *,
        response_format: str = "",
        enable_logprobs: bool = False,
        max_tokens: Optional[int] = None,
        model: Optional[str] = None,
    ) -> str:
        """
        做什么：生成缓存键（SHA256）。
        为什么：保证 prompt/参数 一致时才复用结果，避免“不同参数同 prompt”误命中。
        权衡：key 生成有轻微 CPU 开销，但远小于一次网络调用。
        """
        resolved_model = resolve_deepseek_model(model or self.model, default_model=self.model)
        h = hashlib.sha256()
        h.update(str(kind).encode("utf-8"))
        h.update(b"\0")
        h.update(str(self.base_url).encode("utf-8"))
        h.update(b"\0")
        h.update(str(resolved_model).encode("utf-8"))
        h.update(b"\0")
        h.update(repr(float(self.temperature)).encode("utf-8"))
        h.update(b"\0")
        h.update(str(response_format).encode("utf-8"))
        h.update(b"\0")
        h.update(b"1" if enable_logprobs else b"0")
        h.update(b"\0")
        h.update(str(max_tokens if max_tokens is not None else "").encode("utf-8"))
        h.update(b"\0")
        if system_message:
            h.update(system_message.encode("utf-8"))
        h.update(b"\0")
        if prompt:
            h.update(prompt.encode("utf-8"))
        return h.hexdigest()

    def _cacheable(self, prompt: str, system_message: Optional[str]) -> bool:
        """
        做什么：判断是否允许缓存本次请求。
        为什么：超长 prompt 缓存收益低且容易占用大量内存。
        权衡：仅做字符数级别的快速判断。
        """
        if not self._cache_enabled:
            return False
        total_chars = len(prompt or "") + len(system_message or "")
        return total_chars <= int(self._cache_max_prompt_chars)

    def _entry_to_metadata(self, entry: _LLMCacheEntry) -> "LLMResponse":
        """方法说明：LLMClient._entry_to_metadata 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        return LLMResponse(
            model=entry.model,
            prompt_tokens=int(entry.prompt_tokens),
            completion_tokens=int(entry.completion_tokens),
            total_tokens=int(entry.total_tokens),
            latency_ms=0.0,
            cache_hit=True,
        )

    def _compute_resource_cap(self, base_limit: int) -> int:
        """
        执行逻辑：基于 CPU/内存占用估算并发上限。
        实现方式：按占用比例设置衰减因子。
        核心价值：避免资源紧张时并发过高导致抖动。"""
        try:
            cpu_percent = psutil.cpu_percent(interval=None)
            mem_percent = psutil.virtual_memory().percent
        except Exception:
            return base_limit
        cpu_factor = 1.0
        mem_factor = 1.0
        if cpu_percent > 90:
            cpu_factor = 0.3
        elif cpu_percent > 80:
            cpu_factor = 0.5
        elif cpu_percent > 70:
            cpu_factor = 0.7
        if mem_percent > 90:
            mem_factor = 0.3
        elif mem_percent > 80:
            mem_factor = 0.5
        elif mem_percent > 75:
            mem_factor = 0.7
        return max(1, int(base_limit * cpu_factor * mem_factor))

    def _compute_permits(self, est_tokens: int) -> int:
        """
        做什么：把单次请求的 token 规模映射为并发配额（permits）。
        为什么：避免“按请求数”的并发让大请求把小请求拖慢；按 token 加权更贴近真实成本。
        估算：token≈字符/4；每 token_unit(默认 800) 记为 1 个 permit。
        """
        est_tokens = max(1, int(est_tokens))
        permits = int(math.ceil(est_tokens / float(self.token_unit)))
        permits = max(1, permits)
        # 超过约定上限时，倾向于独占（最终会在 limiter.acquire 内部降级）
        if est_tokens > self.max_request_tokens:
            permits = max(permits, int(self.concurrency_limiter.current_limit))
        return permits

    async def _apply_resource_cap(self):
        """
        执行逻辑：基于 CPU/内存占用对并发上限施加外部 cap。
        实现方式：当资源紧张时设置 external cap；资源充裕时解除 cap。
        核心价值：让并发在高压下自动收敛，优先保证单任务时延稳定。"""
        base_limit = int(self.concurrency_limiter.current_limit)

        # 🚀 节流：避免每个请求都调用 psutil（在高并发/高频调用下这部分会形成可观的 CPU 开销）
        now = time.time()
        if (
            int(self._resource_cap_interval_ms) > 0
            and self._last_resource_cap_base_limit == base_limit
            and (now - float(self._last_resource_cap_check_ts)) * 1000.0 < float(self._resource_cap_interval_ms)
        ):
            return
        self._last_resource_cap_check_ts = now
        self._last_resource_cap_base_limit = base_limit

        resource_cap = int(self._compute_resource_cap(base_limit))
        if resource_cap >= base_limit:
            await self.concurrency_limiter.set_external_cap(None)
        else:
            await self.concurrency_limiter.set_external_cap(resource_cap)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((httpx.NetworkError, httpx.TimeoutException)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True
    )
    
    async def complete_json(
        self,
        prompt: str,
        system_message: str = None,
        need_logprobs: bool = False,
        max_tokens: Optional[int] = None,
        disable_inflight_dedup: bool = False,
        model: Optional[str] = None,
    ) -> Tuple[Dict[str, Any], LLMResponse, Any]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：system_message
        - 条件：'402' in error_msg or 'Insufficient Balance' in error_msg
        依据来源（证据链）：
        - 输入参数：system_message。
        输入参数：
        - prompt: 文本内容（类型：str）。
        - system_message: 函数入参（类型：str）。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        resolved_model = resolve_deepseek_model(model or self.model, default_model=self.model)
        enable_logprobs = bool(need_logprobs or self._enable_logprobs)

        cache_key: Optional[str] = None
        if self._cacheable(prompt, system_message):
            cache_key = self._make_cache_key(
                "json",
                prompt,
                system_message,
                response_format="json_object",
                enable_logprobs=enable_logprobs,
                max_tokens=max_tokens,
                model=resolved_model,
            )
            cached = await _GLOBAL_CACHE.get(cache_key)
            if cached is not None:
                try:
                    parsed_cached = json.loads(cached.payload)
                    return parsed_cached, self._entry_to_metadata(cached), cached.logprobs
                except Exception as e:
                    logger.debug(f"LLM cache entry JSON parse failed, bypassing cache: {e}")

        async def _do_request():
            import time
            start_time = time.time()

            messages = []
            if system_message:
                messages.append({"role": "system", "content": system_message})
            messages.append({"role": "user", "content": prompt})

            client = await self._ensure_openai_client()

            acquired_permits = 0

            # 估算 token 并获取加权 permits（字符/4 -> token）
            est_tokens = self._estimate_tokens(prompt, system_message)
            permits = self._compute_permits(est_tokens)
            await self._apply_resource_cap()

            # 获取并发许可（按 token 加权）
            acquired_permits = await self.concurrency_limiter.acquire(permits)

            try:
                kwargs = {
                    "model": resolved_model,
                    "messages": messages,
                    "temperature": self.temperature,
                    "response_format": {"type": "json_object"},
                }
                if isinstance(max_tokens, int) and max_tokens > 0:
                    kwargs["max_tokens"] = max_tokens
                if enable_logprobs:
                    kwargs["logprobs"] = True
                    kwargs["top_logprobs"] = 1

                logger.info("[LLM] 正在进行调用...")
                response = await client.chat.completions.create(**kwargs)

                # 解析JSON
                content = response.choices[0].message.content
                parsed = json.loads(content)

                # 提取logprobs (可选)
                lprobs = getattr(response.choices[0], "logprobs", None) if enable_logprobs else None

                # 构建响应元数据
                latency_ms = (time.time() - start_time) * 1000
                metadata = LLMResponse(
                    model=response.model,
                    prompt_tokens=response.usage.prompt_tokens,
                    completion_tokens=response.usage.completion_tokens,
                    total_tokens=response.usage.total_tokens,
                    latency_ms=latency_ms,
                )


                # 🚀 记录成功
                await self.concurrency_limiter.record_success()

                # 🚀 写入缓存（超长响应跳过）
                if (
                    cache_key
                    and self._cache_enabled
                    and isinstance(content, str)
                    and len(content) <= int(self._cache_max_response_chars)
                ):
                    now = _GLOBAL_CACHE.now()
                    await _GLOBAL_CACHE.set(
                        cache_key,
                        _LLMCacheEntry(
                            payload=content,
                            model=metadata.model,
                            prompt_tokens=metadata.prompt_tokens,
                            completion_tokens=metadata.completion_tokens,
                            total_tokens=metadata.total_tokens,
                            logprobs=lprobs,
                            created_at=now,
                            expires_at=now + float(_GLOBAL_CACHE.ttl_seconds()),
                        ),
                    )

                return parsed, metadata, lprobs

            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON from LLM response: {e}")
                await self.concurrency_limiter.record_failure(is_rate_limit=False)
                raise ValueError(f"LLM returned invalid JSON: {e}")

            except asyncio.CancelledError:
                raise
            except Exception as e:
                error_msg = str(e)

                # 🚀 检测 429 (Rate Limit) 错误
                is_rate_limit = (
                    "429" in error_msg
                    or "rate" in error_msg.lower()
                    or "Too Many Requests" in error_msg
                )
                await self.concurrency_limiter.record_failure(is_rate_limit=is_rate_limit)

                # 💥 V8.1: 增强对 402 (余额不足) 的识别
                if "402" in error_msg or "Insufficient Balance" in error_msg:
                    logger.error("❌ DeepSeek API 余额不足 (Error 402). 请检查您的账户余额并充值。")
                    raise ValueError(
                        "DeepSeek API 提取失败: 账户余额不足 (Error 402)。建议充值或更换 API Key。"
                    ) from e

                logger.error(f"LLM API call failed (JSON): {e}")
                raise

            finally:
                # 🚀 释放并发许可
                if acquired_permits:
                    await self.concurrency_limiter.release(acquired_permits)

        if cache_key and self._inflight_dedup_enabled and not disable_inflight_dedup:
            return await _GLOBAL_DEDUPER.run(cache_key, _do_request)
        return await _do_request()
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.NetworkError, httpx.TimeoutException)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True
    )
    async def complete_text(
        self,
        prompt: str,
        system_message: str = None,
        need_logprobs: bool = False,
        disable_inflight_dedup: bool = False,
        model: Optional[str] = None,
    ) -> Tuple[str, LLMResponse, Any]:
        """
        完成文本生成请求。支持通过 model 参数临时覆盖默认模型。
        """
        resolved_model = resolve_deepseek_model(model or self.model, default_model=self.model)
        enable_logprobs = bool(need_logprobs or self._enable_logprobs)

        cache_key: Optional[str] = None
        if self._cacheable(prompt, system_message):
            cache_key = self._make_cache_key(
                "text",
                prompt,
                system_message,
                response_format="",
                enable_logprobs=enable_logprobs,
                model=resolved_model,
            )
            cached = await _GLOBAL_CACHE.get(cache_key)
            if cached is not None:
                return str(cached.payload), self._entry_to_metadata(cached), cached.logprobs

        async def _do_request():
            import time
            start_time = time.time()

            messages = []
            if system_message:
                messages.append({"role": "system", "content": system_message})
            messages.append({"role": "user", "content": prompt})

            client = await self._ensure_openai_client()
            acquired_permits = 0

            # 估算 token 并获取加权 permits（字符/4 -> token）
            est_tokens = self._estimate_tokens(prompt, system_message)
            permits = self._compute_permits(est_tokens)
            await self._apply_resource_cap()

            # 获取并发许可（按 token 加权）
            acquired_permits = await self.concurrency_limiter.acquire(permits)
            try:
                kwargs = {
                    "model": resolved_model,
                    "messages": messages,
                    "temperature": self.temperature,
                }
                if enable_logprobs:
                    kwargs["logprobs"] = True
                    kwargs["top_logprobs"] = 1

                logger.info("[LLM] 正在进行调用...")
                response = await client.chat.completions.create(**kwargs)

                content = response.choices[0].message.content
                lprobs = getattr(response.choices[0], "logprobs", None) if enable_logprobs else None

                latency_ms = (time.time() - start_time) * 1000
                metadata = LLMResponse(
                    model=response.model,
                    prompt_tokens=response.usage.prompt_tokens,
                    completion_tokens=response.usage.completion_tokens,
                    total_tokens=response.usage.total_tokens,
                    latency_ms=latency_ms,
                )

                # 🚀 记录成功
                await self.concurrency_limiter.record_success()

                # 🚀 写入缓存（超长响应跳过）
                if (
                    cache_key
                    and self._cache_enabled
                    and isinstance(content, str)
                    and len(content) <= int(self._cache_max_response_chars)
                ):
                    now = _GLOBAL_CACHE.now()
                    await _GLOBAL_CACHE.set(
                        cache_key,
                        _LLMCacheEntry(
                            payload=content,
                            model=metadata.model,
                            prompt_tokens=metadata.prompt_tokens,
                            completion_tokens=metadata.completion_tokens,
                            total_tokens=metadata.total_tokens,
                            logprobs=lprobs,
                            created_at=now,
                            expires_at=now + float(_GLOBAL_CACHE.ttl_seconds()),
                        ),
                    )

                return content, metadata, lprobs

            except asyncio.CancelledError:
                raise
            except Exception as e:
                error_msg = str(e)

                # 🚀 检测 429 (Rate Limit) 错误
                is_rate_limit = (
                    "429" in error_msg
                    or "rate" in error_msg.lower()
                    or "Too Many Requests" in error_msg
                )
                await self.concurrency_limiter.record_failure(is_rate_limit=is_rate_limit)

                # 💥 V8.1: 增强对 402 (余额不足) 的识别
                if "402" in error_msg or "Insufficient Balance" in error_msg:
                    logger.error("❌ DeepSeek API 余额不足 (Error 402). 请检查您的账户余额并充值。")
                    raise ValueError(
                        "DeepSeek API 提取失败: 账户余额不足 (Error 402)。建议充值或更换 API Key。"
                    ) from e

                logger.error(f"LLM API call failed: {e}")
                raise

            finally:
                # 🚀 释放并发许可
                if acquired_permits:
                    await self.concurrency_limiter.release(acquired_permits)

        if cache_key and self._inflight_dedup_enabled and not disable_inflight_dedup:
            return await _GLOBAL_DEDUPER.run(cache_key, _do_request)
        return await _do_request()


def create_llm_client(
    model: str = "deepseek-chat",
    temperature: float = 0.3
) -> LLMClient:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    输入参数：
    - model: 模型/推理配置（类型：str）。
    - temperature: 函数入参（类型：float）。
    输出参数：
    - LLMClient 对象或调用结果。"""
    return LLMClient(model=resolve_deepseek_model(model, default_model="deepseek-chat"), temperature=temperature)
