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
import asyncio
import logging
from typing import Tuple, Dict, Any, List, Optional
from dataclasses import dataclass
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log
)
import httpx

logger = logging.getLogger(__name__)


# =============================================================================
# 🚀 Adaptive Concurrency Controller (AIMD Algorithm)
# =============================================================================

class AdaptiveConcurrencyLimiter:
    """
    类说明：封装 AdaptiveConcurrencyLimiter 的职责与行为。
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
        
        # 滑动窗口统计
        self.window_size = window_size
        self.results: List[bool] = []  # True=成功, False=失败
        
        # 信号量 (动态调整)
        self._semaphore = asyncio.Semaphore(initial_limit)
        self._lock = asyncio.Lock()
        
        logger.info(f"AdaptiveConcurrencyLimiter initialized: limit={initial_limit}, range=[{min_limit}, {max_limit}]")
    
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
        async with self._lock:
            self.results.append(True)
            if len(self.results) > self.window_size:
                self.results.pop(0)
            
            # Additive Increase: 成功率 > 90% 且窗口满 -> 增加并发
            if len(self.results) >= self.window_size:
                success_rate = sum(self.results) / len(self.results)
                if success_rate > 0.9 and self.current_limit < self.max_limit:
                    old_limit = self.current_limit
                    self.current_limit = min(self.current_limit + self.increase_step, self.max_limit)
                    self._update_semaphore(old_limit, self.current_limit)
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
        async with self._lock:
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
                self._update_semaphore(old_limit, self.current_limit)
                logger.warning(f"Concurrency ↓ {old_limit} → {self.current_limit} (rate_limit={is_rate_limit})")
    
    def _update_semaphore(self, old_limit: int, new_limit: int):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、asyncio 异步调度实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - old_limit: 函数入参（类型：int）。
        - new_limit: 函数入参（类型：int）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        # 简化处理: 重新创建信号量 (协程安全)
        # 注意: 这不会立即释放已获取的许可，但新请求会使用新限制
        self._semaphore = asyncio.Semaphore(new_limit)
    
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
            "success_rate": f"{success_rate:.0%}",
            "window_size": len(self.results)
        }


# =============================================================================
# 🚀 Adaptive Connection Pool Manager
# =============================================================================

class AdaptiveConnectionPoolManager:
    """
    类说明：封装 AdaptiveConnectionPoolManager 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    
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
            
            self._client = httpx.AsyncClient(
                limits=httpx.Limits(
                    max_connections=max_connections,
                    max_keepalive_connections=max_keepalive,
                    keepalive_expiry=30.0
                ),
                headers={
                    "Accept-Encoding": "gzip, br",  # 启用压缩
                },
                timeout=httpx.Timeout(120.0, connect=10.0),
                http2=True  # 启用 HTTP/2
            )
            self._last_pool_size = target_pool_size
            
            logger.info(f"🚀 Connection pool rebuilt: max_connections={max_connections}, "
                       f"max_keepalive={max_keepalive}, http2=True, compression=gzip+br")
            
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
        _global_concurrency_limiter = AdaptiveConcurrencyLimiter(
            initial_limit=20,
            min_limit=2,
            max_limit=300
        )
    return _global_concurrency_limiter


@dataclass
class LLMResponse:
    """
    类说明：封装 LLMResponse 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    model: str
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    latency_ms: float


class LLMClient:
    """
    类说明：封装 LLMClient 的职责与行为。
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
        api_key: str = None,
        base_url: str = "https://api.deepseek.com/v1",
        model: str = "deepseek-chat",
        temperature: float = 0.3
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
        self.model = model
        self.temperature = temperature
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
            http_client = await self._pool_manager.get_client(self.concurrency_limiter.current_limit)
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
        new_http_client = await self._pool_manager.get_client(self.concurrency_limiter.current_limit)
        # AsyncOpenAI 不支持运行时更换 http_client，所以这里仅触发池重建
        # 新请求会自动使用更新后的池
    
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
        system_message: str = None
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
        import time
        start_time = time.time()
        
        messages = []
        if system_message:
            messages.append({"role": "system", "content": system_message})
        messages.append({"role": "user", "content": prompt})
        
        client = await self._ensure_openai_client()
        
        # 🚀 获取并发许可
        await self.concurrency_limiter.acquire()
        
        try:
            response = await client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=self.temperature,
                response_format={"type": "json_object"},
                logprobs=True,
                top_logprobs=1
            )
            
            # 解析JSON
            content = response.choices[0].message.content
            parsed = json.loads(content)
            
            # 提取logprobs (用于Perplexity计算)
            lprobs = response.choices[0].logprobs
            
            # 构建响应元数据
            latency_ms = (time.time() - start_time) * 1000
            metadata = LLMResponse(
                model=response.model,
                prompt_tokens=response.usage.prompt_tokens,
                completion_tokens=response.usage.completion_tokens,
                total_tokens=response.usage.total_tokens,
                latency_ms=latency_ms
            )
            
            logger.info(f"LLM call completed: {metadata.total_tokens} tokens, "
                       f"{latency_ms:.0f}ms")
            
            # 🚀 记录成功
            await self.concurrency_limiter.record_success()
            
            return parsed, metadata, lprobs
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON from LLM response: {e}")
            logger.error(f"Raw content: {content}")
            await self.concurrency_limiter.record_failure(is_rate_limit=False)
            raise ValueError(f"LLM returned invalid JSON: {e}")
        
        except Exception as e:
            error_msg = str(e)
            
            # 🚀 检测 429 (Rate Limit) 错误
            is_rate_limit = "429" in error_msg or "rate" in error_msg.lower() or "Too Many Requests" in error_msg
            await self.concurrency_limiter.record_failure(is_rate_limit=is_rate_limit)
            
            # 💥 V8.1: 增强对 402 (余额不足) 的识别
            if "402" in error_msg or "Insufficient Balance" in error_msg:
                logger.error("❌ DeepSeek API 余额不足 (Error 402). 请检查您的账户余额并充值。")
                raise ValueError("DeepSeek API 提取失败: 账户余额不足 (Error 402)。建议充值或更换 API Key。") from e
            
            logger.error(f"LLM API call failed (JSON): {e}")
            raise
        
        finally:
            # 🚀 释放并发许可
            self.concurrency_limiter.release()
    
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
        system_message: str = None
    ) -> Tuple[str, LLMResponse, Any]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
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
        - 多值结果元组（各元素含义见实现）。"""
        import time
        start_time = time.time()
        
        messages = []
        if system_message:
            messages.append({"role": "system", "content": system_message})
        messages.append({"role": "user", "content": prompt})
        
        client = await self._ensure_openai_client()
        try:
            response = await client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=self.temperature,
                logprobs=True,
                top_logprobs=1
            )
            
            content = response.choices[0].message.content
            lprobs = response.choices[0].logprobs
            
            latency_ms = (time.time() - start_time) * 1000
            metadata = LLMResponse(
                model=response.model,
                prompt_tokens=response.usage.prompt_tokens,
                completion_tokens=response.usage.completion_tokens,
                total_tokens=response.usage.total_tokens,
                latency_ms=latency_ms
            )
            
            logger.info(f"LLM call completed: {metadata.total_tokens} tokens")
            
            # 🚀 记录成功
            await self.concurrency_limiter.record_success()
            
            return content, metadata, lprobs
            
        except Exception as e:
            error_msg = str(e)
            
            # 🚀 检测 429 (Rate Limit) 错误
            is_rate_limit = "429" in error_msg or "rate" in error_msg.lower() or "Too Many Requests" in error_msg
            await self.concurrency_limiter.record_failure(is_rate_limit=is_rate_limit)
            
            # 💥 V8.1: 增强对 402 (余额不足) 的识别
            if "402" in error_msg or "Insufficient Balance" in error_msg:
                logger.error("❌ DeepSeek API 余额不足 (Error 402). 请检查您的账户余额并充值。")
                raise ValueError("DeepSeek API 提取失败: 账户余额不足 (Error 402)。建议充值或更换 API Key。") from e
                
            logger.error(f"LLM API call failed: {e}")
            raise
        
        finally:
            # 🚀 释放并发许可
            self.concurrency_limiter.release()


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
    return LLMClient(model=model, temperature=temperature)
