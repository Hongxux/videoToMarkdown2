"""
模块说明：阶段 LLM 适配 deepseek 的实现。
执行逻辑：
1) 聚合本模块的类/函数，对外提供核心能力。
2) 通过内部调用与外部依赖完成具体处理。
实现方式：通过模块内函数组合与外部依赖调用实现。
核心价值：统一模块职责边界，降低跨文件耦合成本。
输入：
- 调用方传入的参数与数据路径。
输出：
- 各函数/类返回的结构化结果或副作用。"""

import json
import httpx
import asyncio
from typing import Dict, Any, Optional, Tuple
from datetime import datetime

from .client import LLMClient, LLMConfig, LLMResponse


class DeepSeekClient(LLMClient):
    """
    类说明：封装 DeepSeekClient 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    
    def __init__(self, config: LLMConfig):
        """
        执行逻辑：
        1) 解析配置或依赖，准备运行环境。
        2) 初始化对象状态、缓存与依赖客户端。
        实现方式：通过内部方法调用/状态更新、HTTP 调用实现。
        核心价值：在初始化阶段固化依赖，保证运行稳定性。
        输入参数：
        - config: 配置对象/字典（类型：LLMConfig）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        super().__init__(config)
        self._client: Optional[httpx.AsyncClient] = None
        
    async def _get_client(self) -> httpx.AsyncClient:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化、HTTP 调用实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：self._client is None
        依据来源（证据链）：
        - 对象内部状态：self._client。
        输入参数：
        - 无。
        输出参数：
        - 函数计算/封装后的结果对象。"""
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.config.base_url,
                headers={
                    "Authorization": f"Bearer {self.config.api_key}",
                    "Content-Type": "application/json"
                },
                timeout=self.config.timeout
            )
        return self._client
    
    async def close(self):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：self._client
        依据来源（证据链）：
        - 对象内部状态：self._client。
        输入参数：
        - 无。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        if self._client:
            await self._client.aclose()
            self._client = None
    
    async def complete(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        temperature: Optional[float] = None,
        **kwargs
    ) -> LLMResponse:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化、HTTP 调用实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：system_prompt
        - 条件：temperature is not None
        依据来源（证据链）：
        - 输入参数：system_prompt, temperature。
        输入参数：
        - prompt: 文本内容（类型：str）。
        - system_prompt: 函数入参（类型：Optional[str]）。
        - temperature: 函数入参（类型：Optional[float]）。
        - **kwargs: 可变参数，含义由调用方决定。
        输出参数：
        - LLMResponse 响应对象。"""
        client = await self._get_client()
        
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})
        
        payload = {
            "model": self.config.model,
            "messages": messages,
            "temperature": temperature if temperature is not None else self.config.temperature,
            "max_tokens": self.config.max_tokens,
            **kwargs
        }
        
        start_time = datetime.now()
        
        try:
            response = await client.post("/chat/completions", json=payload)
            response.raise_for_status()
            data = response.json()
            
            latency_ms = (datetime.now() - start_time).total_seconds() * 1000
            
            content = data["choices"][0]["message"]["content"]
            usage = data.get("usage", {})
            
            # 记录最后一次调用
            self._last_prompt = prompt
            self._last_response = content
            self._last_token_count = usage.get("total_tokens", 0)
            
            return LLMResponse(
                content=content,
                prompt_tokens=usage.get("prompt_tokens", 0),
                completion_tokens=usage.get("completion_tokens", 0),
                total_tokens=usage.get("total_tokens", 0),
                model=self.config.model,
                latency_ms=latency_ms,
                raw_response=data
            )
            
        except httpx.HTTPStatusError as e:
            raise RuntimeError(f"DeepSeek API error: {e.response.status_code} - {e.response.text}")
        except Exception as e:
            raise RuntimeError(f"DeepSeek API error: {str(e)}")
    
    async def complete_json(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        **kwargs
    ) -> Tuple[Dict, LLMResponse]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：'```json' in content
        - 条件：'```' in content
        依据来源（证据链）：
        输入参数：
        - prompt: 文本内容（类型：str）。
        - system_prompt: 函数入参（类型：Optional[str]）。
        - **kwargs: 可变参数，含义由调用方决定。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        # 添加 JSON 模式提示
        json_system = (system_prompt or "") + "\n\n请确保输出为有效的 JSON 格式。"
        
        response = await self.complete(
            prompt=prompt,
            system_prompt=json_system,
            **kwargs
        )
        
        # 解析 JSON
        content = response.content.strip()
        
        # 尝试提取 JSON 块
        if "```json" in content:
            start = content.find("```json") + 7
            end = content.find("```", start)
            content = content[start:end].strip()
        elif "```" in content:
            start = content.find("```") + 3
            end = content.find("```", start)
            content = content[start:end].strip()
            
        try:
            parsed = json.loads(content)
            return parsed, response
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to parse JSON response: {e}\nContent: {content[:500]}")
    
    async def complete_batch(
        self,
        prompts: list[str],
        system_prompt: Optional[str] = None,
        max_concurrency: int = 5
    ) -> list[LLMResponse]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、asyncio 异步调度实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - prompts: 函数入参（类型：list[str]）。
        - system_prompt: 函数入参（类型：Optional[str]）。
        - max_concurrency: 函数入参（类型：int）。
        输出参数：
        - LLMResponse 列表（与输入或处理结果一一对应）。"""
        semaphore = asyncio.Semaphore(max_concurrency)
        
        async def limited_complete(prompt: str) -> LLMResponse:
            """
            执行逻辑：
            1) 准备必要上下文与参数。
            2) 执行核心处理并返回结果。
            实现方式：通过内部方法调用/状态更新实现。
            核心价值：封装逻辑单元，提升复用与可维护性。
            输入参数：
            - prompt: 文本内容（类型：str）。
            输出参数：
            - LLMResponse 响应对象。"""
            async with semaphore:
                return await self.complete(prompt, system_prompt)
        
        tasks = [limited_complete(p) for p in prompts]
        return await asyncio.gather(*tasks)
