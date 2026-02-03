"""
DeepSeek API 客户端
"""

import json
import httpx
import asyncio
from typing import Dict, Any, Optional, Tuple
from datetime import datetime

from .client import LLMClient, LLMConfig, LLMResponse


class DeepSeekClient(LLMClient):
    """DeepSeek API 客户端"""
    
    def __init__(self, config: LLMConfig):
        super().__init__(config)
        self._client: Optional[httpx.AsyncClient] = None
        
    async def _get_client(self) -> httpx.AsyncClient:
        """获取 HTTP 客户端"""
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
        """关闭客户端"""
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
        完成文本生成
        
        Args:
            prompt: 用户提示词
            system_prompt: 系统提示词（可选）
            temperature: 温度参数（可选，覆盖配置）
        """
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
        完成 JSON 生成
        
        Args:
            prompt: 提示词（应包含 JSON 输出要求）
            system_prompt: 系统提示词
            
        Returns:
            (parsed_json, llm_response)
        """
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
        批量完成
        
        Args:
            prompts: 提示词列表
            system_prompt: 系统提示词
            max_concurrency: 最大并发数
        """
        semaphore = asyncio.Semaphore(max_concurrency)
        
        async def limited_complete(prompt: str) -> LLMResponse:
            async with semaphore:
                return await self.complete(prompt, system_prompt)
        
        tasks = [limited_complete(p) for p in prompts]
        return await asyncio.gather(*tasks)
