"""
LLM 客户端基类和工厂
支持从 config.yaml 配置创建客户端
"""

import os
import yaml
import httpx
import asyncio
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass
class LLMResponse:
    """LLM 响应"""
    content: str
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    model: str
    latency_ms: float
    raw_response: Optional[Dict] = None
    
    
@dataclass
class LLMConfig:
    """LLM 配置"""
    api_key: str
    base_url: str
    model: str
    temperature: float = 0.1
    max_tokens: int = 4096
    timeout: float = 180.0  # 增加到 180 秒以应对大批次处理


class LLMClient(ABC):
    """LLM 客户端基类"""
    
    def __init__(self, config: LLMConfig):
        self.config = config
        self._last_prompt = ""
        self._last_response = ""
        self._last_token_count = 0
        
    @property
    def last_prompt(self) -> str:
        return self._last_prompt
    
    @property
    def last_response(self) -> str:
        return self._last_response
    
    @property
    def last_token_count(self) -> int:
        return self._last_token_count
        
    @abstractmethod
    async def complete(
        self, 
        prompt: str,
        system_prompt: Optional[str] = None,
        temperature: Optional[float] = None,
        **kwargs
    ) -> LLMResponse:
        """完成文本生成"""
        pass
    
    @abstractmethod
    async def complete_json(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        **kwargs
    ) -> Tuple[Dict, LLMResponse]:
        """完成 JSON 生成"""
        pass
    
    async def complete_with_retry(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        max_retries: int = 3,
        **kwargs
    ) -> LLMResponse:
        """带重试的完成"""
        last_error = None
        for attempt in range(max_retries):
            try:
                return await self.complete(prompt, system_prompt, **kwargs)
            except Exception as e:
                last_error = e
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)  # 指数退避
        raise last_error


def load_config(config_path: str = "config.yaml") -> Dict[str, Any]:
    """加载配置文件"""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def create_llm_client(
    config_path: str = "config.yaml",
    purpose: str = "analysis"  # refinement, analysis, topic
) -> "LLMClient":
    """
    从配置创建 LLM 客户端
    
    Args:
        config_path: 配置文件路径
        purpose: 用途，决定使用哪个模型配置
            - refinement: 字幕整理
            - analysis: 内容分析
            - topic: 主题识别
    """
    from .deepseek import DeepSeekClient
    
    config = load_config(config_path)
    ai_config = config.get("ai", {})
    
    # 获取 API Key（优先环境变量）
    api_key = os.environ.get("DEEPSEEK_API_KEY") or ai_config.get("api_key", "")
    if not api_key:
        raise ValueError("DEEPSEEK_API_KEY not set")
    
    base_url = ai_config.get("base_url", "https://api.deepseek.com")
    
    # 根据用途选择配置
    purpose_config = ai_config.get(purpose, ai_config.get("analysis", {}))
    model = purpose_config.get("model", "deepseek-chat")
    temperature = purpose_config.get("temperature", 0.1)
    
    llm_config = LLMConfig(
        api_key=api_key,
        base_url=base_url,
        model=model,
        temperature=temperature
    )
    
    return DeepSeekClient(llm_config)


def create_vision_client(config_path: str = "config.yaml") -> "LLMClient":
    """创建 Vision 客户端 (ERNIE)"""
    from .vision import ERNIEVisionClient
    
    config = load_config(config_path)
    vision_config = config.get("vision_ai", {})
    
    bearer_token = vision_config.get("bearer_token", "")
    base_url = vision_config.get("base_url", "https://qianfan.baidubce.com/v2/chat/completions")
    model = vision_config.get("vision_model", "ernie-4.5-turbo-vl-32k")
    temperature = vision_config.get("temperature", 0.3)
    
    llm_config = LLMConfig(
        api_key=bearer_token,
        base_url=base_url,
        model=model,
        temperature=temperature
    )
    
    return ERNIEVisionClient(llm_config)
