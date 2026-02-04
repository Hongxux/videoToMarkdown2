"""
模块说明：LLM 客户端基类、配置结构与客户端工厂的集中封装。
执行逻辑：
1) 定义 LLMConfig/LLMResponse 与 LLMClient 抽象接口，统一调用契约。
2) 读取配置文件与环境变量，构建文本/视觉客户端实例。
3) 提供带重试的统一调用入口，降低上层调用复杂度。
实现方式：使用 dataclass 组织结构，YAML 解析配置，环境变量覆盖敏感字段。
核心价值：统一配置入口与客户端创建逻辑，方便替换模型与扩展供应商。
输入：
- config.yaml 路径与环境变量（例如 DEEPSEEK_API_KEY）。
输出：
- LLMClient 子类实例与 LLMResponse 结构化结果。
补充说明：
- 文本模型默认走 DeepSeek，视觉模型默认走 ERNIE Vision。"""

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
    """
    类说明：封装一次 LLM 调用的结构化响应数据。
    执行逻辑：
    1) 存储模型输出文本与 token 统计。
    2) 记录模型名称与响应延迟，便于监控与追踪。
    实现方式：使用 dataclass 保存字段值，不引入额外逻辑。
    核心价值：统一返回格式，便于下游统计与排查问题。
    输入：
    - 构造参数：content、token 统计、model、latency_ms、raw_response。
    输出：
    - 响应对象字段（文本内容、token 数、模型名、延迟、原始响应）。"""
    content: str
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    model: str
    latency_ms: float
    raw_response: Optional[Dict] = None
    
    
@dataclass
class LLMConfig:
    """
    类说明：封装 LLM 客户端初始化所需的连接与模型配置。
    执行逻辑：
    1) 保存 API Key、Base URL、模型名等核心字段。
    2) 统一温度、最大 tokens、超时等推理参数。
    实现方式：使用 dataclass 直接存储配置字段。
    核心价值：集中管理配置，避免在调用侧重复拼装参数。
    输入：
    - 构造参数：api_key、base_url、model、temperature、max_tokens、timeout。
    输出：
    - 配置对象字段，供 LLMClient 与其子类使用。"""
    api_key: str
    base_url: str
    model: str
    temperature: float = 0.1
    max_tokens: int = 4096
    timeout: float = 180.0  # 增加到 180 秒以应对大批次处理


class LLMClient(ABC):
    """
    类说明：LLM 客户端抽象基类，定义统一调用接口与状态缓存。
    执行逻辑：
    1) 保存配置与最近一次调用信息（prompt/response/token）。
    2) 定义抽象调用方法 complete/complete_json。
    3) 提供带重试的统一封装 complete_with_retry。
    实现方式：抽象方法 + 内部状态字段。
    核心价值：统一不同模型的调用契约，降低替换成本。"""
    
    def __init__(self, config: LLMConfig):
        """
        执行逻辑：
        1) 保存 LLMConfig 作为后续调用的基础配置。
        2) 重置最近一次请求与响应的缓存字段。
        实现方式：直接赋值成员变量。
        核心价值：保证客户端初始状态一致，避免旧状态污染。
        输入参数：
        - config: LLMConfig 配置对象。
        输出参数：
        - 无（仅更新对象内部状态）。"""
        self.config = config
        self._last_prompt = ""
        self._last_response = ""
        self._last_token_count = 0
        
    @property
    def last_prompt(self) -> str:
        """
        执行逻辑：
        1) 读取对象内部缓存的最近一次 prompt。
        2) 返回对应字符串。
        实现方式：直接返回成员变量。
        核心价值：提供统一的调试入口。
        输入参数：
        - 无。
        输出参数：
        - 最近一次请求的 prompt 文本。"""
        return self._last_prompt
    
    @property
    def last_response(self) -> str:
        """
        执行逻辑：
        1) 读取对象内部缓存的最近一次 response。
        2) 返回对应字符串。
        实现方式：直接返回成员变量。
        核心价值：提供统一的调试入口。
        输入参数：
        - 无。
        输出参数：
        - 最近一次返回的文本内容。"""
        return self._last_response
    
    @property
    def last_token_count(self) -> int:
        """
        执行逻辑：
        1) 读取对象内部缓存的 token 统计。
        2) 返回对应数值。
        实现方式：直接返回成员变量。
        核心价值：便于监控与计费统计。
        输入参数：
        - 无。
        输出参数：
        - 最近一次调用的 token 数量。"""
        return self._last_token_count
        
    @abstractmethod
    async def complete(
        self, 
        prompt: str,
        system_prompt: Optional[str] = None,
        temperature: Optional[float] = None,
        **kwargs
    ) -> LLMResponse:
        """
        执行逻辑：
        1) 根据 prompt/system_prompt 调用模型接口。
        2) 解析响应并返回 LLMResponse。
        实现方式：由子类实现具体 HTTP/SDK 调用。
        核心价值：统一文本完成接口与返回格式。
        输入参数：
        - prompt: 文本内容（类型：str）。
        - system_prompt: 系统指令（类型：Optional[str]）。
        - temperature: 温度覆盖（类型：Optional[float]）。
        - **kwargs: 可变参数，含义由调用方决定。
        输出参数：
        - LLMResponse（包含内容、token 统计、模型名、延迟与原始响应）。"""
        pass
    
    @abstractmethod
    async def complete_json(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        **kwargs
    ) -> Tuple[Dict, LLMResponse]:
        """
        执行逻辑：
        1) 调用模型并获取原始文本响应。
        2) 解析 JSON 结果并返回结构化数据。
        实现方式：由子类实现具体解析与容错策略。
        核心价值：统一 JSON 输出的解析与返回格式。
        输入参数：
        - prompt: 文本内容（类型：str）。
        - system_prompt: 系统指令（类型：Optional[str]）。
        - **kwargs: 可变参数，含义由调用方决定。
        输出参数：
        - (dict, LLMResponse)：dict 为解析后的 JSON 数据，LLMResponse 为原始调用信息。"""
        pass
    
    async def complete_with_retry(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        max_retries: int = 3,
        **kwargs
    ) -> LLMResponse:
        """
        执行逻辑：
        1) 循环调用 complete，捕获异常并记录。
        2) 失败时按指数退避重试，最终抛出最后一次错误。
        实现方式：asyncio + 指数退避 (2 ** attempt)。
        核心价值：提升调用稳定性，减少瞬时失败带来的影响。
        决策逻辑：
        - 条件：attempt < max_retries - 1
        依据来源（证据链）：
        - 输入参数：max_retries。
        - 运行状态：complete 抛出的异常。
        输入参数：
        - prompt: 文本内容（类型：str）。
        - system_prompt: 系统指令（类型：Optional[str]）。
        - max_retries: 最大重试次数（类型：int）。
        - **kwargs: 可变参数，含义由调用方决定。
        输出参数：
        - LLMResponse（包含内容、token 统计、模型名、延迟与原始响应）。"""
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
    """
    执行逻辑：
    1) 校验配置文件是否存在。
    2) 读取 YAML 并解析为字典。
    实现方式：文件系统读取 + yaml.safe_load。
    核心价值：统一配置入口，便于多客户端复用。
    决策逻辑：
    - 条件：not path.exists()
    依据来源（证据链）：
    - 文件系统状态：config_path 对应文件是否存在。
    输入参数：
    - config_path: 配置文件路径（类型：str）。
    输出参数：
    - 配置字典（常见键：ai、vision_ai）。"""
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
    执行逻辑：
    1) 读取 ai 配置并合并环境变量中的 API Key。
    2) 按 purpose 选择模型与温度，构建 LLMConfig。
    3) 返回 DeepSeekClient 实例。
    实现方式：YAML 解析 + 环境变量覆盖。
    核心价值：统一文本 LLM 客户端创建流程。
    决策逻辑：
    - 条件：not api_key（环境变量与配置均缺失）
    - 条件：purpose 配置缺失则回退到 analysis
    依据来源（证据链）：
    - 环境变量：DEEPSEEK_API_KEY。
    - 配置字段：ai.api_key、ai.base_url、ai.{purpose}。
    输入参数：
    - config_path: 配置文件路径（类型：str）。
    - purpose: 模型用途（analysis/refinement/topic）。
    输出参数：
    - DeepSeekClient 实例（已注入 model/temperature/base_url）。"""
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
    """
    执行逻辑：
    1) 读取 vision_ai 配置。
    2) 构建 LLMConfig 并返回 ERNIEVisionClient。
    实现方式：YAML 解析。
    核心价值：统一视觉模型客户端创建流程。
    输入参数：
    - config_path: 配置文件路径（类型：str）。
    输出参数：
    - ERNIEVisionClient 实例（包含 bearer_token/base_url/model/temperature）。"""
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
