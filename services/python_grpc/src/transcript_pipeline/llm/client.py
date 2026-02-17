"""LLM 客户端抽象、配置加载与工厂。"""

from __future__ import annotations

import asyncio
import hashlib
import os
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from services.python_grpc.src.config_paths import load_yaml_dict, resolve_video_config_path


@dataclass
class LLMResponse:
    content: str
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    model: str
    latency_ms: float
    raw_response: Optional[Dict[str, Any]] = None


@dataclass
class LLMConfig:
    api_key: str
    base_url: str
    model: str
    temperature: float = 0.1
    max_tokens: int = 4096
    timeout: float = 180.0


class LLMClient(ABC):
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
        **kwargs,
    ) -> LLMResponse:
        raise NotImplementedError

    @abstractmethod
    async def complete_json(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        **kwargs,
    ) -> Tuple[Dict[str, Any], LLMResponse]:
        raise NotImplementedError

    async def complete_with_retry(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        max_retries: int = 3,
        **kwargs,
    ) -> LLMResponse:
        last_error: Optional[Exception] = None
        for attempt in range(max(1, int(max_retries))):
            try:
                return await self.complete(prompt, system_prompt, **kwargs)
            except Exception as error:
                last_error = error
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
        if last_error is not None:
            raise last_error
        raise RuntimeError("LLM call failed without explicit exception")


def load_config(config_path: str = "config.yaml") -> Dict[str, Any]:
    resolved = resolve_video_config_path(config_path, anchor_file=__file__)
    if resolved is None:
        raise FileNotFoundError(f"Config file not found for input: {config_path}")
    return load_yaml_dict(resolved)


_DEEPSEEK_CLIENT_CACHE: Dict[str, "LLMClient"] = {}
_DEEPSEEK_CLIENT_CACHE_LOCK = threading.Lock()


def _build_deepseek_client_cache_key(
    *,
    loop_id: str,
    api_key: str,
    base_url: str,
    model: str,
    temperature: float,
    max_tokens: int,
    timeout: float,
) -> str:
    key_raw = "|".join(
        [
            loop_id,
            hashlib.sha256((api_key or "").encode("utf-8")).hexdigest(),
            str(base_url or ""),
            str(model or ""),
            f"{float(temperature):.4f}",
            str(int(max_tokens)),
            f"{float(timeout):.2f}",
        ]
    )
    return hashlib.sha256(key_raw.encode("utf-8")).hexdigest()


def create_llm_client(
    config_path: str = "config.yaml",
    purpose: str = "analysis",  # refinement, analysis, topic
) -> "LLMClient":
    from .deepseek import DeepSeekClient

    config = load_config(config_path)
    ai_config = config.get("ai", {})

    api_key = os.environ.get("DEEPSEEK_API_KEY") or ai_config.get("api_key", "")
    if not api_key:
        raise ValueError("DEEPSEEK_API_KEY not set")

    base_url = ai_config.get("base_url", "https://api.deepseek.com")
    purpose_config = ai_config.get(purpose, ai_config.get("analysis", {}))
    model = purpose_config.get("model", "deepseek-chat")
    temperature = float(purpose_config.get("temperature", 0.1))
    max_tokens = int(purpose_config.get("max_tokens", 4096))
    timeout = float(purpose_config.get("timeout", ai_config.get("timeout", 180.0)))

    try:
        loop_id = str(id(asyncio.get_running_loop()))
    except RuntimeError:
        loop_id = "no_loop"

    cache_key = _build_deepseek_client_cache_key(
        loop_id=loop_id,
        api_key=api_key,
        base_url=base_url,
        model=model,
        temperature=temperature,
        max_tokens=max_tokens,
        timeout=timeout,
    )
    with _DEEPSEEK_CLIENT_CACHE_LOCK:
        cached_client = _DEEPSEEK_CLIENT_CACHE.get(cache_key)
        if cached_client is not None:
            return cached_client

        llm_config = LLMConfig(
            api_key=api_key,
            base_url=base_url,
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
            timeout=timeout,
        )
        client = DeepSeekClient(llm_config)
        _DEEPSEEK_CLIENT_CACHE[cache_key] = client
        return client


def create_vision_client(config_path: str = "config.yaml") -> "LLMClient":
    from .vision import ERNIEVisionClient

    config = load_config(config_path)
    vision_config = config.get("vision_ai", {})

    bearer_token = vision_config.get("bearer_token", "")
    base_url = vision_config.get("base_url", "https://qianfan.baidubce.com/v2/chat/completions")
    model = vision_config.get("vision_model", "ernie-4.5-turbo-vl-32k")
    temperature = vision_config.get("temperature", 0.3)
    max_tokens = int(vision_config.get("max_tokens", 4096))
    timeout = float(vision_config.get("timeout", 180.0))

    llm_config = LLMConfig(
        api_key=bearer_token,
        base_url=base_url,
        model=model,
        temperature=temperature,
        max_tokens=max_tokens,
        timeout=timeout,
    )
    return ERNIEVisionClient(llm_config)
