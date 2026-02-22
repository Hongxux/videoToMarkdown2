"""DeepSeek 模型别名路由工具。"""

from __future__ import annotations

from typing import Optional

_MODEL_ALIASES = {
    # Reasoner aliases
    "deepseek-r1": "deepseek-reasoner",
    "r1": "deepseek-reasoner",
    "v3 reasoner": "deepseek-reasoner",
    "v3_reasoner": "deepseek-reasoner",
    "v3.2 reasoner": "deepseek-reasoner",
    "v3.2_reasoner": "deepseek-reasoner",
    "deepseek-v3-reasoner": "deepseek-reasoner",
    "deepseek-v3.2-reasoner": "deepseek-reasoner",
    # Common misspells kept for compatibility
    "deepseek-resoner": "deepseek-reasoner",
    "v3-resoner": "deepseek-reasoner",
    "v3.2-resoner": "deepseek-reasoner",
    # Chat aliases (V3.2 non-thinking mode)
    "deepseek-v3": "deepseek-chat",
    "v3": "deepseek-chat",
    "deepseek-v3.2": "deepseek-chat",
    "v3.2": "deepseek-chat",
    "deepseek-v3.2-chat": "deepseek-chat",
}


def resolve_deepseek_model(configured_model: Optional[str], default_model: str = "deepseek-chat") -> str:
    """将配置模型名解析为 DeepSeek V3.2 官方模型标识。"""
    raw = str(configured_model or "").strip()
    if not raw:
        return str(default_model or "deepseek-chat").strip() or "deepseek-chat"
    return _MODEL_ALIASES.get(raw.lower(), raw)
