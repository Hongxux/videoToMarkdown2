"""
数值转换工具。

职责：提供稳定的数值类型转换，统一兜底策略，避免脏数据引发异常。
"""

from typing import Any, Optional


def safe_int(value: Any, default: int = 0) -> int:
    """
    做什么：将 value 安全转换为 int。
    为什么：配置/外部数据常为字符串或空值，统一兜底可降低异常率。
    权衡：发生异常时返回 default，而非抛错，以保证流程稳定性。
    """
    try:
        return int(value)
    except Exception:
        return int(default)


def safe_float(value: Any, default: float = 0.0) -> float:
    """
    做什么：将 value 安全转换为 float。
    为什么：输入类型不稳定时需要一致的兜底策略。
    权衡：发生异常时返回 default，保持业务逻辑连续性。
    """
    try:
        return float(value)
    except Exception:
        return float(default)


def to_float(value: Any) -> Optional[float]:
    """
    做什么：将 value 转换为 float，失败时返回 None。
    为什么：部分场景需要区分“无值”和“默认值”，避免误用默认数值。
    权衡：返回 None 需要调用方显式处理。
    """
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None
