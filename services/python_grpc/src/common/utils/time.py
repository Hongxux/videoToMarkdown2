"""
时间格式工具。

职责：统一时间戳字符串格式，避免重复实现导致格式不一致。
"""


def format_hhmmss(seconds: float) -> str:
    """
    做什么：将秒数格式化为 HH:MM:SS。
    为什么：日志/字幕等输出依赖统一格式，便于对齐与解析。
    权衡：仅保留整数秒，微秒级精度会被截断。
    """
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"
