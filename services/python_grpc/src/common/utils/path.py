"""
文件名与路径清洗工具。

职责：统一文件名清洗规则，避免跨平台字符导致写盘失败。
"""

from typing import Optional
import re


_ILLEGAL_FILENAME_CHARS_RE = re.compile(r'[<>:"/\\\\|?*]+')
_WHITESPACE_RE = re.compile(r"\s+")


def safe_filename(name: Optional[str], max_length: int = 100) -> str:
    """
    做什么：将任意文本清洗为文件系统安全的文件名。
    为什么：避免非法字符/过长路径导致写盘失败。
    权衡：会丢失部分原始可读性，但提升跨平台稳定性。
    """
    import re as _re
    safe = _re.sub(r'[<>:"/\\|?*]', "", str(name or ""))
    safe = safe.replace(" ", "_")
    if len(safe) > max_length:
        safe = safe[:max_length]
    return safe


def sanitize_filename_component(text: Optional[str], max_len: int = 40) -> str:
    """
    做什么：清洗为 Windows 友好的文件名片段。
    为什么：用于拼接文件名时，减少非法字符与过长风险。
    权衡：清洗会损失部分原始字符的可读性。
    """
    if not text:
        return ""
    cleaned = _ILLEGAL_FILENAME_CHARS_RE.sub("_", str(text))
    cleaned = _WHITESPACE_RE.sub(" ", cleaned).strip()
    cleaned = cleaned.strip("._- ")
    if len(cleaned) > max_len:
        cleaned = cleaned[:max_len].rstrip("._- ")
    return cleaned
