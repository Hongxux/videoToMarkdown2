"""Whisper 语言参数归一化工具。"""

from typing import Optional

_AUTO_LANGUAGE_SENTINELS = {
    "",
    "auto",
    "detect",
    "autodetect",
    "auto-detect",
    "none",
    "null",
}

_LANGUAGE_ALIASES = {
    "zh-cn": "zh",
    "zh_cn": "zh",
    "zh-hans": "zh",
    "zh_hans": "zh",
    "zh-hant": "zh",
    "zh_hant": "zh",
    "cmn": "zh",
    "chinese": "zh",
    "en-us": "en",
    "en_us": "en",
    "english": "en",
}


def normalize_whisper_language(language: Optional[str]) -> Optional[str]:
    """
    将上层语言参数归一化为 faster-whisper 可接受的值。
    返回 None 表示自动检测语言。
    """
    if language is None:
        return None

    normalized = str(language).strip().lower()
    if normalized in _AUTO_LANGUAGE_SENTINELS:
        return None

    return _LANGUAGE_ALIASES.get(normalized, normalized)


def language_for_fingerprint(language: Optional[str]) -> str:
    """将语言值归一化为可稳定落盘的指纹文本。"""
    normalized = normalize_whisper_language(language)
    return normalized or "auto"
