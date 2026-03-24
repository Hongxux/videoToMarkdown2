"""
统一的哈希策略模块。

职责分层：
1) `fast_*`：本地缓存、去重、目录内短指纹，优先性能。
2) `sha256_*`：对外协议、发布清单、跨边界完整性校验。
3) `md5_*_compat`：历史兼容链路，避免破坏既有目录命名与文件复用契约。
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Iterable

try:
    import xxhash  # type: ignore
except Exception:  # pragma: no cover
    xxhash = None

_FAST_DIGEST_SIZE_BYTES = 16
_MD5_HEX_LENGTH = 32
_SHA256_HEX_LENGTH = 64


def stable_json_dumps(payload: Any) -> str:
    """生成稳定 JSON 文本，避免字段顺序导致指纹漂移。"""
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _normalize_text(value: Any) -> bytes:
    return str(value or "").encode("utf-8")


def _blake2s(digest_size_bytes: int = _FAST_DIGEST_SIZE_BYTES):
    return hashlib.blake2s(digest_size=max(1, int(digest_size_bytes)))


def fast_hash_name() -> str:
    """返回当前快哈希实现名，便于文档和日志保持一致。"""
    return "xxh3_128" if xxhash is not None else "blake2s_fallback"


def fast_hasher(*, digest_size_bytes: int = _FAST_DIGEST_SIZE_BYTES):
    """
    为增量写入场景提供快速 hasher。

    优先使用 XXH3-128，它正是本地去重和本地损坏探测更关心的“吞吐优先”实现；
    若运行环境缺少 `xxhash`，才退回到标准库的 blake2s。
    """
    if xxhash is not None:
        return xxhash.xxh3_128()
    return _blake2s(digest_size_bytes=digest_size_bytes)


def fast_digest_bytes(payload: bytes, *, digest_size_bytes: int = _FAST_DIGEST_SIZE_BYTES) -> str:
    hasher = fast_hasher(digest_size_bytes=digest_size_bytes)
    hasher.update(payload or b"")
    return hasher.hexdigest()


def fast_digest_text(value: Any, *, digest_size_bytes: int = _FAST_DIGEST_SIZE_BYTES) -> str:
    return fast_digest_bytes(_normalize_text(value), digest_size_bytes=digest_size_bytes)


def fast_digest_json(payload: Any, *, digest_size_bytes: int = _FAST_DIGEST_SIZE_BYTES) -> str:
    return fast_digest_text(stable_json_dumps(payload), digest_size_bytes=digest_size_bytes)


def fast_digest_iter(chunks: Iterable[bytes], *, digest_size_bytes: int = _FAST_DIGEST_SIZE_BYTES) -> str:
    hasher = _blake2s(digest_size_bytes=digest_size_bytes)
    for chunk in chunks:
        if chunk:
            hasher.update(chunk)
    return hasher.hexdigest()


def fast_digest_short_text(value: Any, *, hex_chars: int = 12) -> str:
    return fast_digest_text(value)[: max(1, int(hex_chars))]


def fast_digest_short_bytes(payload: bytes, *, hex_chars: int = 12) -> str:
    return fast_digest_bytes(payload)[: max(1, int(hex_chars))]


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload or b"").hexdigest()


def sha256_text(value: Any) -> str:
    return sha256_bytes(_normalize_text(value))


def sha256_json(payload: Any) -> str:
    return sha256_text(stable_json_dumps(payload))


def sha256_iter(chunks: Iterable[bytes]) -> str:
    hasher = hashlib.sha256()
    for chunk in chunks:
        if chunk:
            hasher.update(chunk)
    return hasher.hexdigest()


def md5_bytes_compat(payload: bytes) -> str:
    return hashlib.md5(payload or b"").hexdigest()


def md5_text_compat(value: Any) -> str:
    return md5_bytes_compat(_normalize_text(value))


def md5_short_text_compat(value: Any, *, hex_chars: int = 12) -> str:
    return md5_text_compat(value)[: max(1, int(hex_chars))]


def is_sha256_hex(value: Any) -> bool:
    normalized = str(value or "").strip().lower()
    return len(normalized) == _SHA256_HEX_LENGTH and all(ch in "0123456789abcdef" for ch in normalized)


def is_md5_hex(value: Any) -> bool:
    normalized = str(value or "").strip().lower()
    return len(normalized) == _MD5_HEX_LENGTH and all(ch in "0123456789abcdef" for ch in normalized)
