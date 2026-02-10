"""ID 相关通用工具。"""

from __future__ import annotations


def build_unit_relative_asset_id(unit_id: str, file_stem: str, *, default_stem: str = "asset_001") -> str:
    """构建 `unit_id/file_stem` 形式的相对资源 ID（不含扩展名）。"""
    clean_unit_id = str(unit_id or "").strip() or "SU000"
    clean_stem = str(file_stem or "").strip().replace("\\", "/").strip("/")
    if clean_stem:
        clean_stem = clean_stem.split("/")[-1]
    else:
        clean_stem = default_stem
    return f"{clean_unit_id}/{clean_stem}"

