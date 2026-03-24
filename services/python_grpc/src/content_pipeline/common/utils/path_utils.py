"""路径相关通用工具。"""

from __future__ import annotations

from pathlib import Path


def find_repo_root(anchor_file: str) -> Path:
    """从锚点文件向上查找仓库根目录。"""
    current = Path(anchor_file).resolve().parent
    for candidate in [current, *current.parents]:
        if (candidate / ".git").exists():
            return candidate
        if (candidate / "services").exists() and (candidate / "config").exists():
            return candidate
    return current

