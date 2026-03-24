"""gRPC 服务导入路径引导执行层。

职责边界：
- 只负责启动阶段 `sys.path` 的路径注入顺序。
- 不承载业务逻辑，不做依赖检查。

主要功能：
- 计算仓库根目录。
- 注入仓库根目录与 `contracts/gen/python` 生成代码目录。
- 注入入口脚本所在目录，兼容脚本方式启动。
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Callable


def setup_import_paths(entry_file: str, prepend_func: Callable[[str], None]) -> str:
    """准备运行时导入路径并返回仓库根目录。"""
    entry_path = Path(entry_file).resolve()
    entry_dir = entry_path.parent
    repo_root = entry_path.parents[4]
    contracts_gen_python = repo_root / "contracts" / "gen" / "python"

    prepend_func(os.fspath(repo_root))
    prepend_func(os.fspath(contracts_gen_python))
    prepend_func(os.fspath(entry_dir))
    return os.fspath(repo_root)


def prepare_runtime_paths(entry_file: str, prepend_func: Callable[[str], None]) -> str:
    """兼容别名：保留历史方法名。"""
    return setup_import_paths(entry_file, prepend_func)


__all__ = ["setup_import_paths", "prepare_runtime_paths"]

