"""兼容模块：转发到 `import_path_setup`。

职责边界：
- 保留历史导入路径 `services.python_grpc.src.server.path_bootstrap`。
- 不再承载真实实现。

主要功能：
- 转发 `prepare_runtime_paths` 与 `setup_import_paths`。
"""

from .import_path_setup import prepare_runtime_paths, setup_import_paths

__all__ = ["prepare_runtime_paths", "setup_import_paths"]

