"""
统一配置路径解析工具。

设计目标：
1) 将“配置文件查找策略”收敛到一个模块，避免各处重复写路径回退逻辑。
2) 优先支持统一配置目录，同时兼容历史目录，保证迁移期间可平滑运行。
3) 对外提供“解析路径 + 读取 YAML”两类稳定接口。
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Dict, Any, Iterable, List, Optional

import yaml


logger = logging.getLogger(__name__)


def _append_unique(items: List[Path], candidate: Optional[Path]) -> None:
    """将路径去重追加到列表。"""
    if candidate is None:
        return
    if candidate not in items:
        items.append(candidate)


def _walk_up(start: Path) -> Iterable[Path]:
    """从起点向上遍历到根目录。"""
    current = start
    yield current
    for parent in current.parents:
        yield parent


def _collect_search_roots(anchor_file: Optional[str] = None) -> List[Path]:
    """
    收集配置查找根目录候选。

    为什么这样做：
    - 不同入口脚本的 `cwd` 可能不同；
    - 测试会通过 monkeypatch 改写 `__file__`；
    - 统一把“当前目录 + 锚点目录”都纳入搜索，可显著降低路径耦合。
    """
    roots: List[Path] = []

    cwd = Path.cwd().resolve()
    for path in _walk_up(cwd):
        _append_unique(roots, path)

    if anchor_file:
        try:
            anchor = Path(anchor_file).resolve()
            anchor_dir = anchor if anchor.is_dir() else anchor.parent
            for path in _walk_up(anchor_dir):
                _append_unique(roots, path)
        except Exception:
            pass

    return roots


def load_yaml_dict(path: Path) -> Dict[str, Any]:
    """读取 YAML 文件并保证返回字典；失败时返回空字典。"""
    if not path or not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as file:
            data = yaml.safe_load(file)
            return data if isinstance(data, dict) else {}
    except Exception as exc:
        logger.warning(f"Failed to load yaml {path}: {exc}")
        return {}


def _resolve_explicit_file(path_value: Optional[str], roots: List[Path]) -> Optional[Path]:
    """解析显式传入的文件路径（支持绝对路径与相对路径）。"""
    if not path_value:
        return None

    raw = str(path_value).strip()
    if not raw:
        return None

    candidate = Path(raw).expanduser()
    if candidate.is_absolute() and candidate.exists() and candidate.is_file():
        return candidate.resolve()

    # 先尝试相对当前工作目录
    cwd_candidate = (Path.cwd() / candidate).resolve()
    if cwd_candidate.exists() and cwd_candidate.is_file():
        return cwd_candidate

    # 再尝试相对各级根目录（用于跨入口执行）
    for root in roots:
        resolved = (root / candidate).resolve()
        if resolved.exists() and resolved.is_file():
            return resolved

    return None


def resolve_video_config_path(
    explicit_path: Optional[str] = None,
    *,
    anchor_file: Optional[str] = None,
) -> Optional[Path]:
    """
    解析主链路视频配置文件路径。

    搜索优先级：
    1) 显式传参 `explicit_path`
    2) 环境变量 `VIDEO_CONFIG_PATH` / `MODULE2_CONFIG_PATH`
    3) 统一目录 `config/video_config.yaml`
    """
    roots = _collect_search_roots(anchor_file)

    explicit_resolved = _resolve_explicit_file(explicit_path, roots)
    if explicit_resolved is not None:
        return explicit_resolved

    env_candidates = [
        _resolve_explicit_file(os.getenv("VIDEO_CONFIG_PATH", ""), roots),
        _resolve_explicit_file(os.getenv("MODULE2_CONFIG_PATH", ""), roots),
    ]
    for candidate in env_candidates:
        if candidate is not None:
            return candidate

    relative_candidates = ["config/video_config.yaml"]
    for root in roots:
        for relative in relative_candidates:
            candidate = (root / relative).resolve()
            if candidate.exists() and candidate.is_file():
                return candidate

    return None


def resolve_module2_config_file(
    filename: str = "module2_config.yaml",
    *,
    config_dir: Optional[str] = None,
    anchor_file: Optional[str] = None,
) -> Optional[Path]:
    """
    解析 Module2 配置文件路径。

    约束：
    - 统一从 `config/` 读取；
    - 仅在显式参数或环境变量指定时允许偏离默认路径。
    """
    roots = _collect_search_roots(anchor_file)

    if config_dir:
        explicit_dir = Path(config_dir).expanduser()
        explicit_dir = explicit_dir if explicit_dir.is_absolute() else (Path.cwd() / explicit_dir)
        candidate = (explicit_dir.resolve() / filename).resolve()
        if candidate.exists() and candidate.is_file():
            return candidate

    env_dir = (os.getenv("MODULE2_CONFIG_DIR", "") or "").strip()
    if env_dir:
        env_candidate = Path(env_dir).expanduser()
        env_candidate = env_candidate if env_candidate.is_absolute() else (Path.cwd() / env_candidate)
        candidate = (env_candidate.resolve() / filename).resolve()
        if candidate.exists() and candidate.is_file():
            return candidate

    env_file = (os.getenv("MODULE2_CONFIG_PATH", "") or "").strip()
    if env_file:
        file_candidate = _resolve_explicit_file(env_file, roots)
        if file_candidate is not None:
            if file_candidate.name == filename:
                return file_candidate
            parent_candidate = file_candidate.parent / filename
            if parent_candidate.exists() and parent_candidate.is_file():
                return parent_candidate

    relative_dirs = ["config"]
    for root in roots:
        for relative_dir in relative_dirs:
            candidate = (root / relative_dir / filename).resolve()
            if candidate.exists() and candidate.is_file():
                return candidate

    return None
