"""
模块说明：Module2 Prompt 加载器。
执行逻辑：
1) 按 Prompt Key 从配置覆盖路径或默认目录读取模板。
2) 在读取失败时回退到内置默认目录，保障运行时兼容性。
3) 提供统一的模板渲染接口，集中处理 format 变量缺失异常。
实现方式：注册表 + 本地缓存 + 配置驱动路径解析。
核心价值：将 Prompt 存储位置与业务调用解耦，降低改文案的代码改动成本。
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

from services.python_grpc.src.content_pipeline.infra.runtime.config_loader import load_module2_config
from services.python_grpc.src.content_pipeline.infra.llm.prompt_registry import get_prompt_entry


logger = logging.getLogger(__name__)

_THIS_FILE = Path(__file__).resolve()
_CONTENT_PIPELINE_ROOT = _THIS_FILE.parents[2]
_PACKAGE_PROMPT_ROOT = _CONTENT_PIPELINE_ROOT / "prompts"


def _detect_repo_root() -> Path:
    """方法说明：_detect_repo_root 工具方法。
    执行步骤：
    1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
    2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
    3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
    for candidate in _THIS_FILE.parents:
        if (candidate / "services" / "python_grpc" / "src").exists() and (candidate / "config").exists():
            return candidate
    return _THIS_FILE.parents[-1]


_REPO_ROOT = _detect_repo_root()

_PROMPT_CONTENT_CACHE: Dict[str, str] = {}
_PROMPT_PATH_CACHE: Dict[str, Path] = {}
_PROMPT_CONFIG_CACHE: Optional[Dict[str, Any]] = None


def _safe_bool(value: Any, default: bool = False) -> bool:
    """方法说明：_safe_bool 工具方法。
    执行步骤：
    1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
    2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
    3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        raw = value.strip().lower()
        if raw in {"1", "true", "yes", "y", "on"}:
            return True
        if raw in {"0", "false", "no", "n", "off"}:
            return False
    return bool(default)


def _resolve_file_path(raw_path: str, root_dir: Optional[Path] = None) -> Optional[Path]:
    """方法说明：_resolve_file_path 工具方法。
    执行步骤：
    1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
    2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
    3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
    if not raw_path:
        return None

    candidate = Path(str(raw_path).strip()).expanduser()
    if candidate.is_absolute():
        return candidate

    search_bases = []
    if root_dir is not None:
        search_bases.append(root_dir)
    search_bases.extend([Path.cwd(), _REPO_ROOT])

    for base in search_bases:
        resolved = (base / candidate).resolve()
        if resolved.exists() and resolved.is_file():
            return resolved

    return None


def _load_prompt_config() -> Dict[str, Any]:
    """方法说明：_load_prompt_config 工具方法。
    执行步骤：
    1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
    2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
    3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
    global _PROMPT_CONFIG_CACHE

    if _PROMPT_CONFIG_CACHE is not None:
        return _PROMPT_CONFIG_CACHE

    try:
        config = load_module2_config()
    except Exception as exc:
        logger.warning(f"Load module2 config for prompt management failed: {exc}")
        config = {}

    section = config.get("prompt_management", {}) if isinstance(config, dict) else {}
    _PROMPT_CONFIG_CACHE = section if isinstance(section, dict) else {}
    return _PROMPT_CONFIG_CACHE


def _resolve_prompt_root(config: Dict[str, Any]) -> Path:
    """方法说明：_resolve_prompt_root 工具方法。
    执行步骤：
    1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
    2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
    3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
    root_dir_raw = str(config.get("root_dir", "") or "").strip()
    if not root_dir_raw:
        return _PACKAGE_PROMPT_ROOT

    root_candidate = Path(root_dir_raw).expanduser()
    if root_candidate.is_absolute():
        return root_candidate

    for base in (Path.cwd(), _REPO_ROOT):
        resolved = (base / root_candidate).resolve()
        if resolved.exists() and resolved.is_dir():
            return resolved

    return (Path.cwd() / root_candidate).resolve()


def _read_text_cached(path: Path) -> str:
    """方法说明：_read_text_cached 工具方法。
    执行步骤：
    1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
    2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
    3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
    key = str(path.resolve())
    if key in _PROMPT_CONTENT_CACHE:
        return _PROMPT_CONTENT_CACHE[key]

    content = path.read_text(encoding="utf-8")
    _PROMPT_CONTENT_CACHE[key] = content
    return content


def _load_prompt_text(key: str, *, strict: bool, fallback: Optional[str] = None) -> str:
    """方法说明：_load_prompt_text 工具方法。
    执行步骤：
    1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
    2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
    3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
    entry = get_prompt_entry(key)
    config = _load_prompt_config()

    enabled = _safe_bool(config.get("enabled", True), default=True)
    if not enabled:
        return fallback if fallback is not None else _read_text_cached(_PACKAGE_PROMPT_ROOT / entry.relative_path)

    root_dir = _resolve_prompt_root(config)
    overrides = config.get("overrides", {}) if isinstance(config.get("overrides", {}), dict) else {}

    selected_path: Optional[Path] = None
    override_value = overrides.get(key)
    if override_value:
        selected_path = _resolve_file_path(str(override_value), root_dir=root_dir)

    if selected_path is None:
        selected_path = (root_dir / entry.relative_path).resolve()

    if selected_path.exists() and selected_path.is_file():
        _PROMPT_PATH_CACHE[key] = selected_path
        return _read_text_cached(selected_path)

    package_default = (_PACKAGE_PROMPT_ROOT / entry.relative_path).resolve()
    if package_default.exists() and package_default.is_file():
        _PROMPT_PATH_CACHE[key] = package_default
        logger.warning(
            f"Prompt file missing for key={key}, fallback to package prompt: {package_default}"
        )
        return _read_text_cached(package_default)

    if fallback is not None:
        logger.warning(f"Prompt file missing for key={key}, fallback to in-code string")
        return fallback

    if strict:
        raise FileNotFoundError(f"Prompt not found for key={key}, path={selected_path}")

    logger.error(f"Prompt not found for key={key}, returning empty string")
    return ""


def clear_prompt_loader_cache() -> None:
    """清空 prompt 加载缓存，主要用于测试。"""

    global _PROMPT_CONFIG_CACHE
    _PROMPT_CONFIG_CACHE = None
    _PROMPT_CONTENT_CACHE.clear()
    _PROMPT_PATH_CACHE.clear()


def get_prompt(
    key: str,
    *,
    strict: Optional[bool] = None,
    fallback: Optional[str] = None,
) -> str:
    """
    读取 prompt 模板原文。
    - strict 为空时使用 prompt_management.strict 配置（默认 False）。
    - fallback 在文件缺失时作为最终兜底。
    """

    config = _load_prompt_config()
    strict_mode = _safe_bool(config.get("strict", False), default=False) if strict is None else bool(strict)
    return _load_prompt_text(key, strict=strict_mode, fallback=fallback)


def render_prompt(
    key: str,
    context: Optional[Dict[str, Any]] = None,
    *,
    strict: Optional[bool] = None,
    fallback: Optional[str] = None,
) -> str:
    """读取并渲染 prompt。"""

    template = get_prompt(key, strict=strict, fallback=fallback)
    if not context:
        return template

    try:
        return template.format(**context)
    except KeyError as exc:
        missing = exc.args[0] if exc.args else "unknown"
        raise KeyError(f"Render prompt failed for key={key}, missing variable={missing}") from exc


def get_prompt_path(key: str) -> Optional[Path]:
    """返回最近一次加载该 key 使用的文件路径，便于排查。"""

    return _PROMPT_PATH_CACHE.get(key)

