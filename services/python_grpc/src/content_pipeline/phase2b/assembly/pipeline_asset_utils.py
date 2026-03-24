"""
模块说明：RichTextPipeline 资源命名与路径工具。

执行逻辑：
1) 统一素材命名（slug、前缀、请求ID）。
2) 统一资产输出路径解析与目录创建。

核心价值：将“命名策略”与“管道编排”解耦，降低主类复杂度。
"""

from pathlib import Path
from typing import Any, Dict, List


def slugify_text(value: str, max_len: int = 48) -> str:
    """将任意文本转为安全短 slug。"""
    raw = str(value or "").strip().lower()
    if not raw:
        return "item"

    normalized: List[str] = []
    for char in raw:
        if char.isalnum():
            normalized.append(char)
        else:
            normalized.append("_")

    slug = "".join(normalized)
    while "__" in slug:
        slug = slug.replace("__", "_")
    slug = slug.strip("_") or "item"

    if len(slug) > max_len:
        slug = slug[:max_len].rstrip("_")
    return slug or "item"


def build_unit_asset_prefix(unit: Any) -> str:
    """生成语义单元的资产名前缀：`{unit_id}_{title_slug}`。"""
    unit_title = str(
        getattr(unit, "knowledge_topic", "")
        or getattr(unit, "title", "")
        or getattr(unit, "full_text", "")
    ).strip()
    title_slug = slugify_text(unit_title, max_len=40)
    return f"{unit.unit_id}_{title_slug}"


def build_action_brief(action: Dict[str, Any], classification: Dict[str, Any], index: int) -> str:
    """从分类与动作信息中提取简短 action 标识。"""
    candidates = [
        classification.get("description", "") if isinstance(classification, dict) else "",
        classification.get("subject", "") if isinstance(classification, dict) else "",
        action.get("description", "") if isinstance(action, dict) else "",
        action.get("type", "") if isinstance(action, dict) else "",
    ]
    for item in candidates:
        slug = slugify_text(str(item or ""), max_len=36)
        if slug and slug != "item":
            return slug
    return f"action_{index:02d}"


def build_request_base_name(unit: Any, suffix: str) -> str:
    """生成请求基础名：`{unit_prefix}_{suffix_slug}`。"""
    return f"{build_unit_asset_prefix(unit)}_{slugify_text(suffix, max_len=48)}"


def build_unit_relative_request_id(unit: Any, suffix: str) -> str:
    """生成带 `unit_id/` 前缀的相对请求ID。"""
    return f"{unit.unit_id}/{build_request_base_name(unit, suffix)}"


def resolve_asset_output_path(assets_dir: str, name: str, ext: str) -> str:
    """解析资产输出绝对路径，并保证目录存在。"""
    clean_name = str(name or "").strip().replace("\\", "/").strip("/")
    if clean_name.lower().endswith(f".{ext.lower()}"):
        clean_name = clean_name[: -(len(ext) + 1)]

    abs_path = Path(assets_dir) / f"{clean_name}.{ext}"
    abs_path.parent.mkdir(parents=True, exist_ok=True)
    return str(abs_path)

