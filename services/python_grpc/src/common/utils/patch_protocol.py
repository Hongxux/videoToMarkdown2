"""
补丁协议公共工具。

职责：
1) 统一 replace/add 补丁项的规范化。
2) 统一补丁 payload 的操作项收集与兼容键处理。
3) 统一文本回退字段的读取顺序。
4) 统一 Step4 removal 补丁项的规范化。
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


def normalize_replace_add_patch_item(item: Any) -> Optional[Dict[str, str]]:
    """标准化 replace/add 补丁项，兼容短键与历史别名。"""
    if not isinstance(item, dict):
        return None
    mode = str(item.get("m", item.get("mode", ""))).strip().lower()
    if not mode:
        if "o" in item or "original" in item:
            mode = "r"
        elif "a" in item or "add" in item or "addition" in item:
            mode = "a"
    if mode in ("replace", "r"):
        original_raw = item.get("o", item.get("original", item.get("from", "")))
        replacement_raw = item.get(
            "n",
            item.get("new", item.get("replacement", item.get("to", ""))),
        )
        original = str(original_raw if original_raw is not None else "")
        replacement = str(replacement_raw if replacement_raw is not None else "")
        if original == "" or replacement == "" or original == replacement:
            return None
        return {
            "mode": "r",
            "o": original,
            "n": replacement,
            "l": str(item.get("l", item.get("left_context", item.get("context_before", ""))) or ""),
            "r": str(item.get("r", item.get("right_context", item.get("context_after", ""))) or ""),
        }
    if mode in ("add", "a"):
        add_raw = item.get(
            "n",
            item.get("a", item.get("add", item.get("addition", item.get("text", "")))),
        )
        add_text = str(add_raw if add_raw is not None else "")
        if add_text == "":
            return None
        position = str(item.get("p", item.get("position", "after"))).strip().lower()
        if position not in ("before", "after", "b", "a"):
            position = "after"
        return {
            "mode": "a",
            "n": add_text,
            "l": str(item.get("l", item.get("left_context", item.get("context_before", ""))) or ""),
            "r": str(item.get("r", item.get("right_context", item.get("context_after", ""))) or ""),
            "p": "before" if position in ("before", "b") else "after",
        }
    return None


def collect_patch_ops(payload: Dict[str, Any]) -> List[Any]:
    """从 payload 中收集补丁操作项，兼容 p/patches/ops/r/a 多形态。"""
    raw_ops: List[Any] = []
    if not isinstance(payload, dict):
        return raw_ops

    if isinstance(payload.get("p"), list):
        raw_ops.extend(payload.get("p", []))
    if isinstance(payload.get("patches"), list):
        raw_ops.extend(payload.get("patches", []))
    if isinstance(payload.get("ops"), list):
        raw_ops.extend(payload.get("ops", []))

    # 兼容分组写法：r=replace[]、a=add[]
    if isinstance(payload.get("r"), list):
        for item in payload.get("r", []):
            if isinstance(item, dict) and "m" not in item and "mode" not in item:
                item = {**item, "m": "r"}
            raw_ops.append(item)
    if isinstance(payload.get("a"), list):
        for item in payload.get("a", []):
            if isinstance(item, dict) and "m" not in item and "mode" not in item:
                item = {**item, "m": "a"}
            raw_ops.append(item)
    return raw_ops


def pick_full_text_fallback(payload: Dict[str, Any]) -> str:
    """按约定优先级提取整段文本回退值。"""
    if not isinstance(payload, dict):
        return ""
    full_text_candidates = (
        payload.get("text"),
        payload.get("full_text"),
        payload.get("enhanced_body"),
        payload.get("body"),
    )
    for candidate in full_text_candidates:
        candidate_text = str(candidate or "").strip()
        if candidate_text:
            return candidate_text
    return ""


def normalize_removal_patch_item(item: Any) -> Optional[Dict[str, str]]:
    """标准化 Step4 removal 补丁项，兼容短键与历史别名。"""
    if not isinstance(item, dict):
        return None
    original_raw = item.get("original", item.get("o", ""))
    original = str(original_raw if original_raw is not None else "")
    if original == "":
        return None

    left_raw = item.get("left_context", item.get("context_before", item.get("l", "")))
    right_raw = item.get("right_context", item.get("context_after", item.get("r", "")))
    return {
        "original": original,
        "left_context": str(left_raw if left_raw is not None else ""),
        "right_context": str(right_raw if right_raw is not None else ""),
        "sentence_id": str(item.get("sentence_id", item.get("sid", ""))).strip(),
    }
