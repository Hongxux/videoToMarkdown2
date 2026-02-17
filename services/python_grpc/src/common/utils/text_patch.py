"""
文本补丁公共工具。

职责：
1) 提供跨模块复用的文本定位与替换能力。
2) 提供 JSON-ish 响应中的首个字典提取能力。
3) 保持纯函数形态，便于单测与稳定复用。
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional


def find_all_occurrences(text: str, needle: str) -> List[int]:
    """返回子串在文本中的全部起始位置（允许重叠）。"""
    if not needle:
        return []
    starts: List[int] = []
    offset = 0
    while True:
        position = text.find(needle, offset)
        if position < 0:
            break
        starts.append(position)
        offset = position + 1
    return starts


def find_contextual_match_positions(
    text: str,
    original: str,
    *,
    left_context: str = "",
    right_context: str = "",
) -> List[int]:
    """按左右上下文约束匹配 original 在 text 中的位置。"""
    candidate_positions = find_all_occurrences(text, original)
    if not candidate_positions:
        return []
    if not left_context and not right_context:
        return candidate_positions

    matches: List[int] = []
    original_len = len(original)
    left_len = len(left_context)
    right_len = len(right_context)

    for pos in candidate_positions:
        if left_len > 0:
            left_start = pos - left_len
            if left_start < 0:
                continue
            if text[left_start:pos] != left_context:
                continue
        if right_len > 0:
            right_start = pos + original_len
            right_end = right_start + right_len
            if text[right_start:right_end] != right_context:
                continue
        matches.append(pos)
    return matches


def replace_by_index(text: str, start: int, length: int, replacement: str) -> str:
    """按起始位置与长度执行文本替换。"""
    return f"{text[:start]}{replacement}{text[start + length:]}"


def find_add_insert_positions(
    text: str,
    *,
    left_context: str,
    right_context: str,
    position: str,
) -> List[int]:
    """
    计算增量 add 的插入位置。

    说明：当前行为与既有链路保持一致，`position` 参数保留兼容但不改变定位规则：
    - 左右上下文同时存在时，插入点固定为二者中间。
    - 仅左上下文时，插入点为左上下文末尾。
    - 仅右上下文时，插入点为右上下文起始。
    """
    _ = position
    if left_context and right_context:
        positions: List[int] = []
        left_positions = find_all_occurrences(text, left_context)
        for left_pos in left_positions:
            pivot = left_pos + len(left_context)
            if text.startswith(right_context, pivot):
                positions.append(pivot)
        return positions

    if left_context:
        return [pos + len(left_context) for pos in find_all_occurrences(text, left_context)]
    if right_context:
        return find_all_occurrences(text, right_context)
    return []


def extract_first_json_dict(payload_text: str) -> Optional[Dict[str, Any]]:
    """从文本中提取首个可解析的 JSON 对象字典。"""
    text = str(payload_text or "").strip()
    if not text:
        return None
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass

    fence_match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, flags=re.DOTALL | re.IGNORECASE)
    if fence_match:
        fenced = str(fence_match.group(1) or "").strip()
        if fenced:
            try:
                parsed = json.loads(fenced)
                if isinstance(parsed, dict):
                    return parsed
            except Exception:
                pass

    start = text.find("{")
    while start >= 0:
        depth = 0
        in_str = False
        escaped = False
        for idx in range(start, len(text)):
            ch = text[idx]
            if in_str:
                if escaped:
                    escaped = False
                elif ch == "\\":
                    escaped = True
                elif ch == "\"":
                    in_str = False
                continue
            if ch == "\"":
                in_str = True
                continue
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    candidate = text[start:idx + 1]
                    try:
                        parsed = json.loads(candidate)
                        if isinstance(parsed, dict):
                            return parsed
                    except Exception:
                        break
        start = text.find("{", start + 1)
    return None
