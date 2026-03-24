import json
import re
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple


JsonRepairer = Callable[[str], str]


def parse_json_payload(
    text: str,
    extra_repairers: Optional[Sequence[JsonRepairer]] = None,
) -> Tuple[Optional[Any], Optional[Exception]]:
    last_err: Optional[Exception] = None
    for candidate in build_json_parse_candidates(text, extra_repairers=extra_repairers):
        try:
            return json.loads(candidate), None
        except json.JSONDecodeError as exc:
            last_err = exc
            continue
    return None, last_err


def build_json_parse_candidates(
    text: str,
    extra_repairers: Optional[Sequence[JsonRepairer]] = None,
) -> List[str]:
    raw = str(text or "").strip()
    if not raw:
        return []

    candidates: List[str] = []
    seen: set[str] = set()
    repairers = [fn for fn in (extra_repairers or []) if callable(fn)]

    def _append(candidate: str) -> None:
        value = str(candidate or "").strip()
        if not value or value in seen:
            return
        seen.add(value)
        candidates.append(value)

    def _append_with_repairers(candidate: str) -> None:
        _append(candidate)
        current = str(candidate or "")
        for repairer in repairers:
            try:
                current = repairer(current)
            except Exception:
                continue
            _append(current)

    _append_with_repairers(raw)
    _append_with_repairers(remove_trailing_commas(raw))

    normalized = normalize_jsonish_text(raw)
    _append_with_repairers(normalized)

    repaired = repair_unclosed_json(normalized)
    _append_with_repairers(repaired)

    return candidates


def extract_salvaged_json_objects(
    text: str,
    extra_repairers: Optional[Sequence[JsonRepairer]] = None,
) -> Tuple[List[Dict[str, Any]], Optional[Exception]]:
    parsed: List[Dict[str, Any]] = []
    last_err: Optional[Exception] = None
    for obj_text in extract_top_level_objects(text):
        data, err = parse_json_payload(obj_text, extra_repairers=extra_repairers)
        if isinstance(data, dict):
            parsed.append(data)
        elif err is not None:
            last_err = err
    return parsed, last_err


def extract_top_level_objects(text: str) -> List[str]:
    if not text:
        return []
    objs: List[str] = []
    depth = 0
    start_idx: Optional[int] = None
    in_str = False
    quote = ""
    escape = False

    for i, ch in enumerate(text):
        if in_str:
            if escape:
                escape = False
                continue
            if ch == "\\":
                escape = True
                continue
            if ch == quote:
                in_str = False
                quote = ""
            continue

        if ch in {"\"", "'"}:
            in_str = True
            quote = ch
            continue
        if ch == "{":
            if depth == 0:
                start_idx = i
            depth += 1
            continue
        if ch == "}" and depth > 0:
            depth -= 1
            if depth == 0 and start_idx is not None:
                segment = text[start_idx : i + 1].strip()
                if segment:
                    objs.append(segment)
                start_idx = None
    return objs


def normalize_jsonish_text(text: str) -> str:
    if not text:
        return ""
    s = str(text).lstrip("\ufeff").strip()
    s = escape_control_chars_in_strings(s)
    s = remove_trailing_commas(s)
    return s


def escape_control_chars_in_strings(text: str) -> str:
    if not text:
        return ""
    out: List[str] = []
    in_str = False
    quote = ""
    escape = False
    for ch in text:
        if in_str:
            if escape:
                out.append(ch)
                escape = False
                continue
            if ch == "\\":
                out.append(ch)
                escape = True
                continue
            if ch == quote:
                out.append(ch)
                in_str = False
                quote = ""
                continue
            code = ord(ch)
            if code < 0x20:
                if ch == "\n":
                    out.append("\\n")
                elif ch == "\r":
                    out.append("\\r")
                elif ch == "\t":
                    out.append("\\t")
                else:
                    out.append(f"\\u{code:04x}")
            else:
                out.append(ch)
            continue

        if ch in {"\"", "'"}:
            in_str = True
            quote = ch
            out.append(ch)
            continue
        out.append(ch)
    return "".join(out)


def remove_trailing_commas(text: str) -> str:
    if not text:
        return ""
    current = str(text)
    for _ in range(4):
        updated = re.sub(r",(\s*[}\]])", r"\1", current)
        if updated == current:
            break
        current = updated
    return current


def repair_unclosed_json(text: str) -> str:
    if not text:
        return ""

    start_idx = -1
    for i, ch in enumerate(text):
        if ch in {"[", "{"}:
            start_idx = i
            break
    if start_idx < 0:
        return text

    src = text[start_idx:].strip()
    if not src:
        return ""

    out: List[str] = []
    stack: List[str] = []
    in_str = False
    quote = ""
    escape = False

    for ch in src:
        out.append(ch)
        if in_str:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == quote:
                in_str = False
                quote = ""
            continue

        if ch in {"\"", "'"}:
            in_str = True
            quote = ch
            continue
        if ch in {"[", "{"}:
            stack.append(ch)
            continue
        if ch in {"]", "}"} and stack:
            top = stack[-1]
            if (top == "[" and ch == "]") or (top == "{" and ch == "}"):
                stack.pop()

    if in_str and quote:
        if escape:
            out.append("\\")
        out.append(quote)

    while stack:
        top = stack.pop()
        out.append("]" if top == "[" else "}")

    repaired = "".join(out)
    return remove_trailing_commas(repaired)
