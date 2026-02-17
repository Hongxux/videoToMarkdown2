#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""巡检 UI 文案：打印可见文案并拦截工程术语。"""

from __future__ import annotations

import argparse
from html.parser import HTMLParser
from pathlib import Path
import re
import sys
from typing import Iterable


DEFAULT_FILES = [
    Path("services/java-orchestrator/src/main/resources/static/mobile-markdown.html"),
]

HTML_ATTR_KEYS = ("placeholder", "title", "aria-label", "alt")

BANNED_RULES = [
    (re.compile(r"导出"), "建议改为“保存”或“分享”"),
    (re.compile(r"\bexport\b", re.IGNORECASE), "建议改为“save/share”语义"),
    (re.compile(r"\bread\b"), "建议改为“开始阅读”"),
    (re.compile(r"更多操作"), "建议改为“更多功能”等用户语义"),
    (re.compile(r"请执行|执行.+操作|该操作"), "建议改为自然动作词，例如“开始/保存/删除”"),
]


def normalize_text(text: str) -> str:
    text = text.replace("\ufeff", "")
    text = re.sub(r"\s+", " ", text.strip())
    if not text:
        return ""
    return text


def unique_in_order(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if not item or item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


class VisibleTextCollector(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._skip_depth = 0
        self.texts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag_l = tag.lower()
        if tag_l in ("script", "style"):
            self._skip_depth += 1
            return
        for key, value in attrs:
            if value is None:
                continue
            if key.lower() in HTML_ATTR_KEYS:
                self.texts.append(normalize_text(value))

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in ("script", "style") and self._skip_depth > 0:
            self._skip_depth -= 1

    def handle_data(self, data: str) -> None:
        if self._skip_depth > 0:
            return
        text = normalize_text(data)
        if text:
            self.texts.append(text)


def collect_html_visible_text(html: str) -> list[str]:
    parser = VisibleTextCollector()
    parser.feed(html)
    return unique_in_order(parser.texts)


def collect_js_ui_literals(text: str) -> list[str]:
    patterns = [
        re.compile(r"setExportStatus\s*\([^,]+,\s*'([^']+)'"),
        re.compile(r"setExportStatus\s*\([^,]+,\s*\"([^\"]+)\""),
        re.compile(r"setExportStatus\s*\([^,]+,\s*`([^`]+)`"),
        re.compile(r"setControlLabel\s*\([^,]+,\s*'([^']+)'"),
        re.compile(r"setControlLabel\s*\([^,]+,\s*\"([^\"]+)\""),
        re.compile(r"setControlLabel\s*\([^,]+,\s*`([^`]+)`"),
        re.compile(r"viewerTitle\.textContent\s*=\s*'([^']+)'"),
        re.compile(r"viewerTitle\.textContent\s*=\s*\"([^\"]+)\""),
        re.compile(r"viewerTitle\.textContent\s*=\s*`([^`]+)`"),
        re.compile(r"alert\s*\(\s*'([^']+)'\s*\)"),
        re.compile(r"alert\s*\(\s*\"([^\"]+)\"\s*\)"),
        re.compile(r"alert\s*\(\s*`([^`]+)`\s*\)"),
        re.compile(r"confirm\s*\(\s*'([^']+)'\s*\)"),
        re.compile(r"confirm\s*\(\s*\"([^\"]+)\"\s*\)"),
        re.compile(r"confirm\s*\(\s*`([^`]+)`\s*\)"),
        re.compile(r"setTaskSummary\s*\(\s*'([^']+)'"),
        re.compile(r"setTaskSummary\s*\(\s*\"([^\"]+)\""),
        re.compile(r"setTaskSummary\s*\(\s*`([^`]+)`"),
    ]

    literals: list[str] = []
    for pattern in patterns:
        for match in pattern.finditer(text):
            raw = normalize_text(match.group(1))
            if not raw:
                continue
            cleaned = re.sub(r"\$\{[^}]+\}", "{...}", raw)
            literals.append(cleaned)
    return unique_in_order(literals)


def collect_copy_from_file(path: Path) -> tuple[list[str], list[str]]:
    raw = path.read_text(encoding="utf-8")
    html_texts = collect_html_visible_text(raw)
    js_texts = collect_js_ui_literals(raw)
    return html_texts, js_texts


def find_banned_texts(items: Iterable[str]) -> list[tuple[str, str]]:
    hits: list[tuple[str, str]] = []
    for text in items:
        for pattern, suggestion in BANNED_RULES:
            if pattern.search(text):
                hits.append((text, suggestion))
                break
    return hits


def print_section(title: str, items: list[str]) -> None:
    print(f"\n[{title}] 共 {len(items)} 条")
    for item in items:
        print(f"- {item}")


def main() -> int:
    parser = argparse.ArgumentParser(description="打印 UI 文案并检查工程术语。")
    parser.add_argument(
        "--file",
        dest="files",
        action="append",
        type=Path,
        help="要检查的文件路径（可重复传入）。默认检查 mobile-markdown.html。",
    )
    parser.add_argument(
        "--no-print",
        action="store_true",
        help="只检查，不打印文案清单。",
    )
    parser.add_argument(
        "--no-fail",
        action="store_true",
        help="发现禁用术语时不返回非 0 退出码。",
    )
    args = parser.parse_args()

    targets = args.files if args.files else DEFAULT_FILES
    missing = [p for p in targets if not p.exists()]
    if missing:
        print("以下文件不存在：")
        for p in missing:
            print(f"- {p}")
        return 1

    all_hits: list[tuple[Path, str, str]] = []
    for path in targets:
        html_texts, js_texts = collect_copy_from_file(path)
        all_texts = unique_in_order([*html_texts, *js_texts])

        if not args.no_print:
            print(f"\n==== {path} ====")
            print_section("HTML 可见文案", html_texts)
            print_section("JS 交互文案", js_texts)

        for text, suggestion in find_banned_texts(all_texts):
            all_hits.append((path, text, suggestion))

    if all_hits:
        print("\n发现术语风险：")
        for path, text, suggestion in all_hits:
            print(f"- {path}: {text} -> {suggestion}")
        return 0 if args.no_fail else 1

    print("\n文案巡检通过：未发现禁用术语。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
