#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""检查 docs/architecture 文档编码，防止中文乱码回归。"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys


MOJIBAKE_TOKENS = (
    "鏃ユ湡",
    "瑙﹀彂",
    "鍘熷洜",
    "淇",
    "锛",
    "銆",
)


def has_utf8_bom(path: Path) -> bool:
    """仅检查 BOM 头，避免把编码判断与文本解析耦合。"""
    raw = path.read_bytes()
    return raw.startswith(b"\xef\xbb\xbf")


def find_mojibake_tokens(text: str) -> list[str]:
    """用低成本特征词筛出高概率乱码，便于快速阻断回归。"""
    return [token for token in MOJIBAKE_TOKENS if token in text]


def check_file(path: Path, fix: bool, check_mojibake: bool) -> list[str]:
    issues: list[str] = []
    has_bom = has_utf8_bom(path)
    if not has_bom:
        issues.append(f"缺少 UTF-8 BOM: {path}")
        if fix:
            # 使用 utf-8-sig 回写，仅补 BOM，不改变正文语义。
            content = path.read_text(encoding="utf-8")
            path.write_text(content, encoding="utf-8-sig")

    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError as exc:
        issues.append(f"UTF-8 解码失败: {path} ({exc})")
        return issues

    if check_mojibake:
        mojibake_hits = find_mojibake_tokens(text)
        if mojibake_hits:
            issues.append(f"疑似乱码特征: {path} ({', '.join(mojibake_hits)})")
    return issues


def collect_issues(files: list[Path], fix: bool, check_mojibake: bool) -> list[str]:
    issues: list[str] = []
    for file_path in files:
        issues.extend(check_file(file_path, fix=fix, check_mojibake=check_mojibake))
    return issues


def main() -> int:
    parser = argparse.ArgumentParser(description="检查 docs/architecture 文档编码")
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parents[2],
        help="仓库根目录",
    )
    parser.add_argument(
        "--fix",
        action="store_true",
        help="自动修复缺少 BOM 的文件",
    )
    parser.add_argument(
        "--check-mojibake",
        action="store_true",
        help="额外检测常见乱码特征（历史存量乱码较多时建议按需开启）",
    )
    args = parser.parse_args()

    target_dir = args.root / "docs" / "architecture"
    files = sorted(target_dir.glob("*.md"))
    if not files:
        print(f"未找到目标目录或无 Markdown 文件: {target_dir}")
        return 1

    all_issues = collect_issues(files, fix=args.fix, check_mojibake=args.check_mojibake)
    if args.fix and all_issues:
        # 修复模式下执行二次校验，确保最终状态可直接用于 CI。
        all_issues = collect_issues(files, fix=False, check_mojibake=args.check_mojibake)

    if all_issues:
        print("发现编码问题:")
        for issue in all_issues:
            print(f"- {issue}")
        return 1

    if args.check_mojibake:
        print("编码检查通过：docs/architecture/*.md 均为 UTF-8 with BOM，且未命中乱码特征。")
    else:
        print("编码检查通过：docs/architecture/*.md 均为 UTF-8 with BOM。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
