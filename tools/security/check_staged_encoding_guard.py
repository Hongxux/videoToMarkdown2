#!/usr/bin/env python3
from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
UTF8_BOM = b"\xef\xbb\xbf"

# 这些源码类型统一要求 UTF-8 无 BOM。
NO_BOM_SUFFIXES = {
    ".java",
    ".js",
    ".jsx",
    ".ts",
    ".tsx",
    ".css",
    ".html",
    ".htm",
    ".vue",
}

# 仅对可读文本文件做乱码检测。
TEXT_SUFFIXES = NO_BOM_SUFFIXES | {".md", ".py", ".yml", ".yaml", ".json", ".xml"}

QUESTION_RUN = re.compile(r"\?{3,}")


def run_git(args: list[str]) -> str:
    proc = subprocess.run(
        ["git", *args],
        cwd=REPO_ROOT,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or "git command failed")
    return proc.stdout


def staged_files() -> list[Path]:
    out = run_git(["diff", "--cached", "--name-only", "--diff-filter=ACMR"])
    items: list[Path] = []
    for raw in out.splitlines():
        rel = raw.strip()
        if not rel:
            continue
        p = REPO_ROOT / rel
        if p.is_file():
            items.append(p)
    return items


def contains_private_use(text: str) -> bool:
    return any("\ue000" <= ch <= "\uf8ff" for ch in text)


def check_bom_and_decode(path: Path, issues: list[str]) -> None:
    suffix = path.suffix.lower()
    data = path.read_bytes()

    if suffix in NO_BOM_SUFFIXES and data.startswith(UTF8_BOM):
        issues.append(f"{path}: source file has UTF-8 BOM")

    if suffix not in TEXT_SUFFIXES:
        return

    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as ex:
        issues.append(f"{path}: cannot decode as UTF-8 ({ex})")
        return

    if "\ufffd" in text:
        issues.append(f"{path}: contains replacement character U+FFFD")
    if contains_private_use(text):
        issues.append(f"{path}: contains private-use unicode chars (U+E000-U+F8FF)")


def added_lines(path: Path) -> list[str]:
    rel = path.relative_to(REPO_ROOT).as_posix()
    out = run_git(["diff", "--cached", "--unified=0", "--", rel])
    lines: list[str] = []
    for line in out.splitlines():
        if not line.startswith("+") or line.startswith("+++"):
            continue
        lines.append(line[1:])
    return lines


def check_mojibake_pattern(path: Path, issues: list[str]) -> None:
    suffix = path.suffix.lower()
    if suffix not in TEXT_SUFFIXES:
        return

    for idx, line in enumerate(added_lines(path), start=1):
        # 这类连串问号通常是中文写入链路错码的直接特征。
        if QUESTION_RUN.search(line):
            issues.append(
                f"{path}: suspicious repeated question marks (>=3) in staged added line #{idx}"
            )


def main() -> int:
    if os.environ.get("BYPASS_ENCODING_GUARD") == "1":
        return 0

    try:
        files = staged_files()
    except RuntimeError as ex:
        print(f"[encoding-guard] failed to inspect staged files: {ex}")
        return 1

    if not files:
        return 0

    issues: list[str] = []
    for path in files:
        check_bom_and_decode(path, issues)
        check_mojibake_pattern(path, issues)

    if not issues:
        print("[encoding-guard] staged encoding scan passed.")
        return 0

    print("[encoding-guard] blocked by staged encoding issues:")
    for item in issues:
        print(f"  - {item}")
    print("[encoding-guard] fix encoding/mojibake issues or set BYPASS_ENCODING_GUARD=1")
    return 1


if __name__ == "__main__":
    sys.exit(main())
