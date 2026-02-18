#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import re
import subprocess
import sys
from dataclasses import dataclass
from typing import Iterable, List, Tuple

MAX_SCAN_BYTES = 2 * 1024 * 1024

BLOCKED_PATH_PATTERNS = [
    ("dotenv", re.compile(r"(^|/)\.env(?!\.example$)(\..*)?$", re.IGNORECASE)),
    ("cookie_file", re.compile(r"(^|/)cookies\.txt$", re.IGNORECASE)),
    ("private_key_ext", re.compile(r"\.(pem|key|p8|p12|pfx|jks|keystore)$", re.IGNORECASE)),
    (
        "service_account_json",
        re.compile(r"(^|/).*(credentials|service-account|secret).+\.json$", re.IGNORECASE),
    ),
]

CONTENT_PATTERNS = [
    ("openai_key", re.compile(r"\bsk-[A-Za-z0-9]{20,}\b")),
    ("anthropic_key", re.compile(r"\bsk-ant-[A-Za-z0-9_-]{20,}\b")),
    ("github_pat", re.compile(r"\bgh[pousr]_[A-Za-z0-9]{30,}\b")),
    ("aws_access_key", re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b")),
    ("google_api_key", re.compile(r"\bAIza[0-9A-Za-z_-]{35}\b")),
    ("slack_token", re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{20,}\b")),
    ("private_key_block", re.compile(r"-----BEGIN (?:RSA|OPENSSH|EC) PRIVATE KEY-----")),
    (
        "generic_secret_assignment",
        re.compile(
            r"""(?im)
            \b(?:api[_-]?key|access[_-]?key|secret|token|password)\b
            \s*[:=]\s*
            ["']?[A-Za-z0-9_/\-+=]{16,}["']?
            """
        ),
    ),
]

PLACEHOLDER_MARKERS = ("example", "sample", "dummy", "fake", "placeholder", "changeme")

SANITIZE_TARGET_FILE = re.compile(r"(^|/)config/.*\.(ya?ml|json|properties|env)$", re.IGNORECASE)
SANITIZE_YAML_PATTERNS = [
    re.compile(rb"(?im)^([ \t]*api_key[ \t]*:[ \t]*)(?:\"[^\r\n\"]*\"|'[^\r\n']*'|[^#\r\n]*)?([ \t]*(?:#.*)?)$"),
    re.compile(
        rb"(?im)^([ \t]*bearer_token[ \t]*:[ \t]*)(?:\"[^\r\n\"]*\"|'[^\r\n']*'|[^#\r\n]*)?([ \t]*(?:#.*)?)$"
    ),
]


@dataclass
class Hit:
    path: str
    rule: str
    evidence: str


def run_git(args: List[str], decode: bool = True):
    result = subprocess.run(
        ["git", *args],
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"git {' '.join(args)} failed: {stderr}")
    if decode:
        return result.stdout.decode("utf-8", errors="replace")
    return result.stdout


def iter_staged_files() -> Iterable[str]:
    output = run_git(["diff", "--cached", "--name-only", "-z", "--diff-filter=ACMR"], decode=False)
    if not output:
        return []
    return [part.decode("utf-8", errors="replace") for part in output.split(b"\x00") if part]


def get_staged_blob(path: str) -> bytes:
    return run_git(["show", f":{path}"], decode=False)


def hash_blob(content: bytes) -> str:
    result = subprocess.run(
        ["git", "hash-object", "-w", "--stdin"],
        input=content,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"git hash-object failed: {stderr}")
    return result.stdout.decode("utf-8", errors="replace").strip()


def get_index_mode(path: str) -> str:
    output = run_git(["ls-files", "-s", "--", path], decode=True).strip()
    if not output:
        raise RuntimeError(f"cannot read index mode for {path}")
    return output.split()[0]


def update_index_blob(path: str, content: bytes) -> None:
    mode = get_index_mode(path)
    oid = hash_blob(content)
    run_git(["update-index", "--cacheinfo", f"{mode},{oid},{path}"], decode=True)


def is_probably_binary(content: bytes) -> bool:
    return b"\x00" in content[:4096]


def redact(text: str, limit: int = 90) -> str:
    compact = " ".join(text.strip().split())
    if len(compact) <= limit:
        return compact
    return compact[:limit] + "..."


def line_of_match(content: str, start_index: int) -> str:
    line_start = content.rfind("\n", 0, start_index) + 1
    line_end = content.find("\n", start_index)
    if line_end == -1:
        line_end = len(content)
    return content[line_start:line_end]


def should_skip_line(line_text: str) -> bool:
    lowered = line_text.lower()
    return any(marker in lowered for marker in PLACEHOLDER_MARKERS)


def scan_paths(path: str) -> List[Hit]:
    normalized = path.replace("\\", "/")
    hits: List[Hit] = []
    for rule_name, pattern in BLOCKED_PATH_PATTERNS:
        if pattern.search(normalized):
            hits.append(Hit(path=path, rule=rule_name, evidence="path matches sensitive rule"))
    return hits


def scan_content(path: str, content: bytes) -> List[Hit]:
    if not content or is_probably_binary(content):
        return []

    if len(content) > MAX_SCAN_BYTES:
        content = content[:MAX_SCAN_BYTES]

    text = content.decode("utf-8", errors="replace")
    hits: List[Hit] = []

    for rule_name, pattern in CONTENT_PATTERNS:
        for match in pattern.finditer(text):
            hit_line = line_of_match(text, match.start())
            if should_skip_line(hit_line):
                continue
            hits.append(Hit(path=path, rule=rule_name, evidence=redact(hit_line)))
    return hits


def _yaml_secret_replacer(match: re.Match[bytes]) -> bytes:
    return match.group(1) + b'""' + (match.group(2) or b"")


def sanitize_ai_secrets_for_index(path: str, content: bytes) -> Tuple[bytes, bool]:
    normalized = path.replace("\\", "/")
    if not SANITIZE_TARGET_FILE.search(normalized):
        return content, False
    if is_probably_binary(content):
        return content, False

    sanitized = content
    for pattern in SANITIZE_YAML_PATTERNS:
        sanitized = pattern.sub(_yaml_secret_replacer, sanitized)

    return sanitized, sanitized != content


def main() -> int:
    try:
        staged_files = list(iter_staged_files())
    except RuntimeError as exc:
        print(f"[secret-guard] failed to read staged files: {exc}", file=sys.stderr)
        return 2

    if not staged_files:
        return 0

    all_hits: List[Hit] = []
    sanitized_paths: List[str] = []
    for path in staged_files:
        all_hits.extend(scan_paths(path))
        try:
            blob = get_staged_blob(path)
        except RuntimeError:
            continue

        sanitized_blob, changed = sanitize_ai_secrets_for_index(path, blob)
        if changed:
            try:
                update_index_blob(path, sanitized_blob)
                blob = sanitized_blob
                sanitized_paths.append(path)
            except RuntimeError as exc:
                print(f"[secret-guard] failed to sanitize index content for {path}: {exc}", file=sys.stderr)
                return 2

        all_hits.extend(scan_content(path, blob))

    if sanitized_paths:
        print("[secret-guard] staged content auto-sanitized (local files unchanged):")
        for path in sanitized_paths:
            print(f"  - {path}")

    if not all_hits:
        return 0

    print("[secret-guard] possible secrets detected. commit blocked:")
    for hit in all_hits:
        print(f"  - {hit.path} | rule={hit.rule} | evidence={hit.evidence}")

    print("\nTemporary bypass (not recommended):")
    print("  - PowerShell: $env:BYPASS_SECRET_SCAN='1'; git commit ...")
    print("  - Bash: BYPASS_SECRET_SCAN=1 git commit ...")
    return 1


if __name__ == "__main__":
    sys.exit(main())
