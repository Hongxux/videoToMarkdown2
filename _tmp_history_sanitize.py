#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from pathlib import Path


TARGETS = [
    Path("config/video_config.yaml"),
    Path("config/module2_config.yaml"),
]


REPLACERS = [
    re.compile(r'(?im)^([ \t]*api_key[ \t]*:[ \t]*)(?:"[^"\r\n]*"|\'[^\'\r\n]*\'|[^#\r\n]*)([ \t]*(?:#.*)?)$'),
    re.compile(r'(?im)^([ \t]*bearer_token[ \t]*:[ \t]*)(?:"[^"\r\n]*"|\'[^\'\r\n]*\'|[^#\r\n]*)([ \t]*(?:#.*)?)$'),
    re.compile(r'(?im)^([ \t]*DEEPSEEK_API_KEY[ \t]*=[ \t]*).*$'),
    re.compile(r'(?im)^([ \t]*DASHSCOPE_API_KEY[ \t]*=[ \t]*).*$'),
    re.compile(r'(?im)^([ \t]*VISION_AI_BEARER_TOKEN[ \t]*=[ \t]*).*$'),
]


def sanitize(text: str) -> str:
    out = text
    out = REPLACERS[0].sub(r'\1""\2', out)
    out = REPLACERS[1].sub(r'\1""\2', out)
    out = REPLACERS[2].sub(r"\1", out)
    out = REPLACERS[3].sub(r"\1", out)
    out = REPLACERS[4].sub(r"\1", out)
    return out


def main() -> int:
    for target in TARGETS:
        if not target.exists() or not target.is_file():
            continue
        raw = target.read_text(encoding="utf-8", errors="replace")
        fixed = sanitize(raw)
        if fixed != raw:
            target.write_text(fixed, encoding="utf-8", newline="\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
