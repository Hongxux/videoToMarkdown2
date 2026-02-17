#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""校验 mobile-markdown 手势模块接线与语法状态。"""

from __future__ import annotations

import re
import subprocess
import sys
import tempfile
from pathlib import Path


def run_node_check(target: Path) -> tuple[bool, str]:
    try:
        proc = subprocess.run(
            ["node", "--check", str(target)],
            capture_output=True,
            text=True,
            check=False,
            encoding="utf-8",
        )
    except FileNotFoundError:
        return False, "未找到 node，可先安装 Node.js 再执行语法检查。"
    if proc.returncode == 0:
        return True, ""
    detail = (proc.stderr or proc.stdout or "").strip()
    return False, detail or "node --check 执行失败。"


def main() -> int:
    root = Path(__file__).resolve().parents[2]
    html_path = root / "services/java-orchestrator/src/main/resources/static/mobile-markdown.html"
    gesture_js_path = root / "services/java-orchestrator/src/main/resources/static/lib/mobile-markdown-gestures.js"

    errors: list[str] = []
    if not html_path.exists():
        errors.append(f"缺少页面文件：{html_path}")
    if not gesture_js_path.exists():
        errors.append(f"缺少手势模块文件：{gesture_js_path}")
    if errors:
        for item in errors:
            print(f"[ERROR] {item}")
        return 1

    html_raw = html_path.read_text(encoding="utf-8")
    gesture_raw = gesture_js_path.read_text(encoding="utf-8")

    required_html_snippets = [
        '<script src="/lib/mobile-markdown-gestures.js"></script>',
        "window.mobileMarkdownGestures",
        "gestureModule.bindMarkdownBodyInteractions({",
        'id="readingOutlineBtn"',
    ]
    for snippet in required_html_snippets:
        if snippet not in html_raw:
            errors.append(f"页面接线缺失片段：{snippet}")

    # 结构债回归校验：确保旧版内联手势函数不再留在主脚本中。
    removed_inline_markers = [
        "function bindMarkdownPinchOutlineShortcut(",
        "function bindMarkdownParagraphGestures(",
        "function bindParagraphDraftSync(",
    ]
    for marker in removed_inline_markers:
        if marker in html_raw:
            errors.append(f"仍存在旧内联手势函数：{marker}")

    # FAB 已被产品决策淘汰，防止样式/DOM/事件链回流。
    fab_markers = [
        "quick-fab-wrap",
        "quickFabBtn",
        "quickActionRead",
        "quickActionOutline",
    ]
    for marker in fab_markers:
        if marker in html_raw:
            errors.append(f"检测到已淘汰 FAB 残留：{marker}")

    if "global.mobileMarkdownGestures" not in gesture_raw:
        errors.append("手势模块未导出 global.mobileMarkdownGestures。")
    if "function bindMarkdownBodyInteractions(" not in gesture_raw:
        errors.append("手势模块缺少 bindMarkdownBodyInteractions 实现。")

    ok_js, detail_js = run_node_check(gesture_js_path)
    if not ok_js:
        errors.append(f"手势模块语法检查失败：{detail_js}")

    # 抽取页面最后一个脚本块，做主脚本语法检查。
    script_blocks = re.findall(r"<script>\s*(.*?)\s*</script>", html_raw, flags=re.S)
    if not script_blocks:
        errors.append("页面中未找到内联主脚本。")
    else:
        with tempfile.NamedTemporaryFile("w", delete=False, suffix=".js", encoding="utf-8") as tmp:
            tmp.write(script_blocks[-1])
            tmp_path = Path(tmp.name)
        try:
            ok_main, detail_main = run_node_check(tmp_path)
            if not ok_main:
                errors.append(f"主脚本语法检查失败：{detail_main}")
        finally:
            tmp_path.unlink(missing_ok=True)

    if errors:
        for item in errors:
            print(f"[ERROR] {item}")
        return 1

    print("[OK] 手势模块接线与语法检查通过。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
