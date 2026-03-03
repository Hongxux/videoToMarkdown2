from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse


EXTRACT_POST_LINKS_SCRIPT = r"""
() => {
  const anchors = Array.from(document.querySelectorAll("a[href]"));
  const links = [];
  const seen = new Set();
  for (const a of anchors) {
    const raw = String(a.getAttribute("href") || "").trim();
    if (!raw) continue;
    let abs = "";
    try {
      abs = new URL(raw, location.href).href;
    } catch {
      continue;
    }
    if (!abs.includes("/post/")) continue;
    if (!/https?:\/\/juejin\.cn\/post\/\d+/.test(abs)) continue;
    if (seen.has(abs)) continue;
    seen.add(abs);
    links.push(abs);
  }
  return links;
}
"""


def now_utc_text() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_playwright() -> Any:
    try:
        from playwright.async_api import async_playwright  # type: ignore
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            "未检测到 Playwright 依赖。请先执行：\n"
            "  pip install playwright\n"
            "  python -m playwright install chromium"
        ) from exc
    return async_playwright


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="抓取掘金用户主页下的全部文章链接（/post/{id}）。",
    )
    parser.add_argument(
        "profile_url",
        nargs="?",
        default="https://juejin.cn/user/4300945219651950",
        help="掘金用户主页 URL。",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("var/juejin_profile_links"),
        help="输出目录。",
    )
    parser.add_argument("--timeout-ms", type=int, default=60000, help="页面超时（毫秒）。")
    parser.add_argument("--scroll-step", type=int, default=2200, help="每轮滚动像素。")
    parser.add_argument("--scroll-wait-ms", type=int, default=1400, help="每轮滚动后等待毫秒。")
    parser.add_argument("--max-rounds", type=int, default=80, help="最大滚动轮数。")
    parser.add_argument(
        "--max-idle-rounds",
        type=int,
        default=6,
        help="连续无新增链接轮数达到阈值后提前停止。",
    )
    parser.add_argument("--headed", action="store_true", help="启用有头浏览器。")
    parser.add_argument("--slow-mo-ms", type=int, default=0, help="浏览器慢放毫秒。")
    parser.add_argument("--user-agent", default="", help="自定义 User-Agent。")
    parser.add_argument("--storage-state", type=Path, help="已保存登录态 JSON 文件路径。")
    parser.add_argument("--save-storage-state", type=Path, help="保存当前登录态到 JSON 文件。")
    args = parser.parse_args()

    if args.timeout_ms <= 0:
        parser.error("--timeout-ms 必须大于 0。")
    if args.scroll_step <= 0:
        parser.error("--scroll-step 必须大于 0。")
    if args.scroll_wait_ms < 0:
        parser.error("--scroll-wait-ms 不能小于 0。")
    if args.max_rounds <= 0:
        parser.error("--max-rounds 必须大于 0。")
    if args.max_idle_rounds <= 0:
        parser.error("--max-idle-rounds 必须大于 0。")
    return args


def normalize_post_url(url: str) -> str:
    value = str(url).strip()
    if not value:
        return ""
    parsed = urlparse(value)
    path = parsed.path or ""
    match = re.search(r"/post/(\d+)", path)
    if not match:
        return ""
    post_id = match.group(1)
    return f"https://juejin.cn/post/{post_id}"


async def collect_post_links(page: Any) -> list[str]:
    raw_links = await page.evaluate(EXTRACT_POST_LINKS_SCRIPT)
    normalized = [normalize_post_url(str(item)) for item in (raw_links or [])]
    unique = [item for item in dict.fromkeys(normalized) if item]
    return unique


async def run(args: argparse.Namespace) -> int:
    async_playwright = ensure_playwright()

    batch_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_dir = args.out_dir / f"extract_{batch_id}"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_json_path = output_dir / "result.json"
    output_txt_path = output_dir / "article_links.txt"

    context_kwargs: dict[str, Any] = {}
    if args.user_agent:
        context_kwargs["user_agent"] = args.user_agent
    if args.storage_state:
        context_kwargs["storage_state"] = str(args.storage_state)

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=not args.headed,
            slow_mo=args.slow_mo_ms if args.slow_mo_ms > 0 else None,
        )
        context = await browser.new_context(**context_kwargs)
        page = await context.new_page()

        try:
            print(f"[info] open profile: {args.profile_url}")
            response = await page.goto(args.profile_url, wait_until="domcontentloaded", timeout=args.timeout_ms)
            if response is not None and int(response.status) >= 400:
                raise RuntimeError(f"profile open failed: status={response.status}, url={args.profile_url}")

            await page.wait_for_selector("body", timeout=args.timeout_ms)
            await page.wait_for_timeout(1200)

            links = await collect_post_links(page)
            idle_rounds = 0

            for round_index in range(1, args.max_rounds + 1):
                await page.mouse.wheel(0, args.scroll_step)
                await page.wait_for_timeout(args.scroll_wait_ms)
                current_links = await collect_post_links(page)
                if len(current_links) > len(links):
                    links = current_links
                    idle_rounds = 0
                else:
                    idle_rounds += 1

                print(
                    f"[info] round={round_index}, total_links={len(links)}, idle_rounds={idle_rounds}",
                )
                if idle_rounds >= args.max_idle_rounds:
                    break

            links = sorted(dict.fromkeys(links))
            output_txt_path.write_text("\n".join(links) + ("\n" if links else ""), encoding="utf-8")

            payload = {
                "profile_url": args.profile_url,
                "final_url": page.url,
                "collected_at_utc": now_utc_text(),
                "count": len(links),
                "links_path": str(output_txt_path),
                "links": links,
                "scroll": {
                    "max_rounds": args.max_rounds,
                    "max_idle_rounds": args.max_idle_rounds,
                    "scroll_step": args.scroll_step,
                    "scroll_wait_ms": args.scroll_wait_ms,
                },
            }
            output_json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

            if args.save_storage_state:
                args.save_storage_state.parent.mkdir(parents=True, exist_ok=True)
                await context.storage_state(path=str(args.save_storage_state))
                print(f"[info] storage state saved: {args.save_storage_state}")
        finally:
            await context.close()
            await browser.close()

    summary = {
        "output_dir": str(output_dir),
        "result_json": str(output_json_path),
        "result_txt": str(output_txt_path),
        "count": payload["count"],
        "generated_at_utc": now_utc_text(),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


def main() -> int:
    args = parse_args()
    try:
        return asyncio.run(run(args))
    except RuntimeError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("[error] 用户中断执行。", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
