from __future__ import annotations

import argparse
import asyncio
import json
import mimetypes
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


DEFAULT_WAIT_SELECTORS: dict[str, str] = {
    "zhihu": "main .RichContent, main .AnswerItem, main",
    "javaguide": "main, article, .vp-doc, .theme-vdoing-content, .markdown-body",
    "generic": "body",
}

EXTRACT_SCRIPT = r"""
({ siteType }) => {
  const pickRoot = (selectors) => {
    for (const selector of selectors) {
      const node = document.querySelector(selector);
      if (node) {
        return node;
      }
    }
    return document.body;
  };

  const siteSelectors = {
    zhihu: [
      "main .AnswerItem .RichContent",
      "main .RichContent",
      "main .Post-RichTextContainer",
      "main"
    ],
    javaguide: [
      ".vp-doc",
      ".theme-vdoing-content",
      ".markdown-body",
      "article",
      "main"
    ],
    generic: [
      "article",
      "main",
      "body"
    ]
  };

  const selectors = siteSelectors[siteType] || siteSelectors.generic;
  const root = pickRoot(selectors);

  const toAbs = (value) => {
    if (!value) return "";
    const v = String(value).trim();
    if (!v || v.startsWith("data:")) return "";
    try {
      return new URL(v, location.href).href;
    } catch {
      return "";
    }
  };

  const fromSrcset = (value) => {
    if (!value) return "";
    const parts = String(value).split(",");
    for (const item of parts) {
      const first = item.trim().split(/\s+/)[0];
      const abs = toAbs(first);
      if (abs) {
        return abs;
      }
    }
    return "";
  };

  const images = new Set();
  const nodes = root.querySelectorAll("img, source");
  for (const node of nodes) {
    const candidates = [
      node.getAttribute("src"),
      node.getAttribute("data-src"),
      node.getAttribute("data-original"),
      node.getAttribute("data-actualsrc"),
      node.currentSrc
    ];
    let selected = "";
    for (const candidate of candidates) {
      const abs = toAbs(candidate);
      if (abs) {
        selected = abs;
        break;
      }
    }
    if (!selected) {
      selected = fromSrcset(node.getAttribute("srcset")) || fromSrcset(node.srcset);
    }
    if (selected) {
      images.add(selected);
    }
  }

  const titleNode = document.querySelector("h1");
  const title = (titleNode?.innerText || document.title || "").trim();
  const text = (root?.innerText || "")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
  const rootSelector = selectors.find((selector) => document.querySelector(selector)) || "body";

  return {
    title,
    text,
    imageUrls: Array.from(images),
    pageUrl: location.href,
    rootSelector
  };
}
"""


@dataclass
class ImageDownloadResult:
    url: str
    ok: bool
    status: int | None
    local_path: str | None
    byte_size: int
    error: str | None


@dataclass
class PageExtractionResult:
    requested_url: str
    final_url: str
    site_type: str
    title: str
    root_selector: str
    text: str
    image_urls: list[str]
    fetched_at_utc: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="使用 Playwright 抓取页面正文与图片链接，支持知乎/Javaguide。",
    )
    parser.add_argument("urls", nargs="+", help="待抓取页面 URL，可传多个。")
    parser.add_argument(
        "--site",
        choices=["auto", "zhihu", "javaguide", "generic"],
        default="auto",
        help="页面类型。默认 auto 根据域名判断。",
    )
    parser.add_argument("--out-dir", type=Path, default=Path("var/playwright_extract"), help="输出目录。")
    parser.add_argument("--save-images", action="store_true", help="下载图片到本地。")
    parser.add_argument("--max-images", type=int, default=0, help="最多下载多少张图片，0 表示不限制。")
    parser.add_argument("--timeout-ms", type=int, default=45000, help="页面与资源请求超时（毫秒）。")
    parser.add_argument("--scroll-rounds", type=int, default=4, help="滚动次数，用于触发懒加载。")
    parser.add_argument("--scroll-step", type=int, default=1800, help="每次滚动的像素高度。")
    parser.add_argument("--scroll-wait-ms", type=int, default=1200, help="每次滚动后等待时间（毫秒）。")
    parser.add_argument("--wait-selector", default="", help="可选。页面加载后额外等待的 CSS 选择器。")
    parser.add_argument("--storage-state", type=Path, help="已保存的登录态 JSON 文件路径。")
    parser.add_argument("--pause-for-login", action="store_true", help="打开首个页面后暂停，手动登录再继续。")
    parser.add_argument("--save-storage-state", type=Path, help="将当前登录态保存到该 JSON 文件。")
    parser.add_argument("--headed", action="store_true", help="有头浏览器模式，便于观察或手动登录。")
    parser.add_argument("--slow-mo-ms", type=int, default=0, help="浏览器动作慢放毫秒数。")
    parser.add_argument("--user-agent", default="", help="覆盖默认 User-Agent。")

    args = parser.parse_args()
    if args.pause_for_login and not args.headed:
        parser.error("--pause-for-login 需要搭配 --headed 使用。")
    if args.max_images < 0:
        parser.error("--max-images 不能为负数。")
    if args.timeout_ms <= 0:
        parser.error("--timeout-ms 必须大于 0。")
    return args


def ensure_playwright() -> Any:
    try:
        from playwright.async_api import async_playwright  # type: ignore
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            "未检测到 playwright 依赖。请先执行:\n"
            "  pip install playwright\n"
            "  playwright install chromium"
        ) from exc
    return async_playwright


def infer_site_type(url: str, declared_site: str) -> str:
    if declared_site != "auto":
        return declared_site
    host = (urlparse(url).hostname or "").lower()
    if "zhihu.com" in host:
        return "zhihu"
    if "javaguide.cn" in host:
        return "javaguide"
    return "generic"


def now_utc_text() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def slugify(value: str, fallback: str = "page") -> str:
    ascii_value = value.encode("ascii", "ignore").decode("ascii")
    cleaned = re.sub(r"[^0-9A-Za-z._-]+", "-", ascii_value).strip("-._")
    return cleaned[:80] if cleaned else fallback


def guess_ext(url: str, content_type: str | None) -> str:
    suffix = Path(urlparse(url).path).suffix.lower()
    if suffix and len(suffix) <= 8:
        return suffix
    if content_type:
        mime = content_type.split(";")[0].strip().lower()
        guessed = mimetypes.guess_extension(mime) or ""
        if guessed == ".jpe":
            return ".jpg"
        if guessed:
            return guessed
    return ".bin"


async def wait_for_selector_if_needed(page: Any, site_type: str, args: argparse.Namespace) -> None:
    selector = args.wait_selector or DEFAULT_WAIT_SELECTORS.get(site_type, "body")
    if not selector:
        return
    try:
        await page.wait_for_selector(selector, timeout=args.timeout_ms)
    except Exception as exc:  # noqa: BLE001
        print(
            f"[warn] 等待选择器失败: selector={selector!r}, site={site_type}, error={exc}",
            file=sys.stderr,
        )


async def auto_scroll(page: Any, rounds: int, step: int, wait_ms: int) -> None:
    if rounds <= 0:
        return
    for _ in range(rounds):
        await page.mouse.wheel(0, step)
        await page.wait_for_timeout(wait_ms)


async def extract_single_page(page: Any, url: str, args: argparse.Namespace) -> PageExtractionResult:
    site_type = infer_site_type(url, args.site)
    response = await page.goto(url, wait_until="domcontentloaded", timeout=args.timeout_ms)
    if response is not None and response.status >= 400:
        raise RuntimeError(f"页面返回异常状态码: url={url}, status={response.status}")

    await wait_for_selector_if_needed(page, site_type, args)
    await auto_scroll(page, args.scroll_rounds, args.scroll_step, args.scroll_wait_ms)

    data = await page.evaluate(EXTRACT_SCRIPT, {"siteType": site_type})
    text = str(data.get("text") or "").strip()
    if not text:
        # 兜底策略：站点结构变化时至少拿到 body 文本，避免空结果。
        text = (await page.evaluate("() => (document.body?.innerText || '').trim()")) or ""

    image_urls = [str(item).strip() for item in (data.get("imageUrls") or []) if str(item).strip()]
    deduped_images = list(dict.fromkeys(image_urls))

    return PageExtractionResult(
        requested_url=url,
        final_url=str(data.get("pageUrl") or page.url or url),
        site_type=site_type,
        title=str(data.get("title") or "").strip(),
        root_selector=str(data.get("rootSelector") or ""),
        text=text,
        image_urls=deduped_images,
        fetched_at_utc=now_utc_text(),
    )


async def download_images(
    *,
    context: Any,
    image_urls: list[str],
    referer: str,
    save_dir: Path,
    timeout_ms: int,
    max_images: int,
) -> list[ImageDownloadResult]:
    save_dir.mkdir(parents=True, exist_ok=True)
    records: list[ImageDownloadResult] = []
    candidates = image_urls if max_images == 0 else image_urls[:max_images]

    for index, image_url in enumerate(candidates, start=1):
        try:
            response = await context.request.get(
                image_url,
                timeout=timeout_ms,
                headers={"Referer": referer},
            )
            status = int(response.status)
            if not response.ok:
                records.append(
                    ImageDownloadResult(
                        url=image_url,
                        ok=False,
                        status=status,
                        local_path=None,
                        byte_size=0,
                        error=f"http_{status}",
                    )
                )
                continue

            body = await response.body()
            content_type = response.headers.get("content-type", "")
            ext = guess_ext(image_url, content_type)
            filename = f"img_{index:04d}{ext}"
            output_path = save_dir / filename
            output_path.write_bytes(body)

            records.append(
                ImageDownloadResult(
                    url=image_url,
                    ok=True,
                    status=status,
                    local_path=str(output_path),
                    byte_size=len(body),
                    error=None,
                )
            )
        except Exception as exc:  # noqa: BLE001
            records.append(
                ImageDownloadResult(
                    url=image_url,
                    ok=False,
                    status=None,
                    local_path=None,
                    byte_size=0,
                    error=f"{type(exc).__name__}: {exc}",
                )
            )
    return records


def build_page_output_dir(base_output_dir: Path, index: int, result: PageExtractionResult) -> Path:
    parsed = urlparse(result.final_url)
    host = (parsed.hostname or "site").replace(".", "_")
    title_slug = slugify(result.title, fallback="untitled")
    page_dir = base_output_dir / f"{index:02d}_{host}_{title_slug}"
    page_dir.mkdir(parents=True, exist_ok=True)
    return page_dir


def write_page_output(
    *,
    page_dir: Path,
    result: PageExtractionResult,
    image_downloads: list[ImageDownloadResult],
) -> Path:

    text_path = page_dir / "content.txt"
    text_path.write_text(result.text, encoding="utf-8")

    metadata: dict[str, Any] = {
        "requested_url": result.requested_url,
        "final_url": result.final_url,
        "site_type": result.site_type,
        "title": result.title,
        "root_selector": result.root_selector,
        "fetched_at_utc": result.fetched_at_utc,
        "text_chars": len(result.text),
        "text_path": str(text_path),
        "image_urls": result.image_urls,
        "downloaded_images": [download.__dict__ for download in image_downloads],
    }
    json_path = page_dir / "result.json"
    json_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")

    return page_dir


async def run(args: argparse.Namespace) -> int:
    async_playwright = ensure_playwright()

    batch_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_root = args.out_dir / f"extract_{batch_id}"
    output_root.mkdir(parents=True, exist_ok=True)

    context_kwargs: dict[str, Any] = {}
    if args.storage_state:
        context_kwargs["storage_state"] = str(args.storage_state)
    if args.user_agent:
        context_kwargs["user_agent"] = args.user_agent

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=not args.headed,
            slow_mo=args.slow_mo_ms if args.slow_mo_ms > 0 else None,
        )
        context = await browser.new_context(**context_kwargs)
        page = await context.new_page()

        if args.pause_for_login:
            first_url = args.urls[0]
            print(f"[info] 登录前置页面: {first_url}")
            await page.goto(first_url, wait_until="domcontentloaded", timeout=args.timeout_ms)
            await asyncio.to_thread(input, "请在浏览器中完成登录后按 Enter 继续...")

        all_output_dirs: list[Path] = []
        all_failures: list[dict[str, str]] = []
        try:
            for idx, url in enumerate(args.urls, start=1):
                print(f"[info] 开始抓取: {url}")
                try:
                    result = await extract_single_page(page, url, args)
                    output_dir = build_page_output_dir(output_root, idx, result)
                    downloads: list[ImageDownloadResult] = []

                    if args.save_images and result.image_urls:
                        downloads = await download_images(
                            context=context,
                            image_urls=result.image_urls,
                            referer=result.final_url,
                            save_dir=output_dir / "images",
                            timeout_ms=args.timeout_ms,
                            max_images=args.max_images,
                        )

                    output_dir = write_page_output(page_dir=output_dir, result=result, image_downloads=downloads)
                    all_output_dirs.append(output_dir)
                    print(
                        "[info] 抓取完成: "
                        f"title={result.title!r}, text_chars={len(result.text)}, images={len(result.image_urls)}"
                    )
                except Exception as exc:  # noqa: BLE001
                    # 单页失败不终止批量抓取，错误上下文单独落盘，便于复盘与重试。
                    failure_payload = {
                        "url": url,
                        "error_type": type(exc).__name__,
                        "error_message": str(exc),
                        "failed_at_utc": now_utc_text(),
                    }
                    failure_path = output_root / f"{idx:02d}_failed.json"
                    failure_path.write_text(
                        json.dumps(failure_payload, ensure_ascii=False, indent=2),
                        encoding="utf-8",
                    )
                    all_failures.append(
                        {
                            "url": url,
                            "error": f"{type(exc).__name__}: {exc}",
                            "failure_path": str(failure_path),
                        }
                    )
                    print(
                        "[warn] 抓取失败: "
                        f"url={url}, error_type={type(exc).__name__}, message={exc}, detail={failure_path}",
                        file=sys.stderr,
                    )

            if args.save_storage_state:
                args.save_storage_state.parent.mkdir(parents=True, exist_ok=True)
                await context.storage_state(path=str(args.save_storage_state))
                print(f"[info] 已保存登录态: {args.save_storage_state}")
        finally:
            await context.close()
            await browser.close()

    summary = {
        "output_root": str(output_root),
        "pages": [str(item) for item in all_output_dirs],
        "failures": all_failures,
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
