from __future__ import annotations

import argparse
import asyncio
import json
import mimetypes
import re
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup, NavigableString, Tag


DEFAULT_ARTICLE_SELECTORS: list[str] = [
    "article",
    ".article-content",
    ".markdown-body",
    ".article-viewer",
    "main article",
    "main",
]

EXTRACT_SCRIPT = r"""
({ selectors }) => {
  const toAbs = (value) => {
    if (!value) return "";
    const raw = String(value).trim();
    if (!raw || raw.startsWith("data:")) return "";
    try {
      return new URL(raw, location.href).href;
    } catch {
      return "";
    }
  };

  const fromSrcset = (value) => {
    if (!value) return "";
    const parts = String(value).split(",");
    for (const part of parts) {
      const candidate = part.trim().split(/\s+/)[0];
      const abs = toAbs(candidate);
      if (abs) return abs;
    }
    return "";
  };

  const pickRoot = () => {
    for (const selector of selectors) {
      const node = document.querySelector(selector);
      if (node && (node.innerText || "").trim().length > 80) {
        return { node, selector };
      }
    }
    for (const selector of selectors) {
      const node = document.querySelector(selector);
      if (node) {
        return { node, selector };
      }
    }
    return { node: document.body, selector: "body" };
  };

  const pickImageSrc = (node) => {
    const candidates = [
      node.getAttribute("src"),
      node.getAttribute("data-src"),
      node.getAttribute("data-original"),
      node.getAttribute("data-actualsrc"),
      node.currentSrc
    ];
    for (const candidate of candidates) {
      const abs = toAbs(candidate);
      if (abs) return abs;
    }
    const srcsetValue = node.getAttribute("srcset") || node.srcset || "";
    return fromSrcset(srcsetValue);
  };

  const picked = pickRoot();
  const root = picked.node;
  const imageUrls = [];
  const seen = new Set();
  const imageNodes = root.querySelectorAll("img, source");
  for (const node of imageNodes) {
    const src = pickImageSrc(node);
    if (!src) continue;
    if (seen.has(src)) continue;
    seen.add(src);
    imageUrls.push(src);
  }

  const titleNode = root.querySelector("h1") || document.querySelector("h1");
  const title = (titleNode?.innerText || document.title || "").trim();
  const text = (root.innerText || "").replace(/\n{3,}/g, "\n\n").trim();
  const articleHtml = root.innerHTML || "";

  return {
    title,
    pageUrl: location.href,
    rootSelector: picked.selector,
    articleHtml,
    text,
    imageUrls
  };
}
"""


@dataclass
class ExtractionResult:
    requested_url: str
    final_url: str
    title: str
    root_selector: str
    article_html: str
    text: str
    image_urls: list[str]
    fetched_at_utc: str


@dataclass
class ImageDownloadResult:
    source_url: str
    ok: bool
    status: int | None
    local_path: str | None
    relative_path: str | None
    byte_size: int
    error: str | None


def now_utc_text() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="使用 Playwright 抓取掘金文章，下载图片并导出为 Markdown。",
    )
    parser.add_argument(
        "url",
        nargs="?",
        default="https://juejin.cn/post/7611470578184470580",
        help="待抓取文章 URL。默认使用内置测试链接。",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("var/juejin_playwright_md"),
        help="输出根目录。",
    )
    parser.add_argument(
        "--article-selectors",
        default=",".join(DEFAULT_ARTICLE_SELECTORS),
        help="正文根节点选择器，英文逗号分隔。",
    )
    parser.add_argument("--wait-selector", default="", help="页面加载后额外等待的 CSS 选择器。")
    parser.add_argument("--timeout-ms", type=int, default=60000, help="页面与资源请求超时（毫秒）。")
    parser.add_argument("--scroll-rounds", type=int, default=6, help="自动滚动次数。")
    parser.add_argument("--scroll-step", type=int, default=1800, help="每次滚动像素。")
    parser.add_argument("--scroll-wait-ms", type=int, default=1000, help="每次滚动后等待毫秒。")
    parser.add_argument("--max-images", type=int, default=0, help="最多下载图片数，0 表示不限制。")
    parser.add_argument("--output-name", default="article.md", help="Markdown 文件名。")
    parser.add_argument("--save-html", action="store_true", help="额外保存正文 HTML。")
    parser.add_argument("--headed", action="store_true", help="使用有头浏览器。")
    parser.add_argument("--slow-mo-ms", type=int, default=0, help="浏览器动作慢放毫秒。")
    parser.add_argument("--user-agent", default="", help="自定义 User-Agent。")
    args = parser.parse_args()

    if args.timeout_ms <= 0:
        parser.error("--timeout-ms 必须大于 0。")
    if args.scroll_rounds < 0:
        parser.error("--scroll-rounds 不能小于 0。")
    if args.scroll_step < 0:
        parser.error("--scroll-step 不能小于 0。")
    if args.scroll_wait_ms < 0:
        parser.error("--scroll-wait-ms 不能小于 0。")
    if args.max_images < 0:
        parser.error("--max-images 不能小于 0。")
    if not str(args.output_name).lower().endswith(".md"):
        parser.error("--output-name 必须以 .md 结尾。")
    return args


def ensure_playwright() -> Any:
    try:
        from playwright.async_api import async_playwright  # type: ignore
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            "未检测到 Playwright 依赖。请先执行：\n"
            "  pip install playwright\n"
            "  playwright install chromium"
        ) from exc
    return async_playwright


def split_selectors(raw: str) -> list[str]:
    selectors = [item.strip() for item in str(raw).split(",") if item.strip()]
    if not selectors:
        return DEFAULT_ARTICLE_SELECTORS[:]
    return selectors


def slugify(value: str, fallback: str = "article") -> str:
    normalized = re.sub(r"[^\w.-]+", "-", str(value).strip(), flags=re.UNICODE).strip("-._")
    if not normalized:
        return fallback
    return normalized[:80]


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


def normalize_inline_text(text: str) -> str:
    value = str(text).replace("\xa0", " ")
    value = re.sub(r"[ \t\r\f\v]+", " ", value)
    value = re.sub(r" *\n *", "\n", value)
    return value.strip()


def escape_inline_text(text: str) -> str:
    value = str(text)
    value = value.replace("\\", "\\\\")
    value = value.replace("`", "\\`")
    value = value.replace("[", "\\[")
    value = value.replace("]", "\\]")
    return value


def abs_url(raw_url: str | None, base_url: str) -> str:
    value = str(raw_url or "").strip()
    if not value:
        return ""
    if value.startswith("data:"):
        return ""
    return urljoin(base_url, value)


def extract_code_lang(tag: Tag) -> str:
    classes = [str(item).strip() for item in (tag.get("class") or []) if str(item).strip()]
    for name in classes:
        match = re.search(r"(?:language|lang)-([a-zA-Z0-9_+-]+)", name)
        if match:
            return match.group(1).lower()
    code_tag = tag.find("code")
    if isinstance(code_tag, Tag):
        for name in [str(item).strip() for item in (code_tag.get("class") or []) if str(item).strip()]:
            match = re.search(r"(?:language|lang)-([a-zA-Z0-9_+-]+)", name)
            if match:
                return match.group(1).lower()
    return ""


def pick_image_src(tag: Tag, base_url: str) -> str:
    candidates = [
        tag.get("src"),
        tag.get("data-src"),
        tag.get("data-original"),
        tag.get("data-actualsrc"),
    ]
    for item in candidates:
        url = abs_url(str(item or ""), base_url)
        if url:
            return url

    srcset_value = str(tag.get("srcset") or "")
    if not srcset_value:
        return ""
    parts = [item.strip() for item in srcset_value.split(",") if item.strip()]
    for part in parts:
        first_token = part.split()[0].strip()
        url = abs_url(first_token, base_url)
        if url:
            return url
    return ""


def render_image(tag: Tag, base_url: str, image_path_map: dict[str, str]) -> str:
    src = pick_image_src(tag, base_url)
    if not src:
        return ""
    mapped = image_path_map.get(src, src)
    alt = normalize_inline_text(str(tag.get("alt") or "image"))
    alt = escape_inline_text(alt or "image")
    return f"![{alt}]({mapped})"


def render_inline(node: Any, base_url: str, image_path_map: dict[str, str]) -> str:
    if isinstance(node, NavigableString):
        return escape_inline_text(str(node))
    if not isinstance(node, Tag):
        return ""

    name = node.name.lower()
    if name in {"script", "style", "noscript"}:
        return ""
    if name == "br":
        return "\n"
    if name == "img":
        return render_image(node, base_url, image_path_map)
    if name in {"strong", "b"}:
        text = normalize_inline_text("".join(render_inline(child, base_url, image_path_map) for child in node.children))
        if not text:
            return ""
        return f"**{text}**"
    if name in {"em", "i"}:
        text = normalize_inline_text("".join(render_inline(child, base_url, image_path_map) for child in node.children))
        if not text:
            return ""
        return f"*{text}*"
    if name == "code":
        text = str(node.get_text("", strip=False)).strip()
        if not text:
            return ""
        text = text.replace("\n", " ")
        text = text.replace("`", "\\`")
        return f"`{text}`"
    if name == "a":
        href = abs_url(str(node.get("href") or ""), base_url)
        text = normalize_inline_text("".join(render_inline(child, base_url, image_path_map) for child in node.children))
        if href:
            label = text or href
            return f"[{label}]({href})"
        return text

    return "".join(render_inline(child, base_url, image_path_map) for child in node.children)


def render_table(tag: Tag, base_url: str, image_path_map: dict[str, str]) -> str:
    rows: list[list[str]] = []
    for row in tag.find_all("tr"):
        cells = row.find_all(["th", "td"])
        if not cells:
            continue
        row_values: list[str] = []
        for cell in cells:
            cell_text = normalize_inline_text("".join(render_inline(child, base_url, image_path_map) for child in cell.children))
            row_values.append(cell_text)
        rows.append(row_values)

    if not rows:
        return ""
    col_count = max(len(row) for row in rows)
    padded_rows = [row + [""] * (col_count - len(row)) for row in rows]
    header = padded_rows[0]
    separator = ["---"] * col_count
    lines = [
        "| " + " | ".join(header) + " |",
        "| " + " | ".join(separator) + " |",
    ]
    for row in padded_rows[1:]:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


def render_block(node: Any, base_url: str, image_path_map: dict[str, str], indent: int = 0) -> list[str]:
    if isinstance(node, NavigableString):
        text = normalize_inline_text(str(node))
        return [text] if text else []
    if not isinstance(node, Tag):
        return []

    name = node.name.lower()
    if name in {"script", "style", "noscript"}:
        return []

    if name in {"h1", "h2", "h3", "h4", "h5", "h6"}:
        level = int(name[1])
        text = normalize_inline_text("".join(render_inline(child, base_url, image_path_map) for child in node.children))
        if not text:
            return []
        return [f"{'#' * level} {text}"]

    if name in {"p", "figcaption"}:
        text = normalize_inline_text("".join(render_inline(child, base_url, image_path_map) for child in node.children))
        return [text] if text else []

    if name == "pre":
        code_text = str(node.get_text("", strip=False)).rstrip("\n")
        if not code_text:
            return []
        lang = extract_code_lang(node)
        fence = f"```{lang}" if lang else "```"
        return [f"{fence}\n{code_text}\n```"]

    if name == "blockquote":
        inner_blocks: list[str] = []
        for child in node.children:
            inner_blocks.extend(render_block(child, base_url, image_path_map, indent=indent))
        if not inner_blocks:
            text = normalize_inline_text("".join(render_inline(child, base_url, image_path_map) for child in node.children))
            if not text:
                return []
            inner_blocks = [text]
        text = "\n\n".join(inner_blocks).strip()
        if not text:
            return []
        quoted_lines = [f"> {line}" if line else ">" for line in text.splitlines()]
        return ["\n".join(quoted_lines)]

    if name in {"ul", "ol"}:
        ordered = name == "ol"
        items = [child for child in node.children if isinstance(child, Tag) and child.name.lower() == "li"]
        lines: list[str] = []
        for index, item in enumerate(items, start=1):
            marker = f"{index}. " if ordered else "- "
            item_segments: list[str] = []
            nested_lines: list[str] = []
            for child in item.children:
                if isinstance(child, Tag) and child.name and child.name.lower() in {"ul", "ol"}:
                    nested = render_block(child, base_url, image_path_map, indent=indent + 2)
                    nested_lines.extend(nested)
                    continue
                child_blocks = render_block(child, base_url, image_path_map, indent=indent + 2)
                if child_blocks:
                    if not item_segments:
                        item_segments.append(child_blocks[0])
                        for extra in child_blocks[1:]:
                            nested_lines.append((" " * (indent + 2)) + extra)
                    else:
                        for extra in child_blocks:
                            nested_lines.append((" " * (indent + 2)) + extra)
                    continue
                inline = normalize_inline_text(render_inline(child, base_url, image_path_map))
                if inline:
                    item_segments.append(inline)
            item_text = normalize_inline_text(" ".join(item_segments))
            lines.append((" " * indent) + marker + item_text)
            lines.extend(nested_lines)
        return [line for line in lines if line.strip()]

    if name == "img":
        rendered = render_image(node, base_url, image_path_map)
        return [rendered] if rendered else []

    if name == "figure":
        lines: list[str] = []
        for child in node.children:
            lines.extend(render_block(child, base_url, image_path_map, indent=indent))
        return [line for line in lines if line.strip()]

    if name == "table":
        rendered = render_table(node, base_url, image_path_map)
        return [rendered] if rendered else []

    if name == "hr":
        return ["---"]

    lines: list[str] = []
    for child in node.children:
        lines.extend(render_block(child, base_url, image_path_map, indent=indent))
    if lines:
        return lines

    text = normalize_inline_text("".join(render_inline(child, base_url, image_path_map) for child in node.children))
    return [text] if text else []


def html_to_markdown(article_html: str, base_url: str, image_path_map: dict[str, str]) -> str:
    soup = BeautifulSoup(article_html, "html.parser")
    for tag in soup.find_all(["script", "style", "noscript"]):
        tag.decompose()

    blocks: list[str] = []
    for node in soup.contents:
        blocks.extend(render_block(node, base_url, image_path_map))

    merged = "\n\n".join(item.strip() for item in blocks if item and item.strip())
    merged = re.sub(r"\n{3,}", "\n\n", merged).strip()
    return merged


def remove_leading_duplicate_title(markdown_body: str, title: str) -> str:
    body = str(markdown_body or "").lstrip()
    if not body:
        return body

    match = re.match(r"^#\s+(.+?)\n+", body)
    if not match:
        return body

    heading = normalize_inline_text(match.group(1))
    expected = normalize_inline_text(title)
    if heading and expected and heading == expected:
        return body[match.end() :].lstrip()
    return body


def build_markdown_document(result: ExtractionResult, markdown_body: str) -> str:
    title = result.title or "未命名文章"
    body = remove_leading_duplicate_title(markdown_body, title)
    lines = [
        f"原文链接：{result.final_url}",
        "",
        f"# {title}",
        f"抓取时间（UTC）：{result.fetched_at_utc}",
        "",
        "---",
        "",
    ]
    if body:
        lines.append(body)
    return "\n".join(lines).rstrip() + "\n"


async def wait_for_article_ready(page: Any, selectors: list[str], args: argparse.Namespace) -> None:
    if args.wait_selector:
        await page.wait_for_selector(args.wait_selector, timeout=args.timeout_ms)
        return

    last_error: Exception | None = None
    per_selector_timeout = min(5000, args.timeout_ms)
    for selector in selectors:
        try:
            await page.wait_for_selector(selector, timeout=per_selector_timeout)
            return
        except Exception as exc:  # noqa: BLE001
            last_error = exc

    if last_error is not None:
        print(
            f"[warn] 未匹配到正文选择器，继续使用 body 兜底: error={type(last_error).__name__}: {last_error}",
            file=sys.stderr,
        )


async def auto_scroll(page: Any, rounds: int, step: int, wait_ms: int) -> None:
    if rounds <= 0:
        return
    for _ in range(rounds):
        await page.mouse.wheel(0, step)
        await page.wait_for_timeout(wait_ms)


async def extract_page(
    page: Any,
    url: str,
    selectors: list[str],
    args: argparse.Namespace,
) -> ExtractionResult:
    response = await page.goto(url, wait_until="domcontentloaded", timeout=args.timeout_ms)
    if response is not None and int(response.status) >= 400:
        raise RuntimeError(f"页面响应异常: status={response.status}, url={url}")

    await wait_for_article_ready(page, selectors, args)
    await auto_scroll(page, args.scroll_rounds, args.scroll_step, args.scroll_wait_ms)

    payload = await page.evaluate(EXTRACT_SCRIPT, {"selectors": selectors})
    image_urls = [str(item).strip() for item in (payload.get("imageUrls") or []) if str(item).strip()]
    deduped_image_urls = list(dict.fromkeys(image_urls))

    return ExtractionResult(
        requested_url=url,
        final_url=str(payload.get("pageUrl") or page.url or url),
        title=str(payload.get("title") or "").strip(),
        root_selector=str(payload.get("rootSelector") or "").strip(),
        article_html=str(payload.get("articleHtml") or ""),
        text=str(payload.get("text") or "").strip(),
        image_urls=deduped_image_urls,
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
) -> tuple[list[ImageDownloadResult], dict[str, str]]:
    save_dir.mkdir(parents=True, exist_ok=True)
    selected = image_urls if max_images == 0 else image_urls[:max_images]
    results: list[ImageDownloadResult] = []
    image_path_map: dict[str, str] = {}

    for index, image_url in enumerate(selected, start=1):
        try:
            response = await context.request.get(
                image_url,
                timeout=timeout_ms,
                headers={"Referer": referer},
            )
            status = int(response.status)
            if not response.ok:
                results.append(
                    ImageDownloadResult(
                        source_url=image_url,
                        ok=False,
                        status=status,
                        local_path=None,
                        relative_path=None,
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

            relative_path = Path("images") / filename
            image_path_map[image_url] = relative_path.as_posix()
            results.append(
                ImageDownloadResult(
                    source_url=image_url,
                    ok=True,
                    status=status,
                    local_path=str(output_path),
                    relative_path=relative_path.as_posix(),
                    byte_size=len(body),
                    error=None,
                )
            )
        except Exception as exc:  # noqa: BLE001
            results.append(
                ImageDownloadResult(
                    source_url=image_url,
                    ok=False,
                    status=None,
                    local_path=None,
                    relative_path=None,
                    byte_size=0,
                    error=f"{type(exc).__name__}: {exc}",
                )
            )

    return results, image_path_map


def build_output_dir(base_output_dir: Path, result: ExtractionResult) -> Path:
    host = (urlparse(result.final_url).hostname or "site").replace(".", "_")
    fallback_slug = (urlparse(result.final_url).path.rstrip("/").split("/")[-1] or "article").strip()
    title_slug = slugify(result.title, fallback=slugify(fallback_slug, fallback="article"))
    directory = base_output_dir / f"{host}_{title_slug}"
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def write_outputs(
    *,
    output_dir: Path,
    extraction: ExtractionResult,
    markdown: str,
    image_downloads: list[ImageDownloadResult],
    output_name: str,
    save_html: bool,
) -> dict[str, Any]:
    markdown_path = output_dir / output_name
    markdown_path.write_text(markdown, encoding="utf-8")

    raw_text_path = output_dir / "content.txt"
    raw_text_path.write_text(extraction.text, encoding="utf-8")

    if save_html:
        html_path = output_dir / "article.html"
        html_path.write_text(extraction.article_html, encoding="utf-8")

    metadata = {
        "requested_url": extraction.requested_url,
        "final_url": extraction.final_url,
        "title": extraction.title,
        "root_selector": extraction.root_selector,
        "fetched_at_utc": extraction.fetched_at_utc,
        "text_chars": len(extraction.text),
        "image_count": len(extraction.image_urls),
        "markdown_path": str(markdown_path),
        "raw_text_path": str(raw_text_path),
        "downloaded_images": [asdict(item) for item in image_downloads],
    }
    metadata_path = output_dir / "result.json"
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
    return metadata


async def run(args: argparse.Namespace) -> int:
    async_playwright = ensure_playwright()
    selectors = split_selectors(args.article_selectors)

    batch_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_root = args.out_dir / f"extract_{batch_id}"
    output_root.mkdir(parents=True, exist_ok=True)

    context_kwargs: dict[str, Any] = {}
    if args.user_agent:
        context_kwargs["user_agent"] = args.user_agent

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=not args.headed,
            slow_mo=args.slow_mo_ms if args.slow_mo_ms > 0 else None,
        )
        context = await browser.new_context(**context_kwargs)
        page = await context.new_page()
        output_dir: Path | None = None
        extraction: ExtractionResult | None = None
        image_downloads: list[ImageDownloadResult] = []
        metadata: dict[str, Any] = {}

        try:
            print(f"[info] 开始抓取: {args.url}")
            extraction = await extract_page(page, args.url, selectors, args)
            output_dir = build_output_dir(output_root, extraction)
            image_downloads, image_path_map = await download_images(
                context=context,
                image_urls=extraction.image_urls,
                referer=extraction.final_url,
                save_dir=output_dir / "images",
                timeout_ms=args.timeout_ms,
                max_images=args.max_images,
            )

            markdown_body = html_to_markdown(
                extraction.article_html,
                extraction.final_url,
                image_path_map=image_path_map,
            )
            markdown = build_markdown_document(extraction, markdown_body)
            metadata = write_outputs(
                output_dir=output_dir,
                extraction=extraction,
                markdown=markdown,
                image_downloads=image_downloads,
                output_name=args.output_name,
                save_html=args.save_html,
            )
        finally:
            await context.close()
            await browser.close()

    summary = {
        "output_root": str(output_root),
        "output_dir": str(output_dir) if output_dir else "",
        "markdown_path": metadata.get("markdown_path"),
        "image_total": len(extraction.image_urls) if extraction else 0,
        "image_downloaded_ok": sum(1 for item in image_downloads if item.ok),
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
