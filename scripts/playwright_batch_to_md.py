from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import mimetypes
import os
import socket
import re
import subprocess
import sys
import time
from contextlib import contextmanager
from functools import lru_cache
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup, NavigableString, Tag

try:
    import httpx
except Exception:  # noqa: BLE001
    httpx = None  # type: ignore[assignment]


SITE_SELECTORS: dict[str, list[str]] = {
    "zhihu": [
        "main .AnswerItem .RichContent",
        "main .RichContent",
        "main .Post-RichTextContainer",
        "main",
    ],
    "juejin": [
        "article",
        ".article-content",
        ".markdown-body",
        ".article-viewer",
        "main article",
        "main",
    ],
    "generic": [
        "article",
        "main",
        "body",
    ],
}

SCRIPT_DIR = Path(__file__).resolve().parent
SCRIPT_DEP_DIR = SCRIPT_DIR / "playwright_batch_to_md"

DEFAULT_ZHIHU_STORAGE_STATE = Path("var/zhihu_storage_state.json")
DEFAULT_ZHIHU_SIGN_JS = SCRIPT_DEP_DIR / "zhihu_sign.js"
DEFAULT_ZHIHU_STEALTH_JS = SCRIPT_DEP_DIR / "stealth.min.js"
DEFAULT_ZHIHU_USER_DATA_DIR = Path("var/zhihu_user_data")
DEFAULT_ZHIHU_CDP_USER_DATA_DIR = Path("var/zhihu_user_data_cdp")
DEFAULT_ZHIHU_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/128.0.0.0 Safari/537.36"
)
DEFAULT_ZHIHU_CDP_DEBUG_PORT = 9222
DEFAULT_ZHIHU_CDP_LAUNCH_TIMEOUT_SECONDS = 60
DEFAULT_ZHIHU_REQUEST_INTERVAL_SEC = 2.0
DEFAULT_MINDSPIDER_MEDIA_CRAWLER_DIR = Path("BettaFish/MindSpider/DeepSentimentCrawling/MediaCrawler")


def detect_system_chrome_paths() -> list[str]:
    if os.name == "nt":
        candidates = [
            os.path.expandvars(r"%PROGRAMFILES%\Google\Chrome\Application\chrome.exe"),
            os.path.expandvars(r"%PROGRAMFILES(X86)%\Google\Chrome\Application\chrome.exe"),
            os.path.expandvars(r"%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe"),
            os.path.expandvars(r"%PROGRAMFILES%\Microsoft\Edge\Application\msedge.exe"),
            os.path.expandvars(r"%PROGRAMFILES(X86)%\Microsoft\Edge\Application\msedge.exe"),
        ]
    elif sys.platform == "darwin":
        candidates = [
            "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
            "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
        ]
    else:
        candidates = [
            "/usr/bin/google-chrome",
            "/usr/bin/google-chrome-stable",
            "/usr/bin/chromium-browser",
            "/usr/bin/chromium",
            "/usr/bin/microsoft-edge",
        ]
    return [path for path in candidates if path and os.path.isfile(path) and os.access(path, os.X_OK)]


def detect_zhihu_cdp_browser_path(explicit_path: str) -> str:
    raw = str(explicit_path or "").strip()
    if raw:
        path = os.path.expandvars(raw)
        if os.path.isfile(path):
            return path
        raise RuntimeError(f"zhihu cdp browser not found: {raw}")
    candidates = detect_system_chrome_paths()
    if candidates:
        return candidates[0]
    raise RuntimeError("No Chrome/Edge executable found for zhihu cdp mode.")


def find_available_local_port(start_port: int, max_tries: int = 100) -> int:
    begin = max(int(start_port), 1)
    end = begin + max(int(max_tries), 1)
    for port in range(begin, end):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind(("127.0.0.1", port))
            except OSError:
                continue
            return port
    raise RuntimeError(f"No free local port found in range [{begin}, {end})")


async def wait_until_tcp_port_ready(host: str, port: int, timeout_seconds: int) -> bool:
    timeout = max(int(timeout_seconds), 1)
    deadline = time.monotonic() + float(timeout)
    while time.monotonic() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(1.0)
            if sock.connect_ex((host, int(port))) == 0:
                return True
        await asyncio.sleep(0.5)
    return False


def build_zhihu_cdp_launch_args(
    *,
    browser_path: str,
    debug_port: int,
    headless: bool,
    user_data_dir: Path,
) -> list[str]:
    args: list[str] = [
        browser_path,
        f"--remote-debugging-port={int(debug_port)}",
        "--remote-debugging-address=127.0.0.1",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-background-timer-throttling",
        "--disable-backgrounding-occluded-windows",
        "--disable-renderer-backgrounding",
        "--disable-features=TranslateUI",
        "--disable-ipc-flooding-protection",
        "--disable-hang-monitor",
        "--disable-prompt-on-repost",
        "--disable-sync",
        "--disable-dev-shm-usage",
        "--no-sandbox",
        "--disable-blink-features=AutomationControlled",
        "--exclude-switches=enable-automation",
        "--disable-infobars",
        f"--user-data-dir={str(user_data_dir)}",
    ]
    if headless:
        args.extend(["--headless=new", "--disable-gpu"])
    else:
        args.append("--start-maximized")
    return args


def cleanup_chrome_singleton_files(user_data_dir: Path) -> None:
    singleton_names = (
        "SingletonCookie",
        "SingletonLock",
        "SingletonSocket",
    )
    for name in singleton_names:
        target = user_data_dir / name
        try:
            if target.exists():
                target.unlink()
        except Exception:
            pass


def terminate_process(process: Any) -> None:
    if process is None:
        return
    try:
        if process.poll() is not None:
            return
    except Exception:
        return
    try:
        process.terminate()
        process.wait(timeout=5)
    except Exception:
        try:
            process.kill()
            process.wait(timeout=5)
        except Exception:
            pass


async def launch_zhihu_cdp_context(
    *,
    playwright: Any,
    context_kwargs: dict[str, Any],
    args: argparse.Namespace,
) -> tuple[Any, Any, Any, int]:
    browser_path = detect_zhihu_cdp_browser_path(str(args.zhihu_cdp_browser_path or "").strip())
    debug_port = find_available_local_port(int(args.zhihu_cdp_debug_port))
    cdp_user_data_raw = str(args.zhihu_cdp_user_data_dir or "").strip()
    if cdp_user_data_raw:
        user_data_dir = Path(cdp_user_data_raw)
    else:
        user_data_dir = Path(args.zhihu_user_data_dir).with_name(f"{Path(args.zhihu_user_data_dir).name}_cdp")
    user_data_dir = user_data_dir.resolve()
    user_data_dir.mkdir(parents=True, exist_ok=True)
    cleanup_chrome_singleton_files(user_data_dir)
    launch_args = build_zhihu_cdp_launch_args(
        browser_path=browser_path,
        debug_port=debug_port,
        headless=not bool(args.headed),
        user_data_dir=user_data_dir,
    )

    popen_kwargs: dict[str, Any] = {
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
    }
    if os.name == "nt":
        popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        popen_kwargs["start_new_session"] = True
    process = subprocess.Popen(launch_args, **popen_kwargs)
    try:
        ready = await wait_until_tcp_port_ready(
            "127.0.0.1",
            debug_port,
            int(args.zhihu_cdp_launch_timeout_seconds),
        )
        if not ready:
            raise RuntimeError(
                f"zhihu cdp browser launch timed out: port={debug_port}, timeout={args.zhihu_cdp_launch_timeout_seconds}s"
            )
        browser = await playwright.chromium.connect_over_cdp(f"http://127.0.0.1:{debug_port}")
        if browser.contexts:
            context = browser.contexts[0]
        else:
            new_context_kwargs: dict[str, Any] = {"accept_downloads": True}
            viewport = context_kwargs.get("viewport")
            user_agent = context_kwargs.get("user_agent")
            if viewport:
                new_context_kwargs["viewport"] = viewport
            if user_agent:
                new_context_kwargs["user_agent"] = user_agent
            context = await browser.new_context(**new_context_kwargs)
        return context, browser, process, debug_port
    except Exception:
        terminate_process(process)
        raise


async def launch_standard_browser_context(
    *,
    playwright: Any,
    context_kwargs: dict[str, Any],
    args: argparse.Namespace,
    use_zhihu_persistent_context: bool,
) -> tuple[Any, Any]:
    browser = None
    if use_zhihu_persistent_context:
        user_data_dir = Path(args.zhihu_user_data_dir)
        user_data_dir.mkdir(parents=True, exist_ok=True)
        persistent_kwargs: dict[str, Any] = {
            "user_data_dir": str(user_data_dir),
            "accept_downloads": True,
            "headless": not args.headed,
            "viewport": context_kwargs.get("viewport"),
            "user_agent": context_kwargs.get("user_agent"),
            "slow_mo": args.slow_mo_ms if args.slow_mo_ms > 0 else None,
            "channel": "chrome",
        }
        try:
            context = await playwright.chromium.launch_persistent_context(**persistent_kwargs)
        except Exception as exc:  # noqa: BLE001
            persistent_kwargs.pop("channel", None)
            print(
                f"[warn] launch_persistent_context with channel=chrome failed, fallback default channel: {type(exc).__name__}: {exc}",
                file=sys.stderr,
            )
            context = await playwright.chromium.launch_persistent_context(**persistent_kwargs)
        return context, browser
    browser = await playwright.chromium.launch(
        headless=not args.headed,
        slow_mo=args.slow_mo_ms if args.slow_mo_ms > 0 else None,
    )
    context = await browser.new_context(**context_kwargs)
    return context, browser

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
      const token = part.trim().split(/\s+/)[0];
      const abs = toAbs(token);
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
      if (node) return { node, selector };
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
    for (const item of candidates) {
      const abs = toAbs(item);
      if (abs) return abs;
    }
    return fromSrcset(node.getAttribute("srcset") || node.srcset || "");
  };

  const picked = pickRoot();
  const root = picked.node;
  const seen = new Set();
  const imageUrls = [];

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
    site_type: str
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


@dataclass(frozen=True)
class MindSpiderZhihuBridge:
    media_crawler_dir: Path
    zhihu_client_cls: Any
    data_fetch_error_cls: type[Exception]
    forbidden_error_cls: type[Exception]


def resolve_mindspider_media_crawler_dir(raw_dir: Path | str | None) -> Path:
    raw_text = str(raw_dir or "").strip()
    candidate = Path(raw_text) if raw_text else DEFAULT_MINDSPIDER_MEDIA_CRAWLER_DIR
    candidate = candidate.expanduser()
    if not candidate.is_absolute():
        cwd_candidate = (Path.cwd() / candidate).resolve()
        if cwd_candidate.exists():
            candidate = cwd_candidate
        else:
            candidate = (SCRIPT_DIR.parent / candidate).resolve()
    else:
        candidate = candidate.resolve()
    if not candidate.exists():
        raise RuntimeError(f"MindSpider MediaCrawler directory not found: {candidate}")
    required_paths = [
        candidate / "main.py",
        candidate / "libs" / "zhihu.js",
        candidate / "media_platform" / "zhihu" / "client.py",
    ]
    missing = [str(item) for item in required_paths if not item.exists()]
    if missing:
        raise RuntimeError(
            "MindSpider MediaCrawler directory is incomplete. Missing: "
            + ", ".join(missing)
        )
    return candidate


def ensure_sys_path_front(path: Path) -> None:
    path_text = str(path)
    if path_text in sys.path:
        sys.path.remove(path_text)
    sys.path.insert(0, path_text)


@contextmanager
def pushd(path: Path):
    previous = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


@lru_cache(maxsize=2)
def load_mindspider_zhihu_bridge(media_crawler_dir_text: str) -> MindSpiderZhihuBridge:
    media_crawler_dir = resolve_mindspider_media_crawler_dir(media_crawler_dir_text)
    ensure_sys_path_front(media_crawler_dir)
    try:
        zhihu_client_module = importlib.import_module("media_platform.zhihu.client")
        zhihu_exception_module = importlib.import_module("media_platform.zhihu.exception")
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            "Failed to import MindSpider zhihu modules. "
            "Please install dependencies from "
            "BettaFish/MindSpider/DeepSentimentCrawling/MediaCrawler/requirements.txt"
        ) from exc

    zhihu_client_cls = getattr(zhihu_client_module, "ZhiHuClient", None)
    data_fetch_error_cls = getattr(zhihu_exception_module, "DataFetchError", None)
    forbidden_error_cls = getattr(zhihu_exception_module, "ForbiddenError", None)
    if zhihu_client_cls is None or data_fetch_error_cls is None or forbidden_error_cls is None:
        raise RuntimeError(
            "MindSpider zhihu bridge load failed: required classes "
            "ZhiHuClient/DataFetchError/ForbiddenError not found."
        )
    return MindSpiderZhihuBridge(
        media_crawler_dir=media_crawler_dir,
        zhihu_client_cls=zhihu_client_cls,
        data_fetch_error_cls=data_fetch_error_cls,
        forbidden_error_cls=forbidden_error_cls,
    )


def build_mindspider_zhihu_default_headers(cookie_header: str, user_agent: str) -> dict[str, str]:
    return {
        "accept": "*/*",
        "accept-language": "zh-CN,zh;q=0.9",
        "cookie": cookie_header,
        "priority": "u=1, i",
        "referer": "https://www.zhihu.com/search?q=python&time_interval=a_year&type=content",
        "user-agent": user_agent,
        "x-api-version": "3.0.91",
        "x-app-za": "OS=Web",
        "x-requested-with": "fetch",
        "x-zse-93": "101_3_3.0",
    }


def build_mindspider_zhihu_client(
    *,
    bridge: MindSpiderZhihuBridge,
    page: Any,
    cookie_header: str,
    cookie_dict: dict[str, str],
    user_agent: str,
    httpx_proxy: str | None,
) -> Any:
    headers = build_mindspider_zhihu_default_headers(cookie_header, user_agent)
    return bridge.zhihu_client_cls(
        proxy=httpx_proxy,
        headers=headers,
        playwright_page=page,
        cookie_dict=cookie_dict,
        proxy_ip_pool=None,
    )


def unwrap_retry_exception(exc: Exception) -> Exception:
    nested: Exception = exc
    last_attempt = getattr(exc, "last_attempt", None)
    if last_attempt is None:
        return nested
    try:
        candidate = last_attempt.exception()
    except Exception:
        return nested
    if isinstance(candidate, Exception):
        return candidate
    return nested


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


def ensure_httpx() -> Any:
    if httpx is None:
        raise RuntimeError("Zhihu extraction requires httpx. Install with: pip install httpx")
    return httpx


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="使用 Playwright 批量抓取文章并导出 Markdown（支持登录态复用）。",
    )
    parser.add_argument("urls", nargs="*", help="待抓取 URL，可传多个。")
    parser.add_argument("--urls-file", type=Path, help="URL 文件路径（每行一个 URL）。")
    parser.add_argument(
        "--site",
        choices=["auto", "zhihu", "juejin", "generic"],
        default="auto",
        help="站点类型。auto 会根据域名推断。",
    )
    parser.add_argument("--out-dir", type=Path, default=Path("var/playwright_md"), help="输出根目录。")
    parser.add_argument("--output-name", default="article.md", help="Markdown 文件名。")
    parser.add_argument("--save-html", action="store_true", help="额外保存正文 HTML。")
    parser.add_argument("--save-images", dest="save_images", action="store_true", help="下载图片到本地。")
    parser.add_argument("--no-save-images", dest="save_images", action="store_false", help="不下载图片。")
    parser.add_argument(
        "--in-memory-output",
        action="store_true",
        help="仅通过 stdout 输出提取结果，不写入 Markdown/图片/失败文件到磁盘。",
    )
    parser.add_argument("--max-images", type=int, default=0, help="最多下载图片数，0 表示不限制。")
    parser.add_argument("--timeout-ms", type=int, default=60000, help="页面与资源请求超时（毫秒）。")
    parser.add_argument("--scroll-rounds", type=int, default=6, help="自动滚动次数。")
    parser.add_argument("--scroll-step", type=int, default=1800, help="每次滚动像素。")
    parser.add_argument("--scroll-wait-ms", type=int, default=1200, help="每次滚动后等待毫秒。")
    parser.add_argument("--wait-selector", default="", help="额外等待的 CSS 选择器。")
    parser.add_argument("--headed", dest="headed", action="store_true", help="使用有头浏览器。")
    parser.add_argument("--headless", dest="headed", action="store_false", help="使用无头浏览器。")
    parser.add_argument("--slow-mo-ms", type=int, default=0, help="浏览器慢放毫秒。")
    parser.add_argument("--user-agent", default="", help="自定义 User-Agent。")
    parser.add_argument("--storage-state", type=Path, help="已保存登录态 JSON 路径。")
    parser.add_argument("--save-storage-state", type=Path, help="保存当前登录态到 JSON。")
    parser.add_argument("--pause-for-login", action="store_true", help="打开首个页面后暂停，手动登录再继续。")
    parser.add_argument(
        "--auto-login-fallback",
        dest="auto_login_fallback",
        action="store_true",
        help="先尝试复用登录态抓取；若遇到 401/403 则触发手动登录等待并重试。",
    )
    parser.add_argument(
        "--no-auto-login-fallback",
        dest="auto_login_fallback",
        action="store_false",
        help="禁用 401/403 后的手动登录兜底。",
    )
    parser.add_argument(
        "--manual-login-wait-seconds",
        type=int,
        default=600,
        help="触发手动登录回退时，浏览器停留等待秒数。",
    )
    parser.add_argument(
        "--zhihu-sign-js",
        type=Path,
        default=DEFAULT_ZHIHU_SIGN_JS,
        help="知乎签名算法 js 文件路径（复刻 MindSpider 的签名抓取链路）。",
    )
    parser.add_argument(
        "--zhihu-stealth-js",
        type=Path,
        default=DEFAULT_ZHIHU_STEALTH_JS,
        help="知乎反检测 init script 路径（可选，存在时自动注入）。",
    )
    parser.add_argument(
        "--zhihu-user-data-dir",
        type=Path,
        default=DEFAULT_ZHIHU_USER_DATA_DIR,
        help="知乎持久化浏览器目录（复刻 MindSpider 的 persistent context）。",
    )
    parser.add_argument(
        "--zhihu-cdp-user-data-dir",
        type=Path,
        default=DEFAULT_ZHIHU_CDP_USER_DATA_DIR,
        help="知乎 CDP 浏览器数据目录（建议与 persistent context 目录分离）。",
    )
    parser.add_argument(
        "--zhihu-cdp-mode",
        dest="zhihu_cdp_mode",
        action="store_true",
        help="启用知乎 CDP 模式（对齐 MindSpider 默认配置）。",
    )
    parser.add_argument(
        "--disable-zhihu-cdp-mode",
        dest="zhihu_cdp_mode",
        action="store_false",
        help="禁用知乎 CDP 模式。",
    )
    parser.add_argument(
        "--zhihu-cdp-debug-port",
        type=int,
        default=DEFAULT_ZHIHU_CDP_DEBUG_PORT,
        help="知乎 CDP 调试端口（若被占用会自动顺延）。",
    )
    parser.add_argument(
        "--zhihu-cdp-browser-path",
        default="",
        help="知乎 CDP 自定义浏览器可执行路径（Chrome/Edge）。",
    )
    parser.add_argument(
        "--zhihu-cdp-launch-timeout-seconds",
        type=int,
        default=DEFAULT_ZHIHU_CDP_LAUNCH_TIMEOUT_SECONDS,
        help="知乎 CDP 浏览器启动超时秒数。",
    )
    parser.add_argument(
        "--disable-zhihu-persistent-context",
        action="store_true",
        help="禁用知乎 persistent context，退回临时 context + storage_state。",
    )
    parser.add_argument(
        "--zhihu-httpx-proxy",
        default="",
        help="知乎 signed 请求使用的 httpx 代理地址（可选）。",
    )
    parser.add_argument(
        "--zhihu-mindspider-dir",
        type=Path,
        default=DEFAULT_MINDSPIDER_MEDIA_CRAWLER_DIR,
        help="MindSpider MediaCrawler directory for zhihu extraction bridge.",
    )
    parser.add_argument(
        "--zhihu-prime-wait-ms",
        type=int,
        default=5000,
        help="知乎预热搜索页后的等待毫秒数，用于稳定 cookie。",
    )
    parser.add_argument(
        "--zhihu-request-interval-sec",
        type=float,
        default=DEFAULT_ZHIHU_REQUEST_INTERVAL_SEC,
        help="知乎 URL 之间的固定间隔秒数（对齐 MindSpider 默认节流）。",
    )
    parser.set_defaults(
        headed=True,
        auto_login_fallback=True,
        zhihu_cdp_mode=True,
        save_images=True,
    )
    args = parser.parse_args()

    if args.pause_for_login and not args.headed:
        parser.error("--pause-for-login 需要搭配 --headed。")
    if args.auto_login_fallback and not args.headed:
        parser.error("--auto-login-fallback 需要搭配 --headed，便于手动登录。")
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
    if args.manual_login_wait_seconds < 0:
        parser.error("--manual-login-wait-seconds 必须大于 0。")
    if args.zhihu_prime_wait_ms < 0:
        parser.error("--zhihu-prime-wait-ms 不能小于 0。")
    if args.zhihu_request_interval_sec < 0:
        parser.error("--zhihu-request-interval-sec 不能小于 0。")
    if args.zhihu_cdp_debug_port <= 0:
        parser.error("--zhihu-cdp-debug-port 必须大于 0。")
    if args.zhihu_cdp_launch_timeout_seconds <= 0:
        parser.error("--zhihu-cdp-launch-timeout-seconds 必须大于 0。")
    if not str(args.output_name).lower().endswith(".md"):
        parser.error("--output-name 必须以 .md 结尾。")
    return args


def read_urls(args: argparse.Namespace) -> list[str]:
    urls: list[str] = []
    if args.urls_file:
        if not args.urls_file.exists():
            raise RuntimeError(f"urls file not found: {args.urls_file}")
        for line in args.urls_file.read_text(encoding="utf-8").splitlines():
            item = line.strip().lstrip("\ufeff")
            if not item or item.startswith("#"):
                continue
            urls.append(item)
    urls.extend([str(item).strip() for item in args.urls if str(item).strip()])
    urls = [item for item in dict.fromkeys(urls) if item]
    if not urls:
        raise RuntimeError("未提供 URL。请传入 urls 或 --urls-file。")
    return urls


def infer_site_type(url: str, declared: str) -> str:
    if declared != "auto":
        return declared
    host = (urlparse(url).hostname or "").lower()
    if "zhihu.com" in host:
        return "zhihu"
    if "juejin.cn" in host:
        return "juejin"
    return "generic"


def selectors_for_site(site_type: str, custom_wait_selector: str) -> tuple[list[str], str]:
    selectors = SITE_SELECTORS.get(site_type, SITE_SELECTORS["generic"])
    if custom_wait_selector:
        wait_selector = custom_wait_selector
    else:
        wait_selector = selectors[0] if selectors else "body"
    return selectors, wait_selector


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


@lru_cache(maxsize=4)
def load_zhihu_signer(sign_js_path: str) -> Any:
    try:
        import execjs  # type: ignore
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            "Zhihu signed crawl requires PyExecJS. Install with: pip install PyExecJS"
        ) from exc

    path = Path(sign_js_path)
    if not path.exists():
        raise RuntimeError(f"zhihu sign js not found: {path}")
    source = path.read_text(encoding="utf-8-sig")
    return execjs.compile(source)


def cookies_to_header(cookies: list[dict[str, Any]]) -> tuple[str, dict[str, str]]:
    parts: list[str] = []
    cookie_dict: dict[str, str] = {}
    for item in cookies:
        name = str(item.get("name") or "").strip()
        value = str(item.get("value") or "")
        if not name:
            continue
        parts.append(f"{name}={value}")
        cookie_dict[name] = value
    return ";".join(parts), cookie_dict


def build_zhihu_signed_headers(
    *,
    request_uri: str,
    cookie_header: str,
    user_agent: str,
    sign_js_path: Path,
) -> dict[str, str]:
    signer = load_zhihu_signer(str(sign_js_path.resolve()))
    sign_res = signer.call("get_sign", request_uri, cookie_header)
    x_zst_81 = str((sign_res or {}).get("x-zst-81") or "")
    x_zse_96 = str((sign_res or {}).get("x-zse-96") or "")
    if not x_zst_81 or not x_zse_96:
        raise RuntimeError("zhihu sign failed: empty x-zst-81 or x-zse-96")
    return {
        "accept": "*/*",
        "accept-language": "zh-CN,zh;q=0.9",
        "cookie": cookie_header,
        "priority": "u=1, i",
        "referer": "https://www.zhihu.com/search?q=python&time_interval=a_year&type=content",
        "user-agent": user_agent,
        "x-api-version": "3.0.91",
        "x-app-za": "OS=Web",
        "x-requested-with": "fetch",
        "x-zse-93": "101_3_3.0",
        "x-zst-81": x_zst_81,
        "x-zse-96": x_zse_96,
    }


def load_storage_state_cookies(storage_state_path: Path) -> list[dict[str, Any]]:
    if not storage_state_path.exists():
        return []
    try:
        payload = json.loads(storage_state_path.read_text(encoding="utf-8-sig"))
    except Exception:  # noqa: BLE001
        return []
    cookies = payload.get("cookies") if isinstance(payload, dict) else None
    if not isinstance(cookies, list):
        return []
    valid: list[dict[str, Any]] = []
    for item in cookies:
        if not isinstance(item, dict):
            continue
        if not str(item.get("name") or "").strip():
            continue
        if not str(item.get("domain") or "").strip():
            continue
        valid.append(item)
    return valid


async def apply_storage_state_cookies(context: Any, storage_state_path: Path) -> int:
    cookies = load_storage_state_cookies(storage_state_path)
    if not cookies:
        return 0
    await context.add_cookies(cookies)
    return len(cookies)


async def httpx_get_text(
    *,
    url: str,
    timeout_ms: int,
    headers: dict[str, str],
    proxy: str | None = None,
    follow_redirects: bool = False,
) -> tuple[int, str, str, dict[str, str]]:
    httpx_mod = ensure_httpx()
    timeout_seconds = max(float(timeout_ms) / 1000.0, 1.0)
    async with httpx_mod.AsyncClient(
        proxy=proxy,
        timeout=timeout_seconds,
        follow_redirects=follow_redirects,
    ) as client:
        response = await client.get(url, headers=headers)
    return int(response.status_code), response.text, str(response.url), dict(response.headers)


def html_to_plain_text(html_text: str) -> str:
    soup = BeautifulSoup(str(html_text or ""), "html.parser")
    text = soup.get_text("\n", strip=True)
    return re.sub(r"\n{3,}", "\n\n", text).strip()


def normalize_possible_url(value: str, base_url: str) -> str:
    raw = str(value or "").strip()
    if not raw or raw.startswith("data:"):
        return ""
    return abs_url(raw, base_url)


def parse_style_image_urls(style_text: str, base_url: str) -> list[str]:
    urls: list[str] = []
    for item in re.findall(r"url\(([^)]+)\)", str(style_text or ""), flags=re.IGNORECASE):
        candidate = item.strip().strip("\"' ")
        parsed = normalize_possible_url(candidate, base_url)
        if parsed:
            urls.append(parsed)
    return urls


def is_likely_image_url(url: str) -> bool:
    value = str(url or "").strip().lower()
    if not value.startswith(("http://", "https://")):
        return False
    host = (urlparse(value).hostname or "").lower()
    if host.endswith("zhimg.com"):
        return True
    path = (urlparse(value).path or "").lower()
    return any(path.endswith(ext) for ext in [".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".svg", ".avif"])


def collect_image_urls_from_struct(data: Any, base_url: str) -> list[str]:
    include_key_parts = ("image", "thumbnail", "cover", "pic", "origin")
    exclude_key_parts = ("avatar", "author", "badge")
    found: list[str] = []

    def walk(node: Any, parent_key: str) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                key_text = str(key or "").lower()
                walk(value, key_text)
            return
        if isinstance(node, list):
            for item in node:
                walk(item, parent_key)
            return
        if not isinstance(node, str):
            return
        candidate = normalize_possible_url(node, base_url)
        if not candidate:
            return
        if not is_likely_image_url(candidate):
            return
        key_text = str(parent_key or "").lower()
        if any(item in key_text for item in exclude_key_parts):
            return
        if any(item in key_text for item in include_key_parts) or "zhimg.com" in candidate.lower():
            found.append(candidate)

    walk(data, "")
    return list(dict.fromkeys(found))


def collect_image_urls_from_html(article_html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(str(article_html or ""), "html.parser")
    urls: list[str] = []
    for tag in soup.find_all(["img", "source"]):
        if not isinstance(tag, Tag):
            continue
        src = pick_image_src(tag, base_url)
        if src:
            urls.append(src)
        for style_url in parse_style_image_urls(str(tag.get("style") or ""), base_url):
            urls.append(style_url)
    for tag in soup.find_all(attrs={"style": True}):
        if not isinstance(tag, Tag):
            continue
        for style_url in parse_style_image_urls(str(tag.get("style") or ""), base_url):
            urls.append(style_url)
    return list(dict.fromkeys(urls))


def parse_zhihu_initial_data(html_text: str) -> dict[str, Any]:
    soup = BeautifulSoup(str(html_text or ""), "html.parser")
    script = soup.find("script", attrs={"id": "js-initialData"})
    if script is None:
        raise RuntimeError("Zhihu initial data script not found.")
    raw = script.get_text("", strip=True)
    if not raw:
        raise RuntimeError("Zhihu initial data script is empty.")
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Zhihu initial data JSON decode failed: {exc}") from exc


def first_entity_item(entities: dict[str, Any], entity_name: str) -> dict[str, Any] | None:
    items = entities.get(entity_name)
    if not isinstance(items, dict):
        return None
    for value in items.values():
        if isinstance(value, dict):
            return value
    return None


def extract_zhihu_content_from_initial_data(
    initial_data: dict[str, Any], target_url: str
) -> tuple[str, str, str, dict[str, Any] | None]:
    state = initial_data.get("initialState")
    if not isinstance(state, dict):
        raise RuntimeError("Zhihu initialState not found.")
    entities = state.get("entities")
    if not isinstance(entities, dict):
        raise RuntimeError("Zhihu entities not found.")

    path = urlparse(target_url).path
    answer_match = re.search(r"/question/(\d+)/answer/(\d+)", path)
    article_match = re.search(r"/p/(\d+)", path)
    video_match = re.search(r"/zvideo/(\d+)", path)

    if answer_match:
        question_id, answer_id = answer_match.groups()
        answers = entities.get("answers")
        answer_obj: dict[str, Any] | None = None
        if isinstance(answers, dict):
            maybe = answers.get(answer_id)
            if isinstance(maybe, dict):
                answer_obj = maybe
        if answer_obj is None:
            answer_obj = first_entity_item(entities, "answers")
        if not answer_obj:
            raise RuntimeError("Zhihu answer entity not found in initialState.")
        title = str(answer_obj.get("title") or "").strip()
        if not title:
            q_inline = answer_obj.get("question")
            if isinstance(q_inline, dict):
                title = str(q_inline.get("title") or "").strip()
                question_id = str(q_inline.get("id") or question_id)
            questions = entities.get("questions")
            q_obj: dict[str, Any] | None = None
            if isinstance(questions, dict):
                maybe_q = questions.get(question_id)
                if isinstance(maybe_q, dict):
                    q_obj = maybe_q
            if q_obj:
                title = str(q_obj.get("title") or "").strip()
        article_html = str(answer_obj.get("content") or "")
        return title, article_html, "zhihu.initialState.entities.answers", answer_obj

    if article_match:
        article_id = article_match.group(1)
        articles = entities.get("articles")
        article_obj: dict[str, Any] | None = None
        if isinstance(articles, dict):
            maybe = articles.get(article_id)
            if isinstance(maybe, dict):
                article_obj = maybe
        if article_obj is None:
            article_obj = first_entity_item(entities, "articles")
        if not article_obj:
            raise RuntimeError("Zhihu article entity not found in initialState.")
        title = str(article_obj.get("title") or "").strip()
        article_html = str(article_obj.get("content") or "")
        return title, article_html, "zhihu.initialState.entities.articles", article_obj

    if video_match:
        videos = entities.get("zvideos")
        video_obj: dict[str, Any] | None = None
        if isinstance(videos, dict):
            maybe = videos.get(video_match.group(1))
            if isinstance(maybe, dict):
                video_obj = maybe
        if video_obj is None:
            video_obj = first_entity_item(entities, "zvideos")
        if not video_obj:
            raise RuntimeError("Zhihu video entity not found in initialState.")
        title = str(video_obj.get("title") or "").strip()
        desc = str(video_obj.get("description") or "").strip()
        article_html = f"<p>{desc}</p>" if desc else ""
        return title, article_html, "zhihu.initialState.entities.zvideos", video_obj

    for name in ("answers", "articles"):
        obj = first_entity_item(entities, name)
        if obj:
            title = str(obj.get("title") or "").strip()
            article_html = str(obj.get("content") or "")
            if article_html:
                return title, article_html, f"zhihu.initialState.entities.{name}", obj
    raise RuntimeError("No zhihu answer/article content found in initialState.")


def normalize_zhihu_target_url(url: str) -> str:
    parsed = urlparse(url)
    path = parsed.path or ""
    if re.match(r"^/p/\d+", path):
        return urlunparse(("https", "zhuanlan.zhihu.com", path, "", parsed.query, ""))
    return url


async def prime_zhihu_search_cookies(page: Any, timeout_ms: int, wait_ms: int) -> None:
    search_url = (
        "https://www.zhihu.com/search?q=python&search_source=Guess"
        "&utm_content=search_hot&type=content"
    )
    await page.goto(search_url, wait_until="domcontentloaded", timeout=timeout_ms)
    if wait_ms > 0:
        await page.wait_for_timeout(wait_ms)


async def zhihu_pong(
    *,
    cookie_header: str,
    user_agent: str,
    args: argparse.Namespace,
    httpx_proxy: str | None,
) -> bool:
    me_uri = "/api/v4/me?include=email,is_active,is_bind_phone"
    headers = build_zhihu_signed_headers(
        request_uri=me_uri,
        cookie_header=cookie_header,
        user_agent=user_agent,
        sign_js_path=args.zhihu_sign_js,
    )
    me_url = f"https://www.zhihu.com{me_uri}"
    try:
        status, body_text, final_url, response_headers = await httpx_get_text(
            url=me_url,
            timeout_ms=args.timeout_ms,
            headers=headers,
            proxy=httpx_proxy,
        )
        location = str(response_headers.get("location") or "").lower()
        if "/account/unhuman" in final_url.lower() or "/account/unhuman" in location:
            return False
        if status != 200:
            return False
        payload = json.loads(body_text)
        if not isinstance(payload, dict):
            return False
        return bool(payload.get("uid") and payload.get("name"))
    except Exception:
        return False


async def extract_zhihu_content_via_browser_page(
    *,
    page: Any,
    url: str,
    timeout_ms: int,
) -> tuple[str, str, str, str, list[str]] | None:
    try:
        response = await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        if response is not None and int(response.status) >= 400:
            return None
        await page.wait_for_timeout(1200)
        final_url = str(page.url or url)
        final_url_lower = final_url.lower()
        if "/signin" in final_url_lower or "/account/unhuman" in final_url_lower:
            return None
        html_text = await page.content()
        initial_data = parse_zhihu_initial_data(html_text)
        title, article_html, root_selector, entity_obj = extract_zhihu_content_from_initial_data(initial_data, final_url)
        if not article_html.strip():
            return None
        image_urls = collect_image_urls_from_html(article_html, final_url)
        if entity_obj is not None:
            image_urls.extend(collect_image_urls_from_struct(entity_obj, final_url))
        return title, article_html, root_selector, final_url, list(dict.fromkeys(image_urls))
    except Exception:
        return None


async def extract_zhihu_page_signed(
    *,
    page: Any,
    context: Any,
    url: str,
    args: argparse.Namespace,
    httpx_proxy: str | None,
) -> ExtractionResult:
    bridge = load_mindspider_zhihu_bridge(str(args.zhihu_mindspider_dir))
    request_url = normalize_zhihu_target_url(url)
    await page.goto("https://www.zhihu.com/", wait_until="domcontentloaded", timeout=args.timeout_ms)

    cookies = await context.cookies()
    cookie_header, cookie_dict = cookies_to_header(cookies)
    if not cookie_header:
        raise RuntimeError("Zhihu cookies are empty; login state not available.")
    if not cookie_dict.get("d_c0"):
        raise RuntimeError("Zhihu cookie d_c0 is missing; please complete login first.")

    parsed = urlparse(request_url)
    request_uri = parsed.path or "/"
    if parsed.query:
        request_uri += f"?{parsed.query}"
    user_agent = str(args.user_agent or DEFAULT_ZHIHU_USER_AGENT)

    client = build_mindspider_zhihu_client(
        bridge=bridge,
        page=page,
        cookie_header=cookie_header,
        cookie_dict=cookie_dict,
        user_agent=user_agent,
        httpx_proxy=httpx_proxy,
    )

    try:
        with pushd(bridge.media_crawler_dir):
            if not await client.pong():
                raise RuntimeError(f"mindspider page status error: status=401, url={request_url}")
            await prime_zhihu_search_cookies(page, args.timeout_ms, int(args.zhihu_prime_wait_ms))
            await client.update_cookies(browser_context=context)
            html_text = await client.get(request_uri, return_response=True)
    except bridge.forbidden_error_cls as exc:
        raise RuntimeError(f"mindspider page status error: status=403, url={request_url}") from exc
    except bridge.data_fetch_error_cls as exc:
        msg = str(exc).lower()
        if "403" in msg or "forbidden" in msg:
            raise RuntimeError(f"mindspider page status error: status=403, url={request_url}") from exc
        if "401" in msg or "signin" in msg:
            raise RuntimeError(f"mindspider page status error: status=401, url={request_url}") from exc
        raise RuntimeError(f"mindspider page request failed: {type(exc).__name__}: {exc}") from exc
    except Exception as exc:  # noqa: BLE001
        root_exc = unwrap_retry_exception(exc)
        msg = str(root_exc).lower()
        if (
            "account/unhuman" in msg
            or "etimedout" in msg
            or "302 found" in msg
            or "zh-zse-ck" in msg
            or "zse_ck" in msg
            or "forbiddenerror" in msg
            or '"code":403' in msg
            or "authenticationinvalidrequest" in msg
            or '"code":100' in msg
        ):
            raise RuntimeError(f"mindspider page status error: status=403, url={request_url}") from exc
        if "d_c0 not found" in msg or "cookies are empty" in msg or "/signin" in msg:
            raise RuntimeError(f"mindspider page status error: status=401, url={request_url}") from exc
        raise RuntimeError(f"mindspider page request failed: {type(exc).__name__}") from exc

    final_url = request_url
    html_lower = str(html_text or "").lower()
    if "/account/unhuman" in html_lower:
        raise RuntimeError(f"mindspider page status error: status=403, url={request_url}")
    if "/signin" in html_lower:
        raise RuntimeError(f"mindspider page status error: status=401, url={request_url}")

    initial_data = parse_zhihu_initial_data(html_text)
    title, article_html, root_selector, entity_obj = extract_zhihu_content_from_initial_data(
        initial_data,
        final_url,
    )
    image_urls = collect_image_urls_from_html(article_html, final_url)
    if entity_obj is not None:
        image_urls.extend(collect_image_urls_from_struct(entity_obj, final_url))
    image_urls.extend(collect_image_urls_from_struct(initial_data, final_url))
    text = html_to_plain_text(article_html)
    image_urls = list(dict.fromkeys(image_urls))
    return ExtractionResult(
        requested_url=url,
        final_url=final_url,
        site_type="zhihu",
        title=title,
        root_selector=root_selector,
        article_html=article_html,
        text=text,
        image_urls=image_urls,
        fetched_at_utc=now_utc_text(),
    )


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
    for item in [tag.get("src"), tag.get("data-src"), tag.get("data-original"), tag.get("data-actualsrc")]:
        url = abs_url(str(item or ""), base_url)
        if url:
            return url
    srcset_value = str(tag.get("srcset") or "")
    if not srcset_value:
        return ""
    for part in [item.strip() for item in srcset_value.split(",") if item.strip()]:
        token = part.split()[0].strip()
        url = abs_url(token, base_url)
        if url:
            return url
    return ""


def render_image(tag: Tag, base_url: str, image_path_map: dict[str, str]) -> str:
    src = pick_image_src(tag, base_url)
    if not src:
        return ""
    mapped = image_path_map.get(src, src)
    alt = escape_inline_text(normalize_inline_text(str(tag.get("alt") or "image")) or "image")
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
        text = normalize_inline_text("".join(render_inline(c, base_url, image_path_map) for c in node.children))
        return f"**{text}**" if text else ""
    if name in {"em", "i"}:
        text = normalize_inline_text("".join(render_inline(c, base_url, image_path_map) for c in node.children))
        return f"*{text}*" if text else ""
    if name == "code":
        text = str(node.get_text("", strip=False)).strip()
        if not text:
            return ""
        return f"`{text.replace('`', '\\`').replace('\n', ' ')}`"
    if name == "a":
        href = abs_url(str(node.get("href") or ""), base_url)
        text = normalize_inline_text("".join(render_inline(c, base_url, image_path_map) for c in node.children))
        if href:
            return f"[{text or href}]({href})"
        return text
    return "".join(render_inline(c, base_url, image_path_map) for c in node.children)


def render_table(tag: Tag, base_url: str, image_path_map: dict[str, str]) -> str:
    rows: list[list[str]] = []
    for row in tag.find_all("tr"):
        cells = row.find_all(["th", "td"])
        if not cells:
            continue
        rows.append(
            [
                normalize_inline_text("".join(render_inline(child, base_url, image_path_map) for child in cell.children))
                for cell in cells
            ]
        )
    if not rows:
        return ""
    col_count = max(len(row) for row in rows)
    rows = [row + [""] * (col_count - len(row)) for row in rows]
    lines = [
        "| " + " | ".join(rows[0]) + " |",
        "| " + " | ".join(["---"] * col_count) + " |",
    ]
    for row in rows[1:]:
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
        text = normalize_inline_text("".join(render_inline(c, base_url, image_path_map) for c in node.children))
        return [f"{'#' * level} {text}"] if text else []
    if name in {"p", "figcaption"}:
        text = normalize_inline_text("".join(render_inline(c, base_url, image_path_map) for c in node.children))
        return [text] if text else []
    if name == "pre":
        code_text = str(node.get_text("", strip=False)).rstrip("\n")
        if not code_text:
            return []
        lang = extract_code_lang(node)
        fence = f"```{lang}" if lang else "```"
        return [f"{fence}\n{code_text}\n```"]
    if name == "blockquote":
        inner: list[str] = []
        for child in node.children:
            inner.extend(render_block(child, base_url, image_path_map, indent=indent))
        text = "\n\n".join(inner).strip()
        if not text:
            text = normalize_inline_text("".join(render_inline(c, base_url, image_path_map) for c in node.children))
        if not text:
            return []
        return ["\n".join([f"> {line}" if line else ">" for line in text.splitlines()])]
    if name in {"ul", "ol"}:
        ordered = name == "ol"
        lines: list[str] = []
        items = [child for child in node.children if isinstance(child, Tag) and child.name and child.name.lower() == "li"]
        for index, item in enumerate(items, start=1):
            marker = f"{index}. " if ordered else "- "
            inline_chunks: list[str] = []
            nested_lines: list[str] = []
            for child in item.children:
                if isinstance(child, Tag) and child.name and child.name.lower() in {"ul", "ol"}:
                    nested_lines.extend(render_block(child, base_url, image_path_map, indent=indent + 2))
                    continue
                blocks = render_block(child, base_url, image_path_map, indent=indent + 2)
                if blocks:
                    if not inline_chunks:
                        inline_chunks.append(blocks[0])
                        nested_lines.extend([(" " * (indent + 2)) + b for b in blocks[1:]])
                    else:
                        nested_lines.extend([(" " * (indent + 2)) + b for b in blocks])
                    continue
                inline = normalize_inline_text(render_inline(child, base_url, image_path_map))
                if inline:
                    inline_chunks.append(inline)
            lines.append((" " * indent) + marker + normalize_inline_text(" ".join(inline_chunks)))
            lines.extend(nested_lines)
        return [line for line in lines if line.strip()]
    if name == "img":
        md = render_image(node, base_url, image_path_map)
        return [md] if md else []
    if name == "figure":
        result: list[str] = []
        for child in node.children:
            result.extend(render_block(child, base_url, image_path_map, indent=indent))
        return [line for line in result if line.strip()]
    if name == "table":
        table_md = render_table(node, base_url, image_path_map)
        return [table_md] if table_md else []
    if name == "hr":
        return ["---"]
    result: list[str] = []
    for child in node.children:
        result.extend(render_block(child, base_url, image_path_map, indent=indent))
    if result:
        return result
    text = normalize_inline_text("".join(render_inline(c, base_url, image_path_map) for c in node.children))
    return [text] if text else []


def html_to_markdown(article_html: str, base_url: str, image_path_map: dict[str, str]) -> str:
    soup = BeautifulSoup(article_html, "html.parser")
    for tag in soup.find_all(["script", "style", "noscript"]):
        tag.decompose()
    blocks: list[str] = []
    for node in soup.contents:
        blocks.extend(render_block(node, base_url, image_path_map))
    merged = "\n\n".join([item.strip() for item in blocks if item and item.strip()]).strip()
    return re.sub(r"\n{3,}", "\n\n", merged)


def remove_leading_duplicate_title(markdown_body: str, title: str) -> str:
    body = str(markdown_body or "").lstrip()
    if not body:
        return body
    match = re.match(r"^#\s+(.+?)\n+", body)
    if not match:
        return body
    heading = normalize_inline_text(match.group(1))
    expect = normalize_inline_text(title)
    if heading and expect and heading == expect:
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


def is_auth_block_error(exc: Exception) -> bool:
    text = str(exc).lower()
    if "no zhihu answer/article content found in initialstate" in text:
        return True
    if "zhihu initial data script not found" in text:
        return True
    if "/account/unhuman" in text or "anti-bot challenge" in text:
        return True
    has_status = ("status=403" in text) or ("status=401" in text)
    return has_status and (
        "page status error" in text
        or "signed page status error" in text
        or "signed api status error" in text
        or "cookie d_c0 is missing" in text
        or "cookies are empty" in text
    )


async def manual_login_then_retry(
    *,
    page: Any,
    context: Any,
    url: str,
    site_type: str,
    args: argparse.Namespace,
    save_state_path: Path | None,
    httpx_proxy: str | None,
) -> ExtractionResult:
    wait_seconds = int(args.manual_login_wait_seconds)
    print(
        f"[info] auth blocked, start manual login fallback: url={url}, wait_seconds={wait_seconds}",
        file=sys.stderr,
    )
    if site_type == "zhihu":
        request_url = normalize_zhihu_target_url(url)
        await page.goto(request_url, wait_until="domcontentloaded", timeout=args.timeout_ms)
        current_url = str(page.url or "").lower()
        current_html_lower = ""
        try:
            current_html_lower = str(await page.content()).lower()
        except Exception:
            current_html_lower = ""
        if (
            "/account/unhuman" in current_url
            or 'id="zh-zse-ck"' in current_html_lower
            or '"appname":"zse_ck"' in current_html_lower
        ):
            print(
                "[info] detected zhihu anti-bot challenge page (/account/unhuman or zse_ck), "
                "please complete verification first.",
                file=sys.stderr,
            )
        else:
            parsed = urlparse(request_url)
            next_target = parsed.path or "/"
            if parsed.query:
                next_target += f"?{parsed.query}"
            login_url = f"https://www.zhihu.com/signin?next={quote(next_target, safe='')}"
            print(f"[info] open zhihu login page: {login_url}", file=sys.stderr)
            await page.goto(login_url, wait_until="domcontentloaded", timeout=args.timeout_ms)
    else:
        await page.goto(url, wait_until="domcontentloaded", timeout=args.timeout_ms)
    try:
        await page.bring_to_front()
    except Exception:
        pass
    print("[info] 请在浏览器中完成登录，等待结束后自动重试抓取...", file=sys.stderr)
    if wait_seconds > 0:
        await page.wait_for_timeout(wait_seconds * 1000)
    else:
        await asyncio.to_thread(input, "Please finish login/verification and press Enter to continue...")

    if save_state_path is not None:
        save_state_path.parent.mkdir(parents=True, exist_ok=True)
        await context.storage_state(path=str(save_state_path))
        print(f"[info] storage state saved after manual login: {save_state_path}")

    return await extract_page(page, context, url, site_type, args, httpx_proxy=httpx_proxy)


async def wait_for_ready(page: Any, selector: str, timeout_ms: int) -> None:
    try:
        await page.wait_for_selector(selector, timeout=timeout_ms)
    except Exception as exc:  # noqa: BLE001
        print(f"[warn] wait selector failed: selector={selector}, error={type(exc).__name__}: {exc}", file=sys.stderr)


async def auto_scroll(page: Any, rounds: int, step: int, wait_ms: int) -> None:
    if rounds <= 0:
        return
    for _ in range(rounds):
        await page.mouse.wheel(0, step)
        await page.wait_for_timeout(wait_ms)


async def extract_page(
    page: Any,
    context: Any,
    url: str,
    site_type: str,
    args: argparse.Namespace,
    httpx_proxy: str | None,
) -> ExtractionResult:
    if site_type == "zhihu":
        return await extract_zhihu_page_signed(
            page=page,
            context=context,
            url=url,
            args=args,
            httpx_proxy=httpx_proxy,
        )

    selectors, wait_selector = selectors_for_site(site_type, args.wait_selector)
    response = await page.goto(url, wait_until="domcontentloaded", timeout=args.timeout_ms)
    if response is not None and int(response.status) >= 400:
        raise RuntimeError(f"page status error: status={response.status}, url={url}")
    await wait_for_ready(page, wait_selector, args.timeout_ms)
    await auto_scroll(page, args.scroll_rounds, args.scroll_step, args.scroll_wait_ms)
    payload = await page.evaluate(EXTRACT_SCRIPT, {"selectors": selectors})
    image_urls = [str(item).strip() for item in (payload.get("imageUrls") or []) if str(item).strip()]
    return ExtractionResult(
        requested_url=url,
        final_url=str(payload.get("pageUrl") or page.url or url),
        site_type=site_type,
        title=str(payload.get("title") or "").strip(),
        root_selector=str(payload.get("rootSelector") or "").strip(),
        article_html=str(payload.get("articleHtml") or ""),
        text=str(payload.get("text") or "").strip(),
        image_urls=list(dict.fromkeys(image_urls)),
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
    site_type: str,
    user_agent: str = "",
) -> tuple[list[ImageDownloadResult], dict[str, str]]:
    save_dir.mkdir(parents=True, exist_ok=True)
    selected = image_urls if max_images == 0 else image_urls[:max_images]
    results: list[ImageDownloadResult] = []
    path_map: dict[str, str] = {}
    for index, image_url in enumerate(selected, start=1):
        try:
            headers = {"Referer": referer}
            if site_type == "zhihu":
                headers["User-Agent"] = user_agent or DEFAULT_ZHIHU_USER_AGENT
                headers["Accept"] = "image/avif,image/webp,image/apng,image/*,*/*;q=0.8"
            response = await context.request.get(image_url, timeout=timeout_ms, headers=headers)
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
            if content_type and "image" not in content_type.lower():
                results.append(
                    ImageDownloadResult(
                        source_url=image_url,
                        ok=False,
                        status=status,
                        local_path=None,
                        relative_path=None,
                        byte_size=0,
                        error=f"non_image_content_type:{content_type}",
                    )
                )
                continue
            ext = guess_ext(image_url, content_type)
            filename = f"img_{index:04d}{ext}"
            output_path = save_dir / filename
            output_path.write_bytes(body)
            rel = (Path("images") / filename).as_posix()
            path_map[image_url] = rel
            results.append(
                ImageDownloadResult(
                    source_url=image_url,
                    ok=True,
                    status=status,
                    local_path=str(output_path),
                    relative_path=rel,
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
    return results, path_map


def build_page_output_dir(base_output_dir: Path, index: int, result: ExtractionResult) -> Path:
    host = (urlparse(result.final_url).hostname or "site").replace(".", "_")
    fallback_slug = (urlparse(result.final_url).path.rstrip("/").split("/")[-1] or "article").strip()
    title_slug = slugify(result.title, fallback=slugify(fallback_slug, fallback="article"))
    page_dir = base_output_dir / f"{index:03d}_{host}_{title_slug}"
    page_dir.mkdir(parents=True, exist_ok=True)
    return page_dir


def write_outputs(
    *,
    page_dir: Path,
    result: ExtractionResult,
    markdown: str,
    output_name: str,
    save_html: bool,
    downloads: list[ImageDownloadResult],
) -> dict[str, Any]:
    markdown_path = page_dir / output_name
    markdown_path.write_text(markdown, encoding="utf-8")
    text_path = page_dir / "content.txt"
    text_path.write_text(result.text, encoding="utf-8")
    html_path = ""
    if save_html:
        raw_html_path = page_dir / "article.html"
        raw_html_path.write_text(result.article_html, encoding="utf-8")
        html_path = str(raw_html_path)
    metadata = {
        "requested_url": result.requested_url,
        "final_url": result.final_url,
        "site_type": result.site_type,
        "title": result.title,
        "root_selector": result.root_selector,
        "fetched_at_utc": result.fetched_at_utc,
        "text_chars": len(result.text),
        "image_count": len(result.image_urls),
        "markdown_path": str(markdown_path),
        "content_path": str(text_path),
        "html_path": html_path,
        "downloaded_images": [asdict(item) for item in downloads],
    }
    result_path = page_dir / "result.json"
    result_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
    return metadata


async def run(args: argparse.Namespace) -> int:
    urls = read_urls(args)
    async_playwright = ensure_playwright()

    persist_outputs = not bool(args.in_memory_output)
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_root = args.out_dir / f"extract_{batch_id}"
    if persist_outputs:
        output_root.mkdir(parents=True, exist_ok=True)

    storage_state_path: Path | None = args.storage_state
    if storage_state_path is None and args.auto_login_fallback and DEFAULT_ZHIHU_STORAGE_STATE.exists():
        storage_state_path = DEFAULT_ZHIHU_STORAGE_STATE

    save_storage_state_path: Path | None = args.save_storage_state
    if save_storage_state_path is None and args.auto_login_fallback:
        save_storage_state_path = storage_state_path or DEFAULT_ZHIHU_STORAGE_STATE

    contains_zhihu = any(infer_site_type(item, args.site) == "zhihu" for item in urls)
    allow_zhihu_persistent_context = contains_zhihu and not bool(args.disable_zhihu_persistent_context)
    use_zhihu_cdp_mode = contains_zhihu and bool(args.zhihu_cdp_mode)
    use_zhihu_persistent_context = allow_zhihu_persistent_context and not use_zhihu_cdp_mode
    httpx_proxy = str(args.zhihu_httpx_proxy or "").strip() or None
    if contains_zhihu:
        ensure_httpx()
        bridge = load_mindspider_zhihu_bridge(str(args.zhihu_mindspider_dir))
        print(f"[info] zhihu extractor backend: mindspider ({bridge.media_crawler_dir})")

    context_kwargs: dict[str, Any] = {}
    if storage_state_path and not use_zhihu_persistent_context and not use_zhihu_cdp_mode:
        context_kwargs["storage_state"] = str(storage_state_path)
    if args.user_agent:
        context_kwargs["user_agent"] = args.user_agent
    elif contains_zhihu:
        context_kwargs["user_agent"] = DEFAULT_ZHIHU_USER_AGENT
    context_kwargs["viewport"] = {"width": 1920, "height": 1080}

    page_outputs: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    async with async_playwright() as playwright:
        browser = None
        cdp_browser = None
        cdp_process = None
        if use_zhihu_cdp_mode:
            try:
                context, cdp_browser, cdp_process, cdp_port = await launch_zhihu_cdp_context(
                    playwright=playwright,
                    context_kwargs=context_kwargs,
                    args=args,
                )
                print(f"[info] zhihu cdp mode enabled: port={cdp_port}")
            except Exception as exc:  # noqa: BLE001
                print(
                    f"[warn] zhihu cdp mode failed, fallback to standard launch: {type(exc).__name__}: {exc}",
                    file=sys.stderr,
                )
                context, browser = await launch_standard_browser_context(
                    playwright=playwright,
                    context_kwargs=context_kwargs,
                    args=args,
                    use_zhihu_persistent_context=allow_zhihu_persistent_context,
                )
        else:
            context, browser = await launch_standard_browser_context(
                playwright=playwright,
                context_kwargs=context_kwargs,
                args=args,
                use_zhihu_persistent_context=use_zhihu_persistent_context,
            )

        if contains_zhihu:
            stealth_path = Path(args.zhihu_stealth_js)
            if stealth_path.exists():
                await context.add_init_script(path=str(stealth_path))
                print(f"[info] zhihu stealth script loaded: {stealth_path}")
            else:
                print(f"[info] zhihu stealth script not found, skip: {stealth_path}")

        if storage_state_path and (use_zhihu_persistent_context or use_zhihu_cdp_mode):
            loaded = await apply_storage_state_cookies(context, storage_state_path)
            if loaded > 0:
                print(f"[info] loaded storage-state cookies into persistent context: count={loaded}")

        page = await context.new_page()

        try:
            if args.pause_for_login:
                first_url = urls[0]
                print(f"[info] login step open: {first_url}")
                await page.goto(first_url, wait_until="domcontentloaded", timeout=args.timeout_ms)
                await asyncio.to_thread(input, "请在浏览器完成登录后按 Enter 继续...")

            total_urls = len(urls)
            for index, url in enumerate(urls, start=1):
                site_type = infer_site_type(url, args.site)
                print(f"[info] fetch index={index}, site={site_type}, url={url}")
                extraction: ExtractionResult | None = None
                final_error: Exception | None = None
                try:
                    extraction = await extract_page(
                        page,
                        context,
                        url,
                        site_type,
                        args,
                        httpx_proxy=httpx_proxy,
                    )
                except Exception as exc:  # noqa: BLE001
                    final_error = exc
                    if args.auto_login_fallback and site_type == "zhihu" and is_auth_block_error(exc):
                        try:
                            extraction = await manual_login_then_retry(
                                page=page,
                                context=context,
                                url=url,
                                site_type=site_type,
                                args=args,
                                save_state_path=save_storage_state_path,
                                httpx_proxy=httpx_proxy,
                            )
                        except Exception as retry_exc:  # noqa: BLE001
                            final_error = retry_exc

                if extraction is not None:
                    downloads: list[ImageDownloadResult] = []
                    image_path_map: dict[str, str] = {}
                    if persist_outputs and args.save_images and extraction.image_urls:
                        page_dir = build_page_output_dir(output_root, index, extraction)
                        downloads, image_path_map = await download_images(
                            context=context,
                            image_urls=extraction.image_urls,
                            referer=extraction.final_url,
                            save_dir=page_dir / "images",
                            timeout_ms=args.timeout_ms,
                            max_images=args.max_images,
                            site_type=site_type,
                            user_agent=str(context_kwargs.get("user_agent") or ""),
                        )
                    markdown_body = html_to_markdown(
                        extraction.article_html,
                        extraction.final_url,
                        image_path_map=image_path_map,
                    )
                    markdown_doc = build_markdown_document(extraction, markdown_body)
                    if persist_outputs:
                        page_dir = build_page_output_dir(output_root, index, extraction)
                        if args.save_images and extraction.image_urls and not downloads:
                            downloads, image_path_map = await download_images(
                                context=context,
                                image_urls=extraction.image_urls,
                                referer=extraction.final_url,
                                save_dir=page_dir / "images",
                                timeout_ms=args.timeout_ms,
                                max_images=args.max_images,
                                site_type=site_type,
                                user_agent=str(context_kwargs.get("user_agent") or ""),
                            )
                            markdown_body = html_to_markdown(
                                extraction.article_html,
                                extraction.final_url,
                                image_path_map=image_path_map,
                            )
                            markdown_doc = build_markdown_document(extraction, markdown_body)
                        metadata = write_outputs(
                            page_dir=page_dir,
                            result=extraction,
                            markdown=markdown_doc,
                            output_name=args.output_name,
                            save_html=args.save_html,
                            downloads=downloads,
                        )
                        page_outputs.append(
                            {
                                "url": url,
                                "final_url": extraction.final_url,
                                "site_type": extraction.site_type,
                                "title": extraction.title,
                                "output_dir": str(page_dir),
                                "markdown_path": metadata["markdown_path"],
                                "markdown_content": "",
                                "image_total": len(extraction.image_urls),
                                "image_downloaded_ok": sum(1 for item in downloads if item.ok),
                            }
                        )
                    else:
                        page_outputs.append(
                            {
                                "url": url,
                                "final_url": extraction.final_url,
                                "site_type": extraction.site_type,
                                "title": extraction.title,
                                "output_dir": "",
                                "markdown_path": "",
                                "markdown_content": markdown_doc,
                                "downloaded_images": [],
                                "image_total": len(extraction.image_urls),
                                "image_downloaded_ok": 0,
                            }
                        )
                    if (
                        site_type == "zhihu"
                        and args.zhihu_request_interval_sec > 0
                        and index < total_urls
                    ):
                        await asyncio.sleep(args.zhihu_request_interval_sec)
                    continue

                exc = final_error if final_error is not None else RuntimeError("unknown fetch error")
                failure_payload = {
                    "url": url,
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "failed_at_utc": now_utc_text(),
                }
                failure_path = ""
                if persist_outputs:
                    failure_file = output_root / f"{index:03d}_failed.json"
                    failure_file.write_text(json.dumps(failure_payload, ensure_ascii=False, indent=2), encoding="utf-8")
                    failure_path = str(failure_file)
                failures.append(
                    {
                        "url": url,
                        "error": f"{type(exc).__name__}: {exc}",
                        "failure_path": failure_path,
                    }
                )
                print(
                    f"[warn] fetch failed: index={index}, url={url}, error={type(exc).__name__}: {exc}",
                    file=sys.stderr,
                )
                if (
                    site_type == "zhihu"
                    and args.zhihu_request_interval_sec > 0
                    and index < total_urls
                ):
                    await asyncio.sleep(args.zhihu_request_interval_sec)

            if save_storage_state_path:
                try:
                    save_storage_state_path.parent.mkdir(parents=True, exist_ok=True)
                    await context.storage_state(path=str(save_storage_state_path))
                    print(f"[info] storage state saved: {save_storage_state_path}")
                except Exception as exc:  # noqa: BLE001
                    print(
                        f"[warn] storage state save skipped: {type(exc).__name__}: {exc}",
                        file=sys.stderr,
                    )
        finally:
            if cdp_browser is not None:
                try:
                    await context.close()
                except Exception:
                    pass
                try:
                    await cdp_browser.close()
                except Exception:
                    pass
                terminate_process(cdp_process)
            else:
                await context.close()
                if browser is not None:
                    await browser.close()

    summary = {
        "output_root": str(output_root) if persist_outputs else "",
        "pages": page_outputs,
        "failures": failures,
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
