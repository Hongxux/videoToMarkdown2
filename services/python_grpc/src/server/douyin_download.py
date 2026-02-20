"""
模块说明：抖音下载适配模块（services 内置版）。
执行逻辑：
1) 从抖音 URL 提取视频 ID，优先转为 /video/{id} 页面。
2) 复用提取出的浏览器策略（Playwright 抓取 aweme_detail 或媒体 URL）。
3) 用 aiohttp 下载视频流到 task_dir/video.mp4，并返回稳定路径。
核心价值：把我们在 douyin-downloader 中实际用到的核心能力沉淀到 services 目录，
避免主链路对仓库根目录脚本的硬依赖。
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiohttp


logger = logging.getLogger(__name__)

try:
    from playwright.async_api import async_playwright, Browser, BrowserContext, Page

    PLAYWRIGHT_AVAILABLE = True
except Exception:
    PLAYWRIGHT_AVAILABLE = False
    Browser = Any  # type: ignore[assignment]
    BrowserContext = Any  # type: ignore[assignment]
    Page = Any  # type: ignore[assignment]


class TaskType(Enum):
    VIDEO = "video"


@dataclass
class DownloadTask:
    task_id: str
    url: str
    task_type: TaskType
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DownloadResult:
    success: bool
    task_id: str
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class BrowserDownloadStrategy:
    """
    从 douyin-downloader 提取并裁剪的浏览器策略：
    仅保留主链路需要的“单视频 URL 抓取”能力。
    """

    def __init__(self, headless: bool = True, timeout_ms: int = 60000):
        if not PLAYWRIGHT_AVAILABLE:
            raise RuntimeError("Playwright 未安装，无法执行抖音浏览器下载策略")
        self.headless = headless
        self.timeout = timeout_ms
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.playwright = None
        self.initialized = False
        self.user_agent = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

    async def initialize(self) -> None:
        if self.initialized:
            return
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=self.headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--disable-web-security",
                "--no-sandbox",
            ],
        )
        self.context = await self.browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=self.user_agent,
            locale="zh-CN",
            timezone_id="Asia/Shanghai",
        )
        self.initialized = True

    async def cleanup(self) -> None:
        if self.context:
            await self.context.close()
            self.context = None
        if self.browser:
            await self.browser.close()
            self.browser = None
        if self.playwright:
            await self.playwright.stop()
            self.playwright = None
        self.initialized = False

    async def download(self, task: DownloadTask) -> DownloadResult:
        start_time = time.time()
        try:
            await self.initialize()
            page = await self.context.new_page()
            captured_payload: Dict[str, Optional[Dict[str, Any]]] = {"aweme_detail": None}

            async def _capture_detail_response(response) -> None:
                try:
                    if response.status != 200:
                        return
                    url = response.url or ""
                    if "aweme/v1/web/aweme/detail" not in url:
                        return
                    text = await response.text()
                    if not text:
                        return
                    data = json.loads(text)
                    detail = data.get("aweme_detail") if isinstance(data, dict) else None
                    if isinstance(detail, dict):
                        captured_payload["aweme_detail"] = detail
                except Exception:
                    return

            page.on("response", lambda resp: asyncio.create_task(_capture_detail_response(resp)))

            try:
                cookies = task.metadata.get("cookies")
                if cookies:
                    await self._set_cookies(page, cookies)
                await page.goto(task.url, wait_until="domcontentloaded", timeout=self.timeout)
                await asyncio.sleep(2)
                video_info = await self._resolve_video_info(page, captured_payload)
                if not video_info or not video_info.get("url"):
                    return DownloadResult(
                        success=False,
                        task_id=task.task_id,
                        error_message="无法获取视频URL",
                    )
                return DownloadResult(
                    success=True,
                    task_id=task.task_id,
                    metadata={
                        "video_url": str(video_info["url"]).strip(),
                        "title": str(video_info.get("title", "")).strip(),
                        "author": str(video_info.get("author", "")).strip(),
                        "duration": time.time() - start_time,
                    },
                )
            finally:
                await page.close()
        except Exception as exc:
            return DownloadResult(
                success=False,
                task_id=task.task_id,
                error_message=str(exc),
            )

    async def _resolve_video_info(
        self,
        page: "Page",
        captured_payload: Dict[str, Optional[Dict[str, Any]]],
    ) -> Optional[Dict[str, Any]]:
        try:
            await page.wait_for_selector("video", timeout=10000)
            video_info = await page.evaluate(
                """
                () => {
                    const video = document.querySelector('video');
                    if (!video) return null;
                    let videoUrl = video.src || video.currentSrc;
                    if (!videoUrl) {
                        const source = video.querySelector('source');
                        if (source) {
                            videoUrl = source.src;
                        }
                    }
                    return {
                        url: videoUrl,
                        title: document.title || '',
                        author: ''
                    };
                }
                """
            )
            if video_info and self._is_likely_real_video_url(str(video_info.get("url", ""))):
                return video_info
        except Exception:
            pass

        detail = captured_payload.get("aweme_detail")
        video_info = self._build_video_info_from_aweme_detail(detail)
        if video_info:
            return video_info

        intercepted = await self._intercept_video_url(page)
        if intercepted:
            return {"url": intercepted}

        # 拦截阶段后再次检查 detail，避免时序导致漏取。
        detail = captured_payload.get("aweme_detail")
        video_info = self._build_video_info_from_aweme_detail(detail)
        if video_info:
            return video_info

        html_fallback = await self._extract_video_url_from_html(page)
        if html_fallback:
            return {"url": html_fallback}
        return None

    async def _intercept_video_url(self, page: "Page") -> Optional[str]:
        hit_url: Optional[str] = None

        def handle_response(response) -> None:
            nonlocal hit_url
            if hit_url is not None:
                return
            try:
                if response.status != 200:
                    return
                url = response.url or ""
                lower_url = url.lower()
                resource_type = response.request.resource_type if response.request else ""
                content_type = (response.headers or {}).get("content-type", "").lower()
                has_media_extension = any(token in lower_url for token in [".mp4", ".m3u8", ".flv"])
                is_media_response = ("video/" in content_type) or (resource_type == "media")
                if (has_media_extension or is_media_response) and self._is_likely_real_video_url(url):
                    hit_url = url
            except Exception:
                return

        page.on("response", handle_response)
        try:
            await page.reload(wait_until="domcontentloaded", timeout=self.timeout)
        except Exception:
            pass
        await asyncio.sleep(5)
        try:
            await page.evaluate(
                """
                () => {
                    const video = document.querySelector('video');
                    if (video) video.play();
                }
                """
            )
        except Exception:
            pass
        await asyncio.sleep(2)
        return hit_url

    async def _extract_video_url_from_html(self, page: "Page") -> Optional[str]:
        try:
            html = await page.content()
            patterns = [
                r'https?://[^"\'\\]+?\.(?:mp4|m3u8)[^"\'\\]*',
                r'https?:\\/\\/[^"\\]+?\\.(?:mp4|m3u8)[^"\\]*',
            ]
            for pattern in patterns:
                for raw in re.findall(pattern, html):
                    candidate = raw.replace("\\/", "/")
                    if self._is_likely_real_video_url(candidate):
                        return candidate
        except Exception:
            return None
        return None

    def _build_video_info_from_aweme_detail(self, detail: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not isinstance(detail, dict):
            return None
        video_url, source = self._pick_video_url_from_aweme_detail(detail)
        if not video_url:
            return None
        logger.info(f"aweme_detail提取到视频URL，来源={source}, url={video_url[:120]}")
        return {
            "url": video_url,
            "title": detail.get("desc") or "",
            "author": ((detail.get("author") or {}).get("nickname") or ""),
        }

    def _pick_video_url_from_aweme_detail(self, detail: Dict[str, Any]) -> Tuple[Optional[str], str]:
        video = detail.get("video") or {}
        candidates: List[Tuple[str, List[str]]] = [
            ("play_addr_h264", ((video.get("play_addr_h264") or {}).get("url_list") or [])),
            ("play_addr", ((video.get("play_addr") or {}).get("url_list") or [])),
            ("download_addr", ((video.get("download_addr") or {}).get("url_list") or [])),
        ]
        bit_rates = video.get("bit_rate") or []
        for idx, item in enumerate(bit_rates):
            url_list = ((item or {}).get("play_addr") or {}).get("url_list") or []
            candidates.append((f"bit_rate[{idx}].play_addr", url_list))

        first_non_empty: Optional[str] = None
        first_source = ""
        for source, url_list in candidates:
            for raw in url_list:
                url = str(raw or "").strip()
                if not url:
                    continue
                if first_non_empty is None:
                    first_non_empty = url
                    first_source = source
                if self._is_likely_real_video_url(url):
                    return url, source
        if first_non_empty:
            return first_non_empty, first_source
        return None, ""

    def _is_likely_real_video_url(self, url: str) -> bool:
        if not url:
            return False
        lower = url.lower()
        invalid_keywords = [
            "douyin_pc_client",
            "uuu_",
            "/obj/douyin-pc-web/",
            "player-",
            ".js",
            ".css",
            ".map",
        ]
        if any(keyword in lower for keyword in invalid_keywords):
            return False
        has_media_token = any(
            token in lower for token in [".mp4", ".m3u8", ".flv", "playwm", "/play/", "/video/tos/"]
        )
        trusted_host = any(
            host in lower for host in ["douyinvod.com", "bytevideo", "ibytedtos", "tos-cn", "aweme.snssdk.com"]
        )
        return has_media_token and trusted_host

    async def _set_cookies(self, page: "Page", cookies: Any) -> None:
        try:
            if isinstance(cookies, str):
                cookie_list = []
                for item in cookies.split(";"):
                    if "=" in item:
                        key, value = item.strip().split("=", 1)
                        cookie_list.append(
                            {
                                "name": key,
                                "value": value,
                                "domain": ".douyin.com",
                                "path": "/",
                            }
                        )
                await page.context.add_cookies(cookie_list)
            elif isinstance(cookies, list):
                await page.context.add_cookies(cookies)
            elif isinstance(cookies, dict):
                cookie_list = [
                    {"name": key, "value": value, "domain": ".douyin.com", "path": "/"}
                    for key, value in cookies.items()
                ]
                await page.context.add_cookies(cookie_list)
        except Exception as exc:
            logger.warning(f"设置抖音Cookies失败: {exc}")


def _extract_video_id(video_url: str) -> Optional[str]:
    if not video_url:
        return None
    patterns = [
        r"/video/(\d+)",
        r"modal_id=(\d+)",
        r"aweme_id=(\d+)",
        r"item_id=(\d+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, video_url)
        if match:
            return match.group(1)
    return None


def _build_candidate_page_url(video_url: str) -> str:
    video_id = _extract_video_id(video_url)
    if video_id:
        return f"https://www.douyin.com/video/{video_id}"
    return video_url


def _resolve_douyin_cookie() -> Optional[str]:
    # 仅复用统一环境入口，避免在服务端落地额外敏感文件。
    cookie = (os.getenv("DOUYIN_COOKIE", "") or "").strip()
    return cookie or None


async def _download_file(video_url: str, output_path: Path, referer_url: str) -> None:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Referer": referer_url,
        "Accept": "*/*",
    }
    timeout = aiohttp.ClientTimeout(total=600, connect=30, sock_connect=30, sock_read=120)
    temp_path = output_path.with_suffix(output_path.suffix + ".part")
    last_error: Optional[Exception] = None
    for attempt in range(3):
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(video_url, headers=headers) as response:
                    if response.status != 200:
                        body = await response.text()
                        raise RuntimeError(
                            f"下载抖音视频失败，HTTP {response.status}，body={body[:200]!r}"
                        )
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    with open(temp_path, "wb") as file_obj:
                        async for chunk in response.content.iter_chunked(1024 * 256):
                            if chunk:
                                file_obj.write(chunk)
            temp_path.replace(output_path)
            if output_path.stat().st_size <= 0:
                raise RuntimeError(f"抖音视频下载为空文件: {output_path}")
            return
        except Exception as exc:
            last_error = exc
            await asyncio.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"抖音视频下载失败: {last_error}")


async def download_video_with_douyin_downloader(
    *,
    task_id: str,
    video_url: str,
    task_dir: str,
    video_filename: str = "video",
) -> str:
    """
    执行逻辑：
    1) 走 services 内置浏览器策略获取可下载视频 URL。
    2) 下载并归一为 task_dir/video.mp4，返回稳定路径。
    """
    page_url = _build_candidate_page_url(video_url)
    logger.info(f"[{task_id}] Douyin URL detected, using services.douyin_download: page={page_url}")

    strategy = BrowserDownloadStrategy(headless=True, timeout_ms=60000)
    task = DownloadTask(
        task_id=task_id,
        url=page_url,
        task_type=TaskType.VIDEO,
        metadata={"cookies": _resolve_douyin_cookie()},
    )

    try:
        result = await strategy.download(task)
        if not result.success:
            raise RuntimeError(result.error_message or "抖音浏览器策略执行失败")
        resolved_url = str(result.metadata.get("video_url", "")).strip()
        if not resolved_url:
            raise RuntimeError("抖音浏览器策略未返回 video_url")

        normalized_path = Path(os.path.abspath(os.path.join(task_dir, f"{video_filename}.mp4")))
        await _download_file(resolved_url, normalized_path, referer_url=page_url)
        if (not normalized_path.exists()) or normalized_path.stat().st_size <= 0:
            raise RuntimeError(f"Normalized douyin video missing or empty: {normalized_path}")
        return str(normalized_path)
    finally:
        await strategy.cleanup()
