"""
模块说明：分享链接统一解析层（抖音/B站）。
"""

from __future__ import annotations

import asyncio
import html as html_utils
import re
from dataclasses import dataclass
from typing import List, Optional, Tuple
from urllib.parse import urlparse

import aiohttp

from .platform_rules import (
    extract_bilibili_video_id as _extract_bilibili_video_id_from_rules,
    extract_douyin_aweme_ref as _extract_douyin_aweme_ref_from_rules,
    is_bilibili_host as _is_bilibili_host_from_rules,
    is_douyin_host as _is_douyin_host_from_rules,
)


try:
    from playwright.async_api import async_playwright

    PLAYWRIGHT_AVAILABLE = True
except Exception:
    PLAYWRIGHT_AVAILABLE = False


_TRAILING_PUNCTUATION = "\"'`()[]{}<>，。！？；:,.!?;"
_URL_PATTERN = re.compile(r"(https?://[^\s]+)", flags=re.IGNORECASE)
_TITLE_SUFFIX_PATTERN = re.compile(
    r"[\s\-_~]*(哔哩哔哩|bilibili|抖音)(\s*[-|｜].*)?$",
    flags=re.IGNORECASE,
)
_NOISE_TITLE_PATTERN = re.compile(r"(https?://|复制打开抖音|douyin\.com|b23\.tv)", flags=re.IGNORECASE)
_GENERIC_TITLE_PATTERN = re.compile(r"^(哔哩哔哩|抖音|bilibili|douyin)$", flags=re.IGNORECASE)
_META_TAG_PATTERN = re.compile(r"<meta\b[^>]*>", flags=re.IGNORECASE)
_META_ATTR_PATTERN = re.compile(
    r"([A-Za-z_:.-]+)\s*=\s*(?:\"([^\"]*)\"|'([^']*)')",
    flags=re.IGNORECASE,
)
_TITLE_TAG_PATTERN = re.compile(r"<title[^>]*>(.*?)</title>", flags=re.IGNORECASE | re.DOTALL)


@dataclass(frozen=True)
class ResolvedShareLink:
    raw_input: str
    extracted_url: str
    resolved_url: str
    platform: str
    canonical_id: str
    resolver: str
    title: str = ""
    title_source: str = ""
    title_confidence: float = 0.0
    content_type: str = "unknown"


@dataclass(frozen=True)
class _TitleCandidate:
    text: str
    source: str
    confidence: float


def _extract_first_url(raw_text: str) -> str:
    text = str(raw_text or "").strip()
    if not text:
        return ""

    match = _URL_PATTERN.search(text)
    if not match:
        return text if text.lower().startswith(("http://", "https://")) else ""

    candidate = match.group(1).strip()
    while candidate and candidate[-1] in _TRAILING_PUNCTUATION:
        candidate = candidate[:-1]
    return candidate.strip()


def _is_douyin_host(host: str) -> bool:
    return _is_douyin_host_from_rules(host)


def _is_bilibili_host(host: str) -> bool:
    return _is_bilibili_host_from_rules(host)


def _extract_douyin_aweme_ref(url: str) -> Tuple[str, str]:
    return _extract_douyin_aweme_ref_from_rules(url)


def _extract_bilibili_video_id(url: str) -> str:
    return _extract_bilibili_video_id_from_rules(url)


def _infer_content_type(platform: str, canonical_url: str) -> str:
    if platform == "bilibili":
        return "video"
    if platform == "douyin":
        if "/note/" in canonical_url:
            return "note"
        if "/video/" in canonical_url:
            return "video"
    return "unknown"


def _canonicalize_url(url: str) -> Tuple[str, str, str, str]:
    parsed = urlparse(url or "")
    host = parsed.netloc

    if _is_douyin_host(host):
        kind, aweme_id = _extract_douyin_aweme_ref(url)
        if kind == "video" and aweme_id:
            canonical_url = f"https://www.douyin.com/video/{aweme_id}"
            return "douyin", aweme_id, canonical_url, "video"
        if kind == "note" and aweme_id:
            canonical_url = f"https://www.douyin.com/note/{aweme_id}"
            return "douyin", aweme_id, canonical_url, "note"
        return "douyin", "", url, "unknown"

    if _is_bilibili_host(host):
        video_id = _extract_bilibili_video_id(url)
        if video_id:
            canonical_url = f"https://www.bilibili.com/video/{video_id}"
            return "bilibili", video_id, canonical_url, "video"
        return "bilibili", "", url, "video"

    return "unknown", "", url, "unknown"


def _normalize_page_title(title: str) -> str:
    normalized = re.sub(r"\s+", " ", str(title or "")).strip()
    if not normalized:
        return ""
    normalized = _TITLE_SUFFIX_PATTERN.sub("", normalized).strip()
    normalized = normalized.strip(" -_|｜:：")
    if _GENERIC_TITLE_PATTERN.fullmatch(normalized):
        return ""
    return normalized


def _extract_title_from_html(html_text: str) -> str:
    content = str(html_text or "")
    if not content:
        return ""

    for meta_tag in _META_TAG_PATTERN.findall(content):
        attrs = {}
        for attr in _META_ATTR_PATTERN.finditer(meta_tag):
            attr_name = str(attr.group(1) or "").lower().strip()
            attr_value = attr.group(2) if attr.group(2) is not None else (attr.group(3) or "")
            attrs[attr_name] = html_utils.unescape(str(attr_value or "").strip())

        title_key = str(attrs.get("property") or attrs.get("name") or "").lower()
        if title_key not in {"og:title", "twitter:title"}:
            continue
        candidate = _normalize_page_title(attrs.get("content", ""))
        if candidate:
            return candidate

    title_match = _TITLE_TAG_PATTERN.search(content)
    if not title_match:
        return ""
    return _normalize_page_title(html_utils.unescape(str(title_match.group(1) or "")))


def _extract_embedded_title(raw_text: str) -> str:
    text = str(raw_text or "")
    patterns = (
        r"[【\[]\s*([^【】\[\]]{1,200})\s*[】\]]",
        r"“([^”]{1,200})”",
        r"\"([^\"]{1,200})\"",
    )
    for pattern in patterns:
        match = re.search(pattern, text)
        if not match:
            continue
        candidate = _normalize_page_title(match.group(1))
        if candidate:
            return candidate
    return ""


def _score_title_candidate(text: str, source: str) -> float:
    score_base = {"embedded": 0.92, "page": 0.78, "fallback": 0.3}
    score = score_base.get(source, 0.2)
    if not text:
        return 0.0

    if _NOISE_TITLE_PATTERN.search(text):
        score -= 0.5
    if "#" in text:
        score -= 0.2
    if "@" in text:
        score -= 0.15
    if len(text) <= 2:
        score -= 0.35
    if len(text) > 90:
        score -= 0.2
    if re.fullmatch(r"[0-9A-Za-z_\-]+", text):
        score -= 0.2

    return max(0.0, min(1.0, score))


def _select_best_title(
    *,
    embedded_title: str,
    page_title: str,
    fallback_title: str = "",
) -> Tuple[str, str, float]:
    candidates: List[_TitleCandidate] = []
    for source, raw in (
        ("embedded", embedded_title),
        ("page", page_title),
        ("fallback", fallback_title),
    ):
        normalized = _normalize_page_title(raw)
        if not normalized:
            continue
        candidates.append(
            _TitleCandidate(
                text=normalized,
                source=source,
                confidence=_score_title_candidate(normalized, source),
            )
        )

    if not candidates:
        return "", "", 0.0

    candidates.sort(key=lambda item: item.confidence, reverse=True)
    best = candidates[0]
    if best.confidence < 0.2:
        return "", "", best.confidence
    return best.text, best.source, best.confidence


async def _resolve_url_with_playwright(url: str, timeout_ms: int = 45000) -> Tuple[str, str]:
    if not PLAYWRIGHT_AVAILABLE:
        raise RuntimeError("Playwright unavailable")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
            ],
        )
        try:
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                ),
                locale="zh-CN",
            )
            page = await context.new_page()
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                await asyncio.sleep(1.2)
                page_title = await page.evaluate(
                    """
                    () => {
                        const ogTitle = document.querySelector('meta[property="og:title"]');
                        if (ogTitle && ogTitle.content) return ogTitle.content;
                        const twitterTitle = document.querySelector('meta[name="twitter:title"]');
                        if (twitterTitle && twitterTitle.content) return twitterTitle.content;
                        return document.title || '';
                    }
                    """
                )
                return (page.url or url), _normalize_page_title(str(page_title or ""))
            finally:
                await page.close()
                await context.close()
        finally:
            await browser.close()


async def _resolve_url_with_http_redirect(url: str, timeout_sec: int = 20) -> str:
    timeout = aiohttp.ClientTimeout(total=timeout_sec, connect=10, sock_connect=10, sock_read=10)
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
    }
    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        async with session.get(url, allow_redirects=True) as response:
            return str(response.url or url)


async def _resolve_url_with_http_page_title(url: str, timeout_sec: int = 20) -> Tuple[str, str]:
    timeout = aiohttp.ClientTimeout(total=timeout_sec, connect=10, sock_connect=10, sock_read=10)
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        async with session.get(url, allow_redirects=True) as response:
            final_url = str(response.url or url)
            body = await response.text(errors="ignore")
            return final_url, _extract_title_from_html(body)


async def resolve_share_link(raw_text: str, timeout_ms: int = 45000) -> ResolvedShareLink:
    extracted_url = _extract_first_url(raw_text)
    if not extracted_url:
        raise ValueError("未在输入中识别到有效链接")

    embedded_title = _extract_embedded_title(raw_text)
    platform, canonical_id, canonical_url, content_type = _canonicalize_url(extracted_url)
    parsed = urlparse(extracted_url)
    host = (parsed.netloc or "").lower()

    if platform == "unknown":
        chosen_title, source, confidence = _select_best_title(embedded_title=embedded_title, page_title="")
        return ResolvedShareLink(
            raw_input=raw_text,
            extracted_url=extracted_url,
            resolved_url=canonical_url,
            platform=platform,
            canonical_id=canonical_id,
            resolver="canonical-unknown",
            title=chosen_title,
            title_source=source,
            title_confidence=confidence,
            content_type=content_type,
        )

    if platform == "douyin" and canonical_id and ("/video/" in canonical_url or "/note/" in canonical_url):
        page_title = ""
        if PLAYWRIGHT_AVAILABLE:
            try:
                _final_url, page_title = await _resolve_url_with_playwright(
                    canonical_url, timeout_ms=min(timeout_ms, 25000)
                )
            except Exception:
                page_title = ""
        if not page_title:
            try:
                _final_url, page_title = await _resolve_url_with_http_page_title(canonical_url, timeout_sec=12)
            except Exception:
                page_title = ""
        chosen_title, source, confidence = _select_best_title(
            embedded_title=embedded_title,
            page_title=page_title,
        )
        return ResolvedShareLink(
            raw_input=raw_text,
            extracted_url=extracted_url,
            resolved_url=canonical_url,
            platform=platform,
            canonical_id=canonical_id,
            resolver="canonical-no-redirect",
            title=chosen_title,
            title_source=source,
            title_confidence=confidence,
            content_type=content_type,
        )

    if platform == "bilibili" and canonical_id and ("b23.tv" not in host):
        page_title = ""
        if PLAYWRIGHT_AVAILABLE:
            try:
                _final_url, page_title = await _resolve_url_with_playwright(
                    canonical_url, timeout_ms=min(timeout_ms, 25000)
                )
            except Exception:
                page_title = ""
        if not page_title:
            try:
                _final_url, page_title = await _resolve_url_with_http_page_title(canonical_url, timeout_sec=12)
            except Exception:
                page_title = ""
        chosen_title, source, confidence = _select_best_title(
            embedded_title=embedded_title,
            page_title=page_title,
        )
        return ResolvedShareLink(
            raw_input=raw_text,
            extracted_url=extracted_url,
            resolved_url=canonical_url,
            platform=platform,
            canonical_id=canonical_id,
            resolver="canonical-no-redirect",
            title=chosen_title,
            title_source=source,
            title_confidence=confidence,
            content_type=content_type,
        )

    resolved_url = extracted_url
    resolved_title = ""
    resolver = "none"

    try:
        resolved_url, resolved_title = await _resolve_url_with_playwright(extracted_url, timeout_ms=timeout_ms)
        resolved_title = _normalize_page_title(resolved_title)
        resolver = "playwright"
    except Exception:
        try:
            resolved_url, resolved_title = await _resolve_url_with_http_page_title(extracted_url)
            resolved_title = _normalize_page_title(resolved_title)
        except Exception:
            resolved_url = await _resolve_url_with_http_redirect(extracted_url)
            resolved_title = ""
        resolver = "http-redirect"

    platform, canonical_id, canonical_url, content_type = _canonicalize_url(resolved_url)
    chosen_title, source, confidence = _select_best_title(
        embedded_title=embedded_title,
        page_title=resolved_title,
    )
    return ResolvedShareLink(
        raw_input=raw_text,
        extracted_url=extracted_url,
        resolved_url=canonical_url,
        platform=platform,
        canonical_id=canonical_id,
        resolver=resolver,
        title=chosen_title,
        title_source=source,
        title_confidence=confidence,
        content_type=content_type,
    )
