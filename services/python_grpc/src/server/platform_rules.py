"""
模块说明：视频平台识别与链接规则（抖音/B站）统一定义。
核心价值：消除解析层与 gRPC 入口的规则重复，避免后续域名/ID 规则升级时多处漏改。
"""

from __future__ import annotations

import re
from typing import Tuple
from urllib.parse import parse_qs, urlparse


def is_douyin_host(host: str) -> bool:
    normalized = (host or "").lower().split(":", 1)[0]
    if not normalized:
        return False
    suffixes = ("douyin.com", "iesdouyin.com", "amemv.com")
    return any(normalized == suffix or normalized.endswith(f".{suffix}") for suffix in suffixes)


def is_bilibili_host(host: str) -> bool:
    normalized = (host or "").lower().split(":", 1)[0]
    if not normalized:
        return False
    suffixes = ("bilibili.com", "b23.tv")
    return any(normalized == suffix or normalized.endswith(f".{suffix}") for suffix in suffixes)


def is_douyin_url(video_url: str) -> bool:
    if not video_url:
        return False
    try:
        parsed = urlparse(video_url)
    except Exception:
        return False
    if parsed.scheme.lower() not in {"http", "https"}:
        return False
    return is_douyin_host(parsed.netloc)


def extract_douyin_aweme_ref(url: str) -> Tuple[str, str]:
    """
    返回：
    - kind: video | note | unknown
    - id: 作品ID
    """
    text = url or ""
    match_video = re.search(r"/video/(\d+)", text)
    if match_video:
        return "video", match_video.group(1)

    match_note = re.search(r"/note/(\d+)", text)
    if match_note:
        return "note", match_note.group(1)

    for pattern in (r"modal_id=(\d+)", r"aweme_id=(\d+)", r"item_id=(\d+)"):
        match = re.search(pattern, text)
        if match:
            return "video", match.group(1)
    return "unknown", ""


def extract_bilibili_video_id(video_url: str) -> str:
    if not video_url:
        return ""

    parsed = urlparse(video_url)
    if not is_bilibili_host(parsed.netloc):
        return ""

    query = parse_qs(parsed.query or "", keep_blank_values=True)
    bvid = (query.get("bvid") or [""])[0].strip()
    if bvid:
        match = re.search(r"BV[0-9A-Za-z]{10}", bvid, flags=re.IGNORECASE)
        if match:
            return match.group(0)

    aid = (query.get("aid") or [""])[0].strip()
    if aid.isdigit():
        return f"AV{aid}"

    search_space = " ".join((parsed.path or "", parsed.query or "", parsed.fragment or ""))
    bv_match = re.search(r"BV[0-9A-Za-z]{10}", search_space, flags=re.IGNORECASE)
    if bv_match:
        return bv_match.group(0)

    av_match = re.search(
        r"(?:^|[^0-9A-Za-z])av(\d{1,20})(?:$|[^0-9A-Za-z])",
        search_space,
        flags=re.IGNORECASE,
    )
    if av_match:
        return f"AV{av_match.group(1)}"
    return ""


def build_task_dir_encoding_source(video_url: str) -> str:
    bilibili_video_id = extract_bilibili_video_id(video_url)
    if bilibili_video_id:
        return bilibili_video_id
    return str(video_url or "")

