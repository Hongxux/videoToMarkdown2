"""
模块说明：下载流程编排层，负责统一处理分享链接解析、下载器分流、元数据输出。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from .platform_rules import (
    extract_bilibili_episode_index as _extract_bilibili_episode_index_from_rules,
    is_bilibili_host as _is_bilibili_host_from_rules,
)

_DOUYIN_RUNTIME_META_FILENAME = "douyin_runtime_meta.json"


@dataclass(frozen=True)
class DownloadFlowResult:
    success: bool
    video_path: str
    file_size_bytes: int
    duration_sec: float
    error_msg: str
    resolved_url: str = ""
    source_platform: str = ""
    canonical_id: str = ""
    link_resolver: str = ""
    video_title: str = ""
    content_type: str = "unknown"


def _infer_content_type(platform: str, url: str) -> str:
    if platform == "bilibili":
        return "video"
    if platform == "douyin":
        if "/note/" in str(url or ""):
            return "note"
        if "/video/" in str(url or ""):
            return "video"
    return "unknown"


def _normalize_title(raw_title: str) -> str:
    return " ".join(str(raw_title or "").split()).strip()


def _extract_bilibili_episode_index(*url_candidates: str) -> int:
    for candidate in url_candidates:
        episode_index = _extract_bilibili_episode_index_from_rules(str(candidate or ""))
        if episode_index > 0:
            return episode_index
    return 0


def _attach_bilibili_episode_index(video_url: str, episode_index: int) -> str:
    normalized_url = str(video_url or "").strip()
    if episode_index <= 0 or not normalized_url:
        return normalized_url
    try:
        parsed = urlparse(normalized_url)
    except Exception:
        return normalized_url
    if parsed.scheme.lower() not in {"http", "https"}:
        return normalized_url
    if not _is_bilibili_host_from_rules(parsed.netloc):
        return normalized_url

    query_values = parse_qs(parsed.query or "", keep_blank_values=True)
    query_values["p"] = [str(episode_index)]
    return urlunparse(parsed._replace(query=urlencode(query_values, doseq=True)))


def _read_runtime_title_from_downloader_meta(*, task_dir: str, platform: str) -> str:
    if str(platform or "").lower() != "douyin":
        return ""
    if not task_dir:
        return ""

    meta_path = os.path.join(task_dir, _DOUYIN_RUNTIME_META_FILENAME)
    if not os.path.exists(meta_path):
        return ""
    try:
        with open(meta_path, "r", encoding="utf-8") as file_obj:
            payload = json.load(file_obj)
    except Exception:
        return ""
    if not isinstance(payload, dict):
        return ""
    return _normalize_title(str(payload.get("title", "") or ""))


async def run_download_flow(
    *,
    task_id: str,
    raw_video_input: str,
    config: Dict[str, Any],
    resolve_share_link: Callable[..., Any],
    build_task_dir_encoding_source: Callable[[str], str],
    get_primary_storage_root: Callable[[], str],
    is_douyin_url: Callable[[str], bool],
    douyin_downloader: Callable[..., Any],
    load_download_video_options: Callable[[Dict[str, Any]], Dict[str, Any]],
    video_processor_cls: Any,
    get_video_duration: Callable[[str], float],
    write_video_meta_file: Callable[..., Any],
    logger: Any,
) -> DownloadFlowResult:
    video_url = str(raw_video_input or "")
    resolved_platform = ""
    resolved_id = ""
    resolved_title = ""
    resolved_by = ""
    resolved_content_type = "unknown"
    resolved_extracted_url = ""

    try:
        try:
            resolved_share = await resolve_share_link(raw_video_input)
            video_url = str(resolved_share.resolved_url or video_url)
            resolved_platform = str(resolved_share.platform or "")
            resolved_id = str(resolved_share.canonical_id or "")
            resolved_title = str(resolved_share.title or "")
            resolved_by = str(resolved_share.resolver or "")
            resolved_extracted_url = str(getattr(resolved_share, "extracted_url", "") or "")
            resolved_content_type = str(
                getattr(resolved_share, "content_type", "") or _infer_content_type(resolved_platform, video_url)
            )

            # B站分P链接必须保留 p 参数，否则合集批量提交会退化为同一集下载。
            if resolved_platform == "bilibili":
                requested_episode_index = _extract_bilibili_episode_index(
                    resolved_extracted_url,
                    raw_video_input,
                    video_url,
                )
                if requested_episode_index > 0:
                    preserved_url = _attach_bilibili_episode_index(video_url, requested_episode_index)
                    if preserved_url != video_url:
                        logger.info(
                            f"[{task_id}] Bilibili episode selector preserved: "
                            f"p={requested_episode_index}, url={preserved_url}"
                        )
                        video_url = preserved_url

            logger.info(
                f"[{task_id}] Share link resolved via {resolved_by}: "
                f"platform={resolved_platform}, extracted={resolved_share.extracted_url}, "
                f"resolved={video_url}, title={resolved_title or '(empty)'}, "
                f"content_type={resolved_content_type}"
            )
        except Exception as resolve_error:
            logger.warning(f"[{task_id}] Share link resolve skipped: {resolve_error}")

        if not resolved_platform and is_douyin_url(video_url):
            resolved_platform = "douyin"
            if not resolved_content_type:
                resolved_content_type = _infer_content_type(resolved_platform, video_url)

        task_dir_source_input = video_url
        if resolved_platform == "bilibili":
            task_dir_source_input = str(
                resolved_extracted_url
                or raw_video_input
                or video_url
            )
        task_dir_source = build_task_dir_encoding_source(task_dir_source_input)
        url_hash = hashlib.md5(task_dir_source.encode("utf-8")).hexdigest()
        if task_dir_source != str(video_url or ""):
            logger.info(f"[{task_id}] Bilibili task-dir key: {task_dir_source}")

        storage_root = get_primary_storage_root()
        task_dir = os.path.join(storage_root, url_hash)
        os.makedirs(task_dir, exist_ok=True)
        video_filename = "video"

        if is_douyin_url(video_url):
            video_path = await douyin_downloader(
                task_id=task_id,
                video_url=video_url,
                task_dir=task_dir,
                video_filename=video_filename,
            )
            downloader_runtime_title = ""
        else:
            download_options = load_download_video_options(config)
            downloader = video_processor_cls(**download_options)
            if download_options.get("cookies_file") or download_options.get("cookies_from_browser"):
                logger.info(
                    f"[{task_id}] Download auth enabled: "
                    f"cookies_file={bool(download_options.get('cookies_file'))}, "
                    f"cookies_from_browser={download_options.get('cookies_from_browser') or ''}, "
                    f"proxy={download_options.get('proxy') or ''}"
                )
            if download_options.get("external_downloader"):
                logger.info(
                    f"[{task_id}] External downloader enabled: "
                    f"{download_options.get('external_downloader')} "
                    f"args={download_options.get('external_downloader_args') or []}"
                )
            video_path = await asyncio.to_thread(
                downloader.download,
                url=video_url,
                output_dir=task_dir,
                filename=video_filename,
            )
            downloader_runtime_title = _normalize_title(getattr(downloader, "last_video_title", ""))

        if not resolved_title and downloader_runtime_title:
            resolved_title = downloader_runtime_title
            resolved_by = f"{resolved_by}+downloader-runtime" if resolved_by else "downloader-runtime"
            logger.info(f"[{task_id}] Video title recovered from downloader runtime: {resolved_title}")

        if not resolved_title:
            runtime_title = _read_runtime_title_from_downloader_meta(
                task_dir=task_dir,
                platform=resolved_platform,
            )
            if runtime_title:
                resolved_title = runtime_title
                resolved_by = f"{resolved_by}+downloader-runtime-meta" if resolved_by else "downloader-runtime-meta"
                logger.info(f"[{task_id}] Video title recovered from runtime metadata: {resolved_title}")

        duration_sec = float(get_video_duration(video_path) or 0.0)
        file_size = int(os.path.getsize(video_path))

        if not resolved_content_type:
            resolved_content_type = _infer_content_type(resolved_platform, video_url)

        try:
            write_video_meta_file(
                task_dir=task_dir,
                video_path=video_path,
                source_url=raw_video_input,
                resolved_url=video_url,
                platform=resolved_platform,
                canonical_id=resolved_id,
                title=resolved_title,
                resolver=resolved_by,
            )
        except Exception as meta_error:
            logger.warning(f"[{task_id}] Failed to write video_meta.json: {meta_error}")

        logger.info(f"[{task_id}] Video saved to: {video_path}")
        return DownloadFlowResult(
            success=True,
            video_path=video_path,
            file_size_bytes=file_size,
            duration_sec=duration_sec,
            error_msg="",
            resolved_url=video_url,
            source_platform=resolved_platform,
            canonical_id=resolved_id,
            link_resolver=resolved_by,
            video_title=resolved_title,
            content_type=resolved_content_type or _infer_content_type(resolved_platform, video_url),
        )
    except Exception as exc:
        logger.error(f"[{task_id}] DownloadVideo failed: {exc}")
        return DownloadFlowResult(
            success=False,
            video_path="",
            file_size_bytes=0,
            duration_sec=0.0,
            error_msg=str(exc),
            resolved_url=video_url,
            source_platform=resolved_platform,
            canonical_id=resolved_id,
            link_resolver=resolved_by,
            video_title=resolved_title,
            content_type=resolved_content_type or _infer_content_type(resolved_platform, video_url),
        )
