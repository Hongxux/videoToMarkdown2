"""
模块说明：下载流程编排层，负责统一处理分享链接解析、下载器分流、元数据输出。
"""

from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse
from services.python_grpc.src.common.utils.hash_policy import md5_text_compat

from .platform_rules import (
    extract_bilibili_episode_index as _extract_bilibili_episode_index_from_rules,
    extract_bilibili_video_id as _extract_bilibili_video_id_from_rules,
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


def _is_youtube_url(video_url: str) -> bool:
    lower_url = str(video_url or "").lower()
    return "youtube.com/" in lower_url or "youtu.be/" in lower_url


def _extract_bilibili_episode_index(*url_candidates: str) -> int:
    for candidate in url_candidates:
        episode_index = _extract_bilibili_episode_index_from_rules(str(candidate or ""))
        if episode_index > 0:
            return episode_index
    return 0


def _extract_bilibili_url_with_query(*url_candidates: str) -> str:
    for candidate in url_candidates:
        normalized_url = str(candidate or "").strip()
        if not normalized_url:
            continue
        try:
            parsed = urlparse(normalized_url)
        except Exception:
            continue
        if parsed.scheme.lower() not in {"http", "https"}:
            continue
        if not _is_bilibili_host_from_rules(parsed.netloc):
            continue
        if not parsed.query:
            continue
        if not _extract_bilibili_video_id_from_rules(normalized_url):
            continue
        return normalized_url
    return ""


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


def _to_int(value: Any, default: int) -> int:
    try:
        return int(str(value).strip())
    except Exception:
        return default


def _to_float(value: Any, default: float) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return default


def _load_download_retry_policy(config: Dict[str, Any]) -> Dict[str, Any]:
    video_cfg = config.get("video", {}) if isinstance(config, dict) else {}
    if not isinstance(video_cfg, dict):
        video_cfg = {}

    attempts = _to_int(video_cfg.get("download_retry_attempts", 3), 3)
    if attempts < 1:
        attempts = 1
    if attempts > 8:
        attempts = 8

    base_delay_sec = _to_float(video_cfg.get("download_retry_base_delay_sec", 1.0), 1.0)
    if base_delay_sec < 0:
        base_delay_sec = 0.0

    max_delay_sec = _to_float(video_cfg.get("download_retry_max_delay_sec", 16.0), 16.0)
    if max_delay_sec < 0:
        max_delay_sec = 0.0
    if max_delay_sec < base_delay_sec:
        max_delay_sec = base_delay_sec

    return {
        "attempts": attempts,
        "base_delay_sec": base_delay_sec,
        "max_delay_sec": max_delay_sec,
    }


def _compute_backoff_delay_sec(*, retry_index: int, base_delay_sec: float, max_delay_sec: float) -> float:
    if retry_index <= 0 or base_delay_sec <= 0:
        return 0.0
    delay_sec = base_delay_sec * (2 ** (retry_index - 1))
    if max_delay_sec > 0:
        delay_sec = min(delay_sec, max_delay_sec)
    if delay_sec < 0:
        return 0.0
    return delay_sec


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
                preserved_query_url = _extract_bilibili_url_with_query(
                    resolved_extracted_url,
                    raw_video_input,
                )
                if preserved_query_url:
                    if preserved_query_url != video_url:
                        logger.info(f"[{task_id}] Bilibili query preserved for download URL: {preserved_query_url}")
                    video_url = preserved_query_url
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
        url_hash = md5_text_compat(task_dir_source)
        if task_dir_source != str(video_url or ""):
            logger.info(f"[{task_id}] Bilibili task-dir key: {task_dir_source}")

        storage_root = get_primary_storage_root()
        task_dir = os.path.join(storage_root, url_hash)
        os.makedirs(task_dir, exist_ok=True)
        video_filename = "video"

        retry_policy = _load_download_retry_policy(config)
        max_download_attempts = int(retry_policy["attempts"])
        backoff_base_sec = float(retry_policy["base_delay_sec"])
        backoff_max_sec = float(retry_policy["max_delay_sec"])

        is_douyin_download = is_douyin_url(video_url)
        download_options = load_download_video_options(config) if not is_douyin_download else {}
        if not is_douyin_download and not _is_youtube_url(video_url):
            download_options = dict(download_options)
            download_options["cookies_file"] = None
            download_options["cookies_from_browser"] = None
            download_options["youtube_download_proxy"] = None
            download_options["proxy"] = None
        if not is_douyin_download and _is_youtube_url(video_url):
            try:
                probe_processor = video_processor_cls(**download_options)
                info = probe_processor.probe_video_info(video_url)
                if isinstance(info, dict) and info:
                    if not resolved_platform or resolved_platform == "unknown":
                        resolved_platform = "youtube"
                    if not resolved_title:
                        resolved_title = str(info.get("title") or "")
                    if resolved_content_type == "unknown":
                        resolved_content_type = "playlist" if info.get("entries") else "video"
                    logger.info(
                        f"[{task_id}] YouTube info resolved via yt-dlp: "
                        f"title={resolved_title or '(empty)'}, content_type={resolved_content_type}"
                    )
            except Exception as exc:
                logger.warning(f"[{task_id}] YouTube info probe skipped: {exc}")
        if not is_douyin_download and (
            download_options.get("cookies_file") or download_options.get("cookies_from_browser")
        ):
            logger.info(
                f"[{task_id}] Download auth enabled: "
                f"cookies_file={bool(download_options.get('cookies_file'))}, "
                f"cookies_from_browser={download_options.get('cookies_from_browser') or ''}, "
                f"proxy={download_options.get('proxy') or ''}"
            )
        if not is_douyin_download and _is_youtube_url(video_url) and download_options.get("youtube_download_proxy"):
            logger.info(
                f"[{task_id}] YouTube proxy enabled: {download_options.get('youtube_download_proxy')}"
            )
        if not is_douyin_download and download_options.get("external_downloader"):
            logger.info(
                f"[{task_id}] External downloader enabled: "
                f"{download_options.get('external_downloader')} "
                f"args={download_options.get('external_downloader_args') or []}"
            )

        video_path = ""
        downloader_runtime_title = ""
        for attempt in range(1, max_download_attempts + 1):
            if attempt > 1:
                logger.warning(
                    f"[{task_id}] Download retry attempt {attempt}/{max_download_attempts} started"
                )
            try:
                if is_douyin_download:
                    video_path = await douyin_downloader(
                        task_id=task_id,
                        video_url=video_url,
                        task_dir=task_dir,
                        video_filename=video_filename,
                    )
                    downloader_runtime_title = ""
                else:
                    downloader = video_processor_cls(**download_options)
                    video_path = await asyncio.to_thread(
                        downloader.download,
                        url=video_url,
                        output_dir=task_dir,
                        filename=video_filename,
                    )
                    downloader_runtime_title = _normalize_title(getattr(downloader, "last_video_title", ""))
                break
            except Exception as download_error:
                if attempt >= max_download_attempts:
                    raise
                retry_index = attempt
                delay_sec = _compute_backoff_delay_sec(
                    retry_index=retry_index,
                    base_delay_sec=backoff_base_sec,
                    max_delay_sec=backoff_max_sec,
                )
                logger.warning(
                    f"[{task_id}] Download attempt {attempt}/{max_download_attempts} failed: {download_error}; "
                    f"retry in {delay_sec:.2f}s"
                )
                if delay_sec > 0:
                    await asyncio.sleep(delay_sec)

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
