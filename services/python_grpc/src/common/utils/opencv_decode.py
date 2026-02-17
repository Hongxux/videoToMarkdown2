"""
OpenCV 视频解码兼容工具。

职责：
1) 判定视频是否“可真正解码”（而非仅容器可打开）。
2) 对不可解码输入（如 AV1）按需转码为 H.264 兜底。
3) 提供进程内路径缓存，避免重复探测与重复转码。
"""

from __future__ import annotations

import hashlib
import logging
import os
import shutil
import subprocess
import threading
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import cv2

_PATH_CACHE_LOCK = threading.Lock()
_PATH_CACHE: Dict[str, Tuple[str, int, int]] = {}
_ASYNC_TRANSCODE_LOCK = threading.Lock()
_ASYNC_TRANSCODE_JOBS: Dict[str, threading.Thread] = {}


def _is_truthy_env(name: str, default: str = "0") -> bool:
    value = str(os.getenv(name, default) or "").strip().lower()
    return value in {"1", "true", "yes", "y", "on"}


def _is_av1_codec(codec_name: str) -> bool:
    normalized = str(codec_name or "").strip().lower()
    return normalized in {"av1", "av01"} or normalized.startswith("av1.")


def _allow_inline_transcode_default() -> bool:
    """
    做什么：读取“是否允许请求内同步整段转码”的默认开关。
    为什么：长视频 AV1 在热路径同步转码会导致数分钟阻塞。
    权衡：默认关闭以优先保证响应性；可通过环境变量显式开启。
    """
    return _is_truthy_env("OPENCV_DECODE_ALLOW_INLINE_TRANSCODE", "0")


def _enable_async_transcode_default() -> bool:
    """
    做什么：读取“是否允许后台异步预转码”的默认开关。
    为什么：在不阻塞当前请求的前提下，给后续请求准备可解码缓存。
    权衡：会产生后台 CPU/I/O 开销；可通过环境变量关闭。
    """
    return _is_truthy_env("OPENCV_DECODE_ENABLE_ASYNC_TRANSCODE", "1")


def _safe_stat_fingerprint(video_path: str) -> Tuple[int, int]:
    try:
        stat = Path(video_path).stat()
        return int(stat.st_size), int(stat.st_mtime_ns)
    except Exception:
        return 0, 0


def resolve_ffmpeg_bin() -> Optional[str]:
    """
    做什么：解析可用 ffmpeg 路径。
    为什么：解码失败时需要转码兜底。
    权衡：优先 PATH，同时兼容历史固定安装路径。
    """
    candidates = [
        str(os.getenv("FFMPEG_BIN", "") or "").strip(),
        str(os.getenv("FFMPEG_PATH", "") or "").strip(),
        "ffmpeg",
        r"D:\New_ANACONDA\envs\whisper_env\Library\bin\ffmpeg.exe",
    ]
    for candidate in candidates:
        if not candidate:
            continue
        if shutil.which(candidate):
            return candidate
        if Path(candidate).exists():
            return candidate
    return None


def resolve_ffprobe_bin() -> Optional[str]:
    """
    做什么：解析可用 ffprobe 路径。
    为什么：在打开 OpenCV 之前先探测主视频编码，避免已知不兼容编码触发噪声报错。
    权衡：增加一次轻量子进程调用，换取更干净的日志与更早回退。
    """
    candidates = [
        str(os.getenv("FFPROBE_BIN", "") or "").strip(),
        str(os.getenv("FFPROBE_PATH", "") or "").strip(),
        "ffprobe",
        r"D:\New_ANACONDA\envs\whisper_env\Library\bin\ffprobe.exe",
    ]
    for candidate in candidates:
        if not candidate:
            continue
        if shutil.which(candidate):
            return candidate
        if Path(candidate).exists():
            return candidate
    return None


def probe_primary_video_codec(video_path: str, timeout_sec: int = 8) -> str:
    """
    做什么：返回输入视频第一路视频流编码名（小写）。
    为什么：对 AV1 等已知 OpenCV/FFmpeg 构建不兼容场景先行转码，避免先触发 OpenCV 错误日志。
    权衡：若 ffprobe 不可用或探测失败，返回空字符串并继续走原有可读性探测路径。
    """
    ffprobe_bin = resolve_ffprobe_bin()
    if not ffprobe_bin:
        return ""
    command = [
        ffprobe_bin,
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-show_entries",
        "stream=codec_name",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(video_path),
    ]
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=max(2, int(timeout_sec)),
        )
    except Exception:
        return ""
    if result.returncode != 0:
        return ""
    codec = str(result.stdout or "").strip().lower()
    return codec


def get_video_basic_metadata(video_path: str) -> Tuple[float, float, int, int]:
    """
    做什么：获取视频基础元数据（FPS, 时长, 宽, 高）。
    为什么：当 OpenCV 无法打开文件时（如 AV1），需要通过 ffprobe 获取元数据以支持虚拟打开模式。
    """
    ffprobe_bin = resolve_ffprobe_bin()
    if not ffprobe_bin:
        return 0.0, 0.0, 0, 0

    command = [
        ffprobe_bin,
        "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "stream=r_frame_rate,duration,width,height",
        "-of", "default=noprint_wrappers=1:nokey=1",
        str(video_path),
    ]
    
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode != 0:
            return 0.0, 0.0, 0, 0
            
        # Output order is not guaranteed by -show_entries with default format in older versions, 
        # but typically it follows the order. 
        # Use json or explicit parsing is safer, but keeping it simple for now matching existing style.
        # Wait, simple format output order IS guaranteed by request order in recent versions, but let's parse safely.
        # Actually existing function uses simple format. Let's stick to it but parse carefully.
        # To be safer let's use json.
    except Exception:
        return 0.0, 0.0, 0, 0

    # Retry with JSON for robust parsing
    command = [
        ffprobe_bin,
        "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "stream=r_frame_rate,duration,width,height",
        "-of", "json",
        str(video_path),
    ]
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode != 0:
            return 0.0, 0.0, 0, 0
            
        import json
        data = json.loads(result.stdout)
        stream = data.get("streams", [])[0]
        
        width = int(stream.get("width", 0))
        height = int(stream.get("height", 0))
        duration = float(stream.get("duration", 0.0))
        
        fps_str = stream.get("r_frame_rate", "0/0")
        if "/" in fps_str:
            num, den = fps_str.split("/")
            fps = float(num) / float(den) if float(den) > 0 else 0.0
        else:
            fps = float(fps_str)
            
        return fps, duration, width, height
    except Exception:
        return 0.0, 0.0, 0, 0


def probe_capture_readable(cap: Any) -> bool:
    """
    做什么：探测 capture 是否可解码出至少 1 帧。
    为什么：`isOpened()` 为真并不保证 `read()` 成功。
    权衡：增加一次轻量 read 开销，换取初始化期确定性。
    """
    try:
        if cap is None or not hasattr(cap, "isOpened") or not cap.isOpened():
            return False

        original_pos = None
        if hasattr(cap, "get"):
            try:
                original_pos = float(cap.get(cv2.CAP_PROP_POS_FRAMES))
            except Exception:
                original_pos = None

        if hasattr(cap, "set"):
            try:
                cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
            except Exception:
                pass

        ret, frame = cap.read()
        readable = bool(ret and frame is not None and getattr(frame, "size", 0) > 0)

        if hasattr(cap, "set"):
            try:
                if isinstance(original_pos, (int, float)) and original_pos >= 0:
                    cap.set(cv2.CAP_PROP_POS_FRAMES, original_pos)
                else:
                    cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
            except Exception:
                pass

        return readable
    except Exception:
        return False


def build_decode_fallback_path(source_video_path: str) -> Path:
    """
    做什么：构造稳定转码缓存路径。
    为什么：同一源文件重复使用，避免重复转码。
    权衡：会在源目录创建 `_opencv_decode_fallback` 子目录。
    """
    source = Path(source_video_path)
    try:
        stat = source.stat()
        fingerprint = f"{source.resolve()}::{stat.st_size}::{int(stat.st_mtime_ns)}"
    except Exception:
        fingerprint = str(source)
    digest = hashlib.md5(fingerprint.encode("utf-8", errors="ignore")).hexdigest()[:12]
    cache_dir = source.parent / "_opencv_decode_fallback"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / f"{source.stem}_{digest}_h264.mp4"


def transcode_to_h264_for_opencv(
    source_video_path: str,
    *,
    output_video_path: Optional[str] = None,
    timeout_sec: int = 300,
    logger: Optional[logging.Logger] = None,
) -> Optional[str]:
    """
    做什么：将视频转码为 OpenCV 更稳定可解码的 H.264。
    为什么：处理 AV1 等在当前运行环境不可读的输入。
    权衡：会增加一次转码耗时，仅在解码探测失败时触发。
    """
    log = logger or logging.getLogger(__name__)
    ffmpeg_bin = resolve_ffmpeg_bin()
    if not ffmpeg_bin:
        log.warning("ffmpeg not found, cannot transcode fallback for: %s", source_video_path)
        return None

    output_path = Path(output_video_path) if output_video_path else build_decode_fallback_path(source_video_path)
    if output_path.exists() and output_path.stat().st_size > 0:
        return str(output_path)

    command = [
        ffmpeg_bin,
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-i",
        source_video_path,
        "-map",
        "0:v:0",
        "-map",
        "0:a?",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "22",
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "aac",
        "-b:a",
        "96k",
        "-movflags",
        "+faststart",
        str(output_path),
    ]

    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=max(10, int(timeout_sec)),
        )
    except Exception as exc:
        log.warning("ffmpeg transcode exception for %s: %s", source_video_path, exc)
        return None

    if result.returncode != 0:
        log.warning(
            "ffmpeg transcode failed for %s: rc=%s, err=%s",
            source_video_path,
            result.returncode,
            str(result.stderr or "").strip()[:300],
        )
        return None

    if not output_path.exists() or output_path.stat().st_size <= 0:
        log.warning("ffmpeg transcode generated empty file for %s", source_video_path)
        return None
    return str(output_path)


def transcode_video_segment(
    source_path: str,
    output_path: str,
    start_sec: float,
    duration_sec: float,
    timeout_sec: int = 60,
    logger: Optional[logging.Logger] = None,
) -> bool:
    """
    做什么：截取并转码视频的特定片段。
    为什么：对于不支持的编码（如 AV1），仅转码需要的片段比全量转码更高效。
    注意：使用 -ss (fast seek) 放在 -i 之前。
    """
    log = logger or logging.getLogger(__name__)
    ffmpeg_bin = resolve_ffmpeg_bin()
    if not ffmpeg_bin:
        return False

    # Ensure output dir exists
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    command = [
        ffmpeg_bin,
        "-hide_banner",
        "-loglevel", "error",
        "-y",
        "-ss", f"{start_sec:.3f}",
        "-t", f"{duration_sec:.3f}",
        "-i", source_path,
        "-c:v", "libx264",
        "-preset", "ultrafast",  # Sacrifice some compression for speed
        "-crf", "23",
        "-pix_fmt", "yuv420p",
        "-an",  # Drop audio for screenshot purpose
        str(output_path),
    ]

    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=max(5, int(timeout_sec)),
        )
        if result.returncode != 0:
            log.warning(
                "ffmpeg segment transcode failed: %s",
                str(result.stderr or "").strip()[:200]
            )
            return False
        return True
    except Exception as e:
        log.warning("ffmpeg segment transcode exception: %s", e)
        return False


def _schedule_async_transcode(
    source_video_path: str,
    *,
    output_video_path: str,
    timeout_sec: int,
    logger: Optional[logging.Logger] = None,
) -> bool:
    """
    做什么：在后台线程启动一次 AV1->H.264 预转码任务。
    为什么：避免在当前请求中阻塞，同时为后续请求准备可复用缓存。
    权衡：仅进程内去重；多进程场景仍可能出现重复转码。
    """
    log = logger or logging.getLogger(__name__)
    source_path = os.path.abspath(source_video_path)
    timeout_value = max(10, int(timeout_sec))

    with _ASYNC_TRANSCODE_LOCK:
        dead_keys = [key for key, job in _ASYNC_TRANSCODE_JOBS.items() if not job.is_alive()]
        for key in dead_keys:
            _ASYNC_TRANSCODE_JOBS.pop(key, None)

        existing_job = _ASYNC_TRANSCODE_JOBS.get(source_path)
        if existing_job is not None and existing_job.is_alive():
            return False

        def _runner() -> None:
            try:
                log.info(
                    "Async OpenCV decode fallback transcode started: source=%s, output=%s, timeout=%ss",
                    source_path,
                    output_video_path,
                    timeout_value,
                )
                result_path = transcode_to_h264_for_opencv(
                    source_path,
                    output_video_path=output_video_path,
                    timeout_sec=timeout_value,
                    logger=log,
                )
                if result_path:
                    log.info(
                        "Async OpenCV decode fallback transcode completed: source=%s, output=%s",
                        source_path,
                        result_path,
                    )
                else:
                    log.warning(
                        "Async OpenCV decode fallback transcode failed: source=%s",
                        source_path,
                    )
            finally:
                with _ASYNC_TRANSCODE_LOCK:
                    _ASYNC_TRANSCODE_JOBS.pop(source_path, None)

        worker = threading.Thread(
            target=_runner,
            name=f"opencv-decode-transcode-{hash(source_path) & 0xFFFF:x}",
            daemon=True,
        )
        _ASYNC_TRANSCODE_JOBS[source_path] = worker
        worker.start()
        return True


def ensure_opencv_readable_video_path(
    video_path: str,
    *,
    timeout_sec: int = 300,
    logger: Optional[logging.Logger] = None,
    allow_inline_transcode: Optional[bool] = None,
    enable_async_transcode: Optional[bool] = None,
) -> Tuple[str, bool]:
    """
    做什么：确保返回一个 OpenCV 可解码的视频路径。
    为什么：统一“不可解码输入”的兜底处理，避免业务层重复实现。
    权衡：失败时回退原路径，调用方可继续按既有容错逻辑处理。
    """
    log = logger or logging.getLogger(__name__)
    if not video_path:
        return video_path, False

    source_path = os.path.abspath(video_path)
    allow_inline = _allow_inline_transcode_default() if allow_inline_transcode is None else bool(allow_inline_transcode)
    allow_async = _enable_async_transcode_default() if enable_async_transcode is None else bool(enable_async_transcode)

    size, mtime_ns = _safe_stat_fingerprint(source_path)
    cache_key = source_path
    with _PATH_CACHE_LOCK:
        cached = _PATH_CACHE.get(cache_key)
        if cached is not None:
            cached_path, cached_size, cached_mtime = cached
            if cached_size == size and cached_mtime == mtime_ns and Path(cached_path).exists():
                return cached_path, os.path.abspath(cached_path) != source_path

    fallback_path = build_decode_fallback_path(source_path)
    if fallback_path.exists() and fallback_path.stat().st_size > 0:
        cap = cv2.VideoCapture(str(fallback_path))
        try:
            if probe_capture_readable(cap):
                resolved = str(fallback_path)
                with _PATH_CACHE_LOCK:
                    _PATH_CACHE[cache_key] = (resolved, size, mtime_ns)
                return resolved, True
        finally:
            cap.release()

    codec_name = probe_primary_video_codec(source_path)
    if _is_av1_codec(codec_name) and allow_inline:
        transcoded_path = transcode_to_h264_for_opencv(
            source_path,
            output_video_path=str(fallback_path),
            timeout_sec=timeout_sec,
            logger=log,
        )
        if transcoded_path:
            cap_fallback = cv2.VideoCapture(transcoded_path)
            try:
                if probe_capture_readable(cap_fallback):
                    with _PATH_CACHE_LOCK:
                        _PATH_CACHE[cache_key] = (transcoded_path, size, mtime_ns)
                    log.info(
                        "OpenCV decode fallback applied by codec probe: codec=%s, source=%s, fallback=%s",
                        codec_name,
                        source_path,
                        transcoded_path,
                    )
                    return transcoded_path, True
            finally:
                cap_fallback.release()

    cap = cv2.VideoCapture(source_path)
    try:
        if probe_capture_readable(cap):
            with _PATH_CACHE_LOCK:
                _PATH_CACHE[cache_key] = (source_path, size, mtime_ns)
            return source_path, False
    finally:
        cap.release()

    if allow_inline:
        transcoded_path = transcode_to_h264_for_opencv(
            source_path,
            output_video_path=str(fallback_path),
            timeout_sec=timeout_sec,
            logger=log,
        )
        if transcoded_path:
            cap_fallback = cv2.VideoCapture(transcoded_path)
            try:
                if probe_capture_readable(cap_fallback):
                    with _PATH_CACHE_LOCK:
                        _PATH_CACHE[cache_key] = (transcoded_path, size, mtime_ns)
                    log.warning(
                        "OpenCV decode fallback applied: source=%s, fallback=%s",
                        source_path,
                        transcoded_path,
                    )
                    return transcoded_path, True
            finally:
                cap_fallback.release()
    else:
        if _is_av1_codec(codec_name) and allow_async:
            _schedule_async_transcode(
                source_path,
                output_video_path=str(fallback_path),
                timeout_sec=timeout_sec,
                logger=log,
            )
        log.warning(
            "OpenCV decode fallback inline transcode disabled, keeping source path: source=%s, codec=%s",
            source_path,
            codec_name or "unknown",
        )

    with _PATH_CACHE_LOCK:
        _PATH_CACHE[cache_key] = (source_path, size, mtime_ns)
    return source_path, False


def open_video_capture_with_fallback(
    video_path: str,
    *,
    timeout_sec: int = 300,
    logger: Optional[logging.Logger] = None,
    allow_inline_transcode: Optional[bool] = None,
    enable_async_transcode: Optional[bool] = None,
) -> Tuple[Optional[cv2.VideoCapture], str, bool]:
    """
    做什么：打开可解码 capture，必要时自动切换到转码路径。
    为什么：让调用方直接获得可读句柄，减少重复模板代码。
    权衡：返回 `None` 时调用方应按原有降级逻辑处理。
    """
    resolved_path, used_fallback = ensure_opencv_readable_video_path(
        video_path,
        timeout_sec=timeout_sec,
        logger=logger,
        allow_inline_transcode=allow_inline_transcode,
        enable_async_transcode=enable_async_transcode,
    )
    cap = cv2.VideoCapture(resolved_path)
    if probe_capture_readable(cap):
        return cap, resolved_path, used_fallback
    cap.release()
    return None, resolved_path, used_fallback
