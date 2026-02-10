"""
视频相关工具。

职责：统一视频时长获取策略（ffprobe 优先，cv2 兜底）。
"""

from typing import Optional
import json
import os
import subprocess

from .numbers import to_float


def probe_video_duration_ffprobe(
    video_path: str,
    ffprobe_path: Optional[str] = None,
    timeout_sec: int = 15,
) -> float:
    """
    做什么：使用 ffprobe 获取视频时长（秒）。
    为什么：ffprobe 通常更准确且稳定，优先用于时长检测。
    权衡：依赖 ffprobe 可用；失败时需要上层处理。
    """
    cmd = [
        ffprobe_path or "ffprobe",
        "-v",
        "quiet",
        "-print_format",
        "json",
        "-show_format",
        "-i",
        video_path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_sec)
    if result.returncode != 0:
        raise RuntimeError(
            f"ffprobe failed (code={result.returncode}): {result.stderr.strip()[:500]}"
        )

    try:
        data = json.loads(result.stdout)
        duration_str = (data.get("format") or {}).get("duration")
        duration = to_float(duration_str)
        if duration is None or duration <= 0:
            raise ValueError(f"Invalid duration: {duration_str!r}")
        return float(duration)
    except Exception as exc:
        raise RuntimeError(f"Failed to parse ffprobe output: {exc}") from exc


def get_video_duration(
    video_path: str,
    default: Optional[float] = 0.0,
    ffprobe_path: Optional[str] = None,
    timeout_sec: int = 15,
    use_cv2_fallback: bool = True,
    raise_on_failure: bool = False,
) -> Optional[float]:
    """
    做什么：获取视频时长，优先 ffprobe，失败后回退 cv2。
    为什么：统一时长获取逻辑，避免各处实现漂移。
    权衡：若 ffprobe 和 cv2 均不可用，可选择返回默认值或抛错。
    """
    if not video_path or not os.path.exists(video_path):
        if raise_on_failure:
            raise FileNotFoundError(f"Video not found: {video_path}")
        return default

    last_error: Optional[Exception] = None
    try:
        return probe_video_duration_ffprobe(
            video_path=video_path,
            ffprobe_path=ffprobe_path,
            timeout_sec=timeout_sec,
        )
    except Exception as exc:
        last_error = exc

    if use_cv2_fallback:
        try:
            import cv2

            cap = cv2.VideoCapture(video_path)
            if not cap.isOpened():
                raise RuntimeError("cv2 failed to open video")
            fps = cap.get(cv2.CAP_PROP_FPS)
            frame_count = cap.get(cv2.CAP_PROP_FRAME_COUNT)
            cap.release()
            duration = frame_count / fps if fps > 0 else 0.0
            if duration > 0:
                return float(duration)
        except Exception as exc:
            last_error = exc

    if raise_on_failure and last_error is not None:
        raise last_error
    return default
