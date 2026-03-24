"""VideoClip 导出相关能力。"""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path

from services.python_grpc.src.common.utils.video import get_video_duration

logger = logging.getLogger(__name__)


def get_video_duration_seconds(extractor, video_path: str) -> float:
    ffprobe_path = None
    if getattr(extractor, "ffmpeg_path", None) and "ffmpeg.exe" in extractor.ffmpeg_path:
        ffprobe_path = extractor.ffmpeg_path.replace("ffmpeg.exe", "ffprobe.exe")
    return float(
        get_video_duration(
            video_path,
            default=3600.0,
            ffprobe_path=ffprobe_path,
            use_cv2_fallback=True,
        )
    )


def export_clip_with_ffmpeg(extractor, video_path, start, end, fid, out_dir) -> str:
    output_dir = Path(out_dir or "video_clips")
    output_dir.mkdir(parents=True, exist_ok=True)
    duration = end - start

    if fid and str(fid).startswith("SU"):
        filename = f"{fid}_clip.mp4"
    else:
        filename = f"clip_{fid}_{start:.2f}s-{end:.2f}s.mp4"
    output_path = output_dir / filename

    if output_path.exists():
        logger.info("📑 [Disk reuse]: Found existing file -> %s", filename)
        return str(output_path)

    pattern = f"clip_*_{start:.2f}s-{end:.2f}s.mp4"
    existing_files = list(output_dir.glob(pattern))
    if existing_files:
        logger.info("📑 [Disk reuse]: Found existing file for time range -> %s", existing_files[0].name)
        return str(existing_files[0])

    cmd = [
        extractor.ffmpeg_path,
        "-i",
        video_path,
        "-ss",
        str(start),
        "-t",
        str(duration),
        "-c:v",
        "libx264",
        "-preset",
        "superfast",
        "-crf",
        "23",
        "-c:a",
        "aac",
        "-y",
        str(output_path),
    ]
    try:
        logger.info("Exporting Physical Anchor: %s", filename)
        subprocess.run(cmd, capture_output=True, check=True)
        return str(output_path)
    except Exception as error:  # noqa: BLE001
        logger.error("FFmpeg Error for %s: %s", fid, error)
        return ""


def export_poster_at_timestamp(extractor, video_path: str, timestamp: float, fid: str, output_dir: str) -> str:
    target_dir = Path(output_dir or "video_clips")
    target_dir.mkdir(parents=True, exist_ok=True)

    if fid and str(fid).startswith("SU"):
        filename = f"{fid}_poster.png"
    else:
        filename = f"poster_{fid}_{timestamp:.2f}s.png"
    output_path = target_dir / filename

    if output_path.exists():
        return str(output_path)

    cmd = [
        extractor.ffmpeg_path,
        "-ss",
        str(timestamp),
        "-i",
        video_path,
        "-vframes",
        "1",
        "-q:v",
        "2",
        "-y",
        str(output_path),
    ]
    try:
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        logger.info("🖼️ [Poster Export]: %s", filename)
        return str(output_path)
    except Exception as error:  # noqa: BLE001
        logger.error("Poster Export Error: %s", error)
        return ""
