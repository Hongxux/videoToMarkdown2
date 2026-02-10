"""
模块说明：VL 侧 FFmpeg 导出与拼接工具。

执行逻辑：
1) 导出区间 clip。
2) 导出关键帧图片。
3) 多段片段拼接。

核心价值：将 FFmpeg 细节从业务编排中剥离，降低主类复杂度。
"""

import asyncio
from pathlib import Path
from typing import List, Tuple


async def export_clip_asset_with_ffmpeg(
    *,
    video_path: str,
    start_sec: float,
    end_sec: float,
    output_path: Path,
    logger,
) -> bool:
    """按时间区间导出视频片段。"""
    if end_sec <= start_sec:
        return False

    output_path.parent.mkdir(parents=True, exist_ok=True)
    duration_sec = end_sec - start_sec
    command = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-ss",
        f"{start_sec:.6f}",
        "-i",
        video_path,
        "-t",
        f"{duration_sec:.6f}",
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
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        "-movflags",
        "+faststart",
        str(output_path),
    ]
    process = await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await process.communicate()
    if process.returncode != 0:
        logger.warning(
            f"[VL-Tutorial] step clip export failed: file={output_path.name}, rc={process.returncode}, "
            f"err={stderr.decode('utf-8', errors='ignore')[:300]}"
        )
        return False
    return output_path.exists() and output_path.stat().st_size > 0


async def export_keyframe_with_ffmpeg(
    *,
    video_path: str,
    timestamp_sec: float,
    output_path: Path,
    logger,
) -> bool:
    """导出指定时间点关键帧。"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    command = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-ss",
        f"{max(0.0, float(timestamp_sec)):.6f}",
        "-i",
        video_path,
        "-frames:v",
        "1",
    ]
    if output_path.suffix.lower() in {".jpg", ".jpeg"}:
        command.extend(["-q:v", "2"])
    command.append(str(output_path))

    process = await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await process.communicate()
    if process.returncode != 0:
        logger.warning(
            f"[VL-Tutorial] keyframe export failed: file={output_path.name}, rc={process.returncode}, "
            f"err={stderr.decode('utf-8', errors='ignore')[:300]}"
        )
        return False
    return output_path.exists() and output_path.stat().st_size > 0


async def concat_segments_with_ffmpeg(
    *,
    source_clip_path: str,
    output_clip_path: str,
    segments: List[Tuple[float, float]],
    logger,
) -> bool:
    """通过 ffmpeg concat demuxer 将多个区段拼接为新片段。"""
    if not segments:
        return False

    out_path = Path(output_clip_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    temp_dir = out_path.parent / f"{out_path.stem}_parts"
    temp_dir.mkdir(parents=True, exist_ok=True)

    part_paths: List[Path] = []
    try:
        for index, (start_sec, end_sec) in enumerate(segments):
            if end_sec <= start_sec:
                continue
            duration_sec = end_sec - start_sec
            part_path = temp_dir / f"part_{index:03d}.mp4"
            part_command = [
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "error",
                "-y",
                "-ss",
                f"{start_sec:.6f}",
                "-i",
                source_clip_path,
                "-t",
                f"{duration_sec:.6f}",
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
                "-c:a",
                "aac",
                "-b:a",
                "128k",
                "-movflags",
                "+faststart",
                str(part_path),
            ]
            process = await asyncio.create_subprocess_exec(
                *part_command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await process.communicate()
            if process.returncode != 0 or not part_path.exists() or part_path.stat().st_size <= 0:
                logger.warning(
                    f"[VL-PrePrune] ffmpeg part cut failed: part={index}, rc={process.returncode}, err={stderr.decode('utf-8', errors='ignore')[:300]}"
                )
                return False
            part_paths.append(part_path)

        if not part_paths:
            return False

        concat_file = temp_dir / "concat_list.txt"
        concat_lines = []
        for part_path in part_paths:
            safe_path = str(part_path).replace("'", "'\\''")
            concat_lines.append(f"file '{safe_path}'")
        concat_file.write_text("\n".join(concat_lines), encoding="utf-8")

        concat_command = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            str(concat_file),
            "-c",
            "copy",
            str(out_path),
        ]
        process = await asyncio.create_subprocess_exec(
            *concat_command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await process.communicate()
        if process.returncode != 0 or not out_path.exists() or out_path.stat().st_size <= 0:
            logger.warning(
                f"[VL-PrePrune] ffmpeg concat failed: rc={process.returncode}, err={stderr.decode('utf-8', errors='ignore')[:300]}"
            )
            return False
        return True
    finally:
        try:
            for part_path in part_paths:
                if part_path.exists():
                    part_path.unlink(missing_ok=True)
            concat_file = temp_dir / "concat_list.txt"
            if concat_file.exists():
                concat_file.unlink(missing_ok=True)
            if temp_dir.exists():
                temp_dir.rmdir()
        except Exception:
            pass

