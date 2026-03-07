"""
VL runtime FFmpeg helpers.
"""

import asyncio
import json
import tempfile
import time
from pathlib import Path
from typing import List, Optional, Tuple

import cv2

from services.python_grpc.src.common.utils.opencv_decode import (
    resolve_ffmpeg_bin,
    resolve_ffprobe_bin,
)


def _decode_output(raw: bytes) -> str:
    if isinstance(raw, (bytes, bytearray)):
        return raw.decode("utf-8", errors="ignore")
    return str(raw or "")


async def _run_subprocess(command: List[str]) -> Tuple[int, str, str]:
    process = await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    return process.returncode, _decode_output(stdout), _decode_output(stderr)


def _build_keyframe_export_command(
    *,
    ffmpeg_bin: str,
    video_path: str,
    timestamp_sec: float,
    output_path: Path,
) -> List[str]:
    command = [
        ffmpeg_bin,
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
        "-an",
    ]
    if output_path.suffix.lower() in {".jpg", ".jpeg"}:
        command.extend(["-q:v", "2"])
    command.append(str(output_path))
    return command


async def _export_keyframe_at_timestamp(
    *,
    ffmpeg_bin: str,
    video_path: str,
    timestamp_sec: float,
    output_path: Path,
) -> Tuple[bool, str]:
    command = _build_keyframe_export_command(
        ffmpeg_bin=ffmpeg_bin,
        video_path=video_path,
        timestamp_sec=timestamp_sec,
        output_path=output_path,
    )
    rc, _, stderr = await _run_subprocess(command)
    ok = rc == 0 and output_path.exists() and output_path.stat().st_size > 0
    return ok, stderr[:300]


def _calc_frame_sharpness_score(image_path: Path) -> float:
    image = cv2.imread(str(image_path), cv2.IMREAD_COLOR)
    if image is None or image.size == 0:
        return -1.0
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    return float(cv2.Laplacian(gray, cv2.CV_64F).var())


def _calc_frame_mse(image_a_path: Path, image_b_path: Path) -> Optional[float]:
    image_a = cv2.imread(str(image_a_path), cv2.IMREAD_GRAYSCALE)
    image_b = cv2.imread(str(image_b_path), cv2.IMREAD_GRAYSCALE)
    if image_a is None or image_b is None or image_a.size == 0 or image_b.size == 0:
        return None
    if image_a.shape != image_b.shape:
        image_b = cv2.resize(
            image_b,
            (int(image_a.shape[1]), int(image_a.shape[0])),
            interpolation=cv2.INTER_AREA,
        )
    diff = image_a.astype("float32") - image_b.astype("float32")
    return float((diff * diff).mean())


async def _probe_iframe_timestamps(
    *,
    video_path: str,
    target_timestamp_sec: float,
    search_window_sec: float,
    search_before_sec: Optional[float] = None,
    search_after_sec: Optional[float] = None,
) -> List[float]:
    ffprobe_bin = resolve_ffprobe_bin()
    if not ffprobe_bin:
        return []

    target_ts = max(0.0, float(target_timestamp_sec))
    window_sec = max(0.0, float(search_window_sec))
    before_sec = window_sec if search_before_sec is None else max(0.0, float(search_before_sec))
    after_sec = window_sec if search_after_sec is None else max(0.0, float(search_after_sec))
    start_sec = max(0.0, target_ts - before_sec)
    end_sec = max(start_sec, target_ts + after_sec)

    command = [
        ffprobe_bin,
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-read_intervals",
        f"{start_sec:.6f}%{end_sec:.6f}",
        "-show_entries",
        "frame=best_effort_timestamp_time,pkt_pts_time,pict_type,key_frame",
        "-of",
        "json",
        video_path,
    ]
    rc, stdout, _ = await _run_subprocess(command)
    if rc != 0:
        return []

    try:
        payload = json.loads(stdout or "{}")
    except Exception:
        return []

    timestamps: List[float] = []
    for frame in payload.get("frames", []) or []:
        if not isinstance(frame, dict):
            continue
        pict_type = str(frame.get("pict_type", "") or "").strip().upper()
        key_flag = int(frame.get("key_frame", 0) or 0)
        if pict_type != "I" and key_flag != 1:
            continue
        ts_text = frame.get("best_effort_timestamp_time", frame.get("pkt_pts_time", None))
        if ts_text is None:
            continue
        try:
            ts_value = float(ts_text)
        except (TypeError, ValueError):
            continue
        if start_sec - 1e-6 <= ts_value <= end_sec + 1e-6:
            timestamps.append(ts_value)

    if not timestamps:
        return []
    return sorted(set(round(ts, 6) for ts in timestamps))


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
    ffmpeg_bin = resolve_ffmpeg_bin() or "ffmpeg"
    command = [
        ffmpeg_bin,
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
    rc, _, stderr = await _run_subprocess(command)
    if rc != 0:
        logger.warning(
            f"[VL-Tutorial] step clip export failed: file={output_path.name}, rc={rc}, "
            f"err={stderr[:300]}"
        )
        return False
    return output_path.exists() and output_path.stat().st_size > 0


async def export_keyframe_with_ffmpeg(
    *,
    video_path: str,
    timestamp_sec: float,
    output_path: Path,
    logger,
    iframe_search_window_sec: float = 0.2,
    iframe_search_before_sec: Optional[float] = None,
    iframe_search_after_sec: Optional[float] = None,
    select_sharpest_iframe: bool = True,
) -> bool:
    """导出关键帧，支持在时间窗内选择最清晰 I 帧。"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    ffmpeg_bin = resolve_ffmpeg_bin() or "ffmpeg"
    requested_ts = max(0.0, float(timestamp_sec))
    selected_ts = requested_ts
    effective_before_sec = (
        max(0.0, float(iframe_search_before_sec))
        if iframe_search_before_sec is not None
        else max(0.0, float(iframe_search_window_sec))
    )
    effective_after_sec = (
        max(0.0, float(iframe_search_after_sec))
        if iframe_search_after_sec is not None
        else max(0.0, float(iframe_search_window_sec))
    )
    search_start_sec = max(0.0, requested_ts - effective_before_sec)
    search_end_sec = max(search_start_sec, requested_ts + effective_after_sec)
    search_interval_text = f"[{search_start_sec:.3f}s,{search_end_sec:.3f}s]"

    if bool(select_sharpest_iframe) and (effective_before_sec > 0.0 or effective_after_sec > 0.0):
        iframe_candidates = await _probe_iframe_timestamps(
            video_path=video_path,
            target_timestamp_sec=requested_ts,
            search_window_sec=float(iframe_search_window_sec),
            search_before_sec=effective_before_sec,
            search_after_sec=effective_after_sec,
        )
        if iframe_candidates:
            best_score = -1.0
            best_rank = -1.0
            best_ts = requested_ts
            accepted_candidates = 0
            drift_guard_mse = 650.0
            with tempfile.TemporaryDirectory(prefix="vl_keyframe_probe_") as tmp_dir:
                tmp_dir_path = Path(tmp_dir)
                anchor_file = tmp_dir_path / "anchor_requested.png"
                anchor_ok, _ = await _export_keyframe_at_timestamp(
                    ffmpeg_bin=ffmpeg_bin,
                    video_path=video_path,
                    timestamp_sec=requested_ts,
                    output_path=anchor_file,
                )
                for idx, candidate_ts in enumerate(iframe_candidates):
                    probe_file = tmp_dir_path / f"candidate_{idx:03d}.png"
                    ok, _ = await _export_keyframe_at_timestamp(
                        ffmpeg_bin=ffmpeg_bin,
                        video_path=video_path,
                        timestamp_sec=float(candidate_ts),
                        output_path=probe_file,
                    )
                    if not ok:
                        continue
                    mse_value: Optional[float] = None
                    if anchor_ok:
                        mse_value = _calc_frame_mse(anchor_file, probe_file)
                        if mse_value is not None and mse_value > drift_guard_mse:
                            continue
                    score = _calc_frame_sharpness_score(probe_file)
                    rank = score
                    if mse_value is not None:
                        # 轻微惩罚视觉偏差，避免“清晰但错位”的 I 帧。
                        rank = score - (0.08 * mse_value)
                    if (
                        rank > best_rank
                        or (
                            abs(rank - best_rank) <= 1e-6
                            and abs(float(candidate_ts) - requested_ts) < abs(best_ts - requested_ts)
                        )
                    ):
                        best_rank = rank
                        best_score = score
                        best_ts = float(candidate_ts)
                        accepted_candidates += 1
            if accepted_candidates > 0:
                selected_ts = best_ts
            logger.info(
                "[VL-Tutorial] keyframe I-frame selection: file=%s requested=%.3fs selected=%.3fs search_interval=%s before=%.3fs after=%.3fs candidates=%s accepted=%s score=%.2f",
                output_path.name,
                requested_ts,
                selected_ts,
                search_interval_text,
                effective_before_sec,
                effective_after_sec,
                len(iframe_candidates),
                accepted_candidates,
                best_score,
            )
        else:
            logger.info(
                "[VL-Tutorial] keyframe I-frame selection: file=%s requested=%.3fs selected=%.3fs search_interval=%s before=%.3fs after=%.3fs candidates=0 accepted=0 score=na",
                output_path.name,
                requested_ts,
                selected_ts,
                search_interval_text,
                effective_before_sec,
                effective_after_sec,
            )

    ok, err_msg = await _export_keyframe_at_timestamp(
        ffmpeg_bin=ffmpeg_bin,
        video_path=video_path,
        timestamp_sec=selected_ts,
        output_path=output_path,
    )
    if ok:
        return True

    if abs(selected_ts - requested_ts) > 1e-6:
        fallback_ok, fallback_err = await _export_keyframe_at_timestamp(
            ffmpeg_bin=ffmpeg_bin,
            video_path=video_path,
            timestamp_sec=requested_ts,
            output_path=output_path,
        )
        if fallback_ok:
            logger.warning(
                "[VL-Tutorial] keyframe fallback to requested timestamp succeeded: file=%s requested=%.3fs selected=%.3fs search_interval=%s",
                output_path.name,
                requested_ts,
                selected_ts,
                search_interval_text,
            )
            return True
        err_msg = fallback_err or err_msg

    logger.warning(
        "[VL-Tutorial] keyframe export failed: file=%s requested=%.3fs selected=%.3fs search_interval=%s err=%s",
        output_path.name,
        requested_ts,
        selected_ts,
        search_interval_text,
        err_msg,
    )
    return False


async def export_keyframes_with_ffmpeg_batch(
    *,
    video_path: str,
    keyframes: List[Tuple[float, Path]],
    logger,
) -> List[bool]:
    if not keyframes:
        return []

    batch_t0 = time.perf_counter()
    ffmpeg_bin = resolve_ffmpeg_bin() or "ffmpeg"
    normalized: List[Tuple[float, Path]] = []
    command: List[str] = [
        ffmpeg_bin,
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
    ]

    for raw_timestamp, raw_output_path in keyframes:
        try:
            timestamp_sec = max(0.0, float(raw_timestamp))
        except (TypeError, ValueError):
            timestamp_sec = 0.0
        output_path = Path(raw_output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        normalized.append((timestamp_sec, output_path))

        command.extend(
            [
                "-ss",
                f"{timestamp_sec:.6f}",
                "-i",
                video_path,
            ]
        )

    for input_index, (_timestamp_sec, output_path) in enumerate(normalized):
        command.extend(
            [
                "-map",
                f"{input_index}:v:0",
                "-frames:v",
                "1",
                "-an",
            ]
        )
        if output_path.suffix.lower() in {".jpg", ".jpeg"}:
            command.extend(["-q:v", "2"])
        command.append(str(output_path))

    rc, _, stderr = await _run_subprocess(command)
    if rc != 0:
        logger.warning(
            "[VL-Tutorial] keyframe batch export failed: count=%s rc=%s err=%s",
            len(normalized),
            rc,
            stderr[:300],
        )
        return [False for _ in normalized]

    results: List[bool] = []
    for _timestamp_sec, output_path in normalized:
        ok = output_path.exists() and output_path.stat().st_size > 0
        results.append(ok)

    if not all(results):
        failed_files = [normalized[index][1].name for index, ok in enumerate(results) if not ok]
        logger.warning(
            "[VL-Tutorial] keyframe batch export partial miss: total=%s failed=%s files=%s",
            len(normalized),
            len(failed_files),
            failed_files[:10],
        )
    logger.info(
        "[VL-Tutorial] keyframe batch export done: count=%s ok=%s ms=%.1f",
        len(normalized),
        sum(1 for ok in results if ok),
        (time.perf_counter() - batch_t0) * 1000.0,
    )

    return results


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
    ffmpeg_bin = resolve_ffmpeg_bin() or "ffmpeg"

    part_paths: List[Path] = []
    try:
        for index, (start_sec, end_sec) in enumerate(segments):
            if end_sec <= start_sec:
                continue
            duration_sec = end_sec - start_sec
            part_path = temp_dir / f"part_{index:03d}.mp4"
            part_command = [
                ffmpeg_bin,
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
            rc, _, stderr = await _run_subprocess(part_command)
            if rc != 0 or not part_path.exists() or part_path.stat().st_size <= 0:
                logger.warning(
                    f"[VL-PrePrune] ffmpeg part cut failed: part={index}, rc={rc}, err={stderr[:300]}"
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
            ffmpeg_bin,
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
        rc, _, stderr = await _run_subprocess(concat_command)
        if rc != 0 or not out_path.exists() or out_path.stat().st_size <= 0:
            logger.warning(
                f"[VL-PrePrune] ffmpeg concat failed: rc={rc}, err={stderr[:300]}"
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
