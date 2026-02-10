"""
按语义单元切割视频（基于 semantic_units_phase2a.json）。

做什么：
- 读取语义单元 JSON（unit_id/start_sec/end_sec/knowledge_topic...）
- 用 ffprobe 获取视频总时长并对区间做裁剪/校验
- 对每个语义单元调用 ffmpeg 重新编码切割，产出多个 mp4
- 生成 manifest.json 记录每段的命令、耗时、状态与错误

为什么：
- 语义单元是上游“语义闭环 + 知识主题唯一”的切分结果，直接用其时间轴切割能保持内容结构一致；
- 重新编码相比 “-c copy” 更接近精确起止时间（代价是更慢、文件更大）。

权衡：
- 若更追求速度，可改为直拷流（-c copy），但边界可能对齐关键帧导致轻微偏差；
- 语义单元时间轴质量取决于上游生成，脚本只做裁剪与告警，不做重新推断。
"""

from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
import sys
import time
from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from services.python_grpc.src.common.utils.numbers import to_float
from services.python_grpc.src.common.utils.path import (
    sanitize_filename_component as _sanitize_filename_component,
)
from services.python_grpc.src.common.utils.video import probe_video_duration_ffprobe


@dataclass
class SegmentItem:
    """
    表示一个语义单元对应的切割任务与结果。

    - start_sec/end_sec：经过校验/裁剪后的最终区间
    - status：
      - planned: dry-run 模式下的计划项
      - success: 切割成功
      - skipped_too_short: 时长不足 min-duration
      - skipped_existing: 输出已存在且未开启 overwrite
      - not_run_due_to_fail_fast: fail-fast 下，前序失败后未执行
      - failed: ffmpeg/校验失败
    """

    index: int
    unit_id: str
    knowledge_topic: str
    start_sec: float
    end_sec: float
    duration_sec: float
    out_path: str
    status: str
    warnings: List[str] = field(default_factory=list)
    ffmpeg_cmd: List[str] = field(default_factory=list)
    ffmpeg_returncode: Optional[int] = None
    ffmpeg_stderr: str = ""
    elapsed_sec: Optional[float] = None


def resolve_default_semantic_units(video_path: str) -> str:
    """
    做什么：为给定 video_path 推导默认的语义单元 JSON 路径（同目录 semantic_units_phase2a.json）。
    为什么：与现有 pipeline 的产物布局保持一致，减少手动指定成本。
    权衡：若目录结构不同，可通过 --semantic-units 显式传入。
    """

    video_dir = str(Path(video_path).resolve().parent)
    return str(Path(video_dir) / "semantic_units_phase2a.json")


def _sanitize_filename_component(text: str, max_len: int = 40) -> str:
    """
    做什么：将任意文本清洗为 Windows 安全的文件名片段。
    为什么：避免非法字符导致写盘失败，同时控制长度降低路径过长风险。
    权衡：清洗会损失原始字符串的部分可读性，但能换取跨环境稳定性。
    """

    return __sanitize_filename_component(text, max_len=max_len)


def ffprobe_duration(video_path: str) -> float:
    """
    做什么：用 ffprobe 获取视频总时长（秒）。
    为什么：需要将语义单元 end_sec 裁剪到视频实际范围内，避免 ffmpeg 报错。
    权衡：依赖 ffprobe 可用；若不可用，脚本会直接失败并提示安装/配置。
    """

    return probe_video_duration_ffprobe(video_path)


def _format_time_range(start_sec: float, end_sec: float) -> str:
    return f"{start_sec:.2f}-{end_sec:.2f}"


def _build_output_name(
    index: int,
    index_width: int,
    unit_id: str,
    knowledge_topic: str,
    start_sec: float,
    end_sec: float,
) -> str:
    safe_unit_id = _sanitize_filename_component(unit_id, max_len=32) or "UNKNOWN"
    safe_topic = _sanitize_filename_component(knowledge_topic, max_len=40)
    tr = _format_time_range(start_sec, end_sec)
    parts = [f"{index:0{index_width}d}", safe_unit_id]
    if safe_topic:
        parts.append(safe_topic)
    parts.append(tr)
    return "_".join(parts) + ".mp4"


def _ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _load_semantic_units(path: str) -> List[Dict[str, Any]]:
    raw = Path(path).read_text(encoding="utf-8")
    data = json.loads(raw)
    if not isinstance(data, list):
        raise ValueError("semantic units json must be a list")
    return data


def _normalize_segments(
    units: List[Dict[str, Any]],
    video_duration: float,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    做什么：对语义单元的时间区间做排序、数值清洗、裁剪，并产生全局告警列表。
    为什么：上游数据可能存在边界超界/逆序/重叠，先规范化可提升切割稳定性。
    权衡：不在此阶段强行修复重叠，只记录 warning，避免隐式改变语义边界。
    """

    warnings: List[str] = []

    normalized: List[Dict[str, Any]] = []
    for idx, u in enumerate(units):
        if not isinstance(u, dict):
            warnings.append(f"unit[{idx}] is not an object, skipped")
            continue
        unit_id = str(u.get("unit_id") or f"SU_{idx+1:03d}")
        start_raw = to_float(u.get("start_sec"))
        end_raw = to_float(u.get("end_sec"))
        if start_raw is None or end_raw is None:
            warnings.append(f"{unit_id}: missing start_sec/end_sec, skipped")
            continue

        start = max(0.0, float(start_raw))
        end = max(start, float(end_raw))
        if end > video_duration + 1e-6:
            warnings.append(
                f"{unit_id}: end_sec={end:.3f}s exceeds video_duration={video_duration:.3f}s, clamped"
            )
            end = min(end, video_duration)

        normalized.append(
            {
                "unit_id": unit_id,
                "knowledge_topic": str(u.get("knowledge_topic") or ""),
                "start_sec": start,
                "end_sec": end,
            }
        )

    normalized.sort(key=lambda x: (x["start_sec"], x["end_sec"], x["unit_id"]))

    # 重叠检测：只告警，不做修改
    prev_end: Optional[float] = None
    prev_id: Optional[str] = None
    for u in normalized:
        if prev_end is not None and u["start_sec"] < prev_end - 1e-6:
            warnings.append(
                f"{u['unit_id']}: overlaps with previous ({prev_id}) "
                f"start={u['start_sec']:.3f}s < prev_end={prev_end:.3f}s"
            )
        prev_end = u["end_sec"]
        prev_id = u["unit_id"]

    return normalized, warnings


def _run_ffmpeg_cut(
    ffmpeg_path: str,
    video_path: str,
    start_sec: float,
    duration_sec: float,
    out_path: str,
    overwrite: bool,
    timeout_sec: float,
) -> Tuple[int, str, List[str], float]:
    """
    做什么：执行一次 ffmpeg 切割（重新编码）。
    为什么：重新编码可获得更准确的时间边界。
    权衡：相对慢；timeout 需随片段时长动态调整。
    """

    cmd: List[str] = [
        ffmpeg_path,
        "-hide_banner",
        "-loglevel",
        "error",
        "-y" if overwrite else "-n",
        "-ss",
        f"{start_sec:.3f}",
        "-i",
        video_path,
        "-t",
        f"{duration_sec:.3f}",
        "-map",
        "0:v:0",
        "-map",
        "0:a?",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "20",
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        "-movflags",
        "+faststart",
        out_path,
    ]

    start_ts = time.time()
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_sec)
        elapsed = time.time() - start_ts
        return result.returncode, (result.stderr or ""), cmd, elapsed
    except subprocess.TimeoutExpired as e:
        elapsed = time.time() - start_ts
        stderr = ""
        if hasattr(e, "stderr") and e.stderr:
            stderr = str(e.stderr)
        return 124, f"ffmpeg timeout after {timeout_sec:.1f}s. {stderr}".strip(), cmd, elapsed


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Split video into semantic-unit clips via ffmpeg.")
    parser.add_argument("--video", required=True, help="Input video path (mp4).")
    parser.add_argument(
        "--semantic-units",
        default=None,
        help="Semantic units json path. Default: semantic_units_phase2a.json in video directory.",
    )
    parser.add_argument(
        "--out-dir",
        default=None,
        help="Output directory. Default: <video_dir>\\semantic_unit_clips",
    )
    parser.add_argument("--ffmpeg", default="ffmpeg", help="ffmpeg executable path (default: ffmpeg).")
    parser.add_argument("--min-duration", type=float, default=0.8, help="Min duration seconds to keep (default: 0.8).")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing output files.")
    parser.add_argument("--fail-fast", action="store_true", help="Fail immediately on first ffmpeg error.")
    parser.add_argument("--dry-run", action="store_true", help="Print planned segments and ffmpeg commands, no execution.")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    video_path = str(Path(args.video).resolve())
    if not Path(video_path).exists():
        print(f"ERROR: video not found: {video_path}", file=sys.stderr)
        return 2

    semantic_units_path = args.semantic_units or resolve_default_semantic_units(video_path)
    semantic_units_path = str(Path(semantic_units_path).resolve())
    if not Path(semantic_units_path).exists():
        print(
            "ERROR: semantic units json not found.\n"
            f"  expected: {semantic_units_path}\n"
            "  hint: pass --semantic-units <path> or generate semantic_units_phase2a.json first.",
            file=sys.stderr,
        )
        return 2

    out_dir = args.out_dir or str(Path(video_path).parent / "semantic_unit_clips")
    out_dir = str(Path(out_dir).resolve())

    video_duration = ffprobe_duration(video_path)
    units_raw = _load_semantic_units(semantic_units_path)
    units_norm, global_warnings = _normalize_segments(units_raw, video_duration=video_duration)

    if not units_norm:
        print("ERROR: no valid semantic units found after normalization.", file=sys.stderr)
        for w in global_warnings:
            print(f"WARNING: {w}", file=sys.stderr)
        return 2

    index_width = max(3, int(math.log10(len(units_norm))) + 1 if len(units_norm) > 0 else 3)

    if global_warnings:
        for w in global_warnings:
            print(f"WARNING: {w}", file=sys.stderr)

    planned_items: List[SegmentItem] = []
    for i, u in enumerate(units_norm, start=1):
        start_sec = float(u["start_sec"])
        end_sec = float(u["end_sec"])
        duration_sec = max(0.0, end_sec - start_sec)
        unit_id = str(u["unit_id"])
        topic = str(u.get("knowledge_topic") or "")

        out_name = _build_output_name(i, index_width, unit_id, topic, start_sec, end_sec)
        out_path = str(Path(out_dir) / out_name)

        warnings: List[str] = []
        if duration_sec < float(args.min_duration):
            warnings.append(f"duration {duration_sec:.3f}s < min-duration {float(args.min_duration):.3f}s")
            status = "skipped_too_short"
        elif Path(out_path).exists() and not args.overwrite:
            status = "skipped_existing"
        else:
            status = "planned" if args.dry_run else "pending"

        # 先构造命令用于 dry-run 打印/manifest
        timeout_sec = max(120.0, duration_sec * 6.0)
        cmd_preview = [
            str(args.ffmpeg),
            "-hide_banner",
            "-loglevel",
            "error",
            "-y" if args.overwrite else "-n",
            "-ss",
            f"{start_sec:.3f}",
            "-i",
            video_path,
            "-t",
            f"{duration_sec:.3f}",
            "-map",
            "0:v:0",
            "-map",
            "0:a?",
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "20",
            "-c:a",
            "aac",
            "-b:a",
            "128k",
            "-movflags",
            "+faststart",
            out_path,
        ]
        if status in ("planned", "pending"):
            warnings.append(f"timeout_sec={timeout_sec:.1f}")

        planned_items.append(
            SegmentItem(
                index=i,
                unit_id=unit_id,
                knowledge_topic=topic,
                start_sec=start_sec,
                end_sec=end_sec,
                duration_sec=duration_sec,
                out_path=out_path,
                status=status,
                warnings=warnings,
                ffmpeg_cmd=cmd_preview,
            )
        )

    if args.dry_run:
        print(f"Video: {video_path}")
        print(f"Semantic units: {semantic_units_path}")
        print(f"Video duration: {video_duration:.3f}s")
        print(f"Out dir: {out_dir}")
        print("")
        for item in planned_items:
            print(
                f"[{item.index:0{index_width}d}] {item.unit_id} "
                f"{item.start_sec:.2f}-{item.end_sec:.2f} ({item.duration_sec:.2f}s) "
                f"-> {item.status}"
            )
            print("  " + " ".join(item.ffmpeg_cmd))
            if item.warnings:
                for w in item.warnings:
                    print(f"  WARNING: {w}")
        planned_count = sum(1 for x in planned_items if x.status == "planned")
        print("")
        print(f"Planned: {planned_count}, Skipped: {len(planned_items) - planned_count}")
        return 0

    Path(out_dir).mkdir(parents=True, exist_ok=True)
    manifest_path = str(Path(out_dir) / "manifest.json")

    items_out: List[SegmentItem] = []
    success = 0
    skipped = 0
    failed = 0
    aborted_due_to_fail_fast = False

    for item in planned_items:
        if item.status.startswith("skipped_"):
            skipped += 1
            items_out.append(item)
            continue

        # pending -> run
        _ensure_parent_dir(Path(item.out_path))
        timeout_sec = max(120.0, item.duration_sec * 6.0)
        rc, stderr, cmd, elapsed = _run_ffmpeg_cut(
            ffmpeg_path=str(args.ffmpeg),
            video_path=video_path,
            start_sec=item.start_sec,
            duration_sec=item.duration_sec,
            out_path=item.out_path,
            overwrite=bool(args.overwrite),
            timeout_sec=timeout_sec,
        )
        item.ffmpeg_cmd = cmd
        item.ffmpeg_returncode = rc
        item.ffmpeg_stderr = (stderr or "").strip()[:2000]
        item.elapsed_sec = elapsed

        if rc == 0 and Path(item.out_path).exists() and Path(item.out_path).stat().st_size > 0:
            item.status = "success"
            success += 1
        else:
            item.status = "failed"
            failed += 1
            if args.fail_fast:
                items_out.append(item)
                aborted_due_to_fail_fast = True
                break

        items_out.append(item)

    if aborted_due_to_fail_fast:
        done_count = len(items_out)
        for rest in planned_items[done_count:]:
            if rest.status.startswith("skipped_"):
                # 理论上不会进入这里（skipped 会先被加入 items_out），但保留一致性
                continue
            rest.status = "not_run_due_to_fail_fast"
            items_out.append(rest)

    manifest: Dict[str, Any] = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "video_path": video_path,
        "semantic_units_path": semantic_units_path,
        "out_dir": out_dir,
        "video_duration_sec": video_duration,
        "min_duration_sec": float(args.min_duration),
        "overwrite": bool(args.overwrite),
        "ffmpeg": str(args.ffmpeg),
        "global_warnings": global_warnings,
        "summary": {
            "total_units": len(planned_items),
            "success": success,
            "skipped": skipped,
            "failed": failed,
            "aborted_due_to_fail_fast": aborted_due_to_fail_fast,
        },
        "items": [asdict(x) for x in items_out],
    }
    Path(manifest_path).write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Out dir: {out_dir}")
    print(f"Manifest: {manifest_path}")
    print(f"Total: {len(planned_items)} | Success: {success} | Skipped: {skipped} | Failed: {failed}")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
