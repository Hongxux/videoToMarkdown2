#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
全流程耗时统计脚本（基于 storage 产物时间戳）。

统计口径：
1) 复用现有产物文件的最后修改时间（mtime），不侵入业务代码；
2) 以主链路阶段产物作为阶段边界：video -> subtitles -> stage1 -> phase2a -> vl(可选) -> 资产提取 -> 最终文档；
3) 若某阶段文件缺失，则该阶段耗时记为 null；
4) 若阶段时间出现明显跨天/倒序，标记为“疑似缓存复用”，便于解释统计偏差。
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


def _file_mtime(path: Path) -> Optional[dt.datetime]:
    if not path.exists() or not path.is_file():
        return None
    return dt.datetime.fromtimestamp(path.stat().st_mtime)


def _find_first_file(directory: Path, patterns: Iterable[str]) -> Optional[Path]:
    for pattern in patterns:
        matched = sorted(directory.glob(pattern))
        if matched:
            return matched[0]
    return None


def _find_last_mtime_in_dir(directory: Path) -> Tuple[Optional[dt.datetime], int]:
    if not directory.exists() or not directory.is_dir():
        return None, 0
    files = [entry for entry in directory.iterdir() if entry.is_file()]
    if not files:
        return None, 0
    latest_file = max(files, key=lambda item: item.stat().st_mtime)
    return dt.datetime.fromtimestamp(latest_file.stat().st_mtime), len(files)


def _safe_seconds(start: Optional[dt.datetime], end: Optional[dt.datetime]) -> Optional[float]:
    if not start or not end:
        return None
    return (end - start).total_seconds()


def _normalize_duration(value: Optional[float], max_sec: float = 7200.0) -> Optional[float]:
    """过滤明显异常的阶段耗时（负值或超长）。"""
    if value is None:
        return None
    if value < 0:
        return None
    if value > max_sec:
        return None
    return value


def _compute_effective_window(
    markers: Dict[str, Optional[dt.datetime]],
    max_gap_sec: float = 3600.0,
) -> Dict[str, Any]:
    """
    从标记点中提取“最近连续执行窗口”。

    目的：避免跨天缓存复用导致的超大耗时污染，保留最接近一次真实执行的时间窗。
    """
    points = sorted(
        [(name, value) for name, value in markers.items() if value],
        key=lambda item: item[1],
    )
    if not points:
        return {"start": None, "end": None, "total_sec": None, "events": []}

    chain: List[Tuple[str, dt.datetime]] = [points[-1]]
    for idx in range(len(points) - 2, -1, -1):
        prev = points[idx]
        gap = (chain[0][1] - prev[1]).total_seconds()
        if gap <= max_gap_sec:
            chain.insert(0, prev)
        else:
            break

    start = chain[0][1]
    end = chain[-1][1]
    return {
        "start": _iso(start),
        "end": _iso(end),
        "total_sec": round((end - start).total_seconds(), 3) if len(chain) >= 2 else 0.0,
        "events": [name for name, _ in chain],
    }


def _parse_manifest_elapsed(manifest_path: Path) -> Optional[Dict[str, Any]]:
    if not manifest_path.exists() or not manifest_path.is_file():
        return None
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    items = payload.get("items", []) if isinstance(payload, dict) else []
    elapsed_values: List[float] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        elapsed = item.get("elapsed_sec")
        if isinstance(elapsed, (int, float)):
            elapsed_values.append(float(elapsed))
    if not elapsed_values:
        return None
    total_elapsed = sum(elapsed_values)
    return {
        "items": len(elapsed_values),
        "elapsed_total_sec": round(total_elapsed, 3),
        "elapsed_avg_sec": round(total_elapsed / len(elapsed_values), 3),
        "elapsed_max_sec": round(max(elapsed_values), 3),
    }


def _iso(value: Optional[dt.datetime]) -> Optional[str]:
    return value.isoformat(sep=" ", timespec="seconds") if value else None


def analyze_task(task_dir: Path) -> Dict[str, Any]:
    video_file = _find_first_file(task_dir, ["video.mp4", "video.*"])
    video_done = _file_mtime(video_file) if video_file else None

    subtitles_done = _file_mtime(task_dir / "subtitles.txt")

    step2_done = _file_mtime(task_dir / "intermediates" / "step2_correction_output.json")
    step6_done = _file_mtime(task_dir / "intermediates" / "step6_merge_cross_output.json")
    stage1_done = max([moment for moment in [step2_done, step6_done] if moment], default=None)

    phase2a_done = _file_mtime(task_dir / "semantic_units_phase2a.json")
    vl_done = _file_mtime(task_dir / "vl_analysis_cache.json")

    screenshots_last, screenshot_count = _find_last_mtime_in_dir(task_dir / "screenshots")
    clips_last, clip_count = _find_last_mtime_in_dir(task_dir / "clips")
    assets_done = max([moment for moment in [screenshots_last, clips_last] if moment], default=None)

    result_done = _file_mtime(task_dir / "result.json")
    enhanced_md_done = _file_mtime(task_dir / "enhanced_output.md")
    final_done = max([moment for moment in [result_done, enhanced_md_done] if moment], default=None)

    phase2b_start = assets_done or vl_done or phase2a_done
    extraction_start = vl_done or phase2a_done

    raw_stage_durations = {
        "transcribe_sec": _safe_seconds(video_done, subtitles_done),
        "stage1_sec": _safe_seconds(subtitles_done, stage1_done),
        "phase2a_sec": _safe_seconds(stage1_done, phase2a_done),
        "vl_analysis_sec": _safe_seconds(phase2a_done, vl_done),
        "extract_assets_sec": _safe_seconds(extraction_start, assets_done),
        "phase2b_sec": _safe_seconds(phase2b_start, final_done),
        "total_sec": _safe_seconds(video_done, final_done or assets_done or vl_done or phase2a_done),
    }

    stage_durations = {
        key: _normalize_duration(value)
        for key, value in raw_stage_durations.items()
    }

    cache_suspect = False
    suspicious_reasons: List[str] = []
    for key, seconds in raw_stage_durations.items():
        if seconds is None:
            continue
        if seconds < 0:
            cache_suspect = True
            suspicious_reasons.append(f"{key} 为负值({seconds:.1f}s)")
        if seconds > 7200:
            cache_suspect = True
            suspicious_reasons.append(f"{key} 超过2小时({seconds:.1f}s)")

    legacy_clip_manifest = _parse_manifest_elapsed(task_dir / "semantic_unit_clips" / "manifest.json")
    vl_clip_manifest = _parse_manifest_elapsed(task_dir / "semantic_unit_clips_vl" / "manifest.json")

    effective_window = _compute_effective_window(
        {
            "video_done": video_done,
            "subtitles_done": subtitles_done,
            "stage1_done": stage1_done,
            "phase2a_done": phase2a_done,
            "vl_done": vl_done,
            "assets_done": assets_done,
            "final_done": final_done,
        }
    )

    return {
        "task_hash": task_dir.name,
        "paths": {
            "video_file": str(video_file) if video_file else None,
        },
        "markers": {
            "video_done": _iso(video_done),
            "subtitles_done": _iso(subtitles_done),
            "stage1_done": _iso(stage1_done),
            "phase2a_done": _iso(phase2a_done),
            "vl_done": _iso(vl_done),
            "assets_done": _iso(assets_done),
            "final_done": _iso(final_done),
        },
        "counts": {
            "screenshots": screenshot_count,
            "clips": clip_count,
        },
        "durations": {k: (round(v, 3) if isinstance(v, (int, float)) else None) for k, v in stage_durations.items()},
        "raw_durations": {k: (round(v, 3) if isinstance(v, (int, float)) else None) for k, v in raw_stage_durations.items()},
        "completed": final_done is not None,
        "cache_reuse_suspect": cache_suspect,
        "cache_reuse_reasons": suspicious_reasons,
        "effective_window": effective_window,
        "manifests": {
            "legacy_clip_manifest": legacy_clip_manifest,
            "vl_clip_manifest": vl_clip_manifest,
        },
    }


def _fmt_duration(value: Optional[float]) -> str:
    if value is None:
        return "-"
    if value < 60:
        return f"{value:.1f}s"
    return f"{value / 60:.2f}m"


def build_markdown_report(records: List[Dict[str, Any]]) -> str:
    lines: List[str] = []
    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines.append("# 全流程耗时统计报告")
    lines.append("")
    lines.append(f"生成时间：{now}")
    lines.append("统计口径：基于 `storage/{task_hash}` 产物文件修改时间推算；若跨天复用缓存会标注为疑似缓存复用。")
    lines.append("")
    lines.append("| task_hash | 完整流程 | transcribe | stage1 | phase2a | vl_analysis | assets | phase2b | total | 有效窗口 |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")

    for record in records:
        durations = record["durations"]
        lines.append(
            "| {task} | {completed} | {transcribe} | {stage1} | {phase2a} | {vl} | {assets} | {phase2b} | {total} | {window} |".format(
                task=record["task_hash"],
                completed="是" if record.get("completed") else "否",
                transcribe=_fmt_duration(durations.get("transcribe_sec")),
                stage1=_fmt_duration(durations.get("stage1_sec")),
                phase2a=_fmt_duration(durations.get("phase2a_sec")),
                vl=_fmt_duration(durations.get("vl_analysis_sec")),
                assets=_fmt_duration(durations.get("extract_assets_sec")),
                phase2b=_fmt_duration(durations.get("phase2b_sec")),
                total=_fmt_duration(durations.get("total_sec")),
                window=_fmt_duration((record.get("effective_window") or {}).get("total_sec")),
            )
        )

    lines.append("")
    lines.append("## 关键观察")
    for record in records:
        durations = record["durations"]
        task_hash = record["task_hash"]
        marker_line = f"- `{task_hash}`: 截图{record['counts']['screenshots']}张, 切片{record['counts']['clips']}段"
        if record.get("cache_reuse_suspect"):
            marker_line += "，疑似缓存复用"
        lines.append(marker_line)

        window = record.get("effective_window") or {}
        if window.get("total_sec") is not None:
            lines.append(
                f"  - 有效连续窗口: {window['start']} -> {window['end']}，持续 {window['total_sec']:.1f}s"
            )

        legacy_manifest = record.get("manifests", {}).get("legacy_clip_manifest")
        vl_manifest = record.get("manifests", {}).get("vl_clip_manifest")
        if legacy_manifest:
            lines.append(
                f"  - legacy manifest: {legacy_manifest['items']} 段, 总FFmpeg耗时 {legacy_manifest['elapsed_total_sec']:.1f}s, 平均 {legacy_manifest['elapsed_avg_sec']:.2f}s"
            )
        if vl_manifest:
            lines.append(
                f"  - vl manifest: {vl_manifest['items']} 段, 总FFmpeg耗时 {vl_manifest['elapsed_total_sec']:.1f}s, 平均 {vl_manifest['elapsed_avg_sec']:.2f}s"
            )

        if durations.get("phase2a_sec") and durations["phase2a_sec"] > 600:
            lines.append("  - 主要瓶颈：Phase2A/语义分析阶段耗时较长")
        if durations.get("extract_assets_sec") and durations["extract_assets_sec"] > 180:
            lines.append("  - 主要瓶颈：素材提取阶段耗时较长")

    lines.append("")
    lines.append("## 说明")
    lines.append("- 本报告不包含排队等待时间，仅统计产物驱动的处理时间。")
    lines.append("- 若任务复用历史产物（缓存命中），阶段耗时会被放大或跨天。")
    lines.append("- 若要得到严格实时统计，建议在 Java Orchestrator 增加阶段开始/结束埋点并落盘。")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="统计 videoToMarkdown 全流程耗时（基于 storage 产物）")
    parser.add_argument("--storage-dir", default="storage", help="storage 根目录")
    parser.add_argument("--task", default="", help="只统计指定 task_hash")
    parser.add_argument("--json-out", default="storage/pipeline_timing_report.json", help="JSON 输出路径")
    parser.add_argument("--md-out", default="storage/pipeline_timing_report.md", help="Markdown 输出路径")
    args = parser.parse_args()

    storage_dir = Path(args.storage_dir).resolve()
    if not storage_dir.exists() or not storage_dir.is_dir():
        raise SystemExit(f"storage 目录不存在: {storage_dir}")

    task_dirs = [entry for entry in storage_dir.iterdir() if entry.is_dir()]
    if args.task:
        task_dirs = [entry for entry in task_dirs if entry.name == args.task]
    task_dirs = sorted(task_dirs, key=lambda item: item.stat().st_mtime, reverse=True)

    records = [analyze_task(task_dir) for task_dir in task_dirs]

    json_out = Path(args.json_out)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")

    markdown_out = Path(args.md_out)
    markdown_out.parent.mkdir(parents=True, exist_ok=True)
    markdown_out.write_text(build_markdown_report(records), encoding="utf-8")

    print(f"已输出 JSON: {json_out}")
    print(f"已输出 Markdown: {markdown_out}")
    print(f"统计任务数: {len(records)}")


if __name__ == "__main__":
    main()
