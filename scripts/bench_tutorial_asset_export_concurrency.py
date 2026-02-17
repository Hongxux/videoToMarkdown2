from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import shutil
import statistics
import threading
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import matplotlib
import psutil

from services.python_grpc.src.content_pipeline.infra.runtime.config_loader import load_module2_config
from services.python_grpc.src.content_pipeline.phase2a.materials.vl_material_generator import VLMaterialGenerator

matplotlib.use("Agg")
from matplotlib import pyplot as plt  # noqa: E402


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _parse_int_list(raw: str) -> List[int]:
    values: List[int] = []
    for item in (raw or "").split(","):
        token = item.strip()
        if not token:
            continue
        value = int(token)
        if value <= 0:
            raise ValueError(f"并发参数必须大于 0: {token}")
        values.append(value)
    dedup = sorted(set(values))
    if not dedup:
        raise ValueError("至少需要一个整数参数")
    return dedup


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _load_units(units_path: Path) -> List[Dict[str, Any]]:
    payload = json.loads(units_path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        if isinstance(payload.get("semantic_units"), list):
            payload = payload["semantic_units"]
        elif isinstance(payload.get("units"), list):
            payload = payload["units"]
    if not isinstance(payload, list):
        raise ValueError("semantic units JSON 必须是 list，或包含 semantic_units/units list")
    return [item for item in payload if isinstance(item, dict)]


def _pick_process_unit(units: List[Dict[str, Any]]) -> Dict[str, Any]:
    process_units: List[Dict[str, Any]] = []
    for unit in units:
        kt = str(unit.get("knowledge_type", "") or "").strip().lower()
        if kt != "process":
            continue
        start_sec = _safe_float(unit.get("start_sec", 0.0), 0.0)
        end_sec = _safe_float(unit.get("end_sec", start_sec), start_sec)
        if end_sec <= start_sec:
            continue
        process_units.append(unit)
    if not process_units:
        raise ValueError("未找到 process 单元，无法构造教程导出压测样本")

    process_units.sort(
        key=lambda item: (_safe_float(item.get("end_sec", 0.0)) - _safe_float(item.get("start_sec", 0.0))),
        reverse=True,
    )
    return process_units[0]


def _build_step_requests(
    *,
    unit: Dict[str, Any],
    step_count: int,
    step_duration_sec: float,
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    unit_id = str(unit.get("unit_id", "") or "").strip() or "UNIT"
    unit_start = _safe_float(unit.get("start_sec", 0.0), 0.0)
    unit_end = _safe_float(unit.get("end_sec", unit_start + step_duration_sec), unit_start + step_duration_sec)
    if unit_end <= unit_start:
        unit_end = unit_start + step_duration_sec

    duration = unit_end - unit_start
    max_steps = max(1, int(duration // max(1.0, step_duration_sec)))
    final_steps = max(1, min(step_count, max_steps))
    actual_step_len = duration / final_steps

    clip_requests: List[Dict[str, Any]] = []
    screenshot_requests: List[Dict[str, Any]] = []
    for idx in range(final_steps):
        step_id = idx + 1
        start_sec = unit_start + idx * actual_step_len
        end_sec = unit_start + (idx + 1) * actual_step_len
        if end_sec <= start_sec:
            end_sec = start_sec + 1.0
        key_ts = start_sec + (end_sec - start_sec) * 0.75

        clip_requests.append(
            {
                "semantic_unit_id": unit_id,
                "analysis_mode": "tutorial_stepwise",
                "step_id": step_id,
                "step_description": f"tutorial step {step_id}",
                "action_brief": f"step_{step_id}",
                "start_sec": float(start_sec),
                "end_sec": float(end_sec),
            }
        )
        screenshot_requests.append(
            {
                "semantic_unit_id": unit_id,
                "analysis_mode": "tutorial_stepwise",
                "step_id": step_id,
                "timestamp_sec": float(key_ts),
            }
        )
    return clip_requests, screenshot_requests


def _build_generator_config(workers: int, hard_cap: int) -> Dict[str, Any]:
    full_config = load_module2_config()
    vl_cfg = dict(full_config.get("vl_material_generation", {}) or {})
    tutorial_cfg = dict(vl_cfg.get("tutorial_mode", {}) or {})
    tutorial_cfg["enabled"] = True
    tutorial_cfg["export_assets"] = True
    tutorial_cfg["save_step_json"] = True
    tutorial_cfg["asset_export_parallel_workers"] = workers
    tutorial_cfg["asset_export_parallel_hard_cap"] = hard_cap
    vl_cfg["tutorial_mode"] = tutorial_cfg
    vl_cfg.setdefault("screenshot_optimization", {})
    vl_cfg.setdefault("fallback", {})
    return vl_cfg


def _percentile(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    k = (len(sorted_values) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_values) - 1)
    if f == c:
        return float(sorted_values[f])
    d0 = sorted_values[f] * (c - k)
    d1 = sorted_values[c] * (k - f)
    return float(d0 + d1)


def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(v) for v in value]
    return str(value)


class _SystemSampler:
    def __init__(self, interval_sec: float) -> None:
        self.interval_sec = max(0.2, float(interval_sec))
        self.samples: List[Dict[str, Any]] = []
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self.samples = []
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> List[Dict[str, Any]]:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self.interval_sec * 3)
        return self.samples

    def _run(self) -> None:
        psutil.cpu_percent(interval=None)
        while not self._stop.is_set():
            cpu = float(psutil.cpu_percent(interval=self.interval_sec))
            vm = psutil.virtual_memory()
            self.samples.append(
                {
                    "ts": _now_iso(),
                    "cpu_percent": cpu,
                    "memory_percent": float(vm.percent),
                }
            )


def _summarize_system_samples(samples: List[Dict[str, Any]]) -> Dict[str, float]:
    if not samples:
        return {"cpu_mean": 0.0, "cpu_p95": 0.0, "mem_percent_mean": 0.0}
    cpu_values = [float(item["cpu_percent"]) for item in samples]
    mem_values = [float(item["memory_percent"]) for item in samples]
    return {
        "cpu_mean": float(statistics.fmean(cpu_values)),
        "cpu_p95": _percentile(cpu_values, 95),
        "mem_percent_mean": float(statistics.fmean(mem_values)),
    }


async def _run_once(
    *,
    video_path: str,
    output_dir: Path,
    unit_id: str,
    clip_requests: List[Dict[str, Any]],
    screenshot_requests: List[Dict[str, Any]],
    workers: int,
    hard_cap: int,
    sample_interval_sec: float,
) -> tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]]]:
    config = _build_generator_config(workers, hard_cap)
    generator = VLMaterialGenerator(config)
    sampler = _SystemSampler(sample_interval_sec)

    if output_dir.exists():
        shutil.rmtree(output_dir, ignore_errors=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    started_at = _now_iso()
    sampler.start()
    t0 = time.perf_counter()
    success = True
    error_message = ""
    try:
        await generator._save_tutorial_assets_for_unit(
            video_path=video_path,
            output_dir=str(output_dir),
            unit_id=unit_id,
            clip_requests=clip_requests,
            screenshot_requests=screenshot_requests,
            raw_response_json=[],
        )
    except Exception as exc:
        success = False
        error_message = str(exc)

    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    samples = sampler.stop()
    ended_at = _now_iso()

    asset_dir = output_dir / "vl_tutorial_units" / unit_id
    clip_files = list(asset_dir.glob("*.mp4")) if asset_dir.exists() else []
    keyframe_files = [*asset_dir.glob("*.png"), *asset_dir.glob("*.jpg"), *asset_dir.glob("*.jpeg")] if asset_dir.exists() else []
    json_files = list(asset_dir.glob("*_steps.json")) if asset_dir.exists() else []

    assets_total = len(clip_files) + len(keyframe_files)
    throughput_assets_per_sec = 0.0
    if success and elapsed_ms > 0:
        throughput_assets_per_sec = assets_total / (elapsed_ms / 1000.0)

    run_record: Dict[str, Any] = {
        "started_at": started_at,
        "ended_at": ended_at,
        "workers": workers,
        "hard_cap": hard_cap,
        "case_id": f"w={workers}",
        "steps_requested": len(clip_requests),
        "clip_files_count": len(clip_files),
        "keyframe_files_count": len(keyframe_files),
        "json_files_count": len(json_files),
        "assets_total": assets_total,
        "elapsed_ms": elapsed_ms,
        "throughput_assets_per_sec": throughput_assets_per_sec,
        "success": success,
        "error": error_message,
        "sample_count": len(samples),
    }
    run_record.update(_summarize_system_samples(samples))
    return run_record, [
        {"type": "clip", "path": str(path)} for path in clip_files
    ] + [
        {"type": "keyframe", "path": str(path)} for path in keyframe_files
    ] + [
        {"type": "json", "path": str(path)} for path in json_files
    ], samples


def _summarize_by_worker(run_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for row in run_rows:
        grouped[int(row["workers"])].append(row)

    summary_rows: List[Dict[str, Any]] = []
    for worker in sorted(grouped):
        rows = grouped[worker]
        success_rows = [item for item in rows if bool(item["success"])]
        elapsed_values = [float(item["elapsed_ms"]) for item in success_rows]
        throughput_values = [float(item["throughput_assets_per_sec"]) for item in success_rows]
        assets_values = [float(item["assets_total"]) for item in success_rows]
        clip_values = [float(item["clip_files_count"]) for item in success_rows]
        keyframe_values = [float(item["keyframe_files_count"]) for item in success_rows]
        summary_rows.append(
            {
                "workers": worker,
                "runs": len(rows),
                "success_runs": len(success_rows),
                "success_rate_percent": (len(success_rows) / len(rows) * 100.0) if rows else 0.0,
                "elapsed_ms_mean": float(statistics.fmean(elapsed_values)) if elapsed_values else 0.0,
                "elapsed_ms_p95": _percentile(elapsed_values, 95) if elapsed_values else 0.0,
                "throughput_assets_per_sec_mean": float(statistics.fmean(throughput_values))
                if throughput_values
                else 0.0,
                "assets_total_mean": float(statistics.fmean(assets_values)) if assets_values else 0.0,
                "clip_files_mean": float(statistics.fmean(clip_values)) if clip_values else 0.0,
                "keyframe_files_mean": float(statistics.fmean(keyframe_values)) if keyframe_values else 0.0,
            }
        )
    return summary_rows


def _select_recommendation(summary_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not summary_rows:
        return {"best_worker": 1, "rule": "无样本"}

    candidates = [row for row in summary_rows if float(row["success_rate_percent"]) >= 99.9]
    if not candidates:
        candidates = list(summary_rows)

    ranked = sorted(
        candidates,
        key=lambda item: (
            -float(item["throughput_assets_per_sec_mean"]),
            float(item["elapsed_ms_mean"]),
        ),
    )
    best = ranked[0]
    return {
        "best_worker": int(best["workers"]),
        "rule": "success_rate 优先，随后导出吞吐最大且时延更低",
        "top3_workers": [int(item["workers"]) for item in ranked[:3]],
    }


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _plot_summary(summary_rows: List[Dict[str, Any]], output_png: Path) -> None:
    if not summary_rows:
        return
    rows = sorted(summary_rows, key=lambda item: int(item["workers"]))
    workers = [int(item["workers"]) for item in rows]
    elapsed = [float(item["elapsed_ms_mean"]) for item in rows]
    throughput = [float(item["throughput_assets_per_sec_mean"]) for item in rows]
    assets_total = [float(item["assets_total_mean"]) for item in rows]
    success = [float(item["success_rate_percent"]) for item in rows]

    fig, axes = plt.subplots(2, 2, figsize=(14, 9))

    ax1 = axes[0][0]
    ax1.plot(workers, elapsed, marker="o")
    ax1.set_title("Latency Mean vs Workers")
    ax1.set_xlabel("workers")
    ax1.set_ylabel("elapsed ms")
    ax1.grid(alpha=0.3)

    ax2 = axes[0][1]
    ax2.plot(workers, throughput, marker="o", color="#2ca02c")
    ax2.set_title("Asset Throughput vs Workers")
    ax2.set_xlabel("workers")
    ax2.set_ylabel("assets / sec")
    ax2.grid(alpha=0.3)

    ax3 = axes[1][0]
    ax3.bar(workers, assets_total, color="#ff7f0e")
    ax3.set_title("Exported Assets Mean")
    ax3.set_xlabel("workers")
    ax3.set_ylabel("count")
    ax3.grid(axis="y", alpha=0.3)

    ax4 = axes[1][1]
    ax4.bar(workers, success, color="#9467bd")
    ax4.set_ylim(0, 105)
    ax4.set_title("Success Rate")
    ax4.set_xlabel("workers")
    ax4.set_ylabel("percent")
    ax4.grid(axis="y", alpha=0.3)

    fig.tight_layout()
    fig.savefig(output_png, dpi=160)
    plt.close(fig)


def _write_report(
    *,
    report_path: Path,
    metadata: Dict[str, Any],
    summary_rows: List[Dict[str, Any]],
    recommendation: Dict[str, Any],
    output_dir: Path,
) -> None:
    lines: List[str] = []
    lines.append("# 并发测试报告（教程资产导出）")
    lines.append("")
    lines.append(f"- 生成时间: {metadata['generated_at']}")
    lines.append(f"- 输入视频: `{metadata['video_path']}`")
    lines.append(f"- 输入语义单元: `{metadata['units_path']}`")
    lines.append(f"- 目标单元: `{metadata['target_unit_id']}`")
    lines.append(f"- 步骤数: `{metadata['steps_requested']}`")
    lines.append(f"- worker 阶梯: `{metadata['workers']}`")
    lines.append(f"- 每档重复次数: `{metadata['repeats']}`")
    lines.append(f"- 原始数据目录: `{output_dir / 'raw'}`")
    lines.append(f"- 图表文件: `{output_dir / 'charts' / 'concurrency_summary.png'}`")
    lines.append("")
    lines.append("## 方法")
    lines.append("- 固定同一组 step clip + keyframe 导出任务。")
    lines.append("- 对 `asset_export_parallel_workers` 做阶梯压测。")
    lines.append("- 统计导出吞吐、时延与成功率。")
    lines.append("")
    lines.append("## 汇总")
    lines.append("| workers | runs | success(%) | elapsed_mean(ms) | throughput(assets/s) | assets_mean | clips_mean | keyframes_mean |")
    lines.append("|---:|---:|---:|---:|---:|---:|---:|---:|")
    for row in sorted(summary_rows, key=lambda item: int(item["workers"])):
        lines.append(
            f"| {int(row['workers'])} | {int(row['runs'])} | {row['success_rate_percent']:.2f} | "
            f"{row['elapsed_ms_mean']:.2f} | {row['throughput_assets_per_sec_mean']:.3f} | "
            f"{row['assets_total_mean']:.2f} | {row['clip_files_mean']:.2f} | {row['keyframe_files_mean']:.2f} |"
        )
    lines.append("")
    lines.append("## 推荐值")
    lines.append(f"- best_worker: `{recommendation.get('best_worker', '')}`")
    lines.append(f"- 判定依据: {recommendation.get('rule', '')}")
    lines.append("")
    lines.append("## 产物")
    lines.append("- `raw/runs_raw.json`")
    lines.append("- `raw/runs_raw.csv`")
    lines.append("- `raw/summary_by_worker.json`")
    lines.append("- `raw/summary_by_worker.csv`")
    lines.append("- `raw/exported_files_*.json`")
    lines.append("- `raw/system_samples_*.json`")
    lines.append("- `charts/concurrency_summary.png`")
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


async def _amain(args: argparse.Namespace) -> int:
    workers = _parse_int_list(args.workers)
    units = _load_units(Path(args.units))
    target_unit = _pick_process_unit(units)
    clip_requests, screenshot_requests = _build_step_requests(
        unit=target_unit,
        step_count=args.step_count,
        step_duration_sec=args.step_duration_sec,
    )

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path(args.output_root) / f"{args.task_name}_{ts}"
    raw_dir = output_dir / "raw"
    charts_dir = output_dir / "charts"
    run_output_root = output_dir / "run_outputs"
    raw_dir.mkdir(parents=True, exist_ok=True)
    charts_dir.mkdir(parents=True, exist_ok=True)
    run_output_root.mkdir(parents=True, exist_ok=True)

    metadata = {
        "generated_at": _now_iso(),
        "video_path": str(Path(args.video).resolve()),
        "units_path": str(Path(args.units).resolve()),
        "output_dir": str(output_dir.resolve()),
        "target_unit_id": str(target_unit.get("unit_id", "")),
        "workers": workers,
        "repeats": int(args.repeats),
        "hard_cap": int(args.hard_cap),
        "step_count_request": int(args.step_count),
        "steps_requested": len(clip_requests),
        "step_duration_sec": float(args.step_duration_sec),
        "sample_interval_sec": float(args.sample_interval_sec),
        "python": os.sys.version,
        "cpu_count": os.cpu_count(),
        "cmd": " ".join(os.sys.argv),
    }
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    run_rows: List[Dict[str, Any]] = []
    unit_id = str(target_unit.get("unit_id", "") or "UNIT")
    for worker in workers:
        for run_idx in range(1, args.repeats + 1):
            print(f"[Run] worker={worker}, round={run_idx}/{args.repeats}")
            this_output = run_output_root / f"w{worker}_r{run_idx}"
            run_record, exported_files, samples = await _run_once(
                video_path=args.video,
                output_dir=this_output,
                unit_id=unit_id,
                clip_requests=clip_requests,
                screenshot_requests=screenshot_requests,
                workers=worker,
                hard_cap=args.hard_cap,
                sample_interval_sec=args.sample_interval_sec,
            )
            run_record["run_index"] = run_idx

            files_path = raw_dir / f"exported_files_w{worker}_r{run_idx}.json"
            sample_path = raw_dir / f"system_samples_w{worker}_r{run_idx}.json"
            files_path.write_text(
                json.dumps(_jsonable(exported_files), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            sample_path.write_text(
                json.dumps(_jsonable(samples), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            run_record["exported_files_file"] = str(files_path.relative_to(output_dir))
            run_record["system_samples_file"] = str(sample_path.relative_to(output_dir))
            run_rows.append(run_record)

    summary_rows = _summarize_by_worker(run_rows)
    recommendation = _select_recommendation(summary_rows)

    (raw_dir / "runs_raw.json").write_text(
        json.dumps(_jsonable(run_rows), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (raw_dir / "summary_by_worker.json").write_text(
        json.dumps(_jsonable(summary_rows), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (raw_dir / "recommendation.json").write_text(
        json.dumps(_jsonable(recommendation), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    _write_csv(raw_dir / "runs_raw.csv", run_rows)
    _write_csv(raw_dir / "summary_by_worker.csv", summary_rows)
    _plot_summary(summary_rows, charts_dir / "concurrency_summary.png")
    _write_report(
        report_path=output_dir / "report.md",
        metadata=metadata,
        summary_rows=summary_rows,
        recommendation=recommendation,
        output_dir=output_dir,
    )

    print("\n=== 并发测试完成 ===")
    print(f"输出目录: {output_dir.resolve()}")
    print(f"推荐 worker: {recommendation.get('best_worker', '')}")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="教程资产导出并发压测（原始数据 + 图表）")
    parser.add_argument("--video", required=True, help="源视频路径")
    parser.add_argument("--units", required=True, help="semantic units JSON 路径")
    parser.add_argument("--output-root", default="var/artifacts/benchmarks", help="产物根目录")
    parser.add_argument("--task-name", default="tutorial_asset_export_concurrency", help="任务名称")

    parser.add_argument("--workers", default="1,2,4,6", help="worker 阶梯")
    parser.add_argument("--repeats", type=int, default=2, help="每档重复次数")
    parser.add_argument("--hard-cap", type=int, default=8, help="并发硬上限")
    parser.add_argument("--step-count", type=int, default=6, help="构造步骤数")
    parser.add_argument("--step-duration-sec", type=float, default=6.0, help="目标步骤时长")
    parser.add_argument("--sample-interval-sec", type=float, default=0.5, help="系统采样周期")

    args = parser.parse_args()
    if args.repeats <= 0:
        raise ValueError("repeats 必须大于 0")
    if args.step_count <= 0:
        raise ValueError("step-count 必须大于 0")

    asyncio.run(_amain(args))


if __name__ == "__main__":
    main()
