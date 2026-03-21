from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import statistics
import threading
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import psutil

from services.python_grpc.src.content_pipeline.infra.runtime.config_loader import load_module2_config
from services.python_grpc.src.content_pipeline.phase2a.materials.vl_material_generator import VLMaterialGenerator

try:
    import matplotlib

    matplotlib.use("Agg")
    from matplotlib import pyplot as plt  # noqa: E402
except Exception as exc:  # pragma: no cover - benchmark helper fallback
    matplotlib = None
    plt = None
    print(f"[bench_pre_vl_concurrency_ladder] matplotlib unavailable, skip charts: {exc}", flush=True)


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _parse_workers(raw: str) -> List[int]:
    values: List[int] = []
    for item in (raw or "").split(","):
        token = item.strip()
        if not token:
            continue
        value = int(token)
        if value <= 0:
            raise ValueError(f"并发值必须大于 0: {token}")
        values.append(value)
    dedup = sorted(set(values))
    if not dedup:
        raise ValueError("至少要提供一个并发值")
    return dedup


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


def _filter_process_units(units: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
        unit
        for unit in units
        if str(unit.get("knowledge_type", "") or "").strip().lower() == "process"
    ]


def _build_generator_config(*, mode: str, workers: int, hard_cap: int) -> Dict[str, Any]:
    full_config = load_module2_config()
    vl_cfg = dict(full_config.get("vl_material_generation", {}) or {})
    pruning_cfg = dict(vl_cfg.get("pre_vl_static_pruning", {}) or {})

    pruning_cfg["enabled"] = True
    pruning_cfg["parallel_mode"] = mode
    pruning_cfg["parallel_workers"] = workers
    pruning_cfg["parallel_hard_cap"] = hard_cap

    vl_cfg["pre_vl_static_pruning"] = pruning_cfg
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
                    "memory_used_gb": float((vm.total - vm.available) / (1024 ** 3)),
                    "memory_available_gb": float(vm.available / (1024 ** 3)),
                }
            )


def _summarize_system_samples(samples: List[Dict[str, Any]]) -> Dict[str, float]:
    if not samples:
        return {
            "cpu_mean": 0.0,
            "cpu_p95": 0.0,
            "mem_percent_mean": 0.0,
            "mem_percent_p95": 0.0,
            "mem_used_gb_mean": 0.0,
            "mem_available_gb_min": 0.0,
        }
    cpu_values = [float(item["cpu_percent"]) for item in samples]
    mem_pct_values = [float(item["memory_percent"]) for item in samples]
    mem_used_values = [float(item["memory_used_gb"]) for item in samples]
    mem_avail_values = [float(item["memory_available_gb"]) for item in samples]
    return {
        "cpu_mean": float(statistics.fmean(cpu_values)),
        "cpu_p95": _percentile(cpu_values, 95),
        "mem_percent_mean": float(statistics.fmean(mem_pct_values)),
        "mem_percent_p95": _percentile(mem_pct_values, 95),
        "mem_used_gb_mean": float(statistics.fmean(mem_used_values)),
        "mem_available_gb_min": float(min(mem_avail_values)),
    }


async def _run_once(
    *,
    video_path: str,
    process_units: List[Dict[str, Any]],
    output_dir: str,
    mode: str,
    workers: int,
    hard_cap: int,
    sample_interval_sec: float,
) -> Tuple[Dict[str, Any], Dict[str, Any], List[Dict[str, Any]]]:
    config = _build_generator_config(mode=mode, workers=workers, hard_cap=hard_cap)
    generator = VLMaterialGenerator(config)
    sampler = _SystemSampler(sample_interval_sec)

    started_at = _now_iso()
    sampler.start()
    t0 = time.perf_counter()

    route_map: Dict[str, Any] = {}
    success = True
    error_message = ""
    try:
        route_map = await generator.preprocess_process_units_for_routing(
            video_path=video_path,
            process_units=process_units,
            output_dir=output_dir,
            force_preprocess=True,
        )
    except Exception as exc:  # pragma: no cover
        success = False
        error_message = str(exc)

    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    samples = sampler.stop()
    ended_at = _now_iso()

    applied_units = sum(
        1 for item in route_map.values() if bool((item or {}).get("preprocess_applied", False))
    )
    effective_duration_total_sec = float(
        sum(float((item or {}).get("effective_duration_sec", 0.0)) for item in route_map.values())
    )

    throughput_units_per_sec = 0.0
    throughput_effective_sec_per_sec = 0.0
    if success and elapsed_ms > 0:
        elapsed_sec = elapsed_ms / 1000.0
        throughput_units_per_sec = len(process_units) / elapsed_sec
        throughput_effective_sec_per_sec = effective_duration_total_sec / elapsed_sec

    run_record = {
        "started_at": started_at,
        "ended_at": ended_at,
        "mode": mode,
        "workers": workers,
        "hard_cap": hard_cap,
        "process_units": len(process_units),
        "route_map_units": len(route_map),
        "applied_units": int(applied_units),
        "effective_duration_total_sec": effective_duration_total_sec,
        "elapsed_ms": elapsed_ms,
        "throughput_units_per_sec": throughput_units_per_sec,
        "throughput_effective_sec_per_sec": throughput_effective_sec_per_sec,
        "success": success,
        "error": error_message,
        "sample_count": len(samples),
    }
    run_record.update(_summarize_system_samples(samples))

    return run_record, route_map, samples


def _summarize_by_worker(run_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for row in run_rows:
        grouped[int(row["workers"])].append(row)

    summary_rows: List[Dict[str, Any]] = []
    for worker in sorted(grouped):
        rows = grouped[worker]
        success_rows = [item for item in rows if bool(item["success"])]

        elapsed_values = [float(item["elapsed_ms"]) for item in success_rows]
        throughput_values = [float(item["throughput_units_per_sec"]) for item in success_rows]
        throughput_effective_values = [
            float(item["throughput_effective_sec_per_sec"]) for item in success_rows
        ]
        cpu_values = [float(item["cpu_mean"]) for item in rows]
        mem_values = [float(item["mem_percent_mean"]) for item in rows]

        summary_rows.append(
            {
                "workers": worker,
                "runs": len(rows),
                "success_runs": len(success_rows),
                "success_rate_percent": (len(success_rows) / len(rows) * 100.0) if rows else 0.0,
                "elapsed_ms_mean": float(statistics.fmean(elapsed_values)) if elapsed_values else 0.0,
                "elapsed_ms_p50": _percentile(elapsed_values, 50) if elapsed_values else 0.0,
                "elapsed_ms_p95": _percentile(elapsed_values, 95) if elapsed_values else 0.0,
                "elapsed_ms_min": float(min(elapsed_values)) if elapsed_values else 0.0,
                "elapsed_ms_max": float(max(elapsed_values)) if elapsed_values else 0.0,
                "throughput_units_per_sec_mean": float(statistics.fmean(throughput_values))
                if throughput_values
                else 0.0,
                "throughput_units_per_sec_p95": _percentile(throughput_values, 95)
                if throughput_values
                else 0.0,
                "throughput_effective_sec_per_sec_mean": float(
                    statistics.fmean(throughput_effective_values)
                )
                if throughput_effective_values
                else 0.0,
                "cpu_mean_percent": float(statistics.fmean(cpu_values)) if cpu_values else 0.0,
                "mem_mean_percent": float(statistics.fmean(mem_values)) if mem_values else 0.0,
            }
        )
    return summary_rows


def _detect_knee(summary_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    rows = sorted(summary_rows, key=lambda item: int(item["workers"]))
    if len(rows) <= 1:
        worker = int(rows[0]["workers"]) if rows else 1
        return {
            "knee_worker": worker,
            "safe_worker_80_percent": worker,
            "rule": "样本不足，默认唯一并发值",
        }

    knee_worker = int(rows[-1]["workers"])
    rule = "未触发拐点规则，采用最高并发"
    for i in range(1, len(rows)):
        prev = rows[i - 1]
        curr = rows[i]
        prev_tp = float(prev["throughput_units_per_sec_mean"])
        curr_tp = float(curr["throughput_units_per_sec_mean"])
        prev_p95 = float(prev["elapsed_ms_p95"])
        curr_p95 = float(curr["elapsed_ms_p95"])

        if prev_tp <= 0 or prev_p95 <= 0:
            continue

        throughput_gain = (curr_tp - prev_tp) / prev_tp
        p95_growth = (curr_p95 - prev_p95) / prev_p95
        success_drop = float(prev["success_rate_percent"]) - float(curr["success_rate_percent"])

        if throughput_gain < 0.05 and (p95_growth > 0.10 or success_drop > 0.1):
            knee_worker = int(prev["workers"])
            rule = (
                f"命中规则: 上调到 {int(curr['workers'])} 时吞吐增幅={throughput_gain*100:.2f}%, "
                f"p95增长={p95_growth*100:.2f}%, 成功率下降={success_drop:.2f}%"
            )
            break

    safe_target = max(1, int(knee_worker * 0.8))
    candidates = [int(item["workers"]) for item in rows if int(item["workers"]) <= safe_target]
    safe_worker = max(candidates) if candidates else min(int(item["workers"]) for item in rows)

    return {
        "knee_worker": knee_worker,
        "safe_worker_80_percent": safe_worker,
        "rule": rule,
    }


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _plot_summary(summary_rows: List[Dict[str, Any]], output_png: Path) -> None:
    if plt is None:
        return
    if not summary_rows:
        return
    rows = sorted(summary_rows, key=lambda item: int(item["workers"]))
    workers = [int(item["workers"]) for item in rows]
    elapsed_mean = [float(item["elapsed_ms_mean"]) for item in rows]
    elapsed_p95 = [float(item["elapsed_ms_p95"]) for item in rows]
    throughput = [float(item["throughput_units_per_sec_mean"]) for item in rows]
    success_rate = [float(item["success_rate_percent"]) for item in rows]
    cpu_mean = [float(item["cpu_mean_percent"]) for item in rows]
    mem_mean = [float(item["mem_mean_percent"]) for item in rows]

    fig, axes = plt.subplots(2, 2, figsize=(14, 9))

    ax1 = axes[0][0]
    ax1.plot(workers, elapsed_mean, marker="o", label="mean")
    ax1.plot(workers, elapsed_p95, marker="s", label="p95")
    ax1.set_title("Latency vs Concurrency")
    ax1.set_xlabel("workers")
    ax1.set_ylabel("elapsed ms")
    ax1.grid(alpha=0.3)
    ax1.legend()

    ax2 = axes[0][1]
    ax2.plot(workers, throughput, marker="o", color="#1f77b4")
    ax2.set_title("Throughput vs Concurrency")
    ax2.set_xlabel("workers")
    ax2.set_ylabel("units / sec")
    ax2.grid(alpha=0.3)

    ax3 = axes[1][0]
    ax3.bar(workers, success_rate, color="#2ca02c")
    ax3.set_ylim(0, 105)
    ax3.set_title("Success Rate")
    ax3.set_xlabel("workers")
    ax3.set_ylabel("percent")
    ax3.grid(axis="y", alpha=0.3)

    ax4 = axes[1][1]
    ax4.plot(workers, cpu_mean, marker="o", label="cpu mean %", color="#ff7f0e")
    ax4.plot(workers, mem_mean, marker="s", label="memory mean %", color="#d62728")
    ax4.set_title("Resource Mean")
    ax4.set_xlabel("workers")
    ax4.set_ylabel("percent")
    ax4.grid(alpha=0.3)
    ax4.legend()

    fig.tight_layout()
    fig.savefig(output_png, dpi=160)
    plt.close(fig)


def _write_report(
    *,
    report_path: Path,
    metadata: Dict[str, Any],
    summary_rows: List[Dict[str, Any]],
    knee: Dict[str, Any],
    output_dir: Path,
) -> None:
    lines: List[str] = []
    lines.append("# 并发测试报告（Pre-VL 静态段预处理）")
    lines.append("")
    lines.append(f"- 生成时间: {metadata['generated_at']}")
    lines.append(f"- 测试目标: {metadata['test_scope']}")
    lines.append(f"- 输入视频: `{metadata['video_path']}`")
    lines.append(f"- 输入语义单元: `{metadata['units_path']}`")
    lines.append(f"- 并发模式: `{metadata['mode']}`")
    lines.append(f"- 阶梯并发: `{metadata['workers']}`")
    lines.append(f"- 每档重复次数: `{metadata['repeats']}`")
    lines.append(f"- 原始数据目录: `{output_dir / 'raw'}`")
    lines.append(f"- 图表文件: `{output_dir / 'charts' / 'concurrency_summary.png'}`")
    lines.append("")
    lines.append("## 方法")
    lines.append("- 阶梯升压：按 1→2→4→6→8 的并发档位执行。")
    lines.append("- 每档重复运行后统计 mean/p50/p95。")
    lines.append("- 拐点判据：吞吐增幅 <5%，且 p95 恶化 >10% 或成功率下降。")
    lines.append("")
    lines.append("## 汇总")
    lines.append("| workers | runs | success_rate(%) | elapsed_mean(ms) | elapsed_p95(ms) | throughput(units/s) | cpu_mean(%) | mem_mean(%) |")
    lines.append("|---:|---:|---:|---:|---:|---:|---:|---:|")
    for row in sorted(summary_rows, key=lambda item: int(item["workers"])):
        lines.append(
            f"| {int(row['workers'])} | {int(row['runs'])} | {row['success_rate_percent']:.2f} | "
            f"{row['elapsed_ms_mean']:.2f} | {row['elapsed_ms_p95']:.2f} | "
            f"{row['throughput_units_per_sec_mean']:.4f} | {row['cpu_mean_percent']:.2f} | "
            f"{row['mem_mean_percent']:.2f} |"
        )
    lines.append("")
    lines.append("## 拐点建议")
    lines.append(f"- 容量拐点并发: `{knee['knee_worker']}`")
    lines.append(f"- 生产安全并发（80%）: `{knee['safe_worker_80_percent']}`")
    lines.append(f"- 判定依据: {knee['rule']}")
    lines.append("")
    lines.append("## 产物")
    lines.append(f"- `raw/runs_raw.json`")
    lines.append(f"- `raw/runs_raw.csv`")
    lines.append(f"- `raw/summary_by_worker.json`")
    lines.append(f"- `raw/summary_by_worker.csv`")
    lines.append(f"- `raw/route_map_w*_r*.json`")
    lines.append(f"- `raw/system_samples_w*_r*.json`")
    lines.append(f"- `charts/concurrency_summary.png`")
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


async def _amain(args: argparse.Namespace) -> int:
    workers = _parse_workers(args.workers)
    units = _load_units(Path(args.units))
    process_units = _filter_process_units(units)
    if not process_units:
        raise ValueError("未找到 knowledge_type=process 的语义单元，无法执行并发测试")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path(args.output_root) / f"{args.task_name}_{ts}"
    raw_dir = output_dir / "raw"
    charts_dir = output_dir / "charts"
    raw_dir.mkdir(parents=True, exist_ok=True)
    charts_dir.mkdir(parents=True, exist_ok=True)

    metadata = {
        "generated_at": _now_iso(),
        "test_scope": "VL 前静态段预处理并发能力（Knee Test）",
        "video_path": str(Path(args.video).resolve()),
        "units_path": str(Path(args.units).resolve()),
        "output_dir": str(output_dir.resolve()),
        "mode": args.mode,
        "workers": workers,
        "hard_cap": args.hard_cap,
        "repeats": args.repeats,
        "sample_interval_sec": args.sample_interval_sec,
        "process_units": len(process_units),
        "python": os.sys.version,
        "cpu_count": os.cpu_count(),
        "cmd": " ".join(os.sys.argv),
    }
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    if args.warmup_runs > 0:
        for i in range(args.warmup_runs):
            print(f"[Warmup] {i + 1}/{args.warmup_runs} ...")
            await _run_once(
                video_path=args.video,
                process_units=process_units,
                output_dir=args.output,
                mode=args.mode,
                workers=workers[0],
                hard_cap=args.hard_cap,
                sample_interval_sec=args.sample_interval_sec,
            )

    run_rows: List[Dict[str, Any]] = []
    for worker in workers:
        for run_idx in range(1, args.repeats + 1):
            print(f"[Run] worker={worker}, round={run_idx}/{args.repeats}")
            run_record, route_map, system_samples = await _run_once(
                video_path=args.video,
                process_units=process_units,
                output_dir=args.output,
                mode=args.mode,
                workers=worker,
                hard_cap=args.hard_cap,
                sample_interval_sec=args.sample_interval_sec,
            )
            run_record["run_index"] = run_idx
            run_record["workers"] = worker
            run_record["mode"] = args.mode

            route_path = raw_dir / f"route_map_w{worker}_r{run_idx}.json"
            sample_path = raw_dir / f"system_samples_w{worker}_r{run_idx}.json"
            route_path.write_text(
                json.dumps(_jsonable(route_map), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            sample_path.write_text(
                json.dumps(_jsonable(system_samples), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            run_record["route_map_file"] = str(route_path.relative_to(output_dir))
            run_record["system_samples_file"] = str(sample_path.relative_to(output_dir))
            run_rows.append(run_record)

    summary_rows = _summarize_by_worker(run_rows)
    knee = _detect_knee(summary_rows)

    (raw_dir / "runs_raw.json").write_text(
        json.dumps(_jsonable(run_rows), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (raw_dir / "summary_by_worker.json").write_text(
        json.dumps(_jsonable(summary_rows), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (raw_dir / "knee_recommendation.json").write_text(
        json.dumps(_jsonable(knee), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    _write_csv(raw_dir / "runs_raw.csv", run_rows)
    _write_csv(raw_dir / "summary_by_worker.csv", summary_rows)
    _plot_summary(summary_rows, charts_dir / "concurrency_summary.png")
    _write_report(
        report_path=output_dir / "report.md",
        metadata=metadata,
        summary_rows=summary_rows,
        knee=knee,
        output_dir=output_dir,
    )

    print("\n=== 并发测试完成 ===")
    print(f"输出目录: {output_dir.resolve()}")
    print(f"拐点并发: {knee['knee_worker']}")
    print(f"安全并发(80%): {knee['safe_worker_80_percent']}")
    print(f"判定依据: {knee['rule']}")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Pre-VL 并发阶梯测试（含原始数据与图表）")
    parser.add_argument("--video", required=True, help="源视频路径")
    parser.add_argument("--units", required=True, help="semantic units JSON 路径")
    parser.add_argument("--output", required=True, help="流程输出目录（用于复用已有切片）")
    parser.add_argument(
        "--output-root",
        default="var/artifacts/benchmarks",
        help="测试产物根目录",
    )
    parser.add_argument("--task-name", default="pre_vl_concurrency", help="测试任务名称")
    parser.add_argument("--mode", default="process", choices=["auto", "process", "async", "off"])
    parser.add_argument("--workers", default="1,2,4,6,8", help="并发阶梯，逗号分隔")
    parser.add_argument("--repeats", type=int, default=2, help="每个并发档位重复次数")
    parser.add_argument("--hard-cap", type=int, default=8, help="并发硬上限")
    parser.add_argument("--sample-interval-sec", type=float, default=0.5, help="系统采样周期")
    parser.add_argument("--warmup-runs", type=int, default=0, help="预热轮次")

    args = parser.parse_args()
    if args.repeats <= 0:
        raise ValueError("repeats 必须大于 0")

    asyncio.run(_amain(args))


if __name__ == "__main__":
    main()
