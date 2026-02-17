from __future__ import annotations

import argparse
import asyncio
import csv
import functools
import json
import os
import statistics
import threading
import time
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import matplotlib
import psutil

from services.python_grpc.src.content_pipeline.phase2a.vision.screenshot_selector import ScreenshotSelector
from services.python_grpc.src.vision_validation.worker import init_cv_worker, run_select_screenshots_for_range_task

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


def _parse_mode_list(raw: str) -> List[str]:
    values: List[str] = []
    for item in (raw or "").split(","):
        token = item.strip().lower()
        if not token:
            continue
        if token not in {"process_streaming", "legacy_batch"}:
            raise ValueError(f"mode 仅支持 process_streaming 或 legacy_batch: {token}")
        values.append(token)
    dedup = sorted(set(values))
    if not dedup:
        raise ValueError("至少需要一个 mode")
    return dedup


def _parse_knowledge_types(raw: str) -> List[str]:
    values: List[str] = []
    for item in (raw or "").split(","):
        token = item.strip().lower()
        if not token:
            continue
        if token == "all":
            return []
        if token not in {"abstract", "concrete", "process"}:
            raise ValueError(f"knowledge_type 仅支持 abstract/concrete/process/all: {token}")
        values.append(token)
    return sorted(set(values))


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


def _filter_units(units: List[Dict[str, Any]], allowed_kts: List[str], max_units: int) -> List[Dict[str, Any]]:
    picked: List[Dict[str, Any]] = []
    allowed = set(allowed_kts)
    for unit in units:
        kt = str(unit.get("knowledge_type", "") or "").strip().lower()
        if allowed and kt not in allowed:
            continue
        start_sec = _safe_float(unit.get("start_sec", 0.0), 0.0)
        end_sec = _safe_float(unit.get("end_sec", start_sec), start_sec)
        if end_sec <= start_sec:
            continue
        picked.append(
            {
                "unit_id": str(unit.get("unit_id", "") or "").strip(),
                "start_sec": start_sec,
                "end_sec": end_sec,
                "knowledge_type": kt,
            }
        )
    picked = [item for item in picked if item["unit_id"]]
    picked.sort(key=lambda item: (item["start_sec"], item["unit_id"]))
    if max_units > 0:
        picked = picked[:max_units]
    return picked


def _case_id(mode: str, workers: int, queue_size: int) -> str:
    return f"m={mode}|w={workers}|q={queue_size}"


def _case_file_tag(case_id: str) -> str:
    return (
        case_id.replace("|", "__")
        .replace("=", "_")
        .replace("/", "_")
        .replace(":", "_")
        .replace(" ", "_")
    )


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


async def _run_process_streaming(
    *,
    video_path: str,
    units: List[Dict[str, Any]],
    workers: int,
    queue_size: int,
    coarse_fps: float,
    fine_fps: float,
) -> Tuple[List[Dict[str, Any]], Counter]:
    loop = asyncio.get_running_loop()
    ordered_units = list(enumerate(units))

    unit_queue: asyncio.Queue = asyncio.Queue(maxsize=queue_size)
    result_queue: asyncio.Queue = asyncio.Queue(maxsize=queue_size)
    pid_counter: Counter = Counter()

    async def _producer() -> None:
        for order_idx, unit in ordered_units:
            await unit_queue.put((order_idx, unit))
        for _ in range(workers):
            await unit_queue.put(None)

    async def _consumer(executor: ProcessPoolExecutor) -> None:
        while True:
            item = await unit_queue.get()
            if item is None:
                unit_queue.task_done()
                break

            order_idx, unit = item
            unit_id = unit["unit_id"]
            start_sec = float(unit["start_sec"])
            end_sec = float(unit["end_sec"])

            try:
                result = await loop.run_in_executor(
                    executor,
                    functools.partial(
                        run_select_screenshots_for_range_task,
                        video_path=video_path,
                        unit_id=unit_id,
                        start_sec=start_sec,
                        end_sec=end_sec,
                        coarse_fps=coarse_fps,
                        fine_fps=fine_fps,
                        stable_islands_override=None,
                    ),
                )
            except Exception as exc:
                mid = (start_sec + end_sec) / 2 if end_sec >= start_sec else start_sec
                result = {
                    "unit_id": unit_id,
                    "start_sec": start_sec,
                    "end_sec": end_sec,
                    "screenshots": [{"timestamp_sec": mid, "score": 0.0}],
                    "worker_pid": -1,
                    "elapsed_ms": 0.0,
                    "error": str(exc),
                }

            await result_queue.put((order_idx, result))
            unit_queue.task_done()

    async def _collector(total_count: int) -> Dict[int, Dict[str, Any]]:
        collected: Dict[int, Dict[str, Any]] = {}
        done = 0
        while done < total_count:
            order_idx, result = await result_queue.get()
            collected[order_idx] = result
            done += 1
            pid = result.get("worker_pid")
            if isinstance(pid, int):
                pid_counter[pid] += 1
        return collected

    with ProcessPoolExecutor(max_workers=workers, initializer=init_cv_worker) as executor:
        producer_task = asyncio.create_task(_producer())
        collector_task = asyncio.create_task(_collector(len(ordered_units)))
        consumer_tasks = [asyncio.create_task(_consumer(executor)) for _ in range(workers)]

        await producer_task
        await unit_queue.join()
        await asyncio.gather(*consumer_tasks)
        collected = await collector_task

    results: List[Dict[str, Any]] = []
    for order_idx, _ in ordered_units:
        results.append(collected.get(order_idx, {}))
    return results, pid_counter


def _select_screenshots_sync(
    video_path: str,
    unit_id: str,
    start_sec: float,
    end_sec: float,
    coarse_fps: float,
    fine_fps: float,
) -> Dict[str, Any]:
    started_at = time.perf_counter()
    try:
        selector = ScreenshotSelector.create_lightweight()
        screenshots = selector.select_screenshots_for_range_sync(
            video_path=video_path,
            start_sec=start_sec,
            end_sec=end_sec,
            coarse_fps=coarse_fps,
            fine_fps=fine_fps,
        )
        if not screenshots:
            mid = (start_sec + end_sec) / 2 if end_sec >= start_sec else start_sec
            screenshots = [{"timestamp_sec": mid, "score": 0.0}]
        return {
            "unit_id": unit_id,
            "start_sec": start_sec,
            "end_sec": end_sec,
            "screenshots": screenshots,
            "worker_pid": os.getpid(),
            "elapsed_ms": (time.perf_counter() - started_at) * 1000.0,
        }
    except Exception as exc:
        mid = (start_sec + end_sec) / 2 if end_sec >= start_sec else start_sec
        return {
            "unit_id": unit_id,
            "start_sec": start_sec,
            "end_sec": end_sec,
            "screenshots": [{"timestamp_sec": mid, "score": 0.0}],
            "worker_pid": os.getpid(),
            "elapsed_ms": (time.perf_counter() - started_at) * 1000.0,
            "error": str(exc),
        }


async def _run_legacy_batch(
    *,
    video_path: str,
    units: List[Dict[str, Any]],
    workers: int,
    coarse_fps: float,
    fine_fps: float,
) -> Tuple[List[Dict[str, Any]], Counter]:
    loop = asyncio.get_running_loop()
    semaphore = asyncio.Semaphore(max(1, workers))
    pid_counter: Counter = Counter()

    async def _run_single(unit: Dict[str, Any]) -> Dict[str, Any]:
        async with semaphore:
            with ThreadPoolExecutor(max_workers=1) as pool:
                return await loop.run_in_executor(
                    pool,
                    functools.partial(
                        _select_screenshots_sync,
                        video_path,
                        unit["unit_id"],
                        float(unit["start_sec"]),
                        float(unit["end_sec"]),
                        coarse_fps,
                        fine_fps,
                    ),
                )

    results = await asyncio.gather(*[_run_single(unit) for unit in units], return_exceptions=False)
    for item in results:
        pid = item.get("worker_pid")
        if isinstance(pid, int):
            pid_counter[pid] += 1
    return list(results), pid_counter


async def _run_once(
    *,
    video_path: str,
    units: List[Dict[str, Any]],
    mode: str,
    workers: int,
    queue_size: int,
    coarse_fps: float,
    fine_fps: float,
    sample_interval_sec: float,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]]]:
    sampler = _SystemSampler(sample_interval_sec)
    started_at = _now_iso()
    sampler.start()
    t0 = time.perf_counter()
    success = True
    error_message = ""
    pid_counter: Counter = Counter()

    results: List[Dict[str, Any]] = []
    try:
        if mode == "process_streaming":
            results, pid_counter = await _run_process_streaming(
                video_path=video_path,
                units=units,
                workers=workers,
                queue_size=queue_size,
                coarse_fps=coarse_fps,
                fine_fps=fine_fps,
            )
        else:
            results, pid_counter = await _run_legacy_batch(
                video_path=video_path,
                units=units,
                workers=workers,
                coarse_fps=coarse_fps,
                fine_fps=fine_fps,
            )
    except Exception as exc:
        success = False
        error_message = str(exc)

    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    samples = sampler.stop()
    ended_at = _now_iso()

    screenshots_total = 0
    errors_total = 0
    worker_elapsed_values: List[float] = []
    for item in results:
        screenshots_total += len(item.get("screenshots", []) or [])
        if item.get("error"):
            errors_total += 1
        worker_elapsed_values.append(_safe_float(item.get("elapsed_ms", 0.0), 0.0))

    throughput_units_per_sec = 0.0
    if success and elapsed_ms > 0:
        throughput_units_per_sec = len(units) / (elapsed_ms / 1000.0)

    case_id = _case_id(mode, workers, queue_size)
    run_record: Dict[str, Any] = {
        "started_at": started_at,
        "ended_at": ended_at,
        "mode": mode,
        "workers": workers,
        "queue_size": queue_size,
        "case_id": case_id,
        "units_total": len(units),
        "screenshots_total": int(screenshots_total),
        "error_units": int(errors_total),
        "worker_elapsed_ms_mean": float(statistics.fmean(worker_elapsed_values))
        if worker_elapsed_values
        else 0.0,
        "elapsed_ms": elapsed_ms,
        "throughput_units_per_sec": throughput_units_per_sec,
        "success": success,
        "error": error_message,
        "pid_unique_count": len(pid_counter),
        "pid_counter": dict(pid_counter),
        "sample_count": len(samples),
    }
    run_record.update(_summarize_system_samples(samples))
    return run_record, results, samples


def _summarize_by_case(run_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in run_rows:
        grouped[str(row["case_id"])].append(row)

    summary_rows: List[Dict[str, Any]] = []
    for case_id in sorted(grouped):
        rows = grouped[case_id]
        success_rows = [item for item in rows if bool(item["success"])]
        elapsed_values = [float(item["elapsed_ms"]) for item in success_rows]
        throughput_values = [float(item["throughput_units_per_sec"]) for item in success_rows]
        screenshots_values = [float(item["screenshots_total"]) for item in success_rows]
        error_units_values = [float(item["error_units"]) for item in rows]
        pid_counts = [float(item["pid_unique_count"]) for item in rows]
        sample = rows[0]
        summary_rows.append(
            {
                "case_id": case_id,
                "mode": sample["mode"],
                "workers": int(sample["workers"]),
                "queue_size": int(sample["queue_size"]),
                "runs": len(rows),
                "success_runs": len(success_rows),
                "success_rate_percent": (len(success_rows) / len(rows) * 100.0) if rows else 0.0,
                "elapsed_ms_mean": float(statistics.fmean(elapsed_values)) if elapsed_values else 0.0,
                "elapsed_ms_p95": _percentile(elapsed_values, 95) if elapsed_values else 0.0,
                "throughput_units_per_sec_mean": float(statistics.fmean(throughput_values))
                if throughput_values
                else 0.0,
                "screenshots_total_mean": float(statistics.fmean(screenshots_values))
                if screenshots_values
                else 0.0,
                "error_units_mean": float(statistics.fmean(error_units_values)) if error_units_values else 0.0,
                "pid_unique_count_mean": float(statistics.fmean(pid_counts)) if pid_counts else 0.0,
            }
        )
    return summary_rows


def _select_recommendation(summary_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not summary_rows:
        return {"best_case_id": "", "rule": "无样本"}

    candidates = [row for row in summary_rows if float(row["success_rate_percent"]) >= 99.9]
    if not candidates:
        candidates = list(summary_rows)

    ranked = sorted(
        candidates,
        key=lambda item: (
            -float(item["throughput_units_per_sec_mean"]),
            float(item["elapsed_ms_mean"]),
            float(item["error_units_mean"]),
        ),
    )
    best = ranked[0]

    best_process = None
    best_legacy = None
    for row in ranked:
        if row["mode"] == "process_streaming" and best_process is None:
            best_process = row
        if row["mode"] == "legacy_batch" and best_legacy is None:
            best_legacy = row

    mode_recommend = "process_streaming"
    mode_reason = "仅发现 process_streaming 样本"
    if best_process and best_legacy:
        if float(best_process["throughput_units_per_sec_mean"]) >= float(best_legacy["throughput_units_per_sec_mean"]):
            mode_recommend = "process_streaming"
            mode_reason = "吞吐更高或持平"
        else:
            mode_recommend = "legacy_batch"
            mode_reason = "吞吐更高"

    return {
        "best_case_id": best["case_id"],
        "best_mode": best["mode"],
        "best_workers": int(best["workers"]),
        "best_queue_size": int(best["queue_size"]),
        "mode_recommendation": mode_recommend,
        "mode_reason": mode_reason,
        "rule": "success_rate 优先，随后吞吐最大且时延更低",
        "top3": [item["case_id"] for item in ranked[:3]],
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

    rows = sorted(summary_rows, key=lambda item: float(item["throughput_units_per_sec_mean"]), reverse=True)
    labels = [f"{row['mode']}\nw{int(row['workers'])}\nq{int(row['queue_size'])}" for row in rows]
    elapsed = [float(item["elapsed_ms_mean"]) for item in rows]
    throughput = [float(item["throughput_units_per_sec_mean"]) for item in rows]
    errors = [float(item["error_units_mean"]) for item in rows]
    pid_counts = [float(item["pid_unique_count_mean"]) for item in rows]

    fig, axes = plt.subplots(2, 2, figsize=(16, 10))

    ax1 = axes[0][0]
    ax1.bar(labels, elapsed, color="#1f77b4")
    ax1.set_title("Latency Mean by Case")
    ax1.set_ylabel("elapsed ms")
    ax1.tick_params(axis="x", rotation=20)
    ax1.grid(axis="y", alpha=0.3)

    ax2 = axes[0][1]
    ax2.bar(labels, throughput, color="#2ca02c")
    ax2.set_title("Throughput by Case")
    ax2.set_ylabel("units / sec")
    ax2.tick_params(axis="x", rotation=20)
    ax2.grid(axis="y", alpha=0.3)

    ax3 = axes[1][0]
    ax3.bar(labels, errors, color="#d62728")
    ax3.set_title("Error Units Mean")
    ax3.set_ylabel("count")
    ax3.tick_params(axis="x", rotation=20)
    ax3.grid(axis="y", alpha=0.3)

    ax4 = axes[1][1]
    ax4.bar(labels, pid_counts, color="#9467bd")
    ax4.set_title("Unique Worker PID Mean")
    ax4.set_ylabel("count")
    ax4.tick_params(axis="x", rotation=20)
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
    lines.append("# 并发测试报告（路由截图流水线）")
    lines.append("")
    lines.append(f"- 生成时间: {metadata['generated_at']}")
    lines.append(f"- 输入视频: `{metadata['video_path']}`")
    lines.append(f"- 输入语义单元: `{metadata['units_path']}`")
    lines.append(f"- 路由单元数: `{metadata['units_count']}`")
    lines.append(f"- 测试模式: `{metadata['modes']}`")
    lines.append(f"- worker 阶梯: `{metadata['workers']}`")
    lines.append(f"- queue 阶梯: `{metadata['queue_sizes']}`")
    lines.append(f"- 每档重复次数: `{metadata['repeats']}`")
    lines.append(f"- 原始数据目录: `{output_dir / 'raw'}`")
    lines.append(f"- 图表文件: `{output_dir / 'charts' / 'concurrency_summary.png'}`")
    lines.append("")
    lines.append("## 方法")
    lines.append("- `process_streaming` 模式复刻生产者-消费者队列 + ProcessPool 调度。")
    lines.append("- `legacy_batch` 模式复刻批处理同步截图选择。")
    lines.append("- 组合执行后按吞吐/时延/错误数汇总。")
    lines.append("")
    lines.append("## 汇总")
    lines.append("| case | runs | success(%) | elapsed_mean(ms) | throughput(units/s) | screenshots_mean | error_units_mean | pid_unique_mean |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")
    for row in sorted(summary_rows, key=lambda item: float(item["throughput_units_per_sec_mean"]), reverse=True):
        lines.append(
            f"| {row['case_id']} | {int(row['runs'])} | {row['success_rate_percent']:.2f} | "
            f"{row['elapsed_ms_mean']:.2f} | {row['throughput_units_per_sec_mean']:.3f} | "
            f"{row['screenshots_total_mean']:.2f} | {row['error_units_mean']:.2f} | "
            f"{row['pid_unique_count_mean']:.2f} |"
        )
    lines.append("")
    lines.append("## 推荐值")
    lines.append(f"- best_case: `{recommendation.get('best_case_id', '')}`")
    lines.append(f"- best_mode: `{recommendation.get('best_mode', '')}`")
    lines.append(f"- best_workers: `{recommendation.get('best_workers', '')}`")
    lines.append(f"- best_queue_size: `{recommendation.get('best_queue_size', '')}`")
    lines.append(f"- mode_recommendation: `{recommendation.get('mode_recommendation', '')}`")
    lines.append(f"- mode_reason: {recommendation.get('mode_reason', '')}")
    lines.append(f"- 判定依据: {recommendation.get('rule', '')}")
    lines.append("")
    lines.append("## 产物")
    lines.append("- `raw/runs_raw.json`")
    lines.append("- `raw/runs_raw.csv`")
    lines.append("- `raw/summary_by_case.json`")
    lines.append("- `raw/summary_by_case.csv`")
    lines.append("- `raw/route_results_*.json`")
    lines.append("- `raw/system_samples_*.json`")
    lines.append("- `charts/concurrency_summary.png`")
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


async def _amain(args: argparse.Namespace) -> int:
    modes = _parse_mode_list(args.modes)
    workers = _parse_int_list(args.workers)
    queue_sizes = _parse_int_list(args.queue_sizes)
    allowed_kts = _parse_knowledge_types(args.knowledge_types)

    units = _load_units(Path(args.units))
    route_units = _filter_units(units, allowed_kts, args.max_units)
    if not route_units:
        raise ValueError("未命中任何路由截图单元")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path(args.output_root) / f"{args.task_name}_{ts}"
    raw_dir = output_dir / "raw"
    charts_dir = output_dir / "charts"
    raw_dir.mkdir(parents=True, exist_ok=True)
    charts_dir.mkdir(parents=True, exist_ok=True)

    metadata = {
        "generated_at": _now_iso(),
        "video_path": str(Path(args.video).resolve()),
        "units_path": str(Path(args.units).resolve()),
        "output_dir": str(output_dir.resolve()),
        "modes": modes,
        "workers": workers,
        "queue_sizes": queue_sizes,
        "repeats": int(args.repeats),
        "coarse_fps": float(args.coarse_fps),
        "fine_fps": float(args.fine_fps),
        "knowledge_types": allowed_kts if allowed_kts else ["all"],
        "max_units": int(args.max_units),
        "units_count": len(route_units),
        "sample_interval_sec": float(args.sample_interval_sec),
        "python": os.sys.version,
        "cpu_count": os.cpu_count(),
        "cmd": " ".join(os.sys.argv),
    }
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    cases: List[Dict[str, Any]] = []
    for mode in modes:
        if mode == "legacy_batch":
            for worker in workers:
                cases.append({"mode": mode, "workers": worker, "queue_size": 1})
        else:
            for worker in workers:
                for queue_size in queue_sizes:
                    cases.append({"mode": mode, "workers": worker, "queue_size": queue_size})

    run_rows: List[Dict[str, Any]] = []
    for case in cases:
        case_id = _case_id(case["mode"], case["workers"], case["queue_size"])
        for run_idx in range(1, args.repeats + 1):
            print(f"[Run] case={case_id}, round={run_idx}/{args.repeats}")
            run_record, route_results, samples = await _run_once(
                video_path=args.video,
                units=route_units,
                mode=case["mode"],
                workers=case["workers"],
                queue_size=case["queue_size"],
                coarse_fps=args.coarse_fps,
                fine_fps=args.fine_fps,
                sample_interval_sec=args.sample_interval_sec,
            )
            run_record["run_index"] = run_idx

            tag = _case_file_tag(case_id)
            result_path = raw_dir / f"route_results_{tag}_r{run_idx}.json"
            sample_path = raw_dir / f"system_samples_{tag}_r{run_idx}.json"

            result_path.write_text(
                json.dumps(_jsonable(route_results), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            sample_path.write_text(
                json.dumps(_jsonable(samples), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            run_record["route_results_file"] = str(result_path.relative_to(output_dir))
            run_record["system_samples_file"] = str(sample_path.relative_to(output_dir))
            run_rows.append(run_record)

    summary_rows = _summarize_by_case(run_rows)
    recommendation = _select_recommendation(summary_rows)

    (raw_dir / "runs_raw.json").write_text(
        json.dumps(_jsonable(run_rows), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (raw_dir / "summary_by_case.json").write_text(
        json.dumps(_jsonable(summary_rows), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (raw_dir / "recommendation.json").write_text(
        json.dumps(_jsonable(recommendation), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    _write_csv(raw_dir / "runs_raw.csv", run_rows)
    _write_csv(raw_dir / "summary_by_case.csv", summary_rows)
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
    print(f"推荐 case: {recommendation.get('best_case_id', '')}")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="路由截图并发压测（原始数据 + 图表）")
    parser.add_argument("--video", required=True, help="源视频路径")
    parser.add_argument("--units", required=True, help="semantic units JSON 路径")
    parser.add_argument("--output-root", default="var/artifacts/benchmarks", help="产物根目录")
    parser.add_argument("--task-name", default="route_screenshot_concurrency", help="任务名称")

    parser.add_argument("--modes", default="process_streaming", help="模式列表: process_streaming,legacy_batch")
    parser.add_argument("--workers", default="1,2,4,6", help="worker 阶梯")
    parser.add_argument("--queue-sizes", default="4,8,16", help="queue 大小阶梯")
    parser.add_argument("--coarse-fps", type=float, default=2.0, help="粗采样 fps")
    parser.add_argument("--fine-fps", type=float, default=10.0, help="细采样 fps")

    parser.add_argument("--knowledge-types", default="process", help="abstract,concrete,process,all")
    parser.add_argument("--max-units", type=int, default=0, help="限制测试单元数量，0=不限制")
    parser.add_argument("--repeats", type=int, default=2, help="每档重复次数")
    parser.add_argument("--sample-interval-sec", type=float, default=0.5, help="系统采样周期")

    args = parser.parse_args()
    if args.repeats <= 0:
        raise ValueError("repeats 必须大于 0")

    asyncio.run(_amain(args))


if __name__ == "__main__":
    main()
