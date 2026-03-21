from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import statistics
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import psutil


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


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


def _parse_int_list(raw: str) -> List[int]:
    values: List[int] = []
    for item in (raw or "").split(","):
        token = item.strip()
        if not token:
            continue
        value = int(token)
        if value <= 0:
            raise ValueError(f"整数参数必须大于 0: {token}")
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
        if token not in {"unbounded_submit", "bounded_streaming"}:
            raise ValueError(f"mode 仅支持 unbounded_submit/bounded_streaming: {token}")
        values.append(token)
    dedup = sorted(set(values))
    if not dedup:
        raise ValueError("至少需要一个 mode")
    return dedup


def _make_payload(task_index: int, payload_bytes: int) -> bytes:
    seed = hashlib.sha256(f"task-{task_index}".encode("utf-8")).digest()
    repeat = (payload_bytes // len(seed)) + 1
    return (seed * repeat)[:payload_bytes]


def _cpu_memory_task(payload: bytes, cpu_iterations: int, sleep_ms: int) -> Dict[str, Any]:
    digest = b""
    for _ in range(max(1, int(cpu_iterations))):
        digest = hashlib.sha256(payload + digest).digest()
    if sleep_ms > 0:
        time.sleep(float(sleep_ms) / 1000.0)
    return {
        "payload_bytes": len(payload),
        "digest_prefix": digest.hex()[:16],
        "worker_pid": os.getpid(),
    }


class _SystemSampler:
    def __init__(self, interval_sec: float, metrics_provider) -> None:
        self.interval_sec = max(0.05, float(interval_sec))
        self.metrics_provider = metrics_provider
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
        current = psutil.Process()
        psutil.cpu_percent(interval=None)
        while not self._stop.is_set():
            cpu_percent = float(psutil.cpu_percent(interval=self.interval_sec))
            vm = psutil.virtual_memory()
            parent_rss_mb = 0.0
            child_rss_mb = 0.0
            process_count = 1
            try:
                parent_rss_mb = float(current.memory_info().rss) / (1024.0 * 1024.0)
                children = current.children(recursive=True)
                process_count = 1 + len(children)
                for child in children:
                    try:
                        child_rss_mb += float(child.memory_info().rss) / (1024.0 * 1024.0)
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        continue
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                process_count = 0

            sample = {
                "ts": _now_iso(),
                "cpu_percent": cpu_percent,
                "memory_percent": float(vm.percent),
                "parent_rss_mb": parent_rss_mb,
                "child_rss_mb": child_rss_mb,
                "process_tree_rss_mb": parent_rss_mb + child_rss_mb,
                "process_count": process_count,
            }
            sample.update(_jsonable(self.metrics_provider() or {}))
            self.samples.append(sample)


def _summarize_system_samples(samples: List[Dict[str, Any]]) -> Dict[str, float]:
    if not samples:
        return {
            "cpu_mean": 0.0,
            "cpu_p95": 0.0,
            "mem_percent_mean": 0.0,
            "parent_rss_mb_peak": 0.0,
            "child_rss_mb_peak": 0.0,
            "process_tree_rss_mb_peak": 0.0,
            "process_count_peak": 0.0,
            "scheduler_outstanding_peak": 0.0,
            "executor_inflight_peak": 0.0,
        }

    cpu_values = [float(item.get("cpu_percent", 0.0)) for item in samples]
    mem_values = [float(item.get("memory_percent", 0.0)) for item in samples]
    parent_rss_values = [float(item.get("parent_rss_mb", 0.0)) for item in samples]
    child_rss_values = [float(item.get("child_rss_mb", 0.0)) for item in samples]
    tree_rss_values = [float(item.get("process_tree_rss_mb", 0.0)) for item in samples]
    process_count_values = [float(item.get("process_count", 0.0)) for item in samples]
    outstanding_values = [float(item.get("scheduler_outstanding", 0.0)) for item in samples]
    inflight_values = [float(item.get("executor_inflight", 0.0)) for item in samples]

    return {
        "cpu_mean": float(statistics.fmean(cpu_values)),
        "cpu_p95": _percentile(cpu_values, 95),
        "mem_percent_mean": float(statistics.fmean(mem_values)),
        "parent_rss_mb_peak": max(parent_rss_values) if parent_rss_values else 0.0,
        "child_rss_mb_peak": max(child_rss_values) if child_rss_values else 0.0,
        "process_tree_rss_mb_peak": max(tree_rss_values) if tree_rss_values else 0.0,
        "process_count_peak": max(process_count_values) if process_count_values else 0.0,
        "scheduler_outstanding_peak": max(outstanding_values) if outstanding_values else 0.0,
        "executor_inflight_peak": max(inflight_values) if inflight_values else 0.0,
    }


def _case_id(mode: str, workers: int, queue_size: int, task_count: int, payload_mb: int) -> str:
    return f"m={mode}|w={workers}|q={queue_size}|tasks={task_count}|payload={payload_mb}MB"


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


def _run_case(
    *,
    mode: str,
    workers: int,
    queue_size: int,
    task_count: int,
    payload_mb: int,
    cpu_iterations: int,
    sleep_ms: int,
    sample_interval_sec: float,
) -> tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]]]:
    payload_bytes = int(payload_mb) * 1024 * 1024
    runtime_metrics: Dict[str, float] = {
        "scheduler_outstanding": 0.0,
        "executor_inflight": 0.0,
    }
    results: List[Dict[str, Any]] = []

    def _metrics_provider() -> Dict[str, float]:
        return {
            "scheduler_outstanding": float(runtime_metrics.get("scheduler_outstanding", 0.0)),
            "executor_inflight": float(runtime_metrics.get("executor_inflight", 0.0)),
        }

    sampler = _SystemSampler(sample_interval_sec, _metrics_provider)
    started_at = _now_iso()
    sampler.start()
    t0 = time.perf_counter()
    success = True
    error_message = ""
    futures: Dict[Any, Dict[str, Any]] = {}
    worker_pid_counter: Dict[str, int] = {}

    try:
        with ProcessPoolExecutor(max_workers=workers) as executor:
            next_task_index = 0
            max_outstanding = task_count if mode == "unbounded_submit" else max(1, workers + queue_size)

            while next_task_index < task_count or futures:
                while next_task_index < task_count and len(futures) < max_outstanding:
                    payload = _make_payload(next_task_index, payload_bytes)
                    future = executor.submit(
                        _cpu_memory_task,
                        payload,
                        cpu_iterations,
                        sleep_ms,
                    )
                    futures[future] = {
                        "task_index": next_task_index,
                        "submitted_at": time.perf_counter(),
                    }
                    del payload
                    next_task_index += 1
                    runtime_metrics["scheduler_outstanding"] = float(len(futures))
                    runtime_metrics["executor_inflight"] = float(min(len(futures), workers))

                if not futures:
                    continue

                if mode == "unbounded_submit" and next_task_index >= task_count:
                    done, _ = wait(set(futures.keys()), return_when=FIRST_COMPLETED)
                else:
                    done, _ = wait(set(futures.keys()), return_when=FIRST_COMPLETED)

                for finished in done:
                    meta = futures.pop(finished, {})
                    elapsed_ms = (time.perf_counter() - float(meta.get("submitted_at", time.perf_counter()))) * 1000.0
                    result = finished.result()
                    results.append(
                        {
                            "task_index": int(meta.get("task_index", -1)),
                            "worker_pid": int(result.get("worker_pid", -1)),
                            "payload_mb": payload_mb,
                            "future_elapsed_ms": elapsed_ms,
                            "digest_prefix": result.get("digest_prefix", ""),
                        }
                    )
                    pid_key = str(result.get("worker_pid", -1))
                    worker_pid_counter[pid_key] = int(worker_pid_counter.get(pid_key, 0)) + 1
                runtime_metrics["scheduler_outstanding"] = float(len(futures))
                runtime_metrics["executor_inflight"] = float(min(len(futures), workers))
    except Exception as exc:
        success = False
        error_message = str(exc)

    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    samples = sampler.stop()
    ended_at = _now_iso()

    run_record = {
        "started_at": started_at,
        "ended_at": ended_at,
        "mode": mode,
        "workers": workers,
        "queue_size": queue_size,
        "task_count": task_count,
        "payload_mb": payload_mb,
        "cpu_iterations": cpu_iterations,
        "sleep_ms": sleep_ms,
        "case_id": _case_id(mode, workers, queue_size, task_count, payload_mb),
        "tasks_completed": len(results),
        "elapsed_ms": elapsed_ms,
        "throughput_tasks_per_sec": (len(results) / (elapsed_ms / 1000.0)) if elapsed_ms > 0 else 0.0,
        "success": success,
        "error": error_message,
        "worker_pid_unique_count": len(worker_pid_counter),
        "worker_pid_counter": worker_pid_counter,
        "sample_count": len(samples),
    }
    run_record.update(_summarize_system_samples(samples))
    return run_record, results, samples


def _summarize_by_case(run_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in run_rows:
        grouped.setdefault(str(row["case_id"]), []).append(row)

    summary_rows: List[Dict[str, Any]] = []
    for case_id, rows in sorted(grouped.items()):
        success_rows = [item for item in rows if bool(item["success"])]
        elapsed_values = [float(item["elapsed_ms"]) for item in success_rows]
        throughput_values = [float(item["throughput_tasks_per_sec"]) for item in success_rows]
        outstanding_values = [float(item.get("scheduler_outstanding_peak", 0.0)) for item in rows]
        tree_rss_values = [float(item.get("process_tree_rss_mb_peak", 0.0)) for item in rows]
        parent_rss_values = [float(item.get("parent_rss_mb_peak", 0.0)) for item in rows]
        child_rss_values = [float(item.get("child_rss_mb_peak", 0.0)) for item in rows]
        sample = rows[0]
        summary_rows.append(
            {
                "case_id": case_id,
                "mode": sample["mode"],
                "workers": int(sample["workers"]),
                "queue_size": int(sample["queue_size"]),
                "task_count": int(sample["task_count"]),
                "payload_mb": int(sample["payload_mb"]),
                "runs": len(rows),
                "success_runs": len(success_rows),
                "success_rate_percent": (len(success_rows) / len(rows) * 100.0) if rows else 0.0,
                "elapsed_ms_mean": float(statistics.fmean(elapsed_values)) if elapsed_values else 0.0,
                "elapsed_ms_p95": _percentile(elapsed_values, 95) if elapsed_values else 0.0,
                "throughput_tasks_per_sec_mean": float(statistics.fmean(throughput_values))
                if throughput_values
                else 0.0,
                "scheduler_outstanding_peak_mean": float(statistics.fmean(outstanding_values))
                if outstanding_values
                else 0.0,
                "process_tree_rss_mb_peak_mean": float(statistics.fmean(tree_rss_values))
                if tree_rss_values
                else 0.0,
                "parent_rss_mb_peak_mean": float(statistics.fmean(parent_rss_values))
                if parent_rss_values
                else 0.0,
                "child_rss_mb_peak_mean": float(statistics.fmean(child_rss_values))
                if child_rss_values
                else 0.0,
            }
        )
    return summary_rows


def _write_report(
    *,
    report_path: Path,
    metadata: Dict[str, Any],
    summary_rows: List[Dict[str, Any]],
    output_dir: Path,
) -> None:
    lines: List[str] = []
    lines.append("# ProcessPool Backpressure Guardrail Benchmark")
    lines.append("")
    lines.append(f"- 生成时间: {metadata['generated_at']}")
    lines.append(f"- 模式: `{metadata['modes']}`")
    lines.append(f"- workers: `{metadata['workers']}`")
    lines.append(f"- queue_sizes: `{metadata['queue_sizes']}`")
    lines.append(f"- task_counts: `{metadata['task_counts']}`")
    lines.append(f"- payload_mb: `{metadata['payload_mb']}`")
    lines.append(f"- cpu_iterations: `{metadata['cpu_iterations']}`")
    lines.append(f"- sleep_ms: `{metadata['sleep_ms']}`")
    lines.append(f"- repeats: `{metadata['repeats']}`")
    lines.append(f"- 原始数据目录: `{output_dir / 'raw'}`")
    lines.append("")
    lines.append("## 方法")
    lines.append("- `unbounded_submit`: 一次性把全部大 payload 任务提交到 ProcessPool。")
    lines.append("- `bounded_streaming`: 最多只保留 `workers + queue_size` 个未完成任务，完成一个补一个。")
    lines.append("- 目标是量化 backlog 峰值和进程树 RSS 峰值，直观看到是否存在雪崩放大。")
    lines.append("")
    lines.append("## 汇总")
    lines.append("| case | success(%) | elapsed_mean(ms) | throughput(tasks/s) | out_peak | tree_rss_peak(MB) | parent_rss_peak(MB) | child_rss_peak(MB) |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")
    for row in summary_rows:
        lines.append(
            f"| {row['case_id']} | {row['success_rate_percent']:.2f} | {row['elapsed_ms_mean']:.2f} | "
            f"{row['throughput_tasks_per_sec_mean']:.3f} | {row['scheduler_outstanding_peak_mean']:.2f} | "
            f"{row['process_tree_rss_mb_peak_mean']:.2f} | {row['parent_rss_mb_peak_mean']:.2f} | "
            f"{row['child_rss_mb_peak_mean']:.2f} |"
        )
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="ProcessPool 背压保护机制级 benchmark")
    parser.add_argument("--output-root", default="var/artifacts/benchmarks", help="产物根目录")
    parser.add_argument("--task-name", default="processpool_backpressure_guardrail", help="任务名称")
    parser.add_argument("--modes", default="unbounded_submit,bounded_streaming", help="模式列表")
    parser.add_argument("--workers", default="4", help="worker 数列表")
    parser.add_argument("--queue-sizes", default="2,8", help="bounded 模式 queue 大小列表")
    parser.add_argument("--task-counts", default="40", help="任务数列表")
    parser.add_argument("--payload-mb", type=int, default=16, help="每个任务 payload 大小(MB)")
    parser.add_argument("--cpu-iterations", type=int, default=2, help="CPU 计算迭代次数")
    parser.add_argument("--sleep-ms", type=int, default=400, help="每个任务额外 sleep 毫秒")
    parser.add_argument("--repeats", type=int, default=3, help="每档重复次数")
    parser.add_argument("--sample-interval-sec", type=float, default=0.1, help="系统采样周期")
    args = parser.parse_args()

    modes = _parse_mode_list(args.modes)
    workers_list = _parse_int_list(args.workers)
    queue_sizes = _parse_int_list(args.queue_sizes)
    task_counts = _parse_int_list(args.task_counts)
    payload_mb = max(1, int(args.payload_mb))
    repeats = max(1, int(args.repeats))

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path(args.output_root) / f"{args.task_name}_{ts}"
    raw_dir = output_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    metadata = {
        "generated_at": _now_iso(),
        "output_dir": str(output_dir.resolve()),
        "modes": modes,
        "workers": workers_list,
        "queue_sizes": queue_sizes,
        "task_counts": task_counts,
        "payload_mb": payload_mb,
        "cpu_iterations": int(args.cpu_iterations),
        "sleep_ms": int(args.sleep_ms),
        "repeats": repeats,
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
    for mode in modes:
        for workers in workers_list:
            queue_candidates = [1] if mode == "unbounded_submit" else queue_sizes
            for queue_size in queue_candidates:
                for task_count in task_counts:
                    case_id = _case_id(mode, workers, queue_size, task_count, payload_mb)
                    for run_idx in range(1, repeats + 1):
                        print(f"[Run] case={case_id}, round={run_idx}/{repeats}")
                        run_record, results, samples = _run_case(
                            mode=mode,
                            workers=workers,
                            queue_size=queue_size,
                            task_count=task_count,
                            payload_mb=payload_mb,
                            cpu_iterations=int(args.cpu_iterations),
                            sleep_ms=int(args.sleep_ms),
                            sample_interval_sec=float(args.sample_interval_sec),
                        )
                        run_record["run_index"] = run_idx

                        safe_tag = (
                            case_id.replace("|", "__")
                            .replace("=", "_")
                            .replace("/", "_")
                            .replace(":", "_")
                            .replace(" ", "_")
                        )
                        result_path = raw_dir / f"results_{safe_tag}_r{run_idx}.json"
                        samples_path = raw_dir / f"system_samples_{safe_tag}_r{run_idx}.json"
                        result_path.write_text(
                            json.dumps(_jsonable(results), ensure_ascii=False, indent=2),
                            encoding="utf-8",
                        )
                        samples_path.write_text(
                            json.dumps(_jsonable(samples), ensure_ascii=False, indent=2),
                            encoding="utf-8",
                        )
                        run_record["results_file"] = str(result_path.relative_to(output_dir))
                        run_record["system_samples_file"] = str(samples_path.relative_to(output_dir))
                        run_rows.append(run_record)

    summary_rows = _summarize_by_case(run_rows)
    (raw_dir / "runs_raw.json").write_text(
        json.dumps(_jsonable(run_rows), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (raw_dir / "summary_rows.json").write_text(
        json.dumps(_jsonable(summary_rows), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    _write_csv(raw_dir / "runs_raw.csv", run_rows)
    _write_csv(raw_dir / "summary_rows.csv", summary_rows)
    _write_report(
        report_path=output_dir / "report.md",
        metadata=metadata,
        summary_rows=summary_rows,
        output_dir=output_dir,
    )
    print("\n=== Benchmark 完成 ===")
    print(f"输出目录: {output_dir.resolve()}")


if __name__ == "__main__":
    main()
