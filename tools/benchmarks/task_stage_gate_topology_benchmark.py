from __future__ import annotations

import argparse
import csv
import json
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


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


def _parse_int_list(raw: str) -> List[int]:
    values: List[int] = []
    for item in (raw or "").split(","):
        token = item.strip()
        if not token:
            continue
        value = int(token)
        if value <= 0:
            raise ValueError(f"参数必须大于 0: {token}")
        values.append(value)
    result = sorted(set(values))
    if not result:
        raise ValueError("至少需要一个整数参数")
    return result


@dataclass(frozen=True)
class StageDurations:
    download_ms: int
    transcribe_ms: int
    stage1_ms: int
    llm_ms: int


class _Recorder:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.download_inflight = 0
        self.download_max = 0
        self.transcribe_inflight = 0
        self.transcribe_max = 0
        self.stage1_inflight = 0
        self.stage1_max = 0
        self.llm_inflight = 0
        self.llm_max = 0
        self.download_end_to_transcribe_start_ms: List[float] = []
        self.transcribe_started_while_stage1_inflight = 0

    def _set_max(self, attr_inflight: str, attr_max: str, delta: int) -> None:
        with self._lock:
            current = getattr(self, attr_inflight) + delta
            setattr(self, attr_inflight, current)
            if delta > 0:
                setattr(self, attr_max, max(getattr(self, attr_max), current))

    def enter_download(self) -> None:
        self._set_max("download_inflight", "download_max", 1)

    def leave_download(self) -> None:
        self._set_max("download_inflight", "download_max", -1)

    def enter_transcribe(self, wait_ms: float) -> None:
        with self._lock:
            self.download_end_to_transcribe_start_ms.append(wait_ms)
            if self.stage1_inflight > 0:
                self.transcribe_started_while_stage1_inflight += 1
        self._set_max("transcribe_inflight", "transcribe_max", 1)

    def leave_transcribe(self) -> None:
        self._set_max("transcribe_inflight", "transcribe_max", -1)

    def enter_stage1(self) -> None:
        self._set_max("stage1_inflight", "stage1_max", 1)

    def leave_stage1(self) -> None:
        self._set_max("stage1_inflight", "stage1_max", -1)

    def enter_llm(self) -> None:
        self._set_max("llm_inflight", "llm_max", 1)

    def leave_llm(self) -> None:
        self._set_max("llm_inflight", "llm_max", -1)


def _sleep_ms(value: int) -> None:
    time.sleep(max(0, int(value)) / 1000.0)


def _run_legacy_single_gate(task_count: int, durations: StageDurations) -> Dict[str, Any]:
    io_gate = threading.Semaphore(1)
    phase2_gate = threading.Semaphore(max(1, task_count))
    recorder = _Recorder()
    started_at = time.perf_counter()

    def _task(_idx: int) -> None:
        with io_gate:
            recorder.enter_download()
            _sleep_ms(durations.download_ms)
            recorder.leave_download()
            download_end = time.perf_counter()

            recorder.enter_transcribe(0.0 if download_end is None else 0.0)
            _sleep_ms(durations.transcribe_ms)
            recorder.leave_transcribe()

            recorder.enter_stage1()
            _sleep_ms(durations.stage1_ms)
            recorder.leave_stage1()

        with phase2_gate:
            recorder.enter_llm()
            _sleep_ms(durations.llm_ms)
            recorder.leave_llm()

    with ThreadPoolExecutor(max_workers=task_count) as executor:
        futures = [executor.submit(_task, idx) for idx in range(task_count)]
        for future in futures:
            future.result()

    elapsed_ms = (time.perf_counter() - started_at) * 1000.0
    return {
        "elapsed_ms": elapsed_ms,
        "download_max_inflight": recorder.download_max,
        "transcribe_max_inflight": recorder.transcribe_max,
        "stage1_max_inflight": recorder.stage1_max,
        "llm_max_inflight": recorder.llm_max,
        "download_end_to_transcribe_wait_mean_ms": 0.0,
        "transcribe_started_while_stage1_inflight": recorder.transcribe_started_while_stage1_inflight,
    }


def _run_split_stage_gates(task_count: int, durations: StageDurations) -> Dict[str, Any]:
    download_gate = threading.Semaphore(3)
    transcribe_gate = threading.Semaphore(1)
    phase2_gate = threading.Semaphore(max(1, task_count))
    recorder = _Recorder()
    started_at = time.perf_counter()

    def _task(_idx: int) -> None:
        with download_gate:
            recorder.enter_download()
            _sleep_ms(durations.download_ms)
            recorder.leave_download()
            download_end = time.perf_counter()

        with transcribe_gate:
            wait_ms = (time.perf_counter() - download_end) * 1000.0
            recorder.enter_transcribe(wait_ms)
            _sleep_ms(durations.transcribe_ms)
            recorder.leave_transcribe()

        recorder.enter_stage1()
        _sleep_ms(durations.stage1_ms)
        recorder.leave_stage1()

        with phase2_gate:
            recorder.enter_llm()
            _sleep_ms(durations.llm_ms)
            recorder.leave_llm()

    with ThreadPoolExecutor(max_workers=task_count) as executor:
        futures = [executor.submit(_task, idx) for idx in range(task_count)]
        for future in futures:
            future.result()

    elapsed_ms = (time.perf_counter() - started_at) * 1000.0
    waits = recorder.download_end_to_transcribe_start_ms
    return {
        "elapsed_ms": elapsed_ms,
        "download_max_inflight": recorder.download_max,
        "transcribe_max_inflight": recorder.transcribe_max,
        "stage1_max_inflight": recorder.stage1_max,
        "llm_max_inflight": recorder.llm_max,
        "download_end_to_transcribe_wait_mean_ms": float(statistics.fmean(waits)) if waits else 0.0,
        "transcribe_started_while_stage1_inflight": recorder.transcribe_started_while_stage1_inflight,
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


def _summarize(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row["case_id"]), []).append(row)

    summary: List[Dict[str, Any]] = []
    for case_id, items in grouped.items():
        elapsed = [float(item["elapsed_ms"]) for item in items]
        wait = [float(item["download_end_to_transcribe_wait_mean_ms"]) for item in items]
        summary.append(
            {
                "case_id": case_id,
                "runs": len(items),
                "task_count": int(items[0]["task_count"]),
                "mode": items[0]["mode"],
                "elapsed_mean_ms": float(statistics.fmean(elapsed)),
                "elapsed_p95_ms": _percentile(elapsed, 95),
                "download_end_to_transcribe_wait_mean_ms": float(statistics.fmean(wait)) if wait else 0.0,
                "download_max_inflight_mean": float(statistics.fmean(float(item["download_max_inflight"]) for item in items)),
                "transcribe_max_inflight_mean": float(statistics.fmean(float(item["transcribe_max_inflight"]) for item in items)),
                "stage1_max_inflight_mean": float(statistics.fmean(float(item["stage1_max_inflight"]) for item in items)),
                "llm_max_inflight_mean": float(statistics.fmean(float(item["llm_max_inflight"]) for item in items)),
                "transcribe_started_while_stage1_inflight_mean": float(
                    statistics.fmean(float(item["transcribe_started_while_stage1_inflight"]) for item in items)
                ),
            }
        )
    return sorted(summary, key=lambda item: (int(item["task_count"]), str(item["mode"])))


def _build_report(
    *,
    output_dir: Path,
    metadata: Dict[str, Any],
    summary_rows: List[Dict[str, Any]],
) -> str:
    lines: List[str] = []
    lines.append("# 多阶段门阀拓扑基准报告")
    lines.append("")
    lines.append(f"- 生成时间: {metadata['generated_at']}")
    lines.append(f"- 任务数量阶梯: `{metadata['task_counts']}`")
    lines.append(f"- 重复次数: `{metadata['repeats']}`")
    lines.append(
        "- 阶段耗时配置: "
        f"download={metadata['durations']['download_ms']}ms, "
        f"transcribe={metadata['durations']['transcribe_ms']}ms, "
        f"stage1={metadata['durations']['stage1_ms']}ms, "
        f"llm={metadata['durations']['llm_ms']}ms"
    )
    lines.append(f"- 原始数据目录: `{output_dir / 'raw'}`")
    lines.append("")
    lines.append("## 对比口径")
    lines.append("- `legacy_single_gate`: 单一粗粒度门阀，下载/转写/文本预处理共用同一阶段锁。")
    lines.append("- `split_stage_gates`: 下载与转写拆分为独立门阀，文本预处理不再占用转写配额。")
    lines.append("")
    lines.append("## 汇总")
    lines.append("| case | runs | elapsed_mean(ms) | elapsed_p95(ms) | download->transcribe wait mean(ms) | download_max | transcribe_max | stage1_max | llm_max | transcribe_started_while_stage1_inflight |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for row in summary_rows:
        lines.append(
            f"| {row['case_id']} | {row['runs']} | {row['elapsed_mean_ms']:.2f} | {row['elapsed_p95_ms']:.2f} | "
            f"{row['download_end_to_transcribe_wait_mean_ms']:.2f} | {row['download_max_inflight_mean']:.2f} | "
            f"{row['transcribe_max_inflight_mean']:.2f} | {row['stage1_max_inflight_mean']:.2f} | "
            f"{row['llm_max_inflight_mean']:.2f} | {row['transcribe_started_while_stage1_inflight_mean']:.2f} |"
        )
    lines.append("")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="基于受控 sleep 的多阶段门阀拓扑 benchmark")
    parser.add_argument("--task-counts", default="2,4", help="任务数量阶梯")
    parser.add_argument("--repeats", type=int, default=5, help="每个 case 的重复次数")
    parser.add_argument("--download-ms", type=int, default=120, help="下载阶段耗时")
    parser.add_argument("--transcribe-ms", type=int, default=80, help="转写阶段耗时")
    parser.add_argument("--stage1-ms", type=int, default=300, help="文本预处理阶段耗时")
    parser.add_argument("--llm-ms", type=int, default=300, help="后处理/LLM阶段耗时")
    parser.add_argument("--output-root", default="var/artifacts/benchmarks", help="产物根目录")
    parser.add_argument("--task-name", default="task_stage_gate_topology", help="任务名")
    args = parser.parse_args()

    task_counts = _parse_int_list(args.task_counts)
    if args.repeats <= 0:
        raise ValueError("repeats 必须大于 0")

    durations = StageDurations(
        download_ms=int(args.download_ms),
        transcribe_ms=int(args.transcribe_ms),
        stage1_ms=int(args.stage1_ms),
        llm_ms=int(args.llm_ms),
    )

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path(args.output_root) / f"{args.task_name}_{ts}"
    raw_dir = output_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    metadata = {
        "generated_at": _now_iso(),
        "task_counts": task_counts,
        "repeats": int(args.repeats),
        "durations": {
            "download_ms": durations.download_ms,
            "transcribe_ms": durations.transcribe_ms,
            "stage1_ms": durations.stage1_ms,
            "llm_ms": durations.llm_ms,
        },
        "cmd": " ".join(__import__("sys").argv),
    }
    (output_dir / "metadata.json").write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")

    run_rows: List[Dict[str, Any]] = []
    for task_count in task_counts:
        for mode in ("legacy_single_gate", "split_stage_gates"):
            for run_index in range(1, int(args.repeats) + 1):
                if mode == "legacy_single_gate":
                    result = _run_legacy_single_gate(task_count, durations)
                else:
                    result = _run_split_stage_gates(task_count, durations)
                result.update(
                    {
                        "task_count": int(task_count),
                        "mode": mode,
                        "run_index": run_index,
                        "case_id": f"tasks={task_count}|mode={mode}",
                        "started_at": metadata["generated_at"],
                    }
                )
                run_rows.append(result)
                print(
                    f"[Run] tasks={task_count} mode={mode} round={run_index}/{args.repeats} "
                    f"elapsed_ms={result['elapsed_ms']:.2f}"
                )

    summary_rows = _summarize(run_rows)
    (raw_dir / "runs_raw.json").write_text(json.dumps(run_rows, ensure_ascii=False, indent=2), encoding="utf-8")
    (raw_dir / "summary_rows.json").write_text(json.dumps(summary_rows, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(raw_dir / "runs_raw.csv", run_rows)
    _write_csv(raw_dir / "summary_rows.csv", summary_rows)
    (output_dir / "report.md").write_text(_build_report(output_dir=output_dir, metadata=metadata, summary_rows=summary_rows), encoding="utf-8")
    print(f"\n=== Stage gate benchmark completed ===\noutput_dir={output_dir}")


if __name__ == "__main__":
    main()
