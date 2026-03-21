from __future__ import annotations

import argparse
import asyncio
import copy
import csv
import json
import os
import statistics
import threading
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

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
    print(f"[bench_screenshot_opt_concurrency] matplotlib unavailable, skip charts: {exc}", flush=True)


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
        if token not in {"streaming", "batch"}:
            raise ValueError(f"mode 仅支持 streaming 或 batch: {token}")
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
    dedup = sorted(set(values))
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


def _filter_units(units: List[Dict[str, Any]], allowed_kts: List[str]) -> List[Dict[str, Any]]:
    if not allowed_kts:
        return list(units)
    allowed = set(allowed_kts)
    filtered: List[Dict[str, Any]] = []
    for unit in units:
        kt = str(unit.get("knowledge_type", "") or "").strip().lower()
        if kt in allowed:
            filtered.append(unit)
    return filtered


def _build_screenshot_requests(units: List[Dict[str, Any]], requests_per_unit: int) -> List[Dict[str, Any]]:
    requests: List[Dict[str, Any]] = []
    per_unit = max(1, int(requests_per_unit))

    for unit in units:
        unit_id = str(unit.get("unit_id", "") or "").strip()
        if not unit_id:
            continue
        try:
            start_sec = float(unit.get("start_sec", 0.0))
            end_sec = float(unit.get("end_sec", start_sec))
        except (TypeError, ValueError):
            continue
        if end_sec <= start_sec:
            continue

        duration = end_sec - start_sec
        for idx in range(per_unit):
            ratio = (idx + 1) / (per_unit + 1)
            ts = start_sec + duration * ratio
            requests.append(
                {
                    "screenshot_id": f"{unit_id}/{unit_id}_bench_{idx + 1:03d}",
                    "semantic_unit_id": unit_id,
                    "timestamp_sec": float(ts),
                    "label": f"bench_{idx + 1:02d}",
                }
            )
    return requests


def _build_generator_config(
    *,
    mode: str,
    workers: int,
    inflight_multiplier: int,
    overlap_buffers: int,
) -> Dict[str, Any]:
    full_config = load_module2_config()
    vl_cfg = dict(full_config.get("vl_material_generation", {}) or {})
    screenshot_cfg = dict(vl_cfg.get("screenshot_optimization", {}) or {})

    screenshot_cfg["enabled"] = True
    screenshot_cfg["streaming_pipeline"] = mode == "streaming"
    screenshot_cfg["max_workers"] = workers
    screenshot_cfg["max_inflight_multiplier"] = inflight_multiplier
    screenshot_cfg["streaming_overlap_buffers"] = overlap_buffers

    vl_cfg["screenshot_optimization"] = screenshot_cfg
    vl_cfg.setdefault("fallback", {})
    return vl_cfg


def _case_id(mode: str, workers: int, inflight_multiplier: int, overlap_buffers: int) -> str:
    return f"m={mode}|w={workers}|inflight={inflight_multiplier}|overlap={overlap_buffers}"


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
                    "memory_used_gb": float((vm.total - vm.available) / (1024**3)),
                    "memory_available_gb": float(vm.available / (1024**3)),
                }
            )


def _summarize_system_samples(samples: List[Dict[str, Any]]) -> Dict[str, float]:
    if not samples:
        return {
            "cpu_mean": 0.0,
            "cpu_p95": 0.0,
            "mem_percent_mean": 0.0,
            "mem_percent_p95": 0.0,
        }

    cpu_values = [float(item["cpu_percent"]) for item in samples]
    mem_pct_values = [float(item["memory_percent"]) for item in samples]
    return {
        "cpu_mean": float(statistics.fmean(cpu_values)),
        "cpu_p95": _percentile(cpu_values, 95),
        "mem_percent_mean": float(statistics.fmean(mem_pct_values)),
        "mem_percent_p95": _percentile(mem_pct_values, 95),
    }


async def _run_once(
    *,
    video_path: str,
    base_requests: List[Dict[str, Any]],
    mode: str,
    workers: int,
    inflight_multiplier: int,
    overlap_buffers: int,
    sample_interval_sec: float,
) -> tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]]]:
    config = _build_generator_config(
        mode=mode,
        workers=workers,
        inflight_multiplier=inflight_multiplier,
        overlap_buffers=overlap_buffers,
    )
    generator = VLMaterialGenerator(config)
    sampler = _SystemSampler(sample_interval_sec)

    requests = copy.deepcopy(base_requests)
    started_at = _now_iso()
    sampler.start()
    t0 = time.perf_counter()
    success = True
    error_message = ""

    optimized_requests: List[Dict[str, Any]] = []
    try:
        optimized_requests = await generator._optimize_screenshots_parallel(video_path, requests)
    except Exception as exc:
        success = False
        error_message = str(exc)
        optimized_requests = requests

    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    samples = sampler.stop()
    ended_at = _now_iso()

    optimized_count = sum(1 for item in optimized_requests if bool(item.get("_optimized", False)))
    changed_count = 0
    for item in optimized_requests:
        if "_original_timestamp" not in item:
            continue
        try:
            if abs(float(item["timestamp_sec"]) - float(item["_original_timestamp"])) > 1e-6:
                changed_count += 1
        except Exception:
            continue

    throughput_req_per_sec = 0.0
    if success and elapsed_ms > 0:
        throughput_req_per_sec = len(base_requests) / (elapsed_ms / 1000.0)

    run_record: Dict[str, Any] = {
        "started_at": started_at,
        "ended_at": ended_at,
        "mode": mode,
        "workers": workers,
        "inflight_multiplier": inflight_multiplier,
        "overlap_buffers": overlap_buffers,
        "case_id": _case_id(mode, workers, inflight_multiplier, overlap_buffers),
        "requests_total": len(base_requests),
        "optimized_count": int(optimized_count),
        "changed_count": int(changed_count),
        "optimized_rate_percent": (optimized_count / len(base_requests) * 100.0) if base_requests else 0.0,
        "changed_rate_percent": (changed_count / len(base_requests) * 100.0) if base_requests else 0.0,
        "elapsed_ms": elapsed_ms,
        "throughput_req_per_sec": throughput_req_per_sec,
        "success": success,
        "error": error_message,
        "sample_count": len(samples),
    }
    run_record.update(_summarize_system_samples(samples))
    return run_record, optimized_requests, samples


def _summarize_by_case(run_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in run_rows:
        grouped[str(row["case_id"])].append(row)

    summary_rows: List[Dict[str, Any]] = []
    for case_id in sorted(grouped):
        rows = grouped[case_id]
        success_rows = [item for item in rows if bool(item["success"])]

        elapsed_values = [float(item["elapsed_ms"]) for item in success_rows]
        throughput_values = [float(item["throughput_req_per_sec"]) for item in success_rows]
        optimized_rate_values = [float(item["optimized_rate_percent"]) for item in success_rows]
        changed_rate_values = [float(item["changed_rate_percent"]) for item in success_rows]
        cpu_values = [float(item["cpu_mean"]) for item in rows]
        mem_values = [float(item["mem_percent_mean"]) for item in rows]

        sample = rows[0]
        summary_rows.append(
            {
                "case_id": case_id,
                "mode": sample["mode"],
                "workers": int(sample["workers"]),
                "inflight_multiplier": int(sample["inflight_multiplier"]),
                "overlap_buffers": int(sample["overlap_buffers"]),
                "runs": len(rows),
                "success_runs": len(success_rows),
                "success_rate_percent": (len(success_rows) / len(rows) * 100.0) if rows else 0.0,
                "elapsed_ms_mean": float(statistics.fmean(elapsed_values)) if elapsed_values else 0.0,
                "elapsed_ms_p95": _percentile(elapsed_values, 95) if elapsed_values else 0.0,
                "throughput_req_per_sec_mean": float(statistics.fmean(throughput_values))
                if throughput_values
                else 0.0,
                "throughput_req_per_sec_p95": _percentile(throughput_values, 95) if throughput_values else 0.0,
                "optimized_rate_percent_mean": float(statistics.fmean(optimized_rate_values))
                if optimized_rate_values
                else 0.0,
                "changed_rate_percent_mean": float(statistics.fmean(changed_rate_values))
                if changed_rate_values
                else 0.0,
                "cpu_mean_percent": float(statistics.fmean(cpu_values)) if cpu_values else 0.0,
                "mem_mean_percent": float(statistics.fmean(mem_values)) if mem_values else 0.0,
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
            -float(item["throughput_req_per_sec_mean"]),
            float(item["elapsed_ms_mean"]),
            -float(item["success_rate_percent"]),
        ),
    )
    best = ranked[0]
    return {
        "best_case_id": best["case_id"],
        "best_mode": best["mode"],
        "best_workers": int(best["workers"]),
        "best_inflight_multiplier": int(best["inflight_multiplier"]),
        "best_overlap_buffers": int(best["overlap_buffers"]),
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
    if plt is None:
        return
    if not summary_rows:
        return

    rows = sorted(summary_rows, key=lambda item: float(item["throughput_req_per_sec_mean"]), reverse=True)
    labels = [
        f"{row['mode']}\nw{int(row['workers'])}\ni{int(row['inflight_multiplier'])}\no{int(row['overlap_buffers'])}"
        for row in rows
    ]
    elapsed_mean = [float(item["elapsed_ms_mean"]) for item in rows]
    throughput = [float(item["throughput_req_per_sec_mean"]) for item in rows]
    optimized_rate = [float(item["optimized_rate_percent_mean"]) for item in rows]
    success_rate = [float(item["success_rate_percent"]) for item in rows]

    fig, axes = plt.subplots(2, 2, figsize=(16, 10))

    ax1 = axes[0][0]
    ax1.bar(labels, elapsed_mean, color="#1f77b4")
    ax1.set_title("Latency Mean by Case")
    ax1.set_ylabel("elapsed ms")
    ax1.tick_params(axis="x", rotation=20)
    ax1.grid(axis="y", alpha=0.3)

    ax2 = axes[0][1]
    ax2.bar(labels, throughput, color="#2ca02c")
    ax2.set_title("Throughput by Case")
    ax2.set_ylabel("requests / sec")
    ax2.tick_params(axis="x", rotation=20)
    ax2.grid(axis="y", alpha=0.3)

    ax3 = axes[1][0]
    ax3.bar(labels, optimized_rate, color="#ff7f0e")
    ax3.set_title("Optimized Rate by Case")
    ax3.set_ylabel("percent")
    ax3.tick_params(axis="x", rotation=20)
    ax3.grid(axis="y", alpha=0.3)

    ax4 = axes[1][1]
    ax4.bar(labels, success_rate, color="#9467bd")
    ax4.set_ylim(0, 105)
    ax4.set_title("Success Rate by Case")
    ax4.set_ylabel("percent")
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
    lines.append("# 并发测试报告（截图优化流水线）")
    lines.append("")
    lines.append(f"- 生成时间: {metadata['generated_at']}")
    lines.append(f"- 输入视频: `{metadata['video_path']}`")
    lines.append(f"- 输入语义单元: `{metadata['units_path']}`")
    lines.append(f"- 请求总数: `{metadata['request_count']}`")
    lines.append(f"- 测试模式: `{metadata['modes']}`")
    lines.append(f"- worker 阶梯: `{metadata['workers']}`")
    lines.append(f"- inflight 阶梯: `{metadata['inflight_multipliers']}`")
    lines.append(f"- overlap 阶梯: `{metadata['overlap_buffers']}`")
    lines.append(f"- 每档重复次数: `{metadata['repeats']}`")
    lines.append(f"- 原始数据目录: `{output_dir / 'raw'}`")
    lines.append(f"- 图表文件: `{output_dir / 'charts' / 'concurrency_summary.png'}`")
    lines.append("")
    lines.append("## 方法")
    lines.append("- 按 mode × workers × inflight_multiplier × overlap_buffers 组合执行。")
    lines.append("- 每档重复运行后统计 mean/p95。")
    lines.append("- 优选规则：成功率优先，其次吞吐最大且时延更低。")
    lines.append("")
    lines.append("## 汇总")
    lines.append("| case | runs | success(%) | elapsed_mean(ms) | elapsed_p95(ms) | throughput(req/s) | optimized_rate(%) | changed_rate(%) |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")
    for row in sorted(summary_rows, key=lambda item: float(item["throughput_req_per_sec_mean"]), reverse=True):
        lines.append(
            f"| {row['case_id']} | {int(row['runs'])} | {row['success_rate_percent']:.2f} | "
            f"{row['elapsed_ms_mean']:.2f} | {row['elapsed_ms_p95']:.2f} | "
            f"{row['throughput_req_per_sec_mean']:.3f} | {row['optimized_rate_percent_mean']:.2f} | "
            f"{row['changed_rate_percent_mean']:.2f} |"
        )
    lines.append("")
    lines.append("## 推荐值")
    lines.append(f"- best_case: `{recommendation.get('best_case_id', '')}`")
    lines.append(f"- best_mode: `{recommendation.get('best_mode', '')}`")
    lines.append(f"- best_workers: `{recommendation.get('best_workers', '')}`")
    lines.append(f"- best_inflight_multiplier: `{recommendation.get('best_inflight_multiplier', '')}`")
    lines.append(f"- best_overlap_buffers: `{recommendation.get('best_overlap_buffers', '')}`")
    lines.append(f"- 判定依据: {recommendation.get('rule', '')}")
    lines.append("")
    lines.append("## 产物")
    lines.append("- `raw/runs_raw.json`")
    lines.append("- `raw/runs_raw.csv`")
    lines.append("- `raw/summary_by_case.json`")
    lines.append("- `raw/summary_by_case.csv`")
    lines.append("- `raw/optimized_requests_*.json`")
    lines.append("- `raw/system_samples_*.json`")
    lines.append("- `charts/concurrency_summary.png`")
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


async def _amain(args: argparse.Namespace) -> int:
    modes = _parse_mode_list(args.modes)
    workers = _parse_int_list(args.workers)
    inflight_multipliers = _parse_int_list(args.inflight_multipliers)
    overlap_buffers = _parse_int_list(args.overlap_buffers)
    allowed_kts = _parse_knowledge_types(args.knowledge_types)

    units = _load_units(Path(args.units))
    filtered_units = _filter_units(units, allowed_kts)
    if not filtered_units:
        raise ValueError("未命中任何语义单元，无法构造截图请求")

    base_requests = _build_screenshot_requests(filtered_units, args.requests_per_unit)
    if not base_requests:
        raise ValueError("截图请求为空，请检查 units 与 requests_per_unit")

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
        "inflight_multipliers": inflight_multipliers,
        "overlap_buffers": overlap_buffers,
        "repeats": int(args.repeats),
        "requests_per_unit": int(args.requests_per_unit),
        "request_count": len(base_requests),
        "knowledge_types": allowed_kts if allowed_kts else ["all"],
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
        for worker in workers:
            for inflight in inflight_multipliers:
                for overlap in overlap_buffers:
                    cases.append(
                        {
                            "mode": mode,
                            "workers": worker,
                            "inflight_multiplier": inflight,
                            "overlap_buffers": overlap,
                        }
                    )

    if args.warmup_runs > 0:
        warm_case = cases[0]
        for i in range(args.warmup_runs):
            print(f"[Warmup] {i + 1}/{args.warmup_runs} ...")
            await _run_once(
                video_path=args.video,
                base_requests=base_requests,
                mode=warm_case["mode"],
                workers=warm_case["workers"],
                inflight_multiplier=warm_case["inflight_multiplier"],
                overlap_buffers=warm_case["overlap_buffers"],
                sample_interval_sec=args.sample_interval_sec,
            )

    run_rows: List[Dict[str, Any]] = []
    for case in cases:
        case_id = _case_id(
            case["mode"],
            case["workers"],
            case["inflight_multiplier"],
            case["overlap_buffers"],
        )
        for run_idx in range(1, args.repeats + 1):
            print(f"[Run] case={case_id}, round={run_idx}/{args.repeats}")
            run_record, optimized_requests, samples = await _run_once(
                video_path=args.video,
                base_requests=base_requests,
                mode=case["mode"],
                workers=case["workers"],
                inflight_multiplier=case["inflight_multiplier"],
                overlap_buffers=case["overlap_buffers"],
                sample_interval_sec=args.sample_interval_sec,
            )
            run_record["run_index"] = run_idx

            tag = _case_file_tag(case_id)
            req_path = raw_dir / f"optimized_requests_{tag}_r{run_idx}.json"
            sample_path = raw_dir / f"system_samples_{tag}_r{run_idx}.json"

            req_path.write_text(
                json.dumps(_jsonable(optimized_requests), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            sample_path.write_text(
                json.dumps(_jsonable(samples), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            run_record["optimized_requests_file"] = str(req_path.relative_to(output_dir))
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
    parser = argparse.ArgumentParser(description="截图优化并发压测（原始数据 + 图表）")
    parser.add_argument("--video", required=True, help="源视频路径")
    parser.add_argument("--units", required=True, help="semantic units JSON 路径")
    parser.add_argument("--output-root", default="var/artifacts/benchmarks", help="产物根目录")
    parser.add_argument("--task-name", default="screenshot_opt_concurrency", help="任务名称")

    parser.add_argument("--modes", default="streaming", help="模式列表: streaming,batch")
    parser.add_argument("--workers", default="1,2,4,6", help="worker 阶梯")
    parser.add_argument("--inflight-multipliers", default="2", help="inflight multiplier 阶梯")
    parser.add_argument("--overlap-buffers", default="2", help="streaming overlap 阶梯")

    parser.add_argument("--knowledge-types", default="process", help="abstract,concrete,process,all")
    parser.add_argument("--requests-per-unit", type=int, default=3, help="每个单元生成多少截图请求")
    parser.add_argument("--repeats", type=int, default=2, help="每档重复次数")
    parser.add_argument("--sample-interval-sec", type=float, default=0.5, help="系统采样周期")
    parser.add_argument("--warmup-runs", type=int, default=0, help="预热轮次")

    args = parser.parse_args()
    if args.repeats <= 0:
        raise ValueError("repeats 必须大于 0")
    if args.requests_per_unit <= 0:
        raise ValueError("requests-per-unit 必须大于 0")

    asyncio.run(_amain(args))


if __name__ == "__main__":
    main()
