from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import types
import uuid
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import matplotlib

from scripts.bench_common.report_builder import (
    SystemSampler,
    ensure_benchmark_dirs,
    now_iso,
    parse_int_list,
    percentile,
    summarize_system_samples,
    write_csv,
    write_json,
)
from services.python_grpc.src.content_pipeline.infra.llm.llm_client import AdaptiveConcurrencyLimiter
from services.python_grpc.src.content_pipeline.phase2a.segmentation.knowledge_classifier import (
    KnowledgeClassifier,
)

matplotlib.use("Agg")
from matplotlib import pyplot as plt  # noqa: E402


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _normalize_label(raw_value: str) -> str:
    token = str(raw_value or "").strip().lower()
    if not token:
        return "unknown"
    if "process" in token or "过程" in token:
        return "process"
    if "concrete" in token or "具象" in token or "实操" in token:
        return "concrete"
    if "abstract" in token or "讲解" in token or "抽象" in token:
        return "abstract"
    if "deduction" in token or "推演" in token:
        return "deduction"
    if "config" in token or "配置" in token:
        return "configuration"
    return token


def _load_dataset(path: Path) -> List[Dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        for key in ("units", "items", "dataset"):
            value = payload.get(key)
            if isinstance(value, list):
                payload = value
                break
    if not isinstance(payload, list):
        raise ValueError("数据集格式错误：应为 list 或包含 units/items list")
    units: List[Dict[str, Any]] = [item for item in payload if isinstance(item, dict)]
    if not units:
        raise ValueError("数据集为空，无法执行压测")
    return units


def _extract_action_texts(unit: Dict[str, Any], fallback_actions: int = 4) -> List[Dict[str, str]]:
    actions_raw = unit.get("actions")
    actions: List[Dict[str, str]] = []

    if isinstance(actions_raw, list) and actions_raw:
        for idx, item in enumerate(actions_raw, start=1):
            if isinstance(item, dict):
                action_id = str(item.get("id", f"a{idx}"))
                text = str(item.get("text", item.get("subtitle", "")) or "").strip()
                gold = str(item.get("gold", item.get("label", "")) or "").strip()
            else:
                action_id = f"a{idx}"
                text = str(item or "").strip()
                gold = ""
            if not text:
                continue
            actions.append({"id": action_id, "text": text, "gold": gold})

    if actions:
        return actions

    full_text = str(unit.get("full_text", unit.get("text", "")) or "").strip()
    if not full_text:
        return []
    chunks: List[str] = []
    piece = max(40, len(full_text) // max(1, fallback_actions))
    for i in range(0, len(full_text), piece):
        token = full_text[i : i + piece].strip()
        if token:
            chunks.append(token)
    chunks = chunks[: max(1, fallback_actions)]
    return [{"id": f"a{idx+1}", "text": text, "gold": ""} for idx, text in enumerate(chunks)]


def _build_units_payload(
    dataset_units: List[Dict[str, Any]],
    max_units: int,
) -> Tuple[List[Dict[str, Any]], Dict[Tuple[float, float], str], Dict[str, str], int]:
    picked = dataset_units[:max_units] if max_units > 0 else list(dataset_units)
    units_payload: List[Dict[str, Any]] = []
    subtitle_lookup: Dict[Tuple[float, float], str] = {}
    gold_lookup: Dict[str, str] = {}
    action_counter = 0

    for idx, unit in enumerate(picked, start=1):
        unit_id = str(unit.get("unit_id", f"L{idx:03d}") or f"L{idx:03d}")
        title = str(unit.get("title", unit.get("knowledge_topic", f"Unit {idx}")) or f"Unit {idx}")
        full_text = str(unit.get("full_text", unit.get("text", "")) or "")
        actions = _extract_action_texts(unit)
        if not actions:
            continue

        action_segments: List[Dict[str, Any]] = []
        for action in actions:
            action_counter += 1
            start = float(action_counter)
            end = float(action_counter) + 0.01
            action_id = str(action.get("id", f"a{action_counter}"))
            subtitle_text = str(action.get("text", "") or "").strip()
            subtitle_lookup[(round(start, 3), round(end, 3))] = subtitle_text
            action_segments.append(
                {
                    "id": action_id,
                    "start_sec": start,
                    "end_sec": end,
                }
            )
            gold = _normalize_label(action.get("gold", ""))
            if gold and gold != "unknown":
                gold_lookup[f"{unit_id}:{action_id}"] = gold

        units_payload.append(
            {
                "unit_id": unit_id,
                "title": title,
                "full_text": full_text,
                "action_segments": action_segments,
            }
        )

    total_actions = sum(len(unit.get("action_segments", [])) for unit in units_payload)
    if not units_payload or total_actions <= 0:
        raise ValueError("数据集中未构造出有效 action_segments，无法执行压测")
    return units_payload, subtitle_lookup, gold_lookup, total_actions


def _patch_subtitle_lookup(classifier: KnowledgeClassifier, subtitle_lookup: Dict[Tuple[float, float], str]) -> None:
    def _lookup(self: KnowledgeClassifier, start: float, end: float) -> str:
        key = (round(_safe_float(start), 3), round(_safe_float(end), 3))
        return subtitle_lookup.get(key, "")

    classifier._get_subtitles_in_range = types.MethodType(_lookup, classifier)


def _analyze_case_results(
    units_payload: List[Dict[str, Any]],
    results_map: Dict[str, List[Dict[str, Any]]],
    gold_lookup: Dict[str, str],
) -> Dict[str, float]:
    total_actions = 0
    batch_miss = 0
    labeled_total = 0
    labeled_correct = 0

    for unit in units_payload:
        unit_id = str(unit.get("unit_id", ""))
        segments = unit.get("action_segments", []) or []
        results = results_map.get(unit_id, []) if isinstance(results_map, dict) else []
        for idx, seg in enumerate(segments):
            total_actions += 1
            action_id = str(seg.get("id", idx))
            result = results[idx] if idx < len(results) and isinstance(results[idx], dict) else {}
            if str(result.get("key_evidence", "")) == "Batch Miss":
                batch_miss += 1
            gold_key = f"{unit_id}:{action_id}"
            if gold_key in gold_lookup:
                labeled_total += 1
                predicted = _normalize_label(result.get("knowledge_type", ""))
                if predicted == gold_lookup[gold_key]:
                    labeled_correct += 1

    accuracy = 0.0
    if labeled_total > 0:
        accuracy = labeled_correct / labeled_total * 100.0
    miss_rate = (batch_miss / total_actions * 100.0) if total_actions > 0 else 0.0
    return {
        "total_actions": float(total_actions),
        "batch_miss_count": float(batch_miss),
        "batch_miss_rate_percent": miss_rate,
        "labeled_actions": float(labeled_total),
        "accuracy_percent": accuracy,
    }


async def _run_once(
    *,
    classifier: KnowledgeClassifier,
    units_payload: List[Dict[str, Any]],
    gold_lookup: Dict[str, str],
    total_actions: int,
    concurrency: int,
    token_budget: int,
    chunk_size: int,
    full_text_chars: int,
    sample_interval_sec: float,
) -> Tuple[Dict[str, Any], Dict[str, Any], List[Dict[str, Any]]]:
    os.environ["MODULE2_KC_MULTI_TOKEN_BUDGET"] = str(token_budget)
    os.environ["MODULE2_KC_MULTI_MAX_UNITS_PER_CHUNK"] = str(chunk_size)
    os.environ["MODULE2_KC_MULTI_FULL_TEXT_CHARS"] = str(full_text_chars)

    limiter = AdaptiveConcurrencyLimiter(
        initial_limit=max(1, concurrency),
        min_limit=1,
        max_limit=max(1, concurrency),
        window_size=8,
    )
    sampler = SystemSampler(sample_interval_sec)
    started_at = now_iso()
    sampler.start()
    t0 = datetime.now().timestamp()
    success = True
    error_message = ""
    results_map: Dict[str, Any] = {}

    try:
        results_map = await classifier.classify_units_batch(
            units_payload,
            external_limiter=limiter,
        )
    except Exception as exc:
        success = False
        error_message = str(exc)

    elapsed_ms = (datetime.now().timestamp() - t0) * 1000.0
    samples = sampler.stop()
    ended_at = now_iso()

    case_stats = _analyze_case_results(units_payload, results_map, gold_lookup) if success else {
        "total_actions": float(total_actions),
        "batch_miss_count": float(total_actions),
        "batch_miss_rate_percent": 100.0,
        "labeled_actions": 0.0,
        "accuracy_percent": 0.0,
    }
    throughput_actions_per_sec = 0.0
    if elapsed_ms > 0:
        throughput_actions_per_sec = total_actions / (elapsed_ms / 1000.0)

    run_record: Dict[str, Any] = {
        "started_at": started_at,
        "ended_at": ended_at,
        "concurrency": concurrency,
        "token_budget": token_budget,
        "max_units_per_chunk": chunk_size,
        "full_text_chars": full_text_chars,
        "case_id": f"c={concurrency}|chunk={chunk_size}|token={token_budget}",
        "elapsed_ms": elapsed_ms,
        "throughput_actions_per_sec": throughput_actions_per_sec,
        "success": success,
        "error": error_message,
        "limiter_current_limit": limiter.stats.get("current_limit", concurrency),
        "limiter_effective_limit": limiter.stats.get("effective_limit", concurrency),
        "sample_count": len(samples),
    }
    run_record.update(case_stats)
    run_record.update(summarize_system_samples(samples))
    return run_record, results_map, samples


def _summarize_by_case(run_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in run_rows:
        grouped[str(row["case_id"])].append(row)

    summary_rows: List[Dict[str, Any]] = []
    for case_id in sorted(grouped):
        rows = grouped[case_id]
        success_rows = [item for item in rows if bool(item["success"])]
        elapsed_values = [float(item["elapsed_ms"]) for item in success_rows]
        throughput_values = [float(item["throughput_actions_per_sec"]) for item in success_rows]
        miss_values = [float(item["batch_miss_rate_percent"]) for item in success_rows]
        acc_values = [float(item["accuracy_percent"]) for item in success_rows if float(item["labeled_actions"]) > 0]
        limiter_values = [float(item["limiter_effective_limit"]) for item in rows]
        sample = rows[0]
        summary_rows.append(
            {
                "case_id": case_id,
                "concurrency": int(sample["concurrency"]),
                "token_budget": int(sample["token_budget"]),
                "max_units_per_chunk": int(sample["max_units_per_chunk"]),
                "runs": len(rows),
                "success_runs": len(success_rows),
                "success_rate_percent": (len(success_rows) / len(rows) * 100.0) if rows else 0.0,
                "elapsed_ms_mean": float(statistics.fmean(elapsed_values)) if elapsed_values else 0.0,
                "elapsed_ms_p95": percentile(elapsed_values, 95) if elapsed_values else 0.0,
                "throughput_actions_per_sec_mean": float(statistics.fmean(throughput_values))
                if throughput_values
                else 0.0,
                "batch_miss_rate_percent_mean": float(statistics.fmean(miss_values)) if miss_values else 100.0,
                "accuracy_percent_mean": float(statistics.fmean(acc_values)) if acc_values else -1.0,
                "limiter_effective_limit_mean": float(statistics.fmean(limiter_values)) if limiter_values else 0.0,
            }
        )
    return summary_rows


def _select_recommendation(summary_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not summary_rows:
        return {"best_case_id": "", "rule": "无样本"}

    candidates = [
        row
        for row in summary_rows
        if float(row["success_rate_percent"]) >= 99.0 and float(row["batch_miss_rate_percent_mean"]) <= 1.0
    ]
    if not candidates:
        candidates = list(summary_rows)

    ranked = sorted(
        candidates,
        key=lambda item: (
            -float(item["throughput_actions_per_sec_mean"]),
            float(item["elapsed_ms_p95"]),
            float(item["batch_miss_rate_percent_mean"]),
        ),
    )
    best = ranked[0]
    return {
        "best_case_id": str(best["case_id"]),
        "best_concurrency": int(best["concurrency"]),
        "best_token_budget": int(best["token_budget"]),
        "best_max_units_per_chunk": int(best["max_units_per_chunk"]),
        "rule": "成功率>=99%且BatchMiss<=1%优先，再取吞吐最高且P95更低",
        "top3": [str(item["case_id"]) for item in ranked[:3]],
    }


def _plot_summary(summary_rows: List[Dict[str, Any]], output_png: Path) -> None:
    if not summary_rows:
        return
    ranked = sorted(summary_rows, key=lambda item: float(item["throughput_actions_per_sec_mean"]), reverse=True)
    top_rows = ranked[:12]
    labels = [
        f"c{int(row['concurrency'])}\nch{int(row['max_units_per_chunk'])}\nt{int(row['token_budget'])}"
        for row in top_rows
    ]
    throughput = [float(row["throughput_actions_per_sec_mean"]) for row in top_rows]
    latency = [float(row["elapsed_ms_p95"]) for row in top_rows]
    miss_rate = [float(row["batch_miss_rate_percent_mean"]) for row in top_rows]
    accuracy = [float(row["accuracy_percent_mean"]) if float(row["accuracy_percent_mean"]) >= 0 else 0.0 for row in top_rows]

    fig, axes = plt.subplots(2, 2, figsize=(16, 10))

    ax1 = axes[0][0]
    ax1.bar(labels, throughput, color="#1f77b4")
    ax1.set_title("Top Throughput Cases")
    ax1.set_ylabel("actions / sec")
    ax1.tick_params(axis="x", rotation=20)
    ax1.grid(axis="y", alpha=0.3)

    ax2 = axes[0][1]
    ax2.bar(labels, latency, color="#ff7f0e")
    ax2.set_title("P95 Latency")
    ax2.set_ylabel("ms")
    ax2.tick_params(axis="x", rotation=20)
    ax2.grid(axis="y", alpha=0.3)

    ax3 = axes[1][0]
    ax3.bar(labels, miss_rate, color="#d62728")
    ax3.set_title("Batch Miss Rate")
    ax3.set_ylabel("percent")
    ax3.tick_params(axis="x", rotation=20)
    ax3.grid(axis="y", alpha=0.3)

    ax4 = axes[1][1]
    ax4.bar(labels, accuracy, color="#2ca02c")
    ax4.set_title("Quality Accuracy (Labeled)")
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
    lines.append("# DeepSeek 分类并发/批量压测报告")
    lines.append("")
    lines.append(f"- 生成时间: {metadata['generated_at']}")
    lines.append(f"- 数据集: `{metadata['dataset_path']}`")
    lines.append(f"- 单元数: `{metadata['unit_count']}`")
    lines.append(f"- Action 总数: `{metadata['action_count']}`")
    lines.append(f"- 并发梯度: `{metadata['concurrency_ladder']}`")
    lines.append(f"- 分块梯度: `{metadata['chunk_ladder']}`")
    lines.append(f"- token 预算梯度: `{metadata['token_budget_ladder']}`")
    lines.append(f"- 每点重复: `{metadata['repeats']}`")
    lines.append(f"- 原始数据目录: `{output_dir / 'raw'}`")
    lines.append(f"- 图表: `{output_dir / 'charts' / 'concurrency_summary.png'}`")
    lines.append("")
    lines.append("## 汇总")
    lines.append("| case | runs | success(%) | throughput(actions/s) | p95(ms) | batch_miss(%) | accuracy(%) |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|")
    for row in sorted(summary_rows, key=lambda item: float(item["throughput_actions_per_sec_mean"]), reverse=True):
        acc_value = float(row["accuracy_percent_mean"])
        acc_text = f"{acc_value:.2f}" if acc_value >= 0 else "N/A"
        lines.append(
            f"| {row['case_id']} | {int(row['runs'])} | {row['success_rate_percent']:.2f} | "
            f"{row['throughput_actions_per_sec_mean']:.3f} | {row['elapsed_ms_p95']:.2f} | "
            f"{row['batch_miss_rate_percent_mean']:.2f} | {acc_text} |"
        )
    lines.append("")
    lines.append("## 推荐参数")
    lines.append(f"- best_case: `{recommendation.get('best_case_id', '')}`")
    lines.append(f"- best_concurrency: `{recommendation.get('best_concurrency', '')}`")
    lines.append(f"- best_token_budget: `{recommendation.get('best_token_budget', '')}`")
    lines.append(f"- best_max_units_per_chunk: `{recommendation.get('best_max_units_per_chunk', '')}`")
    lines.append(f"- 规则: {recommendation.get('rule', '')}")
    lines.append("")
    lines.append("## 产物清单")
    lines.append("- `raw/runs_raw.json`")
    lines.append("- `raw/runs_raw.csv`")
    lines.append("- `raw/summary_by_case.json`")
    lines.append("- `raw/summary_by_case.csv`")
    lines.append("- `raw/results_*.json`")
    lines.append("- `raw/system_samples_*.json`")
    lines.append("- `charts/concurrency_summary.png`")
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


async def _amain(args: argparse.Namespace) -> int:
    dataset_path = Path(args.dataset).resolve()
    dataset_units = _load_dataset(dataset_path)
    units_payload, subtitle_lookup, gold_lookup, total_actions = _build_units_payload(
        dataset_units,
        args.max_units,
    )

    classifier = KnowledgeClassifier()
    if not getattr(classifier, "_enabled", False):
        raise RuntimeError("DEEPSEEK_API_KEY 未配置，无法执行真实 DeepSeek 压测")
    _patch_subtitle_lookup(classifier, subtitle_lookup)

    concurrency_ladder = parse_int_list(args.concurrency, "concurrency")
    chunk_ladder = parse_int_list(args.chunk_sizes, "chunk_sizes")
    token_budget_ladder = parse_int_list(args.token_budgets, "token_budgets")

    output_dir, raw_dir, charts_dir = ensure_benchmark_dirs(args.output_root, args.task_name)
    metadata = {
        "generated_at": now_iso(),
        "dataset_path": str(dataset_path),
        "output_dir": str(output_dir.resolve()),
        "unit_count": len(units_payload),
        "action_count": total_actions,
        "concurrency_ladder": concurrency_ladder,
        "chunk_ladder": chunk_ladder,
        "token_budget_ladder": token_budget_ladder,
        "repeats": int(args.repeats),
        "full_text_chars": int(args.full_text_chars),
        "sample_interval_sec": float(args.sample_interval_sec),
        "python": os.sys.version,
        "cmd": " ".join(os.sys.argv),
    }
    write_json(output_dir / "metadata.json", metadata)

    if args.warmup_runs > 0:
        warm_case = {
            "concurrency": concurrency_ladder[0],
            "chunk": chunk_ladder[0],
            "token": token_budget_ladder[0],
        }
        for i in range(args.warmup_runs):
            print(f"[Warmup] {i + 1}/{args.warmup_runs}")
            await _run_once(
                classifier=classifier,
                units_payload=units_payload,
                gold_lookup=gold_lookup,
                total_actions=total_actions,
                concurrency=warm_case["concurrency"],
                token_budget=warm_case["token"],
                chunk_size=warm_case["chunk"],
                full_text_chars=args.full_text_chars,
                sample_interval_sec=args.sample_interval_sec,
            )

    run_rows: List[Dict[str, Any]] = []
    for concurrency in concurrency_ladder:
        for chunk_size in chunk_ladder:
            for token_budget in token_budget_ladder:
                case_id = f"c={concurrency}|chunk={chunk_size}|token={token_budget}"
                for run_idx in range(1, args.repeats + 1):
                    print(f"[Run] case={case_id}, round={run_idx}/{args.repeats}")
                    run_record, result_map, samples = await _run_once(
                        classifier=classifier,
                        units_payload=units_payload,
                        gold_lookup=gold_lookup,
                        total_actions=total_actions,
                        concurrency=concurrency,
                        token_budget=token_budget,
                        chunk_size=chunk_size,
                        full_text_chars=args.full_text_chars,
                        sample_interval_sec=args.sample_interval_sec,
                    )
                    run_record["run_index"] = run_idx
                    tag = f"{case_id.replace('|', '__').replace('=', '_')}_r{run_idx}_{uuid.uuid4().hex[:6]}"
                    result_path = raw_dir / f"results_{tag}.json"
                    sample_path = raw_dir / f"system_samples_{tag}.json"
                    write_json(result_path, result_map)
                    write_json(sample_path, samples)
                    run_record["results_file"] = str(result_path.relative_to(output_dir))
                    run_record["system_samples_file"] = str(sample_path.relative_to(output_dir))
                    run_rows.append(run_record)

    summary_rows = _summarize_by_case(run_rows)
    recommendation = _select_recommendation(summary_rows)

    write_json(raw_dir / "runs_raw.json", run_rows)
    write_json(raw_dir / "summary_by_case.json", summary_rows)
    write_json(raw_dir / "recommendation.json", recommendation)
    write_csv(raw_dir / "runs_raw.csv", run_rows)
    write_csv(raw_dir / "summary_by_case.csv", summary_rows)
    _plot_summary(summary_rows, charts_dir / "concurrency_summary.png")
    _write_report(
        report_path=output_dir / "report.md",
        metadata=metadata,
        summary_rows=summary_rows,
        recommendation=recommendation,
        output_dir=output_dir,
    )

    print("\n=== DeepSeek 压测完成 ===")
    print(f"输出目录: {output_dir.resolve()}")
    print(f"推荐 case: {recommendation.get('best_case_id', '')}")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="DeepSeek 分类并发/批量压测（保留原始数据并图表化）")
    parser.add_argument(
        "--dataset",
        default="var/artifacts/benchmarks/sample_data/llm_text/deepseek_units.json",
        help="压测数据集 JSON 路径",
    )
    parser.add_argument("--max-units", type=int, default=0, help="限制参与压测的单元数，0 表示全量")
    parser.add_argument("--concurrency", default="1,2,4,6,8,10,12,16", help="并发梯度")
    parser.add_argument("--chunk-sizes", default="1,2,4,6,8,12", help="max_units_per_chunk 梯度")
    parser.add_argument("--token-budgets", default="4000,8000,12000", help="token budget 梯度")
    parser.add_argument("--full-text-chars", type=int, default=600, help="MODULE2_KC_MULTI_FULL_TEXT_CHARS")
    parser.add_argument("--repeats", type=int, default=3, help="每个点重复次数")
    parser.add_argument("--warmup-runs", type=int, default=0, help="预热轮数")
    parser.add_argument("--sample-interval-sec", type=float, default=0.5, help="系统采样间隔")
    parser.add_argument("--output-root", default="var/artifacts/benchmarks", help="压测产物根目录")
    parser.add_argument("--task-name", default="llm_deepseek_concurrency_batch", help="任务名")

    args = parser.parse_args()
    if args.repeats <= 0:
        raise ValueError("repeats 必须大于 0")
    asyncio.run(_amain(args))


if __name__ == "__main__":
    main()

