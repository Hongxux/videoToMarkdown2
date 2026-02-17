from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import uuid
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

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
from services.python_grpc.src.content_pipeline.infra.runtime.config_loader import load_module2_config
from services.python_grpc.src.content_pipeline.phase2a.materials.vl_video_analyzer import (
    VLVideoAnalyzer,
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


def _load_clip_manifest(path: Path) -> List[Dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    items: List[Any]
    if isinstance(payload, dict):
        for key in ("clips", "items", "dataset"):
            value = payload.get(key)
            if isinstance(value, list):
                items = value
                break
        else:
            raise ValueError("clip manifest 格式错误：缺少 clips/items list")
    elif isinstance(payload, list):
        items = payload
    else:
        raise ValueError("clip manifest 格式错误：应为 list 或 dict")

    clips: List[Dict[str, Any]] = []
    for idx, item in enumerate(items, start=1):
        if isinstance(item, dict):
            clip_path = str(item.get("clip_path", item.get("path", "")) or "").strip()
            start_sec = _safe_float(item.get("semantic_unit_start_sec", 0.0), 0.0)
            unit_id = str(item.get("semantic_unit_id", f"CLIP_{idx:03d}") or f"CLIP_{idx:03d}")
        else:
            clip_path = str(item or "").strip()
            start_sec = 0.0
            unit_id = f"CLIP_{idx:03d}"
        if not clip_path:
            continue
        clips.append(
            {
                "clip_path": clip_path,
                "semantic_unit_start_sec": start_sec,
                "semantic_unit_id": unit_id,
            }
        )
    if not clips:
        raise ValueError("clip manifest 为空")
    return clips


def _expand_clips(
    clips: List[Dict[str, Any]],
    *,
    target_clip_count: int,
    max_clips: int,
) -> List[Dict[str, Any]]:
    resolved: List[Dict[str, Any]] = []
    for item in clips:
        clip_path = Path(item["clip_path"])
        if not clip_path.is_absolute():
            clip_path = (Path.cwd() / clip_path).resolve()
        if not clip_path.exists():
            continue
        resolved.append(
            {
                "clip_path": str(clip_path),
                "semantic_unit_start_sec": float(item.get("semantic_unit_start_sec", 0.0)),
                "semantic_unit_id": str(item.get("semantic_unit_id", clip_path.stem)),
            }
        )

    if not resolved:
        raise ValueError("未找到可用 clip 文件，请检查 manifest 中的路径")

    target = max(1, int(target_clip_count))
    expanded: List[Dict[str, Any]] = []
    while len(expanded) < target:
        for item in resolved:
            expanded.append(dict(item))
            if len(expanded) >= target:
                break

    if max_clips > 0:
        expanded = expanded[:max_clips]
    return expanded


def _build_analyzer_config(*, max_input_frames: int, max_tokens: int, video_input_mode: str) -> Dict[str, Any]:
    full_config = load_module2_config()
    vl_cfg = dict(full_config.get("vl_material_generation", {}) or {})
    api_cfg = dict(vl_cfg.get("api", {}) or {})
    api_cfg["max_input_frames"] = int(max_input_frames)
    if max_tokens > 0:
        api_cfg["max_tokens"] = int(max_tokens)
    if video_input_mode:
        api_cfg["video_input_mode"] = video_input_mode
    vl_cfg["api"] = api_cfg
    vl_cfg.setdefault("screenshot_optimization", {})
    vl_cfg.setdefault("fallback", {})
    return vl_cfg


async def _run_once(
    *,
    clips: List[Dict[str, Any]],
    concurrency: int,
    max_input_frames: int,
    max_tokens: int,
    video_input_mode: str,
    analysis_mode: str,
    extra_prompt: str,
    sample_interval_sec: float,
) -> tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]]]:
    config = _build_analyzer_config(
        max_input_frames=max_input_frames,
        max_tokens=max_tokens,
        video_input_mode=video_input_mode,
    )
    analyzer = VLVideoAnalyzer(config)
    sampler = SystemSampler(sample_interval_sec)
    semaphore = asyncio.Semaphore(max(1, concurrency))

    started_at = now_iso()
    sampler.start()
    t0 = datetime.now().timestamp()

    async def _run_single(idx: int, clip_item: Dict[str, Any]) -> Dict[str, Any]:
        clip_path = str(clip_item["clip_path"])
        unit_id = f"{clip_item.get('semantic_unit_id', 'CLIP')}_{idx:03d}"
        start_sec = float(clip_item.get("semantic_unit_start_sec", 0.0))
        clip_t0 = datetime.now().timestamp()
        success = True
        error_message = ""
        token_usage: Dict[str, int] = {}
        result_size = 0
        clip_requests = 0
        screenshot_requests = 0
        try:
            async with semaphore:
                result = await analyzer.analyze_clip(
                    clip_path=clip_path,
                    semantic_unit_start_sec=start_sec,
                    semantic_unit_id=unit_id,
                    extra_prompt=extra_prompt if extra_prompt else None,
                    analysis_mode=analysis_mode,
                )
            success = bool(result.success)
            if not success:
                error_message = str(result.error_msg or "")
            token_usage = dict(result.token_usage or {})
            result_size = len(result.analysis_results or [])
            clip_requests = len(result.clip_requests or [])
            screenshot_requests = len(result.screenshot_requests or [])
        except Exception as exc:
            success = False
            error_message = str(exc)
        elapsed_ms = (datetime.now().timestamp() - clip_t0) * 1000.0
        return {
            "clip_index": idx,
            "clip_path": clip_path,
            "semantic_unit_id": unit_id,
            "elapsed_ms": elapsed_ms,
            "success": success,
            "error": error_message,
            "result_size": result_size,
            "clip_requests_count": clip_requests,
            "screenshot_requests_count": screenshot_requests,
            "prompt_tokens": _safe_int(token_usage.get("prompt_tokens", 0)),
            "completion_tokens": _safe_int(token_usage.get("completion_tokens", 0)),
            "total_tokens": _safe_int(token_usage.get("total_tokens", 0)),
        }

    raw_results = await asyncio.gather(
        *[_run_single(i, clip) for i, clip in enumerate(clips, start=1)],
        return_exceptions=False,
    )
    elapsed_ms = (datetime.now().timestamp() - t0) * 1000.0
    samples = sampler.stop()
    ended_at = now_iso()

    success_count = sum(1 for item in raw_results if bool(item.get("success")))
    parse_fail_count = sum(
        1
        for item in raw_results
        if (not bool(item.get("success"))) or int(item.get("result_size", 0)) <= 0
    )
    total_tokens = sum(_safe_int(item.get("total_tokens", 0)) for item in raw_results)
    prompt_tokens = sum(_safe_int(item.get("prompt_tokens", 0)) for item in raw_results)
    completion_tokens = sum(_safe_int(item.get("completion_tokens", 0)) for item in raw_results)
    clip_latencies = [_safe_float(item.get("elapsed_ms", 0.0), 0.0) for item in raw_results]

    throughput = 0.0
    if elapsed_ms > 0:
        throughput = len(clips) / (elapsed_ms / 1000.0)

    run_record: Dict[str, Any] = {
        "started_at": started_at,
        "ended_at": ended_at,
        "case_id": f"c={concurrency}|frames={max_input_frames}",
        "concurrency": concurrency,
        "max_input_frames": max_input_frames,
        "max_tokens": max_tokens,
        "video_input_mode": video_input_mode,
        "clips_total": len(clips),
        "success_clips": success_count,
        "success_rate_percent": (success_count / len(clips) * 100.0) if clips else 0.0,
        "parse_fail_clips": parse_fail_count,
        "parse_fail_rate_percent": (parse_fail_count / len(clips) * 100.0) if clips else 0.0,
        "elapsed_ms": elapsed_ms,
        "clip_latency_p95_ms": percentile(clip_latencies, 95),
        "throughput_clips_per_sec": throughput,
        "prompt_tokens_total": prompt_tokens,
        "completion_tokens_total": completion_tokens,
        "total_tokens_total": total_tokens,
        "tokens_per_clip": (total_tokens / len(clips)) if clips else 0.0,
        "sample_count": len(samples),
    }
    run_record.update(summarize_system_samples(samples))

    await analyzer.close()
    return run_record, raw_results, samples


def _summarize_by_case(run_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in run_rows:
        grouped[str(row["case_id"])].append(row)

    summary_rows: List[Dict[str, Any]] = []
    for case_id in sorted(grouped):
        rows = grouped[case_id]
        success_values = [float(item["success_rate_percent"]) for item in rows]
        elapsed_values = [float(item["elapsed_ms"]) for item in rows]
        throughput_values = [float(item["throughput_clips_per_sec"]) for item in rows]
        p95_values = [float(item["clip_latency_p95_ms"]) for item in rows]
        fail_values = [float(item["parse_fail_rate_percent"]) for item in rows]
        tokens_values = [float(item["tokens_per_clip"]) for item in rows]
        sample = rows[0]
        summary_rows.append(
            {
                "case_id": case_id,
                "concurrency": int(sample["concurrency"]),
                "max_input_frames": int(sample["max_input_frames"]),
                "runs": len(rows),
                "success_rate_percent_mean": float(statistics.fmean(success_values)),
                "elapsed_ms_mean": float(statistics.fmean(elapsed_values)),
                "elapsed_ms_p95": percentile(elapsed_values, 95),
                "throughput_clips_per_sec_mean": float(statistics.fmean(throughput_values)),
                "clip_latency_p95_ms_mean": float(statistics.fmean(p95_values)),
                "parse_fail_rate_percent_mean": float(statistics.fmean(fail_values)),
                "tokens_per_clip_mean": float(statistics.fmean(tokens_values)),
            }
        )
    return summary_rows


def _select_recommendation(summary_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not summary_rows:
        return {"best_case_id": "", "rule": "无样本"}

    candidates = [
        row
        for row in summary_rows
        if float(row["success_rate_percent_mean"]) >= 99.0 and float(row["parse_fail_rate_percent_mean"]) <= 1.0
    ]
    if not candidates:
        candidates = list(summary_rows)

    ranked = sorted(
        candidates,
        key=lambda item: (
            -float(item["throughput_clips_per_sec_mean"]),
            float(item["clip_latency_p95_ms_mean"]),
            float(item["tokens_per_clip_mean"]),
        ),
    )
    best = ranked[0]
    return {
        "best_case_id": str(best["case_id"]),
        "best_concurrency": int(best["concurrency"]),
        "best_max_input_frames": int(best["max_input_frames"]),
        "rule": "成功率/解析稳定优先，再取吞吐最高且P95更低、tokens更省",
        "top3": [str(item["case_id"]) for item in ranked[:3]],
    }


def _plot_summary(summary_rows: List[Dict[str, Any]], output_png: Path) -> None:
    if not summary_rows:
        return
    top_rows = sorted(summary_rows, key=lambda item: float(item["throughput_clips_per_sec_mean"]), reverse=True)[:12]
    labels = [f"c{int(r['concurrency'])}\nf{int(r['max_input_frames'])}" for r in top_rows]
    throughput = [float(r["throughput_clips_per_sec_mean"]) for r in top_rows]
    latency = [float(r["clip_latency_p95_ms_mean"]) for r in top_rows]
    fail_rate = [float(r["parse_fail_rate_percent_mean"]) for r in top_rows]
    tokens = [float(r["tokens_per_clip_mean"]) for r in top_rows]

    fig, axes = plt.subplots(2, 2, figsize=(16, 10))

    ax1 = axes[0][0]
    ax1.bar(labels, throughput, color="#1f77b4")
    ax1.set_title("Top Throughput Cases")
    ax1.set_ylabel("clips / sec")
    ax1.tick_params(axis="x", rotation=20)
    ax1.grid(axis="y", alpha=0.3)

    ax2 = axes[0][1]
    ax2.bar(labels, latency, color="#ff7f0e")
    ax2.set_title("P95 Clip Latency")
    ax2.set_ylabel("ms")
    ax2.tick_params(axis="x", rotation=20)
    ax2.grid(axis="y", alpha=0.3)

    ax3 = axes[1][0]
    ax3.bar(labels, fail_rate, color="#d62728")
    ax3.set_title("Parse Fail Rate")
    ax3.set_ylabel("percent")
    ax3.tick_params(axis="x", rotation=20)
    ax3.grid(axis="y", alpha=0.3)

    ax4 = axes[1][1]
    ax4.bar(labels, tokens, color="#2ca02c")
    ax4.set_title("Tokens Per Clip")
    ax4.set_ylabel("tokens")
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
    lines.append("# VL LLM 并发与载荷压测报告")
    lines.append("")
    lines.append(f"- 生成时间: {metadata['generated_at']}")
    lines.append(f"- Clip 清单: `{metadata['clip_manifest_path']}`")
    lines.append(f"- Clip 数量: `{metadata['clip_count']}`")
    lines.append(f"- 并发梯度: `{metadata['concurrency_ladder']}`")
    lines.append(f"- max_input_frames 梯度: `{metadata['max_input_frames_ladder']}`")
    lines.append(f"- 每点重复: `{metadata['repeats']}`")
    lines.append(f"- 原始数据目录: `{output_dir / 'raw'}`")
    lines.append(f"- 图表: `{output_dir / 'charts' / 'concurrency_summary.png'}`")
    lines.append("")
    lines.append("## 汇总")
    lines.append("| case | runs | success(%) | throughput(clips/s) | p95(ms) | parse_fail(%) | tokens/clip |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|")
    for row in sorted(summary_rows, key=lambda item: float(item["throughput_clips_per_sec_mean"]), reverse=True):
        lines.append(
            f"| {row['case_id']} | {int(row['runs'])} | {row['success_rate_percent_mean']:.2f} | "
            f"{row['throughput_clips_per_sec_mean']:.3f} | {row['clip_latency_p95_ms_mean']:.2f} | "
            f"{row['parse_fail_rate_percent_mean']:.2f} | {row['tokens_per_clip_mean']:.2f} |"
        )
    lines.append("")
    lines.append("## 推荐参数")
    lines.append(f"- best_case: `{recommendation.get('best_case_id', '')}`")
    lines.append(f"- best_concurrency: `{recommendation.get('best_concurrency', '')}`")
    lines.append(f"- best_max_input_frames: `{recommendation.get('best_max_input_frames', '')}`")
    lines.append(f"- 规则: {recommendation.get('rule', '')}")
    lines.append("")
    lines.append("## 产物清单")
    lines.append("- `raw/runs_raw.json`")
    lines.append("- `raw/runs_raw.csv`")
    lines.append("- `raw/summary_by_case.json`")
    lines.append("- `raw/summary_by_case.csv`")
    lines.append("- `raw/clip_results_*.json`")
    lines.append("- `raw/system_samples_*.json`")
    lines.append("- `charts/concurrency_summary.png`")
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


async def _amain(args: argparse.Namespace) -> int:
    clip_manifest_path = Path(args.clip_manifest).resolve()
    clips = _load_clip_manifest(clip_manifest_path)
    expanded_clips = _expand_clips(
        clips,
        target_clip_count=args.target_clip_count,
        max_clips=args.max_clips,
    )

    concurrency_ladder = parse_int_list(args.concurrency, "concurrency")
    max_input_frames_ladder = parse_int_list(args.max_input_frames, "max_input_frames")

    output_dir, raw_dir, charts_dir = ensure_benchmark_dirs(args.output_root, args.task_name)
    metadata = {
        "generated_at": now_iso(),
        "clip_manifest_path": str(clip_manifest_path),
        "clip_count": len(expanded_clips),
        "concurrency_ladder": concurrency_ladder,
        "max_input_frames_ladder": max_input_frames_ladder,
        "max_tokens": int(args.max_tokens),
        "video_input_mode": args.video_input_mode,
        "analysis_mode": args.analysis_mode,
        "repeats": int(args.repeats),
        "sample_interval_sec": float(args.sample_interval_sec),
        "output_dir": str(output_dir.resolve()),
        "python": os.sys.version,
        "cmd": " ".join(os.sys.argv),
    }
    write_json(output_dir / "metadata.json", metadata)

    run_rows: List[Dict[str, Any]] = []
    for concurrency in concurrency_ladder:
        for max_input_frames in max_input_frames_ladder:
            case_id = f"c={concurrency}|frames={max_input_frames}"
            for run_idx in range(1, args.repeats + 1):
                print(f"[Run] case={case_id}, round={run_idx}/{args.repeats}")
                run_record, clip_results, samples = await _run_once(
                    clips=expanded_clips,
                    concurrency=concurrency,
                    max_input_frames=max_input_frames,
                    max_tokens=args.max_tokens,
                    video_input_mode=args.video_input_mode,
                    analysis_mode=args.analysis_mode,
                    extra_prompt=args.extra_prompt,
                    sample_interval_sec=args.sample_interval_sec,
                )
                run_record["run_index"] = run_idx
                tag = f"{case_id.replace('|', '__').replace('=', '_')}_r{run_idx}_{uuid.uuid4().hex[:6]}"
                result_path = raw_dir / f"clip_results_{tag}.json"
                sample_path = raw_dir / f"system_samples_{tag}.json"
                write_json(result_path, clip_results)
                write_json(sample_path, samples)
                run_record["clip_results_file"] = str(result_path.relative_to(output_dir))
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

    print("\n=== VL LLM 压测完成 ===")
    print(f"输出目录: {output_dir.resolve()}")
    print(f"推荐 case: {recommendation.get('best_case_id', '')}")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="VL LLM 并发/载荷压测（max_input_frames 作为准批量变量）")
    parser.add_argument(
        "--clip-manifest",
        default="var/artifacts/benchmarks/sample_data/vl_llm_sample/clip_manifest.json",
        help="clip manifest 路径",
    )
    parser.add_argument("--target-clip-count", type=int, default=12, help="不足时循环扩展到该 clip 数")
    parser.add_argument("--max-clips", type=int, default=0, help="限制最终 clip 数，0 表示不限制")
    parser.add_argument("--concurrency", default="1,2,3,4,6,8", help="并发梯度")
    parser.add_argument("--max-input-frames", default="4,8,12,16,24", help="max_input_frames 梯度")
    parser.add_argument("--max-tokens", type=int, default=8192, help="VL max_tokens")
    parser.add_argument("--video-input-mode", default="auto", help="VL video_input_mode")
    parser.add_argument("--analysis-mode", default="default", help="default / tutorial_stepwise")
    parser.add_argument("--extra-prompt", default="", help="附加提示词")
    parser.add_argument("--repeats", type=int, default=2, help="每点重复次数")
    parser.add_argument("--sample-interval-sec", type=float, default=0.5, help="系统采样间隔")
    parser.add_argument("--output-root", default="var/artifacts/benchmarks", help="产物根目录")
    parser.add_argument("--task-name", default="vl_llm_concurrency_payload", help="任务名")

    args = parser.parse_args()
    if args.repeats <= 0:
        raise ValueError("repeats 必须大于 0")
    asyncio.run(_amain(args))


if __name__ == "__main__":
    main()
