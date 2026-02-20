from __future__ import annotations

import argparse
import os
import shutil
import statistics
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Tuple

WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

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
from services.python_grpc.src.content_pipeline.phase2b.assembly.request_models import (
    MaterialRequests,
)
from services.python_grpc.src.content_pipeline.phase2b.assembly.rich_text_pipeline import (
    RichTextPipeline,
)


def _build_case_id(structure_workers: int, ocr_workers: int) -> str:
    return f"sw={int(structure_workers)}|ow={int(ocr_workers)}"


def _build_default_requests() -> MaterialRequests:
    return MaterialRequests([], [], [])


def _prepare_units(
    *,
    pipeline: RichTextPipeline,
    semantic_units_json: str,
    max_units: int,
) -> Tuple[List[Any], Dict[str, MaterialRequests]]:
    units, material_requests_map = pipeline._load_semantic_units(semantic_units_json)
    filtered_units = list(units or [])
    if max_units > 0:
        filtered_units = filtered_units[:max_units]
    return filtered_units, material_requests_map


def _run_once(
    *,
    case_id: str,
    run_index: int,
    semantic_units_json: str,
    screenshots_dir: str,
    clips_dir: str,
    video_path: str,
    step2_path: str,
    step6_path: str,
    output_dir: Path,
    structure_mode: str,
    structure_workers: int,
    ocr_workers: int,
    sample_interval_sec: float,
    max_units: int,
    safe_no_delete: bool,
    seed_assets_dir: str,
) -> Dict[str, Any]:
    env_keys = [
        "PHASE2B_STRUCTURE_PREPROCESS_MODE",
        "PHASE2B_STRUCTURE_PREPROCESS_WORKERS",
        "PHASE2B_OCR_VALIDATE_WORKERS",
    ]
    env_backup = {key: os.getenv(key) for key in env_keys}
    os.environ["PHASE2B_STRUCTURE_PREPROCESS_MODE"] = str(structure_mode or "process")
    os.environ["PHASE2B_STRUCTURE_PREPROCESS_WORKERS"] = str(int(structure_workers))
    os.environ["PHASE2B_OCR_VALIDATE_WORKERS"] = str(int(ocr_workers))

    run_output_dir = output_dir / "work" / case_id.replace("|", "_").replace("=", "_") / f"run_{run_index:02d}"
    run_output_dir.mkdir(parents=True, exist_ok=True)
    sampler = SystemSampler(interval_sec=sample_interval_sec)

    started_at = now_iso()
    success = True
    error_message = ""
    units_total = 0
    processed_units = 0
    sampler.start()
    t0 = time.perf_counter()

    try:
        effective_screenshots_dir = str(screenshots_dir or "")
        effective_clips_dir = str(clips_dir or "")
        if seed_assets_dir:
            # 为每次 run 镜像一份素材，避免 no-copy 模式下“外部 assets”被跳过。
            # 同时隔离删除副作用，确保各并发组合输入一致。
            run_assets_dir = run_output_dir / "assets"
            shutil.copytree(seed_assets_dir, run_assets_dir, dirs_exist_ok=True)
            effective_screenshots_dir = str(run_assets_dir)
            effective_clips_dir = str(run_assets_dir)

        pipeline = RichTextPipeline(
            video_path=video_path,
            step2_path=step2_path,
            step6_path=step6_path,
            output_dir=str(run_output_dir),
        )
        if seed_assets_dir:
            pipeline.assets_dir = effective_screenshots_dir
        elif safe_no_delete:
            # 安全模式：将 assets 根目录指向隔离目录，避免压测删除源素材。
            pipeline.assets_dir = str(run_output_dir / "_isolated_assets_root")

        units, material_requests_map = _prepare_units(
            pipeline=pipeline,
            semantic_units_json=semantic_units_json,
            max_units=max_units,
        )
        units_total = len(units)
        for unit in units:
            material_requests = material_requests_map.get(unit.unit_id, _build_default_requests())
            pipeline._apply_external_materials(
                unit=unit,
                screenshots_dir=effective_screenshots_dir,
                clips_dir=effective_clips_dir,
                material_requests=material_requests,
            )
            processed_units += 1
    except Exception as exc:
        success = False
        error_message = str(exc)
    finally:
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        samples = sampler.stop()
        ended_at = now_iso()
        for key, value in env_backup.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    throughput_units_per_sec = 0.0
    if success and elapsed_ms > 0 and processed_units > 0:
        throughput_units_per_sec = float(processed_units) / (elapsed_ms / 1000.0)

    run_row: Dict[str, Any] = {
        "case_id": case_id,
        "run_index": int(run_index),
        "started_at": started_at,
        "ended_at": ended_at,
        "structure_mode": str(structure_mode),
        "structure_workers": int(structure_workers),
        "ocr_workers": int(ocr_workers),
        "units_total": int(units_total),
        "units_processed": int(processed_units),
        "elapsed_ms": float(elapsed_ms),
        "throughput_units_per_sec": float(throughput_units_per_sec),
        "success": bool(success),
        "error": error_message,
        "sample_count": len(samples),
    }
    run_row.update(summarize_system_samples(samples))
    return run_row


def _summarize(run_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[Tuple[int, int], List[Dict[str, Any]]] = defaultdict(list)
    for row in run_rows:
        key = (int(row["structure_workers"]), int(row["ocr_workers"]))
        grouped[key].append(row)

    summary_rows: List[Dict[str, Any]] = []
    for (structure_workers, ocr_workers), rows in sorted(grouped.items()):
        elapsed_values = [float(item["elapsed_ms"]) for item in rows]
        throughput_values = [float(item["throughput_units_per_sec"]) for item in rows]
        success_count = sum(1 for item in rows if bool(item["success"]))
        success_rate = (100.0 * success_count / len(rows)) if rows else 0.0
        summary_rows.append(
            {
                "case_id": _build_case_id(structure_workers, ocr_workers),
                "structure_workers": int(structure_workers),
                "ocr_workers": int(ocr_workers),
                "runs": len(rows),
                "success_rate_percent": float(success_rate),
                "elapsed_mean_ms": float(statistics.fmean(elapsed_values)) if elapsed_values else 0.0,
                "elapsed_p95_ms": percentile(elapsed_values, 95),
                "throughput_mean_units_per_sec": float(statistics.fmean(throughput_values))
                if throughput_values
                else 0.0,
                "throughput_p95_units_per_sec": percentile(throughput_values, 95),
                "cpu_mean_percent": float(
                    statistics.fmean(float(item.get("cpu_mean_percent", 0.0)) for item in rows)
                )
                if rows
                else 0.0,
                "memory_mean_percent": float(
                    statistics.fmean(float(item.get("memory_mean_percent", 0.0)) for item in rows)
                )
                if rows
                else 0.0,
                "memory_available_gb_min": float(
                    min(float(item.get("memory_available_gb_min", 0.0)) for item in rows)
                )
                if rows
                else 0.0,
            }
        )
    return summary_rows


def _recommend(summary_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    candidates = [row for row in summary_rows if float(row.get("success_rate_percent", 0.0)) >= 100.0]
    if not candidates:
        candidates = list(summary_rows)
    if not candidates:
        return {}

    ranked = sorted(
        candidates,
        key=lambda item: (
            -float(item.get("throughput_mean_units_per_sec", 0.0)),
            float(item.get("elapsed_mean_ms", 0.0)),
            int(item.get("structure_workers", 0)) + int(item.get("ocr_workers", 0)),
        ),
    )
    best = ranked[0]
    return {
        "case_id": str(best.get("case_id", "")),
        "structure_workers": int(best.get("structure_workers", 1)),
        "ocr_workers": int(best.get("ocr_workers", 1)),
        "throughput_mean_units_per_sec": float(best.get("throughput_mean_units_per_sec", 0.0)),
        "elapsed_mean_ms": float(best.get("elapsed_mean_ms", 0.0)),
        "reason": "按吞吐优先，其次时延与总并发成本排序。",
    }


def _build_report(
    *,
    args: argparse.Namespace,
    output_dir: Path,
    summary_rows: List[Dict[str, Any]],
    recommendation: Dict[str, Any],
) -> str:
    lines: List[str] = []
    lines.append("# Phase2B 并发压测报告")
    lines.append("")
    lines.append("## 目标")
    lines.append("- 验证结构预处理（ProcessPool）并发与 OCR/校验（ThreadPool）并发组合的性能上限。")
    lines.append("- 给出可直接落地的推荐并发配置。")
    lines.append("")
    lines.append("## 参数")
    lines.append(f"- semantic_units_json: `{args.semantic_units_json}`")
    lines.append(f"- screenshots_dir: `{args.screenshots_dir}`")
    lines.append(f"- clips_dir: `{args.clips_dir}`")
    lines.append(f"- structure_mode: `{args.structure_mode}`")
    lines.append(f"- structure_workers: `{args.structure_workers}`")
    lines.append(f"- ocr_workers: `{args.ocr_workers}`")
    lines.append(f"- rounds: `{args.rounds}`")
    lines.append(f"- max_units: `{args.max_units}`")
    lines.append(f"- safe_no_delete: `{bool(args.safe_no_delete)}`")
    lines.append(f"- seed_assets_dir: `{args.seed_assets_dir}`")
    lines.append("")
    lines.append("## 结果汇总")
    lines.append(
        "| case_id | runs | success_rate(%) | elapsed_mean(ms) | elapsed_p95(ms) | throughput_mean(units/s) | cpu_mean(%) | mem_mean(%) |"
    )
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")
    for row in summary_rows:
        lines.append(
            f"| {row['case_id']} | {int(row['runs'])} | {row['success_rate_percent']:.2f} | "
            f"{row['elapsed_mean_ms']:.1f} | {row['elapsed_p95_ms']:.1f} | "
            f"{row['throughput_mean_units_per_sec']:.4f} | {row['cpu_mean_percent']:.2f} | "
            f"{row['memory_mean_percent']:.2f} |"
        )
    lines.append("")
    lines.append("## 推荐")
    if recommendation:
        lines.append(f"- case_id: `{recommendation['case_id']}`")
        lines.append(
            f"- 推荐环境变量: `PHASE2B_STRUCTURE_PREPROCESS_WORKERS={recommendation['structure_workers']}` "
            f"`PHASE2B_OCR_VALIDATE_WORKERS={recommendation['ocr_workers']}`"
        )
        lines.append(
            f"- 参考吞吐: `{recommendation['throughput_mean_units_per_sec']:.4f} units/s`，"
            f"平均时延 `{recommendation['elapsed_mean_ms']:.1f} ms`"
        )
    else:
        lines.append("- 未产出推荐结果，请检查 raw 目录中的失败记录。")
    lines.append("")
    lines.append("## 产物")
    lines.append(f"- `{output_dir / 'raw' / 'run_rows.json'}`")
    lines.append(f"- `{output_dir / 'raw' / 'run_rows.csv'}`")
    lines.append(f"- `{output_dir / 'raw' / 'summary_rows.json'}`")
    lines.append(f"- `{output_dir / 'raw' / 'summary_rows.csv'}`")
    lines.append(f"- `{output_dir / 'raw' / 'recommendation.json'}`")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Phase2B 结构预处理 + OCR/校验 并发压测")
    parser.add_argument("--semantic-units-json", required=True, help="Phase2A 输出 semantic_units JSON 路径")
    parser.add_argument("--screenshots-dir", required=True, help="截图根目录（通常为 output/assets）")
    parser.add_argument("--clips-dir", default="", help="片段根目录，默认为 screenshots_dir")
    parser.add_argument("--video-path", default="", help="可选：视频路径")
    parser.add_argument("--step2-path", default="", help="可选：step2 字幕路径")
    parser.add_argument("--step6-path", default="", help="可选：step6 句子路径")
    parser.add_argument("--structure-mode", default="process", help="结构预处理模式：off/serial/process/auto")
    parser.add_argument("--structure-workers", default="1,2,4,6,8", help="结构预处理并发阶梯")
    parser.add_argument("--ocr-workers", default="1,2,4,6,8", help="OCR/校验并发阶梯")
    parser.add_argument("--rounds", type=int, default=3, help="每个并发组合执行轮数")
    parser.add_argument("--warmup", action="store_true", help="每个并发组合先执行 1 次预热（不计入结果）")
    parser.add_argument("--max-units", type=int, default=0, help="仅压测前 N 个 unit，0 表示全量")
    parser.add_argument("--sample-interval-sec", type=float, default=0.5, help="系统采样间隔（秒）")
    parser.add_argument("--safe-no-delete", action="store_true", help="安全模式：不允许压测删除源素材")
    parser.add_argument(
        "--seed-assets-dir",
        default="",
        help="将该目录镜像到每个 run 的 workdir/assets，再执行压测（推荐用于 no-copy 模式）",
    )
    parser.add_argument("--output-root", default="var/artifacts/benchmarks", help="产物根目录")
    parser.add_argument("--task-name", default="phase2b_material_flow_concurrency", help="任务名")
    args = parser.parse_args()

    if args.rounds <= 0:
        raise ValueError("rounds 必须大于 0")
    if args.sample_interval_sec <= 0:
        raise ValueError("sample_interval_sec 必须大于 0")

    semantic_units_json = str(Path(args.semantic_units_json).resolve())
    screenshots_dir = str(Path(args.screenshots_dir).resolve())
    clips_dir = str(Path(args.clips_dir).resolve()) if args.clips_dir else screenshots_dir
    structure_workers_ladder = parse_int_list(args.structure_workers, "structure_workers")
    ocr_workers_ladder = parse_int_list(args.ocr_workers, "ocr_workers")

    output_dir, raw_dir, _charts_dir = ensure_benchmark_dirs(args.output_root, args.task_name)
    metadata = {
        "started_at": now_iso(),
        "semantic_units_json": semantic_units_json,
        "screenshots_dir": screenshots_dir,
        "clips_dir": clips_dir,
        "video_path": args.video_path,
        "step2_path": args.step2_path,
        "step6_path": args.step6_path,
        "structure_mode": args.structure_mode,
        "structure_workers_ladder": structure_workers_ladder,
        "ocr_workers_ladder": ocr_workers_ladder,
        "rounds": int(args.rounds),
        "warmup": bool(args.warmup),
        "max_units": int(args.max_units),
        "sample_interval_sec": float(args.sample_interval_sec),
        "safe_no_delete": bool(args.safe_no_delete),
        "seed_assets_dir": str(args.seed_assets_dir or ""),
    }
    write_json(raw_dir / "metadata.json", metadata)

    run_rows: List[Dict[str, Any]] = []
    for structure_workers in structure_workers_ladder:
        for ocr_workers in ocr_workers_ladder:
            case_id = _build_case_id(structure_workers, ocr_workers)
            print(f"\n[Case] {case_id}")
            if args.warmup:
                _ = _run_once(
                    case_id=case_id,
                    run_index=0,
                    semantic_units_json=semantic_units_json,
                    screenshots_dir=screenshots_dir,
                    clips_dir=clips_dir,
                    video_path=str(args.video_path or ""),
                    step2_path=str(args.step2_path or ""),
                    step6_path=str(args.step6_path or ""),
                    output_dir=output_dir,
                    structure_mode=str(args.structure_mode),
                    structure_workers=int(structure_workers),
                    ocr_workers=int(ocr_workers),
                    sample_interval_sec=float(args.sample_interval_sec),
                    max_units=int(args.max_units),
                    safe_no_delete=bool(args.safe_no_delete),
                    seed_assets_dir=str(args.seed_assets_dir or ""),
                )
            for run_index in range(1, int(args.rounds) + 1):
                row = _run_once(
                    case_id=case_id,
                    run_index=run_index,
                    semantic_units_json=semantic_units_json,
                    screenshots_dir=screenshots_dir,
                    clips_dir=clips_dir,
                    video_path=str(args.video_path or ""),
                    step2_path=str(args.step2_path or ""),
                    step6_path=str(args.step6_path or ""),
                    output_dir=output_dir,
                    structure_mode=str(args.structure_mode),
                    structure_workers=int(structure_workers),
                    ocr_workers=int(ocr_workers),
                    sample_interval_sec=float(args.sample_interval_sec),
                    max_units=int(args.max_units),
                    safe_no_delete=bool(args.safe_no_delete),
                    seed_assets_dir=str(args.seed_assets_dir or ""),
                )
                run_rows.append(row)
                print(
                    f"  run={run_index} success={row['success']} elapsed_ms={row['elapsed_ms']:.1f} "
                    f"units={row['units_processed']} throughput={row['throughput_units_per_sec']:.4f}"
                )

    summary_rows = _summarize(run_rows)
    recommendation = _recommend(summary_rows)

    write_json(raw_dir / "run_rows.json", run_rows)
    write_csv(raw_dir / "run_rows.csv", run_rows)
    write_json(raw_dir / "summary_rows.json", summary_rows)
    write_csv(raw_dir / "summary_rows.csv", summary_rows)
    write_json(raw_dir / "recommendation.json", recommendation)

    report_text = _build_report(
        args=args,
        output_dir=output_dir,
        summary_rows=summary_rows,
        recommendation=recommendation,
    )
    report_path = output_dir / "README.md"
    report_path.write_text(report_text, encoding="utf-8")

    print("\n=== Phase2B 并发压测完成 ===")
    print(f"output_dir={output_dir}")
    print(
        "apply_cmd="
        "python -X utf8 scripts/apply_phase2b_concurrency_recommendation.py "
        f"--bench-output-dir \"{output_dir}\""
    )
    if recommendation:
        print(
            f"recommended: PHASE2B_STRUCTURE_PREPROCESS_WORKERS={recommendation['structure_workers']} "
            f"PHASE2B_OCR_VALIDATE_WORKERS={recommendation['ocr_workers']}"
        )


if __name__ == "__main__":
    main()
