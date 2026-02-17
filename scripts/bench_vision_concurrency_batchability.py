from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import statistics
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import matplotlib
import yaml

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
from services.python_grpc.src.config_paths import load_yaml_dict, resolve_video_config_path
from services.python_grpc.src.content_pipeline.infra.llm.vision_ai_client import VisionAIClient, VisionAIConfig
from services.python_grpc.src.content_pipeline.phase2a.segmentation.concrete_knowledge_validator import (
    ConcreteKnowledgeResult,
    ConcreteKnowledgeValidator,
)

matplotlib.use("Agg")
from matplotlib import pyplot as plt  # noqa: E402


def _chunk(items: List[Any], size: int) -> List[List[Any]]:
    size = max(1, int(size))
    return [items[i : i + size] for i in range(0, len(items), size)]


def _parse_manifest(path: Path) -> List[Dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        payload = payload.get("images", payload.get("items", []))
    if not isinstance(payload, list):
        raise ValueError("manifest must be a list or dict(images/items)")
    rows: List[Dict[str, Any]] = []
    for idx, item in enumerate(payload, start=1):
        if isinstance(item, dict):
            p = str(item.get("image_path", item.get("path", "")) or "").strip()
            sample_id = str(item.get("sample_id", f"IMG_{idx:03d}") or f"IMG_{idx:03d}")
            ocr_text = str(item.get("ocr_text", "") or "")
        else:
            p = str(item or "").strip()
            sample_id = f"IMG_{idx:03d}"
            ocr_text = ""
        if not p:
            continue
        fp = Path(p)
        if not fp.is_absolute():
            fp = (Path.cwd() / fp).resolve()
        if not fp.exists():
            continue
        rows.append({"sample_id": sample_id, "image_path": str(fp), "ocr_text": ocr_text})
    if not rows:
        raise ValueError("manifest has no existing image files")
    return rows


def _expand(rows: List[Dict[str, Any]], count: int) -> List[Dict[str, Any]]:
    count = max(1, int(count))
    out: List[Dict[str, Any]] = []
    while len(out) < count:
        for row in rows:
            out.append(dict(row))
            if len(out) >= count:
                break
    return out


def _read_video_config(config_path: str) -> Tuple[Path, Dict[str, Any]]:
    resolved = (
        resolve_video_config_path(config_path, anchor_file=__file__)
        if config_path
        else resolve_video_config_path(anchor_file=__file__)
    )
    if not resolved or not Path(resolved).exists():
        raise FileNotFoundError(f"config not found: {resolved}")
    cfg = load_yaml_dict(Path(resolved))
    return Path(resolved), cfg


def _build_vision_config(base_cfg: Dict[str, Any], *, batch_enabled: bool, batch_size: int, batch_inflight: int) -> VisionAIConfig:
    vision = dict(base_cfg.get("vision_ai", {}) or {})
    if not bool(vision.get("enabled", False)):
        raise RuntimeError("vision_ai.enabled=false")
    token = str(vision.get("bearer_token", "") or "").strip()
    if not token:
        raise RuntimeError("vision_ai.bearer_token is empty")
    return VisionAIConfig(
        enabled=True,
        bearer_token=token,
        base_url=str(vision.get("base_url", "https://qianfan.baidubce.com/v2/chat/completions")),
        model=str(vision.get("model", vision.get("vision_model", "ernie-4.5-turbo-vl-32k"))),
        temperature=float(vision.get("temperature", 0.1)),
        timeout=float(vision.get("timeout", 60.0)),
        rate_limit_per_minute=int(vision.get("rate_limit_per_minute", 60)),
        duplicate_detection_enabled=bool(vision.get("duplicate_detection", True)),
        similarity_threshold=float(vision.get("similarity_threshold", 0.95)),
        batch_enabled=batch_enabled,
        batch_max_size=max(1, int(batch_size)),
        batch_flush_ms=20,
        batch_max_inflight_batches=max(1, int(batch_inflight)),
    )


def _stage_inputs(rows: List[Dict[str, Any]], run_dir: Path) -> List[Dict[str, Any]]:
    run_dir.mkdir(parents=True, exist_ok=True)
    staged: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows, start=1):
        src = Path(row["image_path"])
        dst = run_dir / f"{idx:04d}_{src.name}"
        shutil.copy2(src, dst)
        staged.append({"sample_id": row["sample_id"], "image_path": str(dst), "ocr_text": row.get("ocr_text", "")})
    return staged


def _decision_from_dict(payload: Dict[str, Any]) -> bool:
    if "should_include" in payload:
        return bool(payload.get("should_include"))
    raw = payload.get("has_concrete_knowledge", False)
    if isinstance(raw, bool):
        return raw
    return str(raw).strip().lower() in {"是", "true", "1", "yes", "y"}


async def _run_client_single(staged: List[Dict[str, Any]], cfg: VisionAIConfig, concurrency: int, skip_duplicate_check: bool) -> List[Dict[str, Any]]:
    sem = asyncio.Semaphore(max(1, concurrency))
    client = VisionAIClient(cfg)

    async def _one(item: Dict[str, Any]) -> Dict[str, Any]:
        t0 = datetime.now().timestamp()
        success = True
        payload: Dict[str, Any] = {}
        try:
            async with sem:
                payload = await client.validate_image(
                    image_path=item["image_path"],
                    skip_duplicate_check=skip_duplicate_check,
                )
            if isinstance(payload, dict) and payload.get("error"):
                success = False
        except Exception as exc:
            payload = {"error": str(exc), "should_include": True}
            success = False
        elapsed_ms = (datetime.now().timestamp() - t0) * 1000.0
        return {
            "sample_id": item["sample_id"],
            "elapsed_ms": elapsed_ms,
            "success": success,
            "error": str(payload.get("error", "")),
            "decision_should_include": _decision_from_dict(payload),
            "payload": payload,
        }

    try:
        return await asyncio.gather(*[_one(item) for item in staged])
    finally:
        await client.close()


async def _run_client_batch(staged: List[Dict[str, Any]], cfg: VisionAIConfig, concurrency: int, batch_size: int, skip_duplicate_check: bool) -> List[Dict[str, Any]]:
    sem = asyncio.Semaphore(max(1, concurrency))
    client = VisionAIClient(cfg)
    groups = _chunk(staged, batch_size)

    async def _one_group(group: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        t0 = datetime.now().timestamp()
        try:
            async with sem:
                payloads = await client.validate_images_batch(
                    image_paths=[g["image_path"] for g in group],
                    skip_duplicate_check=skip_duplicate_check,
                    max_batch_size=batch_size,
                )
        except Exception as exc:
            payloads = [{"error": str(exc), "should_include": True} for _ in group]
        elapsed_ms = (datetime.now().timestamp() - t0) * 1000.0 / max(1, len(group))
        rows: List[Dict[str, Any]] = []
        for item, payload in zip(group, payloads):
            p = payload if isinstance(payload, dict) else {"raw_response": str(payload)}
            rows.append(
                {
                    "sample_id": item["sample_id"],
                    "elapsed_ms": elapsed_ms,
                    "success": not bool(p.get("error")),
                    "error": str(p.get("error", "")),
                    "decision_should_include": _decision_from_dict(p),
                    "payload": p,
                }
            )
        return rows

    try:
        rows = await asyncio.gather(*[_one_group(g) for g in groups])
        flat: List[Dict[str, Any]] = []
        for group in rows:
            flat.extend(group)
        return flat
    finally:
        await client.close()


def _run_validator(staged: List[Dict[str, Any]], base_cfg: Dict[str, Any], run_dir: Path, concurrency: int, batch_size: int) -> List[Dict[str, Any]]:
    run_dir.mkdir(parents=True, exist_ok=True)
    cfg_file = run_dir / "validator_batch_config.yaml"
    cfg_data = dict(base_cfg)
    vision = dict(cfg_data.get("vision_ai", {}) or {})
    batch_cfg = dict(vision.get("batch", {}) or {})
    batch_cfg.update({"enabled": True, "max_size": int(batch_size), "max_inflight_batches": int(max(1, concurrency)), "flush_ms": 20})
    vision["batch"] = batch_cfg
    cfg_data["vision_ai"] = vision
    cfg_file.write_text(yaml.safe_dump(cfg_data, allow_unicode=True, sort_keys=False), encoding="utf-8")

    validator = ConcreteKnowledgeValidator(config_path=str(cfg_file), output_dir=str(run_dir))
    rows: List[Optional[Dict[str, Any]]] = [None] * len(staged)
    groups = _chunk(list(enumerate(staged)), batch_size)
    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as executor:
        futures: Dict[Any, Tuple[List[int], float]] = {}
        for group in groups:
            idxs = [idx for idx, _ in group]
            tasks = [{"image_path": it["image_path"], "ocr_text": it.get("ocr_text", "")} for _, it in group]
            start_ts = datetime.now().timestamp()
            f = executor.submit(validator.validate_batch, tasks)
            futures[f] = (idxs, start_ts)
        for f in as_completed(futures):
            idxs, start_ts = futures[f]
            try:
                results = f.result()
            except Exception:
                results = [validator._default_result(True) for _ in idxs]
            each_ms = (datetime.now().timestamp() - start_ts) * 1000.0 / max(1, len(idxs))
            for local_idx, row_idx in enumerate(idxs):
                r = results[local_idx] if local_idx < len(results) and isinstance(results[local_idx], ConcreteKnowledgeResult) else validator._default_result(True)
                rows[row_idx] = {
                    "sample_id": staged[row_idx]["sample_id"],
                    "elapsed_ms": each_ms,
                    "success": True,
                    "error": "",
                    "decision_should_include": bool(r.should_include),
                    "payload": {
                        "has_concrete": bool(r.has_concrete),
                        "has_formula": bool(r.has_formula),
                        "confidence": float(r.confidence),
                        "should_include": bool(r.should_include),
                    },
                }
    return [row for row in rows if row is not None]


def _plot(summary_rows: List[Dict[str, Any]], output_png: Path) -> None:
    if not summary_rows:
        return
    rows = sorted(summary_rows, key=lambda x: float(x["throughput_items_per_sec_mean"]), reverse=True)[:12]
    labels = [f"{r['mode']}\nc{int(r['concurrency'])}\nb{int(r['batch_size'])}" for r in rows]
    tps = [float(r["throughput_items_per_sec_mean"]) for r in rows]
    p95 = [float(r["item_latency_p95_ms_mean"]) for r in rows]
    match = [float(r["decision_match_rate_percent_mean"]) for r in rows]
    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    axes[0].bar(labels, tps, color="#1f77b4"); axes[0].set_title("Throughput")
    axes[1].bar(labels, p95, color="#ff7f0e"); axes[1].set_title("P95 Item Latency")
    axes[2].bar(labels, match, color="#2ca02c"); axes[2].set_title("Quality Match")
    for ax in axes:
        ax.tick_params(axis="x", rotation=20)
        ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(output_png, dpi=160)
    plt.close(fig)


async def _amain(args: argparse.Namespace) -> int:
    manifest_path = Path(args.image_manifest).resolve()
    source_rows = _parse_manifest(manifest_path)
    samples = _expand(source_rows, args.target_image_count)
    config_path, base_cfg = _read_video_config(args.config_path)
    modes = [m.strip() for m in args.modes.split(",") if m.strip()]
    conc_ladder = parse_int_list(args.concurrency, "concurrency")
    batch_ladder = parse_int_list(args.batch_sizes, "batch_sizes")

    output_dir, raw_dir, charts_dir = ensure_benchmark_dirs(args.output_root, args.task_name)
    write_json(
        output_dir / "metadata.json",
        {
            "generated_at": now_iso(),
            "image_manifest_path": str(manifest_path),
            "image_count": len(samples),
            "modes": modes,
            "concurrency_ladder": conc_ladder,
            "batch_size_ladder": batch_ladder,
            "repeats": args.repeats,
            "config_path": str(config_path),
            "output_dir": str(output_dir.resolve()),
        },
    )

    ref_staged = _stage_inputs(samples, raw_dir / "reference_inputs")
    ref_cfg = _build_vision_config(base_cfg, batch_enabled=False, batch_size=1, batch_inflight=1)
    ref_rows = await _run_client_single(ref_staged, ref_cfg, 1, skip_duplicate_check=bool(args.skip_duplicate_check))
    ref_map = {str(r["sample_id"]): bool(r["decision_should_include"]) for r in ref_rows}
    write_json(raw_dir / "reference_decisions.json", ref_map)

    run_rows: List[Dict[str, Any]] = []
    for mode in modes:
        for concurrency in conc_ladder:
            case_batches = [1] if mode == "client_single" else list(batch_ladder)
            for batch_size in case_batches:
                case_id = f"mode={mode}|c={concurrency}|b={batch_size}"
                for run_idx in range(1, args.repeats + 1):
                    print(f"[Run] {case_id} r{run_idx}/{args.repeats}")
                    staged = _stage_inputs(samples, raw_dir / "run_inputs" / f"{case_id.replace('|', '_')}_r{run_idx}")
                    sampler = SystemSampler(args.sample_interval_sec)
                    sampler.start()
                    t0 = datetime.now().timestamp()
                    if mode == "client_single":
                        cfg = _build_vision_config(base_cfg, batch_enabled=False, batch_size=1, batch_inflight=1)
                        item_rows = await _run_client_single(staged, cfg, concurrency, bool(args.skip_duplicate_check))
                    elif mode == "client_batch":
                        cfg = _build_vision_config(base_cfg, batch_enabled=True, batch_size=batch_size, batch_inflight=concurrency)
                        item_rows = await _run_client_batch(staged, cfg, concurrency, batch_size, bool(args.skip_duplicate_check))
                    elif mode == "validator":
                        item_rows = await asyncio.to_thread(_run_validator, staged, base_cfg, raw_dir / "run_cache" / f"{uuid.uuid4().hex[:8]}", concurrency, batch_size)
                    else:
                        raise ValueError(f"unknown mode: {mode}")
                    elapsed_ms = (datetime.now().timestamp() - t0) * 1000.0
                    sys_samples = sampler.stop()
                    latencies = [float(r["elapsed_ms"]) for r in item_rows]
                    success_count = sum(1 for r in item_rows if bool(r.get("success")))
                    comparable = sum(1 for r in item_rows if str(r["sample_id"]) in ref_map)
                    matches = sum(1 for r in item_rows if str(r["sample_id"]) in ref_map and bool(r["decision_should_include"]) == bool(ref_map[str(r["sample_id"])]))
                    tag = f"{case_id.replace('|', '__').replace('=', '_')}_r{run_idx}_{uuid.uuid4().hex[:6]}"
                    item_file = raw_dir / f"item_results_{tag}.json"
                    sample_file = raw_dir / f"system_samples_{tag}.json"
                    write_json(item_file, item_rows)
                    write_json(sample_file, sys_samples)
                    row = {
                        "case_id": case_id,
                        "mode": mode,
                        "concurrency": concurrency,
                        "batch_size": batch_size,
                        "run_index": run_idx,
                        "elapsed_ms": elapsed_ms,
                        "throughput_items_per_sec": (len(item_rows) / (elapsed_ms / 1000.0)) if elapsed_ms > 0 else 0.0,
                        "item_latency_p95_ms": percentile(latencies, 95),
                        "success_rate_percent": (success_count / len(item_rows) * 100.0) if item_rows else 0.0,
                        "decision_match_rate_percent": (matches / comparable * 100.0) if comparable else 0.0,
                        "item_results_file": str(item_file.relative_to(output_dir)),
                        "system_samples_file": str(sample_file.relative_to(output_dir)),
                    }
                    row.update(summarize_system_samples(sys_samples))
                    run_rows.append(row)

    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in run_rows:
        grouped[row["case_id"]].append(row)
    summary_rows: List[Dict[str, Any]] = []
    for case_id, rows in grouped.items():
        sample = rows[0]
        summary_rows.append(
            {
                "case_id": case_id,
                "mode": sample["mode"],
                "concurrency": sample["concurrency"],
                "batch_size": sample["batch_size"],
                "runs": len(rows),
                "success_rate_percent_mean": statistics.fmean(float(r["success_rate_percent"]) for r in rows),
                "throughput_items_per_sec_mean": statistics.fmean(float(r["throughput_items_per_sec"]) for r in rows),
                "item_latency_p95_ms_mean": statistics.fmean(float(r["item_latency_p95_ms"]) for r in rows),
                "decision_match_rate_percent_mean": statistics.fmean(float(r["decision_match_rate_percent"]) for r in rows),
            }
        )

    def _best(mode: str) -> Optional[Dict[str, Any]]:
        rows = [r for r in summary_rows if r["mode"] == mode]
        if not rows:
            return None
        rows = sorted(rows, key=lambda r: (-float(r["throughput_items_per_sec_mean"]), float(r["item_latency_p95_ms_mean"])))
        return rows[0]

    best_single = _best("client_single")
    best_batch = _best("client_batch")
    best_validator = _best("validator")
    throughput_gain = 0.0
    quality_delta = 0.0
    gate = False
    if best_single and best_batch:
        base = max(1e-9, float(best_single["throughput_items_per_sec_mean"]))
        throughput_gain = (float(best_batch["throughput_items_per_sec_mean"]) - base) / base * 100.0
        quality_delta = float(best_batch["decision_match_rate_percent_mean"]) - float(best_single["decision_match_rate_percent_mean"])
        gate = throughput_gain >= 20.0 and quality_delta >= -1.0
    recommendation = {
        "best_client_single_case": best_single["case_id"] if best_single else "",
        "best_client_batch_case": best_batch["case_id"] if best_batch else "",
        "best_validator_case": best_validator["case_id"] if best_validator else "",
        "batchability_gate": {
            "throughput_gain_percent": throughput_gain,
            "quality_delta_percent": quality_delta,
            "go_for_batch_implementation": gate,
            "rule": "throughput gain >= 20% and quality delta >= -1%",
        },
    }

    write_json(raw_dir / "runs_raw.json", run_rows)
    write_json(raw_dir / "summary_by_case.json", summary_rows)
    write_json(raw_dir / "recommendation.json", recommendation)
    write_csv(raw_dir / "runs_raw.csv", run_rows)
    write_csv(raw_dir / "summary_by_case.csv", summary_rows)
    _plot(summary_rows, charts_dir / "concurrency_summary.png")
    report = [
        "# Vision AI 并发与批量可行性压测报告",
        "",
        f"- 生成时间: {now_iso()}",
        f"- 清单: `{manifest_path}`",
        f"- 样本数: `{len(samples)}`",
        f"- 模式: `{modes}`",
        f"- 并发梯度: `{conc_ladder}`",
        f"- 批大小梯度: `{batch_ladder}`",
        f"- 每点重复: `{args.repeats}`",
        f"- 原始数据目录: `{raw_dir}`",
        f"- 图表: `{charts_dir / 'concurrency_summary.png'}`",
        "",
        f"- best_client_single_case: `{recommendation['best_client_single_case']}`",
        f"- best_client_batch_case: `{recommendation['best_client_batch_case']}`",
        f"- best_validator_case: `{recommendation['best_validator_case']}`",
        f"- throughput_gain_percent: `{throughput_gain:.2f}`",
        f"- quality_delta_percent: `{quality_delta:.2f}`",
        f"- go_for_batch_implementation: `{gate}`",
    ]
    (output_dir / "report.md").write_text("\n".join(report) + "\n", encoding="utf-8")

    print(f"\nDone: {output_dir.resolve()}")
    print(f"Recommendation: {recommendation}")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Vision AI concurrency/batch benchmark")
    parser.add_argument("--image-manifest", default="var/artifacts/benchmarks/sample_data/vision_ai_sample/image_manifest.json")
    parser.add_argument("--config-path", default="", help="optional config/video_config.yaml path")
    parser.add_argument("--target-image-count", type=int, default=24)
    parser.add_argument("--modes", default="client_single,client_batch,validator")
    parser.add_argument("--concurrency", default="1,2,4,6,8")
    parser.add_argument("--batch-sizes", default="1,2,4,6,8")
    parser.add_argument("--repeats", type=int, default=2)
    parser.add_argument("--skip-duplicate-check", action="store_true")
    parser.add_argument("--sample-interval-sec", type=float, default=0.5)
    parser.add_argument("--output-root", default="var/artifacts/benchmarks")
    parser.add_argument("--task-name", default="vision_concurrency_batchability")
    args = parser.parse_args()
    if args.repeats <= 0:
        raise ValueError("repeats must be > 0")
    asyncio.run(_amain(args))


if __name__ == "__main__":
    main()
