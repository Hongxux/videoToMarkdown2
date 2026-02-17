"""
Benchmark helper for VL pre-process parallel stage.

Usage example:
  python scripts/bench_pre_vl_preprocess_parallel.py \
    --video "D:/path/to/video.mp4" \
    --units "D:/path/to/semantic_units_phase2a.json" \
    --output "D:/path/to/output" \
    --workers 6 \
    --mode process

This script runs the same pre-process stage twice with different parallel modes,
and prints a compact comparison table to verify speedup.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from pathlib import Path
from typing import Any, Dict, List

from services.python_grpc.src.content_pipeline.infra.runtime.config_loader import load_module2_config
from services.python_grpc.src.content_pipeline.phase2a.materials.vl_material_generator import VLMaterialGenerator


def _load_units(units_path: Path) -> List[Dict[str, Any]]:
    payload = json.loads(units_path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        if isinstance(payload.get("semantic_units"), list):
            payload = payload["semantic_units"]
        elif isinstance(payload.get("units"), list):
            payload = payload["units"]
    if not isinstance(payload, list):
        raise ValueError("semantic units JSON must be a list or contain semantic_units/units list")
    return [item for item in payload if isinstance(item, dict)]


def _build_generator_config(*, mode: str, workers: Any, hard_cap: int) -> Dict[str, Any]:
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


async def _run_once(
    *,
    video_path: str,
    units: List[Dict[str, Any]],
    output_dir: str,
    mode: str,
    workers: Any,
    hard_cap: int,
) -> Dict[str, Any]:
    config = _build_generator_config(mode=mode, workers=workers, hard_cap=hard_cap)
    generator = VLMaterialGenerator(config)

    process_units = [
        unit
        for unit in units
        if str(unit.get("knowledge_type", "") or "").strip().lower() == "process"
    ]

    t0 = time.perf_counter()
    route_map = await generator.preprocess_process_units_for_routing(
        video_path=video_path,
        process_units=process_units,
        output_dir=output_dir,
        force_preprocess=True,
    )
    elapsed_ms = (time.perf_counter() - t0) * 1000.0

    applied = sum(1 for item in route_map.values() if bool((item or {}).get("preprocess_applied", False)))
    return {
        "mode": mode,
        "workers": workers,
        "process_units": len(process_units),
        "applied_units": applied,
        "elapsed_ms": elapsed_ms,
    }


def _print_summary(rows: List[Dict[str, Any]]) -> None:
    print("\n=== VL Pre-Process Parallel Benchmark ===")
    print(f"{'mode':<12} {'workers':<10} {'process_units':<14} {'applied':<10} {'elapsed_ms':<12}")
    for row in rows:
        print(
            f"{str(row['mode']):<12} {str(row['workers']):<10} {int(row['process_units']):<14} "
            f"{int(row['applied_units']):<10} {row['elapsed_ms']:<12.1f}"
        )

    if len(rows) >= 2 and rows[0]["elapsed_ms"] > 0:
        baseline = rows[0]["elapsed_ms"]
        target = rows[1]["elapsed_ms"]
        delta_pct = (baseline - target) / baseline * 100.0
        print(f"\nSpeedup vs first row: {delta_pct:.2f}%")


async def _amain(args: argparse.Namespace) -> None:
    units = _load_units(Path(args.units))

    runs: List[Dict[str, Any]] = []
    runs.append(
        await _run_once(
            video_path=args.video,
            units=units,
            output_dir=args.output,
            mode=args.baseline_mode,
            workers=args.baseline_workers,
            hard_cap=args.hard_cap,
        )
    )
    runs.append(
        await _run_once(
            video_path=args.video,
            units=units,
            output_dir=args.output,
            mode=args.mode,
            workers=args.workers,
            hard_cap=args.hard_cap,
        )
    )

    _print_summary(runs)


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark VL pre-process parallel stage")
    parser.add_argument("--video", required=True, help="Absolute path to source video")
    parser.add_argument("--units", required=True, help="Path to semantic units json")
    parser.add_argument("--output", required=True, help="Output directory")

    parser.add_argument("--baseline-mode", default="async", choices=["auto", "process", "async", "off"])
    parser.add_argument("--baseline-workers", default="1", help="Baseline workers, e.g. 1 or auto")
    parser.add_argument("--mode", default="process", choices=["auto", "process", "async", "off"])
    parser.add_argument("--workers", default="auto", help="Target workers, e.g. 6 or auto")
    parser.add_argument("--hard-cap", type=int, default=8, help="Parallel hard cap")

    args = parser.parse_args()
    asyncio.run(_amain(args))


if __name__ == "__main__":
    main()

