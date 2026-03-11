from __future__ import annotations

import argparse
import asyncio
import copy
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


def _resolve_project_root() -> Path:
    candidates = [Path.cwd(), Path(__file__).resolve().parent.parent]
    for candidate in candidates:
        if (candidate / "services").exists():
            return candidate
    return Path(__file__).resolve().parent.parent


PROJECT_ROOT = _resolve_project_root()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.python_grpc.src.content_pipeline.infra.runtime.config_loader import load_module2_config
from services.python_grpc.src.content_pipeline.phase2a.materials.vl_material_generator import VLMaterialGenerator


_MODE_LABELS = {
    "offline": "offline_batch",
    "sync": "sync_call",
}


def _infer_unit_id(video_path: Path) -> str:
    stem = video_path.stem
    for token in stem.split("_"):
        token_upper = token.strip().upper()
        if token_upper.startswith("SU") and token_upper[2:].isdigit():
            return token_upper
    return "SU001"


def _normalize_analysis_mode(analysis_mode: str) -> str:
    mode = str(analysis_mode or "tutorial_stepwise").strip().lower()
    if mode in {"tutorial", "tutorial_stepwise", "teaching"}:
        return "tutorial_stepwise"
    if mode in {"concrete", "concrete_focus"}:
        return "concrete"
    return "default"


def _build_mode_config(base_vl_config: Dict[str, Any], *, offline_enabled: bool) -> Dict[str, Any]:
    vl_config = copy.deepcopy(base_vl_config)
    vl_config["enabled"] = True
    vl_config["use_cache"] = False
    vl_config["save_cache"] = False

    api_cfg = vl_config.get("api") if isinstance(vl_config.get("api"), dict) else {}
    api_cfg["offline_task_enabled"] = bool(offline_enabled)
    api_cfg["video_input_mode"] = "dashscope_upload"
    vl_config["api"] = api_cfg
    return vl_config


def _extract_transport_summary(raw_llm_interactions: List[Dict[str, Any]]) -> Dict[str, Any]:
    for item in reversed(raw_llm_interactions or []):
        if not isinstance(item, dict):
            continue
        request = item.get("request") if isinstance(item.get("request"), dict) else {}
        response = item.get("response") if isinstance(item.get("response"), dict) else {}
        return {
            "offline_task_enabled": request.get("offline_task_enabled"),
            "message_transport": request.get("message_transport"),
            "message_transport_meta": request.get("message_transport_meta"),
            "timeout_sec": request.get("timeout_sec"),
            "hedge_delay_ms": request.get("hedge_delay_ms"),
            "offline_task_meta": request.get("offline_task_meta") or response.get("offline_task_meta") or {},
            "cache_hit": response.get("cache_hit"),
            "finish_reason": response.get("finish_reason"),
        }
    return {}


def _build_comparison(runs: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_mode = {item["mode"]: item for item in runs}
    offline_run = by_mode.get("offline")
    sync_run = by_mode.get("sync")
    if not (offline_run and sync_run and offline_run.get("success") and sync_run.get("success")):
        return {}

    offline_elapsed = float(offline_run.get("elapsed_sec", 0.0) or 0.0)
    sync_elapsed = float(sync_run.get("elapsed_sec", 0.0) or 0.0)
    return {
        "offline_elapsed_sec": offline_elapsed,
        "sync_elapsed_sec": sync_elapsed,
        "delta_sec": round(sync_elapsed - offline_elapsed, 3),
        "speedup_pct": round(((offline_elapsed - sync_elapsed) / offline_elapsed) * 100.0, 2)
        if offline_elapsed > 0.0
        else None,
    }


async def _run_once(*, video_path: Path, analysis_mode: str, offline_enabled: bool) -> Dict[str, Any]:
    config = load_module2_config()
    base_vl_config = config.get("vl_material_generation", {}) if isinstance(config, dict) else {}
    vl_config = _build_mode_config(base_vl_config, offline_enabled=offline_enabled)
    generator = VLMaterialGenerator(vl_config)
    normalized_mode = _normalize_analysis_mode(analysis_mode)
    analysis_video_path = Path(
        generator._resolve_vl_analysis_clip_path(
            original_clip_path=str(video_path),
            preferred_clip_path="",
        )
    )
    extra_prompt = generator._build_tutorial_extra_prompt() if normalized_mode == "tutorial_stepwise" else None
    start_ts = time.perf_counter()
    try:
        result = await generator.analyzer.analyze_clip(
            clip_path=str(analysis_video_path),
            semantic_unit_start_sec=0.0,
            semantic_unit_id=_infer_unit_id(analysis_video_path),
            extra_prompt=extra_prompt,
            analysis_mode=normalized_mode,
        )
        elapsed_sec = time.perf_counter() - start_ts
        transport_summary = _extract_transport_summary(result.raw_llm_interactions)
        return {
            "mode": "offline" if offline_enabled else "sync",
            "mode_label": _MODE_LABELS["offline" if offline_enabled else "sync"],
            "analysis_mode": normalized_mode,
            "video_path": str(video_path),
            "analysis_video_path": str(analysis_video_path),
            "elapsed_sec": round(elapsed_sec, 3),
            "success": bool(result.success),
            "error_msg": str(result.error_msg or ""),
            "token_usage": dict(result.token_usage or {}),
            "clip_requests": len(result.clip_requests or []),
            "screenshot_requests": len(result.screenshot_requests or []),
            "raw_response_items": len(result.raw_response_json or []),
            "raw_llm_interactions": len(result.raw_llm_interactions or []),
            "transport_summary": transport_summary,
        }
    finally:
        await generator.analyzer.close()


def _write_outputs(output_dir: Path, payload: Dict[str, Any]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "report.json"
    summary_path = output_dir / "summary.md"
    report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    lines = [
        "# VL sync vs offline benchmark",
        "",
        f"- generated_at: {payload.get('generated_at', '')}",
        f"- video_path: {payload.get('video_path', '')}",
        f"- analysis_mode: {payload.get('analysis_mode', '')}",
        f"- mode_order: {', '.join(payload.get('mode_order', []) or [])}",
        f"- completed_modes: {', '.join(payload.get('completed_modes', []) or [])}",
        "",
        "| mode | success | elapsed_sec | transport | offline_task_enabled | poll_count | total_tokens | error |",
        "|---|---:|---:|---|---:|---:|---:|---|",
    ]
    for item in payload.get("runs", []) or []:
        transport_summary = item.get("transport_summary") if isinstance(item.get("transport_summary"), dict) else {}
        offline_task_meta = transport_summary.get("offline_task_meta") if isinstance(transport_summary.get("offline_task_meta"), dict) else {}
        token_usage = item.get("token_usage") if isinstance(item.get("token_usage"), dict) else {}
        lines.append(
            "| {mode} | {success} | {elapsed:.3f} | {transport} | {offline_flag} | {poll_count} | {tokens} | {error} |".format(
                mode=item.get("mode_label", item.get("mode", "")),
                success="yes" if item.get("success") else "no",
                elapsed=float(item.get("elapsed_sec", 0.0) or 0.0),
                transport=str(transport_summary.get("message_transport", "")),
                offline_flag=str(transport_summary.get("offline_task_enabled", "")),
                poll_count=str(offline_task_meta.get("poll_count", "")),
                tokens=str(token_usage.get("total_tokens", "")),
                error=str(item.get("error_msg", "")).replace("|", "/"),
            )
        )
    comparison = payload.get("comparison") if isinstance(payload.get("comparison"), dict) else {}
    if comparison:
        lines.extend(
            [
                "",
                "## Comparison",
                "",
                f"- offline_elapsed_sec: {comparison.get('offline_elapsed_sec')}",
                f"- sync_elapsed_sec: {comparison.get('sync_elapsed_sec')}",
                f"- delta_sec: {comparison.get('delta_sec')}",
                f"- speedup_pct: {comparison.get('speedup_pct')}",
            ]
        )
    summary_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


async def _run_benchmark(*, video_path: Path, analysis_mode: str, mode_order: List[str], output_dir: Path) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "video_path": str(video_path),
        "analysis_mode": _normalize_analysis_mode(analysis_mode),
        "mode_order": mode_order,
        "completed_modes": [],
        "runs": [],
        "comparison": {},
    }
    _write_outputs(output_dir, payload)

    for index, mode in enumerate(mode_order, start=1):
        print(f"[bench] start {index}/{len(mode_order)} mode={mode}", flush=True)
        offline_enabled = mode == "offline"
        result = await _run_once(
            video_path=video_path,
            analysis_mode=analysis_mode,
            offline_enabled=offline_enabled,
        )
        payload["runs"].append(result)
        payload["completed_modes"].append(mode)
        payload["comparison"] = _build_comparison(payload["runs"])
        _write_outputs(output_dir, payload)
        print(
            "[bench] done mode={mode} success={success} elapsed_sec={elapsed:.3f} transport={transport} poll_count={poll_count}".format(
                mode=mode,
                success=result.get("success"),
                elapsed=float(result.get("elapsed_sec", 0.0) or 0.0),
                transport=str((result.get("transport_summary") or {}).get("message_transport", "")),
                poll_count=str((((result.get("transport_summary") or {}).get("offline_task_meta") or {}).get("poll_count", ""))),
            ),
            flush=True,
        )
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare DashScope offline batch vs sync VL latency on the same clip.")
    parser.add_argument("--video", required=True, help="Path to the target semantic unit clip.")
    parser.add_argument("--analysis-mode", default="tutorial_stepwise", help="VL analysis mode.")
    parser.add_argument("--mode-order", default="offline,sync", help="Comma-separated mode order: offline and/or sync.")
    parser.add_argument("--output-dir", default="", help="Directory for benchmark outputs.")
    args = parser.parse_args()

    video_path = Path(args.video).resolve()
    if not video_path.exists():
        raise FileNotFoundError(f"video_not_found: {video_path}")

    mode_order = [item.strip().lower() for item in str(args.mode_order or "").split(",") if item.strip()]
    if not mode_order:
        raise ValueError("mode_order_empty")
    invalid_modes = [item for item in mode_order if item not in {"offline", "sync"}]
    if invalid_modes:
        raise ValueError(f"invalid_modes: {invalid_modes}")

    if args.output_dir:
        output_dir = Path(args.output_dir).resolve()
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = PROJECT_ROOT / "var" / "artifacts" / "benchmarks" / f"vl_sync_vs_offline_{timestamp}"

    payload = asyncio.run(
        _run_benchmark(
            video_path=video_path,
            analysis_mode=args.analysis_mode,
            mode_order=mode_order,
            output_dir=output_dir,
        )
    )
    print(json.dumps({"output_dir": str(output_dir), **payload}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()