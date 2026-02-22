from __future__ import annotations

import argparse
import asyncio
import copy
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.python_grpc.src.common.utils.opencv_decode import open_video_capture_with_fallback
from services.python_grpc.src.content_pipeline.infra.runtime.config_loader import load_module2_config
from services.python_grpc.src.content_pipeline.phase2a.materials.vl_material_generator import VLMaterialGenerator


def _infer_unit_id(video_path: Path) -> str:
    match = re.search(r"(SU\d+)", video_path.stem, flags=re.IGNORECASE)
    if match:
        return str(match.group(1)).upper()
    return "SU001"


def _read_video_duration_sec(video_path: Path) -> float:
    cap, _resolved_path, _used_fallback = open_video_capture_with_fallback(str(video_path))
    if cap is None or not cap.isOpened():
        raise RuntimeError(f"cannot_open_video: {video_path}")
    try:
        fps = float(cap.get(5) or 0.0)  # cv2.CAP_PROP_FPS
        frame_count = float(cap.get(7) or 0.0)  # cv2.CAP_PROP_FRAME_COUNT
        if fps <= 1e-6 or frame_count <= 0:
            return 0.0
        return max(0.0, frame_count / fps)
    finally:
        cap.release()


def _render_step_markdown(step: Dict[str, Any]) -> str:
    step_id = int(step.get("step_id", 0) or 0)
    step_desc = str(step.get("step_description", "") or "").strip() or f"step_{step_id:02d}"
    step_type = str(step.get("step_type", "MAIN_FLOW") or "MAIN_FLOW").strip().upper()
    main_operation = str(step.get("main_operation", "") or "").strip()
    clip_file = str(step.get("clip_file", "") or "").strip()

    lines: List[str] = []
    if step_type in {"CONDITIONAL", "OPTIONAL"}:
        lines.append(f"[!NOTE] 分支情况处理：{step_desc}")
        if main_operation:
            lines.append(main_operation)
    elif step_type == "TROUBLESHOOTING":
        lines.append(f"[!WARNING] 常见报错解决：{step_desc}")
        if main_operation:
            lines.append(main_operation)
    else:
        lines.append(f"#### {step_id}.{step_desc}")
        if main_operation:
            lines.append(main_operation)

    for keyframe in step.get("instructional_keyframe_details", []) or []:
        image_file = str(keyframe.get("image_file", "") or "").strip()
        if image_file:
            lines.append(f"![{step_id}_{image_file}]({image_file})")

    if clip_file:
        lines.append("")
        lines.append(f"<video src=\"{clip_file}\" controls></video>")

    lines.append("")
    return "\n".join(lines)


def _write_preview_markdown(steps_json_path: Path) -> Path:
    payload = json.loads(steps_json_path.read_text(encoding="utf-8"))
    steps = payload.get("steps", []) if isinstance(payload, dict) else []
    lines: List[str] = ["# Tutorial Preview", ""]
    for step in steps:
        if isinstance(step, dict):
            lines.append(_render_step_markdown(step))

    md_path = steps_json_path.with_name(f"{steps_json_path.stem}_preview.md")
    md_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
    return md_path


def _build_step_interaction_payload(
    *,
    steps_json_path: Path,
    cache_json_path: Path,
) -> Path:
    steps_payload = json.loads(steps_json_path.read_text(encoding="utf-8"))
    cache_payload = json.loads(cache_json_path.read_text(encoding="utf-8"))

    unit_level_vl_interactions: List[Dict[str, Any]] = []
    if isinstance(cache_payload.get("raw_llm_interactions"), list):
        unit_level_vl_interactions.extend(
            [x for x in cache_payload.get("raw_llm_interactions", []) if isinstance(x, dict)]
        )
    for item in cache_payload.get("analysis_results", []) or []:
        if not isinstance(item, dict):
            continue
        raw_interactions = item.get("raw_llm_interactions", []) or []
        if isinstance(raw_interactions, list):
            unit_level_vl_interactions.extend([x for x in raw_interactions if isinstance(x, dict)])

    step_interactions: List[Dict[str, Any]] = []
    for step in steps_payload.get("steps", []) or []:
        if not isinstance(step, dict):
            continue
        grid_calls: List[Dict[str, Any]] = []
        for key_detail in step.get("instructional_keyframe_details", []) or []:
            if not isinstance(key_detail, dict):
                continue
            interaction = key_detail.get("grid_anchor_llm_interaction")
            if isinstance(interaction, dict):
                grid_calls.append(interaction)
        step_interactions.append(
            {
                "step_id": int(step.get("step_id", 0) or 0),
                "step_description": str(step.get("step_description", "") or ""),
                "step_type": str(step.get("step_type", "MAIN_FLOW") or "MAIN_FLOW"),
                "grid_anchor_interactions": grid_calls,
            }
        )

    output_payload = {
        "schema": "tutorial_stepwise_llm_interactions_v1",
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "unit_level_vl_interactions": unit_level_vl_interactions,
        "steps": step_interactions,
    }
    output_path = steps_json_path.with_name(f"{steps_json_path.stem}_llm_interactions_raw.json")
    output_path.write_text(json.dumps(output_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return output_path


async def run(video_path: Path, output_dir: Path) -> Dict[str, Any]:
    config = load_module2_config()
    vl_config = copy.deepcopy(config.get("vl_material_generation", {}))
    vl_config["enabled"] = True
    vl_config["use_cache"] = False
    vl_config["save_cache"] = True

    tutorial_cfg = vl_config.get("tutorial_mode") if isinstance(vl_config.get("tutorial_mode"), dict) else {}
    tutorial_cfg["enabled"] = True
    tutorial_cfg["export_assets"] = True
    tutorial_cfg["save_step_json"] = True
    vl_config["tutorial_mode"] = tutorial_cfg

    output_dir.mkdir(parents=True, exist_ok=True)
    unit_id = _infer_unit_id(video_path)

    generator = VLMaterialGenerator(vl_config)
    analysis_video_path = Path(
        generator._resolve_vl_analysis_clip_path(
            original_clip_path=str(video_path),
            preferred_clip_path="",
        )
    )
    if not analysis_video_path.exists():
        raise FileNotFoundError(f"analysis_video_not_found: {analysis_video_path}")

    _ = _read_video_duration_sec(analysis_video_path)

    analysis_result = await generator.analyzer.analyze_clip(
        clip_path=str(analysis_video_path),
        semantic_unit_start_sec=0.0,
        semantic_unit_id=unit_id,
        extra_prompt=generator._build_tutorial_extra_prompt(),
        analysis_mode="tutorial_stepwise",
    )

    if not analysis_result.success:
        raise RuntimeError(f"vl_analyze_failed: {analysis_result.error_msg}")

    await generator._save_tutorial_assets_for_unit(
        video_path=str(analysis_video_path),
        output_dir=str(output_dir),
        unit_id=unit_id,
        clip_requests=analysis_result.clip_requests,
        screenshot_requests=analysis_result.screenshot_requests,
        raw_response_json=analysis_result.raw_response_json,
        raw_llm_interactions=analysis_result.raw_llm_interactions,
        use_analysis_relative_timestamps=True,
        prefer_screenshot_requests_keyframes=False,
    )

    cache_json_path = output_dir / "vl_analysis_cache.json"
    steps_json_path = output_dir / "vl_tutorial_units" / unit_id / f"{unit_id}_steps.json"
    if not steps_json_path.exists():
        raise FileNotFoundError(f"missing_steps_json: {steps_json_path}")

    cache_payload = {
        "version": "1.0",
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "unit_id": unit_id,
        "analysis_video_path": str(analysis_video_path),
        "raw_response_json": analysis_result.raw_response_json or [],
        "raw_llm_interactions": analysis_result.raw_llm_interactions or [],
        "clip_requests": analysis_result.clip_requests or [],
        "screenshot_requests": analysis_result.screenshot_requests or [],
    }
    cache_json_path.write_text(json.dumps(cache_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    preview_md_path = _write_preview_markdown(steps_json_path)
    step_interactions_path = _build_step_interaction_payload(
        steps_json_path=steps_json_path,
        cache_json_path=cache_json_path,
    )

    raw_json_array: List[Dict[str, Any]] = list(analysis_result.raw_response_json or [])

    return {
        "output_dir": str(output_dir),
        "analysis_video_path": str(analysis_video_path),
        "cache_json": str(cache_json_path),
        "steps_json": str(steps_json_path),
        "interactions_json": str(step_interactions_path),
        "preview_md": str(preview_md_path),
        "clip_requests": analysis_result.clip_requests,
        "screenshot_requests": analysis_result.screenshot_requests,
        "raw_json_array": raw_json_array,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run tutorial VL test and persist raw LLM interactions.")
    parser.add_argument("--video", required=True, help="Path to target semantic unit clip video.")
    parser.add_argument("--output-dir", default="", help="Output directory for this run.")
    args = parser.parse_args()

    video_path = Path(args.video).resolve()
    if not video_path.exists():
        raise FileNotFoundError(f"video_not_found: {video_path}")

    if args.output_dir:
        output_dir = Path(args.output_dir).resolve()
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        root = video_path.parent.parent if video_path.parent.name.lower() == "semantic_unit_clips_vl" else video_path.parent
        output_dir = root / f"vl_raw_trace_{timestamp}"

    payload = asyncio.run(run(video_path, output_dir))
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
