from __future__ import annotations

from pathlib import Path
from typing import List


_STAGE1_STEP_FILENAMES = {
    "step1_validate": "step1_validate.json",
    "step2_correction": "step2_correction.json",
    "step3_merge": "step3_merge.json",
    "step3_5_translate": "step3_5_translate.json",
    "step4_clean_local": "step4_clean_local.json",
    "step5_6_dedup_merge": "step5_6_dedup_merge.json",
}

_STAGE1_STEP_LEGACY_FILENAMES = {
    "step1_validate": ["step1_validate_output.json"],
    "step2_correction": ["step2_correction_output.json", "step2_output.json"],
    "step3_merge": ["step3_merge_output.json", "step3_output.json"],
    "step3_5_translate": ["step3_5_translate_output.json", "step3_5_output.json"],
    "step4_clean_local": ["step4_clean_local_output.json", "step4_output.json"],
    "step5_6_dedup_merge": ["step5_6_dedup_merge_output.json", "step6_merge_cross_output.json", "step6_output.json"],
}


def _base_dir(output_dir: str | Path) -> Path:
    resolved = Path(str(output_dir or "").strip()).resolve()
    # 兼容历史调用：部分旧代码把 task root/intermediates 误当成 task root 传入，这里统一回退到任务根目录，
    # 避免再次拼出 intermediates/intermediates/... 造成 Stage1 与下游读取路径不一致。
    if resolved.name.lower() == "intermediates":
        return resolved.parent
    return resolved


def intermediates_dir(output_dir: str | Path) -> Path:
    return _base_dir(output_dir) / "intermediates"


def stage_dir(output_dir: str | Path, stage: str) -> Path:
    return intermediates_dir(output_dir) / "stages" / str(stage or "").strip()


def stage_outputs_dir(output_dir: str | Path, stage: str) -> Path:
    return stage_dir(output_dir, stage) / "outputs"


def stage_audits_dir(output_dir: str | Path, stage: str) -> Path:
    return stage_dir(output_dir, stage) / "audits"


def stage_output_path(output_dir: str | Path, stage: str, filename: str) -> Path:
    return stage_outputs_dir(output_dir, stage) / str(filename or "").strip()


def stage_audit_path(output_dir: str | Path, stage: str, filename: str) -> Path:
    return stage_audits_dir(output_dir, stage) / str(filename or "").strip()


def stage1_step_output_path(output_dir: str | Path, step_name: str) -> Path:
    filename = _STAGE1_STEP_FILENAMES.get(str(step_name or "").strip(), f"{str(step_name or '').strip()}.json")
    return stage_output_path(output_dir, "stage1", filename)


def stage1_step_legacy_paths(output_dir: str | Path, step_name: str) -> List[Path]:
    names = _STAGE1_STEP_LEGACY_FILENAMES.get(str(step_name or "").strip(), [])
    return [intermediates_dir(output_dir) / name for name in names]


def stage1_step_candidates(output_dir: str | Path, step_name: str) -> List[str]:
    candidates = [str(stage1_step_output_path(output_dir, step_name))]
    candidates.extend(str(path) for path in stage1_step_legacy_paths(output_dir, step_name))
    return candidates


def stage1_sentence_timestamps_path(output_dir: str | Path) -> Path:
    return stage_output_path(output_dir, "stage1", "sentence_timestamps.json")


def stage1_sentence_timestamps_candidates(output_dir: str | Path) -> List[str]:
    return [
        str(stage1_sentence_timestamps_path(output_dir)),
        str(intermediates_dir(output_dir) / "sentence_timestamps.json"),
    ]


def phase2a_semantic_units_path(output_dir: str | Path) -> Path:
    return stage_output_path(output_dir, "phase2a", "semantic_units.json")


def phase2a_semantic_units_legacy_paths(output_dir: str | Path) -> List[Path]:
    base = _base_dir(output_dir)
    inter = intermediates_dir(output_dir)
    return [
        base / "semantic_units_phase2a.json",
        inter / "semantic_units_phase2a.json",
    ]


def phase2a_semantic_units_candidates(output_dir: str | Path) -> List[str]:
    candidates = [str(phase2a_semantic_units_path(output_dir))]
    candidates.extend(str(path) for path in phase2a_semantic_units_legacy_paths(output_dir))
    return candidates


def phase2a_vl_subset_path(output_dir: str | Path) -> Path:
    return stage_output_path(output_dir, "phase2a", "semantic_units_vl_subset.json")


def phase2a_vl_analysis_path(output_dir: str | Path) -> Path:
    return stage_output_path(output_dir, "phase2a", "vl_analysis.json")


def phase2a_vl_analysis_candidates(output_dir: str | Path) -> List[str]:
    base = _base_dir(output_dir)
    return [
        str(phase2a_vl_analysis_path(output_dir)),
        str(base / "vl_analysis_cache.json"),
    ]


def phase2a_token_cost_audit_path(output_dir: str | Path) -> Path:
    return stage_audit_path(output_dir, "phase2a", "token_cost_audit.json")


def phase2a_vl_report_path(output_dir: str | Path, filename: str) -> Path:
    return stage_audit_path(output_dir, "phase2a", filename)


def phase2a_vl_analysis_output_candidates(output_dir: str | Path) -> List[Path]:
    base = _base_dir(output_dir)
    candidates = [
        phase2a_vl_report_path(output_dir, "vl_analysis_output_latest.json"),
        intermediates_dir(output_dir) / "vl_analysis_output_latest.json",
        base / "immediates" / "vl_analysis_output_latest.json",
    ]
    latest_outputs = sorted(
        list(stage_audits_dir(output_dir, "phase2a").glob("vl_analysis_output_*.json"))
        + list(intermediates_dir(output_dir).glob("vl_analysis_output_*.json"))
        + list((base / "immediates").glob("vl_analysis_output_*.json")),
        key=lambda path_item: path_item.stat().st_mtime if path_item.exists() else 0,
        reverse=True,
    )
    if latest_outputs:
        candidates.append(latest_outputs[0])
    return candidates
