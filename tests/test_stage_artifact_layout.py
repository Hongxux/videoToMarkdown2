import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.python_grpc.src.common.utils.stage_artifact_paths import (
    phase2a_vl_analysis_path,
    stage1_sentence_timestamps_path,
    stage1_step_output_path,
)
from services.python_grpc.src.content_pipeline.phase2a.materials.vl_material_generator import VLMaterialGenerator
from services.python_grpc.src.content_pipeline.shared.subtitle.subtitle_repository import SubtitleRepository
from services.python_grpc.src.transcript_pipeline.graph import StepOutputConfig


def test_step_output_config_writes_canonical_and_legacy_stage1_outputs(tmp_path: Path):
    config = StepOutputConfig(output_dir=str(tmp_path), enabled_steps=["step2_correction"], async_write=False)
    config.save_step_output(
        "step2_correction",
        {
            "domain": "demo",
            "subtitle_path": "demo.srt",
            "corrected_subtitles": [{"subtitle_id": "S001", "corrected_text": "hello", "corrections": ["x"]}],
            "correction_summary": {"count": 1},
        },
    )

    canonical_path = stage1_step_output_path(tmp_path, "step2_correction")
    legacy_path = tmp_path / "intermediates" / "step2_correction_output.json"

    assert canonical_path.exists()
    assert legacy_path.exists()
    canonical_payload = json.loads(canonical_path.read_text(encoding="utf-8"))
    legacy_payload = json.loads(legacy_path.read_text(encoding="utf-8"))
    assert canonical_payload["step"] == "step2_correction"
    assert legacy_payload["step"] == "step2_correction"
    assert canonical_payload["output"]["corrected_subtitles"][0]["subtitle_id"] == "S001"


def test_step_output_config_intermediates_input_does_not_duplicate_intermediates_segment(tmp_path: Path):
    config = StepOutputConfig(
        output_dir=str(tmp_path / "intermediates"),
        enabled_steps=["step2_correction"],
        async_write=False,
    )
    config.save_step_output(
        "step2_correction",
        {
            "domain": "demo",
            "subtitle_path": "demo.srt",
            "corrected_subtitles": [{"subtitle_id": "S001", "corrected_text": "hello", "corrections": ["x"]}],
            "correction_summary": {"count": 1},
        },
    )

    legacy_path = tmp_path / "intermediates" / "step2_correction_output.json"
    duplicated_legacy_path = tmp_path / "intermediates" / "intermediates" / "step2_correction_output.json"
    canonical_path = tmp_path / "intermediates" / "stages" / "stage1" / "outputs" / "step2_correction.json"
    duplicated_canonical_path = (
        tmp_path / "intermediates" / "intermediates" / "stages" / "stage1" / "outputs" / "step2_correction.json"
    )

    assert legacy_path.exists()
    assert canonical_path.exists()
    assert not duplicated_legacy_path.exists()
    assert not duplicated_canonical_path.exists()


def test_subtitle_repository_prefers_canonical_stage1_outputs(tmp_path: Path):
    step2_path = stage1_step_output_path(tmp_path, "step2_correction")
    step6_path = stage1_step_output_path(tmp_path, "step5_6_dedup_merge")
    sentence_ts_path = stage1_sentence_timestamps_path(tmp_path)
    step2_path.parent.mkdir(parents=True, exist_ok=True)
    sentence_ts_path.parent.mkdir(parents=True, exist_ok=True)

    step2_path.write_text(json.dumps([{"subtitle_id": "S001", "corrected_text": "hello"}], ensure_ascii=False), encoding="utf-8")
    step6_path.write_text(json.dumps([{"paragraph_id": "P001", "text": "world"}], ensure_ascii=False), encoding="utf-8")
    sentence_ts_path.write_text(json.dumps({"S001": {"start_sec": 0.0, "end_sec": 1.0}}, ensure_ascii=False), encoding="utf-8")

    repository = SubtitleRepository.from_output_dir(output_dir=str(tmp_path))

    assert repository.step2_path == str(step2_path)
    assert repository.step6_path == str(step6_path)
    assert repository.sentence_timestamps_path == str(sentence_ts_path)


def test_vl_generator_prefers_canonical_phase2a_vl_analysis_cache(tmp_path: Path):
    generator = VLMaterialGenerator({"enabled": True, "screenshot_optimization": {}})
    canonical_cache_path = generator._get_cache_path(video_path=str(tmp_path / "video.mp4"), output_dir=str(tmp_path))
    canonical_cache_path.parent.mkdir(parents=True, exist_ok=True)
    canonical_cache_path.write_text(
        json.dumps({"version": "1.0", "analysis_results": [], "aggregated_screenshots": [], "aggregated_clips": []}, ensure_ascii=False),
        encoding="utf-8",
    )

    loaded = generator._load_vl_results(canonical_cache_path, output_dir=str(tmp_path))

    assert canonical_cache_path == phase2a_vl_analysis_path(tmp_path)
    assert loaded is not None
    assert loaded["version"] == "1.0"
