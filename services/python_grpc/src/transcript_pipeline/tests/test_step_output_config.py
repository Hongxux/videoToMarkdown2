import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

from services.python_grpc.src.transcript_pipeline.graph import StepOutputConfig
from services.python_grpc.src.transcript_pipeline.tests.test_tmp_utils import make_repo_tmp_dir


def test_step_output_config_persists_full_step2_payload():
    tmp_path = make_repo_tmp_dir("test_step_output_config_persists_full_step2_payload")
    inter_dir = tmp_path / "intermediates"
    config = StepOutputConfig(output_dir=str(inter_dir), disable_all=True)

    corrected_subtitles = [
        {
            "subtitle_id": f"SUB{i:03d}",
            "corrected_text": f"text {i}",
            "start_sec": float(i),
            "end_sec": float(i) + 0.5,
            "corrections": [],
        }
        for i in range(1, 13)
    ]
    state = {
        "domain": "test",
        "subtitle_path": "subtitles.txt",
        "corrected_subtitles": corrected_subtitles,
        "correction_summary": [],
    }

    config.save_step_output("step2_correction", state)

    output_file = inter_dir / "step2_correction_output.json"
    payload = json.loads(output_file.read_text(encoding="utf-8"))
    output = payload["output"]["corrected_subtitles"]
    assert isinstance(output, list)
    assert len(output) == 12
    assert output[0]["subtitle_id"] == "SUB001"
    assert "corrections" not in output[0]
    assert "corrections" in corrected_subtitles[0]


def test_step_output_config_persists_full_step56_payload_with_step6_filename_alias():
    tmp_path = make_repo_tmp_dir("test_step_output_config_persists_full_step56_payload_with_step6_filename_alias")
    inter_dir = tmp_path / "intermediates"
    config = StepOutputConfig(output_dir=str(inter_dir), disable_all=True)

    pure_text_script = [
        {
            "paragraph_id": f"P{i:03d}",
            "text": f"paragraph {i}",
            "source_sentence_ids": [f"S{i:03d}"],
            "merge_type": "normal",
        }
        for i in range(1, 13)
    ]
    state = {
        "non_redundant_sentences": [],
        "pure_text_script": pure_text_script,
    }

    config.save_step_output("step5_6_dedup_merge", state)

    output_file = inter_dir / "step6_merge_cross_output.json"
    payload = json.loads(output_file.read_text(encoding="utf-8"))
    output = payload["output"]["pure_text_script"]
    assert isinstance(output, list)
    assert len(output) == 12
    assert output[-1]["paragraph_id"] == "P012"


def test_step_output_config_disable_all_keeps_required_steps_enabled():
    tmp_path = make_repo_tmp_dir("test_step_output_config_disable_all_keeps_required_steps_enabled")
    inter_dir = tmp_path / "intermediates"
    config = StepOutputConfig(output_dir=str(inter_dir), disable_all=True)

    for step_name in (
        "step2_correction",
        "step5_clean_cross",
        "step6_merge_cross",
        "step5_6_dedup_merge",
    ):
        assert config.should_output(step_name)

    assert not config.should_output("step1_validate")
    assert not config.should_output("step3_merge")
    assert not config.should_output("step3_5_translate")
    assert not config.should_output("step4_clean_local")


def test_step_output_config_persists_full_payload_for_step3_step4_step56_aliases():
    tmp_path = make_repo_tmp_dir("test_step_output_config_persists_full_payload_for_step3_step4_step56_aliases")
    inter_dir = tmp_path / "intermediates"
    config = StepOutputConfig(
        output_dir=str(inter_dir),
        enabled_steps=["step3_merge", "step4_clean_local", "step5_clean_cross"],
    )

    merged_sentences = [
        {
            "sentence_id": f"S{i:03d}",
            "text": f"sentence {i}",
            "start_sec": float(i),
            "end_sec": float(i) + 0.5,
            "source_subtitle_ids": [f"SUB{i:03d}"],
        }
        for i in range(1, 13)
    ]
    cleaned_sentences = [
        {
            "sentence_id": f"S{i:03d}",
            "cleaned_text": f"cleaned {i}",
        }
        for i in range(1, 13)
    ]
    non_redundant_sentences = [
        {
            "sentence_id": f"S{i:03d}",
            "cleaned_text": f"non-redundant {i}",
        }
        for i in range(1, 13)
    ]

    test_cases = [
        (
            "step3_merge",
            {
                "corrected_subtitles": [],
                "merged_sentences": merged_sentences,
            },
            "merged_sentences",
            "step3_merge_output.json",
            True,
        ),
        (
            "step4_clean_local",
            {
                "translated_sentences": merged_sentences,
                "merged_sentences": merged_sentences,
                "cleaned_sentences": cleaned_sentences,
            },
            "cleaned_sentences",
            "step4_clean_local_output.json",
            True,
        ),
        (
            "step5_clean_cross",
            {
                "cleaned_sentences": cleaned_sentences,
                "main_topic": "test",
                "non_redundant_sentences": non_redundant_sentences,
            },
            "non_redundant_sentences",
            "step6_merge_cross_output.json",
            True,
        ),
    ]

    for step_name, state, output_key, output_file_name, expect_full_list in test_cases:
        config.save_step_output(step_name, state)
        payload = json.loads((inter_dir / output_file_name).read_text(encoding="utf-8"))
        output_value = payload["output"][output_key]
        if expect_full_list:
            assert isinstance(output_value, list)
            assert len(output_value) == 12
        else:
            assert isinstance(output_value, dict)
            assert output_value["count"] == 12
            assert len(output_value["sample"]) == 2
