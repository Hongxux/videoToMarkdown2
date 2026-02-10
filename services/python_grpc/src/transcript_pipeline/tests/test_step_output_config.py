import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

from services.python_grpc.src.transcript_pipeline.graph import StepOutputConfig


def test_step_output_config_persists_full_step2_payload(tmp_path):
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


def test_step_output_config_persists_full_step6_payload(tmp_path):
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

    config.save_step_output("step6_merge_cross", state)

    output_file = inter_dir / "step6_merge_cross_output.json"
    payload = json.loads(output_file.read_text(encoding="utf-8"))
    output = payload["output"]["pure_text_script"]
    assert isinstance(output, list)
    assert len(output) == 12
    assert output[-1]["paragraph_id"] == "P012"
