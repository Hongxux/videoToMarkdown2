import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

from services.python_grpc.src.server.vl_report_writer import VLReportWriter


def test_persist_token_report_writes_non_zero_dashscope_cost_and_video_breakdown(tmp_path: Path):
    writes = []

    def _capture(path, payload, **kwargs):
        writes.append((str(path), dict(payload), dict(kwargs)))

    writer = VLReportWriter(
        task_id="VT_CASE_1",
        video_path=str(tmp_path / "video.mp4"),
        semantic_units_path=str(tmp_path / "semantic_units_phase2a.json"),
        output_dir=str(tmp_path),
        logger=logging.getLogger("vl_report_writer_test"),
        enqueue_json_write_fn=_capture,
    )

    report_path = writer.persist_token_report(
        payload={
            "status": "success",
            "token_stats": {
                "prompt_tokens_actual": 305,
                "completion_tokens_actual": 20,
                "text_input_tokens_actual": 39,
                "video_input_tokens_actual": 266,
            },
        },
        vl_model="qwen-vl-max-latest",
    )

    assert report_path.endswith(os.path.join("intermediates", "stages", "phase2a", "audits", "vl_token_report_VT_CASE_1.json"))
    assert len(writes) == 6
    assert any(path.endswith(os.path.join("immediates", "vl_token_report_VT_CASE_1.json")) for path, _, _ in writes)
    assert any(path.endswith(os.path.join("intermediates", "vl_token_report_VT_CASE_1.json")) for path, _, _ in writes)
    payload = writes[0][1]
    assert payload["token_usage"]["prompt_tokens"] == 305
    assert payload["token_usage"]["video_input_tokens"] == 266
    assert payload["pricing"]["status"] == "ok"
    assert payload["pricing"]["currency"] == "CNY"
    assert payload["pricing"]["total_cost"] > 0


def test_persist_analysis_output_unknown_model_marks_pricing_as_unsupported(tmp_path: Path):
    writes = []

    def _capture(path, payload, **kwargs):
        writes.append((str(path), dict(payload), dict(kwargs)))

    writer = VLReportWriter(
        task_id="VT_CASE_2",
        video_path=str(tmp_path / "video.mp4"),
        semantic_units_path=str(tmp_path / "semantic_units_phase2a.json"),
        output_dir=str(tmp_path),
        logger=logging.getLogger("vl_report_writer_test"),
        enqueue_json_write_fn=_capture,
    )

    report_path = writer.persist_analysis_output(
        payload={
            "status": "success",
            "token_stats": {
                "prompt_tokens_actual": 1_000_000,
                "completion_tokens_actual": 1_000_000,
            },
            "merged_screenshots": [],
            "merged_clips": [],
        },
        vl_model="unknown-vl-model",
    )

    assert report_path.endswith(os.path.join("intermediates", "stages", "phase2a", "audits", "vl_analysis_output_VT_CASE_2.json"))
    assert len(writes) == 6
    assert any(path.endswith(os.path.join("immediates", "vl_analysis_output_VT_CASE_2.json")) for path, _, _ in writes)
    assert any(path.endswith(os.path.join("intermediates", "vl_analysis_output_VT_CASE_2.json")) for path, _, _ in writes)
    payload = writes[0][1]
    assert payload["pricing"]["status"] == "unsupported_model"
    assert payload["pricing"]["total_cost"] is None
