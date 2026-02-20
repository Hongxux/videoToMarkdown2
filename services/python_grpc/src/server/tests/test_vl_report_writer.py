import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

from services.python_grpc.src.server.vl_report_writer import VLReportWriter


def test_persist_token_report_writes_immediates_and_intermediates(tmp_path: Path):
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
                "prompt_tokens_actual": 1000,
                "completion_tokens_actual": 500,
            },
        },
        vl_model="qwen3-vl-plus",
    )

    assert report_path.endswith(os.path.join("immediates", "vl_token_report_VT_CASE_1.json"))
    assert len(writes) == 4
    payload = writes[0][1]
    assert payload["token_usage"]["prompt_tokens"] == 1000
    assert payload["token_usage"]["completion_tokens"] == 500
    assert payload["token_usage"]["total_tokens"] == 1500
    assert payload["pricing"]["selected_pricing_model"] == "qwen3-vl-plus"
    assert payload["pricing"]["selected_cost_usd"] == 0.00375


def test_persist_analysis_output_unknown_model_keeps_cost_range(tmp_path: Path):
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

    assert report_path.endswith(os.path.join("immediates", "vl_analysis_output_VT_CASE_2.json"))
    assert len(writes) == 4
    payload = writes[0][1]
    assert payload["pricing"]["selected_pricing_model"] == "unknown"
    assert payload["pricing"]["selected_cost_usd_min"] == 4.0
    assert payload["pricing"]["selected_cost_usd_max"] == 6.0
    assert "selected_cost_usd" not in payload["pricing"]
