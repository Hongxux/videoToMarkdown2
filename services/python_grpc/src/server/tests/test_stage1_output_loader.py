import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

try:
    from services.python_grpc.src.server import grpc_service_impl as impl
except Exception as import_error:
    pytest.skip(f"grpc_service_impl import unavailable: {import_error}", allow_module_level=True)


def test_load_stage1_output_list_reads_nested_output_payload(tmp_path):
    output_path = tmp_path / "step3_merge_output.json"
    output_path.write_text(
        json.dumps(
            {
                "step": "step3_merge",
                "output": {
                    "merged_sentences": [
                        {"sentence_id": "S001", "text": "hello"},
                    ]
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    payload, reason = impl._load_stage1_output_list(str(output_path), "merged_sentences")
    assert reason == "ok"
    assert isinstance(payload, list)
    assert payload[0]["sentence_id"] == "S001"


def test_load_stage1_output_list_rejects_compacted_output(tmp_path):
    output_path = tmp_path / "step4_clean_local_output.json"
    output_path.write_text(
        json.dumps(
            {
                "step": "step4_clean_local",
                "output": {
                    "cleaned_sentences": {
                        "count": 20,
                        "sample": [{"sentence_id": "S001"}],
                    }
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    payload, reason = impl._load_stage1_output_list(str(output_path), "cleaned_sentences")
    assert payload is None
    assert reason == "compacted_output"
