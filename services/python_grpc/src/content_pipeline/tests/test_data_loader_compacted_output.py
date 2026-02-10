import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

from services.python_grpc.src.content_pipeline.shared.subtitle.data_loader import (
    load_corrected_subtitles,
    load_merged_segments,
)


def test_load_corrected_subtitles_rejects_compacted_preview_shape(tmp_path):
    payload = {
        "step": "step2_correction",
        "output": {
            "corrected_subtitles": {
                "count": 370,
                "sample": [
                    {
                        "subtitle_id": "SUB001",
                        "corrected_text": "demo",
                        "start_sec": 0.0,
                        "end_sec": 1.0,
                    }
                ],
            }
        },
    }
    path = tmp_path / "step2_correction_output.json"
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    with pytest.raises(ValueError, match="Missing required fields"):
        load_corrected_subtitles(str(path))


def test_load_merged_segments_rejects_compacted_preview_shape(tmp_path):
    payload = {
        "step": "step6_merge_cross",
        "output": {
            "pure_text_script": {
                "count": 94,
                "sample": [
                    {
                        "paragraph_id": "P001",
                        "text": "demo",
                        "source_sentence_ids": ["S001"],
                    }
                ],
            }
        },
    }
    path = tmp_path / "step6_merge_cross_output.json"
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    with pytest.raises(ValueError, match="Missing required fields"):
        load_merged_segments(str(path))
