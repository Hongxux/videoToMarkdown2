import json
import sys
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[5]))


try:
    from services.python_grpc.src.server import grpc_service_impl as impl
except Exception as exc:  # pragma: no cover
    pytest.skip(f"grpc_service_impl import unavailable: {exc}", allow_module_level=True)


def test_upsert_video_meta_topic_fields_preserves_existing_fields(tmp_path):
    output_dir = tmp_path / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "video_meta.json").write_text(
        json.dumps(
            {
                "title": "existing title",
                "source_url": "https://example.com/video",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    impl._upsert_video_meta_topic_fields(
        task_dir=str(output_dir),
        domain="  growth marketing  ",
        main_topic="  value content strategy in AI era  ",
    )

    payload = json.loads((output_dir / "video_meta.json").read_text(encoding="utf-8"))
    assert payload["title"] == "existing title"
    assert payload["source_url"] == "https://example.com/video"
    assert payload["domain"] == "growth marketing"
    assert payload["main_topic"] == "value content strategy in AI era"


def test_upsert_video_meta_topic_fields_creates_file_when_missing(tmp_path):
    output_dir = tmp_path / "task"
    output_dir.mkdir(parents=True, exist_ok=True)

    impl._upsert_video_meta_topic_fields(
        task_dir=str(output_dir),
        domain="computer science",
        main_topic="",
    )

    payload = json.loads((output_dir / "video_meta.json").read_text(encoding="utf-8"))
    assert payload["domain"] == "computer science"
    assert "main_topic" not in payload
