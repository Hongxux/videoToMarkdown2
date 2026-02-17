import json
import shutil
import sys
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[6]))

from services.python_grpc.src.common.utils.async_disk_writer import (
    enqueue_json_write,
    enqueue_text_write,
    flush_async_json_writes,
    stop_async_json_writer,
)


def _make_repo_tmp_dir(test_name: str) -> Path:
    repo_root = Path(__file__).resolve().parents[6]
    base = repo_root / "var" / "tmp_tests_common_utils"
    base.mkdir(parents=True, exist_ok=True)

    safe_name = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in test_name)
    path = base / safe_name
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)
    return path


def setup_function(_func):
    stop_async_json_writer(timeout_sec=2.0)


def teardown_function(_func):
    stop_async_json_writer(timeout_sec=2.0)


def test_flush_returns_true_when_writer_not_started():
    stop_async_json_writer(timeout_sec=1.0)
    assert flush_async_json_writes(timeout_sec=1.0) is True


def test_async_writer_persists_json_and_text_files():
    tmp_path = _make_repo_tmp_dir("test_async_writer_persists_json_and_text_files")
    json_path = tmp_path / "intermediates" / "step2_correction_output.json"
    text_path = tmp_path / "intermediates" / "subtitles.txt"

    try:
        enqueue_json_write(
            str(json_path),
            {"step": "step2_correction", "output": {"corrected_subtitles": [{"subtitle_id": "SUB001"}]}},
            ensure_ascii=False,
            indent=2,
        )
        enqueue_text_write(str(text_path), "SUB001|0.0|1.0|hello")
    except PermissionError as error:
        pytest.skip(f"spawn process unavailable in current environment: {error}")

    assert flush_async_json_writes(timeout_sec=10.0) is True

    assert json_path.exists()
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["step"] == "step2_correction"
    assert payload["output"]["corrected_subtitles"][0]["subtitle_id"] == "SUB001"

    assert text_path.exists()
    assert "hello" in text_path.read_text(encoding="utf-8")
