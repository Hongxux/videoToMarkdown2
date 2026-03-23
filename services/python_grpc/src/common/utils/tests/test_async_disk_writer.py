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


def test_async_writer_flush_supports_scope_key():
    tmp_path = _make_repo_tmp_dir("test_async_writer_flush_supports_scope_key")
    scope_a = tmp_path / "task_a"
    scope_b = tmp_path / "task_b"
    json_a = scope_a / "intermediates" / "step2_correction_output.json"

    try:
        enqueue_json_write(
            str(json_a),
            {"step": "step2_correction", "output": {"corrected_subtitles": [{"subtitle_id": "SUB_A"}]}},
            ensure_ascii=False,
            indent=2,
            scope_key=str(scope_a),
        )
        for index in range(12):
            enqueue_json_write(
                str(scope_b / "intermediates" / f"bulk_{index}.json"),
                {"index": index, "payload": "x" * 1024},
                ensure_ascii=False,
                indent=2,
                scope_key=str(scope_b),
            )
    except PermissionError as error:
        pytest.skip(f"spawn process unavailable in current environment: {error}")

    assert flush_async_json_writes(timeout_sec=10.0, scope_key=str(scope_a)) is True
    assert json_a.exists()
    assert flush_async_json_writes(timeout_sec=20.0) is True

from services.python_grpc.src.common.utils import runtime_recovery_store as runtime_store_module


def test_runtime_recovery_store_atomic_write_retries_replace_after_windows_access_denied(monkeypatch):
    tmp_path = _make_repo_tmp_dir("test_runtime_recovery_store_atomic_write_retries_replace_after_windows_access_denied")
    target = tmp_path / "intermediates" / "rt" / "resume_index.json"
    payload = {"hint_stage": "stage1", "hint_checkpoint": "step3_merge.wave_0004"}

    original_replace = runtime_store_module.os.replace
    call_counter = {"count": 0}

    def flaky_replace(src, dst):
        call_counter["count"] += 1
        if call_counter["count"] == 1:
            raise PermissionError(5, "Access is denied")
        return original_replace(src, dst)

    sleep_calls = []

    monkeypatch.setattr(runtime_store_module.os, "replace", flaky_replace)
    monkeypatch.setattr(runtime_store_module.time, "sleep", lambda seconds: sleep_calls.append(seconds))
    monkeypatch.setattr(runtime_store_module, "_ATOMIC_WRITE_RETRY_COUNT", 2)
    monkeypatch.setattr(runtime_store_module, "_ATOMIC_WRITE_RETRY_MS", 1)

    runtime_store_module._write_json_atomic_sync(target, payload)

    assert call_counter["count"] == 2
    assert sleep_calls == [0.001]
    assert json.loads(target.read_text(encoding="utf-8")) == payload
