import json
import time
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

from services.python_grpc.src.server import watchdog_signal_writer as watchdog_writer


def test_persist_watchdog_payload_retries_then_success(tmp_path, monkeypatch):
    target = tmp_path / "intermediates" / "task_watchdog_heartbeat.json"
    payload = {"seq": 3, "status": "running", "checkpoint": "probe"}

    original_replace = watchdog_writer.os.replace
    call_counter = {"count": 0}

    def flaky_replace(src, dst):
        call_counter["count"] += 1
        if call_counter["count"] < 3:
            raise PermissionError(5, "Access is denied")
        return original_replace(src, dst)

    monkeypatch.setattr(watchdog_writer.os, "replace", flaky_replace)
    monkeypatch.setattr(watchdog_writer, "_HEARTBEAT_WRITE_RETRY_COUNT", 4)
    monkeypatch.setattr(watchdog_writer, "_HEARTBEAT_WRITE_RETRY_MS", 1)

    error = watchdog_writer.persist_watchdog_payload(str(target), payload)

    assert error is None
    assert call_counter["count"] == 3
    assert json.loads(target.read_text(encoding="utf-8")) == payload


def test_task_watchdog_emit_is_best_effort_when_persist_failed(tmp_path, monkeypatch):
    def always_fail(_path, _payload):
        return "simulated persist error"

    monkeypatch.setattr(watchdog_writer, "persist_watchdog_payload", always_fail)

    task_id = f"VT_WATCHDOG_CASE_{time.time_ns()}"
    writer = watchdog_writer.TaskWatchdogSignalWriter(
        task_id=task_id,
        output_dir=str(tmp_path),
        stage="transcribe",
        total_steps=1,
    )

    writer.emit(
        status="running",
        checkpoint="transcribe_running",
        completed=0,
        pending=1,
        signal_type="hard",
    )

    events = watchdog_writer.read_watchdog_signals(
        task_id=task_id,
        stage="transcribe",
        from_stream_seq=0,
        limit=8,
    )

    assert events
    assert events[-1]["checkpoint"] == "transcribe_running"
