import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.python_grpc.src.common.utils.runtime_recovery_store import RuntimeRecoveryStore
from services.python_grpc.src.server.runtime_stage_state import (
    RuntimeStageSession,
    record_runtime_stage_checkpoint,
)


def _make_repo_tmp_dir(test_name: str) -> Path:
    repo_root = Path(__file__).resolve().parents[1]
    base = repo_root / "var" / "tmp_runtime_stage_state_tests"
    base.mkdir(parents=True, exist_ok=True)
    safe_name = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in test_name)
    unique_suffix = f"{time.time_ns() % 1_000_000:06d}"
    path = base / f"{safe_name[:24]}_{unique_suffix}"
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _load_stage_state(output_dir: Path, stage: str) -> dict:
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-stage-loader")
    assert store._sqlite_index is not None
    payload = store._sqlite_index.load_stage_snapshot(
        output_dir=str(output_dir.resolve()),
        stage=stage,
    )
    assert payload is not None
    return payload


def test_record_runtime_stage_checkpoint_writes_completed_state():
    output_dir = _make_repo_tmp_dir("test_record_runtime_stage_checkpoint_writes_completed_state") / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    step2_path = output_dir / "intermediates" / "step2.json"
    step2_path.parent.mkdir(parents=True, exist_ok=True)
    step2_path.write_text("{}", encoding="utf-8")

    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-stage")
    record_runtime_stage_checkpoint(
        store=store,
        output_dir=str(output_dir),
        stage="stage1",
        status="running",
        checkpoint="pipeline_prepare",
        completed=0,
        pending=6,
        extra_payload={"step2_json_path": str(step2_path)},
    )

    running_state = _load_stage_state(output_dir, "stage1")
    assert running_state["status"] == "EXECUTING"
    assert running_state["checkpoint"] == "pipeline_prepare"
    assert running_state["completed"] == 0
    assert running_state["pending"] == 6

    record_runtime_stage_checkpoint(
        store=store,
        output_dir=str(output_dir),
        stage="stage1",
        status="completed",
        checkpoint="stage1_response_ready",
        completed=6,
        pending=0,
        extra_payload={
            "reused_stage1": True,
            "step2_json_path": str(step2_path),
        },
    )

    completed_state = _load_stage_state(output_dir, "stage1")
    assert completed_state["status"] == "COMPLETED"
    assert completed_state["checkpoint"] == "stage1_response_ready"
    assert completed_state["reused_stage1"] is True
    assert completed_state["step2_json_path"] == str(step2_path)


def test_record_runtime_stage_checkpoint_classifies_retryable_failure():
    output_dir = _make_repo_tmp_dir("test_record_runtime_stage_checkpoint_classifies_retryable_failure") / "task"
    output_dir.mkdir(parents=True, exist_ok=True)

    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-stage-fail")
    record_runtime_stage_checkpoint(
        store=store,
        output_dir=str(output_dir),
        stage="phase2b",
        status="failed",
        checkpoint="phase2b_failed",
        completed=3,
        pending=1,
        error=RuntimeError("429 rate limit exceeded"),
    )

    failed_state = _load_stage_state(output_dir, "phase2b")
    assert failed_state["status"] == "AUTO_RETRY_WAIT"
    assert failed_state["checkpoint"] == "phase2b_failed"
    assert failed_state["completed"] == 3
    assert failed_state["pending"] == 1
    assert failed_state["error_class"] == "AUTO_RETRYABLE"
    assert failed_state["error_message"] == "429 rate limit exceeded"
    assert failed_state["retry_strategy"] == "AUTO_RETRY"
    assert failed_state["operator_action"] == "WAIT_AUTO_RETRY"
    assert failed_state["retry_mode"] == "auto"
    assert failed_state["retry_entry_point"] == "from_last_checkpoint"


def test_record_runtime_stage_checkpoint_classifies_manual_failure_with_guidance():
    output_dir = _make_repo_tmp_dir("test_record_runtime_stage_checkpoint_classifies_manual_failure_with_guidance") / "task"
    output_dir.mkdir(parents=True, exist_ok=True)

    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-stage-manual")
    record_runtime_stage_checkpoint(
        store=store,
        output_dir=str(output_dir),
        stage="phase2b",
        status="failed",
        checkpoint="phase2b_failed",
        completed=2,
        pending=1,
        error=RuntimeError("insufficient credits"),
    )

    failed_state = _load_stage_state(output_dir, "phase2b")
    assert failed_state["status"] == "MANUAL_RETRY_REQUIRED"
    assert failed_state["error_class"] == "MANUAL_RETRY_REQUIRED"
    assert failed_state["retry_strategy"] == "MANUAL_RETRY_AFTER_REPAIR"
    assert failed_state["operator_action"] == "RESTORE_QUOTA_OR_BALANCE"
    assert failed_state["retry_mode"] == "manual"
    assert failed_state["retry_entry_point"] == "from_last_checkpoint"
    assert "额度" in failed_state["required_action"]


def test_runtime_stage_session_bridges_event_emitter_and_failure_state():
    output_dir = _make_repo_tmp_dir("test_runtime_stage_session_bridges_event_emitter_and_failure_state") / "task"
    output_dir.mkdir(parents=True, exist_ok=True)

    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-stage-session")
    hard_events = []
    event_bridge = []

    def _emit(**kwargs):
        hard_events.append(dict(kwargs))

    def _emit_event(event):
        event_bridge.append(dict(event))

    session = RuntimeStageSession(
        store=store,
        output_dir=str(output_dir),
        task_id="task-stage-session",
        stage="stage1",
        base_payload={"video_path": "demo.mp4"},
        heartbeat_emitter=_emit,
        heartbeat_event_emitter=_emit_event,
    )

    session.mark(
        status="running",
        checkpoint="pipeline_prepare",
        completed=0,
        pending=6,
    )
    session.mark_from_event(
        {
            "status": "running",
            "checkpoint": "step2_correction",
            "completed": 2,
            "pending": 4,
            "event": "step_completed",
        },
        emit_watchdog_event=True,
        default_pending=6,
        extra_payload={"step_name": "step2_correction"},
    )
    session.mark_failed(
        checkpoint="stage1_failed",
        error=RuntimeError("disk full"),
        extra_watchdog={"error": "disk full"},
    )

    state = _load_stage_state(output_dir, "stage1")
    assert state["status"] == "MANUAL_RETRY_REQUIRED"
    assert state["checkpoint"] == "stage1_failed"
    assert state["retry_strategy"] == "MANUAL_RETRY_AFTER_REPAIR"
    assert state["operator_action"] == "FREE_DISK_SPACE"
    assert state["retry_mode"] == "manual"
    assert "磁盘" in state["required_action"]
    assert len(hard_events) == 2
    assert hard_events[0]["checkpoint"] == "pipeline_prepare"
    assert hard_events[0]["signal_type"] == "hard"
    assert hard_events[1]["checkpoint"] == "stage1_failed"
    assert hard_events[1]["signal_type"] == "hard"
    assert event_bridge[0]["checkpoint"] == "step2_correction"


def test_runtime_stage_checkpoint_only_writes_stage_file_mirror_when_enabled(monkeypatch):
    monkeypatch.setenv("TASK_RUNTIME_WRITE_STAGE_FILE_MIRRORS", "1")
    output_dir = _make_repo_tmp_dir("test_runtime_stage_file_mirror_enabled") / "task"
    output_dir.mkdir(parents=True, exist_ok=True)

    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-stage-file-mirror")
    record_runtime_stage_checkpoint(
        store=store,
        output_dir=str(output_dir),
        stage="phase2b",
        status="failed",
        checkpoint="phase2b_failed",
        completed=1,
        pending=2,
        error=RuntimeError("insufficient credits"),
    )

    stage_state_path = output_dir / "intermediates" / "rt" / "stage" / "phase2b" / "stage_state.json"
    resume_index_path = output_dir / "intermediates" / "rt" / "resume_index.json"
    assert stage_state_path.exists()
    assert resume_index_path.exists()
