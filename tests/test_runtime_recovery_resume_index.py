import json
import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.python_grpc.src.common.utils.runtime_recovery_store import (
    RuntimeRecoveryStore,
    build_llm_input_fingerprint,
)


def _make_repo_tmp_dir(test_name: str) -> Path:
    repo_root = Path(__file__).resolve().parents[1]
    base = repo_root / "var" / "tmp_runtime_resume_index_tests"
    base.mkdir(parents=True, exist_ok=True)
    safe_name = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in test_name)
    unique_suffix = f"{time.time_ns() % 1_000_000:06d}"
    path = base / f"{safe_name[:24]}_{unique_suffix}"
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)
    return path


def test_runtime_recovery_store_writes_resume_index_for_latest_blocking_stage():
    output_dir = _make_repo_tmp_dir("resume_index_blocking_stage") / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-resume-index")

    store.update_stage_state(
        stage="phase2a",
        status="RUNNING",
        payload={
            "checkpoint": "phase2a_running",
            "completed": 3,
            "pending": 2,
            "output_dir": str(output_dir),
        },
    )
    store.update_stage_state(
        stage="phase2b",
        status="MANUAL_NEEDED",
        payload={
            "checkpoint": "llm_call_commit_pending",
            "retry_mode": "manual",
            "required_action": "repair llm quota and retry",
            "retry_entry_point": "phase2b/chunk-42",
            "output_dir": str(output_dir),
        },
    )

    resume_index_path = output_dir / "intermediates" / "rt" / "resume_index.json"
    assert resume_index_path.exists()
    payload = json.loads(resume_index_path.read_text(encoding="utf-8"))

    assert payload["hint_stage"] == "phase2b"
    assert payload["hint_status"] == "MANUAL_NEEDED"
    assert payload["hint_checkpoint"] == "llm_call_commit_pending"
    assert payload["hint_stage_state_path"].endswith("stage_state.json")
    assert payload["recovery_anchor"]["resume_from_stage"] == "phase2b"


def test_runtime_recovery_store_writes_stage_state_file_only_when_mirror_enabled(monkeypatch):
    monkeypatch.setenv("TASK_RUNTIME_WRITE_STAGE_FILE_MIRRORS", "1")
    output_dir = _make_repo_tmp_dir("stage_state_file_mirror_enabled") / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-stage-state-file")

    store.update_stage_state(
        stage="phase2b",
        status="MANUAL_NEEDED",
        payload={
            "checkpoint": "llm_call_commit_pending",
            "retry_mode": "manual",
            "required_action": "repair llm quota and retry",
            "retry_entry_point": "phase2b/chunk-42",
        },
    )

    resume_index_path = output_dir / "intermediates" / "rt" / "resume_index.json"
    stage_state_path = output_dir / "intermediates" / "rt" / "stage" / "phase2b" / "stage_state.json"
    assert resume_index_path.exists()
    assert stage_state_path.exists()


def test_runtime_recovery_store_reads_resume_index_from_task_local_sqlite_when_file_missing():
    output_dir = _make_repo_tmp_dir("resume_index_sqlite_fallback") / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-resume-index-sqlite")

    store.update_stage_state(
        stage="phase2a",
        status="RUNNING",
        payload={
            "checkpoint": "phase2a_running",
            "completed": 3,
            "pending": 2,
        },
    )
    store.update_stage_state(
        stage="phase2b",
        status="MANUAL_NEEDED",
        payload={
            "checkpoint": "llm_call_commit_pending",
            "retry_mode": "manual",
            "required_action": "repair llm quota and retry",
            "retry_entry_point": "phase2b/chunk-42",
        },
    )

    resume_index_path = output_dir / "intermediates" / "rt" / "resume_index.json"
    resume_index_path.unlink(missing_ok=True)

    reopened_store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-resume-index-sqlite")
    payload = reopened_store._read_resume_index()

    assert payload["hint_stage"] == "phase2b"
    assert payload["hint_status"] == "MANUAL_NEEDED"
    assert payload["hint_checkpoint"] == "llm_call_commit_pending"
    assert payload["recovery_anchor"]["resume_from_stage"] == "phase2b"


def test_runtime_recovery_store_restores_llm_response_from_lookup_index_without_directory_scan():
    output_dir = _make_repo_tmp_dir("resume_index_llm_lookup") / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-llm-index")

    input_fingerprint = build_llm_input_fingerprint(
        step_name="structured_text",
        unit_id="SU900",
        model="deepseek-chat",
        system_prompt="system",
        user_prompt="user",
    )
    llm_call_id = store.build_llm_call_id(
        step_name="structured_text",
        unit_id="SU900",
        input_fingerprint=input_fingerprint,
    )
    handle = store.begin_llm_attempt(
        stage="phase2b",
        chunk_id="unit_SU900",
        llm_call_id=llm_call_id,
        input_fingerprint=input_fingerprint,
        request_payload={"prompt": "user"},
    )
    store.commit_llm_attempt(
        handle=handle,
        response_text="resume index llm payload",
        response_metadata={"model": "deepseek-chat"},
        max_part_bytes=64,
    )

    store._collect_exact_attempt_dirs = lambda **kwargs: []
    store._collect_fallback_attempt_dirs = lambda **kwargs: []
    restored = store.load_committed_llm_response(
        stage="phase2b",
        chunk_id="unit_SU900",
        llm_call_id=llm_call_id,
        input_fingerprint=input_fingerprint,
    )

    assert restored is not None
    assert restored["response_text"] == "resume index llm payload"


def test_runtime_recovery_store_uses_attempt_hint_when_lookup_path_is_stale():
    output_dir = _make_repo_tmp_dir("resume_index_llm_hint_probe") / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-llm-hint")

    input_fingerprint = build_llm_input_fingerprint(
        step_name="structured_text",
        unit_id="SU901",
        model="deepseek-chat",
        system_prompt="system",
        user_prompt="user",
    )
    llm_call_id = store.build_llm_call_id(
        step_name="structured_text",
        unit_id="SU901",
        input_fingerprint=input_fingerprint,
    )
    first_handle = store.begin_llm_attempt(
        stage="phase2b",
        chunk_id="unit_SU901",
        llm_call_id=llm_call_id,
        input_fingerprint=input_fingerprint,
        request_payload={"prompt": "user"},
    )
    store.fail_llm_attempt(
        handle=first_handle,
        error=RuntimeError("temporary provider error"),
        request_snapshot={"prompt": "user"},
    )
    second_handle = store.begin_llm_attempt(
        stage="phase2b",
        chunk_id="unit_SU901",
        llm_call_id=llm_call_id,
        input_fingerprint=input_fingerprint,
        request_payload={"prompt": "user"},
    )
    store.commit_llm_attempt(
        handle=second_handle,
        response_text="bounded hint payload",
        response_metadata={"model": "deepseek-chat"},
        max_part_bytes=64,
    )

    lookup_index_path = store._llm_lookup_index_path(
        stage="phase2b",
        chunk_id="unit_SU901",
        llm_call_id=llm_call_id,
        input_fingerprint=input_fingerprint,
    )
    store._write_llm_lookup_index(
        stage="phase2b",
        chunk_id="unit_SU901",
        llm_call_id=llm_call_id,
        input_fingerprint=input_fingerprint,
        attempt=2,
        attempt_dir=second_handle.attempt_dir,
        response_hash="",
        committed_parts=1,
    )
    lookup_payload = json.loads(lookup_index_path.read_text(encoding="utf-8"))
    lookup_payload["attempt_dir"] = str(second_handle.attempt_dir.parent / "missing.a999")
    lookup_index_path.write_text(json.dumps(lookup_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    store._collect_exact_attempt_dirs = lambda **kwargs: []
    store._collect_fallback_attempt_dirs = lambda **kwargs: []
    restored = store.load_committed_llm_response(
        stage="phase2b",
        chunk_id="unit_SU901",
        llm_call_id=llm_call_id,
        input_fingerprint=input_fingerprint,
    )

    assert restored is not None
    assert restored["response_text"] == "bounded hint payload"


def test_runtime_recovery_store_falls_back_to_previous_lookup_record():
    output_dir = _make_repo_tmp_dir("resume_index_prev_slot") / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-prev-slot")

    input_fingerprint = build_llm_input_fingerprint(
        step_name="structured_text",
        unit_id="SU902",
        model="deepseek-chat",
        system_prompt="system",
        user_prompt="user",
    )
    llm_call_id = store.build_llm_call_id(
        step_name="structured_text",
        unit_id="SU902",
        input_fingerprint=input_fingerprint,
    )

    first_handle = store.begin_llm_attempt(
        stage="phase2b",
        chunk_id="unit_SU902",
        llm_call_id=llm_call_id,
        input_fingerprint=input_fingerprint,
        request_payload={"prompt": "user"},
    )
    store.commit_llm_attempt(
        handle=first_handle,
        response_text="previous slot payload",
        response_metadata={"model": "deepseek-chat"},
        max_part_bytes=64,
    )
    second_handle = store.begin_llm_attempt(
        stage="phase2b",
        chunk_id="unit_SU902",
        llm_call_id=llm_call_id,
        input_fingerprint=input_fingerprint,
        request_payload={"prompt": "user"},
    )
    store.commit_llm_attempt(
        handle=second_handle,
        response_text="current slot payload",
        response_metadata={"model": "deepseek-chat"},
        max_part_bytes=64,
    )

    lookup_index_path = store._llm_lookup_index_path(
        stage="phase2b",
        chunk_id="unit_SU902",
        llm_call_id=llm_call_id,
        input_fingerprint=input_fingerprint,
    )
    store._write_llm_lookup_index(
        stage="phase2b",
        chunk_id="unit_SU902",
        llm_call_id=llm_call_id,
        input_fingerprint=input_fingerprint,
        attempt=2,
        attempt_dir=second_handle.attempt_dir,
        response_hash="",
        committed_parts=1,
    )
    lookup_payload = json.loads(lookup_index_path.read_text(encoding="utf-8"))
    lookup_payload["attempt_dir"] = str(second_handle.attempt_dir.parent / "broken.a999")
    lookup_index_path.write_text(json.dumps(lookup_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    store._collect_exact_attempt_dirs = lambda **kwargs: []
    store._collect_fallback_attempt_dirs = lambda **kwargs: []
    restored = store.load_committed_llm_response(
        stage="phase2b",
        chunk_id="unit_SU902",
        llm_call_id=llm_call_id,
        input_fingerprint=input_fingerprint,
    )

    assert restored is not None
    assert restored["response_text"] == "current slot payload"


def test_runtime_recovery_store_restores_chunk_payload_from_lookup_index_without_chunk_scan():
    output_dir = _make_repo_tmp_dir("resume_index_chunk_lookup") / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-chunk-index")

    payload = {
        "request_updates": [
            {"request_key": "id:shot_001", "timestamp_sec": 12.5, "_optimized": True}
        ]
    }
    store.commit_chunk_payload(
        stage="phase2a",
        chunk_id="ss000123",
        input_fingerprint="fp-resume-index",
        result_payload=payload,
        metadata={"mode": "streaming"},
    )

    bad_path = output_dir / "var" / "missing"
    store.chunk_dir = lambda *, stage, chunk_id: bad_path
    store._legacy_chunk_dir = lambda *, stage, chunk_id: bad_path
    restored = store.load_committed_chunk_payload(
        stage="phase2a",
        chunk_id="ss000123",
        input_fingerprint="fp-resume-index",
    )

    assert restored is not None
    assert restored["result_payload"] == payload
