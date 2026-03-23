import asyncio
import sqlite3
import hashlib
import json
import os
import sys
import shutil
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.python_grpc.src.common.utils.runtime_recovery_store import (
    RuntimeRecoveryStore,
    build_llm_input_fingerprint,
    classify_runtime_error,
)
from services.python_grpc.src.common.utils.runtime_recovery_sqlite import RuntimeRecoverySqliteIndex
from services.python_grpc.src.content_pipeline.markdown_enhancer import MarkdownEnhancer
from services.python_grpc.src.content_pipeline.phase2a.materials.vl_material_generator import VLMaterialGenerator


def _make_repo_tmp_dir(test_name: str) -> Path:
    repo_root = Path(__file__).resolve().parents[1]
    base = repo_root / "var" / "tmp_runtime_recovery_tests"
    base.mkdir(parents=True, exist_ok=True)
    safe_name = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in test_name)
    unique_suffix = f"{time.time_ns() % 1_000_000:06d}"
    path = base / f"{safe_name[:24]}_{unique_suffix}"
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)
    return path


def test_runtime_recovery_store_commits_and_restores_llm_response():
    output_dir = _make_repo_tmp_dir("test_runtime_recovery_store_commits_and_restores_llm_response") / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-1")

    input_fingerprint = build_llm_input_fingerprint(
        step_name="structured_text",
        unit_id="SU100",
        model="deepseek-chat",
        system_prompt="system",
        user_prompt="user",
    )
    llm_call_id = store.build_llm_call_id(
        step_name="structured_text",
        unit_id="SU100",
        input_fingerprint=input_fingerprint,
    )
    handle = store.begin_llm_attempt(
        stage="phase2b",
        chunk_id="unit_SU100",
        llm_call_id=llm_call_id,
        input_fingerprint=input_fingerprint,
        request_payload={"prompt": "user"},
        metadata={"step_name": "structured_text", "storage_backend": "hybrid"},
    )

    response_text = "这是一段很长的输出。" * 40
    commit_payload = store.commit_llm_attempt(
        handle=handle,
        response_text=response_text,
        response_metadata={"model": "deepseek-chat"},
        max_part_bytes=64,
    )

    restored = store.load_committed_llm_response(
        stage="phase2b",
        chunk_id="unit_SU100",
        llm_call_id=llm_call_id,
        input_fingerprint=input_fingerprint,
    )

    assert commit_payload["committed_parts"] > 1
    assert restored is not None
    assert restored["response_text"] == response_text
    assert restored["response_hash"] == commit_payload["response_hash"]
    assert restored["source"] == "sqlite"


def test_runtime_recovery_store_does_not_restore_legacy_llm_layout_after_readable_id_upgrade():
    output_dir = _make_repo_tmp_dir("test_runtime_recovery_store_restores_legacy_llm_layout_after_readable_id_upgrade") / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-legacy")

    input_fingerprint = build_llm_input_fingerprint(
        step_name="structured_text",
        unit_id="SU101",
        model="deepseek-chat",
        system_prompt="system",
        user_prompt="user",
    )
    readable_llm_call_id = store.build_llm_call_id(
        step_name="structured_text",
        unit_id="SU101",
        input_fingerprint=input_fingerprint,
    )
    legacy_suffix = hashlib.sha256(f"structured_text|SU101|{input_fingerprint}".encode("utf-8")).hexdigest()[:16]
    legacy_llm_call_id = f"lc_{legacy_suffix}"
    legacy_attempt_dir = (
        output_dir
        / "intermediates"
        / "rt"
        / "s"
        / "phase2b"
        / "c"
        / "unit_SU101"
        / "l"
        / legacy_llm_call_id
        / "a001"
    )
    legacy_parts_dir = legacy_attempt_dir / "p"
    legacy_parts_dir.mkdir(parents=True, exist_ok=True)
    response_text = "legacy restore payload"
    manifest_payload = {
        "schema_version": "runtime_llm_manifest_v1",
        "task_id": "task-legacy",
        "stage": "phase2b",
        "chunk_id": "unit_SU101",
        "llm_call_id": legacy_llm_call_id,
        "attempt": 1,
        "status": "SUCCESS",
        "input_fingerprint": input_fingerprint,
    }
    commit_payload = {
        "schema_version": "runtime_llm_commit_v1",
        "task_id": "task-legacy",
        "stage": "phase2b",
        "chunk_id": "unit_SU101",
        "llm_call_id": legacy_llm_call_id,
        "attempt": 1,
        "status": "SUCCESS",
        "input_fingerprint": input_fingerprint,
        "response_hash": hashlib.sha256(response_text.encode("utf-8")).hexdigest(),
        "committed_parts": 1,
    }
    part_payload = {
        "schema_version": "runtime_llm_part_v1",
        "task_id": "task-legacy",
        "stage": "phase2b",
        "chunk_id": "unit_SU101",
        "llm_call_id": legacy_llm_call_id,
        "attempt": 1,
        "part_index": 0,
        "char_start": 0,
        "char_end": len(response_text),
        "byte_start": 0,
        "byte_end": len(response_text.encode("utf-8")),
        "payload_chars": len(response_text),
        "payload_bytes": len(response_text.encode("utf-8")),
        "payload_hash": hashlib.sha256(response_text.encode("utf-8")).hexdigest(),
        "content": response_text,
    }
    for path, payload in (
        (legacy_attempt_dir / "request.json", {"prompt": "user"}),
        (legacy_attempt_dir / "manifest.json", manifest_payload),
        (legacy_attempt_dir / "commit.json", commit_payload),
        (legacy_parts_dir / "p0000.json", part_payload),
    ):
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    restored = store.load_committed_llm_response(
        stage="phase2b",
        chunk_id="unit_SU101",
        llm_call_id=readable_llm_call_id,
        input_fingerprint=input_fingerprint,
    )

    assert restored is None


def test_runtime_recovery_store_commits_and_restores_chunk_payload():
    output_dir = _make_repo_tmp_dir("test_runtime_recovery_store_commits_and_restores_chunk_payload") / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-2")

    payload = {
        "request_updates": [
            {"request_key": "id:shot_001", "timestamp_sec": 12.5, "_optimized": True}
        ]
    }
    store.commit_chunk_payload(
        stage="phase2a",
        chunk_id="ss000001",
        input_fingerprint="fp-123",
        result_payload=payload,
        metadata={"mode": "streaming"},
    )

    restored = store.load_committed_chunk_payload(
        stage="phase2a",
        chunk_id="ss000001",
        input_fingerprint="fp-123",
    )

    assert restored is not None
    assert restored["result_payload"] == payload


def test_runtime_recovery_store_loads_latest_committed_chunk_payload_by_chunk_id():
    output_dir = _make_repo_tmp_dir("test_runtime_recovery_store_loads_latest_committed_chunk_payload_by_chunk_id") / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-2b")

    store.commit_chunk_payload(
        stage="phase2b",
        chunk_id="phase2b.document_assemble.wave_0001",
        input_fingerprint="fp-old",
        result_payload={"markdown_path": "old.md", "json_path": "old.json", "title": "old"},
        metadata={"mode": "streaming"},
        attempt=1,
    )
    latest_commit = store.commit_chunk_payload(
        stage="phase2b",
        chunk_id="phase2b.document_assemble.wave_0001",
        input_fingerprint="fp-new",
        result_payload={"markdown_path": "new.md", "json_path": "new.json", "title": "new"},
        metadata={"mode": "streaming"},
        attempt=2,
    )

    restored = store.load_latest_committed_chunk_payload(
        stage="phase2b",
        chunk_id="phase2b.document_assemble.wave_0001",
    )

    assert restored is not None
    assert restored["result_payload"]["markdown_path"] == "new.md"
    assert restored["commit_payload"]["result_hash"] == latest_commit["result_hash"]


def test_runtime_recovery_store_uses_minimal_llm_content_schema():
    output_dir = _make_repo_tmp_dir("test_runtime_recovery_store_field_restore_survives_broken_manifest_and_commit_blobs") / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    db_path = output_dir.parent / "runtime_recovery_field_restore.sqlite3"
    previous_db_path = os.environ.get("TASK_RUNTIME_SQLITE_DB_PATH")
    os.environ["TASK_RUNTIME_SQLITE_DB_PATH"] = str(db_path)
    try:
        store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-field-restore")
        input_fingerprint = build_llm_input_fingerprint(
            step_name="structured_text",
            unit_id="SU110",
            model="deepseek-chat",
            system_prompt="system",
            user_prompt="user",
        )
        llm_call_id = store.build_llm_call_id(
            step_name="structured_text",
            unit_id="SU110",
            input_fingerprint=input_fingerprint,
        )
        handle = store.begin_llm_attempt(
            stage="phase2b",
            chunk_id="unit_SU110",
            llm_call_id=llm_call_id,
            input_fingerprint=input_fingerprint,
            request_payload={"prompt": "user"},
            metadata={"step_name": "structured_text", "storage_backend": "hybrid"},
        )
        commit_payload = store.commit_llm_attempt(
            handle=handle,
            response_text="field-restore-response",
            response_metadata={
                "model": "deepseek-chat",
                "prompt_tokens": 11,
                "completion_tokens": 7,
                "total_tokens": 18,
                "latency_ms": 123.5,
                "cache_hit": True,
                "is_fallback": True,
                "usage_details": {"cached_prompt_tokens": 5},
                "fallback": {"reason": "provider_timeout"},
                "previous_failures": [{"provider": "deepseek", "error": "timeout"}],
                "propagated_scope_refs": ["rt/phase2a/llm_call/vl_call_001"],
                "raw_response": {"id": "resp_001"},
                "custom_tag": "hot-field-restore",
            },
        )

        connection = sqlite3.connect(str(db_path))
        try:
            llm_content_columns = [
                row[1]
                for row in connection.execute("PRAGMA table_info(llm_record_content)").fetchall()
            ]
        finally:
            connection.close()

        restored = store.load_committed_llm_response(
            stage="phase2b",
            chunk_id="unit_SU110",
            llm_call_id=llm_call_id,
            input_fingerprint=input_fingerprint,
        )

        assert restored is not None
        assert restored["response_text"] == "field-restore-response"
        assert restored["response_metadata"] == {}
        assert llm_content_columns == ["llm_record_id", "response_codec", "response_payload"]
    finally:
        if previous_db_path is None:
            os.environ.pop("TASK_RUNTIME_SQLITE_DB_PATH", None)
        else:
            os.environ["TASK_RUNTIME_SQLITE_DB_PATH"] = previous_db_path


def test_runtime_recovery_store_restores_whitelisted_llm_recovery_metadata_only(monkeypatch):
    tmp_root = _make_repo_tmp_dir("sqlite_llm_recovery_metadata_only")
    db_path = tmp_root / "runtime_recovery.sqlite3"
    monkeypatch.setenv("TASK_RUNTIME_SQLITE_DB_PATH", str(db_path))
    output_dir = tmp_root / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-sqlite-recovery-only")

    input_fingerprint = build_llm_input_fingerprint(
        step_name="vl_video_analysis",
        unit_id="SU_META",
        model="qwen-vl-max",
        system_prompt="system",
        user_prompt="user",
    )
    llm_call_id = store.build_llm_call_id(
        step_name="vl_video_analysis",
        unit_id="SU_META",
        input_fingerprint=input_fingerprint,
    )
    handle = store.begin_llm_attempt(
        stage="phase2a",
        chunk_id="unit_SU_META",
        llm_call_id=llm_call_id,
        input_fingerprint=input_fingerprint,
        request_payload={"prompt": "[SU_META] analyze", "system_prompt": "system"},
        metadata={"storage_backend": "sqlite", "stage_step": "vl_video_analysis"},
    )
    store.commit_llm_attempt(
        handle=handle,
        response_text='{"ok":true}',
        response_metadata={
            "model": "qwen-vl-max",
            "prompt_tokens": 17,
            "completion_tokens": 9,
            "total_tokens": 26,
            "latency_ms": 88.5,
            "cache_hit": False,
            "is_fallback": True,
            "usage": {"prompt_tokens": 17, "completion_tokens": 9, "video_tokens": 33},
            "finish_reason": "stop",
            "offline_task_meta": {"task_type": "dashscope"},
            "usage_details": {"cached_prompt_tokens": 3},
            "fallback": {"reason": "provider_timeout"},
            "previous_failures": [{"provider": "deepseek", "error": "timeout"}],
            "propagated_scope_refs": ["rt/phase2a/llm_call/vl_meta"],
            "raw_response": {"id": "resp_meta"},
            "custom_tag": "should_not_persist",
        },
    )

    restored = store.load_committed_llm_response(
        stage="phase2a",
        chunk_id="unit_SU_META",
        llm_call_id=llm_call_id,
        input_fingerprint=input_fingerprint,
    )

    assert restored is not None
    assert restored["response_metadata"] == {}
    assert restored["request_scope_ids"] == ["SU_META"]

    connection = sqlite3.connect(str(db_path))
    try:
        llm_columns = [row[1] for row in connection.execute("PRAGMA table_info(llm_records)").fetchall()]
        llm_content_columns = [
            row[1]
            for row in connection.execute("PRAGMA table_info(llm_record_content)").fetchall()
        ]
    finally:
        connection.close()

    assert llm_columns == [
        "id",
        "normalized_video_key",
        "stage",
        "chunk_id",
        "llm_call_id",
        "input_fingerprint",
        "attempt",
        "status",
        "unit_id",
        "stage_step",
        "response_hash",
        "request_scope_ids_json",
        "error_class",
        "error_code",
        "error_message",
        "updated_at_ms",
        "committed_at_ms",
    ]
    assert llm_content_columns == ["llm_record_id", "response_codec", "response_payload"]


def test_runtime_recovery_store_builds_dirty_scope_plan_from_dependency_graph():
    output_dir = _make_repo_tmp_dir("test_runtime_recovery_store_builds_dirty_scope_plan_from_dependency_graph") / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-graph")

    source_scope_ref = store.build_scope_ref(
        stage="phase2a",
        scope_type="chunk_input",
        scope_id="ss000001",
        scope_variant="streaming",
    )
    chunk_scope_ref = store.build_scope_ref(
        stage="phase2a",
        scope_type="chunk",
        scope_id="ss000001",
        scope_variant="streaming",
    )
    downstream_scope_ref = store.build_scope_ref(
        stage="phase2b",
        scope_type="llm_call",
        scope_id="assemble_chunk_1",
    )

    store.upsert_scope_node(
        scope_ref=source_scope_ref,
        stage="phase2a",
        scope_type="chunk_input",
        scope_id="ss000001",
        scope_variant="streaming",
        status="COMPLETED",
        input_fingerprint="fp-source",
    )
    store.upsert_scope_node(
        scope_ref=chunk_scope_ref,
        stage="phase2a",
        scope_type="chunk",
        scope_id="ss000001",
        scope_variant="streaming",
        status="SUCCESS",
        input_fingerprint="fp-chunk",
        dependency_fingerprints={source_scope_ref: "fp-source"},
    )
    store.upsert_scope_node(
        scope_ref=downstream_scope_ref,
        stage="phase2b",
        scope_type="llm_call",
        scope_id="assemble_chunk_1",
        status="SUCCESS",
        input_fingerprint="fp-downstream",
        dependency_fingerprints={chunk_scope_ref: "fp-chunk"},
    )

    dirty_plan = store.build_dirty_scope_plan([source_scope_ref])

    assert dirty_plan["dirty_scope_refs"] == [
        source_scope_ref,
        chunk_scope_ref,
        downstream_scope_ref,
    ]
    assert dirty_plan["dirty_scope_count"] == 3
    assert dirty_plan["dirty_scope_refs_by_stage"]["phase2a"] == [
        chunk_scope_ref,
        source_scope_ref,
    ]
    assert dirty_plan["dirty_scope_refs_by_stage"]["phase2b"] == [downstream_scope_ref]


def test_runtime_recovery_store_plan_scope_reuse_blocks_restore_when_upstream_marked_dirty():
    output_dir = _make_repo_tmp_dir("test_runtime_recovery_store_plan_scope_reuse_blocks_restore_when_upstream_marked_dirty") / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-dirty-upstream")

    source_scope_ref = store.build_scope_ref(
        stage="phase2a",
        scope_type="chunk_input",
        scope_id="ss000001",
        scope_variant="streaming",
    )
    chunk_scope_ref = store.build_scope_ref(
        stage="phase2a",
        scope_type="chunk",
        scope_id="ss000001",
        scope_variant="streaming",
    )

    store.upsert_scope_node(
        scope_ref=source_scope_ref,
        stage="phase2a",
        scope_type="chunk_input",
        scope_id="ss000001",
        scope_variant="streaming",
        status="COMPLETED",
        input_fingerprint="fp-source-v1",
    )
    store.commit_chunk_payload(
        stage="phase2a",
        chunk_id="ss000001",
        input_fingerprint="fp-chunk-v1",
        result_payload={"request_updates": []},
        metadata={
            "scope_variant": "streaming",
            "dependency_fingerprints": {
                source_scope_ref: "fp-source-v1",
            },
        },
    )

    dirty_plan = store.mark_scope_dirty(
        source_scope_ref,
        reason="upstream_recomputed",
        include_descendants=True,
    )
    reuse_plan = store.plan_scope_reuse(
        scope_ref=chunk_scope_ref,
        expected_input_fingerprint="fp-chunk-v1",
        current_dependency_fingerprints={
            source_scope_ref: "fp-source-v1",
        },
    )

    assert source_scope_ref in dirty_plan["dirty_scope_refs"]
    assert chunk_scope_ref in dirty_plan["dirty_scope_refs"]
    assert reuse_plan["can_restore"] is False
    assert reuse_plan["reason"] == "scope_marked_dirty"
    assert chunk_scope_ref in reuse_plan["dirty_scope_refs"]


def test_runtime_recovery_store_prunes_old_success_attempts_after_retention_window():
    output_dir = _make_repo_tmp_dir("test_runtime_recovery_store_prunes_old_success_attempts_after_retention_window") / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-prune-success")

    input_fingerprint = build_llm_input_fingerprint(
        step_name="structured_text",
        unit_id="SU102",
        model="deepseek-chat",
        system_prompt="system",
        user_prompt="user",
    )
    llm_call_id = store.build_llm_call_id(
        step_name="structured_text",
        unit_id="SU102",
        input_fingerprint=input_fingerprint,
    )

    first_handle = store.begin_llm_attempt(
        stage="phase2b",
        chunk_id="unit_SU102",
        llm_call_id=llm_call_id,
        input_fingerprint=input_fingerprint,
        request_payload={"prompt": "user"},
        metadata={"storage_backend": "hybrid"},
    )
    store.commit_llm_attempt(
        handle=first_handle,
        response_text="first-success",
        response_metadata={"model": "deepseek-chat"},
        max_part_bytes=64,
    )
    first_commit_path = first_handle.attempt_dir / "commit.json"
    first_commit = json.loads(first_commit_path.read_text(encoding="utf-8"))
    first_commit["committed_at_ms"] = int((time.time() - 4 * 24 * 3600) * 1000)
    first_commit["cleanup_after_ms"] = first_commit["committed_at_ms"] + 72 * 3600 * 1000
    first_commit_path.write_text(json.dumps(first_commit, ensure_ascii=False, indent=2), encoding="utf-8")

    second_handle = store.begin_llm_attempt(
        stage="phase2b",
        chunk_id="unit_SU102",
        llm_call_id=llm_call_id,
        input_fingerprint=input_fingerprint,
        request_payload={"prompt": "user"},
        metadata={"storage_backend": "hybrid"},
    )
    store.commit_llm_attempt(
        handle=second_handle,
        response_text="second-success",
        response_metadata={"model": "deepseek-chat"},
        max_part_bytes=64,
    )

    remaining_attempt_dirs = store._collect_exact_attempt_dirs(
        stage="phase2b",
        chunk_id="unit_SU102",
        llm_call_id=llm_call_id,
    )
    remaining_names = sorted(path.name for path in remaining_attempt_dirs)
    assert remaining_names == [second_handle.attempt_dir.name]


def test_runtime_recovery_store_prunes_old_failure_attempts_immediately():
    output_dir = _make_repo_tmp_dir("test_runtime_recovery_store_prunes_old_failure_attempts_immediately") / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-prune-failure")

    input_fingerprint = build_llm_input_fingerprint(
        step_name="structured_text",
        unit_id="SU103",
        model="deepseek-chat",
        system_prompt="system",
        user_prompt="user",
    )
    llm_call_id = store.build_llm_call_id(
        step_name="structured_text",
        unit_id="SU103",
        input_fingerprint=input_fingerprint,
    )

    first_handle = store.begin_llm_attempt(
        stage="phase2b",
        chunk_id="unit_SU103",
        llm_call_id=llm_call_id,
        input_fingerprint=input_fingerprint,
        request_payload={"prompt": "user"},
        metadata={"storage_backend": "hybrid"},
    )
    store.fail_llm_attempt(
        handle=first_handle,
        error=RuntimeError("disk full"),
        request_snapshot={"prompt": "user"},
    )

    second_handle = store.begin_llm_attempt(
        stage="phase2b",
        chunk_id="unit_SU103",
        llm_call_id=llm_call_id,
        input_fingerprint=input_fingerprint,
        request_payload={"prompt": "user"},
        metadata={"storage_backend": "hybrid"},
    )
    store.fail_llm_attempt(
        handle=second_handle,
        error=RuntimeError("disk full again"),
        request_snapshot={"prompt": "user"},
    )

    remaining_attempt_dirs = store._collect_exact_attempt_dirs(
        stage="phase2b",
        chunk_id="unit_SU103",
        llm_call_id=llm_call_id,
    )
    remaining_names = sorted(path.name for path in remaining_attempt_dirs)
    assert remaining_names == [second_handle.attempt_dir.name]


def test_classify_runtime_error_marks_rate_limit_as_auto_retryable():
    error_info = classify_runtime_error(RuntimeError("429 rate limit exceeded"))
    assert error_info["error_class"] == "AUTO_RETRYABLE"
    assert error_info["retry_strategy"] == "AUTO_RETRY"
    assert error_info["operator_action"] == "WAIT_AUTO_RETRY"


def test_classify_runtime_error_marks_disk_full_as_manual_retry_with_hint():
    error_info = classify_runtime_error(RuntimeError("disk full"))
    assert error_info["error_class"] == "MANUAL_RETRY_REQUIRED"
    assert error_info["retry_strategy"] == "MANUAL_RETRY_AFTER_REPAIR"
    assert error_info["operator_action"] == "FREE_DISK_SPACE"


def test_markdown_enhancer_reuses_committed_llm_attempt():
    class _FakeClient:
        def __init__(self) -> None:
            self.model = "deepseek-chat"
            self.calls = 0

        async def complete_text(self, *, prompt: str, system_message: str = "", model: str = ""):
            self.calls += 1
            return (
                "复用恢复成功",
                SimpleNamespace(
                    model=model or self.model,
                    prompt_tokens=11,
                    completion_tokens=7,
                    total_tokens=18,
                    cache_hit=False,
                ),
                None,
            )

    enhancer = MarkdownEnhancer.__new__(MarkdownEnhancer)
    enhancer._llm_client = _FakeClient()
    enhancer._structured_text_model = "deepseek-chat"
    task_dir = _make_repo_tmp_dir("test_markdown_enhancer_reuses_committed_llm_attempt") / "task"
    enhancer._runtime_store = RuntimeRecoveryStore(output_dir=str(task_dir), task_id="task-md")
    enhancer._llm_trace_enabled = False
    enhancer._llm_trace_file_path = ""
    enhancer._llm_trace_level = "summary"
    enhancer._llm_trace_lock = asyncio.Lock()
    enhancer._runtime_llm_restore_cache = {}
    enhancer._runtime_stage_dispatch_summary = {}
    enhancer._runtime_pending_scope_units = set()
    enhancer._runtime_known_scope_units = set()

    async def _run_once():
        return await enhancer._execute_recoverable_llm_call(
            step_name="structured_text",
            unit_id="SU200",
            system_prompt="system",
            user_prompt="user",
            model_name="deepseek-chat",
            call_factory=lambda: enhancer._llm_client.complete_text(
                prompt="user",
                system_message="system",
                model="deepseek-chat",
            ),
        )

    first = asyncio.run(_run_once())
    second = asyncio.run(_run_once())

    assert first[0] == "复用恢复成功"
    assert second[0] == "复用恢复成功"
    assert second[2] is True
    assert enhancer._llm_client.calls == 1


def test_runtime_recovery_store_prefetch_llm_scope_cache_tracks_inflight_pending(monkeypatch):
    tmp_root = _make_repo_tmp_dir("prefetch_llm_scope_cache_tracks_inflight")
    db_path = tmp_root / "runtime_recovery.sqlite3"
    monkeypatch.setenv("TASK_RUNTIME_SQLITE_DB_PATH", str(db_path))
    output_dir = tmp_root / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-prefetch-llm")

    input_fingerprint = build_llm_input_fingerprint(
        step_name="structured_text",
        unit_id="SU210",
        model="deepseek-chat",
        system_prompt="system",
        user_prompt="user",
    )
    llm_call_id = store.build_llm_call_id(
        step_name="structured_text",
        unit_id="SU210",
        input_fingerprint=input_fingerprint,
    )
    store.begin_llm_attempt(
        stage="phase2b",
        chunk_id="unit_SU210",
        llm_call_id=llm_call_id,
        input_fingerprint=input_fingerprint,
        request_payload={"prompt": "user"},
        metadata={"step_name": "structured_text", "unit_id": "SU210"},
    )

    prefetch = store.prefetch_restorable_llm_scope_cache(
        stage="phase2b",
        candidate_chunk_ids=["unit_SU210"],
    )

    assert prefetch["summary"]["pending_count"] == 1
    assert prefetch["summary"]["prefetched_restore_count"] == 0
    assert len(prefetch["pending_hints"]) == 1
    hint = prefetch["pending_hints"][0]
    assert hint["plan_status"] == "IN_FLIGHT"
    assert hint["latest_status"] == "RUNNING"
    assert hint["unit_id"] == "SU210"
    assert hint["stage_step"] == "structured_text"


def test_runtime_recovery_store_defaults_sqlite_to_task_local_runtime_db(monkeypatch):
    monkeypatch.delenv("TASK_RUNTIME_SQLITE_DB_PATH", raising=False)
    tmp_root = _make_repo_tmp_dir("task_local_runtime_state_db_default")
    output_dir = tmp_root / "task"
    output_dir.mkdir(parents=True, exist_ok=True)

    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-local-runtime-db")

    expected_db_path = (output_dir / "intermediates" / "rt" / "runtime_state.db").resolve()
    assert store._sqlite_index is not None
    assert store._sqlite_index.db_path == expected_db_path
    assert expected_db_path.exists()
    assert (output_dir / "intermediates" / "rt" / "task_meta.json").exists()


def test_runtime_recovery_store_loads_stage_retry_context_from_sqlite_when_json_missing(monkeypatch):
    monkeypatch.delenv("TASK_RUNTIME_SQLITE_DB_PATH", raising=False)
    tmp_root = _make_repo_tmp_dir("stage_retry_context_from_sqlite")
    output_dir = tmp_root / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-stage-retry-context")

    store.update_stage_state(
        stage="phase2b",
        status="MANUAL_NEEDED",
        payload={
            "checkpoint": "llm_call_commit_pending",
            "retry_mode": "manual",
            "retry_entry_point": "phase2b/chunk-42",
            "required_action": "repair llm quota and retry",
            "error_class": "MANUAL_RETRY_REQUIRED",
            "error_message": "provider quota exhausted",
        },
    )

    stage_state_path = output_dir / "intermediates" / "rt" / "stage" / "phase2b" / "stage_state.json"
    stage_state_path.unlink(missing_ok=True)

    reopened_store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-stage-retry-context")
    retry_context = reopened_store._load_stage_retry_context("phase2b")

    assert retry_context["status"] == "MANUAL_NEEDED"
    assert retry_context["retry_mode"] == "manual"
    assert retry_context["retry_entry_point"] == "phase2b/chunk-42"
    assert retry_context["required_action"] == "repair llm quota and retry"


def test_markdown_enhancer_stage_prefetch_uses_cache(monkeypatch):
    tmp_root = _make_repo_tmp_dir("markdown_enhancer_stage_prefetch_uses_cache")
    db_path = tmp_root / "runtime_recovery.sqlite3"
    monkeypatch.setenv("TASK_RUNTIME_SQLITE_DB_PATH", str(db_path))
    task_dir = tmp_root / "task"
    task_dir.mkdir(parents=True, exist_ok=True)

    class _FailIfCalledClient:
        def __init__(self) -> None:
            self.model = "deepseek-chat"

        async def complete_text(self, *, prompt: str, system_message: str = "", model: str = ""):
            raise AssertionError("LLM client should not be called when stage prefetch cache exists")

    enhancer = MarkdownEnhancer.__new__(MarkdownEnhancer)
    enhancer._llm_client = _FailIfCalledClient()
    enhancer._structured_text_model = "deepseek-chat"
    enhancer._runtime_store = RuntimeRecoveryStore(output_dir=str(task_dir), task_id="task-md-prefetch")
    enhancer._llm_trace_enabled = False
    enhancer._llm_trace_file_path = ""
    enhancer._llm_trace_level = "summary"
    enhancer._llm_trace_lock = asyncio.Lock()
    enhancer._runtime_llm_restore_cache = {}
    enhancer._runtime_stage_dispatch_summary = {}
    enhancer._runtime_pending_scope_units = set()
    enhancer._runtime_known_scope_units = set()

    input_fingerprint = build_llm_input_fingerprint(
        step_name="structured_text",
        unit_id="SU211",
        model="deepseek-chat",
        system_prompt="system",
        user_prompt="user",
    )
    llm_call_id = enhancer._runtime_store.build_llm_call_id(
        step_name="structured_text",
        unit_id="SU211",
        input_fingerprint=input_fingerprint,
    )
    handle = enhancer._runtime_store.begin_llm_attempt(
        stage="phase2b",
        chunk_id="unit_SU211",
        llm_call_id=llm_call_id,
        input_fingerprint=input_fingerprint,
        request_payload={"prompt": "user"},
        metadata={"step_name": "structured_text", "unit_id": "SU211"},
    )
    enhancer._runtime_store.commit_llm_attempt(
        handle=handle,
        response_text="prefetched cache payload",
        response_metadata={"model": "deepseek-chat"},
    )
    enhancer._prime_phase2b_stage_dispatch([SimpleNamespace(unit_id="SU211")])

    def _should_not_load(**kwargs):
        raise AssertionError("store.load_committed_llm_response should not be called after stage prefetch")

    enhancer._runtime_store.load_committed_llm_response = _should_not_load

    restored = asyncio.run(
        enhancer._execute_recoverable_llm_call(
            step_name="structured_text",
            unit_id="SU211",
            system_prompt="system",
            user_prompt="user",
            model_name="deepseek-chat",
            call_factory=lambda: enhancer._llm_client.complete_text(
                prompt="user",
                system_message="system",
                model="deepseek-chat",
            ),
        )
    )

    assert restored[0] == "prefetched cache payload"
    assert restored[2] is True


def test_vl_generator_restores_committed_screenshot_chunk():
    tmp_path = _make_repo_tmp_dir("test_vl_generator_restores_committed_screenshot_chunk")
    video_path = tmp_path / "video.mp4"
    video_path.write_bytes(b"video")
    output_dir = tmp_path / "task"
    output_dir.mkdir(parents=True, exist_ok=True)

    generator = VLMaterialGenerator({"enabled": True, "screenshot_optimization": {}})
    generator._prepare_runtime_store_for_output_dir(str(output_dir))

    committed_request = {
        "screenshot_id": "shot_001",
        "semantic_unit_id": "SU300",
        "label": "head",
        "timestamp_sec": 9.5,
        "_optimized": True,
        "_original_timestamp": 8.0,
        "_cv_quality_score": 0.88,
        "_cv_candidate_screenshots": [{"timestamp_sec": 9.5, "score": 0.88}],
        "_cv_static_island_threshold_ms": 200.0,
    }
    committed_chunk = {
        "union_start": 7.0,
        "union_end": 10.0,
        "prefetch_profile": "default",
        "prefetch_sample_rate": 2,
        "prefetch_target_height": 360,
        "max_chunk_span_seconds": 3.0,
        "windows": [{"req": committed_request}],
    }
    generator._commit_screenshot_chunk_runtime(
        video_path=str(video_path),
        mode="batch",
        chunk_index=0,
        chunk=committed_chunk,
    )

    restored_generator = VLMaterialGenerator({"enabled": True, "screenshot_optimization": {}})
    restored_generator._prepare_runtime_store_for_output_dir(str(output_dir))
    pending_request = {
        "screenshot_id": "shot_001",
        "semantic_unit_id": "SU300",
        "label": "head",
        "timestamp_sec": 8.0,
    }
    pending_chunk = {
        "union_start": 7.0,
        "union_end": 10.0,
        "prefetch_profile": "default",
        "prefetch_sample_rate": 2,
        "prefetch_target_height": 360,
        "max_chunk_span_seconds": 3.0,
        "windows": [{"req": pending_request}],
    }

    restored = restored_generator._restore_screenshot_chunk_if_committed(
        video_path=str(video_path),
        mode="batch",
        chunk_index=0,
        chunk=pending_chunk,
    )

    assert restored is True
    assert pending_request["timestamp_sec"] == 9.5
    assert pending_request["_optimized"] is True


def test_vl_generator_stage_prefetch_uses_chunk_cache(monkeypatch):
    tmp_path = _make_repo_tmp_dir("test_vl_generator_stage_prefetch_uses_chunk_cache")
    db_path = tmp_path / "runtime_recovery.sqlite3"
    monkeypatch.setenv("TASK_RUNTIME_SQLITE_DB_PATH", str(db_path))
    video_path = tmp_path / "video.mp4"
    video_path.write_bytes(b"video")
    output_dir = tmp_path / "task"
    output_dir.mkdir(parents=True, exist_ok=True)

    generator = VLMaterialGenerator({"enabled": True, "screenshot_optimization": {}})
    generator._prepare_runtime_store_for_output_dir(str(output_dir))

    committed_request = {
        "screenshot_id": "shot_prefetch",
        "semantic_unit_id": "SU310",
        "label": "head",
        "timestamp_sec": 6.5,
        "_optimized": True,
        "_original_timestamp": 5.0,
        "_cv_quality_score": 0.91,
        "_cv_candidate_screenshots": [{"timestamp_sec": 6.5, "score": 0.91}],
        "_cv_static_island_threshold_ms": 200.0,
    }
    committed_chunk = {
        "union_start": 4.0,
        "union_end": 7.0,
        "prefetch_profile": "default",
        "prefetch_sample_rate": 2,
        "prefetch_target_height": 360,
        "max_chunk_span_seconds": 3.0,
        "windows": [{"req": committed_request}],
    }
    generator._commit_screenshot_chunk_runtime(
        video_path=str(video_path),
        mode="batch",
        chunk_index=0,
        chunk=committed_chunk,
    )

    restored_generator = VLMaterialGenerator({"enabled": True, "screenshot_optimization": {}})
    restored_generator._prepare_runtime_store_for_output_dir(str(output_dir))
    pending_request = {
        "screenshot_id": "shot_prefetch",
        "semantic_unit_id": "SU310",
        "label": "head",
        "timestamp_sec": 5.0,
    }
    pending_chunk = {
        "union_start": 4.0,
        "union_end": 7.0,
        "prefetch_profile": "default",
        "prefetch_sample_rate": 2,
        "prefetch_target_height": 360,
        "max_chunk_span_seconds": 3.0,
        "windows": [{"req": pending_request}],
    }
    restored_generator.prime_phase2a_chunk_stage_dispatch(
        video_path=str(video_path),
        chunks=[pending_chunk],
        modes=["batch"],
    )

    def _should_not_load(**kwargs):
        raise AssertionError("store.load_committed_chunk_payload should not be called after stage prefetch")

    restored_generator._runtime_store.load_committed_chunk_payload = _should_not_load

    restored = restored_generator._restore_screenshot_chunk_if_committed(
        video_path=str(video_path),
        mode="batch",
        chunk_index=0,
        chunk=pending_chunk,
    )

    assert restored is True
    assert pending_request["timestamp_sec"] == 6.5
    assert pending_request["_optimized"] is True


def test_runtime_recovery_store_restores_llm_response_from_sqlite_after_attempt_dir_removed(monkeypatch):
    tmp_root = _make_repo_tmp_dir("sqlite_llm_restore_after_dir_removed")
    db_path = tmp_root / "runtime_recovery.sqlite3"
    monkeypatch.setenv("TASK_RUNTIME_SQLITE_DB_PATH", str(db_path))
    output_dir = tmp_root / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-sqlite-llm")

    input_fingerprint = build_llm_input_fingerprint(
        step_name="structured_text",
        unit_id="SU400",
        model="deepseek-chat",
        system_prompt="system",
        user_prompt="user",
    )
    llm_call_id = store.build_llm_call_id(
        step_name="structured_text",
        unit_id="SU400",
        input_fingerprint=input_fingerprint,
    )
    handle = store.begin_llm_attempt(
        stage="phase2b",
        chunk_id="unit_SU400",
        llm_call_id=llm_call_id,
        input_fingerprint=input_fingerprint,
        request_payload={"prompt": "user"},
    )
    store.commit_llm_attempt(
        handle=handle,
        response_text="sqlite restore payload",
        response_metadata={"model": "deepseek-chat"},
        max_part_bytes=64,
    )
    shutil.rmtree(handle.attempt_dir, ignore_errors=True)

    restored = store.load_committed_llm_response(
        stage="phase2b",
        chunk_id="unit_SU400",
        llm_call_id=llm_call_id,
        input_fingerprint=input_fingerprint,
    )

    assert restored is not None
    assert restored["response_text"] == "sqlite restore payload"
    assert restored.get("source") == "sqlite"


def test_runtime_recovery_store_restores_chunk_payload_from_sqlite_after_chunk_dir_removed(monkeypatch):
    tmp_root = _make_repo_tmp_dir("sqlite_chunk_restore_after_dir_removed")
    db_path = tmp_root / "runtime_recovery.sqlite3"
    monkeypatch.setenv("TASK_RUNTIME_SQLITE_DB_PATH", str(db_path))
    output_dir = tmp_root / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-sqlite-chunk")

    payload = {"request_updates": [{"request_key": "id:shot_002", "_optimized": True}]}
    commit_payload = store.commit_chunk_payload(
        stage="phase2a",
        chunk_id="ss000888",
        input_fingerprint="fp-sqlite-chunk",
        result_payload=payload,
        metadata={"mode": "streaming"},
    )
    chunk_dir = output_dir / "intermediates" / "rt" / "stage" / "phase2a" / "chunk" / "ss000888"
    shutil.rmtree(chunk_dir, ignore_errors=True)

    restored = store.load_committed_chunk_payload(
        stage="phase2a",
        chunk_id="ss000888",
        input_fingerprint="fp-sqlite-chunk",
    )

    assert commit_payload["status"] == "SUCCESS"
    assert restored is not None
    assert restored["result_payload"] == payload
    assert restored.get("source") == "sqlite"


def test_runtime_recovery_store_sqlite_mirror_compresses_large_llm_payloads(monkeypatch):
    tmp_root = _make_repo_tmp_dir("sqlite_llm_compress_large_payloads")
    db_path = tmp_root / "runtime_recovery.sqlite3"
    monkeypatch.setenv("TASK_RUNTIME_SQLITE_DB_PATH", str(db_path))
    monkeypatch.setenv("TASK_RUNTIME_SQLITE_COMPRESSION_MIN_BYTES", "16")
    output_dir = tmp_root / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-sqlite-compress")

    input_fingerprint = build_llm_input_fingerprint(
        step_name="structured_text",
        unit_id="SU401",
        model="deepseek-chat",
        system_prompt="system",
        user_prompt="user",
    )
    llm_call_id = store.build_llm_call_id(
        step_name="structured_text",
        unit_id="SU401",
        input_fingerprint=input_fingerprint,
    )
    handle = store.begin_llm_attempt(
        stage="phase2b",
        chunk_id="unit_SU401",
        llm_call_id=llm_call_id,
        input_fingerprint=input_fingerprint,
        request_payload={"prompt": "user"},
    )
    large_response = "repeat-this-payload-" * 200
    store.commit_llm_attempt(
        handle=handle,
        response_text=large_response,
        response_metadata={"model": "deepseek-chat"},
        max_part_bytes=256,
    )

    with sqlite3.connect(str(db_path)) as connection:
        row = connection.execute(
            """
            SELECT c.response_codec
            FROM llm_records m
            JOIN llm_record_content c ON c.llm_record_id = m.id
            WHERE m.stage = ? AND m.chunk_id = ? AND m.llm_call_id = ?
            ORDER BY m.attempt DESC
            LIMIT 1
            """,
            ("phase2b", "unit_SU401", llm_call_id),
        ).fetchone()

    assert row is not None
    assert "+zlib" in str(row[0] or "")


def test_runtime_recovery_sqlite_task_meta_keeps_only_scalar_recovery_fields():
    tmp_root = _make_repo_tmp_dir("sqlite_task_meta_scalar_only")
    db_path = tmp_root / "runtime_recovery.sqlite3"
    index = RuntimeRecoverySqliteIndex(db_path=str(db_path))

    index.refresh_task_meta(
        output_dir=str((tmp_root / "task").resolve()),
        task_id="task-meta",
        storage_key="storage-meta",
        normalized_video_key="video-meta",
        payload={
            "schema_version": "runtime_task_meta_v1",
            "custom_tag": "should_not_persist",
            "llm_audit_payload": {"prompt": "should_not_persist"},
        },
    )
    loaded = index.load_task_meta(output_dir=str((tmp_root / "task").resolve()))

    assert loaded == {
        "schema_version": "runtime_task_meta_v1",
        "output_dir": str((tmp_root / "task").resolve()),
        "normalized_video_key": "video-meta",
        "updated_at_ms": loaded["updated_at_ms"],
    }

    connection = sqlite3.connect(str(db_path))
    try:
        task_meta_columns = [row[1] for row in connection.execute("PRAGMA table_info(task_meta)").fetchall()]
    finally:
        connection.close()

    assert task_meta_columns == [
        "singleton_key",
        "normalized_video_key",
        "schema_version",
        "updated_at_ms",
    ]


def test_runtime_recovery_store_reinitializes_missing_task_tables_for_shared_sqlite_instance():
    output_dir = _make_repo_tmp_dir("sqlite_shared_instance_missing_tables") / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-missing-tables")

    db_path = output_dir / "intermediates" / "rt" / "runtime_state.db"
    with sqlite3.connect(str(db_path)) as connection:
        connection.execute("DROP TABLE IF EXISTS stage_snapshots")
        connection.execute("DROP TABLE IF EXISTS scope_nodes")
        connection.commit()

    reopened_store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-missing-tables")
    reopened_store.update_stage_state(
        stage="phase2a",
        status="RUNNING",
        payload={"checkpoint": "phase2a_segmentation_running"},
    )
    scope_ref = reopened_store.build_scope_ref(
        stage="phase2a",
        scope_type="substage",
        scope_id="semantic_units_build.wave_0001",
    )
    reopened_store.upsert_scope_node(
        scope_ref=scope_ref,
        stage="phase2a",
        scope_type="substage",
        scope_id="semantic_units_build.wave_0001",
        status="RUNNING",
        input_fingerprint="fp-phase2a-wave-1",
    )

    restored_scope = reopened_store.load_scope_node(scope_ref)

    connection = sqlite3.connect(str(db_path))
    try:
        restored_tables = {
            row[0]
            for row in connection.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'table' AND name IN ('stage_snapshots', 'scope_nodes')
                """
            ).fetchall()
        }
    finally:
        connection.close()

    assert restored_scope is not None
    assert restored_scope["status"] == "RUNNING"
    assert restored_tables == {"stage_snapshots", "scope_nodes"}


def test_runtime_recovery_store_exposes_sqlite_batch_search_and_batch_load(monkeypatch):
    tmp_root = _make_repo_tmp_dir("sqlite_batch_search_and_load")
    db_path = tmp_root / "runtime_recovery.sqlite3"
    monkeypatch.setenv("TASK_RUNTIME_SQLITE_DB_PATH", str(db_path))
    output_dir = tmp_root / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-sqlite-batch")

    requests = []
    for index in range(2):
        input_fingerprint = build_llm_input_fingerprint(
            step_name="structured_text",
            unit_id=f"SU50{index}",
            model="deepseek-chat",
            system_prompt="system",
            user_prompt=f"user-{index}",
        )
        llm_call_id = store.build_llm_call_id(
            step_name="structured_text",
            unit_id=f"SU50{index}",
            input_fingerprint=input_fingerprint,
        )
        handle = store.begin_llm_attempt(
            stage="phase2b",
            chunk_id=f"unit_SU50{index}",
            llm_call_id=llm_call_id,
            input_fingerprint=input_fingerprint,
            request_payload={"prompt": f"user-{index}"},
        )
        store.commit_llm_attempt(
            handle=handle,
            response_text=f"payload-{index}",
            response_metadata={"model": "deepseek-chat"},
            max_part_bytes=64,
        )
        requests.append(
            {
                "stage": "phase2b",
                "chunk_id": f"unit_SU50{index}",
                "llm_call_id": llm_call_id,
                "input_fingerprint": input_fingerprint,
            }
        )

    llm_rows = store.list_sqlite_llm_records(stage="phase2b", status="SUCCESS", limit=10)
    llm_payloads = store.batch_load_committed_llm_responses(requests)

    chunk_requests = []
    for index in range(2):
        store.commit_chunk_payload(
            stage="phase2a",
            chunk_id=f"ss9000{index}",
            input_fingerprint=f"fp-chunk-batch-{index}",
            result_payload={"request_updates": [{"request_key": f"id:shot_{index}", "_optimized": True}]},
            metadata={"mode": "batch"},
        )
        chunk_requests.append(
            {
                "stage": "phase2a",
                "chunk_id": f"ss9000{index}",
                "input_fingerprint": f"fp-chunk-batch-{index}",
            }
        )
    chunk_rows = store.list_sqlite_chunk_records(stage="phase2a", status="SUCCESS", limit=10)
    chunk_payloads = store.batch_load_committed_chunk_payloads(chunk_requests)

    assert len(llm_rows) >= 2
    assert [item["restored"]["response_text"] for item in llm_payloads] == ["payload-0", "payload-1"]
    assert len(chunk_rows) >= 2
    assert [
        item["restored"]["result_payload"]["request_updates"][0]["request_key"]
        for item in chunk_payloads
    ] == ["id:shot_0", "id:shot_1"]


def test_runtime_recovery_store_sqlite_mirror_handles_concurrent_llm_writes_without_queue(monkeypatch):
    tmp_root = _make_repo_tmp_dir("sqlite_concurrent_llm_writes")
    db_path = tmp_root / "runtime_recovery.sqlite3"
    monkeypatch.setenv("TASK_RUNTIME_SQLITE_DB_PATH", str(db_path))
    monkeypatch.setenv("TASK_RUNTIME_SQLITE_BUSY_TIMEOUT_MS", "20000")
    output_dir = tmp_root / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-sqlite-concurrency")
    assert store._sqlite_index is not None

    def _write_row(index: int) -> None:
        manifest_payload = {
            "status": "SUCCESS",
            "created_at_ms": int(time.time() * 1000),
            "updated_at_ms": int(time.time() * 1000),
            "response_metadata": {"model": "deepseek-chat"},
            "unit_id": f"SU60{index}",
            "request_name": "structured_text",
        }
        commit_payload = {
            "status": "SUCCESS",
            "attempt": 1,
            "response_hash": hashlib.sha256(f"payload-{index}".encode("utf-8")).hexdigest(),
            "committed_parts": 1,
            "final_bytes": len(f"payload-{index}".encode("utf-8")),
            "committed_at_ms": int(time.time() * 1000),
        }
        store._sqlite_index.record_llm_attempt_committed(
            output_dir=str(output_dir.resolve()),
            task_id=store.task_id,
            storage_key=store.storage_key,
            normalized_video_key=store.normalized_video_key,
            stage="phase2b",
            chunk_id=f"unit_SU60{index}",
            llm_call_id=f"structured_text.su60{index}.h{index:02d}",
            input_fingerprint=f"fp-concurrent-{index}",
            attempt=1,
            request_payload={"prompt": f"user-{index}"},
            manifest_payload=manifest_payload,
            commit_payload=commit_payload,
            response_text=f"payload-{index}",
            attempt_dir=str(output_dir / f"attempt_{index}"),
            manifest_path=str(output_dir / f"attempt_{index}" / "manifest.json"),
            commit_path=str(output_dir / f"attempt_{index}" / "commit.json"),
        )

    with ThreadPoolExecutor(max_workers=6) as executor:
        list(executor.map(_write_row, range(8)))

    rows = store.list_sqlite_llm_records(stage="phase2b", status="SUCCESS", limit=20)
    assert len(rows) >= 8


def test_runtime_recovery_sqlite_reuses_single_write_connection_per_process(monkeypatch):
    tmp_root = _make_repo_tmp_dir("sqlite_single_write_connection")
    db_path = tmp_root / "runtime_recovery.sqlite3"

    import services.python_grpc.src.common.utils.runtime_recovery_sqlite as sqlite_module

    real_connect = sqlite_module.sqlite3.connect
    write_connect_calls = {"count": 0}

    def _counting_connect(*args, **kwargs):
        if kwargs.get("check_same_thread") is False:
            write_connect_calls["count"] += 1
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(sqlite_module.sqlite3, "connect", _counting_connect)

    index = RuntimeRecoverySqliteIndex(db_path=str(db_path))
    index.record_llm_attempt_started(
        output_dir=str(tmp_root / "task"),
        task_id="task-write-conn",
        storage_key="task-write-conn",
        normalized_video_key="",
        stage="phase2b",
        chunk_id="unit_SU950",
        llm_call_id="structured_text.su950.h01",
        input_fingerprint="fp-write-conn-1",
        attempt=1,
        request_payload={"prompt": "user-1"},
        manifest_payload={
            "status": "RUNNING",
            "created_at_ms": int(time.time() * 1000),
            "updated_at_ms": int(time.time() * 1000),
        },
        attempt_dir=str(tmp_root / "attempt_1"),
        manifest_path=str(tmp_root / "attempt_1" / "manifest.json"),
    )
    index.record_llm_attempt_committed(
        output_dir=str(tmp_root / "task"),
        task_id="task-write-conn",
        storage_key="task-write-conn",
        normalized_video_key="",
        stage="phase2b",
        chunk_id="unit_SU951",
        llm_call_id="structured_text.su951.h02",
        input_fingerprint="fp-write-conn-2",
        attempt=1,
        request_payload={"prompt": "user-2"},
        manifest_payload={
            "status": "SUCCESS",
            "created_at_ms": int(time.time() * 1000),
            "updated_at_ms": int(time.time() * 1000),
            "response_metadata": {"model": "deepseek-chat"},
        },
        commit_payload={
            "status": "SUCCESS",
            "attempt": 1,
            "response_hash": hashlib.sha256(b"payload-2").hexdigest(),
            "committed_parts": 1,
            "final_bytes": len(b"payload-2"),
            "committed_at_ms": int(time.time() * 1000),
        },
        response_text="payload-2",
        attempt_dir=str(tmp_root / "attempt_2"),
        manifest_path=str(tmp_root / "attempt_2" / "manifest.json"),
        commit_path=str(tmp_root / "attempt_2" / "commit.json"),
    )

    assert write_connect_calls["count"] == 1


def test_runtime_recovery_store_scope_hints_show_manual_retry_pending_llm(monkeypatch):
    tmp_root = _make_repo_tmp_dir("sqlite_scope_hint_manual_retry")
    db_path = tmp_root / "runtime_recovery.sqlite3"
    monkeypatch.setenv("TASK_RUNTIME_SQLITE_DB_PATH", str(db_path))
    output_dir = tmp_root / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-scope-manual")

    input_fingerprint = build_llm_input_fingerprint(
        step_name="structured_text",
        unit_id="SU700",
        model="deepseek-chat",
        system_prompt="system",
        user_prompt="user",
    )
    llm_call_id = store.build_llm_call_id(
        step_name="structured_text",
        unit_id="SU700",
        input_fingerprint=input_fingerprint,
    )
    handle = store.begin_llm_attempt(
        stage="phase2b",
        chunk_id="unit_SU700",
        llm_call_id=llm_call_id,
        input_fingerprint=input_fingerprint,
        request_payload={"prompt": "user"},
    )
    store.fail_llm_attempt(
        handle=handle,
        error=RuntimeError("insufficient credits"),
        request_snapshot={"prompt": "user"},
    )

    reopened_store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-scope-manual")
    pending = reopened_store.list_pending_scope_hints(stage="phase2b", scope_type="llm_call", retry_mode="manual", limit=20)

    assert any(item["llm_call_id"] == llm_call_id for item in pending)
    target = next(item for item in pending if item["llm_call_id"] == llm_call_id)
    assert target["latest_status"] == "MANUAL_NEEDED"
    assert target["retry_mode"] == "manual"
    assert target["plan_status"] == "MANUAL_REPAIR_REQUIRED"


def test_runtime_recovery_store_scope_hints_show_fallback_retry_pending_dirty_scope(monkeypatch):
    tmp_root = _make_repo_tmp_dir("sqlite_scope_hint_fallback_retry")
    db_path = tmp_root / "runtime_recovery.sqlite3"
    monkeypatch.setenv("TASK_RUNTIME_SQLITE_DB_PATH", str(db_path))
    output_dir = tmp_root / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-scope-fallback")

    llm_scope_ref = store.build_scope_ref(
        stage="phase2b",
        scope_type="llm_call",
        scope_id="assemble_chunk_9",
    )
    store.upsert_scope_node(
        scope_ref=llm_scope_ref,
        stage="phase2b",
        scope_type="llm_call",
        scope_id="assemble_chunk_9",
        status="SUCCESS",
        input_fingerprint="fp-scope-fallback",
        local_path=str(output_dir / "attempt_scope"),
        extra_payload={"chunk_id": "unit_scope_fallback", "attempt": 1},
    )
    store.update_stage_state(
        stage="phase2b",
        status="MANUAL_NEEDED",
        payload={
            "checkpoint": "fallback_repair_requested",
            "retry_mode": "manual",
            "retry_entry_point": "fallback_repair:phase2b",
            "required_action": "先修复 fallback 根因，再从当前阶段重试。",
        },
    )
    store.mark_scope_dirty(
        llm_scope_ref,
        reason="fallback_repair_requested",
        include_descendants=True,
    )

    reopened_store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-scope-fallback")
    pending = reopened_store.list_pending_scope_hints(stage="phase2b", scope_type="llm_call", retry_mode="fallback", limit=20)

    assert any(item["scope_ref"] == llm_scope_ref for item in pending)
    target = next(item for item in pending if item["scope_ref"] == llm_scope_ref)
    assert target["latest_status"] == "DIRTY"
    assert target["retry_mode"] == "fallback"
    assert target["plan_status"] == "FALLBACK_RETRY_PENDING"
    assert target["retry_entry_point"] == "fallback_repair:phase2b"


def test_runtime_recovery_store_runtime_indexes_default_to_task_sqlite_only():
    output_dir = _make_repo_tmp_dir("test_runtime_indexes_sqlite_only") / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-runtime-indexes")

    source_scope_ref = store.build_scope_ref(
        stage="phase2a",
        scope_type="chunk_input",
        scope_id="ss000101",
        scope_variant="streaming",
    )
    chunk_scope_ref = store.build_scope_ref(
        stage="phase2a",
        scope_type="chunk",
        scope_id="ss000101",
        scope_variant="streaming",
    )
    store.upsert_scope_node(
        scope_ref=source_scope_ref,
        stage="phase2a",
        scope_type="chunk_input",
        scope_id="ss000101",
        scope_variant="streaming",
        status="COMPLETED",
        input_fingerprint="fp-stage1-input",
    )
    store.upsert_scope_node(
        scope_ref=chunk_scope_ref,
        stage="phase2a",
        scope_type="chunk",
        scope_id="ss000101",
        scope_variant="streaming",
        status="SUCCESS",
        input_fingerprint="fp-phase2a-chunk",
        dependency_fingerprints={source_scope_ref: "fp-stage1-input"},
    )
    store.append_stage_journal_event(
        stage="phase2a",
        event="checkpoint",
        checkpoint="phase2a_running",
        status="RUNNING",
        payload={"completed": 1, "pending": 2, "message": "phase2a running"},
    )
    store.write_stage_outputs_manifest(
        stage="phase2a",
        payload={"artifacts": {"semantic_units_path": "semantic_units.json"}},
    )

    reopened_store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-runtime-indexes")
    dirty_plan = reopened_store.build_dirty_scope_plan([source_scope_ref])

    runtime_root = output_dir / "intermediates" / "rt"
    assert not (runtime_root / "scope_graph.json").exists()
    assert not (runtime_root / "stage" / "phase2a" / "stage_journal.jsonl").exists()
    assert not (runtime_root / "stage" / "phase2a" / "outputs_manifest.json").exists()
    assert chunk_scope_ref in dirty_plan["dirty_scope_refs"]

    connection = sqlite3.connect(str(store.runtime_state_db_path))
    try:
        scope_node_count = connection.execute("SELECT COUNT(*) FROM scope_nodes").fetchone()[0]
        dependency_edge_count = connection.execute(
            "SELECT COUNT(*) FROM scope_edges"
        ).fetchone()[0]
        dropped_tables = {
            row[0]
            for row in connection.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'table' AND name IN ('stage_journal_events', 'stage_outputs_manifests')
                """
            ).fetchall()
        }
    finally:
        connection.close()

    assert scope_node_count >= 2
    assert dependency_edge_count >= 1
    assert dropped_tables == set()


def test_runtime_recovery_store_substage_scope_state_machine_roundtrip():
    output_dir = _make_repo_tmp_dir("test_substage_scope_state_machine") / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-substage-state")

    planned = store.plan_substage_scope(
        stage="stage1",
        substage_name="step2_correction",
        wave_id="wave_0003",
        input_fingerprint="fp-stage1-step2-wave3",
        dependency_fingerprints={"stage1/input/root": "fp-stage1-input"},
        plan_context={
            "llm_calls": [
                {"llm_call_id": "stage1_step2.wave3.batch_001", "request_scope_ids": ["SU001", "SU002"]},
                {"llm_call_id": "stage1_step2.wave3.batch_002", "request_scope_ids": ["SU003"]},
            ],
            "chunk_units": [],
        },
    )

    assert planned["scope_type"] == "substage"
    assert planned["status"] == "PLANNED"
    assert planned["stage_step"] == "step2_correction"
    assert planned["plan_context"]["wave_id"] == "wave_0003"

    running = store.transition_scope_node(
        scope_ref=planned["scope_ref"],
        stage="stage1",
        scope_type="substage",
        scope_id=planned["scope_id"],
        scope_variant=str(planned.get("scope_variant", "") or ""),
        status="RUNNING",
        input_fingerprint="fp-stage1-step2-wave3",
        dependency_fingerprints={"stage1/input/root": "fp-stage1-input"},
        attempt_count=1,
    )
    assert running["status"] == "RUNNING"
    assert running["attempt_count"] == 1

    manual_needed = store.transition_scope_node(
        scope_ref=planned["scope_ref"],
        stage="stage1",
        scope_type="substage",
        scope_id=planned["scope_id"],
        scope_variant=str(planned.get("scope_variant", "") or ""),
        status="MANUAL_NEEDED",
        input_fingerprint="fp-stage1-step2-wave3",
        dependency_fingerprints={"stage1/input/root": "fp-stage1-input"},
        attempt_count=1,
        retry_mode="manual",
        retry_entry_point="stage1.step2_correction.wave_0003",
        required_action="人工修复提示词或输入后，重试该 wave。",
        error_class="PROMPT_SCHEMA_ERROR",
        error_code="PROMPT_SCHEMA_MISMATCH",
        error_message="LLM 返回结构缺字段。",
        resource_snapshot={"python_workers": 2, "rss_mb": 1536, "gpu_mem_mb": 0},
    )

    assert manual_needed["status"] == "MANUAL_NEEDED"
    assert manual_needed["error_code"] == "PROMPT_SCHEMA_MISMATCH"
    assert manual_needed["resource_snapshot"]["rss_mb"] == 1536

    reopened_store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-substage-state")
    reopened_manual = reopened_store.load_scope_node(planned["scope_ref"])
    assert reopened_manual is not None
    assert reopened_manual["status"] == "MANUAL_NEEDED"
    assert reopened_manual["plan_context"]["llm_calls"][0]["llm_call_id"] == "stage1_step2.wave3.batch_001"
    assert reopened_manual["resource_snapshot"]["python_workers"] == 2

    requeued = reopened_store.requeue_scope_node(
        planned["scope_ref"],
        reason="manual_fixed",
        plan_context={"operator_ticket": "INC-20260317-01"},
    )
    assert requeued is not None
    assert requeued["status"] == "PLANNED"
    assert requeued["error_class"] == ""
    assert requeued["plan_context"]["operator_ticket"] == "INC-20260317-01"
    assert requeued["resource_snapshot"]["last_requeue_reason"] == "manual_fixed"

    success = reopened_store.transition_scope_node(
        scope_ref=planned["scope_ref"],
        stage="stage1",
        scope_type="substage",
        scope_id=planned["scope_id"],
        scope_variant=str(planned.get("scope_variant", "") or ""),
        status="SUCCESS",
        input_fingerprint="fp-stage1-step2-wave3",
        dependency_fingerprints={"stage1/input/root": "fp-stage1-input"},
        attempt_count=2,
        result_hash="result-stage1-step2-wave3",
    )
    assert success["status"] == "SUCCESS"
    assert success["result_hash"] == "result-stage1-step2-wave3"

    connection = sqlite3.connect(str(store.runtime_state_db_path))
    connection.row_factory = sqlite3.Row
    try:
        row = connection.execute(
            """
            SELECT status, plan_context_json, resource_snapshot_json, attempt_count, result_hash
            FROM scope_nodes
            WHERE scope_ref = ?
            """,
            (planned["scope_ref"],),
        ).fetchone()
    finally:
        connection.close()

    assert row is not None
    assert row["status"] == "SUCCESS"
    assert json.loads(row["plan_context_json"])["wave_id"] == "wave_0003"
    assert json.loads(row["resource_snapshot_json"])["last_requeue_reason"] == "manual_fixed"
    assert int(row["attempt_count"]) == 2
    assert row["result_hash"] == "result-stage1-step2-wave3"


def test_runtime_recovery_store_reset_running_scopes_to_planned():
    output_dir = _make_repo_tmp_dir("test_reset_running_scopes_to_planned") / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-reset-running")

    planned = store.plan_substage_scope(
        stage="phase2a",
        substage_name="vl_analysis",
        wave_id="wave_0002",
        input_fingerprint="fp-phase2a-vl-wave2",
        plan_context={"unit_ids": ["SU101", "SU102"]},
    )
    store.transition_scope_node(
        scope_ref=planned["scope_ref"],
        stage="phase2a",
        scope_type="substage",
        scope_id=planned["scope_id"],
        status="RUNNING",
        input_fingerprint="fp-phase2a-vl-wave2",
        attempt_count=1,
    )

    affected = store.reset_running_scopes_to_planned(stage="phase2a", scope_type="substage", reason="process_restarted")

    assert planned["scope_ref"] in affected
    restored = store.load_scope_node(planned["scope_ref"])
    assert restored is not None
    assert restored["status"] == "PLANNED"
    assert restored["resource_snapshot"]["interrupted_status"] == "RUNNING"
    assert restored["plan_context"]["requeue_reason"] == "process_restarted"


def test_runtime_recovery_store_chunk_scope_uses_canonical_current_statuses():
    output_dir = _make_repo_tmp_dir("test_chunk_scope_canonical_statuses") / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-chunk-scope")

    metadata = {
        "storage_backend": "sqlite",
        "scope_variant": "segment",
        "stage_step": "transcript.segment_dispatch",
        "substage_name": "segment_dispatch",
        "wave_id": "wave_0001",
    }

    store.record_chunk_state(
        stage="transcribe",
        chunk_id="ts000001",
        input_fingerprint="fp-transcribe-segment-1",
        status="PLANNING",
        metadata=metadata,
    )
    planned_scope = store.load_scope_node("transcribe/chunk/ts000001@segment")
    assert planned_scope is not None
    assert planned_scope["status"] == "PLANNED"

    store.record_chunk_state(
        stage="transcribe",
        chunk_id="ts000001",
        input_fingerprint="fp-transcribe-segment-1",
        status="RUNNING",
        metadata=metadata,
    )
    running_scope = store.load_scope_node("transcribe/chunk/ts000001@segment")
    assert running_scope is not None
    assert running_scope["status"] == "RUNNING"

    commit_payload = store.commit_chunk_payload(
        stage="transcribe",
        chunk_id="ts000001",
        input_fingerprint="fp-transcribe-segment-1",
        result_payload={"subtitles": [{"id": 1, "text": "hello"}]},
        metadata=metadata,
    )
    success_scope = store.load_scope_node("transcribe/chunk/ts000001@segment")
    assert success_scope is not None
    assert success_scope["status"] == "SUCCESS"
    assert success_scope["result_hash"] == commit_payload["result_hash"]

    store.fail_chunk_payload(
        stage="transcribe",
        chunk_id="ts000002",
        input_fingerprint="fp-transcribe-segment-2",
        error=TimeoutError("timed out while transcribing"),
        metadata=metadata,
    )
    error_scope = store.load_scope_node("transcribe/chunk/ts000002@segment")
    assert error_scope is not None
    assert error_scope["status"] == "ERROR"

    store.fail_chunk_payload(
        stage="transcribe",
        chunk_id="ts000003",
        input_fingerprint="fp-transcribe-segment-3",
        error=ValueError("invalid argument for transcribe segment"),
        metadata=metadata,
    )
    failed_scope = store.load_scope_node("transcribe/chunk/ts000003@segment")
    assert failed_scope is not None
    assert failed_scope["status"] == "FAILED"


def test_runtime_recovery_store_llm_scope_uses_canonical_current_statuses():
    output_dir = _make_repo_tmp_dir("test_llm_scope_canonical_statuses") / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-llm-scope")

    metadata = {
        "storage_backend": "sqlite",
        "provider": "deepseek",
        "request_name": "section_enhance",
        "stage_step": "phase2b.section_enhance",
        "scope_variant": "section",
        "unit_id": "SEC001",
        "substage_name": "section_enhance",
        "wave_id": "wave_0001",
    }
    request_payload = {"request_scope_ids": ["SEC001"]}

    running_handle = store.begin_llm_attempt(
        stage="phase2b",
        chunk_id="section.wave_0001",
        llm_call_id="section_enhance.call_001",
        input_fingerprint="fp-phase2b-sec001",
        request_payload=request_payload,
        metadata=metadata,
    )
    running_scope = store.load_scope_node("phase2b/llm_call/section_enhance.call_001@section")
    assert running_scope is not None
    assert running_scope["status"] == "RUNNING"
    assert running_scope["plan_context"]["request_scope_ids"] == ["SEC001"]

    store.commit_llm_attempt(
        handle=running_handle,
        response_text="ok",
        response_metadata={"usage": {"total_tokens": 10}},
    )
    success_scope = store.load_scope_node("phase2b/llm_call/section_enhance.call_001@section")
    assert success_scope is not None
    assert success_scope["status"] == "SUCCESS"
    assert success_scope["result_hash"]

    manual_handle = store.begin_llm_attempt(
        stage="phase2b",
        chunk_id="section.wave_0001",
        llm_call_id="section_enhance.call_002",
        input_fingerprint="fp-phase2b-sec002",
        request_payload={"request_scope_ids": ["SEC002"]},
        metadata=metadata,
    )
    store.fail_llm_attempt(
        handle=manual_handle,
        error=MemoryError("OOM while calling llm"),
        request_snapshot={"request_scope_ids": ["SEC002"]},
    )
    manual_scope = store.load_scope_node("phase2b/llm_call/section_enhance.call_002@section")
    assert manual_scope is not None
    assert manual_scope["status"] == "MANUAL_NEEDED"

    error_handle = store.begin_llm_attempt(
        stage="phase2b",
        chunk_id="section.wave_0001",
        llm_call_id="section_enhance.call_003",
        input_fingerprint="fp-phase2b-sec003",
        request_payload={"request_scope_ids": ["SEC003"]},
        metadata=metadata,
    )
    store.fail_llm_attempt(
        handle=error_handle,
        error=TimeoutError("timed out while calling llm"),
        request_snapshot={"request_scope_ids": ["SEC003"]},
    )
    error_scope = store.load_scope_node("phase2b/llm_call/section_enhance.call_003@section")
    assert error_scope is not None
    assert error_scope["status"] == "ERROR"
