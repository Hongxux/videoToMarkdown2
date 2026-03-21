from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from services.python_grpc.src.common.utils.runtime_recovery_store import (  # noqa: E402
    RuntimeRecoveryStore,
    build_llm_input_fingerprint,
)


def _build_fixture(temp_root: Path) -> Dict[str, Any]:
    os.environ["TASK_RUNTIME_SQLITE_DB_PATH"] = str(temp_root / "runtime_recovery.sqlite3")
    output_dir = temp_root / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-eqp")

    llm_requests: List[Dict[str, str]] = []
    for index in range(3):
        unit_id = f"SU{index:03d}"
        input_fingerprint = build_llm_input_fingerprint(
            step_name="structured_text",
            unit_id=unit_id,
            model="deepseek-chat",
            system_prompt="system",
            user_prompt=f"user-{index}",
        )
        llm_call_id = store.build_llm_call_id(
            step_name="structured_text",
            unit_id=unit_id,
            input_fingerprint=input_fingerprint,
        )
        handle = store.begin_llm_attempt(
            stage="phase2b",
            chunk_id=f"unit_{unit_id}",
            llm_call_id=llm_call_id,
            input_fingerprint=input_fingerprint,
            request_payload={"prompt": f"user-{index}"},
            metadata={"step_name": "structured_text", "unit_id": unit_id},
        )
        store.commit_llm_attempt(
            handle=handle,
            response_text=f"payload-{index}",
            response_metadata={"model": "deepseek-chat"},
        )
        llm_requests.append(
            {
                "stage": "phase2b",
                "chunk_id": f"unit_{unit_id}",
                "llm_call_id": llm_call_id,
                "input_fingerprint": input_fingerprint,
            }
        )

    chunk_requests: List[Dict[str, str]] = []
    for index in range(3):
        chunk_id = f"ss{index:06d}"
        input_fingerprint = f"fp-chunk-{index}"
        store.commit_chunk_payload(
            stage="phase2a",
            chunk_id=chunk_id,
            input_fingerprint=input_fingerprint,
            result_payload={"request_updates": [{"request_key": f"id:{index}"}]},
            metadata={"mode": "batch", "scope_variant": "batch"},
        )
        chunk_requests.append(
            {
                "stage": "phase2a",
                "chunk_id": chunk_id,
                "input_fingerprint": input_fingerprint,
            }
        )

    dirty_scope_ref = store.build_scope_ref(
        stage="phase2b",
        scope_type="llm_call",
        scope_id=llm_requests[0]["llm_call_id"],
    )
    store.mark_scope_dirty(dirty_scope_ref, reason="fallback_repair", include_descendants=False)

    return {
        "store": store,
        "llm_requests": llm_requests,
        "chunk_requests": chunk_requests,
    }


def _explain(connection, query: str, params: List[Any]) -> List[str]:
    rows = connection.execute("EXPLAIN QUERY PLAN " + query, tuple(params)).fetchall()
    return [str(row[3] or "") for row in rows]


def collect_restore_query_plans() -> Dict[str, List[str]]:
    temp_root = Path(tempfile.mkdtemp(prefix="runtime_recovery_eqp_", dir=str(REPO_ROOT / "var")))
    fixture = _build_fixture(temp_root)
    store: RuntimeRecoveryStore = fixture["store"]
    llm_requests: List[Dict[str, str]] = fixture["llm_requests"]
    chunk_requests: List[Dict[str, str]] = fixture["chunk_requests"]
    connection = store._sqlite_index._connect()
    try:
        llm_request = llm_requests[0]
        chunk_request = chunk_requests[0]

        llm_batch_value_sql: List[str] = []
        llm_batch_params: List[Any] = []
        for request_index, request in enumerate(llm_requests):
            llm_batch_value_sql.append("(?, ?, ?, ?, ?, ?)")
            llm_batch_params.extend(
                [
                    int(request_index),
                    str(store.output_dir),
                    str(request["stage"]),
                    str(request["chunk_id"]),
                    str(request["llm_call_id"]),
                    str(request["input_fingerprint"]),
                ]
            )

        chunk_batch_value_sql: List[str] = []
        chunk_batch_params: List[Any] = []
        for request_index, request in enumerate(chunk_requests):
            chunk_batch_value_sql.append("(?, ?, ?, ?, ?)")
            chunk_batch_params.extend(
                [
                    int(request_index),
                    str(store.output_dir),
                    str(request["stage"]),
                    str(request["chunk_id"]),
                    str(request["input_fingerprint"]),
                ]
            )

        queries = {
            "load_latest_committed_llm": (
                """
                SELECT
                    m.*,
                    c.request_codec,
                    c.request_payload,
                    c.manifest_codec,
                    c.manifest_payload,
                    c.commit_codec,
                    c.commit_payload,
                    c.response_codec,
                    c.response_payload,
                    c.usage_details_json,
                    c.fallback_json,
                    c.previous_failures_json,
                    c.propagated_scope_refs_json,
                    c.response_metadata_extra_json,
                    c.raw_response_json
                FROM llm_records m
                LEFT JOIN llm_record_content c ON c.llm_record_id = m.id
                WHERE m.output_dir = ?
                  AND m.stage = ?
                  AND m.chunk_id = ?
                  AND m.llm_call_id = ?
                  AND m.input_fingerprint = ?
                  AND m.status = 'LOCAL_COMMITTED'
                ORDER BY m.committed_at_ms DESC, m.attempt DESC
                LIMIT 4
                """,
                [
                    str(store.output_dir),
                    str(llm_request["stage"]),
                    str(llm_request["chunk_id"]),
                    str(llm_request["llm_call_id"]),
                    str(llm_request["input_fingerprint"]),
                ],
            ),
            "load_latest_llm_attempt": (
                """
                SELECT
                    attempt,
                    status,
                    attempt_dir,
                    updated_at_ms
                FROM llm_records
                WHERE output_dir = ?
                  AND stage = ?
                  AND chunk_id = ?
                  AND llm_call_id = ?
                ORDER BY attempt DESC, updated_at_ms DESC
                LIMIT 1
                """,
                [
                    str(store.output_dir),
                    str(llm_request["stage"]),
                    str(llm_request["chunk_id"]),
                    str(llm_request["llm_call_id"]),
                ],
            ),
            "load_latest_committed_chunk": (
                """
                SELECT
                    m.*,
                    c.result_codec,
                    c.result_payload,
                    c.chunk_state_codec,
                    c.chunk_state_payload,
                    c.commit_codec,
                    c.commit_payload
                FROM chunk_records m
                LEFT JOIN chunk_record_content c ON c.chunk_record_id = m.id
                WHERE m.output_dir = ?
                  AND m.stage = ?
                  AND m.chunk_id = ?
                  AND m.input_fingerprint = ?
                  AND m.status = 'LOCAL_COMMITTED'
                ORDER BY m.committed_at_ms DESC, m.attempt DESC
                LIMIT 4
                """,
                [
                    str(store.output_dir),
                    str(chunk_request["stage"]),
                    str(chunk_request["chunk_id"]),
                    str(chunk_request["input_fingerprint"]),
                ],
            ),
            "list_scope_hints_pending_llm": (
                """
                SELECT
                    p.output_dir,
                    p.task_id,
                    p.storage_key,
                    p.normalized_video_key,
                    p.stage,
                    p.scope_type,
                    p.scope_id,
                    p.scope_ref,
                    p.scope_variant,
                    p.chunk_id,
                    p.llm_call_id,
                    p.unit_id,
                    p.stage_step,
                    p.input_fingerprint,
                    p.plan_status,
                    p.dirty_reason,
                    p.retry_mode,
                    p.retry_entry_point,
                    p.required_action,
                    p.local_path,
                    p.updated_at_ms,
                    l.latest_status,
                    l.durable_status,
                    l.latest_attempt,
                    l.can_restore,
                    l.error_class,
                    l.error_code,
                    l.error_message,
                    l.source_kind
                FROM scope_hint_plan p
                LEFT JOIN scope_hint_latest l
                  ON l.output_dir = p.output_dir
                 AND l.scope_ref = p.scope_ref
                WHERE 1 = 1
                  AND p.output_dir = ?
                  AND p.task_id = ?
                  AND p.stage = ?
                  AND p.scope_type = ?
                  AND (
                    plan_status IN ('PLANNED','IN_FLIGHT','RECOMPUTE_PENDING','AUTO_RETRY_PENDING','MANUAL_RETRY_PENDING','FALLBACK_RETRY_PENDING','MANUAL_REPAIR_REQUIRED')
                    OR latest_status IN ('AUTO_RETRY_WAIT','MANUAL_RETRY_REQUIRED','DIRTY','LOCAL_WRITING','EXECUTING')
                  )
                ORDER BY p.updated_at_ms DESC, p.scope_type ASC, p.scope_id ASC
                LIMIT ?
                """,
                [
                    str(store.output_dir),
                    str(store.task_id),
                    "phase2b",
                    "llm_call",
                    200,
                ],
            ),
            "batch_load_committed_llm": (
                f"""
                WITH requested(request_index, output_dir, stage, chunk_id, llm_call_id, input_fingerprint) AS (
                    VALUES {", ".join(llm_batch_value_sql)}
                )
                SELECT
                    requested.request_index,
                    m.*,
                    c.request_codec,
                    c.request_payload,
                    c.manifest_codec,
                    c.manifest_payload,
                    c.commit_codec,
                    c.commit_payload,
                    c.response_codec,
                    c.response_payload,
                    c.usage_details_json,
                    c.fallback_json,
                    c.previous_failures_json,
                    c.propagated_scope_refs_json,
                    c.response_metadata_extra_json,
                    c.raw_response_json
                FROM requested
                LEFT JOIN llm_records m
                  ON m.output_dir = requested.output_dir
                 AND m.stage = requested.stage
                 AND m.chunk_id = requested.chunk_id
                 AND m.llm_call_id = requested.llm_call_id
                 AND m.input_fingerprint = requested.input_fingerprint
                 AND m.status = 'LOCAL_COMMITTED'
                LEFT JOIN llm_record_content c
                  ON c.llm_record_id = m.id
                ORDER BY requested.request_index ASC, m.committed_at_ms DESC, m.attempt DESC
                """,
                llm_batch_params,
            ),
            "batch_load_committed_chunk": (
                f"""
                WITH requested(request_index, output_dir, stage, chunk_id, input_fingerprint) AS (
                    VALUES {", ".join(chunk_batch_value_sql)}
                )
                SELECT
                    requested.request_index,
                    m.*,
                    c.result_codec,
                    c.result_payload,
                    c.chunk_state_codec,
                    c.chunk_state_payload,
                    c.commit_codec,
                    c.commit_payload
                FROM requested
                LEFT JOIN chunk_records m
                  ON m.output_dir = requested.output_dir
                 AND m.stage = requested.stage
                 AND m.chunk_id = requested.chunk_id
                 AND m.input_fingerprint = requested.input_fingerprint
                 AND m.status = 'LOCAL_COMMITTED'
                LEFT JOIN chunk_record_content c
                  ON c.chunk_record_id = m.id
                ORDER BY requested.request_index ASC, m.committed_at_ms DESC, m.attempt DESC
                """,
                chunk_batch_params,
            ),
        }

        return {
            query_name: _explain(connection, query_text, query_params)
            for query_name, (query_text, query_params) in queries.items()
        }
    finally:
        connection.close()


def _summarize_plan(details: List[str]) -> Dict[str, Any]:
    upper_details = [str(item or "") for item in list(details or [])]
    return {
        "uses_index": any("USING INDEX" in item for item in upper_details),
        "uses_autoindex": any("sqlite_autoindex_" in item for item in upper_details),
        "uses_temp_btree": any("USE TEMP B-TREE" in item for item in upper_details),
        "details": upper_details,
    }


def main() -> int:
    plans = collect_restore_query_plans()
    output = {
        query_name: _summarize_plan(details)
        for query_name, details in plans.items()
    }
    print(json.dumps(output, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
