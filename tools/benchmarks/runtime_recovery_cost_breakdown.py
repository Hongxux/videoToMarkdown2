from __future__ import annotations

import json
import os
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from services.python_grpc.src.common.utils.runtime_recovery_store import (  # noqa: E402
    RuntimeRecoveryStore,
    build_llm_input_fingerprint,
)


def _measure_ms(callback, *, repeat: int = 5) -> Tuple[float, Any]:
    timings: List[float] = []
    result: Any = None
    safe_repeat = max(1, int(repeat or 1))
    for _ in range(safe_repeat):
        started = time.perf_counter()
        result = callback()
        timings.append((time.perf_counter() - started) * 1000.0)
    average_ms = sum(timings) / float(len(timings))
    return round(average_ms, 3), result


def _build_fixture(temp_root: Path, *, llm_count: int, chunk_count: int) -> Dict[str, Any]:
    os.environ["TASK_RUNTIME_SQLITE_DB_PATH"] = str(temp_root / "runtime_recovery.sqlite3")
    output_dir = temp_root / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-cost-breakdown")

    llm_requests: List[Dict[str, str]] = []
    llm_scope_refs: List[str] = []
    for index in range(llm_count):
        unit_id = f"SU{index:04d}"
        user_prompt = f"user prompt {index} :: " + ("payload " * 32)
        input_fingerprint = build_llm_input_fingerprint(
            step_name="structured_text",
            unit_id=unit_id,
            model="deepseek-chat",
            system_prompt="system",
            user_prompt=user_prompt,
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
            request_payload={"prompt": user_prompt},
            metadata={"step_name": "structured_text", "unit_id": unit_id},
        )
        store.commit_llm_attempt(
            handle=handle,
            response_text=(f"response for {unit_id} " + ("result " * 64)).strip(),
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
        llm_scope_refs.append(
            store.build_scope_ref(stage="phase2b", scope_type="llm_call", scope_id=llm_call_id)
        )

    for scope_ref in llm_scope_refs:
        store.mark_scope_dirty(scope_ref, reason="fallback_repair", include_descendants=False)

    chunk_requests: List[Dict[str, str]] = []
    for index in range(chunk_count):
        chunk_id = f"ss{index:06d}"
        input_fingerprint = f"chunk-fingerprint-{index:04d}"
        store.commit_chunk_payload(
            stage="phase2a",
            chunk_id=chunk_id,
            input_fingerprint=input_fingerprint,
            result_payload={
                "request_updates": [
                    {
                        "request_key": f"id:shot_{index:04d}",
                        "timestamp_sec": float(index) + 0.5,
                        "_optimized": True,
                        "raw_notes": " ".join(["chunk-note"] * 48),
                    }
                ]
            },
            metadata={"mode": "batch", "scope_variant": "batch"},
        )
        chunk_requests.append(
            {
                "stage": "phase2a",
                "chunk_id": chunk_id,
                "input_fingerprint": input_fingerprint,
            }
        )

    return {
        "store": store,
        "llm_requests": llm_requests,
        "chunk_requests": chunk_requests,
    }


def _build_llm_batch_sql(store: RuntimeRecoveryStore, requests: List[Dict[str, str]], *, with_order: bool) -> Tuple[str, List[Any]]:
    value_sql: List[str] = []
    params: List[Any] = []
    for request_index, request in enumerate(requests):
        value_sql.append("(?, ?, ?, ?, ?, ?)")
        params.extend(
            [
                int(request_index),
                str(store.output_dir),
                str(request["stage"]),
                str(request["chunk_id"]),
                str(request["llm_call_id"]),
                str(request["input_fingerprint"]),
            ]
        )
    query = f"""
        WITH requested(request_index, output_dir, stage, chunk_id, llm_call_id, input_fingerprint) AS (
            VALUES {", ".join(value_sql)}
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
    """
    if with_order:
        query += "\nORDER BY requested.request_index ASC, m.committed_at_ms DESC, m.attempt DESC"
    return query, params


def _build_chunk_batch_sql(store: RuntimeRecoveryStore, requests: List[Dict[str, str]], *, with_order: bool) -> Tuple[str, List[Any]]:
    value_sql: List[str] = []
    params: List[Any] = []
    for request_index, request in enumerate(requests):
        value_sql.append("(?, ?, ?, ?, ?)")
        params.extend(
            [
                int(request_index),
                str(store.output_dir),
                str(request["stage"]),
                str(request["chunk_id"]),
                str(request["input_fingerprint"]),
            ]
        )
    query = f"""
        WITH requested(request_index, output_dir, stage, chunk_id, input_fingerprint) AS (
            VALUES {", ".join(value_sql)}
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
    """
    if with_order:
        query += "\nORDER BY requested.request_index ASC, m.committed_at_ms DESC, m.attempt DESC"
    return query, params


def _build_requested_only_sql(requests: List[Dict[str, str]], *, kind: str, output_dir: str) -> Tuple[str, List[Any]]:
    value_sql: List[str] = []
    params: List[Any] = []
    if kind == "llm":
        for request_index, request in enumerate(requests):
            value_sql.append("(?, ?, ?, ?, ?, ?)")
            params.extend(
                [
                    int(request_index),
                    str(output_dir),
                    str(request["stage"]),
                    str(request["chunk_id"]),
                    str(request["llm_call_id"]),
                    str(request["input_fingerprint"]),
                ]
            )
        return (
            f"""
            WITH requested(request_index, output_dir, stage, chunk_id, llm_call_id, input_fingerprint) AS (
                VALUES {", ".join(value_sql)}
            )
            SELECT request_index FROM requested
            """,
            params,
        )
    for request_index, request in enumerate(requests):
        value_sql.append("(?, ?, ?, ?, ?)")
        params.extend(
            [
                int(request_index),
                str(output_dir),
                str(request["stage"]),
                str(request["chunk_id"]),
                str(request["input_fingerprint"]),
            ]
        )
    return (
        f"""
        WITH requested(request_index, output_dir, stage, chunk_id, input_fingerprint) AS (
            VALUES {", ".join(value_sql)}
        )
        SELECT request_index FROM requested
        """,
        params,
    )


def _build_pending_scope_sql(store: RuntimeRecoveryStore, *, with_order: bool) -> Tuple[str, List[Any]]:
    query = """
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
    """
    if with_order:
        query += "\nORDER BY p.updated_at_ms DESC, p.scope_type ASC, p.scope_id ASC"
    query += "\nLIMIT ?"
    params = [str(store.output_dir), str(store.task_id), "phase2b", "llm_call", 400]
    return query, params


def main() -> int:
    temp_root = Path(tempfile.mkdtemp(prefix="runtime_recovery_cost_breakdown_", dir=str(REPO_ROOT / "var")))
    fixture = _build_fixture(temp_root, llm_count=240, chunk_count=240)
    store: RuntimeRecoveryStore = fixture["store"]
    llm_requests: List[Dict[str, str]] = fixture["llm_requests"]
    chunk_requests: List[Dict[str, str]] = fixture["chunk_requests"]
    sqlite_index = store._sqlite_index
    assert sqlite_index is not None
    connection = sqlite_index._connect()
    try:
        llm_batch_with_order_sql, llm_batch_params = _build_llm_batch_sql(store, llm_requests, with_order=True)
        llm_batch_without_order_sql, _ = _build_llm_batch_sql(store, llm_requests, with_order=False)
        chunk_batch_with_order_sql, chunk_batch_params = _build_chunk_batch_sql(store, chunk_requests, with_order=True)
        chunk_batch_without_order_sql, _ = _build_chunk_batch_sql(store, chunk_requests, with_order=False)
        llm_requested_only_sql, llm_requested_only_params = _build_requested_only_sql(
            llm_requests,
            kind="llm",
            output_dir=str(store.output_dir),
        )
        chunk_requested_only_sql, chunk_requested_only_params = _build_requested_only_sql(
            chunk_requests,
            kind="chunk",
            output_dir=str(store.output_dir),
        )
        pending_with_order_sql, pending_params = _build_pending_scope_sql(store, with_order=True)
        pending_without_order_sql, _ = _build_pending_scope_sql(store, with_order=False)

        llm_query_with_order_ms, llm_rows_with_order = _measure_ms(
            lambda: connection.execute(llm_batch_with_order_sql, tuple(llm_batch_params)).fetchall()
        )
        llm_query_without_order_ms, llm_rows_without_order = _measure_ms(
            lambda: connection.execute(llm_batch_without_order_sql, tuple(llm_batch_params)).fetchall()
        )
        llm_requested_only_ms, llm_requested_only_rows = _measure_ms(
            lambda: connection.execute(llm_requested_only_sql, tuple(llm_requested_only_params)).fetchall()
        )
        llm_decode_ms, llm_restored_count = _measure_ms(
            lambda: sum(1 for row in llm_rows_with_order if sqlite_index._restore_llm_from_row(row) is not None)
        )

        chunk_query_with_order_ms, chunk_rows_with_order = _measure_ms(
            lambda: connection.execute(chunk_batch_with_order_sql, tuple(chunk_batch_params)).fetchall()
        )
        chunk_query_without_order_ms, chunk_rows_without_order = _measure_ms(
            lambda: connection.execute(chunk_batch_without_order_sql, tuple(chunk_batch_params)).fetchall()
        )
        chunk_requested_only_ms, chunk_requested_only_rows = _measure_ms(
            lambda: connection.execute(chunk_requested_only_sql, tuple(chunk_requested_only_params)).fetchall()
        )
        chunk_decode_ms, chunk_restored_count = _measure_ms(
            lambda: sum(1 for row in chunk_rows_with_order if sqlite_index._restore_chunk_from_row(row) is not None)
        )

        pending_with_order_ms, pending_rows_with_order = _measure_ms(
            lambda: connection.execute(pending_with_order_sql, tuple(pending_params)).fetchall()
        )
        pending_without_order_ms, pending_rows_without_order = _measure_ms(
            lambda: connection.execute(pending_without_order_sql, tuple(pending_params)).fetchall()
        )

        output = {
            "dataset": {
                "llm_requests": len(llm_requests),
                "chunk_requests": len(chunk_requests),
                "pending_scope_rows": len(pending_rows_with_order),
            },
            "llm_batch_restore": {
                "query_with_order_ms": llm_query_with_order_ms,
                "query_without_order_ms": llm_query_without_order_ms,
                "requested_only_scan_ms": llm_requested_only_ms,
                "decode_ms": llm_decode_ms,
                "temp_btree_estimated_ms": round(max(0.0, llm_query_with_order_ms - llm_query_without_order_ms), 3),
                "requested_only_rows": len(llm_requested_only_rows),
                "fetched_rows_with_order": len(llm_rows_with_order),
                "fetched_rows_without_order": len(llm_rows_without_order),
                "restored_rows": int(llm_restored_count),
            },
            "chunk_batch_restore": {
                "query_with_order_ms": chunk_query_with_order_ms,
                "query_without_order_ms": chunk_query_without_order_ms,
                "requested_only_scan_ms": chunk_requested_only_ms,
                "decode_ms": chunk_decode_ms,
                "temp_btree_estimated_ms": round(max(0.0, chunk_query_with_order_ms - chunk_query_without_order_ms), 3),
                "requested_only_rows": len(chunk_requested_only_rows),
                "fetched_rows_with_order": len(chunk_rows_with_order),
                "fetched_rows_without_order": len(chunk_rows_without_order),
                "restored_rows": int(chunk_restored_count),
            },
            "pending_scope_list": {
                "query_with_order_ms": pending_with_order_ms,
                "query_without_order_ms": pending_without_order_ms,
                "temp_btree_estimated_ms": round(max(0.0, pending_with_order_ms - pending_without_order_ms), 3),
                "rows_with_order": len(pending_rows_with_order),
                "rows_without_order": len(pending_rows_without_order),
            },
        }
        print(json.dumps(output, ensure_ascii=False, indent=2))
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
