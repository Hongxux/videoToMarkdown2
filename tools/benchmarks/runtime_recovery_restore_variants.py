from __future__ import annotations

import json
import os
import shutil
import statistics
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from services.python_grpc.src.common.utils.runtime_recovery_store import (  # noqa: E402
    RuntimeRecoveryStore,
    build_llm_input_fingerprint,
)


SCENARIOS = [
    {
        "name": "full_json_compressed",
        "compress_large_payloads": True,
        "enable_llm_field_restore": False,
    },
    {
        "name": "full_json_uncompressed",
        "compress_large_payloads": False,
        "enable_llm_field_restore": False,
    },
    {
        "name": "field_restore_compressed",
        "compress_large_payloads": True,
        "enable_llm_field_restore": True,
    },
    {
        "name": "field_restore_uncompressed",
        "compress_large_payloads": False,
        "enable_llm_field_restore": True,
    },
]


def _measure_ms(callback, *, repeat: int = 3) -> Dict[str, Any]:
    timings: List[float] = []
    result: Any = None
    safe_repeat = max(1, int(repeat or 1))
    for _ in range(safe_repeat):
        started = time.perf_counter()
        result = callback()
        timings.append((time.perf_counter() - started) * 1000.0)
    return {
        "avg_ms": round(sum(timings) / float(len(timings)), 3),
        "min_ms": round(min(timings), 3),
        "max_ms": round(max(timings), 3),
        "result": result,
    }


def _stats(values: List[float], *, digits: int = 3) -> Dict[str, float]:
    normalized = [float(value) for value in values]
    if not normalized:
        return {"mean": 0.0, "std": 0.0, "min": 0.0, "max": 0.0}
    return {
        "mean": round(statistics.mean(normalized), digits),
        "std": round(statistics.pstdev(normalized), digits),
        "min": round(min(normalized), digits),
        "max": round(max(normalized), digits),
    }


def _restore_env(env_backup: Dict[str, Any]) -> None:
    for key, previous in env_backup.items():
        if previous is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = str(previous)


def _build_fixture(
    temp_root: Path,
    *,
    scenario_name: str,
    llm_count: int,
    chunk_count: int,
) -> Dict[str, Any]:
    db_path = temp_root / f"{scenario_name}.sqlite3"
    output_dir = temp_root / f"{scenario_name}_task"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id=f"task-{scenario_name}")

    llm_requests: List[Dict[str, str]] = []
    for index in range(llm_count):
        unit_id = f"SU{index:04d}"
        prompt = f"user prompt {index} :: " + ("payload " * 256)
        input_fingerprint = build_llm_input_fingerprint(
            step_name="structured_text",
            unit_id=unit_id,
            model="deepseek-chat",
            system_prompt="system",
            user_prompt=prompt,
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
            request_payload={"prompt": prompt},
            metadata={"step_name": "structured_text", "unit_id": unit_id},
        )
        store.commit_llm_attempt(
            handle=handle,
            response_text=(f"response for {unit_id} " + ("result " * 2048)).strip(),
            response_metadata={
                "model": "deepseek-chat",
                "prompt_tokens": 120 + index,
                "completion_tokens": 240 + index,
                "total_tokens": 360 + index,
                "latency_ms": 800.0 + index,
                "cache_hit": bool(index % 2),
                "is_fallback": False,
                "usage_details": {"cached_prompt_tokens": index % 17},
                "fallback": {},
                "previous_failures": [],
                "propagated_scope_refs": [f"rt/phase2a/chunk/ss{index:06d}"],
                "raw_response": {"provider_id": f"resp_{index:04d}"},
                "custom_label": f"label-{index:04d}",
            },
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
                        "raw_notes": " ".join(["chunk-note"] * 1024),
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
        "db_path": db_path,
        "store": store,
        "llm_requests": llm_requests,
        "chunk_requests": chunk_requests,
    }


def _build_llm_batch_sql(store: RuntimeRecoveryStore, requests: List[Dict[str, str]]) -> tuple[str, List[Any]]:
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
        ORDER BY requested.request_index ASC, m.committed_at_ms DESC, m.attempt DESC
    """
    return query, params


def _build_chunk_batch_sql(store: RuntimeRecoveryStore, requests: List[Dict[str, str]]) -> tuple[str, List[Any]]:
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
        ORDER BY requested.request_index ASC, m.committed_at_ms DESC, m.attempt DESC
    """
    return query, params


def _measure_decode_only(store: RuntimeRecoveryStore, *, llm_requests: List[Dict[str, str]], chunk_requests: List[Dict[str, str]]) -> Dict[str, float]:
    sqlite_index = store._sqlite_index
    assert sqlite_index is not None
    connection = sqlite_index._connect()
    try:
        llm_query, llm_params = _build_llm_batch_sql(store, llm_requests)
        llm_rows = connection.execute(llm_query, tuple(llm_params)).fetchall()
        chunk_query, chunk_params = _build_chunk_batch_sql(store, chunk_requests)
        chunk_rows = connection.execute(chunk_query, tuple(chunk_params)).fetchall()

        llm_started = time.perf_counter()
        llm_restored = 0
        for row in llm_rows:
            if sqlite_index._restore_llm_from_row(row) is not None:
                llm_restored += 1
        llm_elapsed_ms = (time.perf_counter() - llm_started) * 1000.0

        chunk_started = time.perf_counter()
        chunk_restored = 0
        for row in chunk_rows:
            if sqlite_index._restore_chunk_from_row(row) is not None:
                chunk_restored += 1
        chunk_elapsed_ms = (time.perf_counter() - chunk_started) * 1000.0
        return {
            "llm_decode_only_ms": round(llm_elapsed_ms, 3),
            "chunk_decode_only_ms": round(chunk_elapsed_ms, 3),
            "llm_decode_rows": float(llm_restored),
            "chunk_decode_rows": float(chunk_restored),
        }
    finally:
        connection.close()


def _run_one_round(
    temp_root: Path,
    *,
    scenario_name: str,
    llm_count: int,
    chunk_count: int,
) -> Dict[str, Any]:
    fixture = _build_fixture(
        temp_root,
        scenario_name=scenario_name,
        llm_count=llm_count,
        chunk_count=chunk_count,
    )
    store: RuntimeRecoveryStore = fixture["store"]
    llm_requests: List[Dict[str, str]] = fixture["llm_requests"]
    chunk_requests: List[Dict[str, str]] = fixture["chunk_requests"]
    db_path: Path = fixture["db_path"]

    llm_batch = _measure_ms(
        lambda: store.batch_load_committed_llm_responses(llm_requests),
        repeat=3,
    )
    llm_single = _measure_ms(
        lambda: sum(
            1
            for request in llm_requests
            if store.load_committed_llm_response(
                stage=request["stage"],
                chunk_id=request["chunk_id"],
                llm_call_id=request["llm_call_id"],
                input_fingerprint=request["input_fingerprint"],
            )
            is not None
        ),
        repeat=3,
    )
    chunk_batch = _measure_ms(
        lambda: store.batch_load_committed_chunk_payloads(chunk_requests),
        repeat=3,
    )
    chunk_single = _measure_ms(
        lambda: sum(
            1
            for request in chunk_requests
            if store.load_committed_chunk_payload(
                stage=request["stage"],
                chunk_id=request["chunk_id"],
                input_fingerprint=request["input_fingerprint"],
            )
            is not None
        ),
        repeat=3,
    )
    decode_only = _measure_ms(
        lambda: _measure_decode_only(
            store,
            llm_requests=llm_requests,
            chunk_requests=chunk_requests,
        ),
        repeat=3,
    )
    decode_only_payload = dict(decode_only["result"] or {})
    return {
        "db_bytes": int(db_path.stat().st_size if db_path.exists() else 0),
        "llm_batch_restore_ms": float(llm_batch["avg_ms"]),
        "llm_single_restore_ms": float(llm_single["avg_ms"]),
        "chunk_batch_restore_ms": float(chunk_batch["avg_ms"]),
        "chunk_single_restore_ms": float(chunk_single["avg_ms"]),
        "llm_decode_only_ms": float(decode_only_payload.get("llm_decode_only_ms", 0.0) or 0.0),
        "chunk_decode_only_ms": float(decode_only_payload.get("chunk_decode_only_ms", 0.0) or 0.0),
        "llm_restored_count": len([item for item in list(llm_batch["result"] or []) if item.get("restored") is not None]),
        "chunk_restored_count": len([item for item in list(chunk_batch["result"] or []) if item.get("restored") is not None]),
    }


def main() -> int:
    rounds = max(3, int(os.getenv("TASK_RUNTIME_SQLITE_BENCH_ROUNDS", "5") or "5"))
    llm_count = max(60, int(os.getenv("TASK_RUNTIME_SQLITE_BENCH_LLM_COUNT", "180") or "180"))
    chunk_count = max(60, int(os.getenv("TASK_RUNTIME_SQLITE_BENCH_CHUNK_COUNT", "180") or "180"))
    env_keys = {
        "TASK_RUNTIME_SQLITE_DB_PATH": os.environ.get("TASK_RUNTIME_SQLITE_DB_PATH"),
        "TASK_RUNTIME_SQLITE_COMPRESS_LARGE_PAYLOADS": os.environ.get("TASK_RUNTIME_SQLITE_COMPRESS_LARGE_PAYLOADS"),
        "TASK_RUNTIME_SQLITE_ENABLE_LLM_FIELD_RESTORE": os.environ.get("TASK_RUNTIME_SQLITE_ENABLE_LLM_FIELD_RESTORE"),
    }
    temp_root = Path(tempfile.mkdtemp(prefix="runtime_recovery_restore_variants_", dir=str(REPO_ROOT / "var")))
    try:
        results: Dict[str, Any] = {
            "dataset": {
                "rounds": rounds,
                "llm_requests": llm_count,
                "chunk_requests": chunk_count,
            },
            "scenarios": {},
        }
        for scenario in SCENARIOS:
            scenario_name = str(scenario["name"])
            round_rows: List[Dict[str, Any]] = []
            for round_index in range(rounds):
                os.environ["TASK_RUNTIME_SQLITE_COMPRESS_LARGE_PAYLOADS"] = (
                    "1" if bool(scenario["compress_large_payloads"]) else "0"
                )
                os.environ["TASK_RUNTIME_SQLITE_ENABLE_LLM_FIELD_RESTORE"] = (
                    "1" if bool(scenario["enable_llm_field_restore"]) else "0"
                )
                round_root = temp_root / f"{scenario_name}_r{round_index + 1:02d}"
                last_error: Exception | None = None
                for retry_index in range(3):
                    try:
                        if round_root.exists():
                            shutil.rmtree(round_root, ignore_errors=True)
                        round_root.mkdir(parents=True, exist_ok=True)
                        os.environ["TASK_RUNTIME_SQLITE_DB_PATH"] = str(round_root / f"{scenario_name}.sqlite3")
                        round_rows.append(
                            _run_one_round(
                                round_root,
                                scenario_name=scenario_name,
                                llm_count=llm_count,
                                chunk_count=chunk_count,
                            )
                        )
                        last_error = None
                        break
                    except PermissionError as error:
                        last_error = error
                        time.sleep(0.2 * float(retry_index + 1))
                if last_error is not None:
                    raise last_error
            results["scenarios"][scenario_name] = {
                "config": {
                    "compress_large_payloads": bool(scenario["compress_large_payloads"]),
                    "enable_llm_field_restore": bool(scenario["enable_llm_field_restore"]),
                },
                "aggregate": {
                    "db_bytes": _stats([row["db_bytes"] for row in round_rows], digits=0),
                    "llm_batch_restore_ms": _stats([row["llm_batch_restore_ms"] for row in round_rows]),
                    "llm_single_restore_ms": _stats([row["llm_single_restore_ms"] for row in round_rows]),
                    "chunk_batch_restore_ms": _stats([row["chunk_batch_restore_ms"] for row in round_rows]),
                    "chunk_single_restore_ms": _stats([row["chunk_single_restore_ms"] for row in round_rows]),
                    "llm_decode_only_ms": _stats([row["llm_decode_only_ms"] for row in round_rows]),
                    "chunk_decode_only_ms": _stats([row["chunk_decode_only_ms"] for row in round_rows]),
                },
                "rounds": round_rows,
            }
        print(json.dumps(results, ensure_ascii=False, indent=2))
        return 0
    finally:
        _restore_env(env_keys)


if __name__ == "__main__":
    raise SystemExit(main())
