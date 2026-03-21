from __future__ import annotations

import json
import os
import shutil
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


def _build_llm_requests(store: RuntimeRecoveryStore, *, count: int) -> List[Dict[str, str]]:
    requests: List[Dict[str, str]] = []
    for index in range(count):
        unit_id = f"SU{index:04d}"
        step_name = "structured_text"
        prompt = f"user prompt {index} :: " + ("payload " * 24)
        input_fingerprint = build_llm_input_fingerprint(
            step_name=step_name,
            unit_id=unit_id,
            model="deepseek-chat",
            system_prompt="system",
            user_prompt=prompt,
        )
        llm_call_id = store.build_llm_call_id(
            step_name=step_name,
            unit_id=unit_id,
            input_fingerprint=input_fingerprint,
        )
        handle = store.begin_llm_attempt(
            stage="phase2b",
            chunk_id=f"unit_{unit_id}",
            llm_call_id=llm_call_id,
            input_fingerprint=input_fingerprint,
            request_payload={"prompt": prompt},
            metadata={"step_name": step_name, "unit_id": unit_id},
        )
        store.commit_llm_attempt(
            handle=handle,
            response_text=(f"response for {unit_id} " + ("result " * 48)).strip(),
            response_metadata={"model": "deepseek-chat"},
        )
        requests.append(
            {
                "stage": "phase2b",
                "chunk_id": f"unit_{unit_id}",
                "llm_call_id": llm_call_id,
                "input_fingerprint": input_fingerprint,
            }
        )
    return requests


def _build_chunk_requests(store: RuntimeRecoveryStore, *, count: int) -> List[Dict[str, str]]:
    requests: List[Dict[str, str]] = []
    for index in range(count):
        chunk_id = store.build_chunk_id(chunk_index=index, prefix="ss")
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
                    }
                ]
            },
            metadata={"mode": "batch", "scope_variant": "batch"},
        )
        requests.append(
            {
                "stage": "phase2a",
                "chunk_id": chunk_id,
                "input_fingerprint": input_fingerprint,
            }
        )
    return requests


def _measure(label: str, callback) -> Dict[str, Any]:
    started = time.perf_counter()
    result = callback()
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    return {"label": label, "elapsed_ms": round(elapsed_ms, 3), "result": result}


def _count_restored(prefetch_result: Dict[str, Any]) -> int:
    return int((prefetch_result.get("summary", {}) or {}).get("prefetched_restore_count", 0) or 0)


def _populate_legacy_lookup_indexes(
    store: RuntimeRecoveryStore,
    *,
    llm_requests: List[Dict[str, str]],
    chunk_requests: List[Dict[str, str]],
) -> None:
    for request in llm_requests:
        attempt_dirs = store._collect_exact_attempt_dirs(
            stage=request["stage"],
            chunk_id=request["chunk_id"],
            llm_call_id=request["llm_call_id"],
        )
        if not attempt_dirs:
            continue
        attempt_dir = attempt_dirs[-1]
        commit_payload = json.loads((attempt_dir / "commit.json").read_text(encoding="utf-8"))
        store._write_llm_lookup_index(
            stage=request["stage"],
            chunk_id=request["chunk_id"],
            llm_call_id=request["llm_call_id"],
            input_fingerprint=request["input_fingerprint"],
            attempt=int(commit_payload.get("attempt", 0) or 0),
            attempt_dir=attempt_dir,
            response_hash=str(commit_payload.get("response_hash", "") or ""),
            committed_parts=int(commit_payload.get("committed_parts", 0) or 0),
        )
    for request in chunk_requests:
        chunk_dir = store.chunk_dir(stage=request["stage"], chunk_id=request["chunk_id"])
        commit_payload = json.loads((chunk_dir / "commit.json").read_text(encoding="utf-8"))
        store._write_chunk_lookup_index(
            stage=request["stage"],
            chunk_id=request["chunk_id"],
            input_fingerprint=request["input_fingerprint"],
            chunk_dir=chunk_dir,
            attempt=int(commit_payload.get("attempt", 0) or 0),
            result_hash=str(commit_payload.get("result_hash", "") or ""),
        )


def main() -> int:
    temp_root = Path(tempfile.mkdtemp(prefix="runtime_recovery_bench_", dir=str(REPO_ROOT / "var")))
    output_dir = temp_root / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    db_path = temp_root / "runtime_recovery.sqlite3"
    os.environ["TASK_RUNTIME_SQLITE_DB_PATH"] = str(db_path)

    llm_count = 240
    chunk_count = 240

    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-bench")
    llm_requests = _build_llm_requests(store, count=llm_count)
    chunk_requests = _build_chunk_requests(store, count=chunk_count)
    _populate_legacy_lookup_indexes(store, llm_requests=llm_requests, chunk_requests=chunk_requests)

    llm_chunk_ids = [str(item["chunk_id"]) for item in llm_requests]
    chunk_ids = [str(item["chunk_id"]) for item in chunk_requests]

    metrics: Dict[str, Any] = {
        "dataset": {
            "llm_records": llm_count,
            "chunk_records": chunk_count,
        }
    }

    metrics["sqlite_llm_prefetch"] = _measure(
        "sqlite_llm_prefetch",
        lambda: _count_restored(
            store.prefetch_restorable_llm_scope_cache(
                stage="phase2b",
                candidate_chunk_ids=llm_chunk_ids,
                limit=llm_count * 8,
            )
        ),
    )
    metrics["sqlite_llm_single"] = _measure(
        "sqlite_llm_single",
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
    )

    json_store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-bench")
    json_store._sqlite_index = None
    metrics["json_llm_indexed_single"] = _measure(
        "json_llm_indexed_single",
        lambda: sum(
            1
            for request in llm_requests
            if json_store.load_committed_llm_response(
                stage=request["stage"],
                chunk_id=request["chunk_id"],
                llm_call_id=request["llm_call_id"],
                input_fingerprint=request["input_fingerprint"],
            )
            is not None
        ),
    )

    shutil.rmtree(output_dir / "intermediates" / "rt" / "index", ignore_errors=True)
    json_scan_store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-bench")
    json_scan_store._sqlite_index = None
    metrics["json_llm_cold_scan_single"] = _measure(
        "json_llm_cold_scan_single",
        lambda: sum(
            1
            for request in llm_requests
            if json_scan_store.load_committed_llm_response(
                stage=request["stage"],
                chunk_id=request["chunk_id"],
                llm_call_id=request["llm_call_id"],
                input_fingerprint=request["input_fingerprint"],
            )
            is not None
        ),
    )

    metrics["sqlite_chunk_prefetch"] = _measure(
        "sqlite_chunk_prefetch",
        lambda: _count_restored(
            store.prefetch_restorable_chunk_scope_cache(
                stage="phase2a",
                candidate_chunk_ids=chunk_ids,
                limit=chunk_count * 4,
            )
        ),
    )
    metrics["sqlite_chunk_single"] = _measure(
        "sqlite_chunk_single",
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
    )

    json_chunk_store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-bench")
    json_chunk_store._sqlite_index = None
    metrics["json_chunk_indexed_single"] = _measure(
        "json_chunk_indexed_single",
        lambda: sum(
            1
            for request in chunk_requests
            if json_chunk_store.load_committed_chunk_payload(
                stage=request["stage"],
                chunk_id=request["chunk_id"],
                input_fingerprint=request["input_fingerprint"],
            )
            is not None
        ),
    )

    shutil.rmtree(output_dir / "intermediates" / "rt" / "index", ignore_errors=True)
    json_chunk_scan_store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-bench")
    json_chunk_scan_store._sqlite_index = None
    metrics["json_chunk_cold_scan_single"] = _measure(
        "json_chunk_cold_scan_single",
        lambda: sum(
            1
            for request in chunk_requests
            if json_chunk_scan_store.load_committed_chunk_payload(
                stage=request["stage"],
                chunk_id=request["chunk_id"],
                input_fingerprint=request["input_fingerprint"],
            )
            is not None
        ),
    )

    print(json.dumps(metrics, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
