import shutil
import sys
import time
import types
from pathlib import Path

import pytest


if "faster_whisper" not in sys.modules:
    faster_whisper_stub = types.ModuleType("faster_whisper")

    class _WhisperModelStub:
        pass

    faster_whisper_stub.WhisperModel = _WhisperModelStub
    sys.modules["faster_whisper"] = faster_whisper_stub


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.python_grpc.src.common.utils.runtime_recovery_store import (
    RuntimeRecoveryStore,
    build_runtime_payload_fingerprint,
)
from services.python_grpc.src.media_engine.knowledge_engine.core import parallel_transcription as pt


SEGMENTS = [
    {"id": 0, "start": 0.0, "end": 10.0, "duration": 10.0},
    {"id": 1, "start": 10.0, "end": 20.0, "duration": 10.0},
]


class _FutureStub:
    def __init__(self, result_payload=None, error=None, on_result=None):
        self._result_payload = result_payload
        self._error = error
        self._on_result = on_result

    def result(self):
        if self._on_result is not None:
            self._on_result()
        if self._error is not None:
            raise self._error
        return self._result_payload


class _ExecutorStub:
    def __init__(self, futures):
        self._futures = futures

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def submit(self, func, args):
        segment_id = args[1]["id"]
        if segment_id not in self._futures:
            pytest.fail(f"unexpected segment submitted: {segment_id}")
        return self._futures[segment_id]


def _make_repo_tmp_dir(test_name: str) -> Path:
    repo_root = Path(__file__).resolve().parents[1]
    base = repo_root / "var" / "tmp_transcribe_segment_runtime_tests"
    base.mkdir(parents=True, exist_ok=True)
    safe_name = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in test_name)
    unique_suffix = f"{time.time_ns() % 1_000_000:06d}"
    path = base / f"{safe_name[:24]}_{unique_suffix}"
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _patch_parallel_common(monkeypatch, futures, extract_audio_calls):
    monkeypatch.setattr(
        pt,
        "_build_process_pool_executor",
        lambda max_workers: _ExecutorStub(futures),
    )
    monkeypatch.setattr(pt, "as_completed", lambda future_map: list(future_map.keys()))
    monkeypatch.setattr(pt, "split_video_segments", lambda *args, **kwargs: [dict(item) for item in SEGMENTS])
    monkeypatch.setattr(
        pt,
        "build_parallel_plan",
        lambda *args, **kwargs: {
            "effective_workers": 2,
            "cpu_threads_per_worker": 1,
            "available_mem_gb": 8.0,
            "requested_workers": 2,
            "segment_count": len(SEGMENTS),
            "cpu_budget": 2,
            "total_cores": 4,
        },
    )
    monkeypatch.setattr(
        pt,
        "format_subtitles",
        lambda subtitles: "|".join(item["text"] for item in sorted(subtitles, key=lambda item: item["start"])),
    )

    def _extract_full_audio(_video_path, full_audio_path):
        extract_audio_calls.append(str(full_audio_path))
        Path(full_audio_path).write_bytes(b"")

    monkeypatch.setattr(pt, "_extract_full_audio", _extract_full_audio)

    model_downloader_module = (
        "services.python_grpc.src.media_engine.knowledge_engine.core.model_downloader"
    )
    monkeypatch.setattr(
        f"{model_downloader_module}.download_whisper_model",
        lambda *args, **kwargs: "mock-model-path",
    )


def _build_segment_runtime_hooks(store: RuntimeRecoveryStore):
    def _build_chunk_id(segment):
        return store.build_chunk_id(chunk_index=int(segment["id"]), prefix="ts")

    def _build_fingerprint(segment, total_segments):
        return build_runtime_payload_fingerprint(
            {
                "schema_version": "transcribe_segment_runtime_test_v1",
                "language": "zh",
                "model_size": "small",
                "device": "cpu",
                "compute_type": "int8",
                "segment_duration": 600,
                "beam_size": 4,
                "vad_filter": False,
                "total_segments": int(total_segments),
                "segment": {
                    "id": int(segment["id"]),
                    "start": float(segment["start"]),
                    "end": float(segment["end"]),
                    "duration": float(segment["duration"]),
                },
            }
        )

    def _restore_segments(segments, total_segments):
        requests = []
        for segment in list(segments or []):
            requests.append(
                {
                    "stage": "transcribe",
                    "chunk_id": _build_chunk_id(segment),
                    "input_fingerprint": _build_fingerprint(segment, total_segments),
                    "segment_id": int(segment["id"]),
                }
            )
        restored_map = {}
        for row in store.batch_load_committed_chunk_payloads(requests):
            request_payload = dict(row.get("request", {}) or {})
            restored_payload = dict(row.get("restored", {}) or {})
            result_payload = dict(restored_payload.get("result_payload", {}) or {})
            if isinstance(result_payload.get("subtitles"), list):
                restored_map[int(request_payload["segment_id"])] = result_payload
        return restored_map

    def _segment_metadata(segment, total_segments):
        return {
            "scope_variant": "segment",
            "storage_backend": "sqlite",
            "segment_id": int(segment["id"]),
            "segment_index": int(segment["id"]) + 1,
            "total_segments": int(total_segments),
            "segment_start_sec": float(segment["start"]),
            "segment_end_sec": float(segment["end"]),
            "segment_duration_sec": float(segment["duration"]),
            "language": "zh",
        }

    def _plan_segments(segments, total_segments):
        for segment in list(segments or []):
            store.record_chunk_state(
                stage="transcribe",
                chunk_id=_build_chunk_id(segment),
                input_fingerprint=_build_fingerprint(segment, total_segments),
                status="PLANNED",
                metadata=_segment_metadata(segment, total_segments),
            )

    def _mark_segment_running(segment, total_segments):
        store.record_chunk_state(
            stage="transcribe",
            chunk_id=_build_chunk_id(segment),
            input_fingerprint=_build_fingerprint(segment, total_segments),
            status="RUNNING",
            metadata=_segment_metadata(segment, total_segments),
        )

    def _commit_segment(segment, total_segments, result_payload):
        store.commit_chunk_payload(
            stage="transcribe",
            chunk_id=_build_chunk_id(segment),
            input_fingerprint=_build_fingerprint(segment, total_segments),
            result_payload=dict(result_payload),
            metadata={
                **_segment_metadata(segment, total_segments),
                "subtitle_count": len(list(result_payload.get("subtitles", []) or [])),
            },
        )

    def _fail_segment(segment, total_segments, error):
        store.fail_chunk_payload(
            stage="transcribe",
            chunk_id=_build_chunk_id(segment),
            input_fingerprint=_build_fingerprint(segment, total_segments),
            error=error,
            metadata=_segment_metadata(segment, total_segments),
        )

    return (
        pt.TranscriptionSegmentRuntimeHooks(
            restore_committed_segments=_restore_segments,
            plan_pending_segments=_plan_segments,
            mark_segment_running=_mark_segment_running,
            commit_segment=_commit_segment,
            fail_segment=_fail_segment,
        ),
        _commit_segment,
        _build_chunk_id,
        _build_fingerprint,
    )


def test_runtime_recovery_store_records_sqlite_authoritative_chunk_state(monkeypatch):
    tmp_root = _make_repo_tmp_dir("transcribe_segment_sqlite_state")
    db_path = tmp_root / "runtime_recovery.sqlite3"
    monkeypatch.setenv("TASK_RUNTIME_SQLITE_DB_PATH", str(db_path))

    output_dir = tmp_root / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-transcribe-state")

    store.record_chunk_state(
        stage="transcribe",
        chunk_id="ts000001",
        input_fingerprint="fp-transcribe-state",
        status="PLANNED",
        metadata={
            "scope_variant": "segment",
            "storage_backend": "sqlite",
            "segment_id": 0,
            "segment_index": 1,
            "total_segments": 2,
        },
    )

    rows = store.list_sqlite_chunk_records(stage="transcribe", status="PLANNED", limit=10)
    assert any(row["chunk_id"] == "ts000001" for row in rows)
    assert not (
        output_dir / "intermediates" / "rt" / "stage" / "transcribe" / "chunk" / "ts000001"
    ).exists()


def test_transcribe_parallel_restores_committed_segments_from_sqlite(monkeypatch):
    tmp_root = _make_repo_tmp_dir("transcribe_segment_runtime_recovery")
    db_path = tmp_root / "runtime_recovery.sqlite3"
    monkeypatch.setenv("TASK_RUNTIME_SQLITE_DB_PATH", str(db_path))

    output_dir = tmp_root / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-transcribe-runtime")
    assert store._sqlite_index is not None

    hooks, commit_segment, build_chunk_id, build_fingerprint = _build_segment_runtime_hooks(store)
    segment_one_chunk_dir = output_dir / "intermediates" / "rt" / "stage" / "transcribe" / "chunk" / "ts000001"
    segment_two_chunk_dir = output_dir / "intermediates" / "rt" / "stage" / "transcribe" / "chunk" / "ts000002"

    commit_segment(
        SEGMENTS[0],
        len(SEGMENTS),
        {
            "segment_id": 0,
            "segment_index": 1,
            "total_segments": len(SEGMENTS),
            "segment": dict(SEGMENTS[0]),
            "subtitles": [{"start": 0.0, "end": 1.0, "text": "A"}],
        },
    )
    assert not segment_one_chunk_dir.exists()
    assert not segment_two_chunk_dir.exists()

    extract_audio_calls = []

    def _assert_segment_two_executing():
        executing_rows = store.list_sqlite_chunk_records(stage="transcribe", status="RUNNING", limit=10)
        assert any(row["chunk_id"] == "ts000002" for row in executing_rows)

    _patch_parallel_common(
        monkeypatch,
        futures={
            1: _FutureStub(
                on_result=_assert_segment_two_executing,
                result_payload={
                    "segment_id": 1,
                    "subtitles": [{"start": 10.0, "end": 11.0, "text": "B"}],
                    "success": True,
                }
            )
        },
        extract_audio_calls=extract_audio_calls,
    )
    monkeypatch.setattr(
        pt,
        "transcribe_segment",
        lambda args: pytest.fail("serial fallback should not run when pending segment succeeds"),
    )

    first_run_events = []
    subtitle_text = pt.transcribe_parallel(
        video_path="demo.mp4",
        model_size="small",
        device="cpu",
        compute_type="int8",
        language="zh",
        segment_duration=600,
        num_workers=2,
        hf_endpoint=None,
        config={"whisper": {"parallel": {"enabled": True}}},
        progress_callback=lambda event: first_run_events.append(dict(event)),
        segment_runtime_hooks=hooks,
    )

    assert subtitle_text == "A|B"
    assert len(extract_audio_calls) == 1
    assert [event["checkpoint"] for event in first_run_events] == [
        "transcribe_segment_1_restored",
        "transcribe_segment_2_completed",
    ]
    assert [event["signal_type"] for event in first_run_events] == ["hard", "hard"]
    assert [event["completed"] for event in first_run_events] == [1, 2]
    assert [event["pending"] for event in first_run_events] == [1, 0]

    chunk_rows = store.list_sqlite_chunk_records(stage="transcribe", status="SUCCESS", limit=10)
    assert sorted(row["chunk_id"] for row in chunk_rows) == ["ts000001", "ts000002"]
    assert not segment_one_chunk_dir.exists()
    assert not segment_two_chunk_dir.exists()

    chunk_payloads = store.batch_load_committed_chunk_payloads(
        [
            {
                "stage": "transcribe",
                "chunk_id": build_chunk_id(segment),
                "input_fingerprint": build_fingerprint(segment, len(SEGMENTS)),
            }
            for segment in SEGMENTS
        ]
    )
    assert [
        item["restored"]["result_payload"]["subtitles"][0]["text"]
        for item in chunk_payloads
    ] == ["A", "B"]

    monkeypatch.setattr(
        pt,
        "_extract_full_audio",
        lambda *args, **kwargs: pytest.fail("all segments restored should skip full audio extraction"),
    )
    monkeypatch.setattr(
        pt,
        "_build_process_pool_executor",
        lambda max_workers: pytest.fail("all segments restored should skip executor creation"),
    )
    monkeypatch.setattr(
        pt,
        "transcribe_segment",
        lambda args: pytest.fail("all segments restored should skip segment transcription"),
    )

    second_run_events = []
    subtitle_text = pt.transcribe_parallel(
        video_path="demo.mp4",
        model_size="small",
        device="cpu",
        compute_type="int8",
        language="zh",
        segment_duration=600,
        num_workers=2,
        hf_endpoint=None,
        config={"whisper": {"parallel": {"enabled": True}}},
        progress_callback=lambda event: second_run_events.append(dict(event)),
        segment_runtime_hooks=hooks,
    )

    assert subtitle_text == "A|B"
    assert [event["checkpoint"] for event in second_run_events] == [
        "transcribe_segment_1_restored",
        "transcribe_segment_2_restored",
    ]
    assert [event["signal_type"] for event in second_run_events] == ["hard", "hard"]
