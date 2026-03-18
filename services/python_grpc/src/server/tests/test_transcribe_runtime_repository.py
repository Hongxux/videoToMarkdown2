from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

from services.python_grpc.src.server.transcribe_runtime_repository import (
    build_transcribe_repository_from_restored_rows,
    build_transcribe_runtime_repository,
    mark_transcribe_repository_completed,
    upsert_transcribe_runtime_segment,
)


def test_transcribe_runtime_repository_tracks_segment_lifecycle_and_rebuilds_subtitle_text(tmp_path):
    output_dir = str(tmp_path / "task")
    subtitle_path = str(tmp_path / "task" / "subtitles.txt")
    payload = build_transcribe_runtime_repository(
        output_dir=output_dir,
        subtitle_path=subtitle_path,
        task_id="task-transcribe",
        video_path="demo.mp4",
        language="zh",
        input_fingerprint="fp-transcribe",
    )

    upsert_transcribe_runtime_segment(
        payload,
        segment={"id": 0, "start": 0.0, "end": 10.0, "duration": 10.0},
        total_segments=2,
        chunk_id="ts000001",
        input_fingerprint="fp-seg-1",
        status="PLANNED",
    )
    upsert_transcribe_runtime_segment(
        payload,
        segment={"id": 1, "start": 10.0, "end": 20.0, "duration": 10.0},
        total_segments=2,
        chunk_id="ts000002",
        input_fingerprint="fp-seg-2",
        status="PLANNED",
    )
    upsert_transcribe_runtime_segment(
        payload,
        segment={"id": 0, "start": 0.0, "end": 10.0, "duration": 10.0},
        total_segments=2,
        chunk_id="ts000001",
        input_fingerprint="fp-seg-1",
        status="RUNNING",
    )
    upsert_transcribe_runtime_segment(
        payload,
        segment={"id": 0, "start": 0.0, "end": 10.0, "duration": 10.0},
        total_segments=2,
        chunk_id="ts000001",
        input_fingerprint="fp-seg-1",
        status="SUCCESS",
        result_payload={
            "segment_id": 0,
            "segment_index": 1,
            "total_segments": 2,
            "segment": {"id": 0, "start": 0.0, "end": 10.0, "duration": 10.0},
            "subtitles": [{"start": 0.0, "end": 1.0, "text": "A"}],
        },
    )
    upsert_transcribe_runtime_segment(
        payload,
        segment={"id": 1, "start": 10.0, "end": 20.0, "duration": 10.0},
        total_segments=2,
        chunk_id="ts000002",
        input_fingerprint="fp-seg-2",
        status="SUCCESS",
        result_payload={
            "segment_id": 1,
            "segment_index": 2,
            "total_segments": 2,
            "segment": {"id": 1, "start": 10.0, "end": 20.0, "duration": 10.0},
            "subtitles": [{"start": 10.0, "end": 11.0, "text": "B"}],
        },
    )

    assert payload["segment_count"] == 2
    assert payload["successful_segment_count"] == 2
    assert payload["running_segment_count"] == 0
    assert payload["status"] == "SUCCESS"
    assert payload["ready"] is True
    assert payload["subtitle_count"] == 2
    assert payload["subtitle_text"] == "[00:00:00] A\n[00:00:10] B"


def test_transcribe_runtime_repository_can_rebuild_from_restored_rows(tmp_path):
    output_dir = str(tmp_path / "task")
    subtitle_path = str(tmp_path / "task" / "subtitles.txt")
    payload = build_transcribe_repository_from_restored_rows(
        output_dir=output_dir,
        subtitle_path=subtitle_path,
        restored_rows=[
            {
                "request": {
                    "stage": "transcribe",
                    "chunk_id": "ts000001",
                    "input_fingerprint": "fp-seg-1",
                    "segment_id": 0,
                },
                "restored": {
                    "result_payload": {
                        "segment_id": 0,
                        "segment_index": 1,
                        "total_segments": 2,
                        "segment": {"id": 0, "start": 0.0, "end": 10.0, "duration": 10.0},
                        "subtitles": [{"start": 0.0, "end": 1.0, "text": "A"}],
                    }
                },
            },
            {
                "request": {
                    "stage": "transcribe",
                    "chunk_id": "ts000002",
                    "input_fingerprint": "fp-seg-2",
                    "segment_id": 1,
                },
                "restored": {
                    "result_payload": {
                        "segment_id": 1,
                        "segment_index": 2,
                        "total_segments": 2,
                        "segment": {"id": 1, "start": 10.0, "end": 20.0, "duration": 10.0},
                        "subtitles": [{"start": 10.0, "end": 11.0, "text": "B"}],
                    }
                },
            },
        ],
        task_id="task-transcribe",
        video_path="demo.mp4",
        language="zh",
        input_fingerprint="fp-transcribe",
    )

    assert payload["status"] == "SUCCESS"
    assert payload["successful_segment_count"] == 2
    assert payload["subtitle_text"] == "[00:00:00] A\n[00:00:10] B"


def test_transcribe_runtime_repository_can_mark_reused_subtitle_payload_ready(tmp_path):
    output_dir = str(tmp_path / "task")
    subtitle_path = str(tmp_path / "task" / "subtitles.txt")
    payload = build_transcribe_runtime_repository(
        output_dir=output_dir,
        subtitle_path=subtitle_path,
    )

    mark_transcribe_repository_completed(
        payload,
        subtitle_path=subtitle_path,
        subtitle_text="[00:00:00] hello",
        reused=True,
    )

    assert payload["status"] == "SUCCESS"
    assert payload["ready"] is True
    assert payload["reused"] is True
    assert payload["subtitle_text"] == "[00:00:00] hello"
