from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

from services.python_grpc.src.server.stage1_runtime_repository import (
    apply_stage1_progress_event,
    build_stage1_repository_from_projected_state,
    build_stage1_runtime_repository,
    get_stage1_repository_views,
    mark_stage1_runtime_outputs_ready,
)


def test_stage1_runtime_repository_tracks_progress_and_outputs(tmp_path):
    output_dir = str(tmp_path / "task")
    payload = build_stage1_runtime_repository(
        output_dir=output_dir,
        subtitle_path=str(tmp_path / "task" / "subtitles.txt"),
        task_id="task-stage1",
        video_path="demo.mp4",
        max_step=6,
        input_fingerprint="fp-stage1",
        resume_from_step="step2_correction",
        resume_entry_step="step3_merge",
        recovery_plan_digest="digest-stage1",
    )

    apply_stage1_progress_event(
        payload,
        event={
            "event": "step_completed",
            "stage": "stage1",
            "step_name": "step2_correction",
            "checkpoint": "step2_correction",
            "completed": 2,
            "pending": 4,
            "status": "running",
        },
    )
    apply_stage1_progress_event(
        payload,
        event={
            "event": "llm_call_completed",
            "stage": "stage1",
            "step_name": "stage1_step3_merge",
            "stage_step": "stage1_step3_merge",
            "checkpoint": "step3_merge.llm_call.window_0001",
            "completed": 2,
            "pending": 4,
            "status": "running",
        },
    )

    assert payload["status"] == "RUNNING"
    assert payload["current_step"] == "step3_merge"
    assert payload["step_statuses"]["step2_correction"] == "SUCCESS"
    assert payload["step_statuses"]["step3_merge"] == "RUNNING"

    mark_stage1_runtime_outputs_ready(
        payload,
        final_state={
            "corrected_subtitles": [{"subtitle_id": "S001", "text": "hello"}],
            "merged_sentences": [{"sentence_id": "X001", "start_sec": 0.0, "end_sec": 1.0}],
            "translated_sentences": [{"sentence_id": "X001", "start_sec": 0.0, "end_sec": 1.0}],
            "pure_text_script": [{"paragraph_id": "P001", "text": "world"}],
            "sentence_timestamps": {"X001": {"start_sec": 0.0, "end_sec": 1.0}},
            "domain": "cs",
            "main_topic": "sorting",
        },
        reused=False,
    )

    assert payload["status"] == "SUCCESS"
    assert payload["ready"] is True
    assert isinstance(payload.get("views"), dict)
    assert payload["views"]["step2_subtitles"][0]["subtitle_id"] == "S001"
    assert payload["views"]["step6_paragraphs"][0]["paragraph_id"] == "P001"
    assert payload["sentence_timestamps_count"] == 1
    assert payload["step_statuses"]["step5_6_dedup_merge"] == "SUCCESS"
    assert payload["output_fingerprint"]
    assert get_stage1_repository_views(payload)["domain"] == "cs"


def test_stage1_runtime_repository_can_rebuild_from_projected_state(tmp_path):
    output_dir = str(tmp_path / "task")
    payload = build_stage1_repository_from_projected_state(
        output_dir=output_dir,
        projected_state={
            "corrected_subtitles": [{"subtitle_id": "S101", "text": "demo"}],
            "pure_text_script": [{"paragraph_id": "P101", "text": "demo"}],
            "sentence_timestamps": {"S101": {"start_sec": 0.0, "end_sec": 1.0}},
            "domain": "math",
            "main_topic": "algebra",
        },
        subtitle_path=str(tmp_path / "task" / "subtitles.txt"),
        task_id="task-stage1",
        video_path="demo.mp4",
        max_step=6,
        input_fingerprint="fp-stage1",
    )

    assert isinstance(payload, dict)
    assert payload["ready"] is True
    assert payload["reused"] is True
    assert payload["status"] == "SUCCESS"
    assert payload["views"]["step2_subtitles"][0]["subtitle_id"] == "S101"
    assert payload["views"]["step6_paragraphs"][0]["paragraph_id"] == "P101"
