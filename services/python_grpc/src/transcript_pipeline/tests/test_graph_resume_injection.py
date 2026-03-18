import asyncio
import sys
import tempfile
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[5]))


from services.python_grpc.src.transcript_pipeline import graph as stage1_graph
from services.python_grpc.src.transcript_pipeline.graph import _execute_pipeline


class _DummyLogger:
    def info(self, *_args, **_kwargs):
        return None

    def warning(self, *_args, **_kwargs):
        return None

    def error(self, *_args, **_kwargs):
        return None


class _CaptureGraph:
    def __init__(self, return_state=None):
        self.initial_state = None
        self.return_state = return_state or {
            "current_step": "step5_6_dedup_merge",
            "current_step_status": "completed",
            "is_valid": True,
            "errors": [],
        }

    async def ainvoke(self, initial_state, _config):
        self.initial_state = initial_state
        merged = dict(initial_state)
        merged.update(self.return_state)
        return merged


def test_execute_pipeline_injects_resume_state_and_step_override():
    graph = _CaptureGraph()
    logger = _DummyLogger()

    final_state = asyncio.run(
        _execute_pipeline(
            graph,
            video_path="video.mp4",
            subtitle_path="subs.txt",
            output_dir="output",
            thread_id="thread-demo",
            resume=False,
            tracer=None,
            metrics=None,
            main_logger=logger,
            resume_state={"corrected_subtitles": [{"subtitle_id": "SUB001", "corrected_text": "x"}]},
            resume_from_step="step2_correction",
            resume_plan=None,
        )
    )

    assert graph.initial_state is not None
    assert graph.initial_state.get("_resume_mode") is True
    assert graph.initial_state.get("_last_completed_index") == 2
    assert graph.initial_state["corrected_subtitles"][0]["subtitle_id"] == "SUB001"
    assert final_state["_resume_mode"] is True


@pytest.mark.parametrize(
    "return_state",
    [
        {
            "current_step": "step1_validate",
            "current_step_status": "error",
            "errors": [{"step": "step1", "type": "exception", "error": "DeepSeek API error: 401"}],
        },
        {
            "current_step": "step1_validate",
            "current_step_status": "failed",
            "is_valid": False,
            "errors": [{"step": "step1", "type": "subtitle_validation", "error": "missing subtitle"}],
        },
    ],
)
def test_execute_pipeline_raises_when_final_state_indicates_failure(return_state):
    graph = _CaptureGraph(return_state=return_state)
    logger = _DummyLogger()

    with pytest.raises(RuntimeError, match="Pipeline ended with"):
        asyncio.run(
            _execute_pipeline(
                graph,
                video_path="video.mp4",
                subtitle_path="subs.txt",
                output_dir="output",
                thread_id="thread-demo",
                resume=False,
                tracer=None,
                metrics=None,
                main_logger=logger,
                resume_state=None,
                resume_from_step=None,
                resume_plan=None,
            )
        )


def test_execute_pipeline_injects_resume_plan():
    graph = _CaptureGraph()
    logger = _DummyLogger()

    final_state = asyncio.run(
        _execute_pipeline(
            graph,
            video_path="video.mp4",
            subtitle_path="subs.txt",
            output_dir="output",
            thread_id="thread-demo",
            resume=False,
            tracer=None,
            metrics=None,
            main_logger=logger,
            resume_state=None,
            resume_from_step=None,
            resume_plan={
                "resume_state": {
                    "corrected_subtitles": [{"subtitle_id": "SUB001", "corrected_text": "x"}],
                },
                "resume_from_step": "step2_correction",
                "resume_entry_step": "step3_merge",
                "resume_last_completed_index": 2,
                "retry_entry_point": "step3_merge",
                "dirty_scope_refs": ["stage1/artifact/step5_6_dedup_merge"],
                "dirty_scope_count": 1,
            },
        )
    )

    assert graph.initial_state is not None
    assert graph.initial_state.get("_resume_mode") is True
    assert graph.initial_state.get("_last_completed_index") == 2
    assert graph.initial_state.get("_resume_entry_step") == "step3_merge"
    assert graph.initial_state["_resume_plan"]["dirty_scope_count"] == 1
    assert graph.initial_state["corrected_subtitles"][0]["subtitle_id"] == "SUB001"
    assert final_state["_resume_mode"] is True


def test_build_stage1_llm_progress_event_uses_llm_call_granularity():
    event = stage1_graph._build_stage1_llm_progress_event(
        {
            "event": "llm_call_completed",
            "stage": "stage1",
            "stage_step": "step2_correction",
            "unit_id": "batch_0001",
            "llm_call_id": "stage1_step2_correction.batch_0001",
        },
        max_step=6,
    )

    assert event["step_name"] == "step2_correction"
    assert event["checkpoint"] == "step2_correction.llm_call.batch_0001"
    assert event["completed"] == 1
    assert event["pending"] == 5
    assert event["signal_type"] == "hard"


def test_run_pipeline_no_longer_creates_stage1_checkpoint_db(monkeypatch):
    tmp_root = Path("var")
    tmp_root.mkdir(exist_ok=True)
    output_dir = Path(tempfile.mkdtemp(prefix="tmp_stage1_no_sqlite_", dir=str(tmp_root.resolve())))

    async def _fake_execute_pipeline(
        graph,
        video_path,
        subtitle_path,
        output_dir,
        thread_id,
        resume,
        tracer,
        metrics,
        main_logger,
        resume_state=None,
        resume_from_step=None,
        resume_plan=None,
        progress_callback=None,
        max_step=6,
        task_id="",
        disable_output_persistence=False,
    ):
        return {
            "graph": graph,
            "video_path": video_path,
            "subtitle_path": subtitle_path,
            "output_dir": output_dir,
            "thread_id": thread_id,
            "resume": resume,
            "resume_plan": resume_plan,
        }

    monkeypatch.setattr(
        stage1_graph,
        "should_use_streaming_stage1_executor",
        lambda **_kwargs: (False, "test"),
    )
    monkeypatch.setattr(stage1_graph, "create_pipeline_graph", lambda **_kwargs: _CaptureGraph())
    monkeypatch.setattr(stage1_graph, "_execute_pipeline", _fake_execute_pipeline)

    result = asyncio.run(
        stage1_graph.run_pipeline(
            video_path="video.mp4",
            subtitle_path="subs.txt",
            output_dir=str(output_dir),
            resume_plan={"resume_last_completed_index": 2},
        )
    )

    assert result["output_dir"] == str(output_dir)
    assert not (output_dir / "checkpoints.db").exists()
