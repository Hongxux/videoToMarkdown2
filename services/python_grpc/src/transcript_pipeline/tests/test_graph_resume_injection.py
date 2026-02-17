import asyncio
import sys
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[5]))


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
        self.return_state = return_state or {}

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
            sqlite_checkpointer=None,
            main_logger=logger,
            resume_state={"corrected_subtitles": [{"subtitle_id": "SUB001", "corrected_text": "x"}]},
            resume_from_step="step2_correction",
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
                sqlite_checkpointer=None,
                main_logger=logger,
                resume_state=None,
                resume_from_step=None,
            )
        )
