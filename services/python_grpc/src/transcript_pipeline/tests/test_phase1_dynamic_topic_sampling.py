import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

from services.python_grpc.src.transcript_pipeline.nodes import phase1_preparation


def _build_subtitles(total: int):
    return [
        {
            "subtitle_id": f"SUB{idx + 1:03d}",
            "start_sec": float(idx * 3),
            "end_sec": float(idx * 3 + 2),
            "text": f"line-{idx}",
        }
        for idx in range(total)
    ]


def test_resolve_topic_sample_count_scales_with_duration():
    assert phase1_preparation._resolve_topic_sample_count(5 * 60, 500) == 20
    assert phase1_preparation._resolve_topic_sample_count(30 * 60, 500) == 30
    assert phase1_preparation._resolve_topic_sample_count(6 * 3600, 5000) == 120
    assert phase1_preparation._resolve_topic_sample_count(6 * 3600, 15) == 15


def test_pick_uniform_subtitle_samples_keeps_count_and_edges():
    subtitles = _build_subtitles(101)
    samples = phase1_preparation._pick_uniform_subtitle_samples(subtitles, 21)

    assert len(samples) == 21
    assert samples[0]["subtitle_id"] == "SUB001"
    assert samples[-1]["subtitle_id"] == "SUB101"


def test_build_topic_sample_text_limits_total_chars():
    subtitles = _build_subtitles(40)
    for subtitle in subtitles:
        subtitle["text"] = "x" * 120

    sample_text = phase1_preparation._build_topic_sample_text(subtitles, max_chars=280)

    assert len(sample_text) <= 280
    assert sample_text


def test_step1_reuses_existing_topic_from_state(monkeypatch):
    class _DummyLogger:
        def start(self):
            return None

        def log_input(self, *_args, **_kwargs):
            return None

        def info(self, *_args, **_kwargs):
            return None

        def log_tool_call(self, *_args, **_kwargs):
            return None

        def log_warning(self, *_args, **_kwargs):
            return None

        def log_output(self, *_args, **_kwargs):
            return None

        def end(self, success=True):
            return {"duration_ms": 1.0, "success": success}

        def log_error(self, *_args, **_kwargs):
            return None

    monkeypatch.setattr(phase1_preparation, "get_logger", lambda *_args, **_kwargs: _DummyLogger())
    monkeypatch.setattr(phase1_preparation, "validate_video", lambda _path: (True, ""))
    monkeypatch.setattr(phase1_preparation, "validate_subtitle", lambda _path: (True, ""))
    monkeypatch.setattr(phase1_preparation, "extract_video_title", lambda _path: "video-title")
    monkeypatch.setattr(
        phase1_preparation,
        "read_subtitle_sample",
        lambda _path, count=None: (_ for _ in ()).throw(AssertionError("Should skip subtitle sampling")),
    )
    monkeypatch.setattr(
        phase1_preparation,
        "create_llm_client",
        lambda purpose="topic": (_ for _ in ()).throw(AssertionError("Should skip LLM inference")),
    )
    monkeypatch.setenv("TRANSCRIPT_STEP1_REUSE_INFERRED_TOPIC", "1")

    state = {
        "output_dir": "output",
        "video_path": "video.mp4",
        "subtitle_path": "subtitle.json",
        "domain": "计算机科学",
        "main_topic": "AI编程助手OpenClaw的开发与使用",
    }
    result = asyncio.run(phase1_preparation.step1_node(state))

    assert result["is_valid"] is True
    assert result["domain"] == "计算机科学"
    assert result["main_topic"] == "AI编程助手OpenClaw的开发与使用"
    assert result["token_usage"]["step1_validate"] == 0
    assert result["llm_calls"] == []


def test_step1_uses_env_sample_budget(monkeypatch):
    class _DummyLogger:
        def start(self):
            return None

        def log_input(self, *_args, **_kwargs):
            return None

        def info(self, *_args, **_kwargs):
            return None

        def log_tool_call(self, *_args, **_kwargs):
            return None

        def log_warning(self, *_args, **_kwargs):
            return None

        def log_llm_call(self, *_args, **_kwargs):
            return None

        def log_output(self, *_args, **_kwargs):
            return None

        def end(self, success=True):
            return {"duration_ms": 1.0, "success": success}

        def log_error(self, *_args, **_kwargs):
            return None

    class _DummyLLM:
        async def complete_json(self, _prompt, system_prompt=None):
            assert system_prompt == phase1_preparation.TOPIC_INFERENCE_SYSTEM_PROMPT
            return {"domain": "计算机科学", "main_topic": "主题"}, _DummyResponse()

    class _DummyResponse:
        content = "{}"
        prompt_tokens = 10
        completion_tokens = 5
        total_tokens = 15
        model = "test-model"
        latency_ms = 10.0

    recorded = {"max_chars": None}

    def _fake_build_topic_sample_text(sample_subtitles, max_chars=phase1_preparation.TOPIC_SAMPLE_MAX_CHARS):
        recorded["max_chars"] = max_chars
        return "sample"

    subtitles = [
        {"subtitle_id": "SUB001", "start_sec": 0.0, "end_sec": 1.0, "text": "one"},
        {"subtitle_id": "SUB002", "start_sec": 1.0, "end_sec": 2.0, "text": "two"},
    ]

    monkeypatch.setattr(phase1_preparation, "get_logger", lambda *_args, **_kwargs: _DummyLogger())
    monkeypatch.setattr(phase1_preparation, "validate_video", lambda _path: (True, ""))
    monkeypatch.setattr(phase1_preparation, "validate_subtitle", lambda _path: (True, ""))
    monkeypatch.setattr(phase1_preparation, "extract_video_title", lambda _path: "video-title")
    monkeypatch.setattr(phase1_preparation, "read_subtitle_sample", lambda _path, count=None: subtitles)
    monkeypatch.setattr(phase1_preparation, "_build_topic_sample_text", _fake_build_topic_sample_text)
    monkeypatch.setattr(phase1_preparation, "create_llm_client", lambda purpose="topic": _DummyLLM())
    monkeypatch.setenv("TRANSCRIPT_STEP1_REUSE_INFERRED_TOPIC", "0")
    monkeypatch.setenv("TRANSCRIPT_STEP1_SAMPLE_MAX_CHARS", "1234")

    state = {
        "output_dir": "output",
        "video_path": "video.mp4",
        "subtitle_path": "subtitle.json",
    }
    result = asyncio.run(phase1_preparation.step1_node(state))

    assert result["is_valid"] is True
    assert recorded["max_chars"] == 1234
