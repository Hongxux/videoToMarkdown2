import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

from services.python_grpc.src.transcript_pipeline.llm.client import LLMResponse
from services.python_grpc.src.transcript_pipeline.nodes import phase2_preprocessing


class _DummyLogger:
    def start(self):
        return None

    def log_input(self, *_args, **_kwargs):
        return None

    def log_llm_call(self, *_args, **_kwargs):
        return None

    def info(self, *_args, **_kwargs):
        return None

    def log_substep(self, *_args, **_kwargs):
        return None

    def log_batch_summary(self, *_args, **_kwargs):
        return None

    def log_output(self, *_args, **_kwargs):
        return None

    def log_error(self, *_args, **_kwargs):
        return None

    def end(self, success=True):
        return {"duration_ms": 1.0, "success": success}


def _fake_response() -> LLMResponse:
    return LLMResponse(
        content="{}",
        prompt_tokens=10,
        completion_tokens=5,
        total_tokens=15,
        model="test-model",
        latency_ms=10.0,
    )


def _build_corrected(count: int):
    corrected = []
    for idx in range(1, count + 1):
        corrected.append(
            {
                "subtitle_id": f"SUB{idx:03d}",
                "corrected_text": f"subtitle {idx}",
                "start_sec": float(idx),
                "end_sec": float(idx) + 0.5,
            }
        )
    return corrected


def test_run_bounded_producer_consumer_preserves_order_and_cap():
    async def _run():
        active = 0
        peak = 0
        lock = asyncio.Lock()

        async def _handler(_idx: int, value: int) -> int:
            nonlocal active, peak
            async with lock:
                active += 1
                peak = max(peak, active)
            await asyncio.sleep(0.01)
            async with lock:
                active -= 1
            return value * 10

        results = await phase2_preprocessing._run_bounded_producer_consumer(
            [1, 2, 3, 4, 5],
            max_inflight=2,
            handler=_handler,
        )
        return results, peak

    results, peak = asyncio.run(_run())
    assert results == [10, 20, 30, 40, 50]
    assert peak <= 2


def test_step3_merge_uses_low_overlap_default(monkeypatch):
    class _CountingLLM:
        def __init__(self):
            self.calls = 0

        async def complete_json(self, prompt, system_prompt=None):
            assert system_prompt == phase2_preprocessing.MERGE_SYSTEM_PROMPT
            self.calls += 1
            source_ids = []
            for line in prompt.splitlines():
                if not line.startswith("[SUB"):
                    continue
                source_ids.append(line.split("]", 1)[0][1:])
            return {"merged_sentences": [{"text": "merged", "source_subtitle_ids": source_ids}]}, _fake_response()

    fake_llm = _CountingLLM()
    monkeypatch.setattr(phase2_preprocessing, "create_llm_client", lambda purpose="refinement": fake_llm)
    monkeypatch.setattr(phase2_preprocessing, "get_logger", lambda *_args, **_kwargs: _DummyLogger())
    monkeypatch.delenv("TRANSCRIPT_STEP3_WINDOW_OVERLAP", raising=False)

    state = {"output_dir": "output", "corrected_subtitles": _build_corrected(29)}
    result = asyncio.run(phase2_preprocessing.step3_node(state))

    # overlap=0, window_size=10 => stride=10, 29条输入切 3 窗。
    assert fake_llm.calls == 3
    assert len(result["merged_sentences"]) == 3


def test_step3_merge_respects_overlap_env(monkeypatch):
    class _CountingLLM:
        def __init__(self):
            self.calls = 0

        async def complete_json(self, prompt, system_prompt=None):
            assert system_prompt == phase2_preprocessing.MERGE_SYSTEM_PROMPT
            self.calls += 1
            source_ids = []
            for line in prompt.splitlines():
                if not line.startswith("[SUB"):
                    continue
                source_ids.append(line.split("]", 1)[0][1:])
            return {"merged_sentences": [{"text": "merged", "source_subtitle_ids": source_ids}]}, _fake_response()

    fake_llm = _CountingLLM()
    monkeypatch.setattr(phase2_preprocessing, "create_llm_client", lambda purpose="refinement": fake_llm)
    monkeypatch.setattr(phase2_preprocessing, "get_logger", lambda *_args, **_kwargs: _DummyLogger())
    monkeypatch.setenv("TRANSCRIPT_STEP3_WINDOW_OVERLAP", "5")

    state = {"output_dir": "output", "corrected_subtitles": _build_corrected(29)}
    result = asyncio.run(phase2_preprocessing.step3_node(state))

    # overlap=5, window_size=10 => stride=5, 窗口起点为 0/5/10/15/20/25，共 6 窗。
    assert fake_llm.calls == 6
    # 虽然窗口增多，但最终按原始顺序组装后仍应稳定为 3 个完整合并句。
    assert len(result["merged_sentences"]) == 3


def test_step3_merge_builds_passthrough_for_uncovered_subtitles(monkeypatch):
    class _MergeOnlyLLM:
        async def complete_json(self, _prompt, system_prompt=None):
            assert system_prompt == phase2_preprocessing.MERGE_SYSTEM_PROMPT
            # 仅返回“需要合并”的组；单条 SUB005 即使返回也应被本地逻辑丢弃为非合并组。
            payload = {
                "merged_groups": [
                    {"text": "merged 2-3", "source_subtitle_ids": ["SUB002", "SUB003"]},
                    {"text": "single 5", "source_subtitle_ids": ["SUB005"]},
                ]
            }
            return payload, _fake_response()

    monkeypatch.setattr(
        phase2_preprocessing,
        "create_llm_client",
        lambda purpose="refinement": _MergeOnlyLLM(),
    )
    monkeypatch.setattr(phase2_preprocessing, "get_logger", lambda *_args, **_kwargs: _DummyLogger())
    monkeypatch.delenv("TRANSCRIPT_STEP3_WINDOW_OVERLAP", raising=False)

    state = {"output_dir": "output", "corrected_subtitles": _build_corrected(5)}
    result = asyncio.run(phase2_preprocessing.step3_node(state))

    merged = result["merged_sentences"]
    assert [item["source_subtitle_ids"] for item in merged] == [
        ["SUB001"],
        ["SUB002", "SUB003"],
        ["SUB004"],
        ["SUB005"],
    ]
    assert [item["text"] for item in merged] == [
        "subtitle 1",
        "merged 2-3",
        "subtitle 4",
        "subtitle 5",
    ]
    assert [item["sentence_id"] for item in merged] == ["S001", "S002", "S003", "S004"]
