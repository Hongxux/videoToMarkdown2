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


def test_step3_5_translation_falls_back_to_original_for_missing_item(monkeypatch):
    class _SingleItemLLM:
        async def complete_json(self, _prompt, system_prompt=None):
            assert system_prompt == phase2_preprocessing.TRANSLATION_SYSTEM_PROMPT
            return (
                {
                    "translated_sentences": [
                        {"sentence_id": "S001", "translated_text": "这是翻译后的第一句。"}
                    ]
                },
                _fake_response(),
            )

    monkeypatch.setattr(phase2_preprocessing, "create_llm_client", lambda purpose="refinement": _SingleItemLLM())
    monkeypatch.setattr(phase2_preprocessing, "get_logger", lambda *_args, **_kwargs: _DummyLogger())

    state = {
        "output_dir": "output",
        "merged_sentences": [
            {"sentence_id": "S001", "text": "first sentence", "start_sec": 0.0, "end_sec": 1.0, "source_subtitle_ids": ["SUB001"]},
            {"sentence_id": "S002", "text": "second sentence", "start_sec": 1.0, "end_sec": 2.0, "source_subtitle_ids": ["SUB002"]},
        ],
    }
    result = asyncio.run(phase2_preprocessing.step3_5_node(state))
    translated = result["translated_sentences"]

    assert translated[0]["text"] == "这是翻译后的第一句。"
    assert translated[1]["text"] == "second sentence"


def test_step3_5_translation_uses_window_size_50(monkeypatch):
    class _CountingLLM:
        def __init__(self):
            self.calls = 0

        async def complete_json(self, prompt, system_prompt=None):
            assert system_prompt == phase2_preprocessing.TRANSLATION_SYSTEM_PROMPT
            self.calls += 1
            translated = []
            for line in prompt.splitlines():
                if not line.startswith("[S"):
                    continue
                sentence_id = line.split("]", 1)[0][1:]
                translated.append(
                    {"sentence_id": sentence_id, "translated_text": f"译文-{sentence_id}"}
                )
            return {"translated_sentences": translated}, _fake_response()

    fake_llm = _CountingLLM()
    monkeypatch.setattr(phase2_preprocessing, "create_llm_client", lambda purpose="refinement": fake_llm)
    monkeypatch.setattr(phase2_preprocessing, "get_logger", lambda *_args, **_kwargs: _DummyLogger())

    merged_sentences = []
    for idx in range(1, 102):
        merged_sentences.append(
            {
                "sentence_id": f"S{idx:03d}",
                "text": f"sentence {idx}",
                "start_sec": float(idx),
                "end_sec": float(idx) + 0.5,
                "source_subtitle_ids": [f"SUB{idx:03d}"],
            }
        )

    state = {"output_dir": "output", "merged_sentences": merged_sentences}
    result = asyncio.run(phase2_preprocessing.step3_5_node(state))

    assert fake_llm.calls == 3
    assert len(result["translated_sentences"]) == 101
    assert result["translated_sentences"][100]["text"] == "译文-S101"


def test_step3_5_translation_skips_chinese_sentences(monkeypatch):
    def _should_not_create_llm(*_args, **_kwargs):
        raise AssertionError("LLM should not be created for all-Chinese input")

    monkeypatch.setattr(phase2_preprocessing, "create_llm_client", _should_not_create_llm)
    monkeypatch.setattr(phase2_preprocessing, "get_logger", lambda *_args, **_kwargs: _DummyLogger())

    state = {
        "output_dir": "output",
        "merged_sentences": [
            {"sentence_id": "S001", "text": "这是第一句中文。", "start_sec": 0.0, "end_sec": 1.0, "source_subtitle_ids": ["SUB001"]},
            {"sentence_id": "S002", "text": "这是第二句中文。", "start_sec": 1.0, "end_sec": 2.0, "source_subtitle_ids": ["SUB002"]},
        ],
    }

    result = asyncio.run(phase2_preprocessing.step3_5_node(state))
    translated = result["translated_sentences"]

    assert translated[0]["text"] == "这是第一句中文。"
    assert translated[1]["text"] == "这是第二句中文。"
    assert result["token_usage"]["step3_5_translate"] == 0


def test_step3_5_translation_prompt_preserves_named_terms():
    assert "中文译名（英文原词）" in phase2_preprocessing.TRANSLATION_PROMPT
    assert "深度求索（deepseek）" in phase2_preprocessing.TRANSLATION_PROMPT
    assert '"t"' in phase2_preprocessing.TRANSLATION_PROMPT
    assert '"sid"' in phase2_preprocessing.TRANSLATION_PROMPT
    assert '"tt"' in phase2_preprocessing.TRANSLATION_PROMPT
    assert "深度求索（deepseek）" in phase2_preprocessing.TRANSLATION_SYSTEM_PROMPT


def test_step3_5_translation_ignores_llm_time_and_source_fields(monkeypatch):
    class _NoisyLLM:
        async def complete_json(self, _prompt, system_prompt=None):
            assert system_prompt == phase2_preprocessing.TRANSLATION_SYSTEM_PROMPT
            return (
                {
                    "t": [
                        {
                            "sid": "S001",
                            "tt": "第一句译文",
                            "start_sec": 99.0,
                            "end_sec": 199.0,
                            "source_subtitle_ids": ["BAD001"],
                        }
                    ]
                },
                _fake_response(),
            )

    monkeypatch.setattr(phase2_preprocessing, "create_llm_client", lambda purpose="refinement": _NoisyLLM())
    monkeypatch.setattr(phase2_preprocessing, "get_logger", lambda *_args, **_kwargs: _DummyLogger())

    state = {
        "output_dir": "output",
        "merged_sentences": [
            {"sentence_id": "S001", "text": "first sentence", "start_sec": 1.0, "end_sec": 2.0, "source_subtitle_ids": ["SUB001"]},
        ],
    }
    result = asyncio.run(phase2_preprocessing.step3_5_node(state))
    translated = result["translated_sentences"][0]

    assert translated["sentence_id"] == "S001"
    assert translated["text"] == "第一句译文"
    assert translated["start_sec"] == 1.0
    assert translated["end_sec"] == 2.0
    assert translated["source_subtitle_ids"] == ["SUB001"]
