import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

from services.python_grpc.src.transcript_pipeline.llm.client import LLMResponse
from services.python_grpc.src.transcript_pipeline.nodes import phase2_preprocessing
from services.python_grpc.src.transcript_pipeline.tools import file_validator


class _DummyLogger:
    def start(self):
        return None

    def log_input(self, *_args, **_kwargs):
        return None

    def log_llm_call(self, *_args, **_kwargs):
        return None

    def info(self, *_args, **_kwargs):
        return None

    def warning(self, *_args, **_kwargs):
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


def _extract_prefixed_ids(prompt: str, prefix: str):
    ids = []
    for line in prompt.splitlines():
        marker = f"[{prefix}"
        if marker not in line:
            continue
        marker_start = line.find(marker)
        ids.append(line[marker_start:].split("]", 1)[0][1:])
    return ids


def _build_cleaned_sentences(count: int):
    rows = []
    for idx in range(1, count + 1):
        rows.append(
            {
                "sentence_id": f"S{idx:03d}",
                "cleaned_text": f"cleaned {idx}",
            }
        )
    return rows


def test_step2_respects_batch_size_env(monkeypatch):
    class _Step2LLM:
        def __init__(self):
            self.calls = 0

        async def complete_json(self, prompt, system_prompt=None):
            assert system_prompt == phase2_preprocessing.CORRECTION_SYSTEM_PROMPT
            assert "能 1 个就不要 2 个" in prompt
            assert f"最多到 {phase2_preprocessing.STEP2_CONTEXT_WINDOW_MAX}" in prompt
            self.calls += 1
            correction_items = []
            for subtitle_id in _extract_prefixed_ids(prompt, "SUB"):
                subtitle_num = int(subtitle_id.replace("SUB", ""))
                correction_items.append(
                    {
                        "sid": subtitle_id,
                        "o": "subtitle",
                        "c": "caption",
                        "l": "",
                        "r": f" {subtitle_num}",
                    }
                )
            return {"c": correction_items}, _fake_response()

    class _DummyStorage:
        def __init__(self, _base_dir):
            pass

        def save_subtitle_timestamps(self, _payload):
            return None

    subtitles = []
    for idx in range(1, 42):
        subtitles.append(
            {
                "subtitle_id": f"SUB{idx:03d}",
                "text": f"subtitle {idx}",
                "start_sec": float(idx),
                "end_sec": float(idx) + 0.5,
            }
        )

    fake_llm = _Step2LLM()
    monkeypatch.setattr(phase2_preprocessing, "create_llm_client", lambda purpose="refinement": fake_llm)
    monkeypatch.setattr(phase2_preprocessing, "get_logger", lambda *_args, **_kwargs: _DummyLogger())
    monkeypatch.setattr(phase2_preprocessing, "LocalStorage", _DummyStorage)
    monkeypatch.setattr(file_validator, "read_subtitle_sample", lambda _path, count=None: subtitles)
    monkeypatch.setenv("TRANSCRIPT_STEP2_BATCH_SIZE", "20")

    state = {"output_dir": "output", "subtitle_path": "dummy", "domain": "test"}
    result = asyncio.run(phase2_preprocessing.step2_node(state))

    assert fake_llm.calls == 3
    assert len(result["corrected_subtitles"]) == 41
    assert result["corrected_subtitles"][0]["subtitle_id"] == "SUB001"
    assert result["corrected_subtitles"][0]["corrected_text"] == "caption 1"
    assert result["corrected_subtitles"][-1]["corrected_text"] == "caption 41"
    assert len(result["correction_summary"]) == 41
    assert result["correction_summary"][0]["subtitle_id"] == "SUB001"


def test_step2_merged_cleanup_applies_removal_patch(monkeypatch):
    class _Step2LLM:
        async def complete_json(self, prompt, system_prompt=None):
            assert system_prompt == phase2_preprocessing.CORRECTION_SYSTEM_PROMPT
            assert "[ADDITIONAL TASK: LOCAL CLEANUP PATCH]" in prompt
            return {
                "c": [
                    {"sid": "SUB001", "o": "subtitle", "c": "caption", "l": "", "r": " one"},
                ],
                "d": [
                    {"sid": "SUB001", "o": " like", "l": "caption one", "r": " demo"},
                ],
            }, _fake_response()

    class _DummyStorage:
        def __init__(self, _base_dir):
            pass

        def save_subtitle_timestamps(self, _payload):
            return None

    subtitles = [
        {
            "subtitle_id": "SUB001",
            "text": "subtitle one like demo",
            "start_sec": 1.0,
            "end_sec": 1.5,
        }
    ]

    monkeypatch.setattr(phase2_preprocessing, "create_llm_client", lambda purpose="refinement": _Step2LLM())
    monkeypatch.setattr(phase2_preprocessing, "get_logger", lambda *_args, **_kwargs: _DummyLogger())
    monkeypatch.setattr(phase2_preprocessing, "LocalStorage", _DummyStorage)
    monkeypatch.setattr(file_validator, "read_subtitle_sample", lambda _path, count=None: subtitles)

    result = asyncio.run(phase2_preprocessing.step2_node({"output_dir": "output", "subtitle_path": "dummy"}))

    assert result["corrected_subtitles"][0]["corrected_text"] == "caption one demo"
    assert result[phase2_preprocessing.STEP2_STEP4_MERGED_STATE_FLAG] is True
    assert result["token_usage"]["step2_correction"] == 15
    metrics = result["step_observability"]["step2_correction"]
    assert metrics["cleanup_final_applied_removals"] == 1


def test_step3_5_respects_window_size_env(monkeypatch):
    class _Step35LLM:
        def __init__(self):
            self.calls = 0

        async def complete_json(self, prompt, system_prompt=None):
            assert system_prompt == phase2_preprocessing.TRANSLATION_SYSTEM_PROMPT
            self.calls += 1
            translated_sentences = []
            for sentence_id in _extract_prefixed_ids(prompt, "S"):
                translated_sentences.append(
                    {
                        "sentence_id": sentence_id,
                        "translated_text": f"translated-{sentence_id}",
                    }
                )
            return {"translated_sentences": translated_sentences}, _fake_response()

    merged = []
    for idx in range(1, 42):
        merged.append(
            {
                "sentence_id": f"S{idx:03d}",
                "text": f"sentence {idx}",
                "start_sec": float(idx),
                "end_sec": float(idx) + 0.5,
                "source_subtitle_ids": [f"SUB{idx:03d}"],
            }
        )

    fake_llm = _Step35LLM()
    monkeypatch.setattr(phase2_preprocessing, "create_llm_client", lambda purpose="refinement": fake_llm)
    monkeypatch.setattr(phase2_preprocessing, "get_logger", lambda *_args, **_kwargs: _DummyLogger())
    monkeypatch.setenv("TRANSCRIPT_STEP35_WINDOW_SIZE", "20")

    result = asyncio.run(phase2_preprocessing.step3_5_node({"output_dir": "output", "merged_sentences": merged}))

    assert fake_llm.calls == 3
    assert len(result["translated_sentences"]) == 41


def test_step4_reassembles_passthrough_by_time_and_blocks_bilingual_pair_drop(monkeypatch):
    class _Step4LLM:
        async def complete_json(self, prompt, system_prompt=None):
            assert system_prompt == phase2_preprocessing.CLEAN_LOCAL_SYSTEM_PROMPT
            # 故意不返回 S003，验证本地直通补齐。
            return {
                "d": [
                    {"sid": "S001", "o": " 嗯 ", "l": "第一句", "r": "我我我想说"},
                    {"sid": "S001", "o": "我我", "l": "第一句", "r": "我想说"},
                    {"sid": "S002", "o": "（agent）", "l": "智能体", "r": "会调用"},
                ]
            }, _fake_response()

    class _DummyStorage:
        def __init__(self, _base_dir):
            self.saved_payload = None

        def save_sentence_timestamps(self, payload):
            self.saved_payload = payload

    merged = [
        {
            "sentence_id": "S002",
            "text": "第二句讲智能体（agent）会调用工具",
            "start_sec": 2.0,
            "end_sec": 2.5,
            "source_subtitle_ids": ["SUB002"],
        },
        {
            "sentence_id": "S001",
            "text": "第一句 嗯 我我我想说",
            "start_sec": 1.0,
            "end_sec": 1.5,
            "source_subtitle_ids": ["SUB001"],
        },
        {
            "sentence_id": "S003",
            "text": "第三句保持原样",
            "start_sec": 3.0,
            "end_sec": 3.5,
            "source_subtitle_ids": ["SUB003"],
        },
    ]

    monkeypatch.setattr(phase2_preprocessing, "create_llm_client", lambda purpose="refinement": _Step4LLM())
    monkeypatch.setattr(phase2_preprocessing, "get_logger", lambda *_args, **_kwargs: _DummyLogger())
    monkeypatch.setattr(phase2_preprocessing, "LocalStorage", _DummyStorage)

    result = asyncio.run(
        phase2_preprocessing.step4_node(
            {
                "output_dir": "output",
                "translated_sentences": merged,
            }
        )
    )

    cleaned = result["cleaned_sentences"]
    assert [item["sentence_id"] for item in cleaned] == ["S001", "S002", "S003"]
    cleaned_by_id = {item["sentence_id"]: item for item in cleaned}
    assert cleaned_by_id["S001"]["cleaned_text"] == "第一句我想说"
    # 双语对照被误删时应回退原句。
    assert cleaned_by_id["S002"]["cleaned_text"] == "第二句讲智能体（agent）会调用工具"
    # LLM 未覆盖句子由本地直通补齐。
    assert cleaned_by_id["S003"]["cleaned_text"] == "第三句保持原样"
    assert all("removed_items" not in item for item in cleaned)

    metrics = result["step_observability"]["step4_clean_local"]
    assert metrics["sentence_passthrough_used"] == 1
    assert metrics["bilingual_pair_guard_fallback_used"] == 1


def test_step4_prompt_no_removed_items_and_keeps_bilingual_terms():
    assert '"removed_items"' not in phase2_preprocessing.CLEAN_LOCAL_PROMPT
    assert "保留中英文对照术语" in phase2_preprocessing.CLEAN_LOCAL_PROMPT
    assert '"d"' in phase2_preprocessing.CLEAN_LOCAL_PROMPT
    assert '"sid"' in phase2_preprocessing.CLEAN_LOCAL_PROMPT


def test_step4_uses_passthrough_when_step2_step4_merged_done(monkeypatch):
    class _NoCallLLM:
        async def complete_json(self, prompt, system_prompt=None):
            raise AssertionError("LLM should not be called when merged mode is enabled")

    class _DummyStorage:
        def __init__(self, _base_dir):
            self.saved_payload = None

        def save_sentence_timestamps(self, payload):
            self.saved_payload = payload

    merged = [
        {
            "sentence_id": "S001",
            "text": "first sentence",
            "start_sec": 2.0,
            "end_sec": 2.5,
            "source_subtitle_ids": ["SUB001"],
        },
        {
            "sentence_id": "S002",
            "text": "second sentence",
            "start_sec": 1.0,
            "end_sec": 1.5,
            "source_subtitle_ids": ["SUB002"],
        },
    ]

    monkeypatch.setattr(phase2_preprocessing, "create_llm_client", lambda purpose="refinement": _NoCallLLM())
    monkeypatch.setattr(phase2_preprocessing, "get_logger", lambda *_args, **_kwargs: _DummyLogger())
    monkeypatch.setattr(phase2_preprocessing, "LocalStorage", _DummyStorage)

    result = asyncio.run(
        phase2_preprocessing.step4_node(
            {
                "output_dir": "output",
                "translated_sentences": merged,
                phase2_preprocessing.STEP2_STEP4_MERGED_STATE_FLAG: True,
            }
        )
    )

    assert result["token_usage"]["step4_clean_local"] == 0
    assert [item["sentence_id"] for item in result["cleaned_sentences"]] == ["S002", "S001"]
    assert result["cleaned_sentences"][0]["cleaned_text"] == "second sentence"
    assert result["cleaned_sentences"][1]["cleaned_text"] == "first sentence"
    metrics = result["step_observability"]["step4_clean_local"]
    assert metrics["compat_passthrough_mode_used"] == 1
    assert metrics["passthrough_sentence_count"] == 2


def test_step56_respects_window_overlap_env(monkeypatch):
    class _Step56LLM:
        def __init__(self):
            self.calls = 0

        async def complete_json(self, prompt, system_prompt=None):
            assert system_prompt == phase2_preprocessing.STEP56_DEDUP_MERGE_SYSTEM_PROMPT
            self.calls += 1
            ids = _extract_prefixed_ids(prompt, "S")
            paragraphs = [
                {
                    "text": f"paragraph-{sentence_id}",
                    "source_sentence_ids": [sentence_id],
                    "merge_type": "single",
                }
                for sentence_id in ids
            ]
            return {"keep_sentence_ids": ids, "paragraphs": paragraphs}, _fake_response()

    fake_llm = _Step56LLM()
    monkeypatch.setattr(phase2_preprocessing, "create_llm_client", lambda purpose="analysis": fake_llm)
    monkeypatch.setattr(phase2_preprocessing, "get_logger", lambda *_args, **_kwargs: _DummyLogger())
    monkeypatch.setenv("TRANSCRIPT_STEP56_WINDOW_SIZE", "8")
    monkeypatch.setenv("TRANSCRIPT_STEP56_WINDOW_OVERLAP", "1")

    result = asyncio.run(
        phase2_preprocessing.step5_6_node(
            {
                "output_dir": "output",
                "cleaned_sentences": _build_cleaned_sentences(23),
                "main_topic": "topic",
            }
        )
    )

    assert fake_llm.calls == 4
    assert len(result["non_redundant_sentences"]) == 23
    assert len(result["pure_text_script"]) == 23
    assert result["current_step"] == "step5_6_dedup_merge"


def test_step56_compatible_with_strict_info_logger_signature(monkeypatch):
    class _Step56LLM:
        async def complete_json(self, prompt, system_prompt=None):
            ids = _extract_prefixed_ids(prompt, "S")
            paragraphs = [
                {
                    "text": f"paragraph-{sentence_id}",
                    "source_sentence_ids": [sentence_id],
                    "merge_type": "single",
                }
                for sentence_id in ids
            ]
            return {"keep_sentence_ids": ids, "paragraphs": paragraphs}, _fake_response()

    class _StrictInfoLogger(_DummyLogger):
        def info(self, message, **_kwargs):
            return None

    monkeypatch.setattr(phase2_preprocessing, "create_llm_client", lambda purpose="analysis": _Step56LLM())
    monkeypatch.setattr(phase2_preprocessing, "get_logger", lambda *_args, **_kwargs: _StrictInfoLogger())
    monkeypatch.setenv("TRANSCRIPT_STEP56_WINDOW_SIZE", "8")
    monkeypatch.setenv("TRANSCRIPT_STEP56_WINDOW_OVERLAP", "1")

    result = asyncio.run(
        phase2_preprocessing.step5_6_node(
            {
                "output_dir": "output",
                "cleaned_sentences": _build_cleaned_sentences(9),
                "main_topic": "topic",
            }
        )
    )

    assert "errors" not in result
    assert result["current_step"] == "step5_6_dedup_merge"


def test_step56_falls_back_to_legacy_step6_then_step5_env(monkeypatch):
    class _Step56LLM:
        def __init__(self):
            self.calls = 0

        async def complete_json(self, prompt, system_prompt=None):
            self.calls += 1
            ids = _extract_prefixed_ids(prompt, "S")
            return {"keep_sentence_ids": ids, "paragraphs": []}, _fake_response()

    fake_llm = _Step56LLM()
    monkeypatch.setattr(phase2_preprocessing, "create_llm_client", lambda purpose="analysis": fake_llm)
    monkeypatch.setattr(phase2_preprocessing, "get_logger", lambda *_args, **_kwargs: _DummyLogger())

    # 不设置 STEP56，验证优先使用 STEP6，再退到 STEP5。
    monkeypatch.delenv("TRANSCRIPT_STEP56_WINDOW_SIZE", raising=False)
    monkeypatch.delenv("TRANSCRIPT_STEP56_WINDOW_OVERLAP", raising=False)
    monkeypatch.setenv("TRANSCRIPT_STEP6_WINDOW_SIZE", "7")
    monkeypatch.setenv("TRANSCRIPT_STEP6_WINDOW_OVERLAP", "1")
    monkeypatch.setenv("TRANSCRIPT_STEP5_WINDOW_SIZE", "5")
    monkeypatch.setenv("TRANSCRIPT_STEP5_WINDOW_OVERLAP", "0")

    asyncio.run(
        phase2_preprocessing.step5_6_node(
            {
                "output_dir": "output",
                "cleaned_sentences": _build_cleaned_sentences(23),
                "main_topic": "topic",
            }
        )
    )

    assert fake_llm.calls == 4


def test_step56_keep_ids_missing_fallback_keep_all(monkeypatch):
    class _Step56LLM:
        async def complete_json(self, prompt, system_prompt=None):
            ids = _extract_prefixed_ids(prompt, "S")
            return {
                "paragraphs": [
                    {
                        "text": "merged",
                        "source_sentence_ids": ids[:2],
                        "merge_type": "merge",
                    }
                ]
            }, _fake_response()

    monkeypatch.setattr(phase2_preprocessing, "create_llm_client", lambda purpose="analysis": _Step56LLM())
    monkeypatch.setattr(phase2_preprocessing, "get_logger", lambda *_args, **_kwargs: _DummyLogger())
    monkeypatch.setenv("TRANSCRIPT_STEP56_WINDOW_SIZE", "8")
    monkeypatch.setenv("TRANSCRIPT_STEP56_WINDOW_OVERLAP", "0")

    result = asyncio.run(
        phase2_preprocessing.step5_6_node(
            {
                "output_dir": "output",
                "cleaned_sentences": _build_cleaned_sentences(10),
                "main_topic": "topic",
            }
        )
    )

    assert len(result["non_redundant_sentences"]) == 10


def test_step56_paragraphs_missing_fallback_single_sentence_paragraphs(monkeypatch):
    class _Step56LLM:
        async def complete_json(self, prompt, system_prompt=None):
            ids = _extract_prefixed_ids(prompt, "S")
            return {"keep_sentence_ids": ids}, _fake_response()

    monkeypatch.setattr(phase2_preprocessing, "create_llm_client", lambda purpose="analysis": _Step56LLM())
    monkeypatch.setattr(phase2_preprocessing, "get_logger", lambda *_args, **_kwargs: _DummyLogger())
    monkeypatch.setenv("TRANSCRIPT_STEP56_WINDOW_SIZE", "8")
    monkeypatch.setenv("TRANSCRIPT_STEP56_WINDOW_OVERLAP", "0")

    result = asyncio.run(
        phase2_preprocessing.step5_6_node(
            {
                "output_dir": "output",
                "cleaned_sentences": _build_cleaned_sentences(9),
                "main_topic": "topic",
            }
        )
    )

    assert len(result["pure_text_script"]) == 9
    assert result["pure_text_script"][0]["source_sentence_ids"] == ["S001"]
