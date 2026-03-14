import asyncio
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

from services.python_grpc.src.transcript_pipeline.llm.client import LLMResponse
from services.python_grpc.src.transcript_pipeline import streaming_executor


def _fake_response() -> LLMResponse:
    return LLMResponse(
        content="{}",
        prompt_tokens=10,
        completion_tokens=5,
        total_tokens=15,
        model="test-model",
        latency_ms=10.0,
    )


def _extract_prefixed_ids(prompt: str, prefix: str) -> list[str]:
    ids: list[str] = []
    marker = f"[{prefix}"
    for line in prompt.splitlines():
        if marker not in line:
            continue
        start = line.find(marker)
        ids.append(line[start:].split("]", 1)[0][1:])
    return ids


def _build_subtitles(count: int) -> list[dict[str, object]]:
    subtitles: list[dict[str, object]] = []
    for idx in range(1, count + 1):
        subtitles.append(
            {
                "subtitle_id": f"SUB{idx:03d}",
                "text": f"subtitle {idx}",
                "start_sec": float(idx),
                "end_sec": float(idx) + 0.5,
            }
        )
    return subtitles


def test_streaming_executor_starts_step3_before_step2_tail_batch_finishes(monkeypatch):
    step3_started = asyncio.Event()
    second_batch_started = asyncio.Event()
    second_batch_finished = asyncio.Event()
    release_second_batch = asyncio.Event()
    tmp_root = Path("var")
    tmp_root.mkdir(exist_ok=True)
    output_dir = Path(tempfile.mkdtemp(prefix="tmp_stage1_streaming_executor_", dir=str(tmp_root.resolve())))

    class _DummyStorage:
        def __init__(self, _base_dir: str):
            return None

        def save_subtitle_timestamps(self, _payload):
            return None

        def save_sentence_timestamps(self, _payload):
            return None

    class _RoutingLLM:
        async def complete_json(self, prompt, system_prompt=None):
            pp = streaming_executor.pp
            if system_prompt == pp.CORRECTION_SYSTEM_PROMPT:
                subtitle_ids = _extract_prefixed_ids(prompt, "SUB")
                if "SUB011" in subtitle_ids:
                    second_batch_started.set()
                    await release_second_batch.wait()
                    second_batch_finished.set()
                return {}, _fake_response()
            if system_prompt == pp.MERGE_SYSTEM_PROMPT:
                step3_started.set()
                subtitle_ids = _extract_prefixed_ids(prompt, "SUB")
                return {
                    "merged_sentences": [
                        {
                            "text": f"merged {'-'.join(subtitle_ids)}",
                            "source_subtitle_ids": subtitle_ids,
                        }
                    ]
                }, _fake_response()
            if system_prompt == pp.TRANSLATION_SYSTEM_PROMPT:
                sentence_ids = _extract_prefixed_ids(prompt, "S")
                return {
                    "translated_sentences": [
                        {
                            "sentence_id": sentence_id,
                            "translated_text": f"translated-{sentence_id}",
                        }
                        for sentence_id in sentence_ids
                    ]
                }, _fake_response()
            if system_prompt == pp.STEP56_DEDUP_MERGE_SYSTEM_PROMPT:
                return {}, _fake_response()
            raise AssertionError(f"unexpected system prompt: {system_prompt}")

    async def _run():
        monkeypatch.setattr(
            streaming_executor,
            "step1_node",
            lambda state: asyncio.sleep(
                0,
                result={
                    **dict(state),
                    "is_valid": True,
                    "current_step": "step1_validate",
                    "current_step_status": "completed",
                },
            ),
        )
        monkeypatch.setattr(
            streaming_executor.file_validator,
            "read_subtitle_sample",
            lambda _path, count=None: _build_subtitles(20),
        )
        monkeypatch.setattr(streaming_executor.pp, "create_llm_client", lambda purpose="refinement": _RoutingLLM())
        monkeypatch.setattr(streaming_executor.pp, "LocalStorage", _DummyStorage)
        monkeypatch.setattr(
            streaming_executor,
            "_save_step4_sentence_timestamps",
            lambda _output_dir, _translated_sentences: asyncio.sleep(0),
        )
        monkeypatch.setenv("TRANSCRIPT_STEP2_BATCH_SIZE", "10")
        monkeypatch.setenv("TRANSCRIPT_STEP35_WINDOW_SIZE", "1")

        task = asyncio.create_task(
            streaming_executor.run_stage1_streaming_executor(
                {
                    "subtitle_path": "dummy",
                    "output_dir": str(output_dir),
                    "domain": "test",
                    "main_topic": "test",
                }
            )
        )
        await asyncio.wait_for(second_batch_started.wait(), timeout=1.0)
        await asyncio.wait_for(step3_started.wait(), timeout=1.0)
        assert not second_batch_finished.is_set()
        release_second_batch.set()
        return await asyncio.wait_for(task, timeout=1.0)

    result = asyncio.run(_run())

    assert len(result["corrected_subtitles"]) == 20
    assert [item["sentence_id"] for item in result["merged_sentences"]] == ["S001", "S002"]
    assert [item["sentence_id"] for item in result["translated_sentences"]] == ["S001", "S002"]
    assert result["step_observability"]["step4_clean_local"]["compat_passthrough_mode_used"] == 1
