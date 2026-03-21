import asyncio
import json
import sys
from pathlib import Path

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

from services.python_grpc.src.common.utils.runtime_llm_context import activate_runtime_llm_context
from services.python_grpc.src.transcript_pipeline.llm.client import LLMConfig, LLMResponse
from services.python_grpc.src.transcript_pipeline.llm.deepseek import DeepSeekClient


def _make_client() -> DeepSeekClient:
    config = LLMConfig(
        api_key="test-key",
        base_url="https://api.example.com",
        model="deepseek-chat",
        temperature=0.1,
    )
    return DeepSeekClient(config)


def _make_response(content: str) -> LLMResponse:
    return LLMResponse(
        content=content,
        prompt_tokens=1,
        completion_tokens=1,
        total_tokens=2,
        model="deepseek-chat",
        latency_ms=1.0,
        raw_response={},
    )


def test_extract_json_content_from_fence_and_noise():
    client = _make_client()
    raw = "前置说明\n```json\n{\"a\": 1}\n```\n后置说明"
    extracted = client._extract_json_content(raw)
    assert extracted == '{"a": 1}'


def test_load_json_with_repair_for_missing_comma_between_objects():
    client = _make_client()
    broken = '{"corrected_subtitles": [{"subtitle_id":"SUB001" "corrected_text":"x"}]}'
    parsed = client._load_json_with_repair(broken)
    assert parsed["corrected_subtitles"][0]["subtitle_id"] == "SUB001"
    assert parsed["corrected_subtitles"][0]["corrected_text"] == "x"


def test_complete_json_retries_once_when_first_decode_fails(monkeypatch):
    client = _make_client()
    responses = [
        _make_response('invalid-json-response'),
        _make_response('{"corrected_subtitles": [{"subtitle_id":"SUB001", "corrected_text":"x"}]}'),
    ]
    captured_prompts = []

    async def fake_complete(prompt, system_prompt=None, **kwargs):
        captured_prompts.append(prompt)
        return responses.pop(0)

    monkeypatch.setattr(client, "complete", fake_complete)

    parsed, response = asyncio.run(client.complete_json("fix subtitles"))

    assert parsed["corrected_subtitles"][0]["subtitle_id"] == "SUB001"
    assert isinstance(response, LLMResponse)
    assert len(captured_prompts) == 2


def test_complete_json_falls_back_when_response_format_unsupported(monkeypatch):
    client = _make_client()
    calls = []

    async def fake_complete(prompt, system_prompt=None, **kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            raise RuntimeError("DeepSeek API error: 400 - unsupported response_format")
        return _make_response(json.dumps({"ok": True}, ensure_ascii=False))

    monkeypatch.setattr(client, "complete", fake_complete)

    parsed, _ = asyncio.run(client.complete_json("只输出json"))

    assert parsed == {"ok": True}
    assert "response_format" in calls[0]
    assert "response_format" not in calls[1]


def test_complete_text_inflight_dedup_runs_once(monkeypatch):
    client = _make_client()
    calls = {"n": 0}

    async def fake_post_with_retry(payload):
        calls["n"] += 1
        await asyncio.sleep(0.05)
        return {
            "model": "deepseek-chat",
            "choices": [{"message": {"content": "ok"}}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
        }

    monkeypatch.setattr(client, "_post_with_retry", fake_post_with_retry)

    async def run_two():
        # 同 prompt 并发，期望 singleflight 合并，只会打一次底层请求。
        r1, r2 = await asyncio.gather(client.complete("hi"), client.complete("hi"))
        return r1, r2

    r1, r2 = asyncio.run(run_two())
    assert r1.content == "ok"
    assert r2.content == "ok"
    assert calls["n"] == 1


def test_complete_text_result_cache_hits(monkeypatch):
    client = _make_client()
    calls = {"n": 0}

    async def fake_post_with_retry(payload):
        calls["n"] += 1
        return {
            "model": "deepseek-chat",
            "choices": [{"message": {"content": "cached"}}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
        }

    monkeypatch.setattr(client, "_post_with_retry", fake_post_with_retry)

    async def run_twice():
        r1 = await client.complete("hello")
        r2 = await client.complete("hello")
        return r1, r2

    r1, r2 = asyncio.run(run_twice())
    assert r1.content == "cached"
    assert r2.content == "cached"
    assert calls["n"] == 1


def test_complete_text_formats_empty_transport_error(monkeypatch):
    client = _make_client()
    request = httpx.Request("POST", "https://api.example.com/chat/completions")

    async def fake_post_with_retry(_payload):
        raise httpx.ConnectError("", request=request)

    monkeypatch.setattr(client, "_post_with_retry", fake_post_with_retry)

    with pytest.raises(RuntimeError) as exc_info:
        asyncio.run(client.complete("hello"))

    error_text = str(exc_info.value)
    assert "DeepSeek API error:" in error_text
    assert "ConnectError" in error_text
    assert "message=<empty>" in error_text
    assert "request=POST https://api.example.com/chat/completions" in error_text
    assert "base_url=https://api.example.com" in error_text
    assert "model=deepseek-chat" in error_text


def test_complete_json_persists_runtime_identity_metadata(tmp_path, monkeypatch):
    client = _make_client()

    async def fake_complete(prompt, system_prompt=None, **kwargs):
        return _make_response(json.dumps({"ok": True}, ensure_ascii=False))

    monkeypatch.setattr(client, "complete", fake_complete)

    async def run_once():
        with activate_runtime_llm_context(
            stage="stage1",
            output_dir=str(tmp_path),
            task_id="task-stage1-runtime",
            storage_key="task-stage1-runtime",
        ) as runtime_context:
            parsed, _response = await client.complete_json(
                "fix subtitles",
                __runtime_identity__={
                    "step_name": "stage1_step2_correction",
                    "request_name": "complete_json",
                    "unit_id": "batch_0001",
                    "llm_call_id": "stage1_step2_correction.batch_0001",
                },
                __runtime_metadata__={
                    "stage_step": "step2_correction",
                    "scope_variant": "batch_0001",
                    "unit_id": "batch_0001",
                },
            )
            nodes = runtime_context.store.list_scope_nodes(stage="stage1", scope_type="llm_call")
            return parsed, nodes

    parsed, nodes = asyncio.run(run_once())

    assert parsed == {"ok": True}
    assert len(nodes) == 1
    assert nodes[0]["scope_id"] == "stage1_step2_correction.batch_0001"
    assert nodes[0]["stage_step"] == "step2_correction"
    assert nodes[0]["scope_variant"] == "batch_0001"


def test_complete_json_emits_llm_call_event(tmp_path, monkeypatch):
    client = _make_client()
    events = []

    async def fake_complete(prompt, system_prompt=None, **kwargs):
        return _make_response(json.dumps({"ok": True}, ensure_ascii=False))

    monkeypatch.setattr(client, "complete", fake_complete)

    async def run_once():
        with activate_runtime_llm_context(
            stage="stage1",
            output_dir=str(tmp_path),
            task_id="task-stage1-heartbeat",
            storage_key="task-stage1-heartbeat",
            llm_event_emitter=events.append,
        ):
            return await client.complete_json(
                "fix subtitles",
                __runtime_identity__={
                    "step_name": "stage1_step2_correction",
                    "request_name": "complete_json",
                    "unit_id": "batch_0001",
                    "llm_call_id": "stage1_step2_correction.batch_0001",
                },
                __runtime_metadata__={
                    "stage_step": "step2_correction",
                    "scope_variant": "batch_0001",
                    "unit_id": "batch_0001",
                },
            )

    parsed, _response = asyncio.run(run_once())

    assert parsed == {"ok": True}
    assert len(events) == 1
    assert events[0]["event"] == "llm_call_completed"
    assert events[0]["signal_type"] == "hard"
    assert events[0]["step_name"] == "step2_correction"
    assert events[0]["checkpoint"] == "step2_correction.llm_call.batch_0001"
    assert events[0]["llm_call_id"] == "stage1_step2_correction.batch_0001"
    assert events[0]["runtime_restored"] is False
