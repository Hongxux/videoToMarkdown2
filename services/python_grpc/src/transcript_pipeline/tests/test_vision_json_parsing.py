import asyncio
import sys
from pathlib import Path

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

from services.python_grpc.src.transcript_pipeline.llm.client import LLMConfig, LLMResponse
from services.python_grpc.src.transcript_pipeline.llm.vision import ERNIEVisionClient


def _make_client() -> ERNIEVisionClient:
    config = LLMConfig(
        api_key="test-key",
        base_url="https://api.example.com",
        model="ernie-4.0-vision",
        temperature=0.1,
    )
    return ERNIEVisionClient(config)


def _make_response(content: str) -> LLMResponse:
    return LLMResponse(
        content=content,
        prompt_tokens=1,
        completion_tokens=1,
        total_tokens=2,
        model="ernie-4.0-vision",
        latency_ms=1.0,
        raw_response={},
    )


def test_extract_json_content_from_fence():
    client = _make_client()
    raw = "header\n```json\n{\"a\": 1}\n```\nfooter"
    extracted = client._extract_json_content(raw)
    assert extracted == '{"a": 1}'


def test_load_json_with_repair_for_trailing_comma_and_unclosed_object():
    client = _make_client()
    broken = '{"a": 1,'
    parsed = client._load_json_with_repair(broken)
    assert parsed == {"a": 1}


def test_complete_json_parses_repaired_payload(monkeypatch):
    client = _make_client()

    async def fake_complete(prompt, system_prompt=None, **kwargs):
        return _make_response("```json\n{\"ok\": true,}\n```")

    monkeypatch.setattr(client, "complete", fake_complete)

    parsed, response = asyncio.run(client.complete_json("only json"))
    assert parsed == {"ok": True}
    assert isinstance(response, LLMResponse)


def test_complete_json_raises_value_error_on_invalid_json(monkeypatch):
    client = _make_client()

    async def fake_complete(prompt, system_prompt=None, **kwargs):
        return _make_response("not-json")

    monkeypatch.setattr(client, "complete", fake_complete)

    with pytest.raises(ValueError) as exc_info:
        asyncio.run(client.complete_json("only json"))

    assert "Failed to parse JSON" in str(exc_info.value)


def test_complete_formats_empty_transport_error(monkeypatch):
    client = _make_client()
    request = httpx.Request("POST", "https://api.example.com")

    class _DummyAsyncClient:
        async def post(self, *_args, **_kwargs):
            raise httpx.ConnectError("", request=request)

    async def fake_get_client():
        return _DummyAsyncClient()

    monkeypatch.setattr(client, "_get_client", fake_get_client)

    with pytest.raises(RuntimeError) as exc_info:
        asyncio.run(client.complete("hello"))

    error_text = str(exc_info.value)
    assert "ERNIE API error:" in error_text
    assert "ConnectError" in error_text
    assert "message=<empty>" in error_text
    assert "request=POST https://api.example.com" in error_text
    assert "base_url=https://api.example.com" in error_text
    assert "model=ernie-4.0-vision" in error_text
