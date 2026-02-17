import asyncio

from services.python_grpc.src.content_pipeline.infra.llm import llm_client


def test_supports_http2_transport_returns_false_when_h2_missing(monkeypatch):
    monkeypatch.delenv("MODULE2_HTTP2_ENABLED", raising=False)
    monkeypatch.setattr(llm_client.importlib.util, "find_spec", lambda name: None)

    assert llm_client._supports_http2_transport() is False


def test_supports_http2_transport_honors_env_override(monkeypatch):
    monkeypatch.setenv("MODULE2_HTTP2_ENABLED", "true")
    assert llm_client._supports_http2_transport() is True

    monkeypatch.setenv("MODULE2_HTTP2_ENABLED", "false")
    assert llm_client._supports_http2_transport() is False


def test_pool_manager_fallback_to_http11_when_h2_missing(monkeypatch):
    manager = llm_client.AdaptiveConnectionPoolManager()
    monkeypatch.setattr(llm_client, "_supports_http2_transport", lambda: True)

    calls = []

    class _DummyAsyncClient:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            self.is_closed = False

        async def aclose(self):
            self.is_closed = True

    def _fake_async_client(**kwargs):
        calls.append(kwargs.get("http2"))
        if kwargs.get("http2"):
            raise RuntimeError("Using http2=True, but the 'h2' package is not installed")
        return _DummyAsyncClient(**kwargs)

    monkeypatch.setattr(llm_client.httpx, "AsyncClient", _fake_async_client)

    client = asyncio.run(manager.get_client(current_limit=4))

    assert calls == [True, False]
    assert client.kwargs["http2"] is False

