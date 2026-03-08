import asyncio
import httpx
from tenacity import RetryCallState, Retrying

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


def test_get_concurrency_limiter_uses_default_deepseek_limits(monkeypatch):
    monkeypatch.delenv("MODULE2_DEEPSEEK_CONCURRENCY_INITIAL", raising=False)
    monkeypatch.delenv("MODULE2_DEEPSEEK_CONCURRENCY_MIN", raising=False)
    monkeypatch.delenv("MODULE2_DEEPSEEK_CONCURRENCY_MAX", raising=False)
    monkeypatch.delenv("MODULE2_DEEPSEEK_CONCURRENCY_INCREASE_STEP", raising=False)
    monkeypatch.delenv("MODULE2_DEEPSEEK_CONCURRENCY_WINDOW_SIZE", raising=False)
    monkeypatch.setattr(llm_client, "_global_concurrency_limiter", None)

    limiter = llm_client.get_concurrency_limiter()

    assert limiter.current_limit == 56
    assert limiter.min_limit == 8
    assert limiter.max_limit == 64


def test_get_concurrency_limiter_honors_env_overrides(monkeypatch):
    monkeypatch.setenv("MODULE2_DEEPSEEK_CONCURRENCY_INITIAL", "6")
    monkeypatch.setenv("MODULE2_DEEPSEEK_CONCURRENCY_MIN", "3")
    monkeypatch.setenv("MODULE2_DEEPSEEK_CONCURRENCY_MAX", "9")
    monkeypatch.setattr(llm_client, "_global_concurrency_limiter", None)

    limiter = llm_client.get_concurrency_limiter()

    assert limiter.current_limit == 6
    assert limiter.min_limit == 3
    assert limiter.max_limit == 9


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


def test_is_retryable_llm_exception_supports_httpx_connect_timeout():
    assert llm_client._is_retryable_llm_exception(httpx.ConnectError("connect failed")) is True
    assert llm_client._is_retryable_llm_exception(httpx.ReadTimeout("timeout")) is True
    assert llm_client._is_retryable_llm_exception(ValueError("bad request")) is False


def test_is_retryable_llm_exception_supports_openai_wrapped_errors(monkeypatch):
    class _FakeAPIConnectionError(Exception):
        pass

    monkeypatch.setattr(
        llm_client,
        "_load_openai_retry_exceptions",
        lambda: (_FakeAPIConnectionError,),
    )

    assert llm_client._is_retryable_llm_exception(_FakeAPIConnectionError("Connection error.")) is True


def test_build_llm_retry_wait_grows_exponentially_with_jitter():
    wait_strategy = llm_client._build_llm_retry_wait(
        initial_backoff_seconds=2.0,
        max_backoff_seconds=10.0,
        jitter_seconds=1.0,
    )
    retrying = Retrying(wait=wait_strategy)
    state = RetryCallState(retrying, None, (), {})

    delays = []
    for attempt_number in (1, 2, 3, 4):
        state.attempt_number = attempt_number
        delays.append(retrying.wait(state))

    assert 2.0 <= delays[0] <= 3.0
    assert 4.0 <= delays[1] <= 5.0
    assert 8.0 <= delays[2] <= 9.0
    assert delays[3] == 10.0
