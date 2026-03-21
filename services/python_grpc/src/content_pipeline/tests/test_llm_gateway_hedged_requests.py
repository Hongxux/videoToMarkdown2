import asyncio
import shutil
from pathlib import Path

import pytest

from services.python_grpc.src.common.utils.runtime_llm_context import activate_runtime_llm_context
from services.python_grpc.src.content_pipeline.infra.llm import llm_gateway
from services.python_grpc.src.content_pipeline.infra.llm.vision_ai_client import VisionAIClient


def _prepare_workspace_tmp_dir(name: str) -> Path:
    target = Path("var") / name
    shutil.rmtree(target, ignore_errors=True)
    target.mkdir(parents=True, exist_ok=True)
    return target


def test_deepseek_hedge_uses_second_and_cancels_slow_primary(monkeypatch):
    monkeypatch.setattr(llm_gateway, "_DEEPSEEK_HEDGE_ENABLED", True)
    monkeypatch.setattr(llm_gateway, "_DEEPSEEK_DYNAMIC_HEDGE_DELAY_ENABLED", False)
    monkeypatch.setattr(llm_gateway, "_DEEPSEEK_HEDGE_DELAY_MS", 20)

    class _FakeDeepSeekClient:
        def __init__(self):
            self.calls = 0
            self.cancelled = 0

        async def complete_text(
            self,
            prompt: str,
            system_message: str = None,
            need_logprobs: bool = False,
            disable_inflight_dedup: bool = False,
        ):
            self.calls += 1
            if self.calls == 1:
                try:
                    await asyncio.sleep(0.25)
                except asyncio.CancelledError:
                    self.cancelled += 1
                    raise
                return "slow-primary", {"path": "primary"}, None
            await asyncio.sleep(0.01)
            return "fast-hedge", {"path": "hedge"}, None

    client = _FakeDeepSeekClient()

    content, metadata, logprobs = asyncio.run(
        llm_gateway.deepseek_complete_text(
            prompt="hello",
            system_message="system",
            client=client,
            model="deepseek-chat",
        )
    )

    assert content == "fast-hedge"
    assert metadata["path"] == "hedge"
    assert logprobs is None
    assert client.calls == 2
    assert client.cancelled == 1


def test_deepseek_hedge_skips_second_when_primary_is_fast(monkeypatch):
    monkeypatch.setattr(llm_gateway, "_DEEPSEEK_HEDGE_ENABLED", True)
    monkeypatch.setattr(llm_gateway, "_DEEPSEEK_DYNAMIC_HEDGE_DELAY_ENABLED", False)
    monkeypatch.setattr(llm_gateway, "_DEEPSEEK_HEDGE_DELAY_MS", 200)

    class _FakeDeepSeekClient:
        def __init__(self):
            self.calls = 0

        async def complete_text(
            self,
            prompt: str,
            system_message: str = None,
            need_logprobs: bool = False,
            disable_inflight_dedup: bool = False,
        ):
            self.calls += 1
            await asyncio.sleep(0.01)
            return "primary", {"path": "primary"}, None

    client = _FakeDeepSeekClient()
    content, _, _ = asyncio.run(
        llm_gateway.deepseek_complete_text(
            prompt="hello",
            system_message="system",
            client=client,
            model="deepseek-chat",
        )
    )

    assert content == "primary"
    assert client.calls == 1


def test_vl_hedge_uses_second_and_cancels_slow_primary(monkeypatch):
    monkeypatch.setattr(llm_gateway, "_VL_HEDGE_ENABLED", True)
    monkeypatch.setattr(llm_gateway, "_VL_HEDGE_DELAY_MS", 20)

    class _FakeLimiter:
        async def acquire(self, permits: int) -> int:
            return permits

        async def record_success(self) -> None:
            return None

        async def record_failure(self, is_rate_limit: bool = False) -> None:
            return None

        async def release(self, permits: int) -> None:
            return None

    state = {"calls": 0, "cancelled": 0}

    async def _fake_call_vl_api_once(**kwargs):
        state["calls"] += 1
        if state["calls"] == 1:
            try:
                await asyncio.sleep(0.25)
            except asyncio.CancelledError:
                state["cancelled"] += 1
                raise
            return "slow-primary", "stop", {"total_tokens": 1}, "fake-model"
        await asyncio.sleep(0.01)
        return "fast-hedge", "stop", {"total_tokens": 2}, "fake-model"

    monkeypatch.setattr(llm_gateway, "_VL_CONCURRENCY", _FakeLimiter())
    monkeypatch.setattr(llm_gateway, "_call_vl_api_once", _fake_call_vl_api_once)

    result = asyncio.run(
        llm_gateway.vl_chat_completion(
            client=object(),
            model="fake-model",
            messages=[],
            max_tokens=16,
            temperature=0.1,
            cache_key=None,
        )
    )

    assert result.content == "fast-hedge"
    assert state["calls"] == 2
    assert state["cancelled"] == 1


def test_vl_chat_completion_allows_custom_timeout_and_hedge_delay(monkeypatch):
    monkeypatch.setattr(llm_gateway, "_VL_HEDGE_ENABLED", True)
    monkeypatch.setattr(llm_gateway, "_VL_CACHE_ENABLED", False)
    monkeypatch.setattr(llm_gateway, "_VL_HEDGE_DELAY_MS", 200)

    class _FakeRateLimiter:
        async def acquire(self, estimated_tokens: int):
            _ = estimated_tokens
            return 0.0, 0.0

    class _FakeLimiter:
        async def acquire(self, permits: int) -> int:
            return permits

        async def record_success(self) -> None:
            return None

        async def record_failure(self, is_rate_limit: bool = False) -> None:
            _ = is_rate_limit
            return None

        async def release(self, permits: int) -> None:
            _ = permits
            return None

    captured = {"timeout": None, "delay_ms": None}

    async def _fake_call_vl_api_once(**kwargs):
        captured["timeout"] = kwargs.get("timeout")
        return "ok", "stop", {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}, "fake-model"

    async def _fake_run_hedged_async_request(
        *,
        request_name: str,
        enabled: bool,
        delay_ms: int,
        primary_factory,
        secondary_factory=None,
    ):
        _ = (request_name, enabled, secondary_factory)
        captured["delay_ms"] = delay_ms
        return await primary_factory()

    monkeypatch.setattr(llm_gateway, "_VL_RATE_LIMITER", _FakeRateLimiter())
    monkeypatch.setattr(llm_gateway, "_VL_CONCURRENCY", _FakeLimiter())
    monkeypatch.setattr(llm_gateway, "_call_vl_api_once", _fake_call_vl_api_once)
    monkeypatch.setattr(llm_gateway, "_run_hedged_async_request", _fake_run_hedged_async_request)

    result = asyncio.run(
        llm_gateway.vl_chat_completion(
            client=object(),
            model="fake-model",
            messages=[],
            max_tokens=16,
            temperature=0.1,
            cache_key=None,
            timeout=33.5,
            hedge_delay_ms=44,
        )
    )

    assert result.content == "ok"
    assert float(captured["timeout"]) == 33.5
    assert int(captured["delay_ms"]) == 44


def test_vl_chat_completions_keeps_order_and_respects_inflight(monkeypatch):
    state = {"active": 0, "max_active": 0}

    async def _fake_vl_chat_completion(**kwargs):
        state["active"] += 1
        state["max_active"] = max(state["max_active"], state["active"])
        try:
            marker = str(kwargs.get("cache_key", ""))
            if marker.endswith("slow"):
                await asyncio.sleep(0.05)
            else:
                await asyncio.sleep(0.01)
            return llm_gateway.VLChatResult(
                content=marker,
                finish_reason="stop",
                usage={"total_tokens": 1},
                model="fake-model",
            )
        finally:
            state["active"] -= 1

    monkeypatch.setattr(llm_gateway, "vl_chat_completion", _fake_vl_chat_completion)

    results = asyncio.run(
        llm_gateway.vl_chat_completions(
            requests=[
                {
                    "client": object(),
                    "model": "fake-model",
                    "messages": [],
                    "max_tokens": 16,
                    "temperature": 0.1,
                    "cache_key": "req-0-slow",
                },
                {
                    "client": object(),
                    "model": "fake-model",
                    "messages": [],
                    "max_tokens": 16,
                    "temperature": 0.1,
                    "cache_key": "req-1-fast",
                },
                {
                    "client": object(),
                    "model": "fake-model",
                    "messages": [],
                    "max_tokens": 16,
                    "temperature": 0.1,
                    "cache_key": "req-2-fast",
                },
            ],
            max_inflight=2,
        )
    )

    assert [item.content for item in results] == ["req-0-slow", "req-1-fast", "req-2-fast"]
    assert state["max_active"] <= 2


def test_vision_sync_hedge_uses_second_and_cancels_slow_primary(monkeypatch):
    monkeypatch.setattr(llm_gateway, "_VISION_HEDGE_ENABLED", True)
    monkeypatch.setattr(llm_gateway, "_VISION_HEDGE_DELAY_MS", 20)

    class _FakeVisionClient:
        def __init__(self):
            self.calls = 0
            self.cancelled = 0

        async def validate_image(
            self,
            image_path: str,
            prompt: str = "",
            system_prompt: str = None,
            skip_duplicate_check: bool = False,
        ):
            self.calls += 1
            if self.calls == 1:
                try:
                    await asyncio.sleep(0.25)
                except asyncio.CancelledError:
                    self.cancelled += 1
                    raise
                return {"path": "slow-primary"}
            await asyncio.sleep(0.01)
            return {"path": "fast-hedge"}

    fake_client = _FakeVisionClient()

    result = llm_gateway.vision_validate_image_sync(
        image_path="demo.jpg",
        client=fake_client,
        timeout=3.0,
    )

    assert result["path"] == "fast-hedge"
    assert fake_client.calls == 2
    assert fake_client.cancelled == 1


def test_vision_gateway_sync_bridge_rejects_running_loop():
    class _FakeVisionClient:
        async def validate_image(
            self,
            image_path: str,
            prompt: str = "",
            system_prompt: str = None,
            skip_duplicate_check: bool = False,
        ):
            return {"path": image_path}

    async def _run():
        with pytest.raises(RuntimeError, match="running event loop"):
            llm_gateway.vision_validate_image_sync(
                image_path="demo.jpg",
                client=_FakeVisionClient(),
            )

    asyncio.run(_run())


def test_vision_client_sync_bridge_rejects_running_loop():
    client = VisionAIClient.__new__(VisionAIClient)

    async def _fake_validate_images_batch(
        image_paths,
        prompt: str = "",
        system_prompt: str = None,
        skip_duplicate_check: bool = False,
        max_batch_size=None,
    ):
        return [{"path": path} for path in image_paths]

    client.validate_images_batch = _fake_validate_images_batch

    async def _run():
        with pytest.raises(RuntimeError, match="running event loop"):
            client.validate_images_batch_sync(["a.png", "b.png"])

    asyncio.run(_run())


def test_deepseek_dynamic_delay_estimate_scales_with_prompt_length():
    estimator = llm_gateway._DeepseekHedgeDelayEstimator(
        quantile=0.82,
        min_pool_samples=4,
        sample_window=128,
        bootstrap_enabled=False,
        bootstrap_glob="",
        bootstrap_max_files=0,
        bootstrap_max_records=0,
    )

    # 小输入样本：延迟较短
    for _ in range(12):
        estimator.observe(
            prompt="a" * 400,
            system_message="s",
            metadata={
                "prompt_tokens": 120,
                "completion_tokens": 50,
                "total_tokens": 170,
                "latency_ms": 1800,
                "cache_hit": False,
            },
        )

    # 大输入样本：延迟较长
    for _ in range(12):
        estimator.observe(
            prompt="b" * 6000,
            system_message="s",
            metadata={
                "prompt_tokens": 1800,
                "completion_tokens": 420,
                "total_tokens": 2220,
                "latency_ms": 18500,
                "cache_hit": False,
            },
        )

    small_delay, _ = estimator.estimate_delay_ms(prompt_tokens=150, max_tokens=256)
    large_delay, _ = estimator.estimate_delay_ms(prompt_tokens=1700, max_tokens=512)

    assert large_delay > small_delay


def test_deepseek_complete_json_uses_dynamic_delay(monkeypatch):
    monkeypatch.setattr(llm_gateway, "_DEEPSEEK_HEDGE_ENABLED", True)
    monkeypatch.setattr(llm_gateway, "_DEEPSEEK_DYNAMIC_HEDGE_DELAY_ENABLED", True)

    captured = {"delay_ms": None}

    class _FakeEstimator:
        def estimate_delay_ms(
            self,
            *,
            prompt_tokens: int,
            max_tokens: int = None,
            hedge_context=None,
        ):
            return 37, "unit-test"

        def observe(self, *, prompt: str, system_message: str = None, metadata=None):
            return None

    async def _fake_run_hedged_async_request(
        *,
        request_name: str,
        enabled: bool,
        delay_ms: int,
        primary_factory,
        secondary_factory=None,
    ):
        captured["delay_ms"] = delay_ms
        return await primary_factory()

    class _FakeDeepSeekClient:
        async def complete_json(
            self,
            prompt: str,
            system_message: str = None,
            need_logprobs: bool = False,
            max_tokens: int = None,
            disable_inflight_dedup: bool = False,
        ):
            return {"ok": True}, {"prompt_tokens": 10, "total_tokens": 12, "latency_ms": 200}, None

    monkeypatch.setattr(llm_gateway, "_DEEPSEEK_HEDGE_ESTIMATOR", _FakeEstimator())
    monkeypatch.setattr(llm_gateway, "_run_hedged_async_request", _fake_run_hedged_async_request)

    result_json, _, _ = asyncio.run(
        llm_gateway.deepseek_complete_json(
            prompt="hello",
            system_message="system",
            max_tokens=64,
            client=_FakeDeepSeekClient(),
            model="deepseek-chat",
        )
    )

    assert result_json == {"ok": True}
    assert captured["delay_ms"] == 37


def test_deepseek_complete_json_retries_four_times_before_qwen_fallback(monkeypatch):
    monkeypatch.setattr(llm_gateway, "_DEEPSEEK_HEDGE_ENABLED", False)
    monkeypatch.setattr(llm_gateway, "_DEEPSEEK_FAST_FALLBACK_OPEN", False)
    monkeypatch.setattr(llm_gateway, "_DEEPSEEK_QWEN_FALLBACK_ENABLED", True)
    monkeypatch.setattr(llm_gateway, "_DEEPSEEK_QWEN_FALLBACK_MODEL", "qwen-plus")
    monkeypatch.setattr(
        llm_gateway,
        "_DEEPSEEK_QWEN_FALLBACK_BASE_URL",
        "https://dashscope.aliyuncs.com/compatible-mode/v1",
    )
    monkeypatch.setattr(llm_gateway, "_DEEPSEEK_QWEN_FALLBACK_API_KEY_ENV", "DASHSCOPE_API_KEY")
    monkeypatch.setattr(llm_gateway, "_DEEPSEEK_QWEN_FALLBACK_API_KEY", "")
    monkeypatch.setenv("DASHSCOPE_API_KEY", "dashscope-test-key")

    sleep_calls = []

    async def _fake_sleep(seconds):
        sleep_calls.append(seconds)

    monkeypatch.setattr(llm_gateway.asyncio, "sleep", _fake_sleep)

    class _PrimaryFailingClient:
        def __init__(self):
            self.calls = 0

        async def complete_json(
            self,
            prompt: str,
            system_message: str = None,
            need_logprobs: bool = False,
            max_tokens: int = None,
            disable_inflight_dedup: bool = False,
        ):
            self.calls += 1
            _ = (prompt, system_message, need_logprobs, max_tokens, disable_inflight_dedup)
            raise RuntimeError("Connection error.")

    class _FallbackClient:
        def __init__(self):
            self.calls = 0

        async def complete_json(
            self,
            prompt: str,
            system_message: str = None,
            need_logprobs: bool = False,
            max_tokens: int = None,
            disable_inflight_dedup: bool = False,
        ):
            self.calls += 1
            _ = (prompt, system_message, need_logprobs, max_tokens, disable_inflight_dedup)
            return {"provider": "qwen", "ok": True}, {"model": "qwen-plus"}, None

    primary_client = _PrimaryFailingClient()
    fallback_client = _FallbackClient()

    def _fake_get_deepseek_client(
        api_key=None,
        base_url="",
        model="",
        temperature=0.3,
        enable_logprobs=None,
        cache_enabled=None,
        inflight_dedup_enabled=None,
    ):
        _ = (temperature, enable_logprobs, cache_enabled, inflight_dedup_enabled)
        assert api_key == "dashscope-test-key"
        assert base_url == "https://dashscope.aliyuncs.com/compatible-mode/v1"
        assert model == "qwen-plus"
        return fallback_client

    monkeypatch.setattr(llm_gateway, "get_deepseek_client", _fake_get_deepseek_client)

    result_json, metadata, logprobs = asyncio.run(
        llm_gateway.deepseek_complete_json(
            prompt="hello",
            system_message="system",
            max_tokens=64,
            client=primary_client,
            model="deepseek-chat",
        )
    )

    assert result_json == {"provider": "qwen", "ok": True}
    assert metadata["model"] == "qwen-plus"
    assert logprobs is None
    assert primary_client.calls == 5
    assert fallback_client.calls == 1
    assert sleep_calls == [2.0, 4.0, 8.0, 16.0]
    assert llm_gateway._DEEPSEEK_FAST_FALLBACK_OPEN is True



def test_deepseek_complete_json_fast_fallback_uses_single_primary_attempt(monkeypatch):
    monkeypatch.setattr(llm_gateway, "_DEEPSEEK_HEDGE_ENABLED", False)
    monkeypatch.setattr(llm_gateway, "_DEEPSEEK_FAST_FALLBACK_OPEN", True)
    monkeypatch.setattr(llm_gateway, "_DEEPSEEK_QWEN_FALLBACK_ENABLED", True)
    monkeypatch.setattr(llm_gateway, "_DEEPSEEK_QWEN_FALLBACK_MODEL", "qwen-plus")
    monkeypatch.setattr(
        llm_gateway,
        "_DEEPSEEK_QWEN_FALLBACK_BASE_URL",
        "https://dashscope.aliyuncs.com/compatible-mode/v1",
    )
    monkeypatch.setattr(llm_gateway, "_DEEPSEEK_QWEN_FALLBACK_API_KEY_ENV", "DASHSCOPE_API_KEY")
    monkeypatch.setattr(llm_gateway, "_DEEPSEEK_QWEN_FALLBACK_API_KEY", "")
    monkeypatch.setenv("DASHSCOPE_API_KEY", "dashscope-test-key")

    sleep_calls = []

    async def _fake_sleep(seconds):
        sleep_calls.append(seconds)

    monkeypatch.setattr(llm_gateway.asyncio, "sleep", _fake_sleep)

    class _PrimaryFailingClient:
        def __init__(self):
            self.calls = 0

        async def complete_json(
            self,
            prompt: str,
            system_message: str = None,
            need_logprobs: bool = False,
            max_tokens: int = None,
            disable_inflight_dedup: bool = False,
        ):
            self.calls += 1
            _ = (prompt, system_message, need_logprobs, max_tokens, disable_inflight_dedup)
            raise RuntimeError("Connection error.")

    class _FallbackClient:
        def __init__(self):
            self.calls = 0

        async def complete_json(
            self,
            prompt: str,
            system_message: str = None,
            need_logprobs: bool = False,
            max_tokens: int = None,
            disable_inflight_dedup: bool = False,
        ):
            self.calls += 1
            _ = (prompt, system_message, need_logprobs, max_tokens, disable_inflight_dedup)
            return {"provider": "qwen", "ok": True}, {"model": "qwen-plus"}, None

    primary_client = _PrimaryFailingClient()
    fallback_client = _FallbackClient()

    def _fake_get_deepseek_client(
        api_key=None,
        base_url="",
        model="",
        temperature=0.3,
        enable_logprobs=None,
        cache_enabled=None,
        inflight_dedup_enabled=None,
    ):
        _ = (temperature, enable_logprobs, cache_enabled, inflight_dedup_enabled)
        assert api_key == "dashscope-test-key"
        assert base_url == "https://dashscope.aliyuncs.com/compatible-mode/v1"
        assert model == "qwen-plus"
        return fallback_client

    monkeypatch.setattr(llm_gateway, "get_deepseek_client", _fake_get_deepseek_client)

    result_json, metadata, logprobs = asyncio.run(
        llm_gateway.deepseek_complete_json(
            prompt="hello",
            system_message="system",
            max_tokens=64,
            client=primary_client,
            model="deepseek-chat",
        )
    )

    assert result_json == {"provider": "qwen", "ok": True}
    assert metadata["model"] == "qwen-plus"
    assert logprobs is None
    assert primary_client.calls == 1
    assert fallback_client.calls == 1
    assert sleep_calls == []
    assert llm_gateway._DEEPSEEK_FAST_FALLBACK_OPEN is True


def test_deepseek_complete_text_retries_four_times_before_qwen_fallback(monkeypatch):
    monkeypatch.setattr(llm_gateway, "_DEEPSEEK_HEDGE_ENABLED", False)
    monkeypatch.setattr(llm_gateway, "_DEEPSEEK_FAST_FALLBACK_OPEN", False)
    monkeypatch.setattr(llm_gateway, "_DEEPSEEK_QWEN_FALLBACK_ENABLED", True)
    monkeypatch.setattr(llm_gateway, "_DEEPSEEK_QWEN_FALLBACK_MODEL", "qwen-plus")
    monkeypatch.setattr(
        llm_gateway,
        "_DEEPSEEK_QWEN_FALLBACK_BASE_URL",
        "https://dashscope.aliyuncs.com/compatible-mode/v1",
    )
    monkeypatch.setattr(llm_gateway, "_DEEPSEEK_QWEN_FALLBACK_API_KEY_ENV", "DASHSCOPE_API_KEY")
    monkeypatch.setattr(llm_gateway, "_DEEPSEEK_QWEN_FALLBACK_API_KEY", "")
    monkeypatch.setenv("DASHSCOPE_API_KEY", "dashscope-test-key")

    sleep_calls = []

    async def _fake_sleep(seconds):
        sleep_calls.append(seconds)

    monkeypatch.setattr(llm_gateway.asyncio, "sleep", _fake_sleep)

    class _PrimaryFailingClient:
        def __init__(self):
            self.calls = 0

        async def complete_text(
            self,
            prompt: str,
            system_message: str = None,
            need_logprobs: bool = False,
            disable_inflight_dedup: bool = False,
        ):
            self.calls += 1
            _ = (prompt, system_message, need_logprobs, disable_inflight_dedup)
            raise RuntimeError("Connection error.")

    class _FallbackClient:
        def __init__(self):
            self.calls = 0

        async def complete_text(
            self,
            prompt: str,
            system_message: str = None,
            need_logprobs: bool = False,
            disable_inflight_dedup: bool = False,
        ):
            self.calls += 1
            _ = (prompt, system_message, need_logprobs, disable_inflight_dedup)
            return "qwen fallback", {"model": "qwen-plus"}, None

    primary_client = _PrimaryFailingClient()
    fallback_client = _FallbackClient()

    def _fake_get_deepseek_client(
        api_key=None,
        base_url="",
        model="",
        temperature=0.3,
        enable_logprobs=None,
        cache_enabled=None,
        inflight_dedup_enabled=None,
    ):
        _ = (temperature, enable_logprobs, cache_enabled, inflight_dedup_enabled)
        assert api_key == "dashscope-test-key"
        assert base_url == "https://dashscope.aliyuncs.com/compatible-mode/v1"
        assert model == "qwen-plus"
        return fallback_client

    monkeypatch.setattr(llm_gateway, "get_deepseek_client", _fake_get_deepseek_client)

    output_text, metadata, logprobs = asyncio.run(
        llm_gateway.deepseek_complete_text(
            prompt="hello",
            system_message="system",
            client=primary_client,
            model="deepseek-chat",
        )
    )

    assert output_text == "qwen fallback"
    assert metadata["model"] == "qwen-plus"
    assert logprobs is None
    assert primary_client.calls == 5
    assert fallback_client.calls == 1
    assert sleep_calls == [2.0, 4.0, 8.0, 16.0]
    assert llm_gateway._DEEPSEEK_FAST_FALLBACK_OPEN is True


def test_deepseek_complete_text_pins_current_task_to_qwen_after_first_failure(monkeypatch):
    monkeypatch.setattr(llm_gateway, "_DEEPSEEK_HEDGE_ENABLED", False)
    monkeypatch.setattr(llm_gateway, "_DEEPSEEK_FAST_FALLBACK_OPEN", False)
    monkeypatch.setattr(llm_gateway, "_DEEPSEEK_QWEN_FALLBACK_ENABLED", True)
    monkeypatch.setattr(llm_gateway, "_DEEPSEEK_QWEN_FALLBACK_MODEL", "qwen-plus")
    monkeypatch.setattr(
        llm_gateway,
        "_DEEPSEEK_QWEN_FALLBACK_BASE_URL",
        "https://dashscope.aliyuncs.com/compatible-mode/v1",
    )
    monkeypatch.setattr(llm_gateway, "_DEEPSEEK_QWEN_FALLBACK_API_KEY_ENV", "DASHSCOPE_API_KEY")
    monkeypatch.setattr(llm_gateway, "_DEEPSEEK_QWEN_FALLBACK_API_KEY", "")
    monkeypatch.setenv("DASHSCOPE_API_KEY", "dashscope-test-key")

    sleep_calls = []

    async def _fake_sleep(seconds):
        sleep_calls.append(seconds)

    monkeypatch.setattr(llm_gateway.asyncio, "sleep", _fake_sleep)

    class _PrimaryFailingClient:
        def __init__(self):
            self.calls = 0

        async def complete_text(
            self,
            prompt: str,
            system_message: str = None,
            need_logprobs: bool = False,
            disable_inflight_dedup: bool = False,
        ):
            self.calls += 1
            _ = (prompt, system_message, need_logprobs, disable_inflight_dedup)
            raise RuntimeError("Connection error.")

    class _FallbackClient:
        def __init__(self):
            self.calls = 0

        async def complete_text(
            self,
            prompt: str,
            system_message: str = None,
            need_logprobs: bool = False,
            disable_inflight_dedup: bool = False,
        ):
            self.calls += 1
            _ = (system_message, need_logprobs, disable_inflight_dedup)
            return f"qwen::{prompt}", {"model": "qwen-plus"}, None

    primary_client = _PrimaryFailingClient()
    fallback_client = _FallbackClient()

    def _fake_get_deepseek_client(
        api_key=None,
        base_url="",
        model="",
        temperature=0.3,
        enable_logprobs=None,
        cache_enabled=None,
        inflight_dedup_enabled=None,
    ):
        _ = (temperature, enable_logprobs, cache_enabled, inflight_dedup_enabled)
        assert api_key == "dashscope-test-key"
        assert base_url == "https://dashscope.aliyuncs.com/compatible-mode/v1"
        assert model == "qwen-plus"
        return fallback_client

    monkeypatch.setattr(llm_gateway, "get_deepseek_client", _fake_get_deepseek_client)

    output_dir = _prepare_workspace_tmp_dir("tmp_pytest_llm_gateway_task_route_text")

    async def _run():
        with activate_runtime_llm_context(
            stage="phase2b_markdown",
            output_dir=str(output_dir),
            task_id="task-text-route",
        ) as runtime_context:
            first_text, first_meta, _ = await llm_gateway.deepseek_complete_text(
                prompt="hello-one",
                system_message="system",
                client=primary_client,
                model="deepseek-chat",
            )
            second_text, second_meta, _ = await llm_gateway.deepseek_complete_text(
                prompt="hello-two",
                system_message="system",
                client=primary_client,
                model="deepseek-chat",
            )
            route_snapshot = runtime_context.get_llm_provider_route(
                llm_gateway._DEEPSEEK_RUNTIME_PROVIDER_ROUTE_NAME,
            )
            return first_text, first_meta, second_text, second_meta, route_snapshot

    first_text, first_meta, second_text, second_meta, route_snapshot = asyncio.run(_run())

    assert first_text == "qwen::hello-one"
    assert second_text == "qwen::hello-two"
    assert first_meta["model"] == "qwen-plus"
    assert second_meta["model"] == "qwen-plus"
    assert route_snapshot["provider"] == "qwen"
    assert primary_client.calls == 5
    assert fallback_client.calls == 2
    assert sleep_calls == [2.0, 4.0, 8.0, 16.0]


def test_deepseek_complete_json_pins_current_task_to_qwen_after_first_failure(monkeypatch):
    monkeypatch.setattr(llm_gateway, "_DEEPSEEK_HEDGE_ENABLED", False)
    monkeypatch.setattr(llm_gateway, "_DEEPSEEK_FAST_FALLBACK_OPEN", False)
    monkeypatch.setattr(llm_gateway, "_DEEPSEEK_QWEN_FALLBACK_ENABLED", True)
    monkeypatch.setattr(llm_gateway, "_DEEPSEEK_QWEN_FALLBACK_MODEL", "qwen-plus")
    monkeypatch.setattr(
        llm_gateway,
        "_DEEPSEEK_QWEN_FALLBACK_BASE_URL",
        "https://dashscope.aliyuncs.com/compatible-mode/v1",
    )
    monkeypatch.setattr(llm_gateway, "_DEEPSEEK_QWEN_FALLBACK_API_KEY_ENV", "DASHSCOPE_API_KEY")
    monkeypatch.setattr(llm_gateway, "_DEEPSEEK_QWEN_FALLBACK_API_KEY", "")
    monkeypatch.setenv("DASHSCOPE_API_KEY", "dashscope-test-key")

    sleep_calls = []

    async def _fake_sleep(seconds):
        sleep_calls.append(seconds)

    monkeypatch.setattr(llm_gateway.asyncio, "sleep", _fake_sleep)

    class _PrimaryFailingClient:
        def __init__(self):
            self.calls = 0

        async def complete_json(
            self,
            prompt: str,
            system_message: str = None,
            need_logprobs: bool = False,
            max_tokens: int = None,
            disable_inflight_dedup: bool = False,
        ):
            self.calls += 1
            _ = (prompt, system_message, need_logprobs, max_tokens, disable_inflight_dedup)
            raise RuntimeError("Connection error.")

    class _FallbackClient:
        def __init__(self):
            self.calls = 0

        async def complete_json(
            self,
            prompt: str,
            system_message: str = None,
            need_logprobs: bool = False,
            max_tokens: int = None,
            disable_inflight_dedup: bool = False,
        ):
            self.calls += 1
            _ = (system_message, need_logprobs, max_tokens, disable_inflight_dedup)
            return {"provider": "qwen", "prompt": prompt}, {"model": "qwen-plus"}, None

    primary_client = _PrimaryFailingClient()
    fallback_client = _FallbackClient()

    def _fake_get_deepseek_client(
        api_key=None,
        base_url="",
        model="",
        temperature=0.3,
        enable_logprobs=None,
        cache_enabled=None,
        inflight_dedup_enabled=None,
    ):
        _ = (temperature, enable_logprobs, cache_enabled, inflight_dedup_enabled)
        assert api_key == "dashscope-test-key"
        assert base_url == "https://dashscope.aliyuncs.com/compatible-mode/v1"
        assert model == "qwen-plus"
        return fallback_client

    monkeypatch.setattr(llm_gateway, "get_deepseek_client", _fake_get_deepseek_client)

    output_dir = _prepare_workspace_tmp_dir("tmp_pytest_llm_gateway_task_route_json")

    async def _run():
        with activate_runtime_llm_context(
            stage="phase2a_semantic",
            output_dir=str(output_dir),
            task_id="task-json-route",
        ) as runtime_context:
            first_json, first_meta, _ = await llm_gateway.deepseek_complete_json(
                prompt="hello-json-one",
                system_message="system",
                max_tokens=64,
                client=primary_client,
                model="deepseek-chat",
            )
            second_json, second_meta, _ = await llm_gateway.deepseek_complete_json(
                prompt="hello-json-two",
                system_message="system",
                max_tokens=64,
                client=primary_client,
                model="deepseek-chat",
            )
            route_snapshot = runtime_context.get_llm_provider_route(
                llm_gateway._DEEPSEEK_RUNTIME_PROVIDER_ROUTE_NAME,
            )
            return first_json, first_meta, second_json, second_meta, route_snapshot

    first_json, first_meta, second_json, second_meta, route_snapshot = asyncio.run(_run())

    assert first_json == {"provider": "qwen", "prompt": "hello-json-one"}
    assert second_json == {"provider": "qwen", "prompt": "hello-json-two"}
    assert first_meta["model"] == "qwen-plus"
    assert second_meta["model"] == "qwen-plus"
    assert route_snapshot["provider"] == "qwen"
    assert primary_client.calls == 5
    assert fallback_client.calls == 2
    assert sleep_calls == [2.0, 4.0, 8.0, 16.0]


def test_deepseek_dynamic_delay_uses_video_and_step6_context():
    estimator = llm_gateway._DeepseekHedgeDelayEstimator(
        quantile=0.82,
        min_pool_samples=4,
        sample_window=128,
        bootstrap_enabled=False,
        bootstrap_glob="",
        bootstrap_max_files=0,
        bootstrap_max_records=0,
    )

    for _ in range(16):
        estimator.observe(
            prompt="x" * 1200,
            system_message="sys",
            metadata={
                "prompt_tokens": 420,
                "completion_tokens": 180,
                "total_tokens": 600,
                "latency_ms": 4800,
                "cache_hit": False,
            },
        )

    short_video_delay, _ = estimator.estimate_delay_ms(
        prompt_tokens=420,
        max_tokens=512,
        hedge_context={
            "video_duration_sec": 120.0,
            "step6_text_chars": 12000,
            "batch_text_chars": 1800,
        },
    )
    long_video_delay, _ = estimator.estimate_delay_ms(
        prompt_tokens=420,
        max_tokens=512,
        hedge_context={
            "video_duration_sec": 1200.0,
            "step6_text_chars": 12000,
            "batch_text_chars": 1800,
        },
    )

    assert short_video_delay > long_video_delay
