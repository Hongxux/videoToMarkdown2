import asyncio

from services.python_grpc.src.content_pipeline.infra.llm import llm_gateway


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
