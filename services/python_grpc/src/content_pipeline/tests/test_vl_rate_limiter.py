import asyncio

from services.python_grpc.src.content_pipeline import rate_limiter as rl_module
from services.python_grpc.src.content_pipeline.infra.llm import llm_gateway


def test_vl_rate_limiter_waits_when_rpm_and_tpm_exhausted(monkeypatch):
    fake_clock = {"now": 1000.0}

    def _fake_monotonic() -> float:
        return float(fake_clock["now"])

    async def _fake_sleep(seconds: float) -> None:
        fake_clock["now"] += float(seconds)

    monkeypatch.setattr(rl_module.time, "monotonic", _fake_monotonic)
    monkeypatch.setattr(rl_module.asyncio, "sleep", _fake_sleep)

    limiter = rl_module.VLRateLimiter(rpm_limit=2, tpm_limit=10)

    async def _run():
        waits = []
        waits.append(await limiter.acquire(5))
        waits.append(await limiter.acquire(5))
        waits.append(await limiter.acquire(5))
        return waits

    waits = asyncio.run(_run())
    assert waits[0] == (0.0, 0.0)
    assert waits[1] == (0.0, 0.0)
    assert waits[2][0] == 30.0
    assert waits[2][1] == 30.0


def test_vl_chat_completion_applies_rate_limiter(monkeypatch):
    monkeypatch.setattr(llm_gateway, "_VL_HEDGE_ENABLED", False)
    monkeypatch.setattr(llm_gateway, "_VL_CACHE_ENABLED", False)

    class _FakeRateLimiter:
        def __init__(self):
            self.calls = []

        async def acquire(self, estimated_tokens: int):
            self.calls.append(int(estimated_tokens))
            return 0.0, 0.0

    class _FakeLimiter:
        async def acquire(self, permits: int) -> int:
            return permits

        async def record_success(self) -> None:
            return None

        async def record_failure(self, is_rate_limit: bool = False) -> None:
            return None

        async def release(self, permits: int) -> None:
            return None

    fake_rate_limiter = _FakeRateLimiter()

    async def _fake_call_vl_api_once(**kwargs):
        return "ok", "stop", {"prompt_tokens": 10, "completion_tokens": 20, "total_tokens": 30}, "fake-model"

    monkeypatch.setattr(llm_gateway, "_VL_RATE_LIMITER", fake_rate_limiter)
    monkeypatch.setattr(llm_gateway, "_VL_CONCURRENCY", _FakeLimiter())
    monkeypatch.setattr(llm_gateway, "_call_vl_api_once", _fake_call_vl_api_once)

    result = asyncio.run(
        llm_gateway.vl_chat_completion(
            client=object(),
            model="fake-model",
            messages=[
                {"role": "system", "content": "system-prompt"},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "分析这段视频并输出结构化 JSON"},
                        {"type": "video_url", "video_url": {"url": "file:///tmp/a.mp4"}},
                    ],
                },
            ],
            max_tokens=128,
            temperature=0.1,
            cache_key=None,
        )
    )

    assert result.content == "ok"
    assert len(fake_rate_limiter.calls) == 1
    assert fake_rate_limiter.calls[0] > 0
