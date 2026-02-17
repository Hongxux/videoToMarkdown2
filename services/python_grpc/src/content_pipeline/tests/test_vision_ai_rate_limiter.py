import asyncio

from services.python_grpc.src.content_pipeline.infra.llm import vision_ai_client as vision_ai_module


def test_rate_limiter_uses_initial_30_tokens_and_refills_per_second(monkeypatch):
    fake_clock = {"now": 1000.0}

    def _fake_monotonic() -> float:
        return float(fake_clock["now"])

    async def _fake_sleep(seconds: float):
        fake_clock["now"] += float(seconds)

    monkeypatch.setattr(vision_ai_module.time, "monotonic", _fake_monotonic)
    monkeypatch.setattr(vision_ai_module.asyncio, "sleep", _fake_sleep)

    limiter = vision_ai_module.VisionAIRateLimiter(rate_per_minute=60)

    async def _run():
        waits = []
        for _ in range(30):
            waits.append(await limiter.acquire())
        waits.append(await limiter.acquire())
        fake_clock["now"] += 5.0
        waits.append(await limiter.acquire())
        return waits

    waits = asyncio.run(_run())
    assert waits[:30] == [0.0] * 30
    assert waits[30] == 1.0
    assert waits[31] == 0.0
