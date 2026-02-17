import asyncio

import services.python_grpc.src.content_pipeline.phase2a.segmentation.knowledge_classifier as kc_module
from services.python_grpc.src.content_pipeline.phase2a.segmentation.knowledge_classifier import (
    KnowledgeClassifier,
)


def _build_units(count: int):
    units = []
    for idx in range(1, count + 1):
        units.append(
            {
                "unit_id": f"U{idx:03d}",
                "title": f"title-{idx}",
                "full_text": "x" * 80,
                "action_segments": [
                    {"id": "a1", "start_sec": float(idx), "end_sec": float(idx) + 0.1}
                ],
            }
        )
    return units


def test_classify_units_batch_respects_chunk_inflight_env(monkeypatch):
    monkeypatch.setenv("DEEPSEEK_API_KEY", "dummy-key")
    monkeypatch.setenv("MODULE2_KC_MULTI_TOKEN_BUDGET", "1")
    monkeypatch.setenv("MODULE2_KC_MULTI_MAX_UNITS_PER_CHUNK", "10")
    monkeypatch.setenv("MODULE2_KC_MULTI_FULL_TEXT_CHARS", "80")
    monkeypatch.setenv("MODULE2_KC_MULTI_CHUNK_MAX_INFLIGHT", "3")

    monkeypatch.setattr(
        kc_module.llm_gateway,
        "get_deepseek_client",
        lambda **_kwargs: object(),
    )

    active = 0
    peak = 0

    async def _fake_deepseek_complete_text(**_kwargs):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.01)
        active -= 1
        return "[]", None, None

    monkeypatch.setattr(
        kc_module.llm_gateway,
        "deepseek_complete_text",
        _fake_deepseek_complete_text,
    )

    classifier = KnowledgeClassifier(api_key="dummy-key", base_url="https://api.deepseek.com")
    monkeypatch.setattr(classifier, "_get_subtitles_in_range", lambda _s, _e: "subtitle")

    result = asyncio.run(classifier.classify_units_batch(_build_units(12)))

    assert len(result) == 12
    assert peak <= 3
    assert peak >= 2
