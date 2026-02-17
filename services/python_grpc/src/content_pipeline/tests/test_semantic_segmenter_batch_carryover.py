import asyncio
from types import SimpleNamespace

from services.python_grpc.src.content_pipeline.phase2a.segmentation import semantic_unit_segmenter as segmenter_module
from services.python_grpc.src.content_pipeline.phase2a.segmentation.semantic_unit_segmenter import (
    SemanticUnit,
    SemanticUnitSegmenter,
)


def _build_paragraphs():
    return [
        {
            "paragraph_id": "P001",
            "text": "Open settings and enter model configuration.",
            "source_sentence_ids": ["S001"],
        },
        {
            "paragraph_id": "P002",
            "text": "Fill API key and save.",
            "source_sentence_ids": ["S002"],
        },
        {
            "paragraph_id": "P003",
            "text": "Continue to add options and click apply.",
            "source_sentence_ids": ["S003"],
        },
        {
            "paragraph_id": "P004",
            "text": "Summarize the configuration result.",
            "source_sentence_ids": ["S004"],
        },
    ]


def test_segment_batches_carry_prev_tail_and_merges_boundary_unit(monkeypatch):
    monkeypatch.setattr(segmenter_module, "SEGMENTATION_INPUT_TOKEN_BUDGET", 250)
    prompts = []

    async def _fake_deepseek_complete_json(**kwargs):
        prompt = kwargs["prompt"]
        prompts.append(prompt)
        if "[Boundary Merge Decision]" in prompt:
            return {
                "merge": 1,
                "reason": "continuous",
                "merged_unit": {"k": 2, "m": 0, "title": "merged configuration topic"},
            }, SimpleNamespace(total_tokens=3), None
        if '"paragraph_id": "P001"' in prompt:
            return {
                "semantic_units": [
                    {"pids": ["P001", "P002"], "k": 2, "m": 1, "title": "configuration flow"}
                ]
            }, SimpleNamespace(total_tokens=11), None
        if '"paragraph_id": "P003"' in prompt:
            return {
                "semantic_units": [
                    {"pids": ["P003"], "k": 2, "m": 1, "title": "configuration flow"},
                    {"pids": ["P004"], "k": 0, "m": 0, "title": "result summary"},
                ]
            }, SimpleNamespace(total_tokens=11), None
        raise AssertionError("unexpected prompt")

    monkeypatch.setattr(
        segmenter_module.llm_gateway,
        "deepseek_complete_json",
        _fake_deepseek_complete_json,
    )

    segmenter = SemanticUnitSegmenter(llm_client=object())
    monkeypatch.setattr(segmenter, "_estimate_segment_input_tokens", lambda batch: len(batch) * 100)
    result = asyncio.run(segmenter.segment(_build_paragraphs(), batch_size=2))

    assert result.total_units_output == 2
    assert result.llm_token_usage == 25
    assert [u.unit_id for u in result.semantic_units] == ["SU001", "SU002"]
    assert result.semantic_units[0].source_paragraph_ids == ["P001", "P002", "P003"]
    assert result.semantic_units[0].knowledge_topic == "merged configuration topic"
    assert result.semantic_units[0].mult_steps is False
    assert result.semantic_units[1].source_paragraph_ids == ["P004"]
    assert len(prompts) == 3
    assert any("[Boundary Merge Decision]" in prompt for prompt in prompts)


def test_chunk_paragraphs_respects_input_token_budget(monkeypatch):
    segmenter = SemanticUnitSegmenter(llm_client=object())
    monkeypatch.setattr(segmenter_module, "SEGMENTATION_INPUT_TOKEN_BUDGET", 7000)
    monkeypatch.setattr(segmenter_module, "SEGMENTATION_PROMPT_TOKEN_BUFFER", 0)
    monkeypatch.setattr(segmenter_module, "SEGMENTATION_PREV_TAIL_TEXT_CHARS", 0)

    paragraphs = [
        {
            "paragraph_id": "P001",
            "text": "a" * 5000,
            "source_sentence_ids": ["S001"],
        },
        {
            "paragraph_id": "P002",
            "text": "b" * 5000,
            "source_sentence_ids": ["S002"],
        },
        {
            "paragraph_id": "P003",
            "text": "c" * 5000,
            "source_sentence_ids": ["S003"],
        },
    ]

    batches = segmenter._chunk_paragraphs(paragraphs_for_llm=paragraphs, batch_size=10)

    assert len(batches) == 3
    assert all(len(batch) == 1 for batch in batches)


def test_segment_batches_concurrency_is_capped_by_env(monkeypatch):
    segmenter = SemanticUnitSegmenter(llm_client=object())
    paragraphs = [
        {
            "paragraph_id": f"P{i:03d}",
            "text": f"line-{i}",
            "source_sentence_ids": [f"S{i:03d}"],
        }
        for i in range(1, 31)
    ]

    monkeypatch.setenv("MODULE2_SEMANTIC_SEGMENT_BATCH_MAX_CONCURRENCY", "10")
    monkeypatch.setattr(segmenter_module, "SEGMENTATION_INPUT_TOKEN_BUDGET", 90)
    monkeypatch.setattr(segmenter, "_estimate_segment_input_tokens", lambda batch: len(batch) * 100)

    active = 0
    peak = 0

    async def _fake_segment_single_batch(**kwargs):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.01)
        active -= 1

        batch_index = kwargs["batch_index"]
        batch_paragraphs = kwargs["batch_paragraphs"]
        paragraph = batch_paragraphs[0]
        unit = SemanticUnit(
            unit_id=f"SU_TMP_{batch_index}_1",
            knowledge_type="abstract",
            knowledge_topic="topic",
            full_text=paragraph["text"],
            source_paragraph_ids=[paragraph["paragraph_id"]],
            source_sentence_ids=paragraph.get("source_sentence_ids", []),
            start_sec=0.0,
            end_sec=0.0,
            confidence=0.8,
            mult_steps=False,
        )
        return [unit], 1

    monkeypatch.setattr(segmenter, "_segment_single_batch", _fake_segment_single_batch)

    batches = segmenter._chunk_paragraphs(paragraphs_for_llm=paragraphs, batch_size=999)
    results = asyncio.run(
        segmenter._segment_batches_concurrently(
            paragraph_batches=batches,
            paragraphs=paragraphs,
            sentence_timestamps=None,
        )
    )

    assert len(results) == 30
    assert peak <= 10
