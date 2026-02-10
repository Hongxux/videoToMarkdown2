from pathlib import Path

from services.python_grpc.src.content_pipeline.phase2a.segmentation.concrete_knowledge_validator import (
    ConcreteKnowledgeResult,
    ConcreteKnowledgeValidator,
)


def _build_result(has_concrete: bool, has_formula: bool) -> ConcreteKnowledgeResult:
    return ConcreteKnowledgeResult(
        has_concrete=has_concrete,
        has_formula=has_formula,
        confidence=0.9,
        concrete_type="娴嬭瘯",
        reason="test",
        is_mixed=False,
        non_text_ratio=0.0,
        should_include=has_concrete or has_formula,
    )


def test_finalize_validation_result_deletes_non_concrete_screenshot(tmp_path):
    image_path = tmp_path / "negative.png"
    image_path.write_bytes(b"img")

    validator = ConcreteKnowledgeValidator.__new__(ConcreteKnowledgeValidator)

    result = _build_result(has_concrete=False, has_formula=False)
    validator._finalize_validation_result(str(image_path), result, cache_result=False)

    assert not image_path.exists()


def test_finalize_validation_result_keeps_formula_screenshot(tmp_path):
    image_path = tmp_path / "formula.png"
    image_path.write_bytes(b"img")

    validator = ConcreteKnowledgeValidator.__new__(ConcreteKnowledgeValidator)

    result = _build_result(has_concrete=False, has_formula=True)
    validator._finalize_validation_result(str(image_path), result, cache_result=False)

    assert image_path.exists()


def test_finalize_validation_result_writes_cache_when_enabled(tmp_path):
    image_path = tmp_path / "cache.png"
    image_path.write_bytes(b"img")

    validator = ConcreteKnowledgeValidator.__new__(ConcreteKnowledgeValidator)
    called = {"count": 0}

    def _fake_cache(image: str, result: ConcreteKnowledgeResult):
        called["count"] += 1
        assert Path(image) == image_path
        assert result.has_concrete

    validator._cache_result = _fake_cache

    result = _build_result(has_concrete=True, has_formula=False)
    returned = validator._finalize_validation_result(str(image_path), result, cache_result=True)

    assert called["count"] == 1
    assert returned is result
    assert image_path.exists()


