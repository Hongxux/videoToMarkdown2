from pathlib import Path

import numpy as np

from services.python_grpc.src.content_pipeline.phase2a.segmentation.concrete_knowledge_validator import (
    ConcreteKnowledgeResult,
    ConcreteKnowledgeValidator,
)
from services.python_grpc.src.content_pipeline.infra.llm import llm_gateway


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


def test_vision_validate_v3_passes_concrete_knowledge_as_system_prompt(monkeypatch):
    validator = ConcreteKnowledgeValidator.__new__(ConcreteKnowledgeValidator)
    validator._vision_client = object()
    validator._concrete_knowledge_system_prompt = "SYSTEM_PROMPT_FROM_USER_MD"

    captured = {}

    def _fake_vision_validate_image_sync(**kwargs):
        captured.update(kwargs)
        return {
            "has_concrete_knowledge": "是",
            "confidence": 0.86,
            "concrete_type": "图示",
            "reason": "ok",
            "img_description": "desc",
        }

    monkeypatch.setattr(llm_gateway, "vision_validate_image_sync", _fake_vision_validate_image_sync)

    region = np.zeros((12, 12, 3), dtype=np.uint8)
    result = validator._vision_validate_v3("unused.png", region)

    assert captured["prompt"] == ""
    assert captured["system_prompt"] == "SYSTEM_PROMPT_FROM_USER_MD"
    assert result.has_concrete is True
    assert result.should_include is True
    assert result.img_description == "desc"


def test_build_result_from_vision_payload_forces_include_true():
    validator = ConcreteKnowledgeValidator.__new__(ConcreteKnowledgeValidator)

    result = validator._build_result_from_vision_payload(
        {
            "has_concrete_knowledge": False,
            "should_include": False,
            "confidence": 0.12,
            "img_description": "only description",
        }
    )

    assert result.has_concrete is True
    assert result.should_include is True
    assert result.img_description == "only description"


def test_build_result_from_vision_payload_uses_raw_response_as_description():
    validator = ConcreteKnowledgeValidator.__new__(ConcreteKnowledgeValidator)

    result = validator._build_result_from_vision_payload(
        {
            "raw_response": "terminal shows pip install output",
            "should_include": False,
        }
    )

    assert result.has_concrete is True
    assert result.should_include is True
    assert "pip install" in result.img_description


def test_validate_does_not_call_extract_graphic_region_when_vision_disabled(monkeypatch, tmp_path):
    image_path = tmp_path / "raw_fallback.png"
    image_path.write_bytes(b"img")

    validator = ConcreteKnowledgeValidator.__new__(ConcreteKnowledgeValidator)
    validator._hash_cache = None
    validator._vision_enabled = False
    validator._vision_client = None

    monkeypatch.setattr(validator, "_detect_math_formula", lambda _ocr_text: False)

    def _should_not_be_called(_self, _image_path):
        raise AssertionError("_extract_graphic_region should not be called in validate()")

    monkeypatch.setattr(
        ConcreteKnowledgeValidator,
        "_extract_graphic_region",
        _should_not_be_called,
        raising=False,
    )
    monkeypatch.setattr(
        validator,
        "_extract_text_page_description",
        lambda image_path, ocr_text="": "ocr summary",
    )
    monkeypatch.setattr(
        validator,
        "_finalize_validation_result",
        lambda _image_path, result, cache_result=True: result,
    )

    result = validator.validate(str(image_path), ocr_text="plain text", skip_duplicate_check=True)

    assert result.should_include is True
    assert "Vision API" in result.reason
    assert result.img_description == "ocr summary"


def test_validate_batch_does_not_call_extract_graphic_region(monkeypatch, tmp_path):
    image_path = tmp_path / "raw_batch.png"
    image_path.write_bytes(b"img")

    validator = ConcreteKnowledgeValidator.__new__(ConcreteKnowledgeValidator)
    validator._hash_cache = None
    validator._vision_enabled = True
    validator._vision_client = type(
        "DummyVisionClient",
        (),
        {"config": type("Cfg", (), {"batch_enabled": True, "batch_max_size": 4})()},
    )()
    validator._concrete_knowledge_system_prompt = "SYSTEM_PROMPT_FROM_USER_MD"

    monkeypatch.setattr(validator, "_detect_math_formula", lambda _ocr_text: False)

    def _should_not_be_called(_self, _image_path):
        raise AssertionError("_extract_graphic_region should not be called in validate_batch()")

    monkeypatch.setattr(
        ConcreteKnowledgeValidator,
        "_extract_graphic_region",
        _should_not_be_called,
        raising=False,
    )
    monkeypatch.setattr(
        validator,
        "_build_result_from_vision_payload",
        lambda payload: ConcreteKnowledgeResult(
            has_concrete=True,
            has_formula=False,
            confidence=0.9,
            concrete_type="demo",
            reason="ok",
            is_mixed=False,
            non_text_ratio=0.0,
            should_include=True,
            img_description="desc",
        ),
    )
    monkeypatch.setattr(
        validator,
        "_finalize_validation_result",
        lambda _image_path, result, cache_result=True: result,
    )
    monkeypatch.setattr(
        llm_gateway,
        "vision_validate_images_sync",
        lambda **kwargs: [{"img_description": "desc"}],
    )

    results = validator._validate_batch_with_vision_api(
        [{"image_path": str(image_path), "ocr_text": "plain text"}]
    )

    assert results is not None
    assert len(results) == 1
    assert results[0].should_include is True


def test_validate_structured_group_sends_multi_images_with_bbox_prompt(monkeypatch, tmp_path):
    image_a = tmp_path / "crop_a.png"
    image_b = tmp_path / "crop_b.png"
    image_a.write_bytes(b"a")
    image_b.write_bytes(b"b")

    class _Cfg:
        batch_enabled = False
        batch_max_size = 1

    class _VisionClient:
        config = _Cfg()

    validator = ConcreteKnowledgeValidator.__new__(ConcreteKnowledgeValidator)
    validator._vision_enabled = True
    validator._vision_client = _VisionClient()
    validator._concrete_knowledge_system_prompt = "SYSTEM_PROMPT_FROM_USER_MD"

    monkeypatch.setattr(
        validator,
        "_finalize_validation_result",
        lambda _image_path, result, cache_result=True: result,
    )

    captured = {}

    def _fake_vision_validate_images_sync(**kwargs):
        captured.update(kwargs)
        return [
            {"img_description": "desc_a"},
            {"img_description": "desc_b"},
        ]

    monkeypatch.setattr(llm_gateway, "vision_validate_images_sync", _fake_vision_validate_images_sync)

    results = validator.validate_structured_group(
        parent_image_path="parent_raw.png",
        items=[
            {
                "image_path": str(image_a),
                "group_type": "figure_bundle",
                "bbox_xyxy": [10, 20, 110, 140],
                "bbox_normalized_xyxy": [0.05, 0.1, 0.55, 0.7],
                "parent_image_size": [200, 200],
            },
            {
                "image_path": str(image_b),
                "group_type": "algorithm",
                "bbox_xyxy": [120, 30, 190, 150],
                "bbox_normalized_xyxy": [0.6, 0.15, 0.95, 0.75],
                "parent_image_size": [200, 200],
            },
        ],
        ocr_text="install command output",
    )

    assert len(results) == 2
    assert results[0].img_description == "desc_a"
    assert results[1].img_description == "desc_b"
    assert results[0].should_include is True
    assert results[1].should_include is True
    assert captured["image_paths"] == [str(image_a), str(image_b)]
    assert captured["max_batch_size"] == 2
    assert "inputs_in_order" in captured["prompt"]
    assert "bbox_xyxy=[10, 20, 110, 140]" in captured["prompt"]
    assert "bbox_xyxy=[120, 30, 190, 150]" in captured["prompt"]
    assert validator._vision_client.config.batch_enabled is False
    assert validator._vision_client.config.batch_max_size == 1
