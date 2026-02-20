from services.python_grpc.src.vision_validation.worker import (
    _extract_ocr_tokens,
    _filter_incremental_screenshots,
    _is_incremental_screenshot,
)
import numpy as np


def test_incremental_by_ocr_tokens():
    base = {
        "ocr_tokens": ["open", "settings"],
        "shape_signature": {"rect_count": 1, "component_count": 2},
    }
    candidate = {
        "ocr_tokens": ["open", "settings", "network"],
        "shape_signature": {"rect_count": 1, "component_count": 2},
    }

    assert _is_incremental_screenshot(base, candidate) is True


def test_incremental_by_shape_signature():
    base = {
        "ocr_tokens": [],
        "shape_signature": {"rect_count": 2, "component_count": 3},
    }
    candidate = {
        "ocr_tokens": [],
        "shape_signature": {"rect_count": 3, "component_count": 4},
    }

    assert _is_incremental_screenshot(base, candidate) is True


def test_filter_incremental_keeps_only_non_covered():
    items = [
        {
            "timestamp_sec": 1.0,
            "score": 0.60,
            "ocr_tokens": ["open"],
            "shape_signature": {"rect_count": 1, "component_count": 1},
        },
        {
            "timestamp_sec": 2.0,
            "score": 0.70,
            "ocr_tokens": ["open", "settings"],
            "shape_signature": {"rect_count": 2, "component_count": 2},
        },
        {
            "timestamp_sec": 3.0,
            "score": 0.65,
            "ocr_tokens": ["other"],
            "shape_signature": {"rect_count": 1, "component_count": 1},
        },
    ]

    filtered = _filter_incremental_screenshots(items)
    kept_ts = sorted(float(item["timestamp_sec"]) for item in filtered)
    assert kept_ts == [2.0]


def test_extract_ocr_tokens_excludes_subtitle_like_regions(monkeypatch):
    frame = np.zeros((100, 100, 3), dtype=np.uint8)

    monkeypatch.setattr(
        "services.python_grpc.src.vision_validation.worker._extract_ocr_regions_from_crop",
        lambda _crop: [
            {"text": "\u8bbe\u7f6e network", "x": 10, "y": 8, "w": 40, "h": 10, "confidence": 0.95},
            {"text": "\u8fd9\u662f\u5b57\u5e55 english subtitle", "x": 5, "y": 82, "w": 92, "h": 10, "confidence": 0.98},
        ],
    )
    monkeypatch.setattr(
        "services.python_grpc.src.vision_validation.worker._get_ocr_extractor",
        lambda: None,
    )

    tokens = _extract_ocr_tokens(frame, roi=(0, 0, 100, 100))
    assert "\u8bbe\u7f6e" in tokens
    assert "network" in tokens
    assert "\u8fd9\u662f\u5b57\u5e55" not in tokens
    assert "subtitle" not in tokens


def test_extract_ocr_tokens_keeps_bottom_text_when_not_subtitle_like(monkeypatch):
    frame = np.zeros((100, 100, 3), dtype=np.uint8)

    monkeypatch.setattr(
        "services.python_grpc.src.vision_validation.worker._extract_ocr_regions_from_crop",
        lambda _crop: [
            {"text": "\u7aef\u53e3 8080", "x": 4, "y": 84, "w": 18, "h": 10, "confidence": 0.92},
        ],
    )
    monkeypatch.setattr(
        "services.python_grpc.src.vision_validation.worker._get_ocr_extractor",
        lambda: None,
    )

    tokens = _extract_ocr_tokens(frame, roi=(0, 0, 100, 100))
    assert "\u7aef\u53e3" in tokens
    assert "8080" in tokens


def test_extract_ocr_tokens_expands_modifier_noun_phrase(monkeypatch):
    frame = np.zeros((100, 100, 3), dtype=np.uint8)

    monkeypatch.setattr(
        "services.python_grpc.src.vision_validation.worker._extract_ocr_regions_from_crop",
        lambda _crop: [
            {"text": "\u7ea2\u8272\u7684\u82f9\u679c", "x": 8, "y": 12, "w": 40, "h": 10, "confidence": 0.96},
        ],
    )
    monkeypatch.setattr(
        "services.python_grpc.src.vision_validation.worker._get_ocr_extractor",
        lambda: None,
    )

    tokens = _extract_ocr_tokens(frame, roi=(0, 0, 100, 100))
    assert "\u7ea2\u8272\u7684\u82f9\u679c" in tokens
    assert "\u7ea2\u8272" in tokens
    assert "\u82f9\u679c" in tokens
    assert "\u7ea2\u8272\u82f9\u679c" in tokens


def test_extract_ocr_tokens_supports_compound_chinese_incremental(monkeypatch):
    frame = np.zeros((100, 100, 3), dtype=np.uint8)

    monkeypatch.setattr(
        "services.python_grpc.src.vision_validation.worker._extract_ocr_regions_from_crop",
        lambda _crop: [
            {"text": "\u7f51\u7edc\u8bbe\u7f6e", "x": 8, "y": 12, "w": 36, "h": 10, "confidence": 0.93},
        ],
    )
    monkeypatch.setattr(
        "services.python_grpc.src.vision_validation.worker._get_ocr_extractor",
        lambda: None,
    )
    base_tokens = _extract_ocr_tokens(frame, roi=(0, 0, 100, 100))

    monkeypatch.setattr(
        "services.python_grpc.src.vision_validation.worker._extract_ocr_regions_from_crop",
        lambda _crop: [
            {"text": "\u6253\u5f00\u7f51\u7edc\u8bbe\u7f6e\u9ad8\u7ea7", "x": 8, "y": 12, "w": 58, "h": 10, "confidence": 0.93},
        ],
    )
    candidate_tokens = _extract_ocr_tokens(frame, roi=(0, 0, 100, 100))

    base = {
        "ocr_tokens": sorted(base_tokens),
        "shape_signature": {"rect_count": 0, "component_count": 0},
    }
    candidate = {
        "ocr_tokens": sorted(candidate_tokens),
        "shape_signature": {"rect_count": 0, "component_count": 0},
    }
    assert _is_incremental_screenshot(base, candidate) is True
