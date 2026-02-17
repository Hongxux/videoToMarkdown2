from services.python_grpc.src.vision_validation.worker import (
    _filter_incremental_screenshots,
    _is_incremental_screenshot,
)


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

    # 3.0 会被 2.0 以“形状覆盖且更多”规则覆盖，仅保留增量更大的候选。
    assert kept_ts == [2.0]
