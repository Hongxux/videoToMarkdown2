import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[5]))


from services.python_grpc.src.server import dependency_check as dc


def test_normalize_version_core_keeps_numeric_prefix():
    assert dc._normalize_version_core("3.3.0") == "3.3.0"
    assert dc._normalize_version_core("3.3.0.post1") == "3.3.0"
    assert dc._normalize_version_core(" 2.7.3+cu118 ") == "2.7.3"


def test_check_preprocess_dependency_versions_all_match(monkeypatch):
    versions = {
        "paddleocr": "2.7.3",
        "paddlepaddle": "3.3.0.post121",
        "paddlex": "3.4.2",
        "mediapipe": "0.10.14",
    }
    monkeypatch.setattr(dc, "_get_distribution_version", lambda name: versions.get(name))

    ok, detail = dc._check_preprocess_dependency_versions()
    assert ok is True
    assert detail == "ok"


def test_check_preprocess_dependency_versions_reports_mismatch(monkeypatch):
    versions = {
        "paddleocr": "2.7.0.3",
        "paddlepaddle": "3.2.2",
        "paddlex": "3.4.2",
        "mediapipe": None,
    }
    monkeypatch.setattr(dc, "_get_distribution_version", lambda name: versions.get(name))

    ok, detail = dc._check_preprocess_dependency_versions()
    assert ok is False
    assert "paddleocr version mismatch" in detail
    assert "paddlepaddle version mismatch" in detail
    assert "mediapipe not installed" in detail
