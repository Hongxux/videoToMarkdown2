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


def test_check_preprocess_dependency_versions_can_scope_to_required_dependencies(monkeypatch):
    versions = {
        "paddleocr": "2.7.0.3",
        "paddlepaddle": "3.2.2",
        "paddlex": "3.4.2",
        "mediapipe": "0.10.14",
    }
    monkeypatch.setattr(dc, "_get_distribution_version", lambda name: versions.get(name))

    ok, detail = dc._check_preprocess_dependency_versions({"mediapipe"})

    assert ok is True
    assert detail == "ok"


def test_resolve_optional_feature_flags_respects_disabled_optional_structure_features():
    config = {
        "content_pipeline": {
            "phase2b": {
                "concrete_ai_vision": {
                    "enabled": False,
                }
            }
        },
        "visual": {
            "structure_preprocess": {
                "enabled": False,
            }
        },
        "vision_ai": {
            "person_subject_filter": {
                "enabled": True,
            }
        },
    }

    flags = dc._resolve_optional_feature_flags(config)

    assert flags["ppstructure_preprocess"] is False
    assert flags["paddlex_layout_fallback"] is False
    assert flags["person_subject_prefilter"] is True


def test_run_dependency_check_skips_disabled_ppstructure_checks(monkeypatch):
    monkeypatch.setattr(dc, "_prepare_preflight_paths", lambda: None)
    monkeypatch.setattr(dc, "_load_preflight_config", lambda: {
        "content_pipeline": {"phase2b": {"concrete_ai_vision": {"enabled": False}}},
        "visual": {"structure_preprocess": {"enabled": False}},
        "vision_ai": {"person_subject_filter": {"enabled": False}},
    })
    monkeypatch.setattr(dc.importlib, "import_module", lambda name: object())
    monkeypatch.setattr(dc, "_check_pydantic_core_schema_compatibility", lambda: (True, "ok"))
    monkeypatch.setattr(dc, "_check_preprocess_dependency_versions", lambda required=None: (_ for _ in ()).throw(AssertionError("should not run")))
    monkeypatch.setattr(dc, "_check_ppstructure_importable", lambda: (_ for _ in ()).throw(AssertionError("should not run")))
    monkeypatch.setattr(dc, "_check_paddlex_importable", lambda: (_ for _ in ()).throw(AssertionError("should not run")))
    monkeypatch.setattr(dc, "_check_person_prefilter_backend", lambda: (_ for _ in ()).throw(AssertionError("should not run")))

    exit_code = dc.run_dependency_check(debug_imports=False)

    assert exit_code == 0
