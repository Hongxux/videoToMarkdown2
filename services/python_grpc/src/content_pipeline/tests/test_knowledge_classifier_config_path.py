import os

from services.python_grpc.src.content_pipeline.phase2a.segmentation.knowledge_classifier import KnowledgeClassifier


def test_resolve_config_path_prefers_unified_config_dir(tmp_path, monkeypatch):
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    unified_config = config_dir / "video_config.yaml"
    unified_config.write_text("ai:\n  analysis:\n    model: deepseek-chat\n", encoding="utf-8")

    # legacy file exists but should lose to unified file
    subdir = tmp_path / "videoToMarkdown"
    subdir.mkdir(parents=True, exist_ok=True)
    (subdir / "config.yaml").write_text("ai:\n  analysis:\n    model: legacy-model\n", encoding="utf-8")

    monkeypatch.delenv("MODULE2_CONFIG_PATH", raising=False)
    monkeypatch.chdir(tmp_path)

    from pathlib import Path
    import services.python_grpc.src.content_pipeline.phase2a.segmentation.knowledge_classifier as kc_mod

    fake_file = tmp_path / "MVP_Module2_HEANCING" / "module2_content_enhancement" / "knowledge_classifier.py"
    fake_file.parent.mkdir(parents=True, exist_ok=True)
    fake_file.write_text("# stub", encoding="utf-8")

    original_file = kc_mod.__file__
    kc_mod.__file__ = str(fake_file)
    try:
        resolved = KnowledgeClassifier._resolve_config_path()
    finally:
        kc_mod.__file__ = original_file

    assert resolved
    assert os.path.normpath(resolved) == os.path.normpath(str(unified_config))


