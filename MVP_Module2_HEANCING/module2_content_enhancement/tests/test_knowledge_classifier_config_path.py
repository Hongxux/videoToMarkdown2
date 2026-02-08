import os

from MVP_Module2_HEANCING.module2_content_enhancement.knowledge_classifier import KnowledgeClassifier


def test_resolve_config_path_falls_back_to_video_to_markdown(tmp_path, monkeypatch):
    root_config = tmp_path / "config.yaml"
    if root_config.exists():
        root_config.unlink()

    subdir = tmp_path / "videoToMarkdown"
    subdir.mkdir(parents=True, exist_ok=True)
    sub_config = subdir / "config.yaml"
    sub_config.write_text("ai:\n  analysis:\n    model: deepseek-chat\n", encoding="utf-8")

    monkeypatch.delenv("MODULE2_CONFIG_PATH", raising=False)
    monkeypatch.chdir(tmp_path)

    from pathlib import Path
    import MVP_Module2_HEANCING.module2_content_enhancement.knowledge_classifier as kc_mod

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
    assert os.path.normpath(resolved) == os.path.normpath(str(sub_config))

