from pathlib import Path

import pytest

from services.python_grpc.src.content_pipeline.infra.llm import prompt_loader
from services.python_grpc.src.content_pipeline.infra.llm.prompt_loader import clear_prompt_loader_cache, get_prompt, render_prompt
from services.python_grpc.src.content_pipeline.infra.llm.prompt_registry import PromptKeys


def _reset_loader_cache() -> None:
    clear_prompt_loader_cache()


def test_get_prompt_uses_package_default(monkeypatch):
    _reset_loader_cache()
    monkeypatch.setattr(
        prompt_loader,
        "load_module2_config",
        lambda: {
            "prompt_management": {
                "enabled": True,
                "root_dir": "",
                "overrides": {},
                "strict": False,
            }
        },
    )

    content = get_prompt(PromptKeys.DEEPSEEK_SEMANTIC_SEGMENT_SYSTEM)
    assert isinstance(content, str)
    assert len(content) > 10


def test_get_prompt_uses_root_dir_override(tmp_path, monkeypatch):
    _reset_loader_cache()
    root_dir = tmp_path / "prompts"
    target = root_dir / "deepseek" / "semantic" / "segment_system.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("custom segment system", encoding="utf-8")

    monkeypatch.setattr(
        prompt_loader,
        "load_module2_config",
        lambda: {
            "prompt_management": {
                "enabled": True,
                "root_dir": str(root_dir),
                "overrides": {},
                "strict": False,
            }
        },
    )

    content = get_prompt(PromptKeys.DEEPSEEK_SEMANTIC_SEGMENT_SYSTEM)
    assert content == "custom segment system"


def test_get_prompt_uses_key_override(tmp_path, monkeypatch):
    _reset_loader_cache()
    custom_file = tmp_path / "my_prompt.md"
    custom_file.write_text("override content", encoding="utf-8")

    monkeypatch.setattr(
        prompt_loader,
        "load_module2_config",
        lambda: {
            "prompt_management": {
                "enabled": True,
                "root_dir": "",
                "overrides": {
                    PromptKeys.DEEPSEEK_KC_BATCH_SYSTEM: str(custom_file),
                },
                "strict": False,
            }
        },
    )

    content = get_prompt(PromptKeys.DEEPSEEK_KC_BATCH_SYSTEM)
    assert content == "override content"


def test_get_prompt_returns_fallback_when_file_missing(tmp_path, monkeypatch):
    _reset_loader_cache()
    empty_root = tmp_path / "empty_prompts"
    empty_root.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        prompt_loader,
        "load_module2_config",
        lambda: {
            "prompt_management": {
                "enabled": True,
                "root_dir": str(empty_root),
                "overrides": {},
                "strict": False,
            }
        },
    )
    monkeypatch.setattr(prompt_loader, "_PACKAGE_PROMPT_ROOT", empty_root)

    content = get_prompt(
        PromptKeys.DEEPSEEK_VIDEO_CLIP_MOTION_VALUE_SYSTEM,
        fallback="fallback content",
    )
    assert content == "fallback content"


def test_get_prompt_strict_mode_raises_when_missing(tmp_path, monkeypatch):
    _reset_loader_cache()
    empty_root = tmp_path / "empty_prompts"
    empty_root.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        prompt_loader,
        "load_module2_config",
        lambda: {
            "prompt_management": {
                "enabled": True,
                "root_dir": str(empty_root),
                "overrides": {},
                "strict": True,
            }
        },
    )
    monkeypatch.setattr(prompt_loader, "_PACKAGE_PROMPT_ROOT", empty_root)

    with pytest.raises(FileNotFoundError):
        get_prompt(PromptKeys.DEEPSEEK_VIDEO_CLIP_TRANSITION_SYSTEM)


def test_render_prompt_missing_variable_raises(monkeypatch):
    _reset_loader_cache()
    monkeypatch.setattr(
        prompt_loader,
        "load_module2_config",
        lambda: {
            "prompt_management": {
                "enabled": True,
                "root_dir": "",
                "overrides": {},
                "strict": False,
            }
        },
    )

    with pytest.raises(KeyError):
        render_prompt(PromptKeys.DEEPSEEK_KC_BATCH_USER, context={"title": "only-title"})

