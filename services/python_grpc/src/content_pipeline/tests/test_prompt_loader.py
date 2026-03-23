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


def test_render_prompt_category_classifier_injects_values(monkeypatch):
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

    content = render_prompt(
        PromptKeys.DEEPSEEK_CATEGORY_CLASSIFIER_USER,
        context={
            "video_title": "Java 并发",
            "first_unit_text": "开场介绍线程池。",
            "group_names": "- 线程池\n- 锁优化",
            "group_evidence": "1. 线程池\n   - 证据1: 讲解核心线程数与阻塞队列",
            "content_evidence_text": "线程池: 讲解核心线程数与阻塞队列",
            "categories": "编程开发/Java\n系统设计/并发",
            "target_level": 2,
            "max_target_level": 4,
            "required_prefix": "",
        },
    )

    assert "Java 并发" in content
    assert "线程池: 讲解核心线程数与阻塞队列" in content
    assert "{video_title}" not in content
    assert "{group_evidence}" not in content


def test_concrete_knowledge_prompt_supports_system_key_and_legacy_user_key(monkeypatch):
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

    content_from_system_key = get_prompt(PromptKeys.VISION_AI_CONCRETE_KNOWLEDGE_SYSTEM)
    content_from_legacy_user_key = get_prompt(PromptKeys.VISION_AI_CONCRETE_KNOWLEDGE_USER)

    assert isinstance(content_from_system_key, str)
    assert len(content_from_system_key) > 10
    assert content_from_system_key == content_from_legacy_user_key


def test_vl_arg_prompt_keys_are_loadable(monkeypatch):
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

    system_prompt = get_prompt(PromptKeys.DEEPSEEK_VL_ARG_STRUCTURED_SYSTEM)
    user_prompt = get_prompt(PromptKeys.DEEPSEEK_VL_ARG_STRUCTURED_USER)

    assert isinstance(system_prompt, str)
    assert isinstance(user_prompt, str)
    assert len(system_prompt) > 10
    assert "{{main_operation}}" in user_prompt
    assert "{{subtitle_context}}" in user_prompt


def test_markdown_preserve_img_prompt_keys_are_loadable(monkeypatch):
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

    system_prompt = get_prompt(PromptKeys.DEEPSEEK_MD_STRUCTURED_SYSTEM_PRESERVE_IMG)
    user_prompt = get_prompt(PromptKeys.DEEPSEEK_MD_STRUCTURED_USER_PRESERVE_IMG)

    assert isinstance(system_prompt, str)
    assert isinstance(user_prompt, str)
    assert len(system_prompt) > 10
    assert "{body_text}" in user_prompt
    assert "{clip_context}" in user_prompt
    assert "[CLIP_{N}]" in system_prompt

