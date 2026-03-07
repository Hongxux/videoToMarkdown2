import asyncio

from services.python_grpc.src.content_pipeline.markdown_enhancer import (
    EnhancedSection,
    MarkdownEnhancer,
)
from services.python_grpc.src.content_pipeline.infra.llm.prompt_registry import PromptKeys
import services.python_grpc.src.content_pipeline.markdown_enhancer as markdown_enhancer_module


class _RecorderLLMClient:
    def __init__(self):
        self.text_calls = []
        self.json_calls = []

    async def complete_text(self, prompt: str, system_message: str = None):
        self.text_calls.append({"prompt": prompt, "system_message": system_message or ""})

        if prompt.startswith("TEXT_ENHANCE_TEMPLATE::"):
            return "enhanced text", None, None
        if prompt.startswith("LOGIC_EXTRACT_TEMPLATE::"):
            return "logic markdown", None, None
        if prompt.startswith("USER_IMG_DESC_TEMPLATE::"):
            return "augmented body text", None, None
        if prompt.startswith("USER_STRUCTURED_TEMPLATE::"):
            return "structured body text", None, None
        if prompt.startswith("USER_STRUCTURED_PRESERVE_TEMPLATE::"):
            return "structured body text", None, None
        return "ok", None, None

    async def complete_json(self, prompt: str, system_message: str = None):
        self.json_calls.append({"prompt": prompt, "system_message": system_message or ""})
        return {"enhanced_body": "combined enhanced", "structured_content": "combined structured"}, None, None


def _install_prompt_loader_stub(monkeypatch):
    prompt_map = {
        PromptKeys.DEEPSEEK_MD_TEXT_ENHANCE: "TEXT_ENHANCE_TEMPLATE::{body_text}|{ocr_text}|{action_info}",
        PromptKeys.DEEPSEEK_MD_LOGIC_EXTRACT: "LOGIC_EXTRACT_TEMPLATE::{title}|{body_text}|{level_info}|{action_info}",
        PromptKeys.DEEPSEEK_MD_COMBINED_SYSTEM: "COMBINED_SYSTEM_TEMPLATE",
        PromptKeys.DEEPSEEK_MD_COMBINED_USER: "COMBINED_USER_TEMPLATE::{title}|{level_info}|{body_text}|{ocr_text}|{action_info}",
        PromptKeys.DEEPSEEK_MD_STRUCTURED_SYSTEM: "STRUCTURED_SYSTEM_TEMPLATE",
        PromptKeys.DEEPSEEK_MD_STRUCTURED_USER: "USER_STRUCTURED_TEMPLATE::{title}|{knowledge_type}|{body_text}|{image_context}",
        PromptKeys.DEEPSEEK_MD_STRUCTURED_SYSTEM_PRESERVE_IMG: "STRUCTURED_SYSTEM_PRESERVE_TEMPLATE",
        PromptKeys.DEEPSEEK_MD_STRUCTURED_USER_PRESERVE_IMG: "USER_STRUCTURED_PRESERVE_TEMPLATE::{title}|{knowledge_type}|{body_text}|{image_context}|{adjacent_context}",
        PromptKeys.DEEPSEEK_MD_IMG_DESC_AUG_SYSTEM: "IMG_DESC_SYSTEM_TEMPLATE",
        PromptKeys.DEEPSEEK_MD_IMG_DESC_AUG_USER: "USER_IMG_DESC_TEMPLATE::{body_text}|{image_evidence}",
    }
    loaded_keys = []

    def _fake_get_prompt(key: str, *, strict=None, fallback=None):
        loaded_keys.append(key)
        return prompt_map.get(key, fallback if fallback is not None else "")

    monkeypatch.setattr(markdown_enhancer_module, "get_prompt", _fake_get_prompt)
    return set(prompt_map.keys()), loaded_keys


def test_markdown_enhancer_loads_all_md_prompts_from_loader(monkeypatch):
    expected_keys, loaded_keys = _install_prompt_loader_stub(monkeypatch)
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)

    enhancer = MarkdownEnhancer()
    assert expected_keys.issubset(set(loaded_keys))
    assert enhancer._combined_system_prompt == "COMBINED_SYSTEM_TEMPLATE"
    assert enhancer._structured_system_prompt == "STRUCTURED_SYSTEM_TEMPLATE"
    assert enhancer._structured_system_preserve_img_prompt == "STRUCTURED_SYSTEM_PRESERVE_TEMPLATE"
    assert enhancer._img_desc_augment_system_prompt == "IMG_DESC_SYSTEM_TEMPLATE"


def test_markdown_enhancer_runtime_paths_use_loader_templates(monkeypatch, tmp_path):
    _install_prompt_loader_stub(monkeypatch)
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)

    enhancer = MarkdownEnhancer()
    enhancer._enabled = True
    enhancer._llm_client = _RecorderLLMClient()
    enhancer._enable_img_desc_text_augment = True
    enhancer._markdown_dir = str(tmp_path)

    section = EnhancedSection(
        unit_id="SU001",
        title="Unit Title",
        knowledge_type="other",
        level=2,
        parent_id=None,
        original_body="original body",
        enhanced_body="",
        structured_content="",
        screenshots=[],
        screenshot_items=[],
        validated_screenshots=[],
        action_classifications=[],
    )

    section.enhanced_body = asyncio.run(enhancer._enhance_text(section))
    assert section.enhanced_body == "enhanced text"
    assert any(call["prompt"].startswith("TEXT_ENHANCE_TEMPLATE::") for call in enhancer._llm_client.text_calls)

    logic = asyncio.run(enhancer._extract_logic(section))
    assert logic == "logic markdown"
    assert any(call["prompt"].startswith("LOGIC_EXTRACT_TEMPLATE::") for call in enhancer._llm_client.text_calls)

    enhanced_text, structured_text = asyncio.run(enhancer._enhance_and_extract(section))
    assert enhanced_text == "combined enhanced"
    assert structured_text == "combined structured"
    assert any(call["system_message"] == "COMBINED_SYSTEM_TEMPLATE" for call in enhancer._llm_client.json_calls)
    assert any(call["prompt"].startswith("COMBINED_USER_TEMPLATE::") for call in enhancer._llm_client.json_calls)

    img_path = tmp_path / "assets" / "SU001_img_01.png"
    img_path.parent.mkdir(parents=True, exist_ok=True)
    img_path.write_bytes(b"img")
    concept_section = EnhancedSection(
        unit_id="SU001",
        title="Concept Unit",
        knowledge_type="concrete",
        level=2,
        parent_id=None,
        original_body="concrete body",
        enhanced_body="",
        structured_content="",
        screenshots=[str(img_path)],
        screenshot_items=[
            {
                "img_id": "SU001_img_01",
                "img_path": str(img_path),
                "img_description": "console shows command",
                "timestamp_sec": 1.2,
                "sentence_id": "S001",
                "sentence_text": "do something",
            }
        ],
        validated_screenshots=[str(img_path)],
        action_classifications=[],
    )
    concept_md = asyncio.run(enhancer._build_structured_text_for_concept(concept_section))
    assert "structured body text" in concept_md
    assert all(call["system_message"] != "IMG_DESC_SYSTEM_TEMPLATE" for call in enhancer._llm_client.text_calls)
    assert any(call["system_message"] == "STRUCTURED_SYSTEM_PRESERVE_TEMPLATE" for call in enhancer._llm_client.text_calls)
    assert all(not call["prompt"].startswith("USER_IMG_DESC_TEMPLATE::") for call in enhancer._llm_client.text_calls)
    assert any(call["prompt"].startswith("USER_STRUCTURED_PRESERVE_TEMPLATE::") for call in enhancer._llm_client.text_calls)
