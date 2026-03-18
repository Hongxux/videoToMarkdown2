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


def test_markdown_enhancer_initializes_phase2b_deepseek_client_via_gateway(monkeypatch):
    _install_prompt_loader_stub(monkeypatch)
    monkeypatch.setenv("DEEPSEEK_API_KEY", "phase2b-key")

    fake_client = object()
    captured = {}

    def _fake_get_deepseek_client(
        api_key=None,
        base_url="",
        model="",
        temperature=0.3,
        enable_logprobs=None,
        cache_enabled=None,
        inflight_dedup_enabled=None,
    ):
        captured.update(
            {
                "api_key": api_key,
                "base_url": base_url,
                "model": model,
                "temperature": temperature,
                "enable_logprobs": enable_logprobs,
                "cache_enabled": cache_enabled,
                "inflight_dedup_enabled": inflight_dedup_enabled,
            }
        )
        return fake_client

    monkeypatch.setattr(markdown_enhancer_module.llm_gateway, "get_deepseek_client", _fake_get_deepseek_client)

    enhancer = MarkdownEnhancer(base_url="https://api.deepseek.com")

    assert enhancer._enabled is True
    assert enhancer._llm_client is fake_client
    assert captured["api_key"] == "phase2b-key"
    assert captured["base_url"] == "https://api.deepseek.com/v1"


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


def test_markdown_enhancer_routes_deepseek_calls_through_gateway(monkeypatch):
    _install_prompt_loader_stub(monkeypatch)
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)

    gateway_calls = {"text": [], "json": []}

    async def _fake_deepseek_complete_text(*, prompt: str, system_message: str = None, client=None, model=""):
        gateway_calls["text"].append(
            {
                "prompt": prompt,
                "system_message": system_message or "",
                "client": client,
                "model": model,
            }
        )
        return "gateway enhanced", None, None

    async def _fake_deepseek_complete_json(*, prompt: str, system_message: str = None, client=None, model=""):
        gateway_calls["json"].append(
            {
                "prompt": prompt,
                "system_message": system_message or "",
                "client": client,
                "model": model,
            }
        )
        return {
            "enhanced_body": "gateway combined enhanced",
            "structured_content": "gateway combined structured",
        }, None, None

    monkeypatch.setattr(markdown_enhancer_module.llm_gateway, "deepseek_complete_text", _fake_deepseek_complete_text)
    monkeypatch.setattr(markdown_enhancer_module.llm_gateway, "deepseek_complete_json", _fake_deepseek_complete_json)

    enhancer = MarkdownEnhancer()
    enhancer._enabled = True
    enhancer._llm_client = _RecorderLLMClient()
    monkeypatch.setattr(enhancer, "_should_route_deepseek_via_gateway", lambda: True)

    section = EnhancedSection(
        unit_id="SU009",
        title="Gateway Unit",
        knowledge_type="other",
        level=2,
        parent_id=None,
        original_body="gateway body",
        enhanced_body="",
        structured_content="",
        screenshots=[],
        screenshot_items=[],
        validated_screenshots=[],
        action_classifications=[],
    )

    enhanced_text = asyncio.run(enhancer._enhance_text(section))
    assert enhanced_text == "gateway enhanced"
    assert gateway_calls["text"]
    assert gateway_calls["text"][0]["prompt"].startswith("TEXT_ENHANCE_TEMPLATE::")
    assert gateway_calls["text"][0]["client"] is enhancer._llm_client
    assert gateway_calls["text"][0]["model"] == enhancer._structured_text_model

    combined_enhanced, combined_structured = asyncio.run(enhancer._enhance_and_extract(section))
    assert combined_enhanced == "gateway combined enhanced"
    assert combined_structured == "gateway combined structured"
    assert gateway_calls["json"]
    assert gateway_calls["json"][0]["prompt"].startswith("COMBINED_USER_TEMPLATE::")
    assert gateway_calls["json"][0]["system_message"] == "COMBINED_SYSTEM_TEMPLATE"
    assert gateway_calls["json"][0]["client"] is enhancer._llm_client
    assert gateway_calls["json"][0]["model"] == enhancer._structured_text_model
