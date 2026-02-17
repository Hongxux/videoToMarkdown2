import asyncio
import json
from pathlib import Path

from services.python_grpc.src.content_pipeline.markdown_enhancer import MarkdownEnhancer, EnhancedSection


STRUCTURED_SYS_MARKER = "教学内容结构化助手"
AUGMENT_SYS_MARKER = "教学文本补全助手"
AUGMENT_PROMPT_MARKER = "图片证据（按时间/句子对齐）"
STRUCTURED_PROMPT_MARKER = "图片候选（可为空）"
AUGMENTED_BODY_TEXT = "body enriched with npm run dev"
BASE_BODY_TEXT = "body base text without command"


class _FakeLLMClient:
    def __init__(self, structured_text: str):
        self._structured_text = structured_text

    async def complete_text(self, prompt: str, system_message: str = None):
        return self._structured_text, None, None


class _Meta:
    def __init__(self, model: str = "fake-model", prompt_tokens: int = 10, completion_tokens: int = 20, total_tokens: int = 30):
        self.model = model
        self.prompt_tokens = prompt_tokens
        self.completion_tokens = completion_tokens
        self.total_tokens = total_tokens


class _TraceLLMClient:
    async def complete_text(self, prompt: str, system_message: str = None):
        sys_msg = system_message or ""
        if AUGMENT_PROMPT_MARKER in prompt or AUGMENT_SYS_MARKER in sys_msg:
            return "augment trace body", _Meta(), None
        if STRUCTURED_PROMPT_MARKER in prompt or STRUCTURED_SYS_MARKER in sys_msg:
            return "structured trace body", _Meta(), None
        # hierarchy
        return '{"hierarchy":[{"unit_id":"SU400","level":2,"parent_id":null}]}', _Meta(), None


class _RecorderAugmentLLMClient:
    def __init__(self):
        self.calls = []

    async def complete_text(self, prompt: str, system_message: str = None):
        system = system_message or ""
        self.calls.append({"prompt": prompt, "system_message": system})

        if AUGMENT_PROMPT_MARKER in prompt or AUGMENT_SYS_MARKER in system:
            if "npm run dev" in prompt:
                return AUGMENTED_BODY_TEXT, None, None
            return BASE_BODY_TEXT, None, None

        if STRUCTURED_PROMPT_MARKER in prompt or STRUCTURED_SYS_MARKER in system:
            if AUGMENTED_BODY_TEXT in prompt:
                return AUGMENTED_BODY_TEXT, None, None
            return BASE_BODY_TEXT, None, None

        return "", None, None


def _write_result_json(path: Path, sections):
    groups = []
    for idx, section in enumerate(sections, start=1):
        if not isinstance(section, dict):
            continue
        group_id = int(section.get("group_id", idx) or idx)
        group_name = str(section.get("group_name", "") or "").strip() or str(section.get("title", "") or f"Group {idx}")
        group_reason = str(section.get("group_reason", "") or "").strip()
        groups.append(
            {
                "group_id": group_id,
                "group_name": group_name,
                "reason": group_reason,
                "units": [section],
            }
        )
    payload = {
        "title": "Demo Document",
        "knowledge_groups": groups,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def test_concrete_section_placeholder_replaced_with_obsidian_embed(tmp_path, monkeypatch):
    assets_dir = tmp_path / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)
    img_path = assets_dir / "SU001_img_01.png"
    img_path.write_bytes(b"img")

    result_path = tmp_path / "result.json"
    _write_result_json(
        result_path,
        [
            {
                "unit_id": "SU001",
                "title": "Concrete Unit",
                "knowledge_type": "concrete",
                "body_text": "concrete concept body",
                "mult_steps": False,
                "instructional_steps": [],
                "materials": {
                    "screenshots": [str(img_path)],
                    "screenshot_items": [
                        {
                            "img_id": "SU001_img_01",
                            "img_path": str(img_path),
                            "img_description": "final configured screen",
                        }
                    ],
                    "clip": "",
                    "action_classifications": [],
                },
            }
        ],
    )

    enhancer = MarkdownEnhancer()
    enhancer._enabled = True
    enhancer._llm_client = _FakeLLMClient("- key step\n【imgneeded_SU001_img_01】\n- end")

    async def _fake_hierarchy(sections, subject):
        return {"SU001": {"level": 2, "parent_id": None}}

    monkeypatch.setattr(enhancer, "_classify_hierarchy", _fake_hierarchy)

    markdown = asyncio.run(
        enhancer.enhance(
            str(result_path),
            subject="test",
            markdown_dir=str(tmp_path),
        )
    )

    assert "【imgneeded_SU001_img_01】" not in markdown
    assert "![[assets/SU001_img_01.png]]" in markdown
    assert "### Concrete Unit" in markdown


def test_concrete_section_embed_preserves_assets_subdirectory(tmp_path, monkeypatch):
    assets_dir = tmp_path / "assets" / "SU888"
    assets_dir.mkdir(parents=True, exist_ok=True)
    img_path = assets_dir / "SU888_ss_head_001.png"
    img_path.write_bytes(b"img")

    result_path = tmp_path / "result.json"
    _write_result_json(
        result_path,
        [
            {
                "unit_id": "SU888",
                "title": "Concrete Unit",
                "knowledge_type": "concrete",
                "body_text": "concrete concept body",
                "mult_steps": False,
                "instructional_steps": [],
                "materials": {
                    "screenshots": [str(img_path)],
                    "screenshot_items": [
                        {
                            "img_id": "SU888_img_01",
                            "img_path": str(img_path),
                            "img_description": "final configured screen",
                        }
                    ],
                    "clip": "",
                    "action_classifications": [],
                },
            }
        ],
    )

    enhancer = MarkdownEnhancer()
    enhancer._enabled = True
    enhancer._llm_client = _FakeLLMClient("- key step\n【imgneeded_SU888_img_01】\n- end")

    async def _fake_hierarchy(sections, subject):
        return {"SU888": {"level": 2, "parent_id": None}}

    monkeypatch.setattr(enhancer, "_classify_hierarchy", _fake_hierarchy)

    markdown = asyncio.run(
        enhancer.enhance(
            str(result_path),
            subject="test",
            markdown_dir=str(tmp_path),
        )
    )

    assert "![[assets/SU888/SU888_ss_head_001.png]]" in markdown


def test_process_multistep_renders_ordered_steps_with_assets(tmp_path):
    unit_dir = tmp_path / "vl_tutorial_units" / "SU002"
    unit_dir.mkdir(parents=True, exist_ok=True)

    clip1 = unit_dir / "SU002_clip_step_01_open_settings.mp4"
    key1 = unit_dir / "SU002_ss_step_01_key_01_open_settings.png"
    clip2 = unit_dir / "SU002_clip_step_02_change_port.mp4"
    key2 = unit_dir / "SU002_ss_step_02_key_01_change_port.png"
    for file_path in [clip1, key1, clip2, key2]:
        file_path.write_bytes(b"asset")

    steps_payload = {
        "unit_id": "SU002",
        "schema": "tutorial_stepwise_v1",
        "raw_response": [
            {
                "step_id": 1,
                "step_description": "open settings",
                "main_action": "open settings panel",
                "main_operation": ["click settings", "open network tab"],
                "precautions": ["do not edit unrelated options"],
                "step_summary": "settings panel opened and network tab visible",
                "operation_guidance": ["click settings first", "then open network tab"],
                "clip_start_sec": 0.0,
                "clip_end_sec": 6.0,
                "instructional_keyframe_timestamp": [5.4],
            },
            {
                "step_id": 2,
                "step_description": "change port",
                "main_action": "change service port",
                "main_operation": ["update port", "save config"],
                "precautions": [],
                "clip_start_sec": 6.0,
                "clip_end_sec": 13.0,
                "instructional_keyframe_timestamp": [12.2],
            },
        ],
        "steps": [
            {
                "step_id": 1,
                "step_description": "open settings",
                "main_action": "open settings panel",
                "main_operation": ["click settings", "open network tab"],
                "precautions": ["do not edit unrelated options"],
                "step_summary": "settings panel opened and network tab visible",
                "operation_guidance": ["click settings first", "then open network tab"],
                "clip_start_sec": 0.0,
                "clip_end_sec": 6.0,
                "clip_file": clip1.name,
                "instructional_keyframes": [key1.name],
            },
            {
                "step_id": 2,
                "step_description": "change port",
                "main_action": "change service port",
                "main_operation": ["update port", "save config"],
                "precautions": [],
                "clip_start_sec": 6.0,
                "clip_end_sec": 13.0,
                "clip_file": clip2.name,
                "instructional_keyframes": [key2.name],
            },
        ],
    }
    (unit_dir / "SU002_steps.json").write_text(json.dumps(steps_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    result_path = tmp_path / "result.json"
    _write_result_json(
        result_path,
        [
            {
                "unit_id": "SU002",
                "title": "Process Unit",
                "knowledge_type": "process",
                "body_text": "process body",
                "mult_steps": True,
                "instructional_steps": [],
                "materials": {
                    "screenshots": [],
                    "screenshot_items": [],
                    "clip": "",
                    "action_classifications": [],
                },
            }
        ],
    )

    enhancer = MarkdownEnhancer()
    markdown = asyncio.run(
        enhancer.enhance(
            str(result_path),
            subject="test",
            markdown_dir=str(tmp_path),
        )
    )

    assert "1. 1. open settings: from 0.00s to 6.00s" in markdown
    assert "2. 2. change port: from 6.00s to 13.00s" in markdown
    assert "主要动作: open settings panel" in markdown
    assert "主要操作: click settings；open network tab" in markdown
    assert "注意事项: do not edit unrelated options" in markdown
    assert "步骤小结: settings panel opened and network tab visible" in markdown
    assert "操作指导: click settings first；then open network tab" in markdown
    assert "注意事项: avoid occupied ports" not in markdown
    assert markdown.count("步骤小结:") == 1
    assert markdown.count("操作指导:") == 1
    assert "![[vl_tutorial_units/SU002/SU002_ss_step_01_key_01_open_settings.png]]" in markdown
    assert "![[vl_tutorial_units/SU002/SU002_clip_step_02_change_port.mp4]]" in markdown
    assert "> ?? **" not in markdown


def test_group_and_unit_headings_use_fixed_two_level_structure(tmp_path, monkeypatch):
    result_path = tmp_path / "result.json"
    _write_result_json(
        result_path,
        [
            {
                "unit_id": "SU010",
                "title": "Root Concept",
                "knowledge_type": "abstract",
                "body_text": "root body",
                "mult_steps": False,
                "instructional_steps": [],
                "materials": {
                    "screenshots": [],
                    "screenshot_items": [],
                    "clip": "",
                    "action_classifications": [],
                },
            },
            {
                "unit_id": "SU011",
                "title": "Leaf Detail",
                "knowledge_type": "abstract",
                "body_text": "leaf body",
                "mult_steps": False,
                "instructional_steps": [],
                "materials": {
                    "screenshots": [],
                    "screenshot_items": [],
                    "clip": "",
                    "action_classifications": [],
                },
            },
        ],
    )

    enhancer = MarkdownEnhancer()

    async def _fake_hierarchy(sections, subject):
        return {
            "SU010": {"level": 1, "parent_id": None},
            "SU011": {"level": 3, "parent_id": "SU010"},
        }

    monkeypatch.setattr(enhancer, "_classify_hierarchy", _fake_hierarchy)

    markdown = asyncio.run(
        enhancer.enhance(
            str(result_path),
            subject="test",
            markdown_dir=str(tmp_path),
        )
    )

    assert "## Root Concept" in markdown
    assert "### Root Concept" in markdown
    assert "## Leaf Detail" in markdown
    assert "### Leaf Detail" in markdown
    assert "#### Leaf Detail" not in markdown


def test_concrete_imgneeded_placeholders_are_replaced(tmp_path, monkeypatch):
    assets_dir = tmp_path / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)
    img1 = assets_dir / "SU100_img_01.png"
    img2 = assets_dir / "SU100_img_02.png"
    img1.write_bytes(b"img1")
    img2.write_bytes(b"img2")

    result_path = tmp_path / "result.json"
    _write_result_json(
        result_path,
        [
            {
                "unit_id": "SU100",
                "title": "Concrete Variants",
                "knowledge_type": "concrete",
                "body_text": "variant placeholder test",
                "mult_steps": False,
                "instructional_steps": [],
                "materials": {
                    "screenshots": [str(img1), str(img2)],
                    "screenshot_items": [
                        {"img_id": "SU100_img_01", "img_path": str(img1), "img_description": "first"},
                        {"img_id": "SU100_img_02", "img_path": str(img2), "img_description": "second"},
                    ],
                    "clip": "",
                    "action_classifications": [],
                },
            }
        ],
    )

    enhancer = MarkdownEnhancer()
    enhancer._enabled = True
    enhancer._llm_client = _FakeLLMClient(
        "line1 【imgneeded_SU100_img_01】\nline2 【imgneeded_SU100_img_02】\nline3"
    )

    async def _fake_hierarchy(sections, subject):
        return {"SU100": {"level": 2, "parent_id": None}}

    monkeypatch.setattr(enhancer, "_classify_hierarchy", _fake_hierarchy)

    markdown = asyncio.run(
        enhancer.enhance(
            str(result_path),
            subject="test",
            markdown_dir=str(tmp_path),
        )
    )

    assert "【imgneeded_SU100_img_01】" not in markdown
    assert "【imgneeded_SU100_img_02】" not in markdown
    assert "![[assets/SU100_img_01.png]]" in markdown
    assert "![[assets/SU100_img_02.png]]" in markdown


def test_old_img_placeholder_not_replaced_but_supplemental_images_present(tmp_path, monkeypatch):
    assets_dir = tmp_path / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)
    img_path = assets_dir / "SU150_img_01.png"
    img_path.write_bytes(b"img")

    result_path = tmp_path / "result.json"
    _write_result_json(
        result_path,
        [
            {
                "unit_id": "SU150",
                "title": "Concrete Old Placeholder",
                "knowledge_type": "concrete",
                "body_text": "old placeholder body",
                "mult_steps": False,
                "instructional_steps": [],
                "materials": {
                    "screenshots": [str(img_path)],
                    "screenshot_items": [
                        {
                            "img_id": "SU150_img_01",
                            "img_path": str(img_path),
                            "img_description": "old format sample",
                        }
                    ],
                    "clip": "",
                    "action_classifications": [],
                },
            }
        ],
    )

    enhancer = MarkdownEnhancer()
    enhancer._enabled = True
    enhancer._llm_client = _FakeLLMClient("line [IMG:SU150_img_01]")

    async def _fake_hierarchy(sections, subject):
        return {"SU150": {"level": 2, "parent_id": None}}

    monkeypatch.setattr(enhancer, "_classify_hierarchy", _fake_hierarchy)

    markdown = asyncio.run(
        enhancer.enhance(
            str(result_path),
            subject="test",
            markdown_dir=str(tmp_path),
        )
    )

    assert "[IMG:SU150_img_01]" in markdown
    assert "Supplemental images:" in markdown
    assert "![[assets/SU150_img_01.png]]" in markdown


def test_no_image_candidates_strip_imgneeded_tokens(tmp_path, monkeypatch):
    result_path = tmp_path / "result.json"
    _write_result_json(
        result_path,
        [
            {
                "unit_id": "SU151",
                "title": "No Image Candidate",
                "knowledge_type": "abstract",
                "body_text": "plain body",
                "mult_steps": False,
                "instructional_steps": [],
                "materials": {
                    "screenshots": [],
                    "screenshot_items": [],
                    "clip": "",
                    "action_classifications": [],
                },
            }
        ],
    )

    enhancer = MarkdownEnhancer()
    enhancer._enabled = True
    enhancer._llm_client = _FakeLLMClient(
        "- line1【imgneeded_{{img_id}}】\n- line2【imgneeded_】\n- line3"
    )

    async def _fake_hierarchy(sections, subject):
        return {"SU151": {"level": 2, "parent_id": None}}

    monkeypatch.setattr(enhancer, "_classify_hierarchy", _fake_hierarchy)

    markdown = asyncio.run(
        enhancer.enhance(
            str(result_path),
            subject="test",
            markdown_dir=str(tmp_path),
        )
    )

    assert "imgneeded" not in markdown
    assert "line1" in markdown
    assert "line2" in markdown
    assert "line3" in markdown


def test_process_non_tutorial_uses_placeholder_replacement_and_video_tail(tmp_path, monkeypatch):
    assets_dir = tmp_path / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)
    img_path = assets_dir / "SU200_img_01.png"
    clip_path = assets_dir / "SU200_clip_01.mp4"
    img_path.write_bytes(b"img")
    clip_path.write_bytes(b"clip")

    result_path = tmp_path / "result.json"
    _write_result_json(
        result_path,
        [
            {
                "unit_id": "SU200",
                "title": "Process Unit",
                "knowledge_type": "process",
                "body_text": "process concept body",
                "mult_steps": False,
                "instructional_steps": [],
                "materials": {
                    "screenshots": [str(img_path)],
                    "screenshot_items": [
                        {
                            "img_id": "SU200_img_01",
                            "img_path": str(img_path),
                            "img_description": "open menu",
                        }
                    ],
                    "clip": str(clip_path),
                    "clips": [str(clip_path)],
                    "action_classifications": [],
                },
            }
        ],
    )

    enhancer = MarkdownEnhancer()
    enhancer._enabled = True
    enhancer._llm_client = _FakeLLMClient("step details\n【imgneeded_SU200_img_01】\ndone")

    async def _fake_hierarchy(sections, subject):
        return {"SU200": {"level": 2, "parent_id": None}}

    monkeypatch.setattr(enhancer, "_classify_hierarchy", _fake_hierarchy)

    markdown = asyncio.run(
        enhancer.enhance(
            str(result_path),
            subject="test",
            markdown_dir=str(tmp_path),
        )
    )

    assert "【imgneeded_SU200_img_01】" not in markdown
    assert "![[assets/SU200_img_01.png]]" in markdown
    assert "> Video **" in markdown
    assert "![[assets/SU200_clip_01.mp4]]" in markdown
    assert "> Images **Keyframes**" not in markdown


def test_process_non_tutorial_renders_multiple_videos(tmp_path, monkeypatch):
    assets_dir = tmp_path / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)
    img_path = assets_dir / "SU201_img_01.png"
    clip_path_1 = assets_dir / "SU201_clip_01.mp4"
    clip_path_2 = assets_dir / "SU201_clip_02.mp4"
    img_path.write_bytes(b"img")
    clip_path_1.write_bytes(b"clip1")
    clip_path_2.write_bytes(b"clip2")

    result_path = tmp_path / "result.json"
    _write_result_json(
        result_path,
        [
            {
                "unit_id": "SU201",
                "title": "Process Multi Video Unit",
                "knowledge_type": "process",
                "body_text": "process concept body",
                "mult_steps": False,
                "instructional_steps": [],
                "materials": {
                    "screenshots": [str(img_path)],
                    "screenshot_items": [
                        {
                            "img_id": "SU201_img_01",
                            "img_path": str(img_path),
                            "img_description": "open menu",
                        }
                    ],
                    "clip": str(clip_path_1),
                    "clips": [str(clip_path_1), str(clip_path_2)],
                    "action_classifications": [],
                },
            }
        ],
    )

    enhancer = MarkdownEnhancer()
    enhancer._enabled = True
    enhancer._llm_client = _FakeLLMClient("step details\n【imgneeded_SU201_img_01】\ndone")

    async def _fake_hierarchy(sections, subject):
        return {"SU201": {"level": 2, "parent_id": None}}

    monkeypatch.setattr(enhancer, "_classify_hierarchy", _fake_hierarchy)

    markdown = asyncio.run(
        enhancer.enhance(
            str(result_path),
            subject="test",
            markdown_dir=str(tmp_path),
        )
    )

    assert "![[assets/SU201_clip_01.mp4]]" in markdown
    assert "![[assets/SU201_clip_02.mp4]]" in markdown


def test_concrete_section_uses_image_desc_augment_before_structuring_when_enabled(tmp_path, monkeypatch):
    assets_dir = tmp_path / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)
    img_path = assets_dir / "SU300_img_01.png"
    img_path.write_bytes(b"img")

    result_path = tmp_path / "result.json"
    _write_result_json(
        result_path,
        [
            {
                "unit_id": "SU300",
                "title": "Concrete Unit",
                "knowledge_type": "concrete",
                "body_text": "open the settings panel",
                "mult_steps": False,
                "instructional_steps": [],
                "materials": {
                    "screenshots": [str(img_path)],
                    "screenshot_items": [
                        {
                            "img_id": "SU300_img_01",
                            "img_path": str(img_path),
                            "img_description": "terminal shows command npm run dev",
                            "timestamp_sec": 5.2,
                            "sentence_id": "S002",
                            "sentence_text": "the command appears in terminal",
                        }
                    ],
                    "clip": "",
                    "action_classifications": [],
                },
            }
        ],
    )

    enhancer = MarkdownEnhancer()
    enhancer._enabled = True
    enhancer._enable_img_desc_text_augment = True
    recorder = _RecorderAugmentLLMClient()
    enhancer._llm_client = recorder

    async def _fake_hierarchy(sections, subject):
        return {"SU300": {"level": 2, "parent_id": None}}

    monkeypatch.setattr(enhancer, "_classify_hierarchy", _fake_hierarchy)

    markdown = asyncio.run(
        enhancer.enhance(
            str(result_path),
            subject="test",
            markdown_dir=str(tmp_path),
        )
    )

    assert AUGMENTED_BODY_TEXT in markdown
    assert sum(1 for call in recorder.calls if AUGMENT_PROMPT_MARKER in call["prompt"]) == 1
    assert sum(1 for call in recorder.calls if STRUCTURED_PROMPT_MARKER in call["prompt"]) == 1


def test_concrete_section_skips_image_desc_augment_when_disabled(tmp_path, monkeypatch):
    assets_dir = tmp_path / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)
    img_path = assets_dir / "SU301_img_01.png"
    img_path.write_bytes(b"img")

    result_path = tmp_path / "result.json"
    _write_result_json(
        result_path,
        [
            {
                "unit_id": "SU301",
                "title": "Concrete Unit",
                "knowledge_type": "concrete",
                "body_text": "open the settings panel",
                "mult_steps": False,
                "instructional_steps": [],
                "materials": {
                    "screenshots": [str(img_path)],
                    "screenshot_items": [
                        {
                            "img_id": "SU301_img_01",
                            "img_path": str(img_path),
                            "img_description": "terminal shows command npm run dev",
                            "timestamp_sec": 5.2,
                            "sentence_id": "S002",
                            "sentence_text": "the command appears in terminal",
                        }
                    ],
                    "clip": "",
                    "action_classifications": [],
                },
            }
        ],
    )

    enhancer = MarkdownEnhancer()
    enhancer._enabled = True
    enhancer._enable_img_desc_text_augment = False
    recorder = _RecorderAugmentLLMClient()
    enhancer._llm_client = recorder

    async def _fake_hierarchy(sections, subject):
        return {"SU301": {"level": 2, "parent_id": None}}

    monkeypatch.setattr(enhancer, "_classify_hierarchy", _fake_hierarchy)

    markdown = asyncio.run(
        enhancer.enhance(
            str(result_path),
            subject="test",
            markdown_dir=str(tmp_path),
        )
    )

    assert BASE_BODY_TEXT in markdown
    assert sum(1 for call in recorder.calls if AUGMENT_PROMPT_MARKER in call["prompt"]) == 0
    assert sum(1 for call in recorder.calls if STRUCTURED_PROMPT_MARKER in call["prompt"]) == 1


def test_markdown_enhancer_img_desc_switch_defaults_true_from_config(tmp_path, monkeypatch):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "content_pipeline:\n  markdown_enhancer:\n    enable_img_desc_text_augment: true\n",
        encoding="utf-8",
    )

    monkeypatch.setenv("MODULE2_CONFIG_PATH", str(config_path))
    monkeypatch.delenv("MODULE2_ENABLE_IMG_DESC_TEXT_AUGMENT", raising=False)

    enhancer = MarkdownEnhancer()
    assert enhancer._enable_img_desc_text_augment is True


def test_markdown_enhancer_img_desc_switch_env_overrides_config(tmp_path, monkeypatch):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "content_pipeline:\n  markdown_enhancer:\n    enable_img_desc_text_augment: true\n",
        encoding="utf-8",
    )

    monkeypatch.setenv("MODULE2_CONFIG_PATH", str(config_path))
    monkeypatch.setenv("MODULE2_ENABLE_IMG_DESC_TEXT_AUGMENT", "0")

    enhancer = MarkdownEnhancer()
    assert enhancer._enable_img_desc_text_augment is False


def test_concrete_section_skips_img_desc_augment_without_alignment_evidence(tmp_path, monkeypatch):
    assets_dir = tmp_path / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)
    img_path = assets_dir / "SU302_img_01.png"
    img_path.write_bytes(b"img")

    result_path = tmp_path / "result.json"
    _write_result_json(
        result_path,
        [
            {
                "unit_id": "SU302",
                "title": "Concrete Unit",
                "knowledge_type": "concrete",
                "body_text": "open the settings panel",
                "mult_steps": False,
                "instructional_steps": [],
                "materials": {
                    "screenshots": [str(img_path)],
                    "screenshot_items": [
                        {
                            "img_id": "SU302_img_01",
                            "img_path": str(img_path),
                            "img_description": "terminal shows command npm run dev",
                        }
                    ],
                    "clip": "",
                    "action_classifications": [],
                },
            }
        ],
    )

    enhancer = MarkdownEnhancer()
    enhancer._enabled = True
    enhancer._enable_img_desc_text_augment = True
    recorder = _RecorderAugmentLLMClient()
    enhancer._llm_client = recorder

    async def _fake_hierarchy(sections, subject):
        return {"SU302": {"level": 2, "parent_id": None}}

    monkeypatch.setattr(enhancer, "_classify_hierarchy", _fake_hierarchy)

    markdown = asyncio.run(
        enhancer.enhance(
            str(result_path),
            subject="test",
            markdown_dir=str(tmp_path),
        )
    )

    assert BASE_BODY_TEXT in markdown
    assert sum(1 for call in recorder.calls if AUGMENT_PROMPT_MARKER in call["prompt"]) == 0
    assert sum(1 for call in recorder.calls if STRUCTURED_PROMPT_MARKER in call["prompt"]) == 1


def test_concrete_section_logs_augment_triggered_with_sentence_ids(tmp_path, monkeypatch, caplog):
    assets_dir = tmp_path / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)
    img_path = assets_dir / "SU303_img_01.png"
    img_path.write_bytes(b"img")

    result_path = tmp_path / "result.json"
    _write_result_json(
        result_path,
        [
            {
                "unit_id": "SU303",
                "title": "Concrete Unit",
                "knowledge_type": "concrete",
                "body_text": "open the settings panel",
                "mult_steps": False,
                "instructional_steps": [],
                "materials": {
                    "screenshots": [str(img_path)],
                    "screenshot_items": [
                        {
                            "img_id": "SU303_img_01",
                            "img_path": str(img_path),
                            "img_description": "terminal shows command npm run dev",
                            "timestamp_sec": 5.2,
                            "sentence_id": "S002",
                            "sentence_text": "the command appears in terminal",
                        }
                    ],
                    "clip": "",
                    "action_classifications": [],
                },
            }
        ],
    )

    enhancer = MarkdownEnhancer()
    enhancer._enabled = True
    enhancer._enable_img_desc_text_augment = True
    recorder = _RecorderAugmentLLMClient()
    enhancer._llm_client = recorder

    async def _fake_hierarchy(sections, subject):
        return {"SU303": {"level": 2, "parent_id": None}}

    monkeypatch.setattr(enhancer, "_classify_hierarchy", _fake_hierarchy)

    caplog.set_level("INFO")
    _ = asyncio.run(
        enhancer.enhance(
            str(result_path),
            subject="test",
            markdown_dir=str(tmp_path),
        )
    )

    logs = "\n".join(record.message for record in caplog.records)
    assert "[SU303] img-desc augment triggered" in logs
    assert "sentence_ids=S002" in logs


def test_img_desc_augment_uses_excluded_screenshot_items_as_evidence():
    class _CaptureLLM:
        def __init__(self):
            self.calls = []

        async def complete_text(self, prompt: str, system_message: str = None):
            self.calls.append({"prompt": prompt, "system_message": system_message or ""})
            return "unchanged", None, None

    enhancer = MarkdownEnhancer()
    enhancer._enabled = True
    enhancer._enable_img_desc_text_augment = True
    cap = _CaptureLLM()
    enhancer._llm_client = cap

    section = EnhancedSection(
        unit_id="SU399",
        title="Concrete Unit",
        knowledge_type="concrete",
        original_body="鍘熷姝ｆ枃",
        screenshot_items=[
            {
                "img_id": "SU399_img_01",
                "img_path": "assets/SU399_img_01.png",
                "img_description": "include desc",
                "should_include": True,
                "timestamp_sec": 1.0,
            }
        ],
        augment_screenshot_items=[
            {
                "img_id": "SU399_img_99",
                "img_path": "assets/SU399_img_99.png",
                "img_description": "exclude desc for augment",
                "should_include": False,
                "timestamp_sec": 2.5,
            }
        ],
    )

    augment_items = enhancer._build_augment_image_items(section)
    _ = asyncio.run(
        enhancer._augment_body_with_image_descriptions(
            section,
            section.original_body,
            augment_items,
        )
    )

    assert cap.calls
    assert any("exclude desc for augment" in call["prompt"] for call in cap.calls)


def test_img_desc_augment_supports_replace_and_add_patch_modes():
    class _PatchLLM:
        async def complete_text(self, prompt: str, system_message: str = None):
            _ = prompt
            _ = system_message
            return (
                '{"p":['
                '{"m":"r","o":"执行命令","n":"执行 `npm run dev` 命令","l":"先打开终端","r":"。然后查看日志。"},'
                '{"m":"a","n":" 并确认端口为 `3000`","l":"查看日志","r":"。","p":"after"}'
                ']}',
                None,
                None,
            )

    enhancer = MarkdownEnhancer()
    enhancer._enabled = True
    enhancer._enable_img_desc_text_augment = True
    enhancer._llm_client = _PatchLLM()

    section = EnhancedSection(
        unit_id="SU401",
        title="Concrete Unit",
        knowledge_type="concrete",
        original_body="先打开终端执行命令。然后查看日志。",
    )
    image_items = [
        {
            "img_id": "SU401_img_01",
            "img_description": "终端显示 npm run dev 与端口 3000",
            "timestamp_sec": 1.5,
            "sentence_id": "S001",
            "sentence_text": "执行命令并查看日志",
        }
    ]

    result = asyncio.run(
        enhancer._augment_body_with_image_descriptions(
            section,
            section.original_body,
            image_items,
        )
    )

    assert result == "先打开终端执行 `npm run dev` 命令。然后查看日志 并确认端口为 `3000`。"


def test_img_desc_augment_ambiguous_patch_keeps_base_text():
    class _AmbiguousPatchLLM:
        async def complete_text(self, prompt: str, system_message: str = None):
            _ = prompt
            _ = system_message
            return '{"p":[{"m":"r","o":"执行命令","n":"执行 `npm run dev` 命令"}]}', None, None

    enhancer = MarkdownEnhancer()
    enhancer._enabled = True
    enhancer._enable_img_desc_text_augment = True
    enhancer._llm_client = _AmbiguousPatchLLM()

    base_text = "先执行命令，再执行命令。"
    section = EnhancedSection(
        unit_id="SU402",
        title="Concrete Unit",
        knowledge_type="concrete",
        original_body=base_text,
    )
    image_items = [
        {
            "img_id": "SU402_img_01",
            "img_description": "终端显示命令",
            "timestamp_sec": 2.1,
            "sentence_id": "S001",
            "sentence_text": "执行命令",
        }
    ]

    result = asyncio.run(
        enhancer._augment_body_with_image_descriptions(
            section,
            base_text,
            image_items,
        )
    )

    assert result == base_text


def test_markdown_enhancer_writes_llm_trace_jsonl(tmp_path, monkeypatch):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "content_pipeline:",
                "  markdown_enhancer:",
                "    enable_img_desc_text_augment: true",
                "  observability:",
                "    llm_trace:",
                "      enabled: true",
                "      level: full",
                "      output_path: intermediates/phase2b_llm_trace.jsonl",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("MODULE2_CONFIG_PATH", str(config_path))
    monkeypatch.delenv("MODULE2_LLM_TRACE_ENABLED", raising=False)

    assets_dir = tmp_path / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)
    img_path = assets_dir / "SU400_img_01.png"
    img_path.write_bytes(b"img")

    result_path = tmp_path / "result.json"
    _write_result_json(
        result_path,
        [
            {
                "unit_id": "SU400",
                "title": "Concrete Unit",
                "knowledge_type": "concrete",
                "body_text": "open config",
                "mult_steps": False,
                "instructional_steps": [],
                "materials": {
                    "screenshots": [str(img_path)],
                    "screenshot_items": [
                        {
                            "img_id": "SU400_img_01",
                            "img_path": str(img_path),
                            "img_description": "閸涙垝鎶ゆ穱鈩冧紖",
                            "timestamp_sec": 5.0,
                            "sentence_id": "S001",
                            "sentence_text": "the command appears in terminal",
                        }
                    ],
                    "clip": "",
                    "action_classifications": [],
                },
            }
        ],
    )

    enhancer = MarkdownEnhancer()
    enhancer._enabled = True
    enhancer._llm_client = _TraceLLMClient()

    markdown = asyncio.run(
        enhancer.enhance(
            str(result_path),
            subject="test",
            markdown_dir=str(tmp_path),
        )
    )
    assert "structured trace body" in markdown

    trace_path = tmp_path / "intermediates" / "phase2b_llm_trace.jsonl"
    assert trace_path.exists()
    lines = [line for line in trace_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(lines) >= 3
    payloads = [json.loads(line) for line in lines]
    steps = {item.get("step_name") for item in payloads}
    assert "hierarchy_classification" in steps
    assert "img_desc_augment" in steps
    assert "structured_text" in steps


