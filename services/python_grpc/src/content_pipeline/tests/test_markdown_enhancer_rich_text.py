import asyncio
import json
from pathlib import Path

from services.python_grpc.src.content_pipeline.markdown_enhancer import MarkdownEnhancer


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
        if "鏁欏鏂囨湰琛ュ叏鍔╂墜" in sys_msg:
            return "琛ュ叏鏂囨湰", _Meta(), None
        if "鏁欏鍐呭缁撴瀯鍖栧姪鎵? in sys_msg:
            return "缁撴瀯鍖栨鏂?, _Meta(), None
        # hierarchy
        return '{"hierarchy":[{"unit_id":"SU400","level":2,"parent_id":null}]}', _Meta(), None


class _RecorderAugmentLLMClient:
    def __init__(self):
        self.calls = []

    async def complete_text(self, prompt: str, system_message: str = None):
        system = system_message or ""
        self.calls.append({"prompt": prompt, "system_message": system})

        if "鏁欏鏂囨湰琛ュ叏鍔╂墜" in system:
            return "鍏堟墦寮€缁堢锛岀劧鍚庢墽琛?npm run dev銆?, None, None

        if "鏁欏鍐呭缁撴瀯鍖栧姪鎵? in system:
            if "鍏堟墦寮€缁堢锛岀劧鍚庢墽琛?npm run dev銆? in prompt:
                return "缁撴瀯鍖栧寘鍚懡浠?npm run dev", None, None
            return "缁撴瀯鍖栨湭鍖呭惈鍛戒护", None, None

        return "", None, None


def _write_result_json(path: Path, sections):
    payload = {
        "title": "Demo Document",
        "sections": sections,
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
    enhancer._llm_client = _FakeLLMClient("- key step\n銆恑mgneeded_SU001_img_01銆慭n- end")

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

    assert "銆恑mgneeded_SU001_img_01銆? not in markdown
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
    enhancer._llm_client = _FakeLLMClient("- key step\n閵嗘亼mgneeded_SU888_img_01閵嗘叚n- end")

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
                "clip_start_sec": 0.0,
                "clip_end_sec": 6.0,
                "instructional_keyframe_timestamp": [5.4],
            },
            {
                "step_id": 2,
                "step_description": "change port",
                "clip_start_sec": 6.0,
                "clip_end_sec": 13.0,
                "instructional_keyframe_timestamp": [12.2],
            },
        ],
        "steps": [
            {
                "step_id": 1,
                "step_description": "open settings",
                "clip_start_sec": 0.0,
                "clip_end_sec": 6.0,
                "clip_file": clip1.name,
                "instructional_keyframes": [key1.name],
            },
            {
                "step_id": 2,
                "step_description": "change port",
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
    assert "![[vl_tutorial_units/SU002/SU002_ss_step_01_key_01_open_settings.png]]" in markdown
    assert "![[vl_tutorial_units/SU002/SU002_clip_step_02_change_port.mp4]]" in markdown
    assert "> ?? **" not in markdown


def test_hierarchy_level_mapping_keeps_deepseek_level_result(tmp_path, monkeypatch):
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
    assert "#### Leaf Detail" in markdown


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
        "line1 銆恑mgneeded_SU100_img_01銆慭nline2 銆恑mgneeded_SU100_img_02銆慭nline3"
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

    assert "銆恑mgneeded_SU100_img_01銆? not in markdown
    assert "銆恑mgneeded_SU100_img_02銆? not in markdown
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
    enhancer._llm_client = _FakeLLMClient("step details\n銆恑mgneeded_SU200_img_01銆慭ndone")

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

    assert "銆恑mgneeded_SU200_img_01銆? not in markdown
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
    enhancer._llm_client = _FakeLLMClient("step details\n閵嗘亼mgneeded_SU201_img_01閵嗘叚ndone")

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
                "body_text": "鍏堟墦寮€缁堢銆?,
                "mult_steps": False,
                "instructional_steps": [],
                "materials": {
                    "screenshots": [str(img_path)],
                    "screenshot_items": [
                        {
                            "img_id": "SU300_img_01",
                            "img_path": str(img_path),
                            "img_description": "缁堢閲屾樉绀哄懡浠?npm run dev",
                            "timestamp_sec": 5.2,
                            "sentence_id": "S002",
                            "sentence_text": "鎵ц鍚姩鍛戒护",
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

    assert "缁撴瀯鍖栧寘鍚懡浠?npm run dev" in markdown
    assert sum(1 for call in recorder.calls if "鏁欏鏂囨湰琛ュ叏鍔╂墜" in call["system_message"]) == 1
    assert sum(1 for call in recorder.calls if "鏁欏鍐呭缁撴瀯鍖栧姪鎵? in call["system_message"]) == 1


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
                "body_text": "鍏堟墦寮€缁堢銆?,
                "mult_steps": False,
                "instructional_steps": [],
                "materials": {
                    "screenshots": [str(img_path)],
                    "screenshot_items": [
                        {
                            "img_id": "SU301_img_01",
                            "img_path": str(img_path),
                            "img_description": "缁堢閲屾樉绀哄懡浠?npm run dev",
                            "timestamp_sec": 5.2,
                            "sentence_id": "S002",
                            "sentence_text": "鎵ц鍚姩鍛戒护",
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

    assert "缁撴瀯鍖栨湭鍖呭惈鍛戒护" in markdown
    assert sum(1 for call in recorder.calls if "鏁欏鏂囨湰琛ュ叏鍔╂墜" in call["system_message"]) == 0
    assert sum(1 for call in recorder.calls if "鏁欏鍐呭缁撴瀯鍖栧姪鎵? in call["system_message"]) == 1


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
                "body_text": "鍏堟墦寮€缁堢銆?,
                "mult_steps": False,
                "instructional_steps": [],
                "materials": {
                    "screenshots": [str(img_path)],
                    "screenshot_items": [
                        {
                            "img_id": "SU302_img_01",
                            "img_path": str(img_path),
                            "img_description": "缁堢閲屾樉绀哄懡浠?npm run dev",
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

    assert "缁撴瀯鍖栨湭鍖呭惈鍛戒护" in markdown
    assert sum(1 for call in recorder.calls if "鏁欏鏂囨湰琛ュ叏鍔╂墜" in call["system_message"]) == 0
    assert sum(1 for call in recorder.calls if "鏁欏鍐呭缁撴瀯鍖栧姪鎵? in call["system_message"]) == 1


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
                "body_text": "鍏堟墦寮€缁堢銆?,
                "mult_steps": False,
                "instructional_steps": [],
                "materials": {
                    "screenshots": [str(img_path)],
                    "screenshot_items": [
                        {
                            "img_id": "SU303_img_01",
                            "img_path": str(img_path),
                            "img_description": "缁堢閲屾樉绀哄懡浠?npm run dev",
                            "timestamp_sec": 5.2,
                            "sentence_id": "S002",
                            "sentence_text": "鎵ц鍚姩鍛戒护",
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
                "body_text": "鍘熸枃",
                "mult_steps": False,
                "instructional_steps": [],
                "materials": {
                    "screenshots": [str(img_path)],
                    "screenshot_items": [
                        {
                            "img_id": "SU400_img_01",
                            "img_path": str(img_path),
                            "img_description": "鍛戒护淇℃伅",
                            "timestamp_sec": 5.0,
                            "sentence_id": "S001",
                            "sentence_text": "鎵ц鍛戒护",
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
    assert "缁撴瀯鍖栨鏂? in markdown

    trace_path = tmp_path / "intermediates" / "phase2b_llm_trace.jsonl"
    assert trace_path.exists()
    lines = [line for line in trace_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(lines) >= 3
    payloads = [json.loads(line) for line in lines]
    steps = {item.get("step_name") for item in payloads}
    assert "hierarchy_classification" in steps
    assert "img_desc_augment" in steps
    assert "structured_text" in steps


