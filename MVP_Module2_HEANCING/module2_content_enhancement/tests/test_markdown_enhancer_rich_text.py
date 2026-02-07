import asyncio
import json
from pathlib import Path

from MVP_Module2_HEANCING.module2_content_enhancement.markdown_enhancer import MarkdownEnhancer


class _FakeLLMClient:
    def __init__(self, structured_text: str):
        self._structured_text = structured_text

    async def complete_text(self, prompt: str, system_message: str = None):
        return self._structured_text, None, None


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


def test_process_multistep_renders_ordered_steps_with_assets(tmp_path):
    unit_dir = tmp_path / "vl_tutorial_units" / "SU002"
    unit_dir.mkdir(parents=True, exist_ok=True)

    clip1 = unit_dir / "SU002_step_01_open_settings.mp4"
    key1 = unit_dir / "SU002_step_01_open_settings_key.png"
    clip2 = unit_dir / "SU002_step_02_change_port.mp4"
    key2 = unit_dir / "SU002_step_02_change_port_key.png"
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
    assert "![[vl_tutorial_units/SU002/SU002_step_01_open_settings_key.png]]" in markdown
    assert "![[vl_tutorial_units/SU002/SU002_step_02_change_port.mp4]]" in markdown
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

