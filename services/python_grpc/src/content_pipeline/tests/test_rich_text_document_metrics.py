from services.python_grpc.src.content_pipeline.phase2b.assembly.rich_text_document import (
    KnowledgeGroup,
    MaterialSet,
    RichTextDocument,
    RichTextSection,
)


def _build_section(unit_id: str, title: str) -> RichTextSection:
    return RichTextSection(
        unit_id=unit_id,
        title=title,
        body_text=f"{title} body",
        knowledge_type="process",
        start_sec=0.0,
        end_sec=1.0,
    )


def test_rich_text_document_sections_keeps_backward_compatible_flatten_view():
    doc = RichTextDocument(
        title="demo",
        knowledge_groups=[
            KnowledgeGroup(
                group_id=1,
                group_name="G1",
                units=[_build_section("U1", "T1"), _build_section("U2", "T2")],
            ),
            KnowledgeGroup(
                group_id=2,
                group_name="G2",
                units=[_build_section("U3", "T3")],
            ),
        ],
    )

    flattened_ids = [section.unit_id for section in doc.sections]
    assert flattened_ids == ["U1", "U2", "U3"]


def test_rich_text_document_total_sections_uses_group_units_count():
    doc = RichTextDocument(
        title="demo",
        knowledge_groups=[
            KnowledgeGroup(
                group_id=1,
                group_name="G1",
                units=[_build_section("U1", "T1")],
            ),
            KnowledgeGroup(
                group_id=2,
                group_name="G2",
                units=[_build_section("U2", "T2"), _build_section("U3", "T3")],
            ),
        ],
    )

    assert doc.total_sections() == 3


def test_rich_text_document_top_level_screenshot_embed_uses_frame_reason_alias(tmp_path):
    image_path = tmp_path / "assets" / "SU900" / "SU900_key.png"
    image_path.parent.mkdir(parents=True, exist_ok=True)
    image_path.write_bytes(b"img")

    section = RichTextSection(
        unit_id="SU900",
        title="Alias Section",
        body_text="body",
        knowledge_type="concrete",
        start_sec=0.0,
        end_sec=3.0,
        materials=MaterialSet(
            screenshot_paths=[str(image_path)],
            screenshot_items=[
                {
                    "img_path": str(image_path),
                    "frame_reason": "关键画面说明",
                }
            ],
        ),
    )
    doc = RichTextDocument(
        title="demo",
        knowledge_groups=[
            KnowledgeGroup(
                group_id=1,
                group_name="G1",
                units=[section],
            )
        ],
    )

    output_path = tmp_path / "result.md"
    doc.to_markdown(str(output_path), assets_relative_dir="assets")
    markdown = output_path.read_text(encoding="utf-8")

    assert "![[assets/SU900/SU900_key.png|关键画面说明]]" in markdown


def test_rich_text_document_step_screenshot_embed_uses_instructional_keyframe_frame_reason(tmp_path):
    image_path = tmp_path / "assets" / "SU901" / "SU901_step_01.png"
    image_path.parent.mkdir(parents=True, exist_ok=True)
    image_path.write_bytes(b"img")

    section = RichTextSection(
        unit_id="SU901",
        title="Step Alias Section",
        body_text="body",
        knowledge_type="process",
        start_sec=0.0,
        end_sec=5.0,
        materials=MaterialSet(),
        instructional_steps=[
            {
                "step_id": 1,
                "step_type": "MAIN_FLOW",
                "step_description": "打开设置",
                "main_action": "点击按钮",
                "materials": {
                    "screenshot_paths": [str(image_path)],
                },
                "instructional_keyframe_details": [
                    {
                        "image_path": str(image_path),
                        "frame_reason": "展示设置面板已展开",
                    }
                ],
            }
        ],
    )
    doc = RichTextDocument(
        title="demo",
        knowledge_groups=[
            KnowledgeGroup(
                group_id=1,
                group_name="G1",
                units=[section],
            )
        ],
    )

    output_path = tmp_path / "step.md"
    doc.to_markdown(str(output_path), assets_relative_dir="assets")
    markdown = output_path.read_text(encoding="utf-8")

    assert "![[assets/SU901/SU901_step_01.png|展示设置面板已展开]]" in markdown
