from services.python_grpc.src.content_pipeline.phase2b.assembly.rich_text_document import (
    KnowledgeGroup,
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
