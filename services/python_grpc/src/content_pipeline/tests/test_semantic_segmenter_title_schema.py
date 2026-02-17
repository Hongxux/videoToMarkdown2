from services.python_grpc.src.content_pipeline.phase2a.segmentation.semantic_unit_segmenter import (
    SemanticUnit,
    SemanticUnitSegmenter,
)


def _build_paragraphs():
    return [
        {
            "paragraph_id": "P001",
            "text": "打开设置并进入配置页面。",
            "source_sentence_ids": ["S001"],
            "start_sec": 0.0,
            "end_sec": 5.0,
        },
        {
            "paragraph_id": "P002",
            "text": "填写 API Key 并保存。",
            "source_sentence_ids": ["S002"],
            "start_sec": 5.0,
            "end_sec": 10.0,
        },
    ]


def test_parse_min_schema_requires_title_and_group_name():
    segmenter = SemanticUnitSegmenter(llm_client=object())
    paragraphs = _build_paragraphs()

    parsed = segmenter._parse_min_schema_unit(
        {
            "pids": ["P001", "P002"],
            "k": 2,
            "m": 1,
            "title": "配置模型参数",
            "group_name": "模型初始化配置",
        },
        paragraphs,
    )

    assert parsed is not None
    assert parsed["title"] == "配置模型参数"
    assert parsed["group_name"] == "模型初始化配置"


def test_parse_min_schema_rejects_missing_group_name():
    segmenter = SemanticUnitSegmenter(llm_client=object())
    paragraphs = _build_paragraphs()

    parsed = segmenter._parse_min_schema_unit(
        {
            "pids": ["P001", "P002"],
            "k": 2,
            "m": 1,
            "title": "配置模型参数",
        },
        paragraphs,
    )

    assert parsed is None


def test_assign_group_ids_reuses_existing_group_name_even_if_non_contiguous():
    segmenter = SemanticUnitSegmenter(llm_client=object())
    units = [
        SemanticUnit(
            unit_id="SU001",
            knowledge_type="abstract",
            knowledge_topic="概念讲解",
            full_text="a",
            source_paragraph_ids=["P001"],
            source_sentence_ids=["S001"],
            group_name="主题A",
        ),
        SemanticUnit(
            unit_id="SU002",
            knowledge_type="process",
            knowledge_topic="流程演示",
            full_text="b",
            source_paragraph_ids=["P002"],
            source_sentence_ids=["S002"],
            group_name="主题B",
        ),
        SemanticUnit(
            unit_id="SU003",
            knowledge_type="concrete",
            knowledge_topic="案例说明",
            full_text="c",
            source_paragraph_ids=["P003"],
            source_sentence_ids=["S003"],
            group_name="主题A",
        ),
    ]

    segmenter._assign_group_ids(units, start_id=1)

    assert units[0].group_id == 1
    assert units[1].group_id == 2
    assert units[2].group_id == 1


def test_parse_group_schema_requires_group_level_reason_and_units():
    segmenter = SemanticUnitSegmenter(llm_client=object())
    paragraphs = _build_paragraphs()

    parsed_group = segmenter._parse_group_schema(
        {
            "group_name": "模型初始化配置",
            "reason": "围绕同一个核心目标：完成模型初始化",
            "units": [
                {"pids": ["P001"], "k": 0, "m": 0, "title": "初始化背景说明"},
                {"pids": ["P002"], "k": 2, "m": 1, "title": "执行初始化步骤"},
            ],
        },
        paragraphs,
    )

    assert parsed_group is not None
    assert parsed_group["group_name"] == "模型初始化配置"
    assert parsed_group["reason"] == "围绕同一个核心目标：完成模型初始化"
    assert len(parsed_group["units"]) == 2


def test_parse_group_schema_rejects_unit_level_group_name_field():
    segmenter = SemanticUnitSegmenter(llm_client=object())
    paragraphs = _build_paragraphs()

    parsed_group = segmenter._parse_group_schema(
        {
            "group_name": "模型初始化配置",
            "reason": "围绕同一个核心目标：完成模型初始化",
            "units": [
                {
                    "pids": ["P001"],
                    "k": 0,
                    "m": 0,
                    "title": "初始化背景说明",
                    "group_name": "不允许出现在unit内",
                }
            ],
        },
        paragraphs,
    )

    assert parsed_group is None


def test_can_merge_boundary_units_rejects_cross_knowledge_type():
    segmenter = SemanticUnitSegmenter(llm_client=object())
    prev_tail = SemanticUnit(
        unit_id="SU001",
        knowledge_type="abstract",
        knowledge_topic="概念解释",
        full_text="explain",
        source_paragraph_ids=["P001"],
        source_sentence_ids=["S001"],
        group_name="查找算法核心思想",
    )
    next_head = SemanticUnit(
        unit_id="SU002",
        knowledge_type="process",
        knowledge_topic="流程演示",
        full_text="demo",
        source_paragraph_ids=["P002"],
        source_sentence_ids=["S002"],
        group_name="查找算法核心思想",
    )

    assert segmenter._can_merge_boundary_units(prev_tail, next_head) is False
