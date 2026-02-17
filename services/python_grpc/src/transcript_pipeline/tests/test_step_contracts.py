import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

from services.python_grpc.src.transcript_pipeline.nodes.step_contracts import (
    assemble_step3_merged_sentences,
    assemble_step4_cleaned_sentences,
    build_step3_window_candidates,
    merge_step4_cleaned_maps,
    order_records_by_reference_ids,
    parse_step1_topic_payload,
    parse_step3_merged_sentences,
    parse_step4_cleaned_sentences,
    parse_step56_dedup_merge_payload,
)
from services.python_grpc.src.transcript_pipeline.state import CleanedSentence


def test_parse_step1_topic_payload_supports_compact_keys():
    domain, main_topic, metrics = parse_step1_topic_payload({"d": "computer science", "mt": "sorting"})
    assert domain == "computer science"
    assert main_topic == "sorting"
    assert metrics["compact_key_hits"] >= 2


def test_order_records_by_reference_ids_is_not_coupled_to_sub_prefix():
    records = [
        {"subtitle_id": "A-2", "value": 2},
        {"subtitle_id": "A-1", "value": 1},
        {"subtitle_id": "A-3", "value": 3},
    ]
    ordered = order_records_by_reference_ids(
        records,
        ["A-1", "A-2", "A-3"],
        id_key="subtitle_id",
    )
    assert [item["subtitle_id"] for item in ordered] == ["A-1", "A-2", "A-3"]


def test_parse_step3_merged_sentences_supports_merged_groups_shape():
    merged_items, metrics = parse_step3_merged_sentences(
        {
            "merged_groups": [
                {"text": "merged sentence", "source_subtitle_ids": ["SUB001", "SUB002"]},
            ]
        },
        valid_subtitle_ids={"SUB001", "SUB002", "SUB003"},
    )
    assert len(merged_items) == 1
    assert merged_items[0]["text"] == "merged sentence"
    assert merged_items[0]["source_subtitle_ids"] == ["SUB001", "SUB002"]
    assert metrics["merged_groups_shape_hits"] == 1


def test_step3_contract_helpers_build_and_assemble_candidates():
    ordered_subtitle_ids = ["SUB001", "SUB002", "SUB003", "SUB004"]
    subtitle_index_by_id = {sid: idx for idx, sid in enumerate(ordered_subtitle_ids)}
    subtitle_by_id = {
        "SUB001": {"subtitle_id": "SUB001", "corrected_text": "s1", "start_sec": 1.0, "end_sec": 1.5},
        "SUB002": {"subtitle_id": "SUB002", "corrected_text": "s2", "start_sec": 2.0, "end_sec": 2.5},
        "SUB003": {"subtitle_id": "SUB003", "corrected_text": "s3", "start_sec": 3.0, "end_sec": 3.5},
        "SUB004": {"subtitle_id": "SUB004", "corrected_text": "s4", "start_sec": 4.0, "end_sec": 4.5},
    }
    parsed_items = [
        {"text": "merged 2-3", "source_subtitle_ids": ["SUB002", "SUB003"]},
        {"text": "single 4", "source_subtitle_ids": ["SUB004"]},
    ]
    window_candidates, build_metrics = build_step3_window_candidates(
        parsed_items,
        subtitle_index_by_id=subtitle_index_by_id,
        ordered_subtitle_ids=ordered_subtitle_ids,
        subtitle_by_id=subtitle_by_id,
    )
    assert len(window_candidates) == 1
    assert window_candidates[0]["source_subtitle_ids"] == ["SUB002", "SUB003"]
    assert build_metrics["dropped_non_merge_group"] == 1

    window_candidates[0]["window_index"] = 0
    merged_sentences, merged_subtitle_ids, assemble_metrics = assemble_step3_merged_sentences(
        window_candidates,
        ordered_subtitle_ids=ordered_subtitle_ids,
        subtitle_by_id=subtitle_by_id,
    )
    assert [item["source_subtitle_ids"] for item in merged_sentences] == [
        ["SUB001"],
        ["SUB002", "SUB003"],
        ["SUB004"],
    ]
    assert merged_subtitle_ids == {"SUB002", "SUB003"}
    assert assemble_metrics["passthrough_sentences"] == 2
    assert assemble_metrics["selected_merge_groups"] == 1


def test_parse_step4_cleaned_sentences_supports_removal_patch_and_legacy_shape():
    cleaned_by_id, metrics = parse_step4_cleaned_sentences(
        {
            "d": [{"sid": "S001", "o": "嗯", "l": "第一句", "r": "我想说"}],
            "c": [{"sid": "S001", "ri": ["um"]}],
        },
        valid_sentence_ids={"S001"},
    )
    assert cleaned_by_id["S001"]["cleaned_text"] == ""
    assert len(cleaned_by_id["S001"]["removals"]) == 1
    assert cleaned_by_id["S001"]["removals"][0]["original"] == "嗯"
    assert "removed_items" not in cleaned_by_id["S001"]
    assert metrics["compact_shape_hits"] == 1
    assert metrics["compact_removal_shape_hits"] == 1
    assert metrics["legacy_removed_items_ignored"] == 1


def test_step4_contract_helpers_merge_and_assemble_with_passthrough():
    source_sentences = [
        {"sentence_id": "S001", "text": "第一句 嗯 我我我想说", "start_sec": 0.0, "end_sec": 1.0},
        {"sentence_id": "S002", "text": "第二句讲智能体（agent）会调用工具", "start_sec": 1.0, "end_sec": 2.0},
        {"sentence_id": "S003", "text": "原文3", "start_sec": 2.0, "end_sec": 3.0},
    ]
    cleaned_maps = [
        {
            "S001": {
                "sentence_id": "S001",
                "removals": [
                    {"sentence_id": "S001", "original": " 嗯 ", "left_context": "第一句", "right_context": "我我我想说"},
                    {"sentence_id": "S001", "original": "我我", "left_context": "第一句", "right_context": "我想说"},
                ],
            },
            "S002": {
                "sentence_id": "S002",
                "removals": [
                    {"sentence_id": "S002", "original": "（agent）", "left_context": "智能体", "right_context": "会调用"},
                ],
            },
        },
        {
            "S001": {"sentence_id": "S001", "cleaned_text": "重复1"},
        },
    ]
    llm_cleaned_by_id, merge_metrics = merge_step4_cleaned_maps(cleaned_maps)
    assert len(llm_cleaned_by_id["S001"]["removals"]) == 2
    assert merge_metrics["dropped_duplicate_sentence_id_across_batches"] == 1

    assembled, assemble_metrics = assemble_step4_cleaned_sentences(
        source_sentences,
        llm_cleaned_by_id=llm_cleaned_by_id,
        glossary_guard=lambda source, cleaned: "（agent）" in source and "（agent）" not in cleaned,
    )
    assert [item["cleaned_text"] for item in assembled] == ["第一句我想说", "第二句讲智能体（agent）会调用工具", "原文3"]
    assert assemble_metrics["sentence_passthrough_used"] == 1
    assert assemble_metrics["bilingual_pair_guard_fallback_used"] == 1
    assert assemble_metrics["selected_removal_patch_path"] == 2


def test_parse_step56_payload_compact_and_fallback():
    keep_ids, paragraphs, metrics = parse_step56_dedup_merge_payload(
        {
            "k": ["S001"],
            "p": [{"t": "paragraph", "sids": ["S001"], "mt": "merge"}],
        },
        ordered_batch_ids=["S001", "S002"],
        sentence_text_map={"S001": "sent1", "S002": "sent2"},
    )
    assert keep_ids == ["S001"]
    assert paragraphs[0]["text"] == "paragraph"
    assert paragraphs[0]["source_sentence_ids"] == ["S001"]
    assert metrics["compact_shape_hits"] == 1

    fallback_keep_ids, fallback_paragraphs, fallback_metrics = parse_step56_dedup_merge_payload(
        {"k": []},
        ordered_batch_ids=["S001", "S002"],
        sentence_text_map={"S001": "sent1", "S002": "sent2"},
    )
    assert fallback_keep_ids == ["S001", "S002"]
    assert len(fallback_paragraphs) == 2
    assert fallback_metrics["keep_ids_fallback_used"] == 1


def test_cleaned_sentence_model_removed_removed_items_field():
    assert "removed_items" not in CleanedSentence.model_fields

    # 兼容历史输入：多余字段应被忽略，避免旧缓存反序列化失败。
    item = CleanedSentence(
        sentence_id="S001",
        cleaned_text="cleaned",
        removed_items=["legacy"],  # type: ignore[arg-type]
    )
    dumped = item.model_dump()
    assert "removed_items" not in dumped
