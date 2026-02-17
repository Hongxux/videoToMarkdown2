import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

from services.python_grpc.src.transcript_pipeline.nodes.phase2_preprocessing import (
    _parse_step2_llm_payload,
    _reconcile_step2_item,
)


def test_reconcile_step2_item_applies_missing_replacement():
    original_text = "不能试图 不能使用工具"
    llm_text = "不能试图 不能使用工具"
    llm_corrections = [
        {"original": "试图", "corrected": "识图", "reason": "同音字修正"}
    ]

    final_text, final_corrections = _reconcile_step2_item(
        original_text=original_text,
        llm_corrected_text=llm_text,
        llm_corrections=llm_corrections,
    )

    assert final_text == "不能识图 不能使用工具"
    assert len(final_corrections) == 1
    assert final_corrections[0]["original"] == "试图"
    assert final_corrections[0]["corrected"] == "识图"


def test_reconcile_step2_item_drops_unverifiable_correction():
    original_text = "2022年10月30号 奥特曼7秒诞写的官宣的第一版材料GPT"
    llm_text = "2022年10月30号 奥特曼7秒诞写的官宣的第一版材料GPT"
    llm_corrections = [
        {"original": "淡寫", "corrected": "诞寫", "reason": "同音字修正"}
    ]

    final_text, final_corrections = _reconcile_step2_item(
        original_text=original_text,
        llm_corrected_text=llm_text,
        llm_corrections=llm_corrections,
    )

    assert final_text == llm_text
    assert final_corrections == []


def test_reconcile_step2_item_skips_ambiguous_contextual_replacement():
    original_text = "图像识别图像识别"
    llm_corrections = [
        {
            "original": "识",
            "corrected": "试",
            "left_context": "图像",
            "right_context": "别",
        }
    ]

    final_text, final_corrections = _reconcile_step2_item(
        original_text=original_text,
        llm_corrected_text=original_text,
        llm_corrections=llm_corrections,
    )

    # 同一上下文出现两次，无法唯一定位时不应误改。
    assert final_text == original_text
    assert final_corrections == []


def test_reconcile_step2_item_supports_context_alias_fields():
    original_text = "这里演示一张图，这里也是一张图"
    llm_corrections = [
        {
            "original": "张",
            "corrected": "幅",
            "context_before": "也是一",
            "context_after": "图",
        }
    ]

    final_text, final_corrections = _reconcile_step2_item(
        original_text=original_text,
        llm_corrected_text=original_text,
        llm_corrections=llm_corrections,
    )

    assert final_text == "这里演示一张图，这里也是一幅图"
    assert len(final_corrections) == 1
    assert final_corrections[0]["left_context"] == "也是一"
    assert final_corrections[0]["right_context"] == "图"


def test_parse_step2_payload_supports_new_and_legacy_shapes():
    payload = {
        "c": [
            {
                "sid": "SUB001",
                "o": "试图",
                "c": "识图",
                "l": "不能",
                "r": " 不能",
            }
        ],
        "corrected_subtitles": [
            {"subtitle_id": "SUB002", "corrected_text": "legacy corrected"},
            {
                "subtitle_id": "SUB003",
                "corrections": [
                    {
                        "original": "张",
                        "corrected": "幅",
                        "context_before": "也是一",
                        "context_after": "图",
                    }
                ],
            },
        ],
    }

    parsed = _parse_step2_llm_payload(payload)

    assert parsed["SUB001"]["corrections"][0]["original"] == "试图"
    assert parsed["SUB001"]["corrections"][0]["left_context"] == "不能"
    assert parsed["SUB002"]["corrected_text"] == "legacy corrected"
    assert parsed["SUB003"]["corrections"][0]["left_context"] == "也是一"
    assert parsed["SUB003"]["corrections"][0]["subtitle_id"] == "SUB003"
