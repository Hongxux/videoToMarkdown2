import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[6]))

from services.python_grpc.src.common.utils.patch_protocol import (  # noqa: E402
    collect_patch_ops,
    normalize_removal_patch_item,
    normalize_replace_add_patch_item,
    pick_full_text_fallback,
)


def test_normalize_replace_add_patch_item_supports_replace_and_add():
    replace_item = normalize_replace_add_patch_item(
        {"m": "r", "o": "old", "n": "new", "l": "L", "r": "R"}
    )
    assert replace_item == {
        "mode": "r",
        "o": "old",
        "n": "new",
        "l": "L",
        "r": "R",
    }

    add_item = normalize_replace_add_patch_item(
        {"add": "append", "left_context": "A", "right_context": "B", "position": "before"}
    )
    assert add_item == {
        "mode": "a",
        "n": "append",
        "l": "A",
        "r": "B",
        "p": "before",
    }


def test_collect_patch_ops_supports_group_lists():
    payload = {
        "p": [{"m": "r", "o": "a", "n": "b"}],
        "r": [{"o": "x", "n": "y"}],
        "a": [{"n": "z"}],
    }
    ops = collect_patch_ops(payload)
    assert len(ops) == 3
    assert ops[0]["m"] == "r"
    assert ops[1]["m"] == "r"
    assert ops[2]["m"] == "a"


def test_pick_full_text_fallback_uses_expected_priority():
    payload = {
        "enhanced_body": "enhanced",
        "body": "body",
    }
    assert pick_full_text_fallback(payload) == "enhanced"
    assert pick_full_text_fallback({"text": " t "}) == "t"
    assert pick_full_text_fallback({}) == ""


def test_normalize_removal_patch_item_supports_aliases():
    item = normalize_removal_patch_item(
        {"o": "remove", "context_before": "L", "context_after": "R", "sid": "S001"}
    )
    assert item == {
        "original": "remove",
        "left_context": "L",
        "right_context": "R",
        "sentence_id": "S001",
    }
    assert normalize_removal_patch_item({"o": ""}) is None
