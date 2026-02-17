import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[6]))

from services.python_grpc.src.common.utils.text_patch import (  # noqa: E402
    extract_first_json_dict,
    find_add_insert_positions,
    find_contextual_match_positions,
    replace_by_index,
)


def test_find_contextual_match_positions_returns_unique_match_with_context():
    text = "pre-foo-mid post-foo-end"
    positions = find_contextual_match_positions(
        text,
        "foo",
        left_context="pre-",
        right_context="-mid",
    )
    assert positions == [4]


def test_replace_by_index_replaces_exact_span():
    text = "abcXYZdef"
    updated = replace_by_index(text, start=3, length=3, replacement="123")
    assert updated == "abc123def"


def test_find_add_insert_positions_keeps_existing_semantics():
    text = "LRT LRT"
    # 左右上下文同时存在时，插入点固定在 L 与 R 之间。
    both = find_add_insert_positions(
        text,
        left_context="L",
        right_context="R",
        position="before",
    )
    assert both == [1, 5]

    # 仅左上下文时，position 参数不改变现有定位语义（默认取左上下文末尾）。
    left_only = find_add_insert_positions(
        text,
        left_context="L",
        right_context="",
        position="before",
    )
    assert left_only == [1, 5]


def test_extract_first_json_dict_supports_fenced_and_inline_json():
    fenced = "说明文本```json\n{\"p\":[]}\n```尾部"
    parsed_fenced = extract_first_json_dict(fenced)
    assert parsed_fenced == {"p": []}

    inline = "前缀 noise {\"m\":\"r\",\"o\":\"a\",\"n\":\"b\"} 后缀"
    parsed_inline = extract_first_json_dict(inline)
    assert parsed_inline == {"m": "r", "o": "a", "n": "b"}
