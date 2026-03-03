from services.python_grpc.src.content_pipeline.common.utils import json_payload_repair


def test_parse_json_payload_handles_unescaped_newline():
    text = """
[
  {
    "id": 1,
    "main_operation": "line1
line2"
  }
]
""".strip()

    data, err = json_payload_repair.parse_json_payload(text)

    assert err is None
    assert isinstance(data, list)
    assert data[0]["main_operation"] == "line1\nline2"


def test_extract_salvaged_json_objects_from_truncated_array():
    text = """
[
  {"id": 1, "name": "a"},
  {"id": 2, "name": "b"}
""".strip()

    objs, err = json_payload_repair.extract_salvaged_json_objects(text)

    assert err is None
    assert [item["id"] for item in objs] == [1, 2]


def test_remove_trailing_commas_preserves_string_content():
    text = '{"a": "x, y", "b": [1,2,],}'
    repaired = json_payload_repair.remove_trailing_commas(text)
    assert repaired == '{"a": "x, y", "b": [1,2]}'
