import json

from services.python_grpc.src.content_pipeline.shared.subtitle.subtitle_repository import SubtitleRepository


def test_repository_discovers_intermediate_files_and_loads_step2_step6(tmp_path):
    output_dir = tmp_path / "out"
    inter_dir = output_dir / "intermediates"
    inter_dir.mkdir(parents=True, exist_ok=True)

    step2_path = inter_dir / "step2_correction_output.json"
    step2_path.write_text(
        json.dumps(
            {
                "output": {
                    "corrected_subtitles": [
                        {
                            "subtitle_id": "SUB001",
                            "corrected_text": "hello",
                            "start_sec": 0.0,
                            "end_sec": 1.0,
                        }
                    ]
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    step6_path = inter_dir / "step6_merge_cross_output.json"
    step6_path.write_text(
        json.dumps(
            {
                "pure_text_script": [
                    {
                        "paragraph_id": "P001",
                        "text": "body",
                        "source_sentence_ids": ["S001"],
                    }
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    repository = SubtitleRepository.from_output_dir(output_dir=str(output_dir))

    assert repository.step2_path == str(step2_path)
    assert repository.step6_path == str(step6_path)
    assert len(repository.list_subtitles()) == 1
    assert len(repository.load_step6_paragraphs()) == 1


def test_map_timestamp_to_sentence_id_prefers_in_range_then_nearest(tmp_path):
    output_dir = tmp_path / "out"
    inter_dir = output_dir / "intermediates"
    inter_dir.mkdir(parents=True, exist_ok=True)

    sentence_ts_path = inter_dir / "sentence_timestamps.json"
    sentence_ts_path.write_text(
        json.dumps(
            {
                "S001": {"start_sec": 0.0, "end_sec": 2.0},
                "S002": {"start_sec": 2.0, "end_sec": 5.0},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    repository = SubtitleRepository.from_output_dir(
        output_dir=str(output_dir),
        sentence_timestamps_path=str(sentence_ts_path),
    )

    assert repository.map_timestamp_to_sentence_id(1.5) == "S001"
    assert repository.map_timestamp_to_sentence_id(4.1) == "S002"
    assert repository.map_timestamp_to_sentence_id(8.0) == "S002"


def test_get_subtitles_in_range_with_boundary_expansion(tmp_path):
    output_dir = tmp_path / "out"
    inter_dir = output_dir / "intermediates"
    inter_dir.mkdir(parents=True, exist_ok=True)

    step2_path = inter_dir / "step2_correction_output.json"
    step2_path.write_text(
        json.dumps(
            {
                "corrected_subtitles": [
                    {"subtitle_id": "S001", "corrected_text": "第一句", "start_sec": 0.0, "end_sec": 1.0},
                    {"subtitle_id": "S002", "corrected_text": "第二句", "start_sec": 1.0, "end_sec": 3.0},
                    {"subtitle_id": "S003", "corrected_text": "第三句", "start_sec": 3.0, "end_sec": 5.0},
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    repository = SubtitleRepository.from_output_dir(output_dir=str(output_dir), step2_path=str(step2_path))

    no_expand = repository.get_subtitles_in_range(
        1.2,
        2.0,
        expand_to_sentence_boundary=False,
        include_ts_prefix=False,
        empty_fallback="",
    )
    with_expand = repository.get_subtitles_in_range(
        1.2,
        2.0,
        expand_to_sentence_boundary=True,
        include_ts_prefix=True,
        empty_fallback="",
    )

    assert "第二句" in no_expand
    assert "[1.0s] 第二句" in with_expand


def test_sentence_text_supports_index_and_subtitle_id(tmp_path):
    output_dir = tmp_path / "out"
    inter_dir = output_dir / "intermediates"
    inter_dir.mkdir(parents=True, exist_ok=True)

    step2_path = inter_dir / "step2_correction_output.json"
    step2_path.write_text(
        json.dumps(
            {
                "corrected_subtitles": [
                    {"subtitle_id": "SUB_A", "corrected_text": "Alpha", "start_sec": 0.0, "end_sec": 1.0},
                    {"subtitle_id": "SUB_B", "corrected_text": "Beta", "start_sec": 1.0, "end_sec": 2.0},
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    repository = SubtitleRepository.from_output_dir(output_dir=str(output_dir), step2_path=str(step2_path))

    assert repository.get_sentence_text("S001") == "Alpha"
    assert repository.get_sentence_text("SUB_B") == "Beta"


def test_set_raw_subtitles_supports_in_memory_range_queries():
    repository = SubtitleRepository()
    repository.set_raw_subtitles(
        [
            {"subtitle_id": "S001", "text": "first", "start_sec": 0.0, "end_sec": 1.0},
            {"subtitle_id": "S002", "text": "second", "start_sec": 1.0, "end_sec": 2.0},
            {"subtitle_id": "S003", "text": "third", "start_sec": 2.0, "end_sec": 3.0},
        ]
    )

    text = repository.get_subtitles_in_range(
        0.8,
        2.2,
        expand_to_sentence_boundary=False,
        include_ts_prefix=False,
        empty_fallback="",
    )
    assert text == "first second third"

