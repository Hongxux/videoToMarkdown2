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


def test_set_raw_paragraphs_supports_in_memory_paragraph_loading():
    repository = SubtitleRepository()
    repository.set_raw_paragraphs(
        [
            {
                "paragraph_id": "P001",
                "text": "alpha",
                "source_sentence_ids": ["S001", "S002"],
                "merge_type": "normal",
            }
        ]
    )

    paragraphs = repository.load_step6_paragraphs()
    assert len(paragraphs) == 1
    assert paragraphs[0]["paragraph_id"] == "P001"
    assert paragraphs[0]["text"] == "alpha"


def test_set_raw_sentence_timestamps_supports_in_memory_lookup():
    repository = SubtitleRepository()
    repository.set_raw_subtitles(
        [
            {"subtitle_id": "S001", "text": "first", "start_sec": 0.0, "end_sec": 1.0},
            {"subtitle_id": "S002", "text": "second", "start_sec": 1.0, "end_sec": 2.0},
        ]
    )
    repository.set_raw_sentence_timestamps(
        {
            "S010": {"start_sec": 10.0, "end_sec": 12.0},
        }
    )

    timestamps = repository.build_sentence_timestamps(prefer_external=False)
    assert timestamps == {"S010": {"start_sec": 10.0, "end_sec": 12.0}}



def test_runtime_payload_precedes_legacy_intermediate_files(tmp_path):
    output_dir = tmp_path / "out"
    inter_dir = output_dir / "intermediates"
    inter_dir.mkdir(parents=True, exist_ok=True)

    (inter_dir / "step2_correction_output.json").write_text(
        json.dumps(
            {
                "output": {
                    "corrected_subtitles": [
                        {"subtitle_id": "LEGACY001", "corrected_text": "legacy", "start_sec": 9.0, "end_sec": 10.0}
                    ]
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (inter_dir / "step6_merge_cross_output.json").write_text(
        json.dumps(
            {
                "output": {
                    "pure_text_script": [
                        {"paragraph_id": "P-LEGACY", "text": "legacy paragraph", "source_sentence_ids": ["LEGACY001"]}
                    ]
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (inter_dir / "sentence_timestamps.json").write_text(
        json.dumps({"LEGACY001": {"start_sec": 9.0, "end_sec": 10.0}}, ensure_ascii=False),
        encoding="utf-8",
    )

    repository = SubtitleRepository.from_output_dir(output_dir=str(output_dir))
    repository.set_raw_subtitles([
        {"subtitle_id": "MEM001", "text": "memory", "start_sec": 0.0, "end_sec": 1.0}
    ], clear_sentence_timestamps=False)
    repository.set_raw_paragraphs([
        {"paragraph_id": "P-MEM", "text": "memory paragraph", "source_sentence_ids": ["MEM001"], "merge_type": "runtime"}
    ])
    repository.set_raw_sentence_timestamps({
        "MEM001": {"start_sec": 0.0, "end_sec": 1.0}
    })

    assert repository.load_step2_subtitles()[0]["subtitle_id"] == "MEM001"
    assert repository.load_step6_paragraphs()[0]["paragraph_id"] == "P-MEM"
    assert repository.build_sentence_timestamps(prefer_external=True) == {
        "MEM001": {"start_sec": 0.0, "end_sec": 1.0}
    }