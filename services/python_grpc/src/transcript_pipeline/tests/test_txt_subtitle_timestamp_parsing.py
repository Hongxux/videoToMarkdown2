import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

from services.python_grpc.src.transcript_pipeline.tools.file_validator import read_subtitle_sample
from services.python_grpc.src.transcript_pipeline.tests.test_tmp_utils import make_repo_tmp_dir


def test_txt_single_timestamp_end_sec_follows_next_start():
    tmp_path = make_repo_tmp_dir("test_txt_single_timestamp_end_sec_follows_next_start")
    subtitle_path = tmp_path / "subtitles.txt"
    subtitle_path.write_text(
        "\n".join(
            [
                "[00:11:20] 第一行",
                "[00:11:21] 第二行",
                "[00:11:27] 第三行",
            ]
        ),
        encoding="utf-8",
    )

    subtitles = read_subtitle_sample(str(subtitle_path), count=10)

    assert len(subtitles) == 3
    assert subtitles[0]["start_sec"] == 680.0
    assert subtitles[0]["end_sec"] == 681.0
    assert subtitles[1]["start_sec"] == 681.0
    assert subtitles[1]["end_sec"] == 687.0
    assert subtitles[2]["start_sec"] == 687.0
    assert subtitles[2]["end_sec"] == 689.0


def test_txt_single_timestamp_skips_equal_timestamp_and_uses_next_greater():
    tmp_path = make_repo_tmp_dir("test_txt_single_timestamp_skips_equal_timestamp_and_uses_next_greater")
    subtitle_path = tmp_path / "subtitles.txt"
    subtitle_path.write_text(
        "\n".join(
            [
                "[00:01:00] A",
                "[00:01:00] B",
                "[00:01:01] C",
            ]
        ),
        encoding="utf-8",
    )

    subtitles = read_subtitle_sample(str(subtitle_path), count=10)

    assert len(subtitles) == 3
    assert subtitles[0]["start_sec"] == 60.0
    assert subtitles[0]["end_sec"] == 61.0
    assert subtitles[1]["start_sec"] == 60.0
    assert subtitles[1]["end_sec"] == 61.0
    assert subtitles[2]["start_sec"] == 61.0
    assert subtitles[2]["end_sec"] == 63.0
