import sys
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

from services.python_grpc.src.transcript_pipeline.tools.file_validator import read_subtitle_sample


def test_txt_read_without_limit_returns_all_rows():
    total_rows = 1205
    lines = []
    for i in range(total_rows):
        mm = i // 60
        ss = i % 60
        lines.append(f"[{mm:02d}:{ss:02d}] line-{i}")

    test_dir = Path("var") / f"test_subtitle_read_all_{uuid4().hex}"
    test_dir.mkdir(parents=True, exist_ok=True)
    subtitle_path = test_dir / "long_subtitles.txt"
    try:
        subtitle_path.write_text("\n".join(lines), encoding="utf-8")
        subtitles = read_subtitle_sample(str(subtitle_path), count=None)
    finally:
        subtitle_path.unlink(missing_ok=True)
        test_dir.rmdir()

    assert len(subtitles) == total_rows
    assert subtitles[0]["subtitle_id"] == "SUB001"
    assert subtitles[-1]["subtitle_id"] == "SUB1205"


def test_txt_read_with_non_positive_limit_returns_all_rows():
    total_rows = 15
    test_dir = Path("var") / f"test_subtitle_read_all_{uuid4().hex}"
    test_dir.mkdir(parents=True, exist_ok=True)
    subtitle_path = test_dir / "long_subtitles.txt"
    try:
        subtitle_path.write_text(
            "\n".join(f"[00:{i:02d}] row-{i}" for i in range(total_rows)),
            encoding="utf-8",
        )
        subtitles = read_subtitle_sample(str(subtitle_path), count=0)
    finally:
        subtitle_path.unlink(missing_ok=True)
        test_dir.rmdir()

    assert len(subtitles) == total_rows
