from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


def _load_split_tool_module():
    current = Path(__file__).resolve()
    for parent in current.parents:
        candidate = parent / "tools" / "split_video_by_semantic_units.py"
        if candidate.exists():
            spec = importlib.util.spec_from_file_location("split_video_by_semantic_units_tool", candidate)
            module = importlib.util.module_from_spec(spec)
            assert spec is not None and spec.loader is not None
            sys.modules.setdefault(spec.name, module)
            spec.loader.exec_module(module)
            return module
    raise FileNotFoundError("tools/split_video_by_semantic_units.py not found")


def test_stream_unit_layout_uses_canonical_filename_and_cleans_stale_outputs(tmp_path, monkeypatch):
    split_tool = _load_split_tool_module()

    video_path = tmp_path / "video.mp4"
    video_path.write_bytes(b"video")
    semantic_units_path = tmp_path / "semantic_units.json"
    out_dir = tmp_path / "semantic_unit_clips_vl"

    all_units = [
        {
            "unit_id": "SU003",
            "start_sec": 101.0,
            "end_sec": 140.0,
            "knowledge_topic": "topic3",
        },
        {
            "unit_id": "SU004",
            "start_sec": 140.0,
            "end_sec": 224.0,
            "knowledge_topic": "topic4",
        },
    ]
    semantic_units_path.write_text(json.dumps(all_units, ensure_ascii=False), encoding="utf-8")

    monkeypatch.setattr(split_tool, "ffprobe_duration", lambda _video_path: 300.0)

    def _fake_run_ffmpeg_cut(
        ffmpeg_path,
        video_path,
        start_sec,
        duration_sec,
        out_path,
        overwrite,
        timeout_sec,
        low_res_scale_height=None,
        low_res_video_bitrate=None,
    ):
        _ = (
            ffmpeg_path,
            video_path,
            start_sec,
            duration_sec,
            overwrite,
            timeout_sec,
            low_res_scale_height,
            low_res_video_bitrate,
        )
        target = Path(out_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"clip")
        return 0, "", ["ffmpeg"], 0.1

    monkeypatch.setattr(split_tool, "_run_ffmpeg_cut", _fake_run_ffmpeg_cut)

    rc = split_tool.main(
        [
            "--video",
            str(video_path),
            "--semantic-units",
            str(semantic_units_path),
            "--out-dir",
            str(out_dir),
            "--stream-unit-layout",
            "--overwrite",
        ]
    )
    assert rc == 0

    unit_dir = out_dir / "_stream_units" / "SU004"
    canonical_path = unit_dir / "001_SU004_topic4_140.00-224.00.mp4"
    stale_path = unit_dir / "002_SU004_topic4_140.00-224.00.mp4"
    assert canonical_path.exists()
    assert not stale_path.exists()

    stale_path.write_bytes(b"stale")
    semantic_units_path.write_text(
        json.dumps([all_units[1]], ensure_ascii=False),
        encoding="utf-8",
    )

    rc = split_tool.main(
        [
            "--video",
            str(video_path),
            "--semantic-units",
            str(semantic_units_path),
            "--out-dir",
            str(out_dir),
            "--stream-unit-layout",
            "--overwrite",
        ]
    )
    assert rc == 0

    clip_files = sorted(path.name for path in unit_dir.glob("*.mp4"))
    assert clip_files == [canonical_path.name]
