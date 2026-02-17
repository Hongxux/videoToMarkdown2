from __future__ import annotations

from types import SimpleNamespace

import tools.split_video_by_semantic_units as split_tool


def test_normalize_even_scale_height_rounds_down_odd() -> None:
    assert split_tool._normalize_even_scale_height(480) == 480
    assert split_tool._normalize_even_scale_height(481) == 480


def test_run_ffmpeg_cut_uses_even_width_scale_filter(monkeypatch) -> None:
    captured = {}

    def _fake_run(cmd, capture_output, text, timeout):
        captured["cmd"] = list(cmd)
        return SimpleNamespace(returncode=0, stderr="")

    monkeypatch.setattr(split_tool.subprocess, "run", _fake_run)

    rc, stderr, cmd, _elapsed = split_tool._run_ffmpeg_cut(
        ffmpeg_path="ffmpeg",
        video_path="input.mp4",
        start_sec=0.0,
        duration_sec=5.0,
        out_path="output.mp4",
        overwrite=True,
        timeout_sec=10.0,
        low_res_scale_height=481,
        low_res_video_bitrate="500k",
    )

    assert rc == 0
    assert stderr == ""
    assert cmd == captured["cmd"]
    assert "-vf" in cmd
    assert cmd[cmd.index("-vf") + 1] == "scale=-2:480"
