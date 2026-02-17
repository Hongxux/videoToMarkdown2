from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, Dict, List

from services.python_grpc.src.content_pipeline.phase2a.materials import flow_ops


class _DummyGenerator:
    def __init__(self, config: Dict[str, Any]):
        self.config = config


class _FakeProcess:
    def __init__(self, returncode: int = 0, stdout: bytes = b"", stderr: bytes = b""):
        self.returncode = returncode
        self._stdout = stdout
        self._stderr = stderr

    async def communicate(self):
        return self._stdout, self._stderr


def _build_semantic_units() -> List[Dict[str, Any]]:
    return [
        {
            "unit_id": "SU001",
            "start_sec": 0.0,
            "end_sec": 300.0,
            "knowledge_topic": "demo",
        }
    ]


def test_split_video_by_semantic_units_passes_pre_cut_low_res_args(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    script_path = repo_root / "tools" / "split_video_by_semantic_units.py"
    script_path.parent.mkdir(parents=True, exist_ok=True)
    script_path.write_text("# test script", encoding="utf-8")

    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    video_path = tmp_path / "input.mp4"
    video_path.write_bytes(b"video")

    captured: Dict[str, Any] = {}

    async def _fake_exec(*cmd, **kwargs):
        captured["cmd"] = list(cmd)
        return _FakeProcess()

    monkeypatch.setattr(flow_ops, "find_repo_root", lambda _: repo_root)
    monkeypatch.setattr(flow_ops.asyncio, "create_subprocess_exec", _fake_exec)

    generator = _DummyGenerator(
        {
            "semantic_split_pre_cut": {
                "enabled": True,
                "large_segment_threshold_sec": 90.0,
                "downscale_height": 480,
                "video_bitrate": "500k",
            }
        }
    )

    clips_dir = asyncio.run(
        flow_ops.split_video_by_semantic_units(
            generator=generator,
            video_path=str(video_path),
            semantic_units=_build_semantic_units(),
            output_dir=str(output_dir),
        )
    )

    assert clips_dir == str(output_dir / "semantic_unit_clips_vl")
    assert "cmd" in captured
    cmd = captured["cmd"]
    assert "--large-segment-threshold-sec" in cmd
    assert cmd[cmd.index("--large-segment-threshold-sec") + 1] == "90.000"
    assert "--large-segment-scale-height" in cmd
    assert cmd[cmd.index("--large-segment-scale-height") + 1] == "480"
    assert "--large-segment-video-bitrate" in cmd
    assert cmd[cmd.index("--large-segment-video-bitrate") + 1] == "500k"


def test_split_video_by_semantic_units_skips_pre_cut_args_when_disabled(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    script_path = repo_root / "tools" / "split_video_by_semantic_units.py"
    script_path.parent.mkdir(parents=True, exist_ok=True)
    script_path.write_text("# test script", encoding="utf-8")

    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    video_path = tmp_path / "input.mp4"
    video_path.write_bytes(b"video")

    captured: Dict[str, Any] = {}

    async def _fake_exec(*cmd, **kwargs):
        captured["cmd"] = list(cmd)
        return _FakeProcess()

    monkeypatch.setattr(flow_ops, "find_repo_root", lambda _: repo_root)
    monkeypatch.setattr(flow_ops.asyncio, "create_subprocess_exec", _fake_exec)

    generator = _DummyGenerator({"semantic_split_pre_cut": {"enabled": False}})

    asyncio.run(
        flow_ops.split_video_by_semantic_units(
            generator=generator,
            video_path=str(video_path),
            semantic_units=_build_semantic_units(),
            output_dir=str(output_dir),
        )
    )

    assert "cmd" in captured
    cmd = captured["cmd"]
    assert "--large-segment-threshold-sec" not in cmd
    assert "--large-segment-scale-height" not in cmd
    assert "--large-segment-video-bitrate" not in cmd
