from __future__ import annotations

import asyncio
import json
import shutil
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


def _build_semantic_units_mixed() -> List[Dict[str, Any]]:
    return [
        {
            "unit_id": "SU002",
            "start_sec": 65.0,
            "end_sec": 103.0,
            "knowledge_topic": "topic2",
        },
        {
            "unit_id": "SU004",
            "start_sec": 371.0,
            "end_sec": 548.0,
            "knowledge_topic": "topic4",
        },
        {
            "unit_id": "SU006",
            "start_sec": 614.0,
            "end_sec": 884.0,
            "knowledge_topic": "topic6",
        },
    ]


def _make_sandbox_dir(name: str) -> Path:
    sandbox_dir = Path("var") / name
    if sandbox_dir.exists():
        shutil.rmtree(sandbox_dir, ignore_errors=True)
    sandbox_dir.mkdir(parents=True, exist_ok=True)
    return sandbox_dir


def test_split_video_by_semantic_units_passes_pre_cut_low_res_args(monkeypatch):
    sandbox_dir = _make_sandbox_dir("tmp_flow_ops_split_pre_cut_case1")
    repo_root = sandbox_dir / "repo"
    script_path = repo_root / "tools" / "split_video_by_semantic_units.py"
    script_path.parent.mkdir(parents=True, exist_ok=True)
    script_path.write_text("# test script", encoding="utf-8")

    output_dir = sandbox_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    video_path = sandbox_dir / "input.mp4"
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
                "apply_to_all_units": True,
                "large_segment_threshold_sec": 90.0,
                "downscale_height": 480,
                "video_bitrate": "500k",
            }
        }
    )

    try:
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
        assert "--stream-unit-layout" in cmd
        assert "--large-segment-threshold-sec" in cmd
        assert cmd[cmd.index("--large-segment-threshold-sec") + 1] == "90.000"
        assert "--large-segment-scale-height" in cmd
        assert cmd[cmd.index("--large-segment-scale-height") + 1] == "480"
        assert "--large-segment-video-bitrate" in cmd
        assert cmd[cmd.index("--large-segment-video-bitrate") + 1] == "500k"
        assert "--apply-low-res-to-all-units" in cmd
        assert "--stream-unit-layout" in cmd
        assert "--apply-low-res-to-all-units" in cmd
    finally:
        shutil.rmtree(sandbox_dir, ignore_errors=True)


def test_split_video_by_semantic_units_skips_pre_cut_args_when_disabled(monkeypatch):
    sandbox_dir = _make_sandbox_dir("tmp_flow_ops_split_pre_cut_case2")
    repo_root = sandbox_dir / "repo"
    script_path = repo_root / "tools" / "split_video_by_semantic_units.py"
    script_path.parent.mkdir(parents=True, exist_ok=True)
    script_path.write_text("# test script", encoding="utf-8")

    output_dir = sandbox_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    video_path = sandbox_dir / "input.mp4"
    video_path.write_bytes(b"video")

    captured: Dict[str, Any] = {}

    async def _fake_exec(*cmd, **kwargs):
        captured["cmd"] = list(cmd)
        return _FakeProcess()

    monkeypatch.setattr(flow_ops, "find_repo_root", lambda _: repo_root)
    monkeypatch.setattr(flow_ops.asyncio, "create_subprocess_exec", _fake_exec)

    generator = _DummyGenerator({"semantic_split_pre_cut": {"enabled": False}})

    try:
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
        assert "--stream-unit-layout" in cmd
        assert "--large-segment-threshold-sec" not in cmd
        assert "--large-segment-scale-height" not in cmd
        assert "--large-segment-video-bitrate" not in cmd
        assert "--apply-low-res-to-all-units" not in cmd
    finally:
        shutil.rmtree(sandbox_dir, ignore_errors=True)


def test_split_video_by_semantic_units_only_splits_missing_units(monkeypatch):
    sandbox_dir = _make_sandbox_dir("tmp_flow_ops_split_incremental_case3")
    repo_root = sandbox_dir / "repo"
    script_path = repo_root / "tools" / "split_video_by_semantic_units.py"
    script_path.parent.mkdir(parents=True, exist_ok=True)
    script_path.write_text("# test script", encoding="utf-8")

    output_dir = sandbox_dir / "output"
    clips_dir = output_dir / "semantic_unit_clips_vl"
    clips_dir.mkdir(parents=True, exist_ok=True)
    video_path = sandbox_dir / "input.mp4"
    video_path.write_bytes(b"video")

    # 模拟路由预处理阶段已经先切过一次 SU006。
    stream_unit_dir = clips_dir / "_stream_units" / "SU006"
    stream_unit_dir.mkdir(parents=True, exist_ok=True)
    (stream_unit_dir / "001_SU006_demo_614.00-884.00.mp4").write_bytes(b"clip")

    captured: Dict[str, Any] = {}

    async def _fake_exec(*cmd, **kwargs):
        _ = kwargs
        captured["cmd"] = list(cmd)
        return _FakeProcess()

    monkeypatch.setattr(flow_ops, "find_repo_root", lambda _: repo_root)
    monkeypatch.setattr(flow_ops.asyncio, "create_subprocess_exec", _fake_exec)

    generator = _DummyGenerator({"semantic_split_pre_cut": {"enabled": False}})

    try:
        returned_dir = asyncio.run(
            flow_ops.split_video_by_semantic_units(
                generator=generator,
                video_path=str(video_path),
                semantic_units=_build_semantic_units_mixed(),
                output_dir=str(output_dir),
            )
        )
        assert returned_dir == str(clips_dir)
        assert "cmd" in captured
        cmd = captured["cmd"]
        assert "--stream-unit-layout" in cmd
        assert "--semantic-units" in cmd
        subset_json = Path(cmd[cmd.index("--semantic-units") + 1])
        subset_payload = json.loads(subset_json.read_text(encoding="utf-8"))
        subset_unit_ids = [str(item.get("unit_id", "")) for item in subset_payload if isinstance(item, dict)]
        assert subset_unit_ids == ["SU002", "SU004"]
    finally:
        shutil.rmtree(sandbox_dir, ignore_errors=True)


def test_find_clip_for_unit_prefers_stream_unit_layout(tmp_path):
    clips_dir = tmp_path / "semantic_unit_clips_vl"
    clips_dir.mkdir(parents=True, exist_ok=True)

    legacy_clip = clips_dir / "001_SU006_demo_614.00-884.00.mp4"
    legacy_clip.write_bytes(b"legacy")

    stream_unit_dir = clips_dir / "_stream_units" / "SU006"
    stream_unit_dir.mkdir(parents=True, exist_ok=True)
    stream_clip = stream_unit_dir / "001_SU006_demo_614.00-884.00.mp4"
    stream_clip.write_bytes(b"stream")

    selected = flow_ops.find_clip_for_unit(
        generator=None,
        clips_dir=str(clips_dir),
        unit_id="SU006",
        start_sec=614.0,
        end_sec=884.0,
    )

    assert selected == str(stream_clip)
