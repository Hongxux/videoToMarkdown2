import asyncio
import json
import logging
from pathlib import Path

from services.python_grpc.src.content_pipeline.infra.runtime import vl_ffmpeg_utils as ffmpeg_utils


def test_probe_iframe_timestamps_honors_asymmetric_window(monkeypatch):
    captured = {}

    def _fake_resolve_ffprobe_bin():
        return "ffprobe"

    async def _fake_run_subprocess(command):
        captured["command"] = list(command)
        return 0, json.dumps({"frames": []}), ""

    monkeypatch.setattr(ffmpeg_utils, "resolve_ffprobe_bin", _fake_resolve_ffprobe_bin)
    monkeypatch.setattr(ffmpeg_utils, "_run_subprocess", _fake_run_subprocess)

    timestamps = asyncio.run(
        ffmpeg_utils._probe_iframe_timestamps(
            video_path="dummy.mp4",
            target_timestamp_sec=10.0,
            search_window_sec=0.2,
            search_before_sec=0.0,
            search_after_sec=0.35,
        )
    )

    assert timestamps == []
    interval_index = captured["command"].index("-read_intervals") + 1
    assert captured["command"][interval_index] == "10.000000%10.350000"


def test_export_keyframe_with_ffmpeg_selects_sharpest_iframe(monkeypatch, tmp_path):
    output_path = Path(tmp_path) / "selected.png"
    calls = []

    async def _fake_probe_iframe_timestamps(**kwargs):
        _ = kwargs
        return [9.800, 10.000, 10.200]

    async def _fake_export_keyframe_at_timestamp(**kwargs):
        ts = float(kwargs["timestamp_sec"])
        target = Path(kwargs["output_path"])
        calls.append((target.name, ts))
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"img")
        return True, ""

    def _fake_calc_frame_sharpness_score(image_path):
        name = Path(image_path).name
        if "candidate_001" in name:
            return 100.0
        if "candidate_002" in name:
            return 40.0
        return 20.0

    def _fake_calc_frame_mse(_a, _b):
        return 0.0

    monkeypatch.setattr(ffmpeg_utils, "_probe_iframe_timestamps", _fake_probe_iframe_timestamps)
    monkeypatch.setattr(ffmpeg_utils, "_export_keyframe_at_timestamp", _fake_export_keyframe_at_timestamp)
    monkeypatch.setattr(ffmpeg_utils, "_calc_frame_sharpness_score", _fake_calc_frame_sharpness_score)
    monkeypatch.setattr(ffmpeg_utils, "_calc_frame_mse", _fake_calc_frame_mse)

    ok = asyncio.run(
        ffmpeg_utils.export_keyframe_with_ffmpeg(
            video_path="dummy.mp4",
            timestamp_sec=10.0,
            output_path=output_path,
            logger=logging.getLogger(__name__),
            iframe_search_window_sec=0.2,
            select_sharpest_iframe=True,
        )
    )
    assert ok is True

    final_calls = [item for item in calls if item[0] == "selected.png"]
    assert len(final_calls) == 1
    assert abs(final_calls[0][1] - 10.0) < 1e-6


def test_export_keyframe_with_ffmpeg_fallback_to_requested_when_no_iframe(monkeypatch, tmp_path):
    output_path = Path(tmp_path) / "fallback.png"
    final_calls = []

    async def _fake_probe_iframe_timestamps(**kwargs):
        _ = kwargs
        return []

    async def _fake_export_keyframe_at_timestamp(**kwargs):
        ts = float(kwargs["timestamp_sec"])
        target = Path(kwargs["output_path"])
        if target.name == "fallback.png":
            final_calls.append(ts)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"img")
        return True, ""

    monkeypatch.setattr(ffmpeg_utils, "_probe_iframe_timestamps", _fake_probe_iframe_timestamps)
    monkeypatch.setattr(ffmpeg_utils, "_export_keyframe_at_timestamp", _fake_export_keyframe_at_timestamp)

    ok = asyncio.run(
        ffmpeg_utils.export_keyframe_with_ffmpeg(
            video_path="dummy.mp4",
            timestamp_sec=7.25,
            output_path=output_path,
            logger=logging.getLogger(__name__),
            iframe_search_window_sec=0.2,
            select_sharpest_iframe=True,
        )
    )
    assert ok is True
    assert len(final_calls) == 1
    assert abs(final_calls[0] - 7.25) < 1e-6


def test_export_keyframe_with_ffmpeg_final_selected_fail_then_requested_success(monkeypatch, tmp_path):
    output_path = Path(tmp_path) / "recover.png"
    final_calls = []

    async def _fake_probe_iframe_timestamps(**kwargs):
        _ = kwargs
        return [12.0]

    async def _fake_export_keyframe_at_timestamp(**kwargs):
        ts = float(kwargs["timestamp_sec"])
        target = Path(kwargs["output_path"])
        if target.name.startswith("candidate_"):
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(b"probe")
            return True, ""
        if target.name == "recover.png":
            final_calls.append(ts)
            if abs(ts - 12.0) < 1e-6:
                return False, "failed"
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(b"ok")
            return True, ""
        return True, ""

    def _fake_calc_frame_sharpness_score(_):
        return 50.0

    def _fake_calc_frame_mse(_a, _b):
        return 0.0

    monkeypatch.setattr(ffmpeg_utils, "_probe_iframe_timestamps", _fake_probe_iframe_timestamps)
    monkeypatch.setattr(ffmpeg_utils, "_export_keyframe_at_timestamp", _fake_export_keyframe_at_timestamp)
    monkeypatch.setattr(ffmpeg_utils, "_calc_frame_sharpness_score", _fake_calc_frame_sharpness_score)
    monkeypatch.setattr(ffmpeg_utils, "_calc_frame_mse", _fake_calc_frame_mse)

    ok = asyncio.run(
        ffmpeg_utils.export_keyframe_with_ffmpeg(
            video_path="dummy.mp4",
            timestamp_sec=10.5,
            output_path=output_path,
            logger=logging.getLogger(__name__),
            iframe_search_window_sec=0.2,
            select_sharpest_iframe=True,
        )
    )
    assert ok is True
    assert final_calls == [12.0, 10.5]


def test_export_keyframe_with_ffmpeg_filters_drifted_iframe(monkeypatch, tmp_path):
    output_path = Path(tmp_path) / "drift_guard.png"
    final_calls = []

    async def _fake_probe_iframe_timestamps(**kwargs):
        _ = kwargs
        return [9.8, 10.0]

    async def _fake_export_keyframe_at_timestamp(**kwargs):
        ts = float(kwargs["timestamp_sec"])
        target = Path(kwargs["output_path"])
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"img")
        if target.name == "drift_guard.png":
            final_calls.append(ts)
        return True, ""

    def _fake_calc_frame_sharpness_score(image_path):
        if "candidate_000" in Path(image_path).name:
            return 200.0
        return 100.0

    def _fake_calc_frame_mse(_a, b):
        if "candidate_000" in Path(b).name:
            return 1200.0
        return 20.0

    monkeypatch.setattr(ffmpeg_utils, "_probe_iframe_timestamps", _fake_probe_iframe_timestamps)
    monkeypatch.setattr(ffmpeg_utils, "_export_keyframe_at_timestamp", _fake_export_keyframe_at_timestamp)
    monkeypatch.setattr(ffmpeg_utils, "_calc_frame_sharpness_score", _fake_calc_frame_sharpness_score)
    monkeypatch.setattr(ffmpeg_utils, "_calc_frame_mse", _fake_calc_frame_mse)

    ok = asyncio.run(
        ffmpeg_utils.export_keyframe_with_ffmpeg(
            video_path="dummy.mp4",
            timestamp_sec=10.0,
            output_path=output_path,
            logger=logging.getLogger(__name__),
            iframe_search_window_sec=0.2,
            select_sharpest_iframe=True,
        )
    )
    assert ok is True
    assert len(final_calls) == 1
    assert abs(final_calls[0] - 10.0) < 1e-6
