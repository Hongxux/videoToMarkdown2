import numpy as np

from services.python_grpc.src.content_pipeline.phase2a.vision import screenshot_selector as ss_mod


class _FakeCapture:
    def __init__(self, *_args, **_kwargs):
        self._fps = 30.0
        self._frame_count = 120000
        self._pos = 0

    def isOpened(self):
        return True

    def get(self, prop):
        if prop == ss_mod.cv2.CAP_PROP_FPS:
            return self._fps
        if prop == ss_mod.cv2.CAP_PROP_FRAME_COUNT:
            return self._frame_count
        if prop == ss_mod.cv2.CAP_PROP_POS_FRAMES:
            return self._pos
        return 0.0

    def set(self, prop, value):
        if prop == ss_mod.cv2.CAP_PROP_POS_FRAMES:
            self._pos = int(value)
        return True

    def read(self):
        self._pos += 1
        return True, np.zeros((720, 1280, 3), dtype=np.uint8)

    def release(self):
        return None


def test_resize_frame_max_width_downsamples():
    selector = ss_mod.ScreenshotSelector.create_lightweight()
    frame = np.zeros((1080, 1920, 3), dtype=np.uint8)

    resized = selector._resize_frame_max_width(frame, max_width=640)

    assert resized.shape[1] <= 640
    assert resized.shape[0] < 1080


def test_split_time_range_for_long_window():
    chunks = ss_mod.ScreenshotSelector._split_time_range(0.0, 35.0, 10.0)
    assert chunks == [(0.0, 10.0), (10.0, 20.0), (20.0, 30.0), (30.0, 35.0)]


def test_route_coarse_fine_long_island_chunked_sampling(monkeypatch):
    monkeypatch.setattr(ss_mod.cv2, "VideoCapture", lambda _path: _FakeCapture())
    monkeypatch.setattr(ss_mod.ScreenshotSelector, "_ensure_detector", lambda self: None)

    call_log = []

    def _fake_read(self, cap, timestamps, video_fps, total_frames, max_width=None):
        call_log.append({"count": len(timestamps), "max_width": max_width})
        frames = [np.zeros((720, 1280, 3), dtype=np.uint8) for _ in timestamps]
        return list(timestamps), frames

    def _fake_detect_islands(self, frames, timestamps, interval, roi=None):
        return [{"start_sec": 0.0, "end_sec": 35.0}]

    def _fake_select_best(self, frames, timestamps, roi=None, return_index=False):
        ts = timestamps[len(timestamps) // 2]
        score = float(len(timestamps))
        if return_index:
            return ts, score, len(timestamps) // 2
        return ts, score

    monkeypatch.setattr(ss_mod.ScreenshotSelector, "_read_frames_at_timestamps_sequential", _fake_read)
    monkeypatch.setattr(ss_mod.ScreenshotSelector, "detect_stable_islands_from_frames", _fake_detect_islands)
    monkeypatch.setattr(ss_mod.ScreenshotSelector, "select_best_frame_from_frames", _fake_select_best)

    selector = ss_mod.ScreenshotSelector.create_lightweight()
    results = selector.select_screenshots_for_range_sync(
        video_path="fake.mp4",
        start_sec=0.0,
        end_sec=35.0,
        coarse_fps=2.0,
        fine_fps=10.0,
        analysis_max_width=640,
        long_window_fine_chunk_sec=10.0,
    )

    assert len(results) == 1
    assert len(call_log) == 5  # 1 次 coarse + 4 次 fine 子区间
    assert all(item["max_width"] == 640 for item in call_log)

    fine_calls = call_log[1:]
    assert len(fine_calls) == 4
    assert max(item["count"] for item in fine_calls) <= 105


def test_route_decode_policy_is_forwarded(monkeypatch):
    monkeypatch.setattr(ss_mod.ScreenshotSelector, "_ensure_detector", lambda self: None)

    captured_kwargs = {}

    def _fake_open_video_capture_with_fallback(video_path, **kwargs):
        captured_kwargs.update(kwargs)
        return _FakeCapture(), video_path, False

    def _fake_read(self, cap, timestamps, video_fps, total_frames, max_width=None):
        frames = [np.zeros((32, 32, 3), dtype=np.uint8) for _ in timestamps]
        return list(timestamps), frames

    monkeypatch.setattr(ss_mod, "open_video_capture_with_fallback", _fake_open_video_capture_with_fallback)
    monkeypatch.setattr(ss_mod.ScreenshotSelector, "_read_frames_at_timestamps_sequential", _fake_read)
    monkeypatch.setattr(
        ss_mod.ScreenshotSelector,
        "detect_stable_islands_from_frames",
        lambda self, frames, timestamps, interval, roi=None: [{"start_sec": 0.0, "end_sec": 1.0}],
    )
    monkeypatch.setattr(
        ss_mod.ScreenshotSelector,
        "select_best_frame_from_frames",
        lambda self, frames, timestamps, roi=None, return_index=False: (timestamps[0], 1.0),
    )

    selector = ss_mod.ScreenshotSelector.create_lightweight()
    results = selector.select_screenshots_for_range_sync(
        video_path="fake.mp4",
        start_sec=0.0,
        end_sec=1.0,
        coarse_fps=2.0,
        fine_fps=2.0,
        decode_open_timeout_sec=12,
        decode_allow_inline_transcode=False,
        decode_enable_async_transcode=True,
    )

    assert len(results) == 1
    assert int(captured_kwargs.get("timeout_sec")) == 12
    assert captured_kwargs.get("allow_inline_transcode") is False
    assert captured_kwargs.get("enable_async_transcode") is True
