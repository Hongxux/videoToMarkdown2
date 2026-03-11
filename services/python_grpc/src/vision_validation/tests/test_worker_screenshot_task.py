from pathlib import Path
import sys

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

from services.python_grpc.src.vision_validation import worker as worker_mod


class _FakeSelector:
    def select_from_shared_frames(
        self,
        frames,
        timestamps,
        fps=30.0,
        res_factor=1.0,
        min_static_island_ms=200.0,
    ):
        return {
            "selected_timestamp": float(timestamps[0]),
            "quality_score": 1.0,
            "island_count": 0,
            "analyzed_frames": len(frames),
            "candidates": [
                {
                    "timestamp_sec": float(timestamps[0]),
                    "score": 1.0,
                    "island_index": 0,
                    "island_start": float(timestamps[0]),
                    "island_end": float(timestamps[0]),
                }
            ],
        }


class _FakeSelectorFactory:
    @staticmethod
    def create_lightweight():
        return _FakeSelector()


def test_run_screenshot_selection_task_skips_route_roi_probe(monkeypatch):
    """验证流式截图 worker 任务不会触发无效 route ROI 视频探测。"""
    worker_mod._validator_cache.clear()
    worker_mod._thread_local.screenshot_selector_cache = {}
    monkeypatch.setattr(worker_mod, "_check_memory_usage", lambda: None)
    monkeypatch.setattr(
        worker_mod,
        "get_frame_from_shm",
        lambda _ref: np.zeros((16, 16, 3), dtype=np.uint8),
    )

    roi_called = {"value": 0}

    def _unexpected_route_roi(*_args, **_kwargs):
        roi_called["value"] += 1
        raise AssertionError("run_screenshot_selection_task should not call _get_route_roi")

    monkeypatch.setattr(worker_mod, "_get_route_roi", _unexpected_route_roi)

    from services.python_grpc.src.content_pipeline.phase2a.vision import screenshot_selector as ss_mod

    monkeypatch.setattr(ss_mod, "ScreenshotSelector", _FakeSelectorFactory)

    result = worker_mod.run_screenshot_selection_task(
        video_path="dummy.mp4",
        unit_id="SU001",
        island_index=0,
        expanded_start=10.0,
        expanded_end=12.0,
        shm_frames={10.5: {"shm_name": "fake_shm"}},
        fps=30.0,
    )

    assert roi_called["value"] == 0
    assert result["selected_timestamp"] == 10.5
    assert result["analyzed_frames"] == 1
    assert result["static_island_threshold_ms"] == 200.0
    assert len(result["candidate_screenshots"]) == 1
    assert result["candidate_screenshots"][0]["timestamp_sec"] == 10.5


def test_run_screenshot_selection_task_supports_explicit_static_island_threshold(monkeypatch):
    worker_mod._validator_cache.clear()
    worker_mod._thread_local.screenshot_selector_cache = {}
    monkeypatch.setattr(worker_mod, "_check_memory_usage", lambda: None)
    monkeypatch.setattr(
        worker_mod,
        "get_frame_from_shm",
        lambda _ref: np.zeros((16, 16, 3), dtype=np.uint8),
    )

    captured: dict = {}

    class _CaptureSelector:
        def select_from_shared_frames(
            self,
            frames,
            timestamps,
            fps=30.0,
            res_factor=1.0,
            min_static_island_ms=200.0,
        ):
            captured["min_static_island_ms"] = float(min_static_island_ms)
            return {
                "selected_timestamp": float(timestamps[0]),
                "quality_score": 1.0,
                "island_count": 0,
                "analyzed_frames": len(frames),
                "candidates": [
                    {
                        "timestamp_sec": float(timestamps[0]),
                        "score": 1.0,
                        "island_index": 0,
                        "island_start": float(timestamps[0]),
                        "island_end": float(timestamps[0]),
                    }
                ],
            }

    class _CaptureFactory:
        @staticmethod
        def create_lightweight():
            return _CaptureSelector()

    from services.python_grpc.src.content_pipeline.phase2a.vision import screenshot_selector as ss_mod

    monkeypatch.setattr(ss_mod, "ScreenshotSelector", _CaptureFactory)

    result = worker_mod.run_screenshot_selection_task(
        video_path="dummy.mp4",
        unit_id="SU002",
        island_index=0,
        expanded_start=20.0,
        expanded_end=22.0,
        shm_frames={20.5: {"shm_name": "fake_shm"}},
        fps=30.0,
        static_island_min_ms=100.0,
    )

    assert captured["min_static_island_ms"] == 100.0
    assert result["static_island_threshold_ms"] == 100.0



def test_run_coarse_fine_screenshot_task_reuses_shared_frames_for_analysis(monkeypatch):
    worker_mod._validator_cache.clear()
    worker_mod._thread_local.screenshot_selector_cache = {}
    monkeypatch.setattr(worker_mod, "_check_memory_usage", lambda: None)
    monkeypatch.setattr(worker_mod, "release_attached_shm_refs", lambda *_args, **_kwargs: None)

    frame_map = {
        "coarse_1": np.full((8, 12, 3), 10, dtype=np.uint8),
        "coarse_2": np.full((8, 12, 3), 20, dtype=np.uint8),
        "fine_1": np.full((8, 12, 3), 30, dtype=np.uint8),
        "fine_2": np.full((8, 12, 3), 40, dtype=np.uint8),
    }
    monkeypatch.setattr(worker_mod, "get_frame_from_shm", lambda ref: frame_map[ref["shm_name"]])
    monkeypatch.setattr(worker_mod, "_resolve_worker_readable_video_path", lambda *_args, **_kwargs: "dummy.mp4")
    monkeypatch.setattr(worker_mod, "_get_route_roi", lambda *_args, **_kwargs: (0, 0, 12, 8))

    open_calls = {"value": 0}

    def _unexpected_open(*_args, **_kwargs):
        open_calls["value"] += 1
        raise AssertionError("coarse/fine analysis should reuse shared frames before reopening video")

    monkeypatch.setattr(worker_mod, "open_video_capture_with_fallback", _unexpected_open)
    monkeypatch.setattr(worker_mod, "_extract_ocr_tokens", lambda frame, _roi: {f"{frame.shape[1]}x{frame.shape[0]}"})
    monkeypatch.setattr(
        worker_mod,
        "_extract_shape_signature",
        lambda frame, _roi: {
            "rect_count": int(frame.shape[1]),
            "component_count": int(frame.shape[0]),
            "edge_density": 0.25,
        },
    )

    class _CoarseFineSelector:
        def detect_stable_islands_from_frames(self, frames, timestamps, interval=0.5, roi=None):
            return [{"start_sec": timestamps[0], "end_sec": timestamps[-1] + interval}]

        def select_best_frame_from_frames(self, frames, timestamps, roi=None, return_index=False):
            assert return_index is True
            if return_index:
                return (float(timestamps[-1]), 0.9, len(frames) - 1)
            return (float(timestamps[-1]), 0.9)

    class _CoarseFineFactory:
        @staticmethod
        def create_lightweight():
            return _CoarseFineSelector()

    from services.python_grpc.src.content_pipeline.phase2a.vision import screenshot_selector as ss_mod

    monkeypatch.setattr(ss_mod, "ScreenshotSelector", _CoarseFineFactory)

    result = worker_mod.run_coarse_fine_screenshot_task(
        unit_id="SU003",
        start_sec=0.0,
        end_sec=1.0,
        coarse_shm_frames={0.0: {"shm_name": "coarse_1"}, 0.5: {"shm_name": "coarse_2"}},
        coarse_interval=0.5,
        fine_shm_frames_by_island=[{0.25: {"shm_name": "fine_1"}, 0.75: {"shm_name": "fine_2"}}],
        video_path="dummy.mp4",
        analysis_max_width=640,
    )

    assert open_calls["value"] == 0
    assert len(result["screenshots"]) == 1
    assert result["screenshots"][0]["timestamp_sec"] == 0.75
    assert result["screenshots"][0]["ocr_tokens"] == ["12x8"]
    assert result["screenshots"][0]["shape_signature"]["rect_count"] == 12
    assert "_analysis_frame" not in result["screenshots"][0]



def test_run_select_screenshots_for_range_task_resizes_frame_before_analysis(monkeypatch):
    worker_mod._validator_cache.clear()
    worker_mod._thread_local.screenshot_selector_cache = {}
    monkeypatch.setattr(worker_mod, "_check_memory_usage", lambda: None)
    monkeypatch.setattr(worker_mod, "_resolve_worker_readable_video_path", lambda *_args, **_kwargs: "dummy.mp4")
    monkeypatch.setattr(worker_mod, "_get_route_roi", lambda *_args, **_kwargs: (100, 50, 1100, 650))

    captured: dict = {}

    class _RangeSelector:
        def select_screenshots_for_range_sync(self, **kwargs):
            captured["selector_analysis_max_width"] = kwargs.get("analysis_max_width")
            return [{"timestamp_sec": 1.0, "score": 0.8}]

    monkeypatch.setattr(worker_mod, "_get_thread_local_screenshot_selector", lambda _key: _RangeSelector())

    class _FakeCap:
        def __init__(self, frame):
            self._frame = frame
            self.released = False

        def get(self, prop):
            if prop == worker_mod.cv2.CAP_PROP_FPS:
                return 30.0
            return 0.0

        def isOpened(self):
            return True

        def set(self, _prop, _value):
            return True

        def read(self):
            return True, self._frame.copy()

        def release(self):
            self.released = True

    fake_cap = _FakeCap(np.zeros((720, 1280, 3), dtype=np.uint8))
    monkeypatch.setattr(
        worker_mod,
        "open_video_capture_with_fallback",
        lambda *_args, **_kwargs: (fake_cap, "dummy.mp4", None),
    )

    def _fake_extract_ocr_tokens(frame, roi):
        captured["ocr_shape"] = frame.shape
        captured["ocr_roi"] = roi
        return {"token"}

    def _fake_extract_shape_signature(frame, roi):
        captured["shape_shape"] = frame.shape
        captured["shape_roi"] = roi
        return {"rect_count": 1, "component_count": 2, "edge_density": 0.1}

    monkeypatch.setattr(worker_mod, "_extract_ocr_tokens", _fake_extract_ocr_tokens)
    monkeypatch.setattr(worker_mod, "_extract_shape_signature", _fake_extract_shape_signature)

    result = worker_mod.run_select_screenshots_for_range_task(
        video_path="dummy.mp4",
        unit_id="SU004",
        start_sec=0.0,
        end_sec=2.0,
        analysis_max_width=640,
    )

    assert captured["selector_analysis_max_width"] == 640
    assert captured["ocr_shape"][1] == 640
    assert captured["shape_shape"][1] == 640
    assert captured["ocr_roi"] == (50, 25, 550, 325)
    assert captured["shape_roi"] == (50, 25, 550, 325)
    assert result["screenshots"][0]["ocr_tokens"] == ["token"]
