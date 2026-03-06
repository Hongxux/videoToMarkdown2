import numpy as np

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
