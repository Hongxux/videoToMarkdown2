import numpy as np
import pytest
from multiprocessing import shared_memory


def test_visual_feature_extractor_cleanup_releases_owned_registry(monkeypatch):
    from services.python_grpc.src.content_pipeline.phase2a.vision import visual_feature_extractor as module

    class _FakeCap:
        def __init__(self):
            self.released = False

        def isOpened(self):
            return not self.released

        def release(self):
            self.released = True

        def get(self, prop):
            mapping = {
                module.cv2.CAP_PROP_FPS: 30.0,
                module.cv2.CAP_PROP_FRAME_COUNT: 120.0,
                module.cv2.CAP_PROP_FRAME_WIDTH: 1920.0,
                module.cv2.CAP_PROP_FRAME_HEIGHT: 1080.0,
            }
            return mapping.get(prop, 0.0)

    class _FakeRegistry:
        def __init__(self):
            self.cleaned = False

        def cleanup(self):
            self.cleaned = True

    fake_cap = _FakeCap()
    monkeypatch.setattr(module, "open_video_capture_with_fallback", lambda *args, **kwargs: (fake_cap, "dummy.mp4", False))
    monkeypatch.setattr(module, "SharedFrameRegistry", _FakeRegistry)
    monkeypatch.setattr(module, "_get_clip_model", lambda: None)
    monkeypatch.setattr(module, "VisualElementDetector", lambda: object())
    monkeypatch.setattr(module, "DynamicDecisionEngine", lambda: object())
    monkeypatch.setattr(module.ResourceOrchestrator, "get_adaptive_cache_size", lambda **kwargs: 8)

    extractor = module.VisualFeatureExtractor("dummy.mp4")
    registry = extractor.shm_registry

    assert extractor._owns_shm_registry is True

    extractor.cleanup()

    assert fake_cap.released is True
    assert registry.cleaned is True
    assert extractor.shm_registry is None


def test_visual_feature_extractor_cleanup_does_not_release_shared_registry(monkeypatch):
    from services.python_grpc.src.content_pipeline.phase2a.vision import visual_feature_extractor as module

    class _FakeCap:
        def __init__(self):
            self.released = False

        def isOpened(self):
            return not self.released

        def release(self):
            self.released = True

        def get(self, prop):
            mapping = {
                module.cv2.CAP_PROP_FPS: 30.0,
                module.cv2.CAP_PROP_FRAME_COUNT: 120.0,
                module.cv2.CAP_PROP_FRAME_WIDTH: 1920.0,
                module.cv2.CAP_PROP_FRAME_HEIGHT: 1080.0,
            }
            return mapping.get(prop, 0.0)

    class _SharedRegistry:
        def __init__(self):
            self.cleaned = False

        def cleanup(self):
            self.cleaned = True

    fake_cap = _FakeCap()
    shared_registry = _SharedRegistry()
    monkeypatch.setattr(module, "open_video_capture_with_fallback", lambda *args, **kwargs: (fake_cap, "dummy.mp4", False))
    monkeypatch.setattr(module, "_get_clip_model", lambda: None)
    monkeypatch.setattr(module, "VisualElementDetector", lambda: object())
    monkeypatch.setattr(module, "DynamicDecisionEngine", lambda: object())
    monkeypatch.setattr(module.ResourceOrchestrator, "get_adaptive_cache_size", lambda **kwargs: 8)

    extractor = module.VisualFeatureExtractor("dummy.mp4", shared_frame_registry=shared_registry)

    assert extractor._owns_shm_registry is False

    extractor.cleanup()

    assert fake_cap.released is True
    assert shared_registry.cleaned is False


def test_visual_element_detector_closes_shm_when_analysis_raises(monkeypatch):
    from services.python_grpc.src.content_pipeline.phase2a.vision import visual_element_detection_helpers as module

    class _FakeShm:
        def __init__(self):
            self.buf = bytearray(12)
            self.closed = False

        def close(self):
            self.closed = True

    fake_shm = _FakeShm()
    monkeypatch.setattr(shared_memory, "SharedMemory", lambda name: fake_shm)
    monkeypatch.setattr(module.gc, "collect", lambda: None)
    monkeypatch.setattr(module.cv2, "cvtColor", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))

    with pytest.raises(RuntimeError):
        module.VisualElementDetector.analyze_frame(
            {
                "shm_name": "unit-test-shm",
                "shape": (2, 2, 3),
                "dtype": np.uint8,
            }
        )

    assert fake_shm.closed is True
