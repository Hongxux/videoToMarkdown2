from __future__ import annotations

import numpy as np


def test_shared_frame_registry_rejects_second_frame_when_byte_budget_is_full():
    from services.python_grpc.src.content_pipeline.phase2a.vision.visual_feature_extractor import SharedFrameRegistry

    registry = SharedFrameRegistry(max_frames=10, max_bytes=32)
    frame = np.ones((2, 4, 3), dtype=np.uint8)

    try:
        registry.register_frame(1, frame)
        registry.register_frame(2, frame)

        assert registry.get_shm_ref(1) is not None
        assert registry.get_shm_ref(2) is None
        assert registry.current_bytes == frame.nbytes
    finally:
        registry.cleanup()


def test_shared_frame_registry_rejects_oversized_single_frame_without_evicting_existing_frame():
    from services.python_grpc.src.content_pipeline.phase2a.vision.visual_feature_extractor import SharedFrameRegistry

    registry = SharedFrameRegistry(max_frames=10, max_bytes=24)
    small_frame = np.ones((2, 2, 3), dtype=np.uint8)
    large_frame = np.ones((3, 3, 3), dtype=np.uint8)

    try:
        registry.register_frame(1, small_frame)
        registry.register_frame(2, large_frame)

        assert registry.get_shm_ref(1) is not None
        assert registry.get_shm_ref(2) is None
        assert registry.current_bytes == small_frame.nbytes
    finally:
        registry.cleanup()


def test_shared_frame_registry_respects_env_byte_budget(monkeypatch):
    from services.python_grpc.src.content_pipeline.phase2a.vision.visual_feature_extractor import SharedFrameRegistry

    monkeypatch.setenv("MODULE2_SHARED_FRAME_REGISTRY_MAX_MB", "384")

    assert SharedFrameRegistry.resolve_default_max_bytes() == 384 * 1024 * 1024