import logging

import numpy as np

from services.python_grpc.src.content_pipeline.phase2a.vision import cv_knowledge_validator
from services.python_grpc.src.content_pipeline.phase2a.vision.cv_knowledge_validator import CVKnowledgeValidator


class _DummyCap:
    def __init__(self, fail_at_or_after=None):
        self._opened = True
        self._pos_msec = 0.0
        self._fail_at_or_after = fail_at_or_after

    def isOpened(self):
        return self._opened

    def set(self, _prop, value):
        self._pos_msec = float(value)
        return True

    def read(self):
        sec = self._pos_msec / 1000.0
        if self._fail_at_or_after is not None and sec >= self._fail_at_or_after:
            return False, None
        return True, np.zeros((4, 4, 3), dtype=np.uint8)


class _InitCap:
    def __init__(self, readable=True):
        self._opened = True
        self._readable = readable
        self._pos_frames = 0.0
        self._released = False

    def isOpened(self):
        return self._opened

    def set(self, prop, value):
        if prop == cv_knowledge_validator.cv2.CAP_PROP_POS_FRAMES:
            self._pos_frames = float(value)
        return True

    def read(self):
        if not self._readable:
            return False, None
        return True, np.zeros((8, 8, 3), dtype=np.uint8)

    def get(self, prop):
        if prop == cv_knowledge_validator.cv2.CAP_PROP_FPS:
            return 10.0
        if prop == cv_knowledge_validator.cv2.CAP_PROP_FRAME_COUNT:
            return 100.0
        if prop == cv_knowledge_validator.cv2.CAP_PROP_FRAME_WIDTH:
            return 1280.0
        if prop == cv_knowledge_validator.cv2.CAP_PROP_POS_FRAMES:
            return self._pos_frames
        return 0.0

    def release(self):
        self._released = True


def _build_validator(cap, duration_sec=0.0, fps=30.0):
    validator = CVKnowledgeValidator.__new__(CVKnowledgeValidator)
    validator.video_path = "dummy.mp4"
    validator.use_resource_manager = False
    validator.cap = cap
    validator.fps = fps
    validator.duration_sec = duration_sec
    validator._resize_frame = lambda frame: frame
    return validator


def test_sample_frames_skip_unreadable_tail_warning(monkeypatch, caplog):
    monkeypatch.setattr(cv_knowledge_validator.cv2, "VideoCapture", _DummyCap)
    validator = _build_validator(_DummyCap(fail_at_or_after=15.0), duration_sec=15.0, fps=30.0)

    with caplog.at_level(logging.WARNING, logger=cv_knowledge_validator.__name__):
        sampled = validator._sample_frames(start_sec=13.0, end_sec=15.0, fps=1.0)

    assert [ts for ts, _ in sampled] == [13.0, 14.0]
    assert not any("Failed to read frame" in rec.message for rec in caplog.records)


def test_sample_frames_use_indexed_timestamps_without_drift(monkeypatch):
    monkeypatch.setattr(cv_knowledge_validator.cv2, "VideoCapture", _DummyCap)
    validator = _build_validator(_DummyCap(), duration_sec=0.0, fps=30.0)

    sampled = validator._sample_frames(start_sec=12.0, end_sec=13.0, fps=10.0)
    timestamps = [ts for ts, _ in sampled]

    assert len(timestamps) == 11
    assert timestamps[0] == 12.0
    assert timestamps[-1] == 13.0
    assert all(ts == round(ts, 6) for ts in timestamps)


def test_init_video_fallback_to_transcoded_path_when_decode_probe_fails(monkeypatch):
    created_paths = []

    def _video_capture_factory(path):
        created_paths.append(path)
        if path == "source.mp4":
            return _InitCap(readable=False)
        if path == "fallback.mp4":
            return _InitCap(readable=True)
        return _InitCap(readable=False)

    monkeypatch.setattr(cv_knowledge_validator.cv2, "VideoCapture", _video_capture_factory)
    monkeypatch.setattr(
        CVKnowledgeValidator,
        "_transcode_to_h264_for_opencv",
        lambda self, _source_path: "fallback.mp4",
    )

    validator = CVKnowledgeValidator("source.mp4", use_resource_manager=False)

    assert validator.source_video_path == "source.mp4"
    assert validator.video_path == "fallback.mp4"
    assert validator.fps == 10.0
    assert created_paths[:2] == ["source.mp4", "fallback.mp4"]


def test_init_video_keeps_source_path_when_decode_probe_passes(monkeypatch):
    monkeypatch.setattr(cv_knowledge_validator.cv2, "VideoCapture", lambda _path: _InitCap(readable=True))

    def _unexpected_transcode(_self, _source_path):
        raise AssertionError("transcode fallback should not be called for readable input")

    monkeypatch.setattr(CVKnowledgeValidator, "_transcode_to_h264_for_opencv", _unexpected_transcode)

    validator = CVKnowledgeValidator("source.mp4", use_resource_manager=False)

    assert validator.source_video_path == "source.mp4"
    assert validator.video_path == "source.mp4"
    assert validator.fps == 10.0
