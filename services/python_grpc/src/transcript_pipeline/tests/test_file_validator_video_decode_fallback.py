import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

from services.python_grpc.src.transcript_pipeline.tools import file_validator


class _FakeCapture:
    def __init__(self) -> None:
        self.released = False

    def release(self) -> None:
        self.released = True


def _mock_valid_video_path(monkeypatch) -> None:
    monkeypatch.setattr(file_validator.Path, "exists", lambda _self: True)
    monkeypatch.setattr(file_validator.Path, "is_file", lambda _self: True)
    monkeypatch.setattr(
        file_validator.Path,
        "stat",
        lambda _self: SimpleNamespace(st_size=128),
    )


def test_validate_video_succeeds_when_quick_probe_readable(monkeypatch):
    _mock_valid_video_path(monkeypatch)
    video_path = "input.mp4"

    fake_cap = _FakeCapture()
    monkeypatch.setattr(file_validator.cv2, "VideoCapture", lambda _path: fake_cap)
    monkeypatch.setattr(file_validator, "probe_primary_video_codec", lambda _path: "h264")
    monkeypatch.setattr(file_validator, "probe_capture_readable", lambda _cap: True)

    is_valid, error = file_validator.validate_video(video_path)

    assert is_valid is True
    assert error is None
    assert fake_cap.released is True


def test_validate_video_allows_av1_without_opencv_probe(monkeypatch):
    _mock_valid_video_path(monkeypatch)
    video_path = "input.mp4"

    def _unexpected_video_capture(_path):
        raise AssertionError("VideoCapture should not be called for AV1 codec in step1 validate")

    monkeypatch.setattr(file_validator.cv2, "VideoCapture", _unexpected_video_capture)
    monkeypatch.setattr(file_validator, "probe_primary_video_codec", lambda _path: "av1")

    is_valid, error = file_validator.validate_video(video_path)

    assert is_valid is True
    assert error is None


def test_validate_video_treats_av01_as_av1_without_opencv_probe(monkeypatch):
    _mock_valid_video_path(monkeypatch)
    video_path = "input.mp4"

    def _unexpected_video_capture(_path):
        raise AssertionError("VideoCapture should not be called for AV01 codec in step1 validate")

    monkeypatch.setattr(file_validator.cv2, "VideoCapture", _unexpected_video_capture)
    monkeypatch.setattr(file_validator, "probe_primary_video_codec", lambda _path: "av01")

    is_valid, error = file_validator.validate_video(video_path)

    assert is_valid is True
    assert error is None


def test_validate_video_returns_codec_hint_when_non_av1_decode_failed(monkeypatch):
    _mock_valid_video_path(monkeypatch)
    video_path = "input.mp4"

    fake_cap = _FakeCapture()
    monkeypatch.setattr(file_validator.cv2, "VideoCapture", lambda _path: fake_cap)
    monkeypatch.setattr(file_validator, "probe_primary_video_codec", lambda _path: "vp9")
    monkeypatch.setattr(file_validator, "probe_capture_readable", lambda _cap: False)

    is_valid, error = file_validator.validate_video(video_path)

    assert is_valid is False
    assert error is not None
    assert "Cannot decode video file" in error
    assert "codec=vp9" in error
    assert fake_cap.released is True
