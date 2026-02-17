from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[6]))

from services.python_grpc.src.common.utils import opencv_decode as decode_mod


class _FakeCap:
    def __init__(self, readable: bool):
        self._readable = readable
        self._released = False
        self._pos = 0.0

    def isOpened(self):
        return True

    def get(self, _prop):
        return self._pos

    def set(self, _prop, value):
        self._pos = float(value)
        return True

    def read(self):
        if not self._readable:
            return False, None
        return True, _Frame()

    def release(self):
        self._released = True


class _Frame:
    size = 1


@pytest.fixture(autouse=True)
def _clear_path_cache():
    with decode_mod._PATH_CACHE_LOCK:
        decode_mod._PATH_CACHE.clear()
    yield
    with decode_mod._PATH_CACHE_LOCK:
        decode_mod._PATH_CACHE.clear()


def test_ensure_path_disables_inline_transcode_by_default_and_schedules_async(monkeypatch, tmp_path):
    video_path = tmp_path / "source.mp4"
    video_path.write_bytes(b"fake")
    scheduled = []
    transcode_called = []

    monkeypatch.delenv("OPENCV_DECODE_ALLOW_INLINE_TRANSCODE", raising=False)
    monkeypatch.setattr(decode_mod, "probe_primary_video_codec", lambda _path: "av1")
    monkeypatch.setattr(decode_mod.cv2, "VideoCapture", lambda _path: _FakeCap(readable=False))
    monkeypatch.setattr(
        decode_mod,
        "_schedule_async_transcode",
        lambda source_video_path, **_kwargs: scheduled.append(source_video_path) or True,
    )
    monkeypatch.setattr(
        decode_mod,
        "transcode_to_h264_for_opencv",
        lambda *_args, **_kwargs: transcode_called.append(True) or None,
    )

    resolved, used_fallback = decode_mod.ensure_opencv_readable_video_path(str(video_path))

    assert resolved == str(video_path.resolve())
    assert used_fallback is False
    assert scheduled == [str(video_path.resolve())]
    assert transcode_called == []


def test_ensure_path_can_force_inline_transcode(monkeypatch, tmp_path):
    video_path = tmp_path / "source.mp4"
    fallback_path = tmp_path / "_opencv_decode_fallback" / "source_h264.mp4"
    video_path.write_bytes(b"fake")

    monkeypatch.setattr(decode_mod, "probe_primary_video_codec", lambda _path: "av1")

    def _cap_factory(path: str):
        if str(path) == str(video_path.resolve()):
            return _FakeCap(readable=False)
        if str(path) == str(fallback_path.resolve()):
            return _FakeCap(readable=True)
        return _FakeCap(readable=False)

    monkeypatch.setattr(decode_mod.cv2, "VideoCapture", _cap_factory)
    monkeypatch.setattr(
        decode_mod,
        "transcode_to_h264_for_opencv",
        lambda *_args, **_kwargs: str(fallback_path.resolve()),
    )
    monkeypatch.setattr(decode_mod, "_schedule_async_transcode", lambda *_args, **_kwargs: False)

    resolved, used_fallback = decode_mod.ensure_opencv_readable_video_path(
        str(video_path),
        allow_inline_transcode=True,
    )

    assert resolved == str(fallback_path.resolve())
    assert used_fallback is True
