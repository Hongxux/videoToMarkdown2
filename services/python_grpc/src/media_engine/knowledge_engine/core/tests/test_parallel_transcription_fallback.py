import sys
import types

import pytest


if "faster_whisper" not in sys.modules:
    faster_whisper_stub = types.ModuleType("faster_whisper")

    class _WhisperModelStub:
        pass

    faster_whisper_stub.WhisperModel = _WhisperModelStub
    sys.modules["faster_whisper"] = faster_whisper_stub


from services.python_grpc.src.media_engine.knowledge_engine.core import parallel_transcription as pt


class _FutureStub:
    def __init__(self, result_payload=None, error=None):
        self._result_payload = result_payload
        self._error = error

    def result(self):
        if self._error is not None:
            raise self._error
        return self._result_payload


class _ExecutorStub:
    def __init__(self, futures):
        self._futures = futures

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def submit(self, func, args):
        segment_id = args[1]["id"]
        return self._futures[segment_id]


def _patch_common(monkeypatch, futures):
    monkeypatch.setattr(
        pt,
        "ProcessPoolExecutor",
        lambda max_workers: _ExecutorStub(futures),
    )
    monkeypatch.setattr(pt, "as_completed", lambda future_map: list(future_map.keys()))
    monkeypatch.setattr(
        pt,
        "split_video_segments",
        lambda *args, **kwargs: [
            {"id": 0, "start": 0.0, "end": 10.0, "duration": 10.0},
            {"id": 1, "start": 10.0, "end": 20.0, "duration": 10.0},
        ],
    )
    monkeypatch.setattr(
        pt,
        "build_parallel_plan",
        lambda *args, **kwargs: {
            "effective_workers": 2,
            "cpu_threads_per_worker": 1,
            "available_mem_gb": 8.0,
            "requested_workers": 2,
            "segment_count": 2,
            "cpu_budget": 2,
            "total_cores": 4,
        },
    )
    monkeypatch.setattr(
        pt,
        "format_subtitles",
        lambda subtitles: "|".join(sorted(item["text"] for item in subtitles)),
    )
    monkeypatch.setattr(
        pt,
        "_extract_full_audio",
        lambda _video_path, full_audio_path: open(full_audio_path, "wb").close(),
    )

    model_downloader_module = (
        "services.python_grpc.src.media_engine.knowledge_engine.core.model_downloader"
    )
    monkeypatch.setattr(
        f"{model_downloader_module}.download_whisper_model",
        lambda *args, **kwargs: "mock-model-path",
    )


def test_parallel_failure_then_serial_fallback_success(monkeypatch):
    futures = {
        0: _FutureStub(
            result_payload={
                "segment_id": 0,
                "subtitles": [{"start": 0.0, "end": 1.0, "text": "A"}],
                "success": True,
            }
        ),
        1: _FutureStub(error=RuntimeError("worker-crashed")),
    }
    _patch_common(monkeypatch, futures)

    def _fallback_transcribe(args):
        segment_id = args[1]["id"]
        if segment_id == 1:
            return {
                "segment_id": 1,
                "subtitles": [{"start": 10.0, "end": 11.0, "text": "B"}],
                "success": True,
            }
        return {
            "segment_id": segment_id,
            "subtitles": [],
            "success": True,
        }

    monkeypatch.setattr(pt, "transcribe_segment", _fallback_transcribe)

    subtitle_text = pt.transcribe_parallel(
        video_path="demo.mp4",
        model_size="small",
        device="cpu",
        compute_type="int8",
        language="zh",
        segment_duration=600,
        num_workers=2,
        hf_endpoint=None,
        config={"whisper": {"parallel": {"enabled": True}}},
    )

    assert subtitle_text == "A|B"


def test_parallel_and_fallback_partial_failure_raises(monkeypatch):
    futures = {
        0: _FutureStub(
            result_payload={
                "segment_id": 0,
                "subtitles": [{"start": 0.0, "end": 1.0, "text": "A"}],
                "success": True,
            }
        ),
        1: _FutureStub(error=RuntimeError("worker-crashed")),
    }
    _patch_common(monkeypatch, futures)

    monkeypatch.setattr(
        pt,
        "transcribe_segment",
        lambda args: {
            "segment_id": args[1]["id"],
            "subtitles": [],
            "success": False,
            "error": "fallback-failed",
        },
    )

    with pytest.raises(RuntimeError, match=r"1/2"):
        pt.transcribe_parallel(
            video_path="demo.mp4",
            model_size="small",
            device="cpu",
            compute_type="int8",
            language="zh",
            segment_duration=600,
            num_workers=2,
            hf_endpoint=None,
            config={"whisper": {"parallel": {"enabled": True}}},
        )

