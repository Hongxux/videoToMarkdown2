import sys
import types
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[7]))


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
        "_build_process_pool_executor",
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


def test_iter_parallel_batch_results_yields_in_completion_order(monkeypatch):
    futures = {
        0: _FutureStub(
            result_payload={
                "segment_id": 0,
                "subtitles": [{"start": 0.0, "end": 1.0, "text": "A"}],
                "success": True,
            }
        ),
        1: _FutureStub(
            result_payload={
                "segment_id": 1,
                "subtitles": [{"start": 10.0, "end": 11.0, "text": "B"}],
                "success": True,
            }
        ),
    }
    monkeypatch.setattr(
        pt,
        "_build_process_pool_executor",
        lambda max_workers: _ExecutorStub(futures),
    )
    monkeypatch.setattr(
        pt,
        "as_completed",
        lambda future_map: list(reversed(list(future_map.keys()))),
    )

    tasks_args = [
        ("full-audio.wav", {"id": 0}, "model", "cpu", "int8", "zh", 1, 4, False),
        ("full-audio.wav", {"id": 1}, "model", "cpu", "int8", "zh", 1, 4, False),
    ]

    iterator = pt._iter_parallel_batch_results(tasks_args=tasks_args, max_workers=2)

    first_task_args, first_result = next(iterator)
    second_task_args, second_result = next(iterator)

    assert first_task_args[1]["id"] == 1
    assert first_result["segment_id"] == 1
    assert second_task_args[1]["id"] == 0
    assert second_result["segment_id"] == 0
    with pytest.raises(StopIteration):
        next(iterator)


def test_build_failed_segment_result_preserves_empty_memory_error_as_type_name():
    result = pt._build_failed_segment_result(segment_id=7, error=MemoryError())

    assert result["error"] == "MemoryError"
    assert pt._is_resource_exhaustion_error(result["error"]) is True


def test_iter_parallel_batch_results_converts_submit_failure_to_failed_result(monkeypatch):
    class _SubmitFailureExecutor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            return False

        def submit(self, func, args):
            raise RuntimeError(
                "A process in the process pool was terminated abruptly while the future was running or pending."
            )

    monkeypatch.setattr(
        pt,
        "_build_process_pool_executor",
        lambda max_workers: _SubmitFailureExecutor(),
    )

    results = list(
        pt._iter_parallel_batch_results(
            tasks_args=[
                ("full-audio.wav", {"id": 0}, "model", "cpu", "int8", "zh", 1, 4, False),
            ],
            max_workers=2,
        )
    )

    assert len(results) == 1
    assert results[0][0][1]["id"] == 0
    assert results[0][1]["success"] is False
    assert "terminated abruptly" in results[0][1]["error"]


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


def test_parallel_resource_exhaustion_retries_with_lower_workers(monkeypatch):
    _patch_common(monkeypatch, {})

    executor_calls = []
    attempt_futures = [
        {
            0: _FutureStub(
                result_payload={
                    "segment_id": 0,
                    "subtitles": [{"start": 0.0, "end": 1.0, "text": "A"}],
                    "success": True,
                }
            ),
            1: _FutureStub(
                result_payload={
                    "segment_id": 1,
                    "error": "mkl_malloc: failed to allocate memory",
                    "success": False,
                }
            ),
        },
        {
            1: _FutureStub(
                result_payload={
                    "segment_id": 1,
                    "subtitles": [{"start": 10.0, "end": 11.0, "text": "B"}],
                    "success": True,
                }
            ),
        },
    ]

    def _build_executor(max_workers):
        executor_calls.append(max_workers)
        return _ExecutorStub(attempt_futures[len(executor_calls) - 1])

    monkeypatch.setattr(pt, "_build_process_pool_executor", _build_executor)
    monkeypatch.setattr(
        pt,
        "transcribe_segment",
        lambda args: pytest.fail("resource retry should complete before serial fallback"),
    )

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
    assert executor_calls == [2, 1]


def test_parallel_process_pool_crash_retries_with_lower_workers(monkeypatch):
    _patch_common(monkeypatch, {})

    executor_calls = []
    abrupt_error = RuntimeError(
        "A process in the process pool was terminated abruptly while the future was running or pending."
    )
    attempt_futures = [
        {
            0: _FutureStub(error=abrupt_error),
            1: _FutureStub(error=abrupt_error),
        },
        {
            0: _FutureStub(
                result_payload={
                    "segment_id": 0,
                    "subtitles": [{"start": 0.0, "end": 1.0, "text": "A"}],
                    "success": True,
                }
            ),
            1: _FutureStub(
                result_payload={
                    "segment_id": 1,
                    "subtitles": [{"start": 10.0, "end": 11.0, "text": "B"}],
                    "success": True,
                }
            ),
        },
    ]

    def _build_executor(max_workers):
        executor_calls.append(max_workers)
        return _ExecutorStub(attempt_futures[len(executor_calls) - 1])

    monkeypatch.setattr(pt, "_build_process_pool_executor", _build_executor)
    monkeypatch.setattr(
        pt,
        "transcribe_segment",
        lambda args: pytest.fail("process pool crash should be retried before serial fallback"),
    )

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
    assert executor_calls == [2, 1]


def test_parallel_progress_callback_emits_per_completed_segment(monkeypatch):
    futures = {
        0: _FutureStub(
            result_payload={
                "segment_id": 0,
                "subtitles": [{"start": 0.0, "end": 1.0, "text": "A"}],
                "success": True,
            }
        ),
        1: _FutureStub(
            result_payload={
                "segment_id": 1,
                "subtitles": [{"start": 10.0, "end": 11.0, "text": "B"}],
                "success": True,
            }
        ),
    }
    _patch_common(monkeypatch, futures)
    monkeypatch.setattr(
        pt,
        "transcribe_segment",
        lambda args: {
            "segment_id": args[1]["id"],
            "subtitles": [{"start": float(args[1]["id"]), "end": float(args[1]["id"]) + 1.0, "text": str(args[1]["id"])}],
            "success": True,
        },
    )

    events = []
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
        progress_callback=lambda event: events.append(dict(event)),
    )

    assert subtitle_text == "A|B"
    assert [event["completed"] for event in events] == [1, 2]
    assert [event["pending"] for event in events] == [1, 0]
    assert [event["segment_index"] for event in events] == [1, 2]
    assert all(event["stage"] == "transcribe" for event in events)
    assert all(event["status"] == "running" for event in events)
    assert all(event["signal_type"] == "hard" for event in events)


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

    failed_segments = []
    hooks = pt.TranscriptionSegmentRuntimeHooks(
        fail_segment=lambda segment, total_segments, error: failed_segments.append(
            {
                "segment_id": segment["id"],
                "total_segments": total_segments,
                "error": str(error),
            }
        )
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
            segment_runtime_hooks=hooks,
        )

    assert failed_segments == [
        {
            "segment_id": 1,
            "total_segments": 2,
            "error": "fallback-failed",
        }
    ]


def test_pick_stable_language_from_probe_samples_locks_only_single_language():
    samples = [
        {"window_index": 0, "start": 0.0, "duration": 40.0, "language": "zh", "probability": 0.91, "speech_duration": 32.0},
        {"window_index": 1, "start": 180.0, "duration": 40.0, "language": "zh", "probability": 0.93, "speech_duration": 34.0},
        {"window_index": 2, "start": 360.0, "duration": 40.0, "language": "zh", "probability": 0.90, "speech_duration": 31.0},
    ]

    assert pt._pick_stable_language_from_probe_samples(samples) == "zh"


def test_pick_stable_language_from_probe_samples_keeps_auto_for_mixed_zh_en():
    samples = [
        {"window_index": 0, "start": 0.0, "duration": 40.0, "language": "en", "probability": 0.91, "speech_duration": 32.0},
        {"window_index": 1, "start": 180.0, "duration": 40.0, "language": "zh", "probability": 0.93, "speech_duration": 34.0},
        {"window_index": 2, "start": 360.0, "duration": 40.0, "language": "zh", "probability": 0.90, "speech_duration": 31.0},
    ]

    assert pt._pick_stable_language_from_probe_samples(samples) is None


def test_pick_stable_language_from_probe_samples_keeps_auto_when_single_window_is_not_strong_enough():
    samples = [
        {"window_index": 0, "start": 0.0, "duration": 40.0, "language": "zh", "probability": 0.80, "speech_duration": 18.0},
    ]

    assert pt._pick_stable_language_from_probe_samples(samples) is None


def test_detect_language_by_probe_uses_multi_window_vad_vote(monkeypatch):
    calls = []
    probe_root = Path("var/tmp_pytest_language_probe_case")
    probe_root.mkdir(parents=True, exist_ok=True)

    class _InfoStub:
        def __init__(self, language, probability, speech_duration):
            self.language = language
            self.language_probability = probability
            self.duration_after_vad = speech_duration

    class _WhisperModelStub:
        def __init__(self, *args, **kwargs):
            pass

        def transcribe(self, audio_path, language=None, beam_size=1, vad_filter=False):
            calls.append(
                {
                    "audio_path": audio_path,
                    "language": language,
                    "beam_size": beam_size,
                    "vad_filter": vad_filter,
                }
            )
            if ".probe_0_" in audio_path:
                return iter(()), _InfoStub("en", 0.92, 30.0)
            if ".probe_1_" in audio_path:
                return iter(()), _InfoStub("zh", 0.94, 32.0)
            return iter(()), _InfoStub("zh", 0.91, 28.0)

    monkeypatch.setattr(pt, "WhisperModel", _WhisperModelStub)
    monkeypatch.setattr(
        pt,
        "_extract_audio_slice",
        lambda _source, _start, _duration, output_path: Path(output_path).write_bytes(b"probe"),
    )

    selected_language, samples = pt._detect_language_by_probe(
        full_audio_path=str(probe_root / "full_audio.wav"),
        model_path="mock-model-path",
        device="cpu",
        compute_type="int8",
        cpu_threads=1,
        probe_sec=120,
        total_duration_sec=400.0,
    )

    assert selected_language is None
    assert len(samples) == 3
    assert all(call["language"] is None for call in calls)
    assert all(call["beam_size"] == 1 for call in calls)
    assert all(call["vad_filter"] is True for call in calls)




def test_build_process_pool_executor_uses_spawn_context(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        pt,
        "create_spawn_process_pool",
        lambda max_workers: captured.setdefault("max_workers", max_workers) or "executor",
    )

    pt._build_process_pool_executor(max_workers=3)

    assert captured["max_workers"] == 3
