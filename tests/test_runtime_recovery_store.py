import asyncio
import sys
import shutil
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.python_grpc.src.common.utils.runtime_recovery_store import (
    RuntimeRecoveryStore,
    build_llm_input_fingerprint,
    classify_runtime_error,
)
from services.python_grpc.src.content_pipeline.markdown_enhancer import MarkdownEnhancer
from services.python_grpc.src.content_pipeline.phase2a.materials.vl_material_generator import VLMaterialGenerator


def _make_repo_tmp_dir(test_name: str) -> Path:
    repo_root = Path(__file__).resolve().parents[1]
    base = repo_root / "var" / "tmp_runtime_recovery_tests"
    base.mkdir(parents=True, exist_ok=True)
    safe_name = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in test_name)
    path = base / safe_name
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)
    return path


def test_runtime_recovery_store_commits_and_restores_llm_response():
    output_dir = _make_repo_tmp_dir("test_runtime_recovery_store_commits_and_restores_llm_response") / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-1")

    input_fingerprint = build_llm_input_fingerprint(
        step_name="structured_text",
        unit_id="SU100",
        model="deepseek-chat",
        system_prompt="system",
        user_prompt="user",
    )
    llm_call_id = store.build_llm_call_id(
        step_name="structured_text",
        unit_id="SU100",
        input_fingerprint=input_fingerprint,
    )
    handle = store.begin_llm_attempt(
        stage="phase2b",
        chunk_id="unit_SU100",
        llm_call_id=llm_call_id,
        input_fingerprint=input_fingerprint,
        request_payload={"prompt": "user"},
        metadata={"step_name": "structured_text"},
    )

    response_text = "这是一段很长的输出。" * 40
    commit_payload = store.commit_llm_attempt(
        handle=handle,
        response_text=response_text,
        response_metadata={"model": "deepseek-chat"},
        max_part_bytes=64,
    )

    restored = store.load_committed_llm_response(
        stage="phase2b",
        chunk_id="unit_SU100",
        llm_call_id=llm_call_id,
        input_fingerprint=input_fingerprint,
    )

    assert commit_payload["committed_parts"] > 1
    assert restored is not None
    assert restored["response_text"] == response_text
    assert Path(restored["attempt_dir"]).joinpath("commit.json").exists()


def test_runtime_recovery_store_commits_and_restores_chunk_payload():
    output_dir = _make_repo_tmp_dir("test_runtime_recovery_store_commits_and_restores_chunk_payload") / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-2")

    payload = {
        "request_updates": [
            {"request_key": "id:shot_001", "timestamp_sec": 12.5, "_optimized": True}
        ]
    }
    store.commit_chunk_payload(
        stage="phase2a",
        chunk_id="ss000001",
        input_fingerprint="fp-123",
        result_payload=payload,
        metadata={"mode": "streaming"},
    )

    restored = store.load_committed_chunk_payload(
        stage="phase2a",
        chunk_id="ss000001",
        input_fingerprint="fp-123",
    )

    assert restored is not None
    assert restored["result_payload"] == payload


def test_classify_runtime_error_marks_rate_limit_as_auto_retryable():
    error_info = classify_runtime_error(RuntimeError("429 rate limit exceeded"))
    assert error_info["error_class"] == "AUTO_RETRYABLE"
    assert error_info["retry_strategy"] == "AUTO_RETRY"
    assert error_info["operator_action"] == "WAIT_AUTO_RETRY"


def test_classify_runtime_error_marks_disk_full_as_manual_retry_with_hint():
    error_info = classify_runtime_error(RuntimeError("disk full"))
    assert error_info["error_class"] == "MANUAL_RETRY_REQUIRED"
    assert error_info["retry_strategy"] == "MANUAL_RETRY_AFTER_REPAIR"
    assert error_info["operator_action"] == "FREE_DISK_SPACE"


def test_markdown_enhancer_reuses_committed_llm_attempt():
    class _FakeClient:
        def __init__(self) -> None:
            self.model = "deepseek-chat"
            self.calls = 0

        async def complete_text(self, *, prompt: str, system_message: str = "", model: str = ""):
            self.calls += 1
            return (
                "复用恢复成功",
                SimpleNamespace(
                    model=model or self.model,
                    prompt_tokens=11,
                    completion_tokens=7,
                    total_tokens=18,
                    cache_hit=False,
                ),
                None,
            )

    enhancer = MarkdownEnhancer.__new__(MarkdownEnhancer)
    enhancer._llm_client = _FakeClient()
    enhancer._structured_text_model = "deepseek-chat"
    task_dir = _make_repo_tmp_dir("test_markdown_enhancer_reuses_committed_llm_attempt") / "task"
    enhancer._runtime_store = RuntimeRecoveryStore(output_dir=str(task_dir), task_id="task-md")
    enhancer._llm_trace_enabled = False
    enhancer._llm_trace_file_path = ""
    enhancer._llm_trace_level = "summary"
    enhancer._llm_trace_lock = asyncio.Lock()

    async def _run_once():
        return await enhancer._execute_recoverable_llm_call(
            step_name="structured_text",
            unit_id="SU200",
            system_prompt="system",
            user_prompt="user",
            model_name="deepseek-chat",
            call_factory=lambda: enhancer._llm_client.complete_text(
                prompt="user",
                system_message="system",
                model="deepseek-chat",
            ),
        )

    first = asyncio.run(_run_once())
    second = asyncio.run(_run_once())

    assert first[0] == "复用恢复成功"
    assert second[0] == "复用恢复成功"
    assert second[2] is True
    assert enhancer._llm_client.calls == 1


def test_vl_generator_restores_committed_screenshot_chunk():
    tmp_path = _make_repo_tmp_dir("test_vl_generator_restores_committed_screenshot_chunk")
    video_path = tmp_path / "video.mp4"
    video_path.write_bytes(b"video")
    output_dir = tmp_path / "task"
    output_dir.mkdir(parents=True, exist_ok=True)

    generator = VLMaterialGenerator({"enabled": True, "screenshot_optimization": {}})
    generator._prepare_runtime_store_for_output_dir(str(output_dir))

    committed_request = {
        "screenshot_id": "shot_001",
        "semantic_unit_id": "SU300",
        "label": "head",
        "timestamp_sec": 9.5,
        "_optimized": True,
        "_original_timestamp": 8.0,
        "_cv_quality_score": 0.88,
        "_cv_candidate_screenshots": [{"timestamp_sec": 9.5, "score": 0.88}],
        "_cv_static_island_threshold_ms": 200.0,
    }
    committed_chunk = {
        "union_start": 7.0,
        "union_end": 10.0,
        "prefetch_profile": "default",
        "prefetch_sample_rate": 2,
        "prefetch_target_height": 360,
        "max_chunk_span_seconds": 3.0,
        "windows": [{"req": committed_request}],
    }
    generator._commit_screenshot_chunk_runtime(
        video_path=str(video_path),
        mode="batch",
        chunk_index=0,
        chunk=committed_chunk,
    )

    restored_generator = VLMaterialGenerator({"enabled": True, "screenshot_optimization": {}})
    restored_generator._prepare_runtime_store_for_output_dir(str(output_dir))
    pending_request = {
        "screenshot_id": "shot_001",
        "semantic_unit_id": "SU300",
        "label": "head",
        "timestamp_sec": 8.0,
    }
    pending_chunk = {
        "union_start": 7.0,
        "union_end": 10.0,
        "prefetch_profile": "default",
        "prefetch_sample_rate": 2,
        "prefetch_target_height": 360,
        "max_chunk_span_seconds": 3.0,
        "windows": [{"req": pending_request}],
    }

    restored = restored_generator._restore_screenshot_chunk_if_committed(
        video_path=str(video_path),
        mode="batch",
        chunk_index=0,
        chunk=pending_chunk,
    )

    assert restored is True
    assert pending_request["timestamp_sec"] == 9.5
    assert pending_request["_optimized"] is True
