import json
import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

from services.python_grpc.src.common.utils.runtime_llm_context import (
    activate_runtime_llm_context,
    build_runtime_llm_request_payload,
)
from services.python_grpc.src.transcript_pipeline.stage1_projection_repository import Stage1ProjectionRepository


def _make_repo_tmp_dir(test_name: str) -> Path:
    repo_root = Path(__file__).resolve().parents[5]
    base = repo_root / "var" / "tmp_stage1_projection_tests"
    base.mkdir(parents=True, exist_ok=True)
    safe_name = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in test_name)
    unique_suffix = f"{time.time_ns() % 1_000_000:06d}"
    path = base / f"{safe_name[:24]}_{unique_suffix}"
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _persist_stage1_call(
    runtime_context,
    *,
    prompt: str,
    stage_step: str,
    unit_id: str,
    response_payload: dict,
) -> None:
    request_payload = build_runtime_llm_request_payload(
        model="deepseek-chat",
        prompt=prompt,
        system_prompt="只返回 JSON",
        kwargs={"response_format": {"type": "json_object"}},
    )
    runtime_context.persist_success(
        provider="deepseek",
        request_name="complete_json",
        request_payload=request_payload,
        response_text=json.dumps(response_payload, ensure_ascii=False),
        response_metadata={
            "model": "deepseek-chat",
            "prompt_tokens": 1,
            "completion_tokens": 1,
            "total_tokens": 2,
            "latency_ms": 1.0,
        },
        runtime_identity={
            "step_name": f"stage1_{stage_step}",
            "request_name": "complete_json",
            "unit_id": unit_id,
            "llm_call_id": f"stage1_{stage_step}.{unit_id}",
        },
        metadata={
            "stage_step": stage_step,
            "scope_variant": unit_id,
            "unit_id": unit_id,
        },
    )


def test_stage1_projection_repository_restores_runtime_outputs_from_sqlite():
    tmp_path = _make_repo_tmp_dir("stage1_projection_restore")
    subtitle_path = tmp_path / "subtitles.json"
    subtitle_path.write_text(
        json.dumps(
            [
                {"subtitle_id": "SUB001", "text": "hello", "start_sec": 0.0, "end_sec": 1.0},
                {"subtitle_id": "SUB002", "text": "world", "start_sec": 1.0, "end_sec": 2.0},
                {"subtitle_id": "SUB003", "text": "中文句子", "start_sec": 2.0, "end_sec": 3.0},
            ],
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    with activate_runtime_llm_context(
        stage="stage1",
        output_dir=str(tmp_path),
        task_id="task_stage1_projection",
        storage_key="task_stage1_projection",
        storage_backend="sqlite",
    ) as runtime_context:
        runtime_context.store.update_stage_state(
            stage="stage1",
            status="completed",
            payload={
                "checkpoint": "stage1_response_ready",
                "subtitle_path": str(subtitle_path),
                "domain": "technology",
                "main_topic": "runtime projection",
            },
        )
        _persist_stage1_call(
            runtime_context,
            prompt="请识别主题\n[topic_inference] hello world 中文句子",
            stage_step="step1_validate",
            unit_id="topic_inference",
            response_payload={"domain": "technology", "main_topic": "runtime projection"},
        )
        _persist_stage1_call(
            runtime_context,
            prompt="[SUB001] hello\n[SUB002] world\n[SUB003] 中文句子",
            stage_step="step2_correction",
            unit_id="batch_0001",
            response_payload={
                "corrections": [
                    {
                        "subtitle_id": "SUB001",
                        "original": "hello",
                        "corrected": "Hello",
                        "left_context": "",
                        "right_context": "",
                    }
                ]
            },
        )
        _persist_stage1_call(
            runtime_context,
            prompt="[SUB001] Hello\n[SUB002] world\n[SUB003] 中文句子",
            stage_step="step3_merge",
            unit_id="window_0001",
            response_payload={
                "merged_sentences": [
                    {
                        "text": "Hello world",
                        "source_subtitle_ids": ["SUB001", "SUB002"],
                    }
                ]
            },
        )
        _persist_stage1_call(
            runtime_context,
            prompt="[S001] Hello world",
            stage_step="step3_5_translate",
            unit_id="window_0001",
            response_payload={
                "translated_sentences": [
                    {
                        "sentence_id": "S001",
                        "translated_text": "你好世界",
                    }
                ]
            },
        )
        _persist_stage1_call(
            runtime_context,
            prompt="[S001] 你好世界\n[S002] 中文句子",
            stage_step="step5_6_dedup_merge",
            unit_id="window_0001",
            response_payload={
                "keep_sentence_ids": ["S001", "S002"],
                "paragraphs": [
                    {
                        "text": "你好世界 中文句子",
                        "source_sentence_ids": ["S001", "S002"],
                        "merge_type": "merge",
                    }
                ],
            },
        )

    projected = Stage1ProjectionRepository(output_dir=str(tmp_path)).load_projected_state()

    assert projected is not None
    assert projected["domain"] == "technology"
    assert projected["main_topic"] == "runtime projection"
    assert [item["corrected_text"] for item in projected["corrected_subtitles"]] == [
        "Hello",
        "world",
        "中文句子",
    ]
    assert [item["sentence_id"] for item in projected["merged_sentences"]] == ["S001", "S002"]
    assert projected["translated_sentences"][0]["text"] == "你好世界"
    assert projected["translated_sentences"][1]["text"] == "中文句子"
    assert projected["cleaned_sentences"][0]["cleaned_text"] == "你好世界"
    assert projected["non_redundant_sentences"][0]["sentence_id"] == "S001"
    assert projected["pure_text_script"][0]["source_sentence_ids"] == ["S001", "S002"]
    assert projected["sentence_timestamps"] == {
        "S001": {"start_sec": 0.0, "end_sec": 2.0},
        "S002": {"start_sec": 2.0, "end_sec": 3.0},
    }
