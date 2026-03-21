import asyncio
import json

from services.python_grpc.src.content_pipeline.infra.llm.deepseek_audit import (
    build_phase2b_audit_context,
    push_deepseek_audit_context,
    pop_deepseek_audit_context,
    append_deepseek_call_record,
)
from services.python_grpc.src.content_pipeline.infra.llm import llm_gateway


class _FakeGatewayClient:
    async def complete_text(self, prompt: str, system_message: str = None, need_logprobs: bool = False):
        class _Meta:
            model = "fake-deepseek"
            prompt_tokens = 11
            completion_tokens = 7
            total_tokens = 18
            latency_ms = 12.5

        return "补全文本", _Meta(), None


def test_append_deepseek_call_record_writes_input_output_pair(tmp_path):
    ctx = build_phase2b_audit_context(
        output_dir=str(tmp_path),
        task_id="task-demo",
        video_path="demo.mp4",
        enabled=True,
    )
    token = push_deepseek_audit_context(ctx)
    try:
        append_deepseek_call_record(
            prompt="请基于图片描述做增量补全",
            system_message="你是教学文本补全助手。",
            model="deepseek-chat",
            temperature=0.3,
            need_logprobs=False,
            output_text="这是补全后的正文",
            metadata={"total_tokens": 10},
        )
    finally:
        pop_deepseek_audit_context(token)

    audit_path = tmp_path / "intermediates" / "phase2b_deepseek_call_audit.json"
    payload = json.loads(audit_path.read_text(encoding="utf-8"))

    assert payload["total_calls"] == 1
    assert payload["overview"]["total_calls"] == 1
    assert payload["overview"]["failed_calls"] == 0
    assert len(payload["records"]) == 1
    assert len(payload["compact_records"]) == 1
    assert any(item["kind"] == "system_message" for item in payload["shared_texts"])
    assert audit_path.with_suffix(".md").exists()

    record = payload["records"][0]
    assert record["input"]["model"] == "deepseek-chat"
    assert "增量补全" in record["input"]["prompt"]
    assert record["output"]["success"] is True
    assert "补全后的正文" in record["output"]["content"]


def test_append_deepseek_call_record_skips_non_img_desc_when_filter_enabled(tmp_path):
    ctx = build_phase2b_audit_context(
        output_dir=str(tmp_path),
        enabled=True,
        only_img_desc_augment=True,
    )
    token = push_deepseek_audit_context(ctx)
    try:
        append_deepseek_call_record(
            prompt="普通文本增强",
            system_message="你是写作助手。",
            model="deepseek-chat",
            temperature=0.2,
            need_logprobs=False,
            output_text="普通输出",
            metadata=None,
        )
    finally:
        pop_deepseek_audit_context(token)

    audit_path = tmp_path / "intermediates" / "phase2b_deepseek_call_audit.json"
    payload = json.loads(audit_path.read_text(encoding="utf-8"))
    assert payload["total_calls"] == 0
    assert payload["records"] == []


def test_deepseek_gateway_call_is_audited(tmp_path):
    ctx = build_phase2b_audit_context(
        output_dir=str(tmp_path),
        enabled=True,
    )
    token = push_deepseek_audit_context(ctx)
    try:
        asyncio.run(
            llm_gateway.deepseek_complete_text(
                prompt="结合图片描述补全操作说明",
                system_message="你是教学文本补全助手。",
                client=_FakeGatewayClient(),
                model="deepseek-chat",
                temperature=0.3,
            )
        )
    finally:
        pop_deepseek_audit_context(token)

    audit_path = tmp_path / "intermediates" / "phase2b_deepseek_call_audit.json"
    payload = json.loads(audit_path.read_text(encoding="utf-8"))
    assert payload["total_calls"] == 1
    assert payload["records"][0]["step_name"] == "img_desc_augment"


def test_deepseek_audit_writes_token_cost_summary(tmp_path):
    ctx = build_phase2b_audit_context(
        output_dir=str(tmp_path),
        task_id="task-cost",
        video_path="demo.mp4",
        enabled=True,
        only_img_desc_augment=False,
    )
    token = push_deepseek_audit_context(ctx)
    try:
        append_deepseek_call_record(
            prompt="img description augment request",
            system_message="You are an image description augmentation assistant.",
            model="deepseek-chat",
            temperature=0.3,
            need_logprobs=False,
            output_text="augmented body text",
            metadata={
                "prompt_tokens": 1000,
                "completion_tokens": 500,
                "total_tokens": 1500,
                "prompt_cache_hit_tokens": 400,
                "prompt_cache_miss_tokens": 600,
            },
        )
    finally:
        pop_deepseek_audit_context(token)

    audit_path = tmp_path / "intermediates" / "phase2b_deepseek_call_audit.json"
    payload = json.loads(audit_path.read_text(encoding="utf-8"))

    assert payload["summary"]["total_records"] == 1
    assert payload["summary"]["priced_records"] == 1
    assert payload["records"][0]["token_usage"]["prompt_cache_hit_tokens"] == 400
    assert payload["records"][0]["cost_estimate"]["status"] == "ok"
    assert payload["records"][0]["cost_estimate"]["currency"] == "CNY"


def test_deepseek_audit_uses_actual_qwen_model_for_fallback_pricing(tmp_path):
    ctx = build_phase2b_audit_context(
        output_dir=str(tmp_path),
        task_id="task-qwen-fallback",
        enabled=True,
        only_img_desc_augment=False,
    )
    token = push_deepseek_audit_context(ctx)
    try:
        append_deepseek_call_record(
            prompt="img description augment request",
            system_message="You are an image description augmentation assistant.",
            model="deepseek-chat",
            temperature=0.2,
            need_logprobs=False,
            output_text="fallback output",
            metadata={
                "model": "qwen-plus",
                "prompt_tokens": 800,
                "completion_tokens": 200,
                "total_tokens": 1000,
            },
        )
    finally:
        pop_deepseek_audit_context(token)

    audit_path = tmp_path / "intermediates" / "phase2b_deepseek_call_audit.json"
    payload = json.loads(audit_path.read_text(encoding="utf-8"))
    record = payload["records"][0]

    assert record["input"]["model"] == "qwen-plus"
    assert record["input"]["requested_model"] == "deepseek-chat"
    assert record["cost_estimate"]["resolved_model_key"] == "qwen-plus"
    assert record["cost_estimate"]["currency"] == "CNY"
    assert record["cost_estimate"]["total_cost"] > 0
    assert payload["overview"]["failed_calls"] == 1
    assert any(item["type"] == "failed_calls" for item in payload["problem_summary"])
