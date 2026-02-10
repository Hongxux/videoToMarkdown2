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
    assert len(payload["records"]) == 1

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

