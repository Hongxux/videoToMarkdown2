import json

from services.python_grpc.src.common.utils.runtime_llm_context import activate_runtime_llm_context
from services.python_grpc.src.content_pipeline.infra.llm import llm_gateway


class _FakeVisionClient:
    async def validate_image(
        self,
        image_path: str,
        prompt: str = "",
        system_prompt: str = None,
        skip_duplicate_check: bool = False,
    ):
        _ = (image_path, prompt, system_prompt, skip_duplicate_check)
        return {
            "has_concrete_knowledge": True,
            "should_include": True,
            "__llm_response_metadata": {
                "model": "qwen-vl-max-latest",
                "prompt_tokens": 305,
                "completion_tokens": 20,
                "total_tokens": 325,
                "latency_ms": 321.5,
                "usage_details": {
                    "prompt_tokens": 305,
                    "completion_tokens": 20,
                    "total_tokens": 325,
                    "prompt_tokens_details": {
                        "image_tokens": 266,
                        "text_tokens": 39,
                    },
                },
            },
        }


def test_vision_gateway_writes_task_audit_and_strips_internal_metadata(tmp_path):
    with activate_runtime_llm_context(
        stage="phase2a",
        output_dir=str(tmp_path),
        task_id="task-vision-audit",
    ):
        result = llm_gateway.vision_validate_image_sync(
            image_path="demo.png",
            prompt="describe image",
            client=_FakeVisionClient(),
            timeout=3.0,
        )

    assert "__llm_response_metadata" not in result
    assert result["has_concrete_knowledge"] is True

    audit_path = tmp_path / "intermediates" / "vision_ai_call_audit.json"
    payload = json.loads(audit_path.read_text(encoding="utf-8"))

    assert payload["total_calls"] == 1
    assert payload["summary"]["priced_records"] == 1
    assert payload["summary"]["by_model"][0]["model"] == "qwen-vl-max-latest"
    assert payload["summary"]["estimated_cost_by_currency"]["CNY"] > 0
