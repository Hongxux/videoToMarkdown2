import math

from services.python_grpc.src.content_pipeline.infra.llm.token_costing import (
    build_token_cost_estimate,
    normalize_usage_payload,
    summarize_token_cost_records,
)


def test_normalize_usage_payload_reads_cache_hit_and_media_breakdown():
    usage = {
        "prompt_tokens": 305,
        "completion_tokens": 20,
        "total_tokens": 325,
        "prompt_tokens_details": {
            "cached_tokens": 0,
            "video_tokens": 266,
            "text_tokens": 39,
        },
        "completion_tokens_details": {"text_tokens": 20},
    }

    normalized = normalize_usage_payload(usage)

    assert normalized["prompt_tokens"] == 305
    assert normalized["completion_tokens"] == 20
    assert normalized["total_tokens"] == 325
    assert normalized["text_input_tokens"] == 39
    assert normalized["video_input_tokens"] == 266
    assert normalized["media_input_tokens"] == 266
    assert normalized["prompt_tokens_details"]["video_tokens"] == 266


def test_build_token_cost_estimate_deepseek_uses_current_standard_rates():
    estimate = build_token_cost_estimate(
        usage={
            "prompt_tokens": 1000,
            "completion_tokens": 500,
            "total_tokens": 1500,
            "prompt_cache_hit_tokens": 400,
            "prompt_cache_miss_tokens": 600,
        },
        model="deepseek-chat",
        timestamp_utc="2026-03-15T10:00:00+08:00",
        local_cache_hit=False,
    )

    assert estimate["status"] == "ok"
    assert estimate["pricing_mode"] == "standard"
    assert estimate["cached_prompt_tokens"] == 400
    assert estimate["uncached_prompt_tokens"] == 600
    assert estimate["currency"] == "CNY"
    assert math.isclose(float(estimate["total_cost"]), 0.00278, rel_tol=1e-9)


def test_build_token_cost_estimate_qwen_vl_keeps_video_breakdown_and_non_zero_cost():
    estimate = build_token_cost_estimate(
        usage={
            "prompt_tokens": 305,
            "completion_tokens": 20,
            "total_tokens": 325,
            "prompt_tokens_details": {
                "video_tokens": 266,
                "text_tokens": 39,
            },
        },
        model="qwen-vl-max-latest",
    )

    assert estimate["status"] == "ok"
    assert estimate["currency"] == "CNY"
    assert estimate["video_input_tokens"] == 266
    assert estimate["text_input_tokens"] == 39
    assert estimate["prompt_tokens_include_media"] is True
    assert math.isclose(float(estimate["total_cost"]), 0.000568, rel_tol=1e-9)


def test_build_token_cost_estimate_qwen_plus_uses_dashscope_text_rates():
    estimate = build_token_cost_estimate(
        usage={
            "prompt_tokens": 800,
            "completion_tokens": 200,
            "total_tokens": 1000,
        },
        model="qwen-plus",
    )

    assert estimate["status"] == "ok"
    assert estimate["currency"] == "CNY"
    assert estimate["resolved_model_key"] == "qwen-plus"
    assert math.isclose(float(estimate["total_cost"]), 0.00104, rel_tol=1e-9)


def test_summarize_token_cost_records_rolls_up_currency_totals_and_media_tokens():
    summary = summarize_token_cost_records(
        [
            {
                "model": "qwen-vl-max-latest",
                "token_usage": {
                    "prompt_tokens": 305,
                    "completion_tokens": 20,
                    "total_tokens": 325,
                    "prompt_tokens_details": {
                        "video_tokens": 266,
                        "text_tokens": 39,
                    },
                },
                "cost_estimate": {
                    "status": "ok",
                    "provider": "dashscope",
                    "resolved_model_key": "qwen-vl-max-latest",
                    "currency": "CNY",
                    "total_cost": 0.000568,
                },
            },
            {
                "model": "deepseek-chat",
                "token_usage": {
                    "prompt_tokens": 100,
                    "completion_tokens": 50,
                    "total_tokens": 150,
                    "prompt_cache_hit_tokens": 0,
                    "prompt_cache_miss_tokens": 100,
                },
                "cost_estimate": {
                    "status": "app_cache_hit",
                    "provider": "deepseek",
                    "resolved_model_key": "deepseek-chat",
                    "currency": "CNY",
                    "total_cost": 0.0,
                },
            },
        ]
    )

    assert summary["total_records"] == 2
    assert summary["priced_records"] == 1
    assert summary["app_cache_hit_records"] == 1
    assert summary["total_video_input_tokens"] == 266
    assert math.isclose(float(summary["estimated_cost_by_currency"]["CNY"]), 0.000568, rel_tol=1e-9)
    assert summary["by_model"][0]["model"] == "qwen-vl-max-latest"
