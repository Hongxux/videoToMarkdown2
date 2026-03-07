import math

from services.python_grpc.src.content_pipeline.infra.llm.token_costing import (
    build_token_cost_estimate,
    normalize_usage_payload,
    summarize_token_cost_records,
)


def test_normalize_usage_payload_reads_cache_hit_and_miss_tokens():
    usage = {
        "prompt_tokens": 1000,
        "completion_tokens": 200,
        "total_tokens": 1200,
        "prompt_tokens_details": {"cached_tokens": 700},
        "prompt_cache_miss_tokens": 300,
    }

    normalized = normalize_usage_payload(usage)

    assert normalized["prompt_tokens"] == 1000
    assert normalized["completion_tokens"] == 200
    assert normalized["total_tokens"] == 1200
    assert normalized["prompt_cache_hit_tokens"] == 700
    assert normalized["prompt_cache_miss_tokens"] == 300


def test_build_token_cost_estimate_deepseek_supports_offpeak_and_cached_prompt_tokens():
    estimate = build_token_cost_estimate(
        usage={
            "prompt_tokens": 1000,
            "completion_tokens": 500,
            "total_tokens": 1500,
            "prompt_cache_hit_tokens": 400,
            "prompt_cache_miss_tokens": 600,
        },
        model="deepseek-chat",
        timestamp_utc="2026-03-06T17:00:00+00:00",
        local_cache_hit=False,
    )

    assert estimate["status"] == "ok"
    assert estimate["pricing_mode"] == "offpeak_half_price"
    assert estimate["cached_prompt_tokens"] == 400
    assert estimate["uncached_prompt_tokens"] == 600
    assert estimate["currency"] == "CNY"
    assert math.isclose(float(estimate["total_cost"]), 0.00264, rel_tol=1e-9)


def test_summarize_token_cost_records_rolls_up_currency_totals():
    summary = summarize_token_cost_records(
        [
            {
                "model": "deepseek-chat",
                "token_usage": {
                    "prompt_tokens": 100,
                    "completion_tokens": 50,
                    "total_tokens": 150,
                    "prompt_cache_hit_tokens": 20,
                    "prompt_cache_miss_tokens": 80,
                },
                "cost_estimate": {
                    "status": "ok",
                    "provider": "deepseek",
                    "resolved_model_key": "deepseek-chat",
                    "currency": "CNY",
                    "total_cost": 0.001,
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
    assert math.isclose(float(summary["estimated_cost_by_currency"]["CNY"]), 0.001, rel_tol=1e-9)
