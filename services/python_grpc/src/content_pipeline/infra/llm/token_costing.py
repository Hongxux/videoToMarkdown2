"""Token pricing snapshots, usage normalization, and cost rollups."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional


_PRICING_CHECKED_AT = "2026-03-15"
_PRICING_SNAPSHOT_VERSION = "2026-03-15"


@dataclass(frozen=True)
class TokenPricing:
    """Pricing snapshot for one model."""

    provider: str
    model_key: str
    currency: str
    input_per_million: float
    output_per_million: float
    cached_input_per_million: Optional[float] = None
    offpeak_input_per_million: Optional[float] = None
    offpeak_output_per_million: Optional[float] = None
    offpeak_cached_input_per_million: Optional[float] = None
    source_url: str = ""
    source_title: str = ""
    checked_at: str = _PRICING_CHECKED_AT


_MODEL_PRICING: Dict[str, TokenPricing] = {
    "deepseek-chat": TokenPricing(
        provider="deepseek",
        model_key="deepseek-chat",
        currency="CNY",
        input_per_million=2.0,
        output_per_million=3.0,
        cached_input_per_million=0.2,
        source_url="https://api-docs.deepseek.com/quick_start/pricing",
        source_title="DeepSeek API Pricing",
    ),
    "qwen-plus": TokenPricing(
        provider="dashscope",
        model_key="qwen-plus",
        currency="CNY",
        input_per_million=0.8,
        output_per_million=2.0,
        source_url="https://help.aliyun.com/zh/model-studio/getting-started/models",
        source_title="Alibaba Cloud Model Studio Pricing",
    ),
    "qwen-vl-max-latest": TokenPricing(
        provider="dashscope",
        model_key="qwen-vl-max-latest",
        currency="CNY",
        input_per_million=1.6,
        output_per_million=4.0,
        source_url="https://help.aliyun.com/zh/model-studio/getting-started/models",
        source_title="Alibaba Cloud Model Studio Pricing",
    ),
    "qwen-vl-max-2025-08-13": TokenPricing(
        provider="dashscope",
        model_key="qwen-vl-max-2025-08-13",
        currency="CNY",
        input_per_million=1.6,
        output_per_million=4.0,
        source_url="https://help.aliyun.com/zh/model-studio/getting-started/models",
        source_title="Alibaba Cloud Model Studio Pricing",
    ),
}

_MODEL_ALIASES: Dict[str, str] = {
    "qwen-plus-latest": "qwen-plus",
    "qwen-vl-max": "qwen-vl-max-latest",
}


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _normalize_model_key(model: Any) -> str:
    raw = str(model or "").strip().lower()
    if not raw:
        return ""
    return _MODEL_ALIASES.get(raw, raw)


def _infer_provider(*, model: Any = None, base_url: Any = None) -> str:
    model_key = _normalize_model_key(model)
    base_url_text = str(base_url or "").strip().lower()
    if model_key.startswith("deepseek") or "deepseek" in base_url_text:
        return "deepseek"
    if model_key.startswith("qwen") or "dashscope" in base_url_text or "aliyuncs.com" in base_url_text:
        return "dashscope"
    if model_key.startswith("gpt") or "openai.com" in base_url_text:
        return "openai"
    return ""


def _coerce_mapping(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if value is None:
        return {}

    result: Dict[str, Any] = {}
    for key in (
        "prompt_tokens",
        "completion_tokens",
        "total_tokens",
        "cached_tokens",
        "prompt_cache_hit_tokens",
        "prompt_cache_miss_tokens",
        "input_tokens",
        "output_tokens",
        "cache_hit",
        "prompt_tokens_details",
        "input_tokens_details",
        "completion_tokens_details",
        "output_tokens_details",
        "usage_details",
        "text_tokens",
        "image_tokens",
        "audio_tokens",
        "video_tokens",
    ):
        attr = getattr(value, key, None)
        if attr is not None:
            result[key] = attr
    return result


def _copy_detail_mapping(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    return _coerce_mapping(value)


def _detail_token_value(payload: Dict[str, Any], *keys: str) -> int:
    for key in keys:
        if key in payload:
            return max(0, _as_int(payload.get(key, 0)))
    return 0


def normalize_usage_payload(usage: Any) -> Dict[str, Any]:
    """Normalize provider usage into one audit-friendly payload."""

    payload = _coerce_mapping(usage)
    usage_details = _copy_detail_mapping(payload.get("usage_details"))
    prompt_tokens_details = _copy_detail_mapping(payload.get("prompt_tokens_details"))
    input_tokens_details = _copy_detail_mapping(payload.get("input_tokens_details"))
    if not input_tokens_details and prompt_tokens_details:
        input_tokens_details = dict(prompt_tokens_details)

    completion_tokens_details = _copy_detail_mapping(payload.get("completion_tokens_details"))
    output_tokens_details = _copy_detail_mapping(payload.get("output_tokens_details"))
    if not output_tokens_details and completion_tokens_details:
        output_tokens_details = dict(completion_tokens_details)

    prompt_tokens = _as_int(
        payload.get(
            "prompt_tokens",
            usage_details.get("prompt_tokens", payload.get("input_tokens", usage_details.get("input_tokens", 0))),
        )
    )
    completion_tokens = _as_int(
        payload.get(
            "completion_tokens",
            usage_details.get(
                "completion_tokens",
                payload.get("output_tokens", usage_details.get("output_tokens", 0)),
            ),
        )
    )
    total_tokens = _as_int(
        payload.get(
            "total_tokens",
            usage_details.get("total_tokens", prompt_tokens + completion_tokens),
        )
    )

    cached_tokens = max(
        0,
        _as_int(prompt_tokens_details.get("cached_tokens", 0)),
        _as_int(input_tokens_details.get("cached_tokens", 0)),
        _as_int(payload.get("cached_tokens", 0)),
        _as_int(usage_details.get("cached_tokens", 0)),
        _as_int(payload.get("prompt_cache_hit_tokens", 0)),
        _as_int(usage_details.get("prompt_cache_hit_tokens", 0)),
    )
    prompt_cache_hit_tokens = max(
        0,
        _as_int(payload.get("prompt_cache_hit_tokens", usage_details.get("prompt_cache_hit_tokens", cached_tokens))),
        cached_tokens,
    )
    prompt_cache_miss_tokens = max(
        0,
        _as_int(
            payload.get(
                "prompt_cache_miss_tokens",
                usage_details.get("prompt_cache_miss_tokens", max(0, prompt_tokens - prompt_cache_hit_tokens)),
            )
        ),
    )
    if prompt_tokens > 0 and prompt_cache_hit_tokens + prompt_cache_miss_tokens != prompt_tokens:
        prompt_cache_miss_tokens = max(0, prompt_tokens - prompt_cache_hit_tokens)

    text_input_tokens = max(
        _detail_token_value(input_tokens_details, "text_tokens"),
        _detail_token_value(prompt_tokens_details, "text_tokens"),
        _detail_token_value(usage_details, "text_tokens"),
        _detail_token_value(payload, "text_tokens"),
    )
    image_input_tokens = max(
        _detail_token_value(input_tokens_details, "image_tokens"),
        _detail_token_value(prompt_tokens_details, "image_tokens"),
        _detail_token_value(usage_details, "image_tokens"),
        _detail_token_value(payload, "image_tokens"),
    )
    audio_input_tokens = max(
        _detail_token_value(input_tokens_details, "audio_tokens"),
        _detail_token_value(prompt_tokens_details, "audio_tokens"),
        _detail_token_value(usage_details, "audio_tokens"),
        _detail_token_value(payload, "audio_tokens"),
    )
    video_input_tokens = max(
        _detail_token_value(input_tokens_details, "video_tokens"),
        _detail_token_value(prompt_tokens_details, "video_tokens"),
        _detail_token_value(usage_details, "video_tokens"),
        _detail_token_value(payload, "video_tokens"),
    )
    media_input_tokens = max(0, image_input_tokens + audio_input_tokens + video_input_tokens)

    return {
        "prompt_tokens": max(0, prompt_tokens),
        "completion_tokens": max(0, completion_tokens),
        "total_tokens": max(0, total_tokens),
        "input_tokens": max(0, prompt_tokens),
        "output_tokens": max(0, completion_tokens),
        "cached_tokens": max(0, cached_tokens),
        "prompt_cache_hit_tokens": max(0, prompt_cache_hit_tokens),
        "prompt_cache_miss_tokens": max(0, prompt_cache_miss_tokens),
        "text_input_tokens": max(0, text_input_tokens),
        "image_input_tokens": max(0, image_input_tokens),
        "audio_input_tokens": max(0, audio_input_tokens),
        "video_input_tokens": max(0, video_input_tokens),
        "media_input_tokens": max(0, media_input_tokens),
        "prompt_tokens_details": prompt_tokens_details,
        "input_tokens_details": input_tokens_details,
        "completion_tokens_details": completion_tokens_details,
        "output_tokens_details": output_tokens_details,
        "usage_details": usage_details,
        "cache_hit": bool(payload.get("cache_hit", usage_details.get("cache_hit", False))),
    }


def _resolve_pricing(*, model: Any, provider: Any = None, base_url: Any = None) -> Optional[TokenPricing]:
    model_key = _normalize_model_key(model)
    pricing = _MODEL_PRICING.get(model_key)
    if pricing is not None:
        return pricing

    inferred_provider = _infer_provider(model=model_key, base_url=base_url)
    if provider and str(provider).strip().lower() != inferred_provider:
        return None
    return None


def build_token_cost_estimate(
    *,
    usage: Any,
    model: Any,
    provider: Any = None,
    base_url: Any = None,
    timestamp_utc: Any = None,
    local_cache_hit: bool = False,
) -> Dict[str, Any]:
    """Build one cost estimate using the current pricing snapshot."""

    del timestamp_utc

    usage_payload = normalize_usage_payload(usage)
    pricing = _resolve_pricing(model=model, provider=provider, base_url=base_url)
    model_key = _normalize_model_key(model)
    inferred_provider = _infer_provider(model=model_key, base_url=base_url)

    prompt_tokens = int(usage_payload.get("prompt_tokens", 0) or 0)
    completion_tokens = int(usage_payload.get("completion_tokens", 0) or 0)
    total_tokens = int(usage_payload.get("total_tokens", prompt_tokens + completion_tokens) or 0)
    cached_prompt_tokens = int(usage_payload.get("prompt_cache_hit_tokens", 0) or 0)
    uncached_prompt_tokens = int(
        usage_payload.get("prompt_cache_miss_tokens", max(0, prompt_tokens - cached_prompt_tokens)) or 0
    )
    text_input_tokens = int(usage_payload.get("text_input_tokens", 0) or 0)
    image_input_tokens = int(usage_payload.get("image_input_tokens", 0) or 0)
    audio_input_tokens = int(usage_payload.get("audio_input_tokens", 0) or 0)
    video_input_tokens = int(usage_payload.get("video_input_tokens", 0) or 0)
    media_input_tokens = int(usage_payload.get("media_input_tokens", 0) or 0)

    base_result = {
        "provider": inferred_provider if pricing is None else pricing.provider,
        "model": str(model or ""),
        "resolved_model_key": model_key if pricing is None else pricing.model_key,
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": total_tokens,
        "input_tokens_billed": int(usage_payload.get("input_tokens", prompt_tokens) or 0),
        "output_tokens_billed": int(usage_payload.get("output_tokens", completion_tokens) or 0),
        "cached_prompt_tokens": cached_prompt_tokens,
        "uncached_prompt_tokens": uncached_prompt_tokens,
        "text_input_tokens": text_input_tokens,
        "image_input_tokens": image_input_tokens,
        "audio_input_tokens": audio_input_tokens,
        "video_input_tokens": video_input_tokens,
        "media_input_tokens": media_input_tokens,
        "billing_input_token_field": "prompt_tokens",
        "prompt_tokens_include_media": bool(model_key.startswith("qwen-vl") or media_input_tokens > 0),
    }

    if bool(local_cache_hit):
        return {
            "status": "app_cache_hit",
            "currency": pricing.currency if pricing is not None else "",
            "pricing_mode": "app_cache_hit",
            "input_cost": 0.0,
            "cached_input_cost": 0.0,
            "output_cost": 0.0,
            "total_cost": 0.0,
            "source_url": pricing.source_url if pricing is not None else "",
            "source_title": pricing.source_title if pricing is not None else "",
            "checked_at": pricing.checked_at if pricing is not None else _PRICING_CHECKED_AT,
            **base_result,
        }

    if pricing is None:
        return {
            "status": "unsupported_model",
            "currency": "",
            "pricing_mode": "unsupported",
            "input_cost": None,
            "cached_input_cost": None,
            "output_cost": None,
            "total_cost": None,
            "source_url": "",
            "source_title": "",
            "checked_at": _PRICING_CHECKED_AT,
            **base_result,
        }

    pricing_mode = "standard"
    input_rate = float(pricing.input_per_million)
    output_rate = float(pricing.output_per_million)
    cached_input_rate = (
        float(pricing.cached_input_per_million) if pricing.cached_input_per_million is not None else input_rate
    )

    input_cost = float(uncached_prompt_tokens) * input_rate / 1_000_000.0
    cached_input_cost = float(cached_prompt_tokens) * cached_input_rate / 1_000_000.0
    output_cost = float(completion_tokens) * output_rate / 1_000_000.0
    total_cost = input_cost + cached_input_cost + output_cost

    return {
        "status": "ok",
        "currency": pricing.currency,
        "pricing_mode": pricing_mode,
        "input_rate_per_million": input_rate,
        "cached_input_rate_per_million": cached_input_rate,
        "output_rate_per_million": output_rate,
        "input_cost": round(input_cost, 12),
        "cached_input_cost": round(cached_input_cost, 12),
        "output_cost": round(output_cost, 12),
        "total_cost": round(total_cost, 12),
        "source_url": pricing.source_url,
        "source_title": pricing.source_title,
        "checked_at": pricing.checked_at,
        **base_result,
    }


def get_token_pricing_snapshot() -> Dict[str, Any]:
    """Return the current embedded pricing snapshot."""

    models = []
    sources: Dict[str, Dict[str, Any]] = {}
    for pricing in _MODEL_PRICING.values():
        models.append(
            {
                "provider": pricing.provider,
                "model": pricing.model_key,
                "currency": pricing.currency,
                "input_per_million": pricing.input_per_million,
                "cached_input_per_million": pricing.cached_input_per_million,
                "output_per_million": pricing.output_per_million,
                "offpeak_input_per_million": pricing.offpeak_input_per_million,
                "offpeak_cached_input_per_million": pricing.offpeak_cached_input_per_million,
                "offpeak_output_per_million": pricing.offpeak_output_per_million,
                "checked_at": pricing.checked_at,
                "source_url": pricing.source_url,
                "source_title": pricing.source_title,
            }
        )
        if pricing.source_url and pricing.source_url not in sources:
            sources[pricing.source_url] = {
                "source_url": pricing.source_url,
                "source_title": pricing.source_title,
                "checked_at": pricing.checked_at,
            }
    return {
        "version": _PRICING_SNAPSHOT_VERSION,
        "checked_at": _PRICING_CHECKED_AT,
        "sources": list(sources.values()),
        "models": models,
    }


def summarize_token_cost_records(records: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    """Roll up canonical token-cost records."""

    total_records = 0
    priced_records = 0
    app_cache_hit_records = 0
    unsupported_records = 0
    total_prompt_tokens = 0
    total_completion_tokens = 0
    total_tokens = 0
    total_input_tokens = 0
    total_output_tokens = 0
    total_cached_prompt_tokens = 0
    total_uncached_prompt_tokens = 0
    total_text_input_tokens = 0
    total_image_input_tokens = 0
    total_audio_input_tokens = 0
    total_video_input_tokens = 0
    total_media_input_tokens = 0
    currency_totals: Dict[str, float] = {}
    model_totals: Dict[str, Dict[str, Any]] = {}

    for record in records or []:
        total_records += 1
        usage = normalize_usage_payload(record.get("token_usage", {}))
        cost_estimate = dict(record.get("cost_estimate", {}) or {})

        total_prompt_tokens += int(usage.get("prompt_tokens", 0) or 0)
        total_completion_tokens += int(usage.get("completion_tokens", 0) or 0)
        total_tokens += int(usage.get("total_tokens", 0) or 0)
        total_input_tokens += int(usage.get("input_tokens", usage.get("prompt_tokens", 0)) or 0)
        total_output_tokens += int(usage.get("output_tokens", usage.get("completion_tokens", 0)) or 0)
        total_cached_prompt_tokens += int(usage.get("prompt_cache_hit_tokens", 0) or 0)
        total_uncached_prompt_tokens += int(usage.get("prompt_cache_miss_tokens", 0) or 0)
        total_text_input_tokens += int(usage.get("text_input_tokens", 0) or 0)
        total_image_input_tokens += int(usage.get("image_input_tokens", 0) or 0)
        total_audio_input_tokens += int(usage.get("audio_input_tokens", 0) or 0)
        total_video_input_tokens += int(usage.get("video_input_tokens", 0) or 0)
        total_media_input_tokens += int(usage.get("media_input_tokens", 0) or 0)

        status = str(cost_estimate.get("status", "") or "")
        if status == "ok":
            priced_records += 1
            currency = str(cost_estimate.get("currency", "") or "")
            total_cost = cost_estimate.get("total_cost")
            if currency and total_cost is not None:
                currency_totals[currency] = round(float(currency_totals.get(currency, 0.0)) + float(total_cost), 12)
        elif status == "app_cache_hit":
            app_cache_hit_records += 1
        elif status == "unsupported_model":
            unsupported_records += 1

        model_key = str(cost_estimate.get("resolved_model_key", record.get("model", "")) or "")
        if not model_key:
            continue

        bucket = model_totals.setdefault(
            model_key,
            {
                "model": model_key,
                "provider": str(cost_estimate.get("provider", record.get("provider", "")) or ""),
                "currency": str(cost_estimate.get("currency", "") or ""),
                "records": 0,
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
                "input_tokens": 0,
                "output_tokens": 0,
                "cached_prompt_tokens": 0,
                "uncached_prompt_tokens": 0,
                "text_input_tokens": 0,
                "image_input_tokens": 0,
                "audio_input_tokens": 0,
                "video_input_tokens": 0,
                "media_input_tokens": 0,
                "estimated_total_cost": 0.0,
            },
        )
        bucket["records"] += 1
        bucket["prompt_tokens"] += int(usage.get("prompt_tokens", 0) or 0)
        bucket["completion_tokens"] += int(usage.get("completion_tokens", 0) or 0)
        bucket["total_tokens"] += int(usage.get("total_tokens", 0) or 0)
        bucket["input_tokens"] += int(usage.get("input_tokens", usage.get("prompt_tokens", 0)) or 0)
        bucket["output_tokens"] += int(usage.get("output_tokens", usage.get("completion_tokens", 0)) or 0)
        bucket["cached_prompt_tokens"] += int(usage.get("prompt_cache_hit_tokens", 0) or 0)
        bucket["uncached_prompt_tokens"] += int(usage.get("prompt_cache_miss_tokens", 0) or 0)
        bucket["text_input_tokens"] += int(usage.get("text_input_tokens", 0) or 0)
        bucket["image_input_tokens"] += int(usage.get("image_input_tokens", 0) or 0)
        bucket["audio_input_tokens"] += int(usage.get("audio_input_tokens", 0) or 0)
        bucket["video_input_tokens"] += int(usage.get("video_input_tokens", 0) or 0)
        bucket["media_input_tokens"] += int(usage.get("media_input_tokens", 0) or 0)
        if cost_estimate.get("total_cost") is not None:
            bucket["estimated_total_cost"] = round(
                float(bucket.get("estimated_total_cost", 0.0)) + float(cost_estimate.get("total_cost", 0.0) or 0.0),
                12,
            )

    return {
        "total_records": total_records,
        "priced_records": priced_records,
        "app_cache_hit_records": app_cache_hit_records,
        "unsupported_records": unsupported_records,
        "total_prompt_tokens": total_prompt_tokens,
        "total_completion_tokens": total_completion_tokens,
        "total_tokens": total_tokens,
        "total_input_tokens": total_input_tokens,
        "total_output_tokens": total_output_tokens,
        "total_cached_prompt_tokens": total_cached_prompt_tokens,
        "total_uncached_prompt_tokens": total_uncached_prompt_tokens,
        "total_text_input_tokens": total_text_input_tokens,
        "total_image_input_tokens": total_image_input_tokens,
        "total_audio_input_tokens": total_audio_input_tokens,
        "total_video_input_tokens": total_video_input_tokens,
        "total_media_input_tokens": total_media_input_tokens,
        "estimated_cost_by_currency": currency_totals,
        "by_model": list(model_totals.values()),
    }
