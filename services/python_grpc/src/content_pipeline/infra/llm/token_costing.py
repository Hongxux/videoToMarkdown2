"""LLM token 成本估算与审计工具。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional
from zoneinfo import ZoneInfo


_PRICING_CHECKED_AT = "2026-03-07"
_PRICING_SNAPSHOT_VERSION = "2026-03-07"
_SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


@dataclass(frozen=True)
class TokenPricing:
    """描述单个模型的 token 计费快照。"""

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
        output_per_million=8.0,
        cached_input_per_million=0.2,
        offpeak_input_per_million=1.0,
        offpeak_output_per_million=4.0,
        offpeak_cached_input_per_million=0.1,
        source_url="https://api-docs.deepseek.com/quick_start/pricing",
        source_title="DeepSeek API Pricing",
    ),
    "qwen-vl-max-latest": TokenPricing(
        provider="dashscope",
        model_key="qwen-vl-max-latest",
        currency="CNY",
        input_per_million=1.6,
        output_per_million=4.0,
        source_url="https://help.aliyun.com/zh/model-studio/models",
        source_title="阿里云百炼模型价格",
    ),
    "qwen-vl-max-2025-08-13": TokenPricing(
        provider="dashscope",
        model_key="qwen-vl-max-2025-08-13",
        currency="CNY",
        input_per_million=1.6,
        output_per_million=4.0,
        source_url="https://help.aliyun.com/zh/model-studio/models",
        source_title="阿里云百炼模型价格",
    ),
}

_MODEL_ALIASES: Dict[str, str] = {
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
        "usage_details",
    ):
        attr = getattr(value, key, None)
        if attr is not None:
            result[key] = attr
    return result


def normalize_usage_payload(usage: Any) -> Dict[str, Any]:
    """把 usage/metadata 统一归一为可审计结构。"""

    payload = _coerce_mapping(usage)
    usage_details = payload.get("usage_details")
    if isinstance(usage_details, dict):
        details_mapping = dict(usage_details)
    else:
        details_mapping = _coerce_mapping(usage_details)

    prompt_tokens_details_raw = payload.get("prompt_tokens_details")
    if isinstance(prompt_tokens_details_raw, dict):
        prompt_tokens_details = dict(prompt_tokens_details_raw)
    else:
        prompt_tokens_details = _coerce_mapping(prompt_tokens_details_raw)

    prompt_tokens = _as_int(
        payload.get(
            "prompt_tokens",
            details_mapping.get("prompt_tokens", payload.get("input_tokens", details_mapping.get("input_tokens", 0))),
        )
    )
    completion_tokens = _as_int(
        payload.get(
            "completion_tokens",
            details_mapping.get(
                "completion_tokens",
                payload.get("output_tokens", details_mapping.get("output_tokens", 0)),
            ),
        )
    )
    total_tokens = _as_int(
        payload.get(
            "total_tokens",
            details_mapping.get("total_tokens", prompt_tokens + completion_tokens),
        )
    )

    cached_tokens = max(
        0,
        _as_int(prompt_tokens_details.get("cached_tokens", 0)),
        _as_int(payload.get("cached_tokens", 0)),
        _as_int(details_mapping.get("cached_tokens", 0)),
        _as_int(payload.get("prompt_cache_hit_tokens", 0)),
        _as_int(details_mapping.get("prompt_cache_hit_tokens", 0)),
    )
    prompt_cache_hit_tokens = max(
        0,
        _as_int(payload.get("prompt_cache_hit_tokens", details_mapping.get("prompt_cache_hit_tokens", cached_tokens))),
        cached_tokens,
    )
    prompt_cache_miss_tokens = max(
        0,
        _as_int(
            payload.get(
                "prompt_cache_miss_tokens",
                details_mapping.get("prompt_cache_miss_tokens", max(0, prompt_tokens - prompt_cache_hit_tokens)),
            )
        ),
    )
    if prompt_tokens > 0 and prompt_cache_hit_tokens + prompt_cache_miss_tokens != prompt_tokens:
        prompt_cache_miss_tokens = max(0, prompt_tokens - prompt_cache_hit_tokens)

    return {
        "prompt_tokens": max(0, prompt_tokens),
        "completion_tokens": max(0, completion_tokens),
        "total_tokens": max(0, total_tokens),
        "cached_tokens": max(0, cached_tokens),
        "prompt_cache_hit_tokens": max(0, prompt_cache_hit_tokens),
        "prompt_cache_miss_tokens": max(0, prompt_cache_miss_tokens),
        "prompt_tokens_details": prompt_tokens_details,
        "cache_hit": bool(payload.get("cache_hit", details_mapping.get("cache_hit", False))),
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


def _parse_timestamp_to_shanghai(timestamp_value: Any) -> Optional[datetime]:
    text = str(timestamp_value or "").strip()
    if not text:
        return None
    try:
        normalized = text.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(_SHANGHAI_TZ)


def _is_deepseek_offpeak(timestamp_value: Any) -> bool:
    dt = _parse_timestamp_to_shanghai(timestamp_value)
    if dt is None:
        return False
    current_minutes = dt.hour * 60 + dt.minute
    return 30 <= current_minutes < 510


def build_token_cost_estimate(
    *,
    usage: Any,
    model: Any,
    provider: Any = None,
    base_url: Any = None,
    timestamp_utc: Any = None,
    local_cache_hit: bool = False,
) -> Dict[str, Any]:
    """基于官方价格快照估算单次调用成本。"""

    usage_payload = normalize_usage_payload(usage)
    pricing = _resolve_pricing(model=model, provider=provider, base_url=base_url)
    model_key = _normalize_model_key(model)
    inferred_provider = _infer_provider(model=model_key, base_url=base_url)

    if bool(local_cache_hit):
        return {
            "status": "app_cache_hit",
            "provider": inferred_provider,
            "model": str(model or ""),
            "resolved_model_key": model_key,
            "currency": pricing.currency if pricing is not None else "",
            "pricing_mode": "app_cache_hit",
            "prompt_tokens": int(usage_payload.get("prompt_tokens", 0) or 0),
            "completion_tokens": int(usage_payload.get("completion_tokens", 0) or 0),
            "total_tokens": int(usage_payload.get("total_tokens", 0) or 0),
            "cached_prompt_tokens": int(usage_payload.get("prompt_cache_hit_tokens", 0) or 0),
            "uncached_prompt_tokens": int(usage_payload.get("prompt_cache_miss_tokens", 0) or 0),
            "input_cost": 0.0,
            "cached_input_cost": 0.0,
            "output_cost": 0.0,
            "total_cost": 0.0,
            "source_url": pricing.source_url if pricing is not None else "",
            "checked_at": pricing.checked_at if pricing is not None else _PRICING_CHECKED_AT,
        }

    if pricing is None:
        return {
            "status": "unsupported_model",
            "provider": inferred_provider,
            "model": str(model or ""),
            "resolved_model_key": model_key,
            "currency": "",
            "pricing_mode": "unsupported",
            "prompt_tokens": int(usage_payload.get("prompt_tokens", 0) or 0),
            "completion_tokens": int(usage_payload.get("completion_tokens", 0) or 0),
            "total_tokens": int(usage_payload.get("total_tokens", 0) or 0),
            "cached_prompt_tokens": int(usage_payload.get("prompt_cache_hit_tokens", 0) or 0),
            "uncached_prompt_tokens": int(usage_payload.get("prompt_cache_miss_tokens", 0) or 0),
            "input_cost": None,
            "cached_input_cost": None,
            "output_cost": None,
            "total_cost": None,
            "source_url": "",
            "checked_at": _PRICING_CHECKED_AT,
        }

    prompt_tokens = int(usage_payload.get("prompt_tokens", 0) or 0)
    completion_tokens = int(usage_payload.get("completion_tokens", 0) or 0)
    cached_prompt_tokens = int(usage_payload.get("prompt_cache_hit_tokens", 0) or 0)
    uncached_prompt_tokens = int(usage_payload.get("prompt_cache_miss_tokens", max(0, prompt_tokens - cached_prompt_tokens)) or 0)

    pricing_mode = "standard"
    input_rate = float(pricing.input_per_million)
    output_rate = float(pricing.output_per_million)
    cached_input_rate = float(pricing.cached_input_per_million) if pricing.cached_input_per_million is not None else input_rate
    if pricing.provider == "deepseek" and _is_deepseek_offpeak(timestamp_utc):
        pricing_mode = "offpeak_half_price"
        input_rate = float(pricing.offpeak_input_per_million or pricing.input_per_million)
        output_rate = float(pricing.offpeak_output_per_million or pricing.output_per_million)
        cached_input_rate = float(
            pricing.offpeak_cached_input_per_million
            if pricing.offpeak_cached_input_per_million is not None
            else (pricing.cached_input_per_million if pricing.cached_input_per_million is not None else input_rate)
        )

    input_cost = float(uncached_prompt_tokens) * input_rate / 1_000_000.0
    cached_input_cost = float(cached_prompt_tokens) * cached_input_rate / 1_000_000.0
    output_cost = float(completion_tokens) * output_rate / 1_000_000.0
    total_cost = input_cost + cached_input_cost + output_cost

    return {
        "status": "ok",
        "provider": pricing.provider,
        "model": str(model or ""),
        "resolved_model_key": pricing.model_key,
        "currency": pricing.currency,
        "pricing_mode": pricing_mode,
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": int(usage_payload.get("total_tokens", prompt_tokens + completion_tokens) or 0),
        "cached_prompt_tokens": cached_prompt_tokens,
        "uncached_prompt_tokens": uncached_prompt_tokens,
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
    }


def get_token_pricing_snapshot() -> Dict[str, Any]:
    """返回当前硬编码价格快照与官方来源。"""

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
    """汇总 canonical token-cost records。"""

    total_records = 0
    priced_records = 0
    app_cache_hit_records = 0
    unsupported_records = 0
    total_prompt_tokens = 0
    total_completion_tokens = 0
    total_tokens = 0
    total_cached_prompt_tokens = 0
    total_uncached_prompt_tokens = 0
    currency_totals: Dict[str, float] = {}
    model_totals: Dict[str, Dict[str, Any]] = {}

    for record in records or []:
        total_records += 1
        usage = normalize_usage_payload(record.get("token_usage", {}))
        cost_estimate = dict(record.get("cost_estimate", {}) or {})
        total_prompt_tokens += int(usage.get("prompt_tokens", 0) or 0)
        total_completion_tokens += int(usage.get("completion_tokens", 0) or 0)
        total_tokens += int(usage.get("total_tokens", 0) or 0)
        total_cached_prompt_tokens += int(usage.get("prompt_cache_hit_tokens", 0) or 0)
        total_uncached_prompt_tokens += int(usage.get("prompt_cache_miss_tokens", 0) or 0)

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
        if model_key:
            bucket = model_totals.setdefault(
                model_key,
                {
                    "provider": str(cost_estimate.get("provider", record.get("provider", "")) or ""),
                    "currency": str(cost_estimate.get("currency", "") or ""),
                    "records": 0,
                    "prompt_tokens": 0,
                    "completion_tokens": 0,
                    "total_tokens": 0,
                    "cached_prompt_tokens": 0,
                    "uncached_prompt_tokens": 0,
                    "estimated_total_cost": 0.0,
                },
            )
            bucket["records"] += 1
            bucket["prompt_tokens"] += int(usage.get("prompt_tokens", 0) or 0)
            bucket["completion_tokens"] += int(usage.get("completion_tokens", 0) or 0)
            bucket["total_tokens"] += int(usage.get("total_tokens", 0) or 0)
            bucket["cached_prompt_tokens"] += int(usage.get("prompt_cache_hit_tokens", 0) or 0)
            bucket["uncached_prompt_tokens"] += int(usage.get("prompt_cache_miss_tokens", 0) or 0)
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
        "total_cached_prompt_tokens": total_cached_prompt_tokens,
        "total_uncached_prompt_tokens": total_uncached_prompt_tokens,
        "estimated_cost_by_currency": currency_totals,
        "by_model": list(model_totals.values()),
    }
