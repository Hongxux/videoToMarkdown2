from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from services.python_grpc.src.common.utils.async_disk_writer import enqueue_json_write


QWEN3_VL_PLUS_INPUT_PER_M = 1.50
QWEN3_VL_PLUS_OUTPUT_PER_M = 4.50
ERNIE_45_TURBO_VL_INPUT_MIN_PER_M = 0.80
ERNIE_45_TURBO_VL_INPUT_MAX_PER_M = 1.50
ERNIE_45_TURBO_VL_OUTPUT_MIN_PER_M = 3.20
ERNIE_45_TURBO_VL_OUTPUT_MAX_PER_M = 4.50


def _safe_int_token(value: Any, default: int = 0) -> int:
    try:
        return max(0, int(value))
    except Exception:
        return max(0, int(default))


def _round_usd(value: Any) -> float:
    try:
        return round(max(0.0, float(value)), 6)
    except Exception:
        return 0.0


def _token_cost_usd(tokens: int, rate_per_million: float) -> float:
    return _round_usd((max(0, int(tokens)) / 1_000_000.0) * float(rate_per_million))


@dataclass
class VLReportWriter:
    """VL 报告写盘器：负责 token/价格补齐与异步双目录落盘。"""

    task_id: str
    video_path: str
    semantic_units_path: str
    output_dir: str
    logger: Any
    enqueue_json_write_fn: Callable[..., None] = field(default=enqueue_json_write)

    def _resolve_report_dirs(self) -> List[str]:
        base_dir = self.output_dir or (os.path.dirname(self.video_path) if self.video_path else os.getcwd())
        report_dirs: List[str] = []
        for folder_name in ("immediates", "intermediates"):
            report_dir = os.path.join(base_dir, folder_name)
            os.makedirs(report_dir, exist_ok=True)
            report_dirs.append(report_dir)
        return report_dirs

    def _build_token_usage(self, token_stats: Any) -> Dict[str, int]:
        stats = token_stats if isinstance(token_stats, dict) else {}
        prompt_tokens = _safe_int_token(stats.get("prompt_tokens_actual", stats.get("prompt_tokens", 0)), 0)
        completion_tokens = _safe_int_token(stats.get("completion_tokens_actual", stats.get("completion_tokens", 0)), 0)
        total_tokens = _safe_int_token(
            stats.get("total_tokens_actual", stats.get("total_tokens", prompt_tokens + completion_tokens)),
            prompt_tokens + completion_tokens,
        )
        if total_tokens <= 0:
            total_tokens = prompt_tokens + completion_tokens
        return {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": max(0, total_tokens),
        }

    def _build_vl_pricing(self, token_usage: Dict[str, int], model_name: str) -> Dict[str, Any]:
        prompt_tokens = _safe_int_token(token_usage.get("prompt_tokens", 0), 0)
        completion_tokens = _safe_int_token(token_usage.get("completion_tokens", 0), 0)

        qwen_input_cost = _token_cost_usd(prompt_tokens, QWEN3_VL_PLUS_INPUT_PER_M)
        qwen_output_cost = _token_cost_usd(completion_tokens, QWEN3_VL_PLUS_OUTPUT_PER_M)
        qwen_total_cost = _round_usd(qwen_input_cost + qwen_output_cost)

        ernie_input_min_cost = _token_cost_usd(prompt_tokens, ERNIE_45_TURBO_VL_INPUT_MIN_PER_M)
        ernie_input_max_cost = _token_cost_usd(prompt_tokens, ERNIE_45_TURBO_VL_INPUT_MAX_PER_M)
        ernie_output_min_cost = _token_cost_usd(completion_tokens, ERNIE_45_TURBO_VL_OUTPUT_MIN_PER_M)
        ernie_output_max_cost = _token_cost_usd(completion_tokens, ERNIE_45_TURBO_VL_OUTPUT_MAX_PER_M)
        ernie_total_min_cost = _round_usd(ernie_input_min_cost + ernie_output_min_cost)
        ernie_total_max_cost = _round_usd(ernie_input_max_cost + ernie_output_max_cost)

        normalized_model = str(model_name or "").strip().lower()
        if "qwen3-vl-plus" in normalized_model:
            selected_pricing_model = "qwen3-vl-plus"
            selected_min_cost = qwen_total_cost
            selected_max_cost = qwen_total_cost
        elif "ernie-4.5-turbo-vl" in normalized_model or "ernie" in normalized_model:
            selected_pricing_model = "ernie-4.5-turbo-vl"
            selected_min_cost = ernie_total_min_cost
            selected_max_cost = ernie_total_max_cost
        else:
            selected_pricing_model = "unknown"
            selected_min_cost = min(qwen_total_cost, ernie_total_min_cost)
            selected_max_cost = max(qwen_total_cost, ernie_total_max_cost)

        pricing_payload: Dict[str, Any] = {
            "currency": "USD",
            "pricing_basis": "per_1m_tokens",
            "rates_usd_per_1m_tokens": {
                "qwen3_vl_plus_input": QWEN3_VL_PLUS_INPUT_PER_M,
                "qwen3_vl_plus_output": QWEN3_VL_PLUS_OUTPUT_PER_M,
                "ernie_4_5_turbo_vl_input_min": ERNIE_45_TURBO_VL_INPUT_MIN_PER_M,
                "ernie_4_5_turbo_vl_input_max": ERNIE_45_TURBO_VL_INPUT_MAX_PER_M,
                "ernie_4_5_turbo_vl_output_min": ERNIE_45_TURBO_VL_OUTPUT_MIN_PER_M,
                "ernie_4_5_turbo_vl_output_max": ERNIE_45_TURBO_VL_OUTPUT_MAX_PER_M,
            },
            "qwen3_vl_plus_cost": {
                "input_cost_usd": qwen_input_cost,
                "output_cost_usd": qwen_output_cost,
                "total_cost_usd": qwen_total_cost,
            },
            "ernie_4_5_turbo_vl_cost_range": {
                "input_cost_usd_min": ernie_input_min_cost,
                "input_cost_usd_max": ernie_input_max_cost,
                "output_cost_usd_min": ernie_output_min_cost,
                "output_cost_usd_max": ernie_output_max_cost,
                "total_cost_usd_min": ernie_total_min_cost,
                "total_cost_usd_max": ernie_total_max_cost,
            },
            "selected_pricing_model": selected_pricing_model,
            "selected_cost_usd_min": _round_usd(selected_min_cost),
            "selected_cost_usd_max": _round_usd(selected_max_cost),
        }
        if abs(selected_max_cost - selected_min_cost) < 1e-12:
            pricing_payload["selected_cost_usd"] = _round_usd(selected_min_cost)
        return pricing_payload

    def _build_report_payload(self, payload: Dict[str, Any], vl_model: str) -> Dict[str, Any]:
        report_payload: Dict[str, Any] = {
            "version": "1.0",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "task_id": self.task_id,
            "video_path": self.video_path,
            "semantic_units_path": self.semantic_units_path,
            "output_dir": self.output_dir,
        }
        report_payload.update(payload or {})
        report_payload["vl_model"] = str(report_payload.get("vl_model") or vl_model or "").strip()
        token_usage = self._build_token_usage(report_payload.get("token_stats", {}))
        report_payload["token_usage"] = token_usage
        report_payload["pricing"] = self._build_vl_pricing(token_usage, report_payload.get("vl_model", ""))
        return report_payload

    def _persist_report(self, *, base_name: str, payload: Dict[str, Any], vl_model: str) -> str:
        report_payload = self._build_report_payload(payload, vl_model)
        report_dirs = self._resolve_report_dirs()
        if not report_dirs:
            return ""

        report_name = f"{base_name}_{self.task_id}.json" if self.task_id else f"{base_name}_unknown.json"
        latest_name = f"{base_name}_latest.json"
        primary_path = ""
        for index, report_dir in enumerate(report_dirs):
            report_path = os.path.join(report_dir, report_name)
            latest_path = os.path.join(report_dir, latest_name)
            self.enqueue_json_write_fn(report_path, report_payload, ensure_ascii=False, indent=2)
            self.enqueue_json_write_fn(latest_path, report_payload, ensure_ascii=False, indent=2)
            if index == 0:
                primary_path = report_path
        self.logger.info(f"[{self.task_id}] queued async {base_name} report: {primary_path}")
        return primary_path

    def persist_token_report(self, payload: Optional[Dict[str, Any]], vl_model: str) -> str:
        try:
            return self._persist_report(base_name="vl_token_report", payload=payload or {}, vl_model=vl_model)
        except Exception as report_error:
            self.logger.warning(f"[{self.task_id}] async vl_token_report enqueue failed: {report_error}")
            return ""

    def persist_analysis_output(self, payload: Optional[Dict[str, Any]], vl_model: str) -> str:
        try:
            return self._persist_report(base_name="vl_analysis_output", payload=payload or {}, vl_model=vl_model)
        except Exception as report_error:
            self.logger.warning(f"[{self.task_id}] async vl_analysis_output enqueue failed: {report_error}")
            return ""
