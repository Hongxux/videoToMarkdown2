from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from services.python_grpc.src.common.utils.async_disk_writer import enqueue_json_write
from services.python_grpc.src.common.utils.stage_artifact_paths import stage_audits_dir
from services.python_grpc.src.content_pipeline.infra.llm.token_costing import (
    build_token_cost_estimate,
    get_token_pricing_snapshot,
    normalize_usage_payload,
)


def _safe_int_token(value: Any, default: int = 0) -> int:
    try:
        return max(0, int(value))
    except Exception:
        return max(0, int(default))


@dataclass
class VLReportWriter:
    """Persist VL reports with one shared pricing/costing path."""

    task_id: str
    video_path: str
    semantic_units_path: str
    output_dir: str
    logger: Any
    enqueue_json_write_fn: Callable[..., None] = field(default=enqueue_json_write)

    def _resolve_report_dirs(self) -> List[str]:
        base_dir = self.output_dir or (os.path.dirname(self.video_path) if self.video_path else os.getcwd())
        report_dirs: List[str] = []
        canonical_dir = str(stage_audits_dir(base_dir, "phase2a"))
        for report_dir in (canonical_dir, os.path.join(base_dir, "immediates"), os.path.join(base_dir, "intermediates")):
            os.makedirs(report_dir, exist_ok=True)
            if report_dir not in report_dirs:
                report_dirs.append(report_dir)
        return report_dirs

    def _build_token_usage(self, token_stats: Any) -> Dict[str, Any]:
        stats = token_stats if isinstance(token_stats, dict) else {}
        prompt_tokens = _safe_int_token(
            stats.get(
                "input_tokens_actual",
                stats.get("prompt_tokens_actual", stats.get("input_tokens", stats.get("prompt_tokens", 0))),
            ),
            0,
        )
        completion_tokens = _safe_int_token(
            stats.get(
                "output_tokens_actual",
                stats.get("completion_tokens_actual", stats.get("output_tokens", stats.get("completion_tokens", 0))),
            ),
            0,
        )
        total_tokens = _safe_int_token(
            stats.get("total_tokens_actual", stats.get("total_tokens", prompt_tokens + completion_tokens)),
            prompt_tokens + completion_tokens,
        )
        if total_tokens <= 0:
            total_tokens = prompt_tokens + completion_tokens

        detail_payload: Dict[str, Any] = {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": max(0, total_tokens),
            "input_tokens_details": {
                "text_tokens": _safe_int_token(stats.get("text_input_tokens_actual", 0), 0),
                "image_tokens": _safe_int_token(stats.get("image_input_tokens_actual", 0), 0),
                "audio_tokens": _safe_int_token(stats.get("audio_input_tokens_actual", 0), 0),
                "video_tokens": _safe_int_token(stats.get("video_input_tokens_actual", 0), 0),
            },
        }
        return normalize_usage_payload(detail_payload)

    def _build_vl_pricing(self, token_usage: Dict[str, Any], model_name: str) -> Dict[str, Any]:
        return build_token_cost_estimate(
            usage=token_usage,
            model=str(model_name or "").strip(),
        )

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
        report_payload["pricing_snapshot"] = get_token_pricing_snapshot()
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
