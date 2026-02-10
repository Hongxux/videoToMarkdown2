"""
汇总 Phase2B 观测产物：
- phase2b_llm_trace.jsonl
- phase2b_image_match_audit.json

输出：
- intermediates/phase2b_test_report.json
- intermediates/phase2b_test_report.md
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List


def _load_json(path: Path) -> Any:
    with open(path, "r", encoding="utf-8") as file_obj:
        return json.load(file_obj)


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as file_obj:
        for raw in file_obj:
            line = raw.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
                if isinstance(payload, dict):
                    rows.append(payload)
            except Exception:
                continue
    return rows


def build_report(storage_dir: Path) -> Dict[str, Any]:
    inter = storage_dir / "intermediates"
    llm_trace_path = inter / "phase2b_llm_trace.jsonl"
    image_audit_path = inter / "phase2b_image_match_audit.json"
    deepseek_audit_path = inter / "phase2b_deepseek_call_audit.json"

    llm_rows = _load_jsonl(llm_trace_path) if llm_trace_path.exists() else []
    image_audit = _load_json(image_audit_path) if image_audit_path.exists() else {}
    deepseek_audit = _load_json(deepseek_audit_path) if deepseek_audit_path.exists() else {}
    image_records = image_audit.get("records", []) if isinstance(image_audit, dict) else []
    deepseek_records = deepseek_audit.get("records", []) if isinstance(deepseek_audit, dict) else []

    llm_total = len(llm_rows)
    llm_failed = sum(1 for row in llm_rows if not bool(row.get("success", False)))
    step_counter = Counter(str(row.get("step_name") or "") for row in llm_rows)
    durations = [float(row.get("duration_ms") or 0.0) for row in llm_rows]
    duration_max = max(durations) if durations else 0.0
    duration_avg = (sum(durations) / len(durations)) if durations else 0.0

    mapping_counter = Counter(str(item.get("mapping_status") or "") for item in image_records if isinstance(item, dict))
    total_images = len(image_records)
    with_timestamp = mapping_counter.get("mapped", 0) + mapping_counter.get("unmapped", 0)
    mapped = mapping_counter.get("mapped", 0)
    mapping_rate = (mapped / with_timestamp) if with_timestamp else 0.0

    if isinstance(deepseek_audit, dict):
        reported_total = int(deepseek_audit.get("total_calls", len(deepseek_records)) or 0)
        deepseek_total = max(reported_total, len(deepseek_records))
    else:
        deepseek_total = len(deepseek_records)
    deepseek_failed = 0
    deepseek_step_counter: Counter[str] = Counter()
    deepseek_model_counter: Counter[str] = Counter()
    deepseek_tokens_total = 0
    deepseek_has_io_pair = True

    for record in deepseek_records:
        if not isinstance(record, dict):
            deepseek_has_io_pair = False
            continue

        input_obj = record.get("input")
        output_obj = record.get("output")
        if not isinstance(input_obj, dict) or not isinstance(output_obj, dict):
            deepseek_has_io_pair = False

        step_name = str(record.get("step_name") or "")
        if step_name:
            deepseek_step_counter[step_name] += 1

        input_model = str((input_obj or {}).get("model") or "")
        output_model = str(((output_obj or {}).get("metadata") or {}).get("model") or "") if isinstance(output_obj, dict) else ""
        model_name = input_model or output_model
        if model_name:
            deepseek_model_counter[model_name] += 1

        success = bool((output_obj or {}).get("success", False)) if isinstance(output_obj, dict) else False
        error_text = str((output_obj or {}).get("error") or "") if isinstance(output_obj, dict) else ""
        if (not success) or error_text:
            deepseek_failed += 1

        metadata = (output_obj or {}).get("metadata", {}) if isinstance(output_obj, dict) else {}
        if isinstance(metadata, dict):
            try:
                deepseek_tokens_total += int(metadata.get("total_tokens", 0) or 0)
            except Exception:
                pass

    deepseek_tokens_avg = (deepseek_tokens_total / deepseek_total) if deepseek_total else 0.0

    report: Dict[str, Any] = {
        "storage_dir": str(storage_dir),
        "llm_trace": {
            "path": str(llm_trace_path),
            "exists": llm_trace_path.exists(),
            "total_calls": llm_total,
            "failed_calls": llm_failed,
            "by_step": dict(step_counter),
            "duration_ms_avg": duration_avg,
            "duration_ms_max": duration_max,
        },
        "image_match": {
            "path": str(image_audit_path),
            "exists": image_audit_path.exists(),
            "total_records": total_images,
            "status_counts": dict(mapping_counter),
            "with_timestamp": with_timestamp,
            "mapped": mapped,
            "mapping_rate": mapping_rate,
        },
        "deepseek_audit": {
            "path": str(deepseek_audit_path),
            "exists": deepseek_audit_path.exists(),
            "total_calls": deepseek_total,
            "failed_calls": deepseek_failed,
            "by_step": dict(deepseek_step_counter),
            "by_model": dict(deepseek_model_counter),
            "tokens_total": deepseek_tokens_total,
            "tokens_avg": deepseek_tokens_avg,
            "input_output_paired": deepseek_has_io_pair,
        },
        "conclusion": {
            "llm_trace_ready": llm_trace_path.exists() and llm_total > 0,
            "image_mapping_ready": image_audit_path.exists() and total_images > 0,
            "image_mapping_nonzero": mapping_rate > 0.0,
            "deepseek_audit_ready": deepseek_audit_path.exists() and deepseek_total > 0,
            "deepseek_input_output_paired": deepseek_has_io_pair,
        },
    }

    return report


def write_report(storage_dir: Path, report: Dict[str, Any]) -> None:
    inter = storage_dir / "intermediates"
    json_path = inter / "phase2b_test_report.json"
    md_path = inter / "phase2b_test_report.md"

    inter.mkdir(parents=True, exist_ok=True)
    with open(json_path, "w", encoding="utf-8") as file_obj:
        json.dump(report, file_obj, ensure_ascii=False, indent=2)

    lines: List[str] = []
    lines.append("# Phase2B Test Report")
    lines.append("")
    lines.append(f"- storage_dir: `{report.get('storage_dir', '')}`")
    lines.append("")

    llm = report.get("llm_trace", {})
    lines.append("## LLM Trace")
    lines.append(f"- exists: {llm.get('exists', False)}")
    lines.append(f"- total_calls: {llm.get('total_calls', 0)}")
    lines.append(f"- failed_calls: {llm.get('failed_calls', 0)}")
    lines.append(f"- duration_ms_avg: {llm.get('duration_ms_avg', 0):.2f}")
    lines.append(f"- duration_ms_max: {llm.get('duration_ms_max', 0):.2f}")
    lines.append(f"- by_step: {llm.get('by_step', {})}")
    lines.append("")

    image = report.get("image_match", {})
    lines.append("## Image Match")
    lines.append(f"- exists: {image.get('exists', False)}")
    lines.append(f"- total_records: {image.get('total_records', 0)}")
    lines.append(f"- status_counts: {image.get('status_counts', {})}")
    lines.append(f"- with_timestamp: {image.get('with_timestamp', 0)}")
    lines.append(f"- mapped: {image.get('mapped', 0)}")
    lines.append(f"- mapping_rate: {image.get('mapping_rate', 0):.4f}")
    lines.append("")

    deepseek = report.get("deepseek_audit", {})
    lines.append("## DeepSeek Audit")
    lines.append(f"- exists: {deepseek.get('exists', False)}")
    lines.append(f"- total_calls: {deepseek.get('total_calls', 0)}")
    lines.append(f"- failed_calls: {deepseek.get('failed_calls', 0)}")
    lines.append(f"- by_step: {deepseek.get('by_step', {})}")
    lines.append(f"- by_model: {deepseek.get('by_model', {})}")
    lines.append(f"- tokens_total: {deepseek.get('tokens_total', 0)}")
    lines.append(f"- tokens_avg: {deepseek.get('tokens_avg', 0):.2f}")
    lines.append(f"- input_output_paired: {deepseek.get('input_output_paired', False)}")
    lines.append("")

    conc = report.get("conclusion", {})
    lines.append("## Conclusion")
    lines.append(f"- llm_trace_ready: {conc.get('llm_trace_ready', False)}")
    lines.append(f"- image_mapping_ready: {conc.get('image_mapping_ready', False)}")
    lines.append(f"- image_mapping_nonzero: {conc.get('image_mapping_nonzero', False)}")
    lines.append(f"- deepseek_audit_ready: {conc.get('deepseek_audit_ready', False)}")
    lines.append(f"- deepseek_input_output_paired: {conc.get('deepseek_input_output_paired', False)}")
    lines.append("")

    with open(md_path, "w", encoding="utf-8") as file_obj:
        file_obj.write("\n".join(lines))

    print(f"Report written: {json_path}")
    print(f"Report written: {md_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Phase2B observability report")
    parser.add_argument("--storage-dir", required=True, help="Path to storage/<task_id> directory")
    args = parser.parse_args()

    storage_dir = Path(args.storage_dir).resolve()
    report = build_report(storage_dir)
    write_report(storage_dir, report)


if __name__ == "__main__":
    main()
