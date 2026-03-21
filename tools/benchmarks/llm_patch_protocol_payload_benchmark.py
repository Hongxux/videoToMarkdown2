from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import statistics
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import tiktoken

from services.python_grpc.src.content_pipeline.markdown_enhancer import MarkdownEnhancer
from services.python_grpc.src.transcript_pipeline.nodes import phase2_preprocessing
from services.python_grpc.src.transcript_pipeline.nodes.step_contracts import (
    assemble_step4_cleaned_sentences,
    parse_step35_translated_sentences,
    parse_step4_cleaned_sentences,
)


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(v) for v in value]
    return str(value)


def _percentile(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    k = (len(sorted_values) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_values) - 1)
    if f == c:
        return float(sorted_values[f])
    d0 = sorted_values[f] * (c - k)
    d1 = sorted_values[c] * (k - f)
    return float(d0 + d1)


def _serialize_payload(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _repo_token_proxy(text: str) -> int:
    return MarkdownEnhancer._estimate_tokens_from_chars(len(str(text or "")))


_TOKENIZER = tiktoken.get_encoding("cl100k_base")


def _cl100k_tokens(text: str) -> int:
    return len(_TOKENIZER.encode(str(text or "")))


def _measure_function(
    fn: Callable[[], Any],
    *,
    warmup_runs: int,
    measured_runs: int,
) -> Dict[str, float]:
    for _ in range(max(0, int(warmup_runs))):
        fn()

    samples_ms: List[float] = []
    for _ in range(max(1, int(measured_runs))):
        started_at = time.perf_counter()
        fn()
        samples_ms.append((time.perf_counter() - started_at) * 1000.0)

    return {
        "latency_ms_mean": float(statistics.fmean(samples_ms)),
        "latency_ms_p95": _percentile(samples_ms, 95),
        "latency_ms_min": min(samples_ms) if samples_ms else 0.0,
        "latency_ms_max": max(samples_ms) if samples_ms else 0.0,
    }


def _build_img_desc_case() -> Dict[str, Any]:
    intro = "本段用于介绍背景与上下文，强调正文补全必须保持原语义稳定。" * 26
    middle = "先打开终端执行命令。然后查看日志。最后保存配置并继续验证。"
    ending = "补充说明部分用于拉长正文长度，但不改变需要补全的关键位置。" * 22
    base_text = f"{intro}{middle}{ending}"
    final_text = base_text.replace("执行命令", "执行 `npm run dev` 命令", 1).replace(
        "然后查看日志。",
        "然后查看日志 并确认端口为 `3000`。",
        1,
    )

    legacy_payload = {
        "text": final_text,
    }
    compact_payload = {
        "p": [
            {
                "m": "r",
                "o": "执行命令",
                "n": "执行 `npm run dev` 命令",
                "l": "先打开终端",
                "r": "。然后查看日志。",
            },
            {
                "m": "a",
                "n": " 并确认端口为 `3000`",
                "l": "然后查看日志",
                "r": "。",
                "p": "after",
            },
        ]
    }

    legacy_payload_text = _serialize_payload(legacy_payload)
    compact_payload_text = _serialize_payload(compact_payload)

    def _apply_legacy() -> str:
        return MarkdownEnhancer._apply_img_desc_incremental_ops(base_text, legacy_payload_text)[0]

    def _apply_compact() -> str:
        return MarkdownEnhancer._apply_img_desc_incremental_ops(base_text, compact_payload_text)[0]

    legacy_result = _apply_legacy()
    compact_result = _apply_compact()
    assert legacy_result == final_text
    assert compact_result == final_text

    return {
        "scenario": "img_desc_augment",
        "base_text_chars": len(base_text),
        "legacy_payload_text": legacy_payload_text,
        "compact_payload_text": compact_payload_text,
        "legacy_apply_fn": _apply_legacy,
        "compact_apply_fn": _apply_compact,
        "final_equivalent": True,
        "notes": "图片描述补全：整段回写 JSON vs replace/add 最小补丁 JSON",
    }


def _build_step35_case() -> Dict[str, Any]:
    source_sentences: List[Dict[str, Any]] = []
    compact_rows: List[Dict[str, Any]] = []
    verbose_rows: List[Dict[str, Any]] = []
    for index in range(1, 51):
        sentence_id = f"S{index:03d}"
        source = {
            "sentence_id": sentence_id,
            "text": f"sentence {index} about deepseek and agent orchestration",
            "start_sec": float(index),
            "end_sec": float(index) + 0.5,
            "source_subtitle_ids": [f"SUB{index:03d}"],
        }
        translated_text = f"第{index}句中文译文，保留术语 deepseek 与 agent 的语义。"
        source_sentences.append(source)
        compact_rows.append({"sid": sentence_id, "tt": translated_text})
        verbose_rows.append(
            {
                "sentence_id": sentence_id,
                "translated_text": translated_text,
            }
        )

    valid_sentence_ids = {item["sentence_id"] for item in source_sentences}
    legacy_payload_text = _serialize_payload({"translated_sentences": verbose_rows})
    compact_payload_text = _serialize_payload({"t": compact_rows})

    def _assemble_from_payload(payload_text: str) -> List[Dict[str, Any]]:
        result = json.loads(payload_text)
        translated_by_id, _ = parse_step35_translated_sentences(
            result,
            valid_sentence_ids=valid_sentence_ids,
        )
        final_sentences: List[Dict[str, Any]] = []
        for source in source_sentences:
            sentence_id = source["sentence_id"]
            final_sentences.append(
                {
                    "sentence_id": sentence_id,
                    "text": translated_by_id.get(sentence_id, source["text"]),
                    "start_sec": source["start_sec"],
                    "end_sec": source["end_sec"],
                    "source_subtitle_ids": list(source["source_subtitle_ids"]),
                }
            )
        return final_sentences

    legacy_result = _assemble_from_payload(legacy_payload_text)
    compact_result = _assemble_from_payload(compact_payload_text)
    assert legacy_result == compact_result

    return {
        "scenario": "step3_5_translate",
        "item_count": len(source_sentences),
        "legacy_payload_text": legacy_payload_text,
        "compact_payload_text": compact_payload_text,
        "legacy_apply_fn": lambda: _assemble_from_payload(legacy_payload_text),
        "compact_apply_fn": lambda: _assemble_from_payload(compact_payload_text),
        "final_equivalent": True,
        "notes": "Step3.5：长键 translated_sentences/ sentence_id / translated_text vs 短键 t / sid / tt",
    }


def _build_step35_local_fill_case() -> Dict[str, Any]:
    source_sentences: List[Dict[str, Any]] = []
    compact_rows: List[Dict[str, Any]] = []
    verbose_rows: List[Dict[str, Any]] = []
    for index in range(1, 51):
        sentence_id = f"S{index:03d}"
        source = {
            "sentence_id": sentence_id,
            "text": f"sentence {index} about deepseek and agent orchestration",
            "start_sec": float(index),
            "end_sec": float(index) + 0.5,
            "source_subtitle_ids": [f"SUB{index:03d}"],
        }
        translated_text = f"第{index}句中文译文，保留术语 deepseek 与 agent 的语义。"
        source_sentences.append(source)
        compact_rows.append({"sid": sentence_id, "tt": translated_text})
        verbose_rows.append(
            {
                "sentence_id": sentence_id,
                "translated_text": translated_text,
                "start_sec": source["start_sec"],
                "end_sec": source["end_sec"],
                "source_subtitle_ids": list(source["source_subtitle_ids"]),
            }
        )

    valid_sentence_ids = {item["sentence_id"] for item in source_sentences}
    legacy_payload_text = _serialize_payload({"translated_sentences": verbose_rows})
    compact_payload_text = _serialize_payload({"t": compact_rows})

    def _assemble_from_payload(payload_text: str) -> List[Dict[str, Any]]:
        result = json.loads(payload_text)
        translated_by_id, _ = parse_step35_translated_sentences(
            result,
            valid_sentence_ids=valid_sentence_ids,
        )
        final_sentences: List[Dict[str, Any]] = []
        for source in source_sentences:
            sentence_id = source["sentence_id"]
            final_sentences.append(
                {
                    "sentence_id": sentence_id,
                    "text": translated_by_id.get(sentence_id, source["text"]),
                    "start_sec": source["start_sec"],
                    "end_sec": source["end_sec"],
                    "source_subtitle_ids": list(source["source_subtitle_ids"]),
                }
            )
        return final_sentences

    legacy_result = _assemble_from_payload(legacy_payload_text)
    compact_result = _assemble_from_payload(compact_payload_text)
    assert legacy_result == compact_result

    return {
        "scenario": "step3_5_translate_local_fill",
        "item_count": len(source_sentences),
        "legacy_payload_text": legacy_payload_text,
        "compact_payload_text": compact_payload_text,
        "legacy_apply_fn": lambda: _assemble_from_payload(legacy_payload_text),
        "compact_apply_fn": lambda: _assemble_from_payload(compact_payload_text),
        "final_equivalent": True,
        "notes": "Step3.5：旧回包携带 start/end/source，紧凑回包仅保留 sid/tt，本地完成时间轴与来源回填",
    }


def _build_step4_case() -> Dict[str, Any]:
    source_sentences: List[Dict[str, Any]] = []
    verbose_rows: List[Dict[str, Any]] = []
    compact_rows: List[Dict[str, Any]] = []
    for index in range(1, 21):
        sentence_id = f"S{index:03d}"
        original_text = (
            f"第{index}句 我们现在就说 这个方案的关键是智能体（agent）调度，"
            "它会负责状态同步、工具调用、错误恢复和配置热更新。"
        )
        cleaned_text = f"第{index}句 这个方案的关键是智能体（agent）调度。"
        source_sentences.append(
            {
                "sentence_id": sentence_id,
                "text": original_text,
                "start_sec": float(index),
                "end_sec": float(index) + 0.5,
                "source_subtitle_ids": [f"SUB{index:03d}"],
            }
        )
        verbose_rows.append(
            {
                "sentence_id": sentence_id,
                "original": "我们现在就说 ",
                "left_context": f"第{index}句 ",
                "right_context": "这个方案的关键是",
            }
        )
        compact_rows.append(
            {
                "sid": sentence_id,
                "o": "我们现在就说 ",
                "l": f"第{index}句 ",
                "r": "这个方案的关键是",
            }
        )

    valid_sentence_ids = {item["sentence_id"] for item in source_sentences}
    legacy_payload_text = _serialize_payload({"removals": verbose_rows})
    compact_payload_text = _serialize_payload({"d": compact_rows})

    def _assemble_from_payload(payload_text: str) -> List[Dict[str, Any]]:
        result = json.loads(payload_text)
        cleaned_by_id, _ = parse_step4_cleaned_sentences(
            result,
            valid_sentence_ids=valid_sentence_ids,
        )
        assembled, _ = assemble_step4_cleaned_sentences(
            source_sentences,
            llm_cleaned_by_id=cleaned_by_id,
        )
        return assembled

    legacy_result = _assemble_from_payload(legacy_payload_text)
    compact_result = _assemble_from_payload(compact_payload_text)
    assert legacy_result == compact_result

    return {
        "scenario": "step4_clean_local",
        "item_count": len(source_sentences),
        "legacy_payload_text": legacy_payload_text,
        "compact_payload_text": compact_payload_text,
        "legacy_apply_fn": lambda: _assemble_from_payload(legacy_payload_text),
        "compact_apply_fn": lambda: _assemble_from_payload(compact_payload_text),
        "final_equivalent": True,
        "notes": "Step4：长键 removals / sentence_id / original / left_context / right_context vs 短键 d / sid / o / l / r",
    }


class _DummyLogger:
    def start(self):
        return None

    def log_input(self, *_args, **_kwargs):
        return None

    def log_llm_call(self, *_args, **_kwargs):
        return None

    def info(self, *_args, **_kwargs):
        return None

    def warning(self, *_args, **_kwargs):
        return None

    def log_substep(self, *_args, **_kwargs):
        return None

    def log_batch_summary(self, *_args, **_kwargs):
        return None

    def log_output(self, *_args, **_kwargs):
        return None

    def log_error(self, *_args, **_kwargs):
        return None

    def end(self, success=True):
        return {"duration_ms": 1.0, "success": success}


def _run_step4_zero_call_validation(*, measured_runs: int, warmup_runs: int) -> Dict[str, Any]:
    merged_sentences = [
        {
            "sentence_id": f"S{index:03d}",
            "text": f"第{index}句清理后正文",
            "start_sec": float(index),
            "end_sec": float(index) + 0.5,
            "source_subtitle_ids": [f"SUB{index:03d}"],
        }
        for index in range(1, 21)
    ]

    original_get_logger = phase2_preprocessing.get_logger
    phase2_preprocessing.get_logger = lambda *_args, **_kwargs: _DummyLogger()
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            state = {
                "output_dir": temp_dir,
                "translated_sentences": merged_sentences,
                phase2_preprocessing.STEP2_STEP4_MERGED_STATE_FLAG: True,
                "_disable_stage1_artifact_persistence": True,
            }

            async def _invoke_once() -> Dict[str, Any]:
                return await phase2_preprocessing.step4_node(dict(state))

            for _ in range(max(0, int(warmup_runs))):
                result = asyncio.run(_invoke_once())
                assert int(result["token_usage"]["step4_clean_local"]) == 0

            samples_ms: List[float] = []
            for _ in range(max(1, int(measured_runs))):
                started_at = time.perf_counter()
                result = asyncio.run(_invoke_once())
                samples_ms.append((time.perf_counter() - started_at) * 1000.0)
                assert int(result["token_usage"]["step4_clean_local"]) == 0
                assert len(result.get("cleaned_sentences", [])) == len(merged_sentences)
            return {
                "scenario": "step4_zero_call_merge_mode",
                "token_usage": 0,
                "sentence_count": len(merged_sentences),
                "latency_ms_mean": float(statistics.fmean(samples_ms)),
                "latency_ms_p95": _percentile(samples_ms, 95),
                "notes": "Step2+Step4 merged mode 命中后，Step4 仅本地直通，不触发 LLM",
            }
    finally:
        phase2_preprocessing.get_logger = original_get_logger


def _summarize_case(
    case: Dict[str, Any],
    *,
    warmup_runs: int,
    measured_runs: int,
) -> Dict[str, Any]:
    legacy_payload_text = str(case["legacy_payload_text"])
    compact_payload_text = str(case["compact_payload_text"])
    legacy_latency = _measure_function(case["legacy_apply_fn"], warmup_runs=warmup_runs, measured_runs=measured_runs)
    compact_latency = _measure_function(case["compact_apply_fn"], warmup_runs=warmup_runs, measured_runs=measured_runs)

    legacy_chars = len(legacy_payload_text)
    compact_chars = len(compact_payload_text)
    legacy_bytes = len(legacy_payload_text.encode("utf-8"))
    compact_bytes = len(compact_payload_text.encode("utf-8"))
    legacy_proxy_tokens = _repo_token_proxy(legacy_payload_text)
    compact_proxy_tokens = _repo_token_proxy(compact_payload_text)
    legacy_cl100k_tokens = _cl100k_tokens(legacy_payload_text)
    compact_cl100k_tokens = _cl100k_tokens(compact_payload_text)

    return {
        "scenario": case["scenario"],
        "notes": case.get("notes", ""),
        "final_equivalent": bool(case.get("final_equivalent", False)),
        "legacy_chars": legacy_chars,
        "compact_chars": compact_chars,
        "chars_saved": legacy_chars - compact_chars,
        "chars_saved_pct": ((legacy_chars - compact_chars) / legacy_chars * 100.0) if legacy_chars > 0 else 0.0,
        "legacy_bytes": legacy_bytes,
        "compact_bytes": compact_bytes,
        "bytes_saved": legacy_bytes - compact_bytes,
        "bytes_saved_pct": ((legacy_bytes - compact_bytes) / legacy_bytes * 100.0) if legacy_bytes > 0 else 0.0,
        "legacy_repo_token_proxy": legacy_proxy_tokens,
        "compact_repo_token_proxy": compact_proxy_tokens,
        "repo_token_saved_pct": ((legacy_proxy_tokens - compact_proxy_tokens) / legacy_proxy_tokens * 100.0)
        if legacy_proxy_tokens > 0
        else 0.0,
        "legacy_cl100k_tokens": legacy_cl100k_tokens,
        "compact_cl100k_tokens": compact_cl100k_tokens,
        "cl100k_token_saved_pct": ((legacy_cl100k_tokens - compact_cl100k_tokens) / legacy_cl100k_tokens * 100.0)
        if legacy_cl100k_tokens > 0
        else 0.0,
        "legacy_local_apply_ms_mean": legacy_latency["latency_ms_mean"],
        "legacy_local_apply_ms_p95": legacy_latency["latency_ms_p95"],
        "compact_local_apply_ms_mean": compact_latency["latency_ms_mean"],
        "compact_local_apply_ms_p95": compact_latency["latency_ms_p95"],
    }


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_report(
    *,
    report_path: Path,
    metadata: Dict[str, Any],
    summary_rows: List[Dict[str, Any]],
    zero_call_summary: Dict[str, Any],
    output_dir: Path,
) -> None:
    lines: List[str] = []
    lines.append("# LLM Patch Protocol Payload Benchmark")
    lines.append("")
    lines.append(f"- 生成时间: {metadata['generated_at']}")
    lines.append(f"- warmup_runs: `{metadata['warmup_runs']}`")
    lines.append(f"- measured_runs: `{metadata['measured_runs']}`")
    lines.append(f"- tokenizer: `{metadata['tokenizer']}`")
    lines.append(f"- 原始数据目录: `{output_dir / 'raw'}`")
    lines.append("")
    lines.append("## 方法")
    lines.append("- 对同一语义结果分别构造 legacy 全量回写 payload 与 compact 补丁 payload。")
    lines.append("- 统计字符数、UTF-8 字节数、repo chars/4 token proxy、cl100k_base 近似 token。")
    lines.append("- 对本地回放函数做 warmup + measured 微基准，验证补丁协议不会引入不可接受的本地执行开销。")
    lines.append("")
    lines.append("## 汇总")
    lines.append("| scenario | equivalent | chars | chars_compact | chars_saved(%) | bytes_saved(%) | proxy_token_saved(%) | cl100k_saved(%) | legacy_apply_ms | compact_apply_ms |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for row in summary_rows:
        lines.append(
            f"| {row['scenario']} | {int(bool(row['final_equivalent']))} | {row['legacy_chars']} | "
            f"{row['compact_chars']} | {row['chars_saved_pct']:.2f} | {row['bytes_saved_pct']:.2f} | "
            f"{row['repo_token_saved_pct']:.2f} | {row['cl100k_token_saved_pct']:.2f} | "
            f"{row['legacy_local_apply_ms_mean']:.4f} | {row['compact_local_apply_ms_mean']:.4f} |"
        )
    lines.append("")
    lines.append("## Zero Call")
    lines.append(
        f"- Step4 merged mode: token_usage=`{zero_call_summary['token_usage']}`, "
        f"sentence_count=`{zero_call_summary['sentence_count']}`, "
        f"latency_mean=`{zero_call_summary['latency_ms_mean']:.4f}ms`, "
        f"latency_p95=`{zero_call_summary['latency_ms_p95']:.4f}ms`"
    )
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="LLM 最小补丁协议 payload benchmark")
    parser.add_argument("--output-root", default="var/artifacts/benchmarks", help="产物根目录")
    parser.add_argument("--task-name", default="llm_patch_protocol_payload_benchmark", help="任务名称")
    parser.add_argument("--warmup-runs", type=int, default=200, help="每条本地回放函数预热次数")
    parser.add_argument("--measured-runs", type=int, default=1000, help="每条本地回放函数测量次数")
    args = parser.parse_args()

    output_dir = Path(args.output_root) / f"{args.task_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    raw_dir = output_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    metadata = {
        "generated_at": _now_iso(),
        "output_dir": str(output_dir.resolve()),
        "warmup_runs": int(args.warmup_runs),
        "measured_runs": int(args.measured_runs),
        "tokenizer": "tiktoken.cl100k_base",
        "cmd": " ".join(os.sys.argv),
        "python": os.sys.version,
    }
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    cases = [
        _build_img_desc_case(),
        _build_step35_case(),
        _build_step35_local_fill_case(),
        _build_step4_case(),
    ]
    summary_rows = [
        _summarize_case(
            case,
            warmup_runs=int(args.warmup_runs),
            measured_runs=int(args.measured_runs),
        )
        for case in cases
    ]
    zero_call_summary = _run_step4_zero_call_validation(
        warmup_runs=max(1, int(args.warmup_runs) // 20),
        measured_runs=max(10, int(args.measured_runs) // 20),
    )

    (raw_dir / "summary_rows.json").write_text(
        json.dumps(_jsonable(summary_rows), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (raw_dir / "zero_call_summary.json").write_text(
        json.dumps(_jsonable(zero_call_summary), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    _write_csv(raw_dir / "summary_rows.csv", summary_rows)
    _write_report(
        report_path=output_dir / "report.md",
        metadata=metadata,
        summary_rows=summary_rows,
        zero_call_summary=zero_call_summary,
        output_dir=output_dir,
    )
    print("=== Benchmark 完成 ===")
    print(f"输出目录: {output_dir.resolve()}")


if __name__ == "__main__":
    main()
