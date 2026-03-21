from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from services.python_grpc.src.content_pipeline.markdown_enhancer import (  # noqa: E402
    EnhancedSection,
    MarkdownEnhancer,
)
from services.python_grpc.src.transcript_pipeline.nodes import phase2_preprocessing  # noqa: E402
from services.python_grpc.src.transcript_pipeline.nodes.step_contracts import (  # noqa: E402
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


@dataclass
class _ScenarioResult:
    scenario: str
    category: str
    baseline_mode: str
    baseline_safe: bool
    current_safe: bool
    llm_calls_avoided: bool
    expected_behavior: str
    baseline_output_preview: str
    current_output_preview: str
    notes: str


def _preview(text: str, limit: int = 120) -> str:
    value = str(text or "")
    if len(value) <= limit:
        return value
    if limit <= 3:
        return value[:limit]
    return value[: limit - 3] + "..."


def _run_img_desc_valid_patch() -> _ScenarioResult:
    base_text = "先打开终端执行命令。然后查看日志。"
    payload_text = (
        '{"p":['
        '{"m":"r","o":"执行命令","n":"执行 `npm run dev` 命令","l":"先打开终端","r":"。然后查看日志。"},'
        '{"m":"a","n":" 并确认端口为 `3000`","l":"然后查看日志","r":"。","p":"after"}'
        ']}'
    )
    current_output, _ = MarkdownEnhancer._apply_img_desc_incremental_ops(base_text, payload_text)
    expected = "先打开终端执行 `npm run dev` 命令。然后查看日志 并确认端口为 `3000`。"
    return _ScenarioResult(
        scenario="img_desc_valid_patch",
        category="valid_change",
        baseline_mode="N/A",
        baseline_safe=True,
        current_safe=current_output == expected,
        llm_calls_avoided=False,
        expected_behavior="合法补丁应被正确执行",
        baseline_output_preview="(not compared)",
        current_output_preview=_preview(current_output),
        notes="证明补丁协议不是只会保守拒绝，合法增量修改可以正常落地。",
    )


def _run_img_desc_ambiguous_guard() -> _ScenarioResult:
    base_text = "先执行命令，再执行命令。"
    # 基线：若直接相信整段回写，模型会把两个命中位都一起改掉。
    baseline_output = "先执行 `npm run dev` 命令，再执行 `npm run dev` 命令。"
    payload_text = '{"p":[{"m":"r","o":"执行命令","n":"执行 `npm run dev` 命令"}]}'
    current_output, _ = MarkdownEnhancer._apply_img_desc_incremental_ops(base_text, payload_text)
    expected = base_text
    return _ScenarioResult(
        scenario="img_desc_ambiguous_guard",
        category="unsafe_change",
        baseline_mode="legacy_full_text_rewrite",
        baseline_safe=baseline_output == expected,
        current_safe=current_output == expected,
        llm_calls_avoided=False,
        expected_behavior="定位歧义时保持原文，不做不确定修改",
        baseline_output_preview=_preview(baseline_output),
        current_output_preview=_preview(current_output),
        notes="旧整段回写会放大改动范围；当前补丁协议在无法唯一定位时直接拒绝应用。",
    )


def _run_step35_metadata_guard() -> _ScenarioResult:
    source = {
        "sentence_id": "S001",
        "text": "first sentence",
        "start_sec": 1.0,
        "end_sec": 2.0,
        "source_subtitle_ids": ["SUB001"],
    }
    # 基线：如果直接信任模型返回，会把时间轴与来源元数据一起污染。
    legacy_payload = {
        "translated_sentences": [
            {
                "sentence_id": "S001",
                "translated_text": "第一句译文",
                "start_sec": 99.0,
                "end_sec": 199.0,
                "source_subtitle_ids": ["BAD001"],
            }
        ]
    }
    baseline_output = legacy_payload["translated_sentences"][0]

    compact_payload = {"t": [{"sid": "S001", "tt": "第一句译文"}]}
    translated_by_id, _ = parse_step35_translated_sentences(
        compact_payload,
        valid_sentence_ids={"S001"},
    )
    current_output = {
        "sentence_id": "S001",
        "text": translated_by_id["S001"],
        "start_sec": source["start_sec"],
        "end_sec": source["end_sec"],
        "source_subtitle_ids": list(source["source_subtitle_ids"]),
    }
    current_safe = (
        current_output["start_sec"] == 1.0
        and current_output["end_sec"] == 2.0
        and current_output["source_subtitle_ids"] == ["SUB001"]
    )
    baseline_safe = (
        baseline_output["start_sec"] == 1.0
        and baseline_output["end_sec"] == 2.0
        and baseline_output["source_subtitle_ids"] == ["SUB001"]
    )
    return _ScenarioResult(
        scenario="step35_metadata_guard",
        category="unsafe_change",
        baseline_mode="legacy_remote_metadata_accept",
        baseline_safe=baseline_safe,
        current_safe=current_safe,
        llm_calls_avoided=False,
        expected_behavior="翻译结果允许变化，但时间轴和来源元数据必须保持本地权威值",
        baseline_output_preview=_preview(json.dumps(baseline_output, ensure_ascii=False)),
        current_output_preview=_preview(json.dumps(current_output, ensure_ascii=False)),
        notes="当前链路只接受句子编号和译文，本地重新回填时间轴与来源字段。",
    )


def _run_step4_bilingual_guard() -> _ScenarioResult:
    source_sentences = [
        {
            "sentence_id": "S002",
            "text": "第二句讲智能体（agent）会调用工具",
            "start_sec": 2.0,
            "end_sec": 2.5,
            "source_subtitle_ids": ["SUB002"],
        }
    ]
    # 基线：若直接相信整句 cleaned_text，关键双语术语会被误删。
    baseline_output = "第二句讲智能体会调用工具"
    compact_payload = {
        "d": [
            {
                "sid": "S002",
                "o": "（agent）",
                "l": "第二句讲智能体",
                "r": "会调用工具",
            }
        ]
    }
    cleaned_by_id, _ = parse_step4_cleaned_sentences(
        compact_payload,
        valid_sentence_ids={"S002"},
    )
    current_rows, _ = assemble_step4_cleaned_sentences(
        source_sentences,
        llm_cleaned_by_id=cleaned_by_id,
        glossary_guard=phase2_preprocessing._drops_cjk_en_glossary_pair,
    )
    current_output = current_rows[0]["cleaned_text"]
    expected = source_sentences[0]["text"]
    return _ScenarioResult(
        scenario="step4_bilingual_guard",
        category="unsafe_change",
        baseline_mode="legacy_full_sentence_cleaned_text",
        baseline_safe=baseline_output == expected,
        current_safe=current_output == expected,
        llm_calls_avoided=False,
        expected_behavior="关键双语术语被误删时，应回退到原句而不是直接接受模型输出",
        baseline_output_preview=_preview(baseline_output),
        current_output_preview=_preview(current_output),
        notes="当前清理链路在本地执行补丁后还会经过双语术语保护，防止误删核心术语。",
    )


def _run_img_desc_no_evidence_skip() -> _ScenarioResult:
    class _CaptureLLM:
        def __init__(self) -> None:
            self.calls = 0

        async def complete_text(self, prompt: str, system_message: str = None):
            self.calls += 1
            _ = prompt
            _ = system_message
            return "unsafe rewritten body", None, None

    enhancer = MarkdownEnhancer()
    enhancer._enabled = True
    enhancer._enable_img_desc_text_augment = True
    cap = _CaptureLLM()
    enhancer._llm_client = cap

    section = EnhancedSection(
        unit_id="SU410",
        title="Concrete Unit",
        knowledge_type="concrete",
        original_body="原始正文",
        augment_screenshot_items=[
            {
                "img_id": "SU410_img_01",
                "img_path": "assets/SU410_img_01.png",
                "img_description": "head",
                "label": "head",
                "timestamp_sec": 1.1,
                "sentence_id": "S001",
                "sentence_text": "打开配置",
            }
        ],
    )
    result = asyncio.run(
        enhancer._augment_body_with_image_descriptions(
            section,
            section.original_body,
            [],
        )
    )
    return _ScenarioResult(
        scenario="img_desc_no_evidence_skip_call",
        category="call_avoidance",
        baseline_mode="legacy_call_without_evidence",
        baseline_safe=False,
        current_safe=result == section.original_body and cap.calls == 0,
        llm_calls_avoided=cap.calls == 0,
        expected_behavior="没有有效对齐证据时，不应让模型参与生成",
        baseline_output_preview="legacy would still have a chance to rewrite body",
        current_output_preview=f"result={result}, llm_calls={cap.calls}",
        notes="这项验证的是“减少幻觉机会”而不是“拦截错误输出”：无证据直接不调用模型。",
    )


def _run_step4_zero_call() -> _ScenarioResult:
    class _NoCallLLM:
        def __init__(self) -> None:
            self.calls = 0

    llm_factory_calls = {"count": 0}

    def _fake_create_llm(*_args, **_kwargs):
        llm_factory_calls["count"] += 1
        return _NoCallLLM()

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

    original_create_llm = phase2_preprocessing.create_llm_client
    original_get_logger = phase2_preprocessing.get_logger
    phase2_preprocessing.create_llm_client = _fake_create_llm
    phase2_preprocessing.get_logger = lambda *_args, **_kwargs: _DummyLogger()
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            merged_sentences = [
                {
                    "sentence_id": f"S{index:03d}",
                    "text": f"第{index}句清理后正文",
                    "start_sec": float(index),
                    "end_sec": float(index) + 0.5,
                    "source_subtitle_ids": [f"SUB{index:03d}"],
                }
                for index in range(1, 6)
            ]
            state = {
                "output_dir": temp_dir,
                "translated_sentences": merged_sentences,
                phase2_preprocessing.STEP2_STEP4_MERGED_STATE_FLAG: True,
                "_disable_stage1_artifact_persistence": True,
            }
            result = asyncio.run(phase2_preprocessing.step4_node(state))
            current_safe = (
                llm_factory_calls["count"] == 0
                and int(result["token_usage"]["step4_clean_local"]) == 0
            )
            return _ScenarioResult(
                scenario="step4_merged_mode_zero_call",
                category="call_avoidance",
                baseline_mode="legacy_step4_always_calls_llm",
                baseline_safe=False,
                current_safe=current_safe,
                llm_calls_avoided=llm_factory_calls["count"] == 0,
                expected_behavior="纠错与清理合并模式命中后，清理阶段应完全跳过模型调用",
                baseline_output_preview="legacy would still allocate one LLM call",
                current_output_preview=f"llm_factory_calls={llm_factory_calls['count']}, token_usage={result['token_usage']['step4_clean_local']}",
                notes="这类零调用直接消除了该阶段的幻觉机会。",
            )
    finally:
        phase2_preprocessing.create_llm_client = original_create_llm
        phase2_preprocessing.get_logger = original_get_logger


def _write_report(
    *,
    report_path: Path,
    metadata: Dict[str, Any],
    rows: List[Dict[str, Any]],
    summary: Dict[str, Any],
    output_dir: Path,
) -> None:
    lines: List[str] = []
    lines.append("# LLM Hallucination Guardrail Benchmark")
    lines.append("")
    lines.append(f"- 生成时间: {metadata['generated_at']}")
    lines.append(f"- 原始数据目录: `{output_dir / 'raw'}`")
    lines.append("")
    lines.append("## 方法")
    lines.append("- 用机制级对照场景验证：旧协议若直接信任整段文本/冗长字段，会不会放行误改；当前协议是否能通过最小补丁、本地回填和保护回退拦住这些风险。")
    lines.append("- 指标拆成三类：unsafe_change、防止误改；call_avoidance、减少无证据调用；valid_change、确认新协议仍可正常落地合法修改。")
    lines.append("")
    lines.append("## 汇总")
    lines.append("| scenario | category | baseline_safe | current_safe | llm_calls_avoided |")
    lines.append("|---|---|---:|---:|---:|")
    for row in rows:
        lines.append(
            f"| {row['scenario']} | {row['category']} | {int(bool(row['baseline_safe']))} | "
            f"{int(bool(row['current_safe']))} | {int(bool(row['llm_calls_avoided']))} |"
        )
    lines.append("")
    lines.append("## Summary")
    lines.append(f"- unsafe_case_count: `{summary['unsafe_case_count']}`")
    lines.append(f"- legacy_safe_rate_percent: `{summary['legacy_safe_rate_percent']:.2f}`")
    lines.append(f"- current_safe_rate_percent: `{summary['current_safe_rate_percent']:.2f}`")
    lines.append(f"- call_avoidance_case_count: `{summary['call_avoidance_case_count']}`")
    lines.append(f"- call_avoidance_success_rate_percent: `{summary['call_avoidance_success_rate_percent']:.2f}`")
    lines.append(f"- valid_case_count: `{summary['valid_case_count']}`")
    lines.append(f"- valid_case_success_rate_percent: `{summary['valid_case_success_rate_percent']:.2f}`")
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="LLM 幻觉风险保护机制 benchmark")
    parser.add_argument("--output-root", default="var/artifacts/benchmarks", help="产物根目录")
    parser.add_argument("--task-name", default="llm_hallucination_guardrail_benchmark", help="任务名称")
    args = parser.parse_args()

    output_dir = Path(args.output_root) / f"{args.task_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    raw_dir = output_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    metadata = {
        "generated_at": _now_iso(),
        "output_dir": str(output_dir.resolve()),
        "cmd": " ".join(os.sys.argv),
        "python": os.sys.version,
    }
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    scenario_results = [
        _run_img_desc_valid_patch(),
        _run_img_desc_ambiguous_guard(),
        _run_step35_metadata_guard(),
        _run_step4_bilingual_guard(),
        _run_img_desc_no_evidence_skip(),
        _run_step4_zero_call(),
    ]
    rows = [row.__dict__ for row in scenario_results]

    unsafe_rows = [row for row in rows if row["category"] == "unsafe_change"]
    call_rows = [row for row in rows if row["category"] == "call_avoidance"]
    valid_rows = [row for row in rows if row["category"] == "valid_change"]
    summary = {
        "unsafe_case_count": len(unsafe_rows),
        "legacy_safe_rate_percent": (
            sum(1 for row in unsafe_rows if row["baseline_safe"]) / len(unsafe_rows) * 100.0
            if unsafe_rows
            else 0.0
        ),
        "current_safe_rate_percent": (
            sum(1 for row in unsafe_rows if row["current_safe"]) / len(unsafe_rows) * 100.0
            if unsafe_rows
            else 0.0
        ),
        "call_avoidance_case_count": len(call_rows),
        "call_avoidance_success_rate_percent": (
            sum(1 for row in call_rows if row["llm_calls_avoided"]) / len(call_rows) * 100.0
            if call_rows
            else 0.0
        ),
        "valid_case_count": len(valid_rows),
        "valid_case_success_rate_percent": (
            sum(1 for row in valid_rows if row["current_safe"]) / len(valid_rows) * 100.0
            if valid_rows
            else 0.0
        ),
    }

    (raw_dir / "scenario_rows.json").write_text(
        json.dumps(_jsonable(rows), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (raw_dir / "summary.json").write_text(
        json.dumps(_jsonable(summary), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    _write_csv(raw_dir / "scenario_rows.csv", rows)
    _write_report(
        report_path=output_dir / "report.md",
        metadata=metadata,
        rows=rows,
        summary=summary,
        output_dir=output_dir,
    )
    print("=== Benchmark 完成 ===")
    print(f"输出目录: {output_dir.resolve()}")


if __name__ == "__main__":
    main()
