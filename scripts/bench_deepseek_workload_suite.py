from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.bench_common.report_builder import (
    ensure_benchmark_dirs,
    now_iso,
    parse_int_list,
    percentile,
    write_csv,
    write_json,
)
from services.python_grpc.src.config_paths import load_yaml_dict, resolve_video_config_path
from services.python_grpc.src.content_pipeline.infra.llm.prompt_loader import get_prompt, render_prompt
from services.python_grpc.src.content_pipeline.infra.llm.prompt_registry import PromptKeys


@dataclass(frozen=True)
class WorkloadScenario:
    scenario_id: str
    name: str
    mode: str  # "json" | "text"
    system_prompt: str
    user_prompt: str
    max_tokens: int
    temperature: float
    response_format_json: bool


def _resolve_deepseek_config() -> Tuple[str, str, str]:
    config_path = resolve_video_config_path(anchor_file=__file__)
    config_obj = load_yaml_dict(config_path) if config_path else {}
    ai_conf = config_obj.get("ai", {}) if isinstance(config_obj, dict) else {}
    refinement_conf = ai_conf.get("refinement", {}) if isinstance(ai_conf, dict) else {}
    api_key = os.getenv("DEEPSEEK_API_KEY") or str(ai_conf.get("api_key", "") or "")
    base_url = str(ai_conf.get("base_url", "https://api.deepseek.com") or "https://api.deepseek.com")
    model = str(refinement_conf.get("model", "deepseek-chat") or "deepseek-chat")
    return api_key, base_url, model


def _status_bucket(status_code: int) -> str:
    if status_code == 200:
        return "ok"
    if status_code == 429:
        return "rate_limit_429"
    if 500 <= status_code <= 599:
        return "server_5xx"
    if 400 <= status_code <= 499:
        return "client_4xx"
    return "other_http"


def _extract_text_content(body: Dict[str, Any]) -> str:
    try:
        choices = body.get("choices", [])
        if not choices:
            return ""
        message = choices[0].get("message", {})
        content = message.get("content", "")
        return str(content or "")
    except Exception:
        return ""


def _safe_json_parse(text: str) -> bool:
    if not isinstance(text, str) or not text.strip():
        return False
    raw = text.strip()
    candidates = [raw]
    if "```" in raw:
        trimmed = raw.replace("```json", "").replace("```", "").strip()
        candidates.append(trimmed)
    for cand in candidates:
        try:
            json.loads(cand)
            return True
        except Exception:
            continue
    return False


def _build_paragraphs_json(count: int = 12) -> str:
    paragraphs: List[Dict[str, Any]] = []
    for idx in range(1, count + 1):
        paragraphs.append(
            {
                "paragraph_id": f"P{idx:03d}",
                "text": (
                    f"第{idx}段：讲解 AI 编程助手在项目中的使用方法，"
                    "包括问题拆解、代码生成、错误定位与回归验证。"
                ),
            }
        )
    return json.dumps(paragraphs, ensure_ascii=False)


def _build_kc_batch_content(count: int = 10) -> str:
    rows: List[str] = []
    for idx in range(1, count + 1):
        rows.append(
            "\n".join(
                [
                    "---",
                    f"ID: {idx}",
                    f"时间: {idx * 2.0:.1f}-{idx * 2.0 + 1.5:.1f}",
                    (
                        "字幕: 先在配置文件中设置并发上限，再运行批处理任务，"
                        "观察响应延迟、失败率和输出质量。"
                    ),
                ]
            )
        )
    return "\n".join(rows)


def _build_kc_multi_units_json(unit_count: int = 4, actions_per_unit: int = 6) -> str:
    units: List[Dict[str, Any]] = []
    action_cursor = 1
    for uidx in range(1, unit_count + 1):
        actions: List[Dict[str, Any]] = []
        for aidx in range(1, actions_per_unit + 1):
            actions.append(
                {
                    "id": f"action_{aidx}",
                    "start": float(action_cursor),
                    "end": float(action_cursor) + 0.8,
                    "subtitles": (
                        f"第{uidx}单元第{aidx}步：检查日志，定位失败请求，"
                        "确认是否为429、超时、或结构化输出异常。"
                    ),
                }
            )
            action_cursor += 1
        units.append(
            {
                "unit_id": f"SU{uidx:03d}",
                "title": f"并发压测单元{uidx}",
                "full_text": (
                    "本单元聚焦并发压测与稳定性验证，要求在满足成功率的前提下最大化吞吐。"
                ),
                "actions": actions,
            }
        )
    return json.dumps(units, ensure_ascii=False, separators=(",", ":"))


def _build_scenarios() -> List[WorkloadScenario]:
    scenarios: List[WorkloadScenario] = []

    segment_system = get_prompt(PromptKeys.DEEPSEEK_SEMANTIC_SEGMENT_SYSTEM)
    segment_user = render_prompt(
        PromptKeys.DEEPSEEK_SEMANTIC_SEGMENT_USER,
        context={"paragraphs_json": _build_paragraphs_json(18)},
    )
    scenarios.append(
        WorkloadScenario(
            scenario_id="cp_semantic_segment_json",
            name="ContentPipeline Semantic Segment",
            mode="json",
            system_prompt=segment_system,
            user_prompt=segment_user,
            max_tokens=900,
            temperature=0.1,
            response_format_json=True,
        )
    )

    resegment_system = get_prompt(PromptKeys.DEEPSEEK_SEMANTIC_RESEGMENT_SYSTEM)
    resegment_user = render_prompt(
        PromptKeys.DEEPSEEK_SEMANTIC_RESEGMENT_USER,
        context={
            "unit_id": "SU009",
            "text": "该语义单元描述了从日志观察到修复完成的全过程，需要判断是否应拆分。",
            "start_sec": 120.0,
            "end_sec": 168.0,
            "llm_type": "过程性知识",
            "s_stable": 0.22,
            "s_action": 0.81,
            "s_redundant": 0.15,
            "anchors": ["A1", "A2", "A3"],
            "reason": "cross_modal_conflict",
        },
    )
    scenarios.append(
        WorkloadScenario(
            scenario_id="cp_semantic_resegment_json",
            name="ContentPipeline Semantic Resegment",
            mode="json",
            system_prompt=resegment_system,
            user_prompt=resegment_user,
            max_tokens=400,
            temperature=0.1,
            response_format_json=True,
        )
    )

    kc_batch_system = get_prompt(PromptKeys.DEEPSEEK_KC_BATCH_SYSTEM)
    kc_batch_user = render_prompt(
        PromptKeys.DEEPSEEK_KC_BATCH_USER,
        context={
            "title": "AI编程助手并发压测实践",
            "full_text": "本文讲解并发压测中的吞吐、时延、错误率以及稳定性权衡。",
            "batch_content": _build_kc_batch_content(16),
        },
    )
    scenarios.append(
        WorkloadScenario(
            scenario_id="cp_kc_batch_text",
            name="ContentPipeline KnowledgeClassifier Batch",
            mode="text",
            system_prompt=kc_batch_system,
            user_prompt=kc_batch_user,
            max_tokens=900,
            temperature=0.0,
            response_format_json=False,
        )
    )

    kc_multi_user = render_prompt(
        PromptKeys.DEEPSEEK_KC_MULTI_UNIT_USER,
        context={"units_json": _build_kc_multi_units_json(5, 8)},
    )
    scenarios.append(
        WorkloadScenario(
            scenario_id="cp_kc_multi_unit_text",
            name="ContentPipeline KnowledgeClassifier MultiUnit",
            mode="text",
            system_prompt=kc_batch_system,
            user_prompt=kc_multi_user,
            max_tokens=1200,
            temperature=0.0,
            response_format_json=False,
        )
    )

    md_combined_system = get_prompt(PromptKeys.DEEPSEEK_MD_COMBINED_SYSTEM)
    md_combined_user = render_prompt(
        PromptKeys.DEEPSEEK_MD_COMBINED_USER,
        context={
            "title": "并发压测结果分析",
            "level_info": "当前层级: 二级(子知识点), 父节点: PARENT_01",
            "body_text": (
                "我们先看基线并发配置，然后逐步提高并发，观察总时长与错误率。"
                "如果总时长下降且无429/超时，就继续上探。"
            ),
            "ocr_text": "图中显示并发48时吞吐明显提高，64时达到稳定峰值。",
            "action_info": (
                "- [过程性] 并发分档压测: 依次运行12/16/20/32/48/64\n"
                "- [讲解型] 指标解释: 关注QPS、P95和失败率"
            ),
        },
    )
    scenarios.append(
        WorkloadScenario(
            scenario_id="cp_markdown_combined_json",
            name="ContentPipeline MarkdownEnhancer Combined",
            mode="json",
            system_prompt=md_combined_system,
            user_prompt=md_combined_user,
            max_tokens=1200,
            temperature=0.2,
            response_format_json=True,
        )
    )

    motion_system = get_prompt(PromptKeys.DEEPSEEK_VIDEO_CLIP_MOTION_VALUE_SYSTEM)
    motion_user = (
        "Context: Educational video segment. ASR Text: \"这里演示如何在IDE里逐步排查异常并验证修复效果。\" "
        "Visual: Detected 4 smooth motion segments. "
        "Question: Is dynamic motion essential or optional? "
        "Answer only ESSENTIAL or OPTIONAL."
    )
    scenarios.append(
        WorkloadScenario(
            scenario_id="cp_video_clip_motion_text",
            name="ContentPipeline VideoClip MotionValue",
            mode="text",
            system_prompt=motion_system,
            user_prompt=motion_user,
            max_tokens=64,
            temperature=0.0,
            response_format_json=False,
        )
    )

    transition_system = get_prompt(PromptKeys.DEEPSEEK_VIDEO_CLIP_TRANSITION_SYSTEM)
    transition_user = (
        "请根据以下知识点生成一句短过渡语。\n"
        "知识点：通过并发压测确定DeepSeek最佳并发窗口，兼顾吞吐与稳定性。\n"
        "视频时长：8.0秒\n"
        "要求：一句话，不超过20个汉字。"
    )
    scenarios.append(
        WorkloadScenario(
            scenario_id="cp_video_clip_transition_text",
            name="ContentPipeline VideoClip TransitionText",
            mode="text",
            system_prompt=transition_system,
            user_prompt=transition_user,
            max_tokens=96,
            temperature=0.2,
            response_format_json=False,
        )
    )

    return scenarios


async def _run_case(
    *,
    scenario: WorkloadScenario,
    concurrency: int,
    total_requests: int,
    api_key: str,
    base_url: str,
    model: str,
    timeout_sec: float,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    timeout = httpx.Timeout(timeout_sec, connect=min(10.0, timeout_sec))
    limits = httpx.Limits(
        max_connections=max(20, concurrency * 2),
        max_keepalive_connections=max(10, concurrency),
        keepalive_expiry=30.0,
    )

    payload: Dict[str, Any] = {
        "model": model,
        "messages": [
            {"role": "system", "content": scenario.system_prompt},
            {"role": "user", "content": scenario.user_prompt},
        ],
        "temperature": float(scenario.temperature),
        "max_tokens": int(scenario.max_tokens),
    }
    if scenario.response_format_json:
        payload["response_format"] = {"type": "json_object"}

    request_records: List[Optional[Dict[str, Any]]] = [None] * total_requests
    queue: "asyncio.Queue[Optional[int]]" = asyncio.Queue()
    for index in range(total_requests):
        queue.put_nowait(index)
    for _ in range(concurrency):
        queue.put_nowait(None)

    case_started_at = time.perf_counter()

    async with httpx.AsyncClient(
        base_url=base_url,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        timeout=timeout,
        limits=limits,
        http2=True,
    ) as client:
        async def _worker() -> None:
            while True:
                index = await queue.get()
                if index is None:
                    queue.task_done()
                    return
                request_started_at = time.perf_counter()
                try:
                    response = await client.post("/chat/completions", json=payload)
                    elapsed_ms = (time.perf_counter() - request_started_at) * 1000.0
                    bucket = _status_bucket(int(response.status_code))
                    record: Dict[str, Any] = {
                        "request_index": int(index),
                        "status_code": int(response.status_code),
                        "status_bucket": bucket,
                        "elapsed_ms": elapsed_ms,
                        "http_success": response.status_code == 200,
                        "json_parse_ok": None,
                        "error": "",
                    }
                    if response.status_code == 200:
                        try:
                            body = response.json()
                        except Exception:
                            body = {}
                        usage = body.get("usage", {}) if isinstance(body, dict) else {}
                        record["prompt_tokens"] = int(usage.get("prompt_tokens", 0) or 0)
                        record["completion_tokens"] = int(usage.get("completion_tokens", 0) or 0)
                        record["total_tokens"] = int(
                            usage.get(
                                "total_tokens",
                                int(record["prompt_tokens"]) + int(record["completion_tokens"]),
                            )
                            or 0
                        )
                        if scenario.mode == "json":
                            content_text = _extract_text_content(body)
                            record["json_parse_ok"] = _safe_json_parse(content_text)
                    else:
                        record["prompt_tokens"] = 0
                        record["completion_tokens"] = 0
                        record["total_tokens"] = 0
                        snippet = response.text[:300] if isinstance(response.text, str) else ""
                        record["error"] = snippet
                    request_records[index] = record
                except httpx.TimeoutException as exc:
                    elapsed_ms = (time.perf_counter() - request_started_at) * 1000.0
                    request_records[index] = {
                        "request_index": int(index),
                        "status_code": 0,
                        "status_bucket": "timeout",
                        "elapsed_ms": elapsed_ms,
                        "http_success": False,
                        "json_parse_ok": None,
                        "prompt_tokens": 0,
                        "completion_tokens": 0,
                        "total_tokens": 0,
                        "error": str(exc),
                    }
                except Exception as exc:
                    elapsed_ms = (time.perf_counter() - request_started_at) * 1000.0
                    request_records[index] = {
                        "request_index": int(index),
                        "status_code": 0,
                        "status_bucket": "network_or_unknown_error",
                        "elapsed_ms": elapsed_ms,
                        "http_success": False,
                        "json_parse_ok": None,
                        "prompt_tokens": 0,
                        "completion_tokens": 0,
                        "total_tokens": 0,
                        "error": str(exc),
                    }
                finally:
                    queue.task_done()

        workers = [asyncio.create_task(_worker()) for _ in range(concurrency)]
        await queue.join()
        await asyncio.gather(*workers)

    case_elapsed_ms = (time.perf_counter() - case_started_at) * 1000.0
    valid_records = [item for item in request_records if isinstance(item, dict)]
    if len(valid_records) != total_requests:
        raise RuntimeError("压测记录缺失，无法生成可靠结果")

    ok_records = [item for item in valid_records if item["status_bucket"] == "ok"]
    timeout_records = [item for item in valid_records if item["status_bucket"] == "timeout"]
    rate_429_records = [item for item in valid_records if item["status_bucket"] == "rate_limit_429"]
    other_http_records = [
        item
        for item in valid_records
        if item["status_bucket"] in {"server_5xx", "client_4xx", "other_http"}
    ]
    network_records = [
        item for item in valid_records if item["status_bucket"] == "network_or_unknown_error"
    ]

    parse_fail_count = 0
    parse_total = 0
    if scenario.mode == "json":
        for item in ok_records:
            flag = item.get("json_parse_ok")
            if flag is None:
                continue
            parse_total += 1
            if not bool(flag):
                parse_fail_count += 1

    elapsed_values = [float(item["elapsed_ms"]) for item in valid_records]
    ok_elapsed_values = [float(item["elapsed_ms"]) for item in ok_records]

    summary = {
        "scenario_id": scenario.scenario_id,
        "scenario_name": scenario.name,
        "mode": scenario.mode,
        "concurrency": int(concurrency),
        "total_requests": int(total_requests),
        "ok_count": len(ok_records),
        "rate_limit_429_count": len(rate_429_records),
        "timeout_count": len(timeout_records),
        "other_http_error_count": len(other_http_records),
        "network_error_count": len(network_records),
        "json_parse_fail_count": int(parse_fail_count),
        "json_parse_total_count": int(parse_total),
        "success_rate_percent": (len(ok_records) / total_requests * 100.0) if total_requests else 0.0,
        "json_parse_success_rate_percent": (
            (1.0 - parse_fail_count / parse_total) * 100.0 if parse_total > 0 else 100.0
        ),
        "case_total_elapsed_ms": case_elapsed_ms,
        "avg_request_elapsed_ms": float(statistics.fmean(elapsed_values)) if elapsed_values else 0.0,
        "p95_request_elapsed_ms": percentile(elapsed_values, 95) if elapsed_values else 0.0,
        "avg_success_elapsed_ms": float(statistics.fmean(ok_elapsed_values)) if ok_elapsed_values else 0.0,
        "p95_success_elapsed_ms": percentile(ok_elapsed_values, 95) if ok_elapsed_values else 0.0,
        "total_tokens_success_only": int(sum(int(item.get("total_tokens", 0) or 0) for item in ok_records)),
        "requests_per_second_case_level": (
            total_requests / (case_elapsed_ms / 1000.0) if case_elapsed_ms > 0 else 0.0
        ),
    }
    return summary, valid_records


def _pick_best_concurrency(rows: List[Dict[str, Any]], mode: str) -> Dict[str, Any]:
    if not rows:
        return {}
    perfect_rows: List[Dict[str, Any]] = []
    for row in rows:
        is_perfect = (
            int(row.get("ok_count", 0)) == int(row.get("total_requests", 0))
            and int(row.get("rate_limit_429_count", 0)) == 0
            and int(row.get("timeout_count", 0)) == 0
            and int(row.get("other_http_error_count", 0)) == 0
            and int(row.get("network_error_count", 0)) == 0
        )
        if mode == "json":
            is_perfect = is_perfect and int(row.get("json_parse_fail_count", 0)) == 0
        if is_perfect:
            perfect_rows.append(row)

    target = perfect_rows if perfect_rows else rows
    target = sorted(
        target,
        key=lambda item: (
            float(item.get("requests_per_second_case_level", 0.0)),
            -float(item.get("p95_success_elapsed_ms", item.get("p95_request_elapsed_ms", 0.0))),
            -int(item.get("concurrency", 0)),
        ),
        reverse=True,
    )
    return target[0]


def _write_report(
    *,
    output_path: Path,
    metadata: Dict[str, Any],
    scenario_rows: List[Dict[str, Any]],
    best_rows: List[Dict[str, Any]],
) -> None:
    lines: List[str] = []
    lines.append("# DeepSeek 全场景并发压测报告")
    lines.append("")
    lines.append(f"- 生成时间: {metadata['generated_at']}")
    lines.append(f"- 基础URL: `{metadata['base_url']}`")
    lines.append(f"- 模型: `{metadata['model']}`")
    lines.append(f"- 并发阶梯: `{metadata['concurrency_ladder']}`")
    lines.append(f"- 每档请求数: `{metadata['requests_per_case']}`")
    lines.append(f"- 覆盖场景数: `{metadata['scenario_count']}`")
    lines.append("")
    lines.append("## 各场景最佳并发")
    lines.append("| 场景ID | 模式 | 推荐并发 | 成功率(%) | 429 | timeout | 总时长(ms) | P95成功时延(ms) | QPS(case) |")
    lines.append("|---|---|---:|---:|---:|---:|---:|---:|---:|")
    for row in sorted(best_rows, key=lambda item: str(item.get("scenario_id", ""))):
        lines.append(
            f"| {row.get('scenario_id', '')} | {row.get('mode', '')} | {int(row.get('concurrency', 0))} | "
            f"{float(row.get('success_rate_percent', 0.0)):.2f} | {int(row.get('rate_limit_429_count', 0))} | "
            f"{int(row.get('timeout_count', 0))} | {float(row.get('case_total_elapsed_ms', 0.0)):.2f} | "
            f"{float(row.get('p95_success_elapsed_ms', row.get('p95_request_elapsed_ms', 0.0))):.2f} | "
            f"{float(row.get('requests_per_second_case_level', 0.0)):.3f} |"
        )
    lines.append("")
    lines.append("## 全部结果")
    lines.append("| 场景ID | 并发 | 成功率(%) | 429 | timeout | 其他HTTP | 网络错误 | JSON解析失败 | 总时长(ms) | QPS(case) |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for row in sorted(scenario_rows, key=lambda item: (str(item.get("scenario_id", "")), int(item.get("concurrency", 0)))):
        lines.append(
            f"| {row.get('scenario_id', '')} | {int(row.get('concurrency', 0))} | "
            f"{float(row.get('success_rate_percent', 0.0)):.2f} | {int(row.get('rate_limit_429_count', 0))} | "
            f"{int(row.get('timeout_count', 0))} | {int(row.get('other_http_error_count', 0))} | "
            f"{int(row.get('network_error_count', 0))} | {int(row.get('json_parse_fail_count', 0))} | "
            f"{float(row.get('case_total_elapsed_ms', 0.0)):.2f} | {float(row.get('requests_per_second_case_level', 0.0)):.3f} |"
        )
    lines.append("")
    lines.append("## 产物")
    lines.append("- `metadata.json`")
    lines.append("- `raw/all_summaries.json`")
    lines.append("- `raw/all_summaries.csv`")
    lines.append("- `raw/best_concurrency_by_scenario.json`")
    lines.append("- `scenarios/<scenario_id>/raw/requests_c*.json`")
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


async def _amain(args: argparse.Namespace) -> int:
    api_key, base_url, model = _resolve_deepseek_config()
    if not api_key or api_key == "your-deepseek-api-key-here":
        raise RuntimeError("DEEPSEEK_API_KEY 未配置，无法执行真实压测")

    concurrency_ladder = parse_int_list(args.concurrency, "concurrency")
    scenarios = _build_scenarios()
    selected_scenarios_raw = str(args.scenarios or "").strip()
    if selected_scenarios_raw:
        selected_ids = {item.strip() for item in selected_scenarios_raw.split(",") if item.strip()}
        scenarios = [scenario for scenario in scenarios if scenario.scenario_id in selected_ids]
        if not scenarios:
            raise ValueError("scenarios 参数未匹配到任何有效场景")

    output_dir, raw_dir, _charts_dir = ensure_benchmark_dirs(args.output_root, args.task_name)
    metadata = {
        "generated_at": now_iso(),
        "base_url": base_url,
        "model": model,
        "concurrency_ladder": concurrency_ladder,
        "requests_per_case": int(args.requests_per_case),
        "timeout_sec": float(args.timeout_sec),
        "scenario_count": len(scenarios),
        "output_dir": str(output_dir.resolve()),
    }
    write_json(output_dir / "metadata.json", metadata)

    all_rows: List[Dict[str, Any]] = []
    best_rows: List[Dict[str, Any]] = []

    for scenario in scenarios:
        scenario_root = output_dir / "scenarios" / scenario.scenario_id
        scenario_raw = scenario_root / "raw"
        scenario_raw.mkdir(parents=True, exist_ok=True)
        print(f"[Scenario] {scenario.scenario_id} ({scenario.mode})")
        scenario_rows: List[Dict[str, Any]] = []
        for concurrency in concurrency_ladder:
            print(f"  [Run] c={concurrency}, requests={args.requests_per_case}")
            summary, request_rows = await _run_case(
                scenario=scenario,
                concurrency=concurrency,
                total_requests=int(args.requests_per_case),
                api_key=api_key,
                base_url=base_url,
                model=model,
                timeout_sec=float(args.timeout_sec),
            )
            summary["started_at"] = now_iso()
            scenario_rows.append(summary)
            write_json(scenario_raw / f"requests_c{concurrency}.json", request_rows)

        scenario_rows.sort(key=lambda item: int(item["concurrency"]))
        write_json(scenario_raw / "summary.json", scenario_rows)
        write_csv(scenario_raw / "summary.csv", scenario_rows)

        best = _pick_best_concurrency(scenario_rows, scenario.mode)
        best_rows.append(best)
        all_rows.extend(scenario_rows)
        print(
            "  [Best] "
            + " | ".join(
                [
                    f"c={int(best.get('concurrency', 0))}",
                    f"ok={int(best.get('ok_count', 0))}/{int(best.get('total_requests', 0))}",
                    f"429={int(best.get('rate_limit_429_count', 0))}",
                    f"timeout={int(best.get('timeout_count', 0))}",
                    f"qps={float(best.get('requests_per_second_case_level', 0.0)):.3f}",
                ]
            )
        )

    all_rows.sort(key=lambda item: (str(item.get("scenario_id", "")), int(item.get("concurrency", 0))))
    write_json(raw_dir / "all_summaries.json", all_rows)
    write_csv(raw_dir / "all_summaries.csv", all_rows)
    write_json(raw_dir / "best_concurrency_by_scenario.json", best_rows)
    _write_report(
        output_path=output_dir / "report.md",
        metadata=metadata,
        scenario_rows=all_rows,
        best_rows=best_rows,
    )

    print("\n=== DeepSeek 全场景并发压测完成 ===")
    print(f"输出目录: {output_dir.resolve()}")
    for row in sorted(best_rows, key=lambda item: str(item.get("scenario_id", ""))):
        print(
            " | ".join(
                [
                    f"{row.get('scenario_id', '')}",
                    f"c={int(row.get('concurrency', 0))}",
                    f"ok={int(row.get('ok_count', 0))}/{int(row.get('total_requests', 0))}",
                    f"429={int(row.get('rate_limit_429_count', 0))}",
                    f"timeout={int(row.get('timeout_count', 0))}",
                    f"total_ms={float(row.get('case_total_elapsed_ms', 0.0)):.2f}",
                    f"qps={float(row.get('requests_per_second_case_level', 0.0)):.3f}",
                ]
            )
        )
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="DeepSeek 全场景并发压测（content_pipeline）")
    parser.add_argument("--concurrency", default="12,16,20,24,32,40,48,56,64", help="并发阶梯，逗号分隔")
    parser.add_argument("--requests-per-case", type=int, default=18, help="每档并发请求总数")
    parser.add_argument("--timeout-sec", type=float, default=60.0, help="单请求超时（秒）")
    parser.add_argument(
        "--scenarios",
        default="",
        help="仅运行指定场景ID，逗号分隔；为空表示运行全部场景",
    )
    parser.add_argument("--output-root", default="var/artifacts/benchmarks", help="压测产物根目录")
    parser.add_argument("--task-name", default="deepseek_workload_suite", help="任务名")
    args = parser.parse_args()

    if args.requests_per_case <= 0:
        raise ValueError("requests-per-case 必须大于 0")
    if args.timeout_sec <= 0:
        raise ValueError("timeout-sec 必须大于 0")
    asyncio.run(_amain(args))


if __name__ == "__main__":
    main()
