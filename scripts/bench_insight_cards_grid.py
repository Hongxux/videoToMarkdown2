from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import re
import statistics
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.bench_common.report_builder import ensure_benchmark_dirs, now_iso, parse_int_list, percentile, write_csv, write_json


DEFAULT_BATCH_USER_PROMPT = "\n".join(
    [
        "术语列表（同一语境）：",
        "{terms_block}",
        "模式：{scenario}",
        "语境段落：{context_block}",
        "锚点句（共享）：{example_block}",
        "",
        "请输出一个 JSON 对象，格式固定为：",
        "{",
        '  "items": [',
        "    {",
        '      "term": "术语原文",',
        '      "contextual_explanations": ["..."],',
        '      "depth": ["..."],',
        '      "breadth": ["..."]',
        "    }",
        "  ]",
        "}",
        "",
        "要求：",
        "- items 覆盖全部术语，顺序与输入保持一致",
        "- term 必须与输入术语原文一致",
        "- 每个数组 1~3 条短句",
        "- 仅输出 JSON，不得输出其他文本",
    ]
)


@dataclass(frozen=True)
class TagContext:
    tag: str
    canonical_key: str
    node_ids: List[str]
    reasons: List[str]
    context_blocks: List[str]


@dataclass(frozen=True)
class RequestUnit:
    request_id: str
    mode: str  # single | batch
    terms: List[str]
    shared_context: str
    shared_example: str


def _load_properties(path: Path) -> Dict[str, str]:
    props: Dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        props[key.strip()] = value.strip()
    return props


def _normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").replace("\r\n", "\n").replace("\r", "\n")).strip()


def _canonical_tag(tag: str) -> str:
    return _normalize_space(tag).lower()


def _slug_for_id(text: str) -> str:
    value = _normalize_space(text).lower()
    value = re.sub(r"[^\w\u4e00-\u9fff\-]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "term"


def _extract_chunk_number(chunk_id: str) -> Optional[int]:
    m = re.search(r"c-(\d+)", str(chunk_id or ""))
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def _split_markdown_sections(markdown_text: str) -> List[str]:
    text = str(markdown_text or "").replace("\r\n", "\n").replace("\r", "\n")
    heading_iter = list(re.finditer(r"(?m)^###\s+.*$", text))
    if not heading_iter:
        return []
    sections: List[str] = []
    for idx, hit in enumerate(heading_iter):
        start = hit.start()
        end = heading_iter[idx + 1].start() if idx + 1 < len(heading_iter) else len(text)
        section = text[start:end].strip()
        if section:
            sections.append(section)
    return sections


def _build_neighbor_context(sections: List[str], chunk_number: Optional[int], max_chars: int) -> str:
    if chunk_number is None or chunk_number <= 0:
        return ""
    index = chunk_number - 1
    if index >= len(sections):
        return ""
    prev_text = sections[index - 1] if index - 1 >= 0 else "（无）"
    cur_text = sections[index]
    next_text = sections[index + 1] if index + 1 < len(sections) else "（无）"
    merged = "\n".join(
        [
            "【前文语境】",
            prev_text,
            "",
            "【当前聚焦段落（在此处该词横空出世）】",
            cur_text,
            "",
            "【后文发展】",
            next_text,
        ]
    ).strip()
    if max_chars > 0 and len(merged) > max_chars:
        return merged[:max_chars].strip()
    return merged


def _parse_json_object(text: str) -> Optional[Dict[str, Any]]:
    raw = str(text or "").strip()
    if not raw:
        return None
    if "```" in raw:
        raw = raw.replace("```json", "").replace("```", "").strip()
    start = raw.find("{")
    if start < 0:
        return None
    depth = 0
    end = -1
    for i in range(start, len(raw)):
        ch = raw[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = i
                break
    if end < 0:
        return None
    try:
        obj = json.loads(raw[start : end + 1])
    except Exception:
        return None
    if not isinstance(obj, dict):
        return None
    return obj


def _to_list_of_str(raw: Any) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        out: List[str] = []
        for item in raw:
            text = _normalize_space(str(item))
            if text:
                out.append(text)
            if len(out) >= 6:
                break
        return out
    text = _normalize_space(str(raw))
    if not text:
        return []
    return [text]


def _count_single_parsed_items(raw_text: str) -> int:
    obj = _parse_json_object(raw_text)
    if not obj:
        return 0
    contextual = _to_list_of_str(obj.get("contextual_explanations") or obj.get("contextualExplanations"))
    depth = _to_list_of_str(obj.get("depth"))
    breadth = _to_list_of_str(obj.get("breadth") or obj.get("width"))
    return len(contextual) + len(depth) + len(breadth)


def _count_batch_parsed_items(raw_text: str) -> int:
    obj = _parse_json_object(raw_text)
    if not obj:
        return 0
    items = obj.get("items")
    if not isinstance(items, list):
        return 0
    parsed = 0
    for item in items:
        if not isinstance(item, dict):
            continue
        contextual = _to_list_of_str(item.get("contextual_explanations") or item.get("contextualExplanations"))
        depth = _to_list_of_str(item.get("depth"))
        breadth = _to_list_of_str(item.get("breadth") or item.get("width"))
        parsed += len(contextual) + len(depth) + len(breadth)
    return parsed


def _build_structured_user_prompt(template: str, term: str, context: str, example: str) -> str:
    prompt = str(template or "")
    prompt = prompt.replace("{term}", term)
    prompt = prompt.replace("{scenario}", "段落绑定")
    prompt = prompt.replace("{context_block}", context if context else "（无）")
    prompt = prompt.replace("{example_block}", example if example else "（无）")
    return prompt


def _build_structured_batch_user_prompt(terms: List[str], context: str, example: str) -> str:
    terms_block = "\n".join(f"{idx + 1}. {term}" for idx, term in enumerate(terms)) if terms else "（无）"
    prompt = DEFAULT_BATCH_USER_PROMPT
    prompt = prompt.replace("{terms_block}", terms_block)
    prompt = prompt.replace("{scenario}", "段落绑定")
    prompt = prompt.replace("{context_block}", context if context else "（无）")
    prompt = prompt.replace("{example_block}", example if example else "（无）")
    return prompt


def _build_tag_contexts(
    annotations: List[Dict[str, Any]],
    sections: List[str],
    max_tags: int,
    context_max_chars: int,
) -> List[TagContext]:
    merged: Dict[str, Dict[str, Any]] = {}
    for ann in annotations:
        if not isinstance(ann, dict):
            continue
        chunk_id = _normalize_space(str(ann.get("chunk_id", "")))
        reason = _normalize_space(str(ann.get("reason", "")))
        tags_raw = ann.get("insights_tags")
        if not isinstance(tags_raw, list):
            continue
        tags: List[str] = []
        for tag in tags_raw:
            value = _normalize_space(str(tag))
            if value and all(value.lower() != x.lower() for x in tags):
                tags.append(value)
        if not tags:
            continue
        chunk_no = _extract_chunk_number(chunk_id)
        context_block = _build_neighbor_context(sections, chunk_no, context_max_chars)
        for tag in tags:
            canonical = _canonical_tag(tag)
            if not canonical:
                continue
            entry = merged.setdefault(
                canonical,
                {
                    "tag": tag,
                    "node_ids": [],
                    "reasons": [],
                    "context_blocks": [],
                },
            )
            if chunk_id and chunk_id not in entry["node_ids"]:
                entry["node_ids"].append(chunk_id)
            if reason and reason not in entry["reasons"]:
                entry["reasons"].append(reason)
            if context_block and context_block not in entry["context_blocks"]:
                entry["context_blocks"].append(context_block)
    contexts: List[TagContext] = []
    for canonical, item in merged.items():
        contexts.append(
            TagContext(
                tag=str(item["tag"]),
                canonical_key=canonical,
                node_ids=list(item["node_ids"]),
                reasons=list(item["reasons"]),
                context_blocks=list(item["context_blocks"]),
            )
        )
    contexts.sort(key=lambda x: x.tag.lower())
    return contexts[: max(1, max_tags)]


def _node_signature(context: TagContext) -> str:
    if not context.node_ids:
        return f"single:{context.canonical_key}"
    values = sorted({_normalize_space(x).lower() for x in context.node_ids if _normalize_space(x)})
    if not values:
        return f"single:{context.canonical_key}"
    return "nodes:" + "|".join(values)


def _build_shared_context(group: List[TagContext], max_blocks: int = 6) -> str:
    blocks: List[str] = []
    for ctx in group:
        for block in ctx.context_blocks:
            b = _normalize_space(block.replace("\n", " "))
            if not b:
                continue
            if b not in blocks:
                blocks.append(block)
            if len(blocks) >= max_blocks:
                break
        if len(blocks) >= max_blocks:
            break
    return "\n\n---\n\n".join(blocks).strip()


def _build_shared_anchor(group: List[TagContext], max_items: int = 4) -> str:
    anchors: List[str] = []
    for ctx in group:
        for reason in ctx.reasons:
            r = _normalize_space(reason)
            if not r:
                continue
            if r not in anchors:
                anchors.append(r)
            if len(anchors) >= max_items:
                break
        if len(anchors) >= max_items:
            break
    return "；".join(anchors).strip()


def _build_request_units(contexts: List[TagContext], batch_max_terms: int) -> List[RequestUnit]:
    grouped: Dict[str, List[TagContext]] = {}
    for ctx in contexts:
        grouped.setdefault(_node_signature(ctx), []).append(ctx)
    units: List[RequestUnit] = []
    batch_limit = max(2, int(batch_max_terms))
    for signature, group in grouped.items():
        group = sorted(group, key=lambda x: x.tag.lower())
        for i in range(0, len(group), batch_limit):
            sub = group[i : i + batch_limit]
            terms = [x.tag for x in sub]
            shared_context = _build_shared_context(sub)
            shared_example = _build_shared_anchor(sub)
            if len(terms) >= 2:
                rid = f"{_slug_for_id(signature)}__b{i // batch_limit}"
                units.append(
                    RequestUnit(
                        request_id=rid,
                        mode="batch",
                        terms=terms,
                        shared_context=shared_context,
                        shared_example=shared_example,
                    )
                )
            else:
                term = terms[0]
                rid = _slug_for_id(term)
                units.append(
                    RequestUnit(
                        request_id=rid,
                        mode="single",
                        terms=[term],
                        shared_context=shared_context,
                        shared_example=shared_example,
                    )
                )
    units.sort(key=lambda x: x.request_id)
    return units


def _expand_units(units: List[RequestUnit], workload_multiplier: int) -> List[RequestUnit]:
    expanded: List[RequestUnit] = []
    for rep in range(max(1, workload_multiplier)):
        for idx, unit in enumerate(units):
            rid = f"{unit.request_id}__rep{rep}__idx{idx}"
            expanded.append(
                RequestUnit(
                    request_id=rid,
                    mode=unit.mode,
                    terms=list(unit.terms),
                    shared_context=unit.shared_context,
                    shared_example=unit.shared_example,
                )
            )
    return expanded


async def _run_case(
    units: List[RequestUnit],
    *,
    concurrency: int,
    endpoint: str,
    api_key: str,
    model: str,
    timeout_sec: int,
    structured_system_prompt: str,
    structured_user_prompt: str,
    structured_max_tokens: int,
) -> Dict[str, Any]:
    limits = httpx.Limits(max_connections=max(64, concurrency * 3), max_keepalive_connections=max(32, concurrency * 2))
    timeout = httpx.Timeout(timeout=max(20.0, float(timeout_sec)))
    sem = asyncio.Semaphore(max(1, concurrency))
    rows: List[Dict[str, Any]] = []
    started_at = time.perf_counter()

    async with httpx.AsyncClient(limits=limits, timeout=timeout, http2=True) as client:

        async def _call_one(unit: RequestUnit) -> Dict[str, Any]:
            row: Dict[str, Any] = {
                "request_id": unit.request_id,
                "tag": unit.terms[0] if unit.terms else "",
                "mode": unit.mode,
                "term_count": len(unit.terms),
                "ok": False,
                "attempt_count": 0,
                "error": "",
                "finish_reason": "",
                "usage_prompt_tokens": 0,
                "usage_completion_tokens": 0,
                "usage_total_tokens": 0,
                "http_status": 0,
                "parsed_items": 0,
                "latency_ms": 0.0,
                "concurrency": concurrency,
            }
            max_tokens = max(256, int(structured_max_tokens))
            attempts = 0
            while attempts < 3:
                attempts += 1
                row["attempt_count"] = attempts
                if unit.mode == "batch":
                    user_prompt = _build_structured_batch_user_prompt(unit.terms, unit.shared_context, unit.shared_example)
                else:
                    user_prompt = _build_structured_user_prompt(
                        structured_user_prompt,
                        unit.terms[0] if unit.terms else "",
                        unit.shared_context,
                        unit.shared_example,
                    )
                payload: Dict[str, Any] = {
                    "model": model,
                    "temperature": 0.2,
                    "max_tokens": max_tokens,
                    "stream": False,
                    "messages": [
                        {"role": "system", "content": structured_system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                }
                if unit.mode == "batch":
                    payload["response_format"] = {"type": "json_object"}
                req_started = time.perf_counter()
                try:
                    async with sem:
                        resp = await client.post(
                            f"{endpoint}/chat/completions",
                            headers={
                                "Authorization": f"Bearer {api_key}",
                                "Content-Type": "application/json",
                                "Accept": "application/json",
                            },
                            json=payload,
                        )
                except Exception as ex:
                    row["error"] = f"http_exception:{ex}"
                    row["latency_ms"] = round((time.perf_counter() - req_started) * 1000.0, 3)
                    continue

                row["latency_ms"] = round((time.perf_counter() - req_started) * 1000.0, 3)
                row["http_status"] = int(resp.status_code)
                if resp.status_code < 200 or resp.status_code >= 300:
                    row["error"] = f"http_{resp.status_code}"
                    continue
                try:
                    body = resp.json()
                except Exception as ex:
                    row["error"] = f"invalid_json_response:{ex}"
                    continue
                usage = body.get("usage") if isinstance(body, dict) else {}
                if isinstance(usage, dict):
                    row["usage_prompt_tokens"] = int(usage.get("prompt_tokens") or 0)
                    row["usage_completion_tokens"] = int(usage.get("completion_tokens") or 0)
                    row["usage_total_tokens"] = int(usage.get("total_tokens") or 0)
                choices = body.get("choices") if isinstance(body, dict) else None
                if not isinstance(choices, list) or not choices:
                    row["error"] = "no_choices"
                    continue
                first = choices[0] if isinstance(choices[0], dict) else {}
                row["finish_reason"] = str(first.get("finish_reason") or "")
                msg = first.get("message") if isinstance(first.get("message"), dict) else {}
                content = str(msg.get("content") or "").strip()
                if not content:
                    row["error"] = "empty_content"
                    continue
                if unit.mode == "batch":
                    parsed_items = _count_batch_parsed_items(content)
                else:
                    parsed_items = _count_single_parsed_items(content)
                row["parsed_items"] = int(parsed_items)
                if row["finish_reason"].lower() == "length":
                    max_tokens = min(16000, max(max_tokens + 1024, max_tokens * 2))
                    row["error"] = "finish_reason_length"
                    continue
                if parsed_items <= 0:
                    row["error"] = "parse_empty"
                    continue
                row["ok"] = True
                row["error"] = ""
                return row
            return row

        tasks = [asyncio.create_task(_call_one(unit)) for unit in units]
        rows = await asyncio.gather(*tasks, return_exceptions=False)

    elapsed_sec = max(0.0001, time.perf_counter() - started_at)
    ok_rows = [r for r in rows if bool(r.get("ok"))]
    latencies = [float(r.get("latency_ms") or 0.0) for r in ok_rows]
    error_type_counts: Dict[str, int] = {}
    for row in rows:
        if row.get("ok"):
            continue
        key = str(row.get("error") or "unknown_error")
        error_type_counts[key] = error_type_counts.get(key, 0) + 1
    summary = {
        "concurrency": int(concurrency),
        "total_requests": int(len(rows)),
        "ok_requests": int(len(ok_rows)),
        "error_requests": int(len(rows) - len(ok_rows)),
        "coverage_ratio": float(len(ok_rows) / len(rows)) if rows else 0.0,
        "elapsed_sec": round(elapsed_sec, 6),
        "request_throughput_per_sec": round(len(rows) / elapsed_sec, 6),
        "latency_avg_ms": round(float(statistics.fmean(latencies)) if latencies else 0.0, 6),
        "latency_p50_ms": round(percentile(latencies, 50), 6),
        "latency_p95_ms": round(percentile(latencies, 95), 6),
        "usage_prompt_tokens": int(sum(int(r.get("usage_prompt_tokens") or 0) for r in rows)),
        "usage_completion_tokens": int(sum(int(r.get("usage_completion_tokens") or 0) for r in rows)),
        "usage_total_tokens": int(sum(int(r.get("usage_total_tokens") or 0) for r in rows)),
        "error_type_counts": error_type_counts,
        "request_rows": rows,
    }
    return summary


def _pick_best(rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not rows:
        return None
    ordered = sorted(
        rows,
        key=lambda r: (
            -float(r.get("coverage_ratio") or 0.0),
            -float(r.get("request_throughput_per_sec") or 0.0),
            float(r.get("latency_p95_ms") or 0.0),
            int(r.get("error_requests") or 0),
            int(r.get("concurrency") or 0),
        ),
    )
    return ordered[0]


def _render_report(metadata: Dict[str, Any], summary_rows: List[Dict[str, Any]], best: Optional[Dict[str, Any]]) -> str:
    lines: List[str] = []
    lines.append("# Insight Cards Concurrency Benchmark")
    lines.append("")
    lines.append(f"- generated_at: `{metadata['generated_at']}`")
    lines.append(f"- md_path: `{metadata['md_path']}`")
    lines.append(f"- persona_case_path: `{metadata['persona_case_path']}`")
    lines.append(f"- model: `{metadata['model']}`")
    lines.append(f"- base_units(按同nodes分组后): `{metadata['base_unit_count']}`")
    lines.append(f"- batch_max_terms: `{metadata['batch_max_terms']}`")
    lines.append(f"- workload_multiplier: `{metadata['workload_multiplier']}`")
    lines.append(f"- total_requests: `{metadata['total_requests']}`")
    lines.append(f"- concurrency_sizes: `{metadata['concurrency_sizes']}`")
    lines.append("")
    lines.append("## Best Case")
    if best:
        lines.append(
            f"- concurrency={best['concurrency']}, coverage={best['coverage_ratio']:.4f}, "
            f"elapsed={best['elapsed_sec']:.2f}s, tps={best['request_throughput_per_sec']:.3f}, "
            f"p95={best['latency_p95_ms']:.1f}ms, errors={best['error_requests']}"
        )
    else:
        lines.append("- 无可用结果")
    lines.append("")
    lines.append("## Grid Summary")
    lines.append("")
    lines.append("| concurrency | coverage | elapsed_sec | req_tps | avg_ms | p95_ms | error_requests | total_tokens |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- | --- |")
    for row in summary_rows:
        lines.append(
            f"| {row['concurrency']} | {row['coverage_ratio']:.4f} | {row['elapsed_sec']:.2f} | "
            f"{row['request_throughput_per_sec']:.3f} | {row['latency_avg_ms']:.1f} | {row['latency_p95_ms']:.1f} | "
            f"{row['error_requests']} | {row['usage_total_tokens']} |"
        )
    lines.append("")
    lines.append("## Artifacts")
    lines.append("")
    lines.append("- `raw/summary_rows.json`")
    lines.append("- `raw/summary_rows.csv`")
    lines.append("- `raw/request_rows.json`")
    lines.append("- `raw/request_rows.csv`")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Insight cards concurrency benchmark (node-group batch aware)")
    parser.add_argument(
        "--md-path",
        default="var/storage/storage/c786a1956e66ba020dfb2ed46a3b0c3c_ab_b11_20260222_012830/enhanced_output.md",
    )
    parser.add_argument(
        "--persona-case-path",
        default="var/benchmarks/persona_reading_grid_chat_c48_80_repeat2_20260222_023938/raw/case_b1_c64.json",
    )
    parser.add_argument("--concurrency-sizes", default="48,56,64,72,80,88")
    parser.add_argument("--max-tags", type=int, default=48)
    parser.add_argument("--batch-max-terms", type=int, default=8)
    parser.add_argument("--workload-multiplier", type=int, default=0, help="0 表示自动计算")
    parser.add_argument("--timeout-sec", type=int, default=90)
    parser.add_argument("--context-max-chars", type=int, default=2400)
    parser.add_argument("--output-root", default="var/benchmarks")
    parser.add_argument("--task-name", default="insight_cards_grid_batch_shared_chat_c48_start")
    args = parser.parse_args()

    md_path = Path(args.md_path).resolve()
    persona_case_path = Path(args.persona_case_path).resolve()
    if not md_path.is_file():
        raise FileNotFoundError(f"markdown not found: {md_path}")
    if not persona_case_path.is_file():
        raise FileNotFoundError(f"persona case not found: {persona_case_path}")

    properties_path = Path("services/java-orchestrator/src/main/resources/application.properties").resolve()
    props = _load_properties(properties_path)
    model = props.get("deepseek.advisor.model", "deepseek-chat")
    base_url = props.get("deepseek.advisor.base-url", "https://api.deepseek.com/v1")
    structured_max_tokens = int(props.get("deepseek.advisor.structured-max-tokens", "8000") or "8000")
    if base_url.endswith("/"):
        base_url = base_url[:-1]
    if not re.search(r"/v\d+$", base_url):
        base_url = base_url + "/v1"

    api_key = os.getenv("DEEPSEEK_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("DEEPSEEK_API_KEY is empty")

    system_prompt = Path(
        "services/java-orchestrator/src/main/resources/prompts/deepseek-advisor/structured-system-zh.txt"
    ).read_text(encoding="utf-8").strip()
    user_prompt = Path(
        "services/java-orchestrator/src/main/resources/prompts/deepseek-advisor/structured-user-zh.txt"
    ).read_text(encoding="utf-8").strip()

    persona_case = json.loads(persona_case_path.read_text(encoding="utf-8"))
    annotations = persona_case.get("chunk_annotations")
    if not isinstance(annotations, list):
        raise RuntimeError("chunk_annotations missing in persona_case_path")
    sections = _split_markdown_sections(md_path.read_text(encoding="utf-8"))
    contexts = _build_tag_contexts(
        annotations=annotations,
        sections=sections,
        max_tags=max(1, int(args.max_tags)),
        context_max_chars=max(600, int(args.context_max_chars)),
    )
    if not contexts:
        raise RuntimeError("no tag contexts extracted from persona case")
    base_units = _build_request_units(contexts, batch_max_terms=max(2, int(args.batch_max_terms)))
    if not base_units:
        raise RuntimeError("no request units built")

    concurrency_sizes = parse_int_list(args.concurrency_sizes, "concurrency_sizes")
    if min(concurrency_sizes) < 48:
        raise ValueError("并发梯度必须从 48 或更高开始")
    max_concurrency = max(concurrency_sizes)
    if int(args.workload_multiplier) > 0:
        workload_multiplier = int(args.workload_multiplier)
    else:
        workload_multiplier = max(2, int(math.ceil((max_concurrency + 8) / max(1, len(base_units)))))
    expanded_units = _expand_units(base_units, workload_multiplier)
    if len(expanded_units) <= max_concurrency:
        needed = max_concurrency + 1
        workload_multiplier = int(math.ceil(needed / max(1, len(base_units))))
        expanded_units = _expand_units(base_units, workload_multiplier)
    if len(expanded_units) <= max_concurrency:
        raise RuntimeError("workload still not greater than max concurrency")

    output_dir, raw_dir, _charts_dir = ensure_benchmark_dirs(args.output_root, args.task_name)
    metadata: Dict[str, Any] = {
        "generated_at": now_iso(),
        "md_path": str(md_path),
        "persona_case_path": str(persona_case_path),
        "model": model,
        "base_url": base_url,
        "max_tags": int(args.max_tags),
        "base_context_count": len(contexts),
        "base_unit_count": len(base_units),
        "batch_max_terms": int(args.batch_max_terms),
        "workload_multiplier": int(workload_multiplier),
        "total_requests": len(expanded_units),
        "concurrency_sizes": concurrency_sizes,
        "timeout_sec": int(args.timeout_sec),
        "structured_max_tokens": int(structured_max_tokens),
        "prompts": {
            "structured_system": str(
                Path("services/java-orchestrator/src/main/resources/prompts/deepseek-advisor/structured-system-zh.txt").resolve()
            ),
            "structured_user": str(
                Path("services/java-orchestrator/src/main/resources/prompts/deepseek-advisor/structured-user-zh.txt").resolve()
            ),
            "structured_batch": "built-in (aligned with DeepSeekAdvisorService DEFAULT_STRUCTURED_BATCH_USER_PROMPT)",
        },
        "load_guard": {
            "max_concurrency": max_concurrency,
            "total_requests_gt_max_concurrency": len(expanded_units) > max_concurrency,
        },
    }
    write_json(raw_dir / "metadata.json", metadata)

    print(
        f"[Plan] base_units={len(base_units)}, multiplier={workload_multiplier}, total_requests={len(expanded_units)}, "
        f"max_concurrency={max_concurrency}"
    )

    summary_rows: List[Dict[str, Any]] = []
    all_request_rows: List[Dict[str, Any]] = []
    for concurrency in concurrency_sizes:
        print(f"[Run] concurrency={concurrency}, requests={len(expanded_units)}")
        case = asyncio.run(
            _run_case(
                expanded_units,
                concurrency=int(concurrency),
                endpoint=base_url,
                api_key=api_key,
                model=model,
                timeout_sec=int(args.timeout_sec),
                structured_system_prompt=system_prompt,
                structured_user_prompt=user_prompt,
                structured_max_tokens=int(structured_max_tokens),
            )
        )
        case_path = raw_dir / f"case_c{int(concurrency)}.json"
        write_json(case_path, case)
        flattened = {k: v for k, v in case.items() if k != "request_rows"}
        summary_rows.append(flattened)
        for row in case.get("request_rows", []):
            all_request_rows.append(row)

    summary_rows.sort(key=lambda x: int(x.get("concurrency", 0)))
    best = _pick_best(summary_rows)
    if best:
        write_json(raw_dir / "best_case.json", best)
    write_json(raw_dir / "summary_rows.json", summary_rows)
    write_csv(raw_dir / "summary_rows.csv", summary_rows)
    write_json(raw_dir / "request_rows.json", all_request_rows)
    write_csv(raw_dir / "request_rows.csv", all_request_rows)
    report = _render_report(metadata, summary_rows, best)
    (output_dir / "report.md").write_text(report, encoding="utf-8")
    if best:
        print(
            f"[Best] concurrency={best['concurrency']} coverage={best['coverage_ratio']:.4f} "
            f"tps={best['request_throughput_per_sec']:.3f} p95={best['latency_p95_ms']:.1f}ms"
        )
    print(f"[Done] output_dir={output_dir}")


if __name__ == "__main__":
    main()
