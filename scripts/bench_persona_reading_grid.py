from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import re
import statistics
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

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


LIST_LINE_PATTERN = re.compile(r"^\s*(?:[-*+]\s+|\d+[.)]\s+).+")
HEADING_PATTERN = re.compile(r"^\s{0,3}#{1,6}\s+.+")
QUOTE_PATTERN = re.compile(r"^\s{0,3}>\s*.+")
CODE_FENCE_PATTERN = re.compile(r"^\s*```.*")
IMAGE_ONLY_PATTERN = re.compile(r"^\s*!\[[^\]]*\]\([^)]+\)\s*$")

MAX_CHUNK_TEXT_CHARS = 1400
AUX_QUOTE_MAX_CHARS = 160
DEFAULT_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
RETRYABLE_EXCEPTION_NAMES = {
    "EndOfStream",
    "PoolTimeout",
    "ReadTimeout",
    "ConnectTimeout",
    "WriteTimeout",
    "RemoteProtocolError",
    "ReadError",
    "WriteError",
    "ConnectError",
}


@dataclass
class ParagraphNode:
    node_id: str
    order: int
    node_type: str
    raw_markdown: str


@dataclass
class NodeChunk:
    chunk_id: str
    node_ids: List[str]
    primary_node_id: str
    chunk_text: str


def _expand_chunks_for_load(chunks: List[NodeChunk], multiplier: int) -> List[NodeChunk]:
    factor = max(1, int(multiplier))
    if factor <= 1:
        return list(chunks)
    expanded: List[NodeChunk] = []
    for rep in range(factor):
        suffix = f"__rep{rep + 1}"
        for chunk in chunks:
            expanded.append(
                NodeChunk(
                    chunk_id=f"{chunk.chunk_id}{suffix}",
                    node_ids=[f"{node_id}{suffix}" for node_id in chunk.node_ids],
                    primary_node_id=f"{chunk.primary_node_id}{suffix}",
                    chunk_text=chunk.chunk_text,
                )
            )
    return expanded


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _extract_json_array(text: str) -> Optional[str]:
    raw = (text or "").strip()
    if not raw:
        return None
    start = raw.find("[")
    if start < 0:
        return None
    depth = 0
    for idx in range(start, len(raw)):
        ch = raw[idx]
        if ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0:
                return raw[start : idx + 1]
    return None


def _join_lines(lines: List[str], start: int, end_exclusive: int) -> str:
    return "\n".join(lines[start:end_exclusive])


def _indent_width(line: str) -> int:
    width = 0
    for ch in line:
        if ch == " ":
            width += 1
        elif ch == "\t":
            width += 4
        else:
            break
    return width


def _parse_markdown_nodes(markdown: str) -> List[ParagraphNode]:
    lines = str(markdown or "").replace("\r\n", "\n").split("\n")
    nodes: List[ParagraphNode] = []
    i = 0
    order = 0
    while i < len(lines):
        line = lines[i]
        if not line.strip():
            i += 1
            continue
        start = i
        end_exclusive = i + 1
        node_type = "paragraph"

        if CODE_FENCE_PATTERN.match(line):
            node_type = "code_block"
            while end_exclusive < len(lines):
                if CODE_FENCE_PATTERN.match(lines[end_exclusive]):
                    end_exclusive += 1
                    break
                end_exclusive += 1
        elif LIST_LINE_PATTERN.match(line):
            node_type = "list_block"
            base_indent = _indent_width(line)
            while end_exclusive < len(lines):
                cursor = lines[end_exclusive]
                if not cursor.strip():
                    if end_exclusive + 1 < len(lines):
                        nxt = lines[end_exclusive + 1]
                        if LIST_LINE_PATTERN.match(nxt) or _indent_width(nxt) > base_indent:
                            end_exclusive += 1
                            continue
                    break
                if LIST_LINE_PATTERN.match(cursor) or _indent_width(cursor) > base_indent:
                    end_exclusive += 1
                    continue
                break
        elif HEADING_PATTERN.match(line):
            node_type = "heading"
        elif QUOTE_PATTERN.match(line):
            node_type = "quote"
            while end_exclusive < len(lines):
                cursor = lines[end_exclusive]
                if not cursor.strip():
                    break
                if QUOTE_PATTERN.match(cursor):
                    end_exclusive += 1
                    continue
                break
        else:
            node_type = "paragraph"
            while end_exclusive < len(lines):
                cursor = lines[end_exclusive]
                if not cursor.strip():
                    break
                if (
                    CODE_FENCE_PATTERN.match(cursor)
                    or HEADING_PATTERN.match(cursor)
                    or LIST_LINE_PATTERN.match(cursor)
                    or QUOTE_PATTERN.match(cursor)
                ):
                    break
                end_exclusive += 1

        raw = _join_lines(lines, start, end_exclusive).strip()
        if raw:
            nodes.append(
                ParagraphNode(
                    node_id=f"p-{order + 1}",
                    order=order,
                    node_type=node_type,
                    raw_markdown=raw,
                )
            )
            order += 1
        i = max(end_exclusive, i + 1)
    return nodes


def _is_short_guide_quote(raw_markdown: str) -> bool:
    text = str(raw_markdown or "").strip()
    if not text:
        return True
    normalized = re.sub(r"(?m)^\s*>\s?", "", text).strip()
    return len(normalized) <= AUX_QUOTE_MAX_CHARS


def _is_image_only_node(raw_markdown: str) -> bool:
    text = str(raw_markdown or "").strip()
    if not text:
        return True
    if IMAGE_ONLY_PATTERN.match(text):
        return True
    lower = text.lower()
    return lower.startswith("<img") and lower.endswith(">")


def _is_entity_node(node: ParagraphNode) -> bool:
    return node.node_type in {"paragraph", "list_block", "code_block"}


def _is_auxiliary_node(node: ParagraphNode) -> bool:
    if node.node_type == "heading":
        return True
    if node.node_type == "quote" and _is_short_guide_quote(node.raw_markdown):
        return True
    return _is_image_only_node(node.raw_markdown)


def _create_chunk(members: List[ParagraphNode], primary: ParagraphNode, chunk_order: int) -> NodeChunk:
    node_ids: List[str] = []
    parts: List[str] = []
    for node in members:
        node_ids.append(node.node_id)
        raw = str(node.raw_markdown or "").strip()
        if raw:
            parts.append(raw)
    chunk_id = f"c-{chunk_order + 1}"
    primary_id = primary.node_id if primary else (node_ids[-1] if node_ids else "")
    return NodeChunk(
        chunk_id=chunk_id,
        node_ids=node_ids,
        primary_node_id=primary_id,
        chunk_text="\n\n".join(parts).strip(),
    )


def _build_semantic_chunks(nodes: List[ParagraphNode]) -> List[NodeChunk]:
    chunks: List[NodeChunk] = []
    buffer_nodes: List[ParagraphNode] = []
    chunk_order = 0

    for node in nodes:
        if _is_entity_node(node):
            members = list(buffer_nodes)
            members.append(node)
            buffer_nodes = []
            chunks.append(_create_chunk(members, node, chunk_order))
            chunk_order += 1
            continue
        if _is_auxiliary_node(node):
            buffer_nodes.append(node)
            continue
        members = list(buffer_nodes)
        members.append(node)
        buffer_nodes = []
        chunks.append(_create_chunk(members, node, chunk_order))
        chunk_order += 1

    if buffer_nodes:
        if not chunks:
            primary = buffer_nodes[-1]
            chunks.append(_create_chunk(buffer_nodes, primary, chunk_order))
        else:
            tail = chunks[-1]
            merged_ids = list(tail.node_ids)
            merged_parts = [str(tail.chunk_text or "").strip()] if str(tail.chunk_text or "").strip() else []
            for node in buffer_nodes:
                merged_ids.append(node.node_id)
                raw = str(node.raw_markdown or "").strip()
                if raw:
                    merged_parts.append(raw)
            chunks[-1] = NodeChunk(
                chunk_id=tail.chunk_id,
                node_ids=merged_ids,
                primary_node_id=tail.primary_node_id,
                chunk_text="\n\n".join(merged_parts).strip(),
            )
    return chunks


def _trim_text(text: str, max_length: int) -> str:
    value = str(text or "").strip()
    if len(value) <= max_length:
        return value
    return value[:max_length].strip()


def _chunk_list(items: List[Any], size: int) -> List[List[Any]]:
    if size <= 0:
        return [items]
    return [items[i : i + size] for i in range(0, len(items), size)]


def _load_persona(path: Path) -> Dict[str, Any]:
    if not path.is_file():
        return {
            "surface_context": {"profession": [], "skillset": [], "current_challenges": []},
            "deep_soul_matrix": {},
            "evolution_verdict": "default persona",
        }
    try:
        text = path.read_text(encoding="utf-8")
        data = json.loads(text)
        if isinstance(data, dict) and data:
            return data
    except Exception:
        pass
    return {
        "surface_context": {"profession": [], "skillset": [], "current_challenges": []},
        "deep_soul_matrix": {},
        "evolution_verdict": "default persona",
    }


def _render_user_prompt(template: str, persona_json: str, nodes_json: str) -> str:
    out = str(template or "")
    out = out.replace("{persona_json}", persona_json)
    out = out.replace("{nodes_json}", nodes_json)
    return out


def _normalize_chunk_id(row: Dict[str, Any], chunk_index: int, chunk_batches: List[NodeChunk]) -> str:
    if not isinstance(row, dict):
        return ""
    for key in ("chunk_id", "chunkId", "id"):
        val = row.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    for key in ("index", "idx", "order"):
        val = row.get(key)
        try:
            idx = int(val)
        except Exception:
            continue
        if 0 <= idx < len(chunk_batches):
            return chunk_batches[idx].chunk_id
    if 0 <= chunk_index < len(chunk_batches):
        return chunk_batches[chunk_index].chunk_id
    return ""


def _to_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _normalize_tags(value: Any) -> List[str]:
    if isinstance(value, list):
        out: List[str] = []
        for item in value:
            text = str(item or "").strip()
            if text:
                out.append(text)
        return out[:6]
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        if raw.startswith("[") and raw.endswith("]"):
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, list):
                    return _normalize_tags(parsed)
            except Exception:
                pass
        return [part.strip() for part in raw.split(",") if part.strip()][:6]
    return []


def _is_retryable_exception(exc: Exception) -> bool:
    if isinstance(exc, (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError)):
        return True
    return type(exc).__name__ in RETRYABLE_EXCEPTION_NAMES


def _is_retryable_status(status_code: int, retryable_status_codes: Set[int]) -> bool:
    return int(status_code) in retryable_status_codes


def _compute_retry_delay_sec(
    *,
    retry_index: int,
    base_delay_sec: float,
    max_delay_sec: float,
    jitter_sec: float,
) -> float:
    base = max(0.0, float(base_delay_sec))
    max_delay = max(base, float(max_delay_sec))
    jitter = max(0.0, float(jitter_sec))
    exp = max(0, int(retry_index) - 1)
    delay = min(max_delay, base * (2**exp))
    if jitter > 0.0:
        delay += random.uniform(0.0, jitter)
    return max(0.0, delay)


async def _call_one_batch(
    client: httpx.AsyncClient,
    *,
    endpoint: str,
    api_key: str,
    model: str,
    system_prompt: str,
    user_prompt_template: str,
    persona_json: str,
    batch_chunks: List[NodeChunk],
    batch_index: int,
    max_retries: int,
    retry_base_delay_sec: float,
    retry_max_delay_sec: float,
    retry_jitter_sec: float,
    retryable_status_codes: Set[int],
) -> Dict[str, Any]:
    chunk_rows: List[Dict[str, Any]] = []
    for chunk in batch_chunks:
        chunk_rows.append(
            {
                "chunk_id": chunk.chunk_id,
                "node_ids": chunk.node_ids,
                "node_count": len(chunk.node_ids),
                "primary_node_id": chunk.primary_node_id,
                "text_chunk": _trim_text(chunk.chunk_text, MAX_CHUNK_TEXT_CHARS),
            }
        )
    nodes_json = json.dumps(chunk_rows, ensure_ascii=False)
    user_prompt = _render_user_prompt(user_prompt_template, persona_json, nodes_json)
    payload = {
        "model": model,
        "temperature": 0.2,
        "max_tokens": 320,
        "stream": False,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    }

    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    row: Dict[str, Any] = {
        "batch_index": batch_index,
        "chunk_ids": [c.chunk_id for c in batch_chunks],
        "chunk_count": len(batch_chunks),
        "status_code": 0,
        "latency_ms": 0.0,
        "ok": False,
        "parsed_chunk_count": 0,
        "usage_prompt_tokens": 0,
        "usage_completion_tokens": 0,
        "usage_total_tokens": 0,
        "error": "",
        "attempt_count": 0,
        "retried": False,
        "retry_sleep_ms_total": 0.0,
        "retry_reasons": [],
        "annotation_rows": [],
    }
    request_started = time.perf_counter()
    attempt_count = 1
    max_retry_count = max(0, int(max_retries))
    retry_reasons: List[str] = []
    while True:
        row["attempt_count"] = attempt_count
        try:
            resp = await client.post(f"{endpoint}/chat/completions", headers=headers, json=payload)
        except Exception as exc:
            error_code = f"request_exception:{type(exc).__name__}"
            row["status_code"] = 0
            if _is_retryable_exception(exc) and attempt_count <= max_retry_count:
                retry_reasons.append(error_code)
                row["retried"] = True
                delay_sec = _compute_retry_delay_sec(
                    retry_index=attempt_count,
                    base_delay_sec=retry_base_delay_sec,
                    max_delay_sec=retry_max_delay_sec,
                    jitter_sec=retry_jitter_sec,
                )
                row["retry_sleep_ms_total"] = round(float(row["retry_sleep_ms_total"]) + (delay_sec * 1000.0), 3)
                await asyncio.sleep(delay_sec)
                attempt_count += 1
                continue
            row["error"] = error_code
            row["retry_reasons"] = retry_reasons
            row["latency_ms"] = round((time.perf_counter() - request_started) * 1000.0, 3)
            return row

        row["status_code"] = int(resp.status_code)
        if resp.status_code < 200 or resp.status_code >= 300:
            error_code = f"http_{resp.status_code}"
            if _is_retryable_status(int(resp.status_code), retryable_status_codes) and attempt_count <= max_retry_count:
                retry_reasons.append(error_code)
                row["retried"] = True
                delay_sec = _compute_retry_delay_sec(
                    retry_index=attempt_count,
                    base_delay_sec=retry_base_delay_sec,
                    max_delay_sec=retry_max_delay_sec,
                    jitter_sec=retry_jitter_sec,
                )
                row["retry_sleep_ms_total"] = round(float(row["retry_sleep_ms_total"]) + (delay_sec * 1000.0), 3)
                await asyncio.sleep(delay_sec)
                attempt_count += 1
                continue
            row["error"] = error_code
            row["retry_reasons"] = retry_reasons
            row["latency_ms"] = round((time.perf_counter() - request_started) * 1000.0, 3)
            return row

        try:
            body = resp.json()
        except Exception:
            row["error"] = "invalid_json_response"
            row["retry_reasons"] = retry_reasons
            row["latency_ms"] = round((time.perf_counter() - request_started) * 1000.0, 3)
            return row

        usage = body.get("usage", {}) if isinstance(body, dict) else {}
        row["usage_prompt_tokens"] = int(usage.get("prompt_tokens", 0) or 0)
        row["usage_completion_tokens"] = int(usage.get("completion_tokens", 0) or 0)
        row["usage_total_tokens"] = int(usage.get("total_tokens", 0) or 0)

        content = ""
        try:
            content = str(body.get("choices", [{}])[0].get("message", {}).get("content", "") or "")
        except Exception:
            content = ""
        json_array = _extract_json_array(content)
        if not json_array:
            row["error"] = "parse_array_missing"
            row["retry_reasons"] = retry_reasons
            row["latency_ms"] = round((time.perf_counter() - request_started) * 1000.0, 3)
            return row

        try:
            parsed = json.loads(json_array)
        except Exception:
            row["error"] = "parse_array_invalid"
            row["retry_reasons"] = retry_reasons
            row["latency_ms"] = round((time.perf_counter() - request_started) * 1000.0, 3)
            return row

        if not isinstance(parsed, list):
            row["error"] = "parse_not_list"
            row["retry_reasons"] = retry_reasons
            row["latency_ms"] = round((time.perf_counter() - request_started) * 1000.0, 3)
            return row

        annotation_rows: List[Dict[str, Any]] = []
        for idx, item in enumerate(parsed):
            if not isinstance(item, dict):
                continue
            chunk_id = _normalize_chunk_id(item, idx, batch_chunks)
            if not chunk_id:
                continue
            score = max(0.0, min(1.0, _to_float(item.get("relevance_score", item.get("score", 0.5)), 0.5)))
            reason = str(item.get("reason", "") or "").strip()
            bridge_text = item.get("bridge_text", item.get("bridgeText"))
            bridge = str(bridge_text).strip() if isinstance(bridge_text, str) and bridge_text is not None else None
            tags = _normalize_tags(item.get("insights_tags", item.get("insight_tags", item.get("insights_terms"))))
            annotation_rows.append(
                {
                    "chunk_id": chunk_id,
                    "relevance_score": score,
                    "reason": reason,
                    "bridge_text": bridge,
                    "insights_tags": tags,
                }
            )

        if not annotation_rows:
            row["error"] = "parse_rows_empty"
            row["retry_reasons"] = retry_reasons
            row["latency_ms"] = round((time.perf_counter() - request_started) * 1000.0, 3)
            return row
        row["ok"] = True
        row["parsed_chunk_count"] = len(annotation_rows)
        row["annotation_rows"] = annotation_rows
        row["retry_reasons"] = retry_reasons
        row["latency_ms"] = round((time.perf_counter() - request_started) * 1000.0, 3)
        return row


async def _run_case(
    *,
    endpoint: str,
    api_key: str,
    model: str,
    timeout_sec: float,
    system_prompt: str,
    user_prompt_template: str,
    persona_json: str,
    chunks: List[NodeChunk],
    batch_size: int,
    concurrency: int,
    repeat: int,
    max_retries: int,
    retry_base_delay_sec: float,
    retry_max_delay_sec: float,
    retry_jitter_sec: float,
    retryable_status_codes: Set[int],
    connect_timeout_sec: float,
    write_timeout_sec: float,
    pool_timeout_sec: float,
    max_connections: int,
    max_keepalive_connections: int,
    keepalive_expiry_sec: float,
    http2: bool,
) -> Dict[str, Any]:
    batches = _chunk_list(chunks, batch_size)
    if repeat <= 0:
        repeat = 1

    latency_values: List[float] = []
    request_rows: List[Dict[str, Any]] = []
    chunk_annotations: Dict[str, Dict[str, Any]] = {}
    case_started = time.perf_counter()

    timeout = httpx.Timeout(
        timeout=float(timeout_sec),
        connect=max(0.1, float(connect_timeout_sec)),
        read=max(0.1, float(timeout_sec)),
        write=max(0.1, float(write_timeout_sec)),
        pool=max(0.1, float(pool_timeout_sec)),
    )
    limits = httpx.Limits(
        max_connections=max(1, int(max_connections)),
        max_keepalive_connections=max(1, int(max_keepalive_connections)),
        keepalive_expiry=max(0.1, float(keepalive_expiry_sec)),
    )
    async with httpx.AsyncClient(timeout=timeout, limits=limits, http2=bool(http2)) as client:
        for round_index in range(repeat):
            sem = asyncio.Semaphore(max(1, concurrency))

            async def _runner(batch_index: int, batch_chunks: List[NodeChunk]) -> Dict[str, Any]:
                async with sem:
                    row = await _call_one_batch(
                        client,
                        endpoint=endpoint,
                        api_key=api_key,
                        model=model,
                        system_prompt=system_prompt,
                        user_prompt_template=user_prompt_template,
                        persona_json=persona_json,
                        batch_chunks=batch_chunks,
                        batch_index=batch_index,
                        max_retries=max_retries,
                        retry_base_delay_sec=retry_base_delay_sec,
                        retry_max_delay_sec=retry_max_delay_sec,
                        retry_jitter_sec=retry_jitter_sec,
                        retryable_status_codes=retryable_status_codes,
                    )
                    row["round_index"] = round_index
                    return row

            tasks = [
                asyncio.create_task(_runner(batch_index, batch_chunks))
                for batch_index, batch_chunks in enumerate(batches)
            ]
            batch_results = await asyncio.gather(*tasks, return_exceptions=False)
            for row in batch_results:
                request_rows.append(row)
                latency_values.append(float(row.get("latency_ms", 0.0) or 0.0))
                if row.get("ok"):
                    for ann in row.get("annotation_rows", []):
                        chunk_id = str(ann.get("chunk_id", "")).strip()
                        if chunk_id and chunk_id not in chunk_annotations:
                            chunk_annotations[chunk_id] = ann

    case_elapsed = time.perf_counter() - case_started
    total_requests = len(request_rows)
    ok_requests = sum(1 for row in request_rows if row.get("ok"))
    error_requests = total_requests - ok_requests
    retry_requests = sum(1 for row in request_rows if int(row.get("attempt_count", 1) or 1) > 1)
    total_retry_attempts = sum(max(0, int(row.get("attempt_count", 1) or 1) - 1) for row in request_rows)
    retry_sleep_ms_total = sum(float(row.get("retry_sleep_ms_total", 0.0) or 0.0) for row in request_rows)
    annotated_chunks = len(chunk_annotations)
    total_chunks = len(chunks)
    coverage = (annotated_chunks / total_chunks) if total_chunks > 0 else 0.0
    total_tokens = sum(int(row.get("usage_total_tokens", 0) or 0) for row in request_rows)
    prompt_tokens = sum(int(row.get("usage_prompt_tokens", 0) or 0) for row in request_rows)
    completion_tokens = sum(int(row.get("usage_completion_tokens", 0) or 0) for row in request_rows)

    scores = [float(ann.get("relevance_score", 0.5) or 0.5) for ann in chunk_annotations.values()]
    tags_count = [len(ann.get("insights_tags", [])) for ann in chunk_annotations.values()]
    error_type_counts: Dict[str, int] = {}
    for row in request_rows:
        err = str(row.get("error", "") or "").strip()
        if not err:
            continue
        error_type_counts[err] = error_type_counts.get(err, 0) + 1

    return {
        "batch_size": batch_size,
        "concurrency": concurrency,
        "repeat": repeat,
        "total_chunks": total_chunks,
        "batch_count_per_round": len(batches),
        "total_requests": total_requests,
        "ok_requests": ok_requests,
        "error_requests": error_requests,
        "retry_requests": retry_requests,
        "retry_requests_ratio": round((retry_requests / total_requests) if total_requests > 0 else 0.0, 6),
        "total_retry_attempts": total_retry_attempts,
        "attempts_avg_per_request": round((total_requests + total_retry_attempts) / total_requests, 6)
        if total_requests > 0
        else 0.0,
        "retry_sleep_ms_total": round(retry_sleep_ms_total, 6),
        "annotated_chunks": annotated_chunks,
        "coverage_ratio": round(coverage, 6),
        "elapsed_sec": round(case_elapsed, 6),
        "chunk_throughput_per_sec": round((annotated_chunks / case_elapsed) if case_elapsed > 0 else 0.0, 6),
        "request_throughput_per_sec": round((total_requests / case_elapsed) if case_elapsed > 0 else 0.0, 6),
        "latency_p50_ms": round(percentile(latency_values, 50), 6) if latency_values else 0.0,
        "latency_p95_ms": round(percentile(latency_values, 95), 6) if latency_values else 0.0,
        "latency_avg_ms": round(float(statistics.fmean(latency_values)) if latency_values else 0.0, 6),
        "usage_prompt_tokens": prompt_tokens,
        "usage_completion_tokens": completion_tokens,
        "usage_total_tokens": total_tokens,
        "score_avg": round(float(statistics.fmean(scores)) if scores else 0.0, 6),
        "score_p95": round(percentile(scores, 95), 6) if scores else 0.0,
        "insights_tags_avg_count": round(float(statistics.fmean(tags_count)) if tags_count else 0.0, 6),
        "max_retries": int(max_retries),
        "retry_base_delay_sec": round(float(retry_base_delay_sec), 6),
        "retry_max_delay_sec": round(float(retry_max_delay_sec), 6),
        "retry_jitter_sec": round(float(retry_jitter_sec), 6),
        "retryable_status_codes": sorted(int(code) for code in retryable_status_codes),
        "max_connections": int(max_connections),
        "max_keepalive_connections": int(max_keepalive_connections),
        "keepalive_expiry_sec": round(float(keepalive_expiry_sec), 6),
        "http2_enabled": bool(http2),
        "error_type_counts": error_type_counts,
        "request_rows": request_rows,
        "chunk_annotations": list(chunk_annotations.values()),
    }


def _build_markdown_report(
    *,
    metadata: Dict[str, Any],
    summary_rows: List[Dict[str, Any]],
    best_row: Optional[Dict[str, Any]],
) -> str:
    lines: List[str] = []
    lines.append("# Persona Reading Grid Benchmark")
    lines.append("")
    lines.append(f"- generated_at: `{metadata['generated_at']}`")
    lines.append(f"- md_path: `{metadata['md_path']}`")
    lines.append(f"- mode: `{metadata['mode']}`")
    lines.append(f"- chunk_count: `{metadata['chunk_count']}`")
    lines.append(f"- batch_sizes: `{metadata['batch_sizes']}`")
    lines.append(f"- concurrency_sizes: `{metadata['concurrency_sizes']}`")
    lines.append(f"- repeat: `{metadata['repeat']}`")
    lines.append(f"- max_retries: `{metadata['max_retries']}`")
    lines.append(f"- retry_status_codes: `{metadata['retry_status_codes']}`")
    lines.append(
        f"- retry_backoff_ms(base/max/jitter): `{metadata['retry_base_delay_ms']}/"
        f"{metadata['retry_max_delay_ms']}/{metadata['retry_jitter_ms']}`"
    )
    lines.append(
        f"- client_pool(max/max_keepalive/keepalive_expiry_sec/http2): "
        f"`{metadata['max_connections']}/{metadata['max_keepalive_connections']}/"
        f"{metadata['keepalive_expiry_sec']}/{metadata['http2_enabled']}`"
    )
    lines.append("")
    if best_row:
        lines.append("## Best Case")
        lines.append(
            f"- batch_size={best_row['batch_size']}, concurrency={best_row['concurrency']}, "
            f"coverage={best_row['coverage_ratio']:.4f}, elapsed={best_row['elapsed_sec']:.2f}s, "
            f"throughput={best_row['chunk_throughput_per_sec']:.3f} chunk/s, "
            f"p95={best_row['latency_p95_ms']:.1f}ms, errors={best_row['error_requests']}"
        )
        lines.append("")
    lines.append("## Grid Summary")
    lines.append("")
    lines.append(
        "| batch_size | concurrency | coverage | elapsed_sec | chunk_tps | avg_ms | p95_ms | error_requests | retry_requests | total_tokens |"
    )
    lines.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for row in summary_rows:
        lines.append(
            f"| {row['batch_size']} | {row['concurrency']} | {row['coverage_ratio']:.4f} | "
            f"{row['elapsed_sec']:.2f} | {row['chunk_throughput_per_sec']:.3f} | "
            f"{row['latency_avg_ms']:.1f} | {row['latency_p95_ms']:.1f} | {row['error_requests']} | "
            f"{row['retry_requests']} | {row['usage_total_tokens']} |"
        )
    lines.append("")
    lines.append("## Artifacts")
    lines.append("")
    lines.append("- `raw/summary_rows.json`")
    lines.append("- `raw/summary_rows.csv`")
    lines.append("- `raw/request_rows.json`")
    lines.append("- `raw/request_rows.csv`")
    return "\n".join(lines) + "\n"


def _select_best(rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not rows:
        return None
    ranked = sorted(
        rows,
        key=lambda item: (
            -float(item.get("coverage_ratio", 0.0)),
            int(item.get("error_requests", 0)),
            float(item.get("elapsed_sec", 1e18)),
            -float(item.get("chunk_throughput_per_sec", 0.0)),
            float(item.get("latency_p95_ms", 1e18)),
            int(item.get("batch_size", 0)),
            int(item.get("concurrency", 0)),
        ),
    )
    return ranked[0]


def main() -> int:
    parser = argparse.ArgumentParser(description="Persona reading batch/concurrency grid benchmark")
    parser.add_argument(
        "--md-path",
        default=r"var\storage\storage\c786a1956e66ba020dfb2ed46a3b0c3c\enhanced_output.md",
        help="Markdown file path",
    )
    parser.add_argument(
        "--persona-path",
        default=r"services\java-orchestrator\var\tmp_mock_persona.json",
        help="Persona JSON path",
    )
    parser.add_argument(
        "--system-prompt-path",
        default=r"services\java-orchestrator\src\main\resources\prompts\telemetry\persona-reading\system-zh.txt",
        help="System prompt path",
    )
    parser.add_argument(
        "--user-prompt-path",
        default=r"services\java-orchestrator\src\main\resources\prompts\telemetry\persona-reading\user-zh.txt",
        help="User prompt path",
    )
    parser.add_argument("--base-url", default="https://api.deepseek.com/v1", help="DeepSeek base url")
    parser.add_argument("--model", default="deepseek-chat", help="Model")
    parser.add_argument("--batch-sizes", default="1,2,4", help="Comma-separated batch sizes")
    parser.add_argument("--concurrency-sizes", default="1,2,4", help="Comma-separated concurrency sizes")
    parser.add_argument("--repeat", type=int, default=1, help="Rounds per grid point")
    parser.add_argument("--timeout-sec", type=float, default=90.0, help="HTTP timeout seconds")
    parser.add_argument("--connect-timeout-sec", type=float, default=20.0, help="HTTP connect timeout seconds")
    parser.add_argument("--write-timeout-sec", type=float, default=20.0, help="HTTP write timeout seconds")
    parser.add_argument("--pool-timeout-sec", type=float, default=20.0, help="HTTP pool timeout seconds")
    parser.add_argument("--max-connections", type=int, default=0, help="HTTP max connections (0=auto)")
    parser.add_argument(
        "--max-keepalive-connections",
        type=int,
        default=0,
        help="HTTP max keepalive connections (0=auto)",
    )
    parser.add_argument("--keepalive-expiry-sec", type=float, default=8.0, help="HTTP keepalive expiry seconds")
    parser.add_argument("--disable-http2", action="store_true", help="Disable HTTP/2")
    parser.add_argument("--max-retries", type=int, default=2, help="Max retries for transient errors")
    parser.add_argument("--retry-base-delay-ms", type=float, default=180.0, help="Retry base delay milliseconds")
    parser.add_argument("--retry-max-delay-ms", type=float, default=1800.0, help="Retry max delay milliseconds")
    parser.add_argument("--retry-jitter-ms", type=float, default=120.0, help="Retry jitter milliseconds")
    parser.add_argument(
        "--retry-status-codes",
        default="429,500,502,503,504",
        help="Comma-separated HTTP status codes to retry",
    )
    parser.add_argument("--output-root", default="var/benchmarks", help="Benchmark output root")
    parser.add_argument("--task-name", default="persona_reading_grid", help="Task name")
    parser.add_argument("--max-nodes", type=int, default=220, help="Max parsed nodes")
    parser.add_argument(
        "--workload-multiplier",
        type=int,
        default=1,
        help="Duplicate semantic chunks to amplify concurrent workload",
    )
    args = parser.parse_args()

    api_key = str(os.getenv("DEEPSEEK_API_KEY", "")).strip()
    if not api_key:
        raise RuntimeError("DEEPSEEK_API_KEY is not set")

    md_path = Path(args.md_path).resolve()
    if not md_path.is_file():
        raise FileNotFoundError(f"Markdown file not found: {md_path}")

    persona_path = Path(args.persona_path).resolve()
    system_prompt_path = Path(args.system_prompt_path).resolve()
    user_prompt_path = Path(args.user_prompt_path).resolve()

    markdown_text = _read_text(md_path)
    nodes = _parse_markdown_nodes(markdown_text)
    if args.max_nodes > 0:
        nodes = nodes[: args.max_nodes]
    chunks = _build_semantic_chunks(nodes)
    if not chunks:
        raise RuntimeError("No semantic chunks parsed from markdown")
    base_chunk_count = len(chunks)
    chunks = _expand_chunks_for_load(chunks, args.workload_multiplier)

    persona = _load_persona(persona_path)
    persona_json = json.dumps(persona, ensure_ascii=False)
    system_prompt = _read_text(system_prompt_path)
    user_prompt_template = _read_text(user_prompt_path)

    batch_sizes = parse_int_list(args.batch_sizes, "batch_sizes")
    concurrency_sizes = parse_int_list(args.concurrency_sizes, "concurrency_sizes")
    retry_status_codes = set(parse_int_list(args.retry_status_codes, "retry_status_codes"))
    if not retry_status_codes:
        retry_status_codes = set(DEFAULT_RETRYABLE_STATUS_CODES)
    http2_enabled = not bool(args.disable_http2)
    endpoint = str(args.base_url or "").rstrip("/")
    if not endpoint:
        endpoint = "https://api.deepseek.com/v1"
    if not endpoint.lower().endswith("/v1"):
        endpoint = f"{endpoint}/v1"

    output_dir, raw_dir, _charts_dir = ensure_benchmark_dirs(args.output_root, args.task_name)
    metadata: Dict[str, Any] = {
        "generated_at": now_iso(),
        "md_path": str(md_path),
        "persona_path": str(persona_path),
        "system_prompt_path": str(system_prompt_path),
        "user_prompt_path": str(user_prompt_path),
        "mode": "semantic_unit",
        "node_count": len(nodes),
        "base_chunk_count": base_chunk_count,
        "chunk_count": len(chunks),
        "workload_multiplier": max(1, int(args.workload_multiplier)),
        "batch_sizes": batch_sizes,
        "concurrency_sizes": concurrency_sizes,
        "repeat": max(1, int(args.repeat)),
        "max_retries": max(0, int(args.max_retries)),
        "retry_base_delay_ms": max(0.0, float(args.retry_base_delay_ms)),
        "retry_max_delay_ms": max(0.0, float(args.retry_max_delay_ms)),
        "retry_jitter_ms": max(0.0, float(args.retry_jitter_ms)),
        "retry_status_codes": sorted(int(code) for code in retry_status_codes),
        "connect_timeout_sec": max(0.1, float(args.connect_timeout_sec)),
        "write_timeout_sec": max(0.1, float(args.write_timeout_sec)),
        "pool_timeout_sec": max(0.1, float(args.pool_timeout_sec)),
        "max_connections": int(args.max_connections),
        "max_keepalive_connections": int(args.max_keepalive_connections),
        "keepalive_expiry_sec": max(0.1, float(args.keepalive_expiry_sec)),
        "http2_enabled": http2_enabled,
        "model": args.model,
        "base_url": endpoint,
    }
    write_json(raw_dir / "metadata.json", metadata)

    all_request_rows: List[Dict[str, Any]] = []
    summary_rows: List[Dict[str, Any]] = []

    for batch_size in batch_sizes:
        for concurrency in concurrency_sizes:
            resolved_max_connections = (
                int(args.max_connections) if int(args.max_connections) > 0 else max(64, int(concurrency) * 3)
            )
            resolved_max_keepalive = (
                int(args.max_keepalive_connections)
                if int(args.max_keepalive_connections) > 0
                else max(32, int(concurrency) * 2)
            )
            print(f"[Run] batch_size={batch_size}, concurrency={concurrency}")
            case = asyncio.run(
                _run_case(
                    endpoint=endpoint,
                    api_key=api_key,
                    model=str(args.model),
                    timeout_sec=float(args.timeout_sec),
                    system_prompt=system_prompt,
                    user_prompt_template=user_prompt_template,
                    persona_json=persona_json,
                    chunks=chunks,
                    batch_size=batch_size,
                    concurrency=concurrency,
                    repeat=max(1, int(args.repeat)),
                    max_retries=max(0, int(args.max_retries)),
                    retry_base_delay_sec=max(0.0, float(args.retry_base_delay_ms)) / 1000.0,
                    retry_max_delay_sec=max(0.0, float(args.retry_max_delay_ms)) / 1000.0,
                    retry_jitter_sec=max(0.0, float(args.retry_jitter_ms)) / 1000.0,
                    retryable_status_codes=retry_status_codes,
                    connect_timeout_sec=max(0.1, float(args.connect_timeout_sec)),
                    write_timeout_sec=max(0.1, float(args.write_timeout_sec)),
                    pool_timeout_sec=max(0.1, float(args.pool_timeout_sec)),
                    max_connections=resolved_max_connections,
                    max_keepalive_connections=resolved_max_keepalive,
                    keepalive_expiry_sec=max(0.1, float(args.keepalive_expiry_sec)),
                    http2=http2_enabled,
                )
            )
            summary = {
                key: value
                for key, value in case.items()
                if key not in {"request_rows", "chunk_annotations"}
            }
            summary_rows.append(summary)

            for row in case["request_rows"]:
                flattened = dict(row)
                flattened["batch_size"] = batch_size
                flattened["concurrency"] = concurrency
                flattened["chunk_ids"] = json.dumps(flattened.get("chunk_ids", []), ensure_ascii=False)
                flattened["retry_reasons"] = json.dumps(flattened.get("retry_reasons", []), ensure_ascii=False)
                flattened["annotation_rows"] = json.dumps(flattened.get("annotation_rows", []), ensure_ascii=False)
                all_request_rows.append(flattened)

            write_json(raw_dir / f"case_b{batch_size}_c{concurrency}.json", case)

    summary_rows.sort(key=lambda item: (int(item["batch_size"]), int(item["concurrency"])))
    best_row = _select_best(summary_rows)
    report_md = _build_markdown_report(metadata=metadata, summary_rows=summary_rows, best_row=best_row)

    write_json(raw_dir / "summary_rows.json", summary_rows)
    write_csv(raw_dir / "summary_rows.csv", summary_rows)
    write_json(raw_dir / "request_rows.json", all_request_rows)
    write_csv(raw_dir / "request_rows.csv", all_request_rows)
    write_json(raw_dir / "best_case.json", best_row or {})
    (output_dir / "report.md").write_text(report_md, encoding="utf-8")

    print(f"[Done] output_dir={output_dir}")
    if best_row:
        print(
            "[Best] "
            f"batch_size={best_row['batch_size']} concurrency={best_row['concurrency']} "
            f"coverage={best_row['coverage_ratio']:.4f} elapsed={best_row['elapsed_sec']:.2f}s "
            f"chunk_tps={best_row['chunk_throughput_per_sec']:.3f} p95={best_row['latency_p95_ms']:.1f}ms "
            f"errors={best_row['error_requests']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
