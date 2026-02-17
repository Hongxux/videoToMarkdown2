from __future__ import annotations

import argparse
import asyncio
import os
import statistics
import sys
import time
from datetime import datetime
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


async def _run_case(
    *,
    concurrency: int,
    total_requests: int,
    api_key: str,
    base_url: str,
    model: str,
    prompt: str,
    system_prompt: str,
    timeout_sec: float,
    max_tokens: int,
    temperature: float,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    timeout = httpx.Timeout(timeout_sec, connect=min(10.0, timeout_sec))
    limits = httpx.Limits(
        max_connections=max(20, concurrency * 2),
        max_keepalive_connections=max(10, concurrency),
        keepalive_expiry=30.0,
    )

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
        ],
        "temperature": float(temperature),
        "max_tokens": int(max_tokens),
        "response_format": {"type": "json_object"},
    }

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
                        "success": response.status_code == 200,
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
                        "success": False,
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
                        "success": False,
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

    elapsed_values = [float(item["elapsed_ms"]) for item in valid_records]
    ok_elapsed_values = [float(item["elapsed_ms"]) for item in ok_records]

    summary = {
        "concurrency": int(concurrency),
        "total_requests": int(total_requests),
        "ok_count": len(ok_records),
        "rate_limit_429_count": len(rate_429_records),
        "timeout_count": len(timeout_records),
        "other_http_error_count": len(other_http_records),
        "network_error_count": len(network_records),
        "success_rate_percent": (len(ok_records) / total_requests * 100.0) if total_requests else 0.0,
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


def _write_report(
    *,
    output_path: Path,
    metadata: Dict[str, Any],
    summary_rows: List[Dict[str, Any]],
) -> None:
    lines: List[str] = []
    lines.append("# Transcript DeepSeek 并发阶梯压测报告")
    lines.append("")
    lines.append(f"- 生成时间: {metadata['generated_at']}")
    lines.append(f"- 基础URL: `{metadata['base_url']}`")
    lines.append(f"- 模型: `{metadata['model']}`")
    lines.append(f"- 并发阶梯: `{metadata['concurrency_ladder']}`")
    lines.append(f"- 每档请求数: `{metadata['requests_per_case']}`")
    lines.append(f"- 请求超时: `{metadata['timeout_sec']}` 秒")
    lines.append("")
    lines.append("## 结果汇总")
    lines.append(
        "| 并发 | 请求数 | 成功 | 429 | timeout | 其他HTTP | 网络异常 | 成功率(%) | 总时长(ms) | P95请求时延(ms) | QPS(case) |"
    )
    lines.append("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for row in summary_rows:
        lines.append(
            f"| {int(row['concurrency'])} | {int(row['total_requests'])} | {int(row['ok_count'])} | "
            f"{int(row['rate_limit_429_count'])} | {int(row['timeout_count'])} | "
            f"{int(row['other_http_error_count'])} | {int(row['network_error_count'])} | "
            f"{float(row['success_rate_percent']):.2f} | {float(row['case_total_elapsed_ms']):.2f} | "
            f"{float(row['p95_request_elapsed_ms']):.2f} | {float(row['requests_per_second_case_level']):.3f} |"
        )
    lines.append("")
    lines.append("## 产物")
    lines.append("- `metadata.json`")
    lines.append("- `raw/summary.json`")
    lines.append("- `raw/summary.csv`")
    lines.append("- `raw/requests_c*.json`")
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


async def _amain(args: argparse.Namespace) -> int:
    api_key, base_url, model = _resolve_deepseek_config()
    if not api_key or api_key == "your-deepseek-api-key-here":
        raise RuntimeError("DEEPSEEK_API_KEY 未配置，无法执行真实压测")

    concurrency_ladder = parse_int_list(args.concurrency, "concurrency")
    output_dir, raw_dir, _charts_dir = ensure_benchmark_dirs(args.output_root, args.task_name)

    metadata = {
        "generated_at": now_iso(),
        "base_url": base_url,
        "model": model,
        "concurrency_ladder": concurrency_ladder,
        "requests_per_case": int(args.requests_per_case),
        "timeout_sec": float(args.timeout_sec),
        "max_tokens": int(args.max_tokens),
        "temperature": float(args.temperature),
        "prompt_preview": args.prompt[:120],
        "system_prompt_preview": args.system_prompt[:120],
        "output_dir": str(output_dir.resolve()),
    }
    write_json(output_dir / "metadata.json", metadata)

    summary_rows: List[Dict[str, Any]] = []
    for concurrency in concurrency_ladder:
        print(f"[Run] concurrency={concurrency}, total_requests={args.requests_per_case}")
        summary, request_rows = await _run_case(
            concurrency=concurrency,
            total_requests=args.requests_per_case,
            api_key=api_key,
            base_url=base_url,
            model=model,
            prompt=args.prompt,
            system_prompt=args.system_prompt,
            timeout_sec=args.timeout_sec,
            max_tokens=args.max_tokens,
            temperature=args.temperature,
        )
        summary["started_at"] = now_iso()
        summary_rows.append(summary)
        write_json(raw_dir / f"requests_c{concurrency}.json", request_rows)

    summary_rows.sort(key=lambda item: int(item["concurrency"]))
    write_json(raw_dir / "summary.json", summary_rows)
    write_csv(raw_dir / "summary.csv", summary_rows)
    _write_report(
        output_path=output_dir / "report.md",
        metadata=metadata,
        summary_rows=summary_rows,
    )

    print("\n=== DeepSeek 并发阶梯压测完成 ===")
    print(f"输出目录: {output_dir.resolve()}")
    for row in summary_rows:
        print(
            " | ".join(
                [
                    f"c={int(row['concurrency'])}",
                    f"ok={int(row['ok_count'])}/{int(row['total_requests'])}",
                    f"429={int(row['rate_limit_429_count'])}",
                    f"timeout={int(row['timeout_count'])}",
                    f"total_ms={float(row['case_total_elapsed_ms']):.2f}",
                ]
            )
        )
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Transcript DeepSeek 并发阶梯压测（关注 429/timeout/总时长）")
    parser.add_argument("--concurrency", default="12,16,20", help="并发阶梯，逗号分隔")
    parser.add_argument("--requests-per-case", type=int, default=90, help="每档并发的总请求数")
    parser.add_argument("--timeout-sec", type=float, default=45.0, help="单请求超时（秒）")
    parser.add_argument("--max-tokens", type=int, default=96, help="单请求 max_tokens")
    parser.add_argument("--temperature", type=float, default=0.0, help="采样温度")
    parser.add_argument(
        "--system-prompt",
        default="你是一个严格的JSON助手。只返回JSON对象。",
        help="system prompt",
    )
    parser.add_argument(
        "--prompt",
        default='请输出 {"ok": true, "msg": "ping"}',
        help="user prompt",
    )
    parser.add_argument("--output-root", default="var/artifacts/benchmarks", help="压测产物根目录")
    parser.add_argument("--task-name", default="transcript_deepseek_concurrency_ladder", help="任务名")
    args = parser.parse_args()

    if args.requests_per_case <= 0:
        raise ValueError("requests-per-case 必须大于 0")
    if args.timeout_sec <= 0:
        raise ValueError("timeout-sec 必须大于 0")
    if args.max_tokens <= 0:
        raise ValueError("max-tokens 必须大于 0")
    asyncio.run(_amain(args))


if __name__ == "__main__":
    main()
