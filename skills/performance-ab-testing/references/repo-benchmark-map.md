# Repo Benchmark Map

## Table of Contents
- [Artifact Layout](#artifact-layout)
- [Core Entry Points](#core-entry-points)
- [Script Selection Guide](#script-selection-guide)
- [Reuse Rules](#reuse-rules)
- [Useful Search Patterns](#useful-search-patterns)

## Artifact Layout

Prefer this output layout:
- `var/artifacts/benchmarks/<task_name>_<timestamp>/`

Common files:
- `raw/runs_raw.json|csv`
- `raw/summary_rows.json|csv` or `raw/summary_by_case.json|csv`
- `raw/recommendation.json`
- `raw/system_samples_*.json`
- `charts/concurrency_summary.png`
- `report.md`

Reference:
- `docs/benchmarks/README.md`

## Core Entry Points

### Existing benchmark overview

Use these before writing anything new:

| Path | Purpose |
| --- | --- |
| `scripts/bench_llm_deepseek_concurrency_batch.py` | DeepSeek text concurrency, chunk size, token-budget comparison |
| `scripts/bench_transcript_deepseek_concurrency_ladder.py` | Transcript-side model concurrency ladder and timeout/429 tracking |
| `scripts/bench_phase2b_material_flow_concurrency.py` | Phase2B structure preprocess vs OCR worker tuning |
| `scripts/bench_route_screenshot_concurrency.py` | Route screenshot worker count, queue size, mode comparison |
| `scripts/bench_screenshot_opt_concurrency.py` | Screenshot optimization concurrency and overlap tuning |
| `scripts/bench_pre_vl_concurrency_ladder.py` | Pre-VL pruning concurrency knee detection |
| `scripts/bench_vl_llm_concurrency_payload.py` | VL payload concurrency and input-frame tradeoff |
| `scripts/bench_vision_concurrency_batchability.py` | Vision single vs batch validation throughput |
| `scripts/bench_tutorial_asset_export_concurrency.py` | Tutorial asset export worker tuning |
| `tools/benchmarks/TaskWebSocketHeartbeatBenchmark.java` | Java-side WebSocket heartbeat microbenchmark |
| `tools/benchmarks/TaskWebSocketE2EBenchmark.java` | Java-side WebSocket end-to-end delivery benchmark |
| `tools/benchmarks/runtime_recovery_*.py` | Runtime recovery read/write/query-plan comparisons |

### Existing evidence docs

Look here before making claims:
- `docs/architecture/perf-benchmarks.md`
- `docs/architecture/upgrade-log.md`
- `docs/architecture/面试.md`

## Script Selection Guide

Choose by bottleneck type:

- Remote model or API I/O bottleneck:
  - start with `bench_llm_deepseek_concurrency_batch.py`
  - use `bench_transcript_deepseek_concurrency_ladder.py` when concurrency ladders, timeouts, or 429 behavior matter

- CPU-bound computer vision or screenshot selection:
  - start with `bench_route_screenshot_concurrency.py`
  - use `bench_screenshot_opt_concurrency.py` for screenshot-quality pipeline tuning

- Mixed CPU + I/O material flow:
  - start with `bench_phase2b_material_flow_concurrency.py`

- Preprocessing or data-pruning stage:
  - start with `bench_pre_vl_concurrency_ladder.py`

- Java WebSocket path:
  - start with `tools/benchmarks/TaskWebSocketHeartbeatBenchmark.java`
  - use `tools/benchmarks/TaskWebSocketE2EBenchmark.java` for delivery and latency

If no script fits exactly, copy the nearest one and keep its output schema unless there is a strong reason to change it.

## Reuse Rules

- Reuse existing sample data under `var/artifacts/benchmarks/sample_data/` whenever possible.
- Keep benchmark names descriptive and timestamped.
- Keep warmup, repeats, and ladder settings explicit in metadata or report output.
- Capture both performance metrics and resource metrics.
- If a benchmark result changes architecture or production configuration, update `docs/architecture/upgrade-log.md`.
- If the result fixes a regression or bug, update `docs/architecture/error-fixes.md`.

## Useful Search Patterns

Use `rg` before inventing a new script:

```powershell
rg -n "summary_rows|recommendation|throughput|p95|cpu_mean" scripts tools docs/architecture var/artifacts/benchmarks
rg --files scripts | rg "^bench_.*\\.py$"
rg -n "benchmark|压测|throughput|latency|P99" docs/architecture docs/benchmarks
```

Use these repo files as quick orientation:
- `docs/benchmarks/README.md`
- `var/artifacts/benchmarks/*/report.md`
