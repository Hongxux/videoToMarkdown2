---
name: performance-ab-testing
description: Design, run, review, and document scientific performance A/B tests, load tests, and benchmark comparisons. Use when validating whether a code, config, architecture, caching, concurrency, or pipeline optimization truly improves latency, tail latency, throughput, resource usage, or stability; when choosing or extending this repo's benchmark scripts; or when turning raw benchmark data into defensible engineering conclusions with controlled variables, reproducible setup, and statistical checks.
---

# Performance AB Testing

## Overview

Use this skill to turn a performance claim into a defensible experiment.

Prefer existing benchmark scripts and artifact conventions in this repo over inventing new one-off measurements. Treat any "performance improvement" as unproven until the comparison controls variables, measures tail latency and stability, and explains business impact.

## Workflow

### 1. Freeze the Hypothesis

Start by restating:
- the optimization under test
- the baseline version
- the single variable that changes
- the user-visible goal: lower P99, higher throughput, lower CPU, better stability, or a mix

If more than one variable changes, do not present the result as a clean A/B test. Downgrade the claim or split the experiment.

### 2. Choose the Benchmark Path

Use the narrowest path that already exists:
- If an existing repo benchmark matches the target path, reuse it. Read [references/repo-benchmark-map.md](references/repo-benchmark-map.md).
- If an existing benchmark is close, extend it without changing its output schema unless necessary.
- If no benchmark exists, create one that preserves this repo's artifact layout and emits raw rows, summary rows, system samples, recommendation output, and a readable markdown report.

### 3. Control the Comparison

Hold constant:
- hardware or container limits
- runtime flags, dependency versions, and downstream dependencies
- input data, request mix, concurrency ladder, and warmup rules
- benchmark duration, repetition count, and collection method

If the environment is not fully identical, state the exact mismatch and reduce confidence in the conclusion.

### 4. Build the Metric Set

Always measure three layers:
- Core metrics: throughput, latency, and especially tail latency such as P95/P99/P999
- Supporting metrics: CPU, memory, disk I/O, network, GC, cache hit rate, or stage timings
- Safety metrics: error rate, timeout rate, restart count, data correctness, and any user-visible regression

Prefer percentiles over averages. Average-only reporting is insufficient.

### 5. Run the Experiment Cleanly

Enforce:
- a warmup phase before sampling
- enough repetitions to reject one-off noise
- enough sample volume to cover tail behavior
- a load generator that is not itself the bottleneck
- config snapshots and exact commands in the output

When practical, repeat the same test at least three times and compare consistency before making claims.

### 6. Judge Validity Before Declaring a Win

Treat a result as valid only when all of the following hold:
- the core metric moves in the intended direction
- the effect size is meaningful, not just numerically different
- supporting metrics do not show obvious resource overdraw
- safety metrics do not regress
- repeated runs tell the same story

If the sample set is large enough and raw data is available, prefer a non-parametric test such as Mann-Whitney U for latency comparisons. If significance testing is not practical, say so explicitly instead of implying scientific certainty.

### 7. Convert the Result into Engineering Language

Do not stop at "faster" or "slower". Explain:
- what bottleneck was removed
- why the new topology or algorithm should help
- where the new limit moved
- what tradeoff remains

Prefer language a backend interviewer understands:
- "fixed queueing and stage blocking" instead of project-specific stage nicknames
- "bounded concurrency window" instead of internal flag names
- "separated I/O-bound work from CPU-bound work" instead of local jargon

## Repo Rules

Read [references/repo-benchmark-map.md](references/repo-benchmark-map.md) before adding new benchmark code.

Preserve this repo's benchmark artifact style under `var/artifacts/benchmarks/<task_name>_<timestamp>/`.

If the benchmark supports or changes an architecture decision, update:
- `docs/architecture/upgrade-log.md`
- `docs/architecture/error-fixes.md` when the work is a bug fix or regression fix

If a benchmark result is later used in resume or interview material, make sure the claim is traceable to a concrete report, raw summary file, or test.

## References

- Read [references/scientific-ab-test-design.md](references/scientific-ab-test-design.md) when designing or reviewing a controlled experiment.
- Read [references/repo-benchmark-map.md](references/repo-benchmark-map.md) when selecting scripts, output layouts, or repo-specific benchmark entry points.

## Standard Deliverables

When completing a benchmark task with this skill, produce:
- a one-paragraph hypothesis
- the single variable and what stayed constant
- exact commands or scripts used
- sample size, warmup rule, and environment summary
- a baseline-vs-candidate table
- validity risks and uncontrolled factors
- a conclusion that states both performance impact and confidence level
