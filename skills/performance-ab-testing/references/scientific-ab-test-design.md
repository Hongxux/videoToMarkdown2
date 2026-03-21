# Scientific AB Test Design

## Table of Contents
- [Principles](#principles)
- [1. Define the Goal and Metrics](#1-define-the-goal-and-metrics)
- [2. Control Variables and Environment](#2-control-variables-and-environment)
- [3. Run with Enough Sample Volume](#3-run-with-enough-sample-volume)
- [4. Clean and Analyze the Data](#4-clean-and-analyze-the-data)
- [5. Make the Decision](#5-make-the-decision)
- [6. Common Failure Modes](#6-common-failure-modes)
- [7. Example](#7-example)

## Principles

Use the experiment only when it satisfies all five constraints:
- single variable
- comparable traffic and environment
- statistically meaningful evidence
- reproducible setup
- business-relevant impact

Reject conclusions drawn from averages alone, from short cold-start runs, or from runs where environment drift was not controlled.

## 1. Define the Goal and Metrics

### 1.1 Clarify the Experiment Boundary

State:
- the optimization scenario: API latency, page load, throughput, resource use, or high-concurrency stability
- the baseline version: current stable implementation
- the candidate version: baseline plus only the optimization under test
- the only intended variable change

### 1.2 Use a Three-Layer Metric Model

| Layer | Purpose | Typical metrics |
| --- | --- | --- |
| Core | Prove the performance result | P90/P99/P999 latency, QPS/TPS, throughput ceiling |
| Supporting | Explain why the result is real | CPU, memory, disk I/O, network, cache hit rate, GC pause |
| Safety | Ensure no hidden regression | error rate, timeout rate, restart count, data consistency |

Rules:
- Prefer percentiles over averages.
- Always pair latency improvements with resource metrics.
- Always pair throughput improvements with error and timeout metrics.

## 2. Control Variables and Environment

### 2.1 Keep the Environment Isomorphic

Match both sides on:
- CPU model and core count
- memory size and limits
- disk type
- network path
- operating system and runtime version
- middleware version and config
- deployment topology and replica count
- container resource quota and scheduling rule
- downstream database, cache, MQ, and third-party dependency

Avoid:
- physical machine vs virtual machine comparisons
- cross-zone comparisons
- one side hitting real downstream while the other hits mocks
- resource sharing that lets one group steal CPU or memory from the other

### 2.2 Keep the Traffic Homogeneous

For offline load testing:
- replay realistic request shapes from production logs
- keep scripts, think time, concurrency ladder, and load generators identical

For online A/B:
- use stable hashing such as user ID or request-feature hashing
- keep the same user on the same side
- start with a small canary and scale toward balanced traffic only after stability checks pass
- verify the two groups have comparable user, region, request-type, and scenario distributions

## 3. Run with Enough Sample Volume

### 3.1 Warm Up Before Sampling

Reserve a warmup period so that:
- JIT compilation stabilizes
- caches warm
- connection pools fill
- background workers and thread pools settle

Typical guidance:
- offline benchmark: at least 10 minutes when the path is complex
- online experiment: at least 1 hour before trusting the data window

### 3.2 Use Enough Sample Size and Duration

Aim for:
- enough request volume to cover long-tail latency
- at least three repeated offline runs when practical
- at least one full business cycle online, including peak and off-peak periods

Avoid conclusions from a few minutes of traffic or tiny sample sets.

## 4. Clean and Analyze the Data

### 4.1 Keep Collection Rules Identical

Use the same:
- instrumentation point
- timing boundary
- sampling rule
- aggregation interval
- error categorization

Prefer full data collection. If sampling is necessary, state the sample rule.

### 4.2 Clean Data Conservatively

Only exclude observations when the anomaly is clearly unrelated to the experiment itself, such as:
- external third-party outage
- unrelated network event
- infrastructure-wide incident

Do not remove failures caused by the candidate implementation.

### 4.3 Check Statistical Significance

Latency distributions are usually skewed. Prefer non-parametric testing such as Mann-Whitney U instead of assuming normality.

Use significance plus effect size together:
- p-value tells whether the difference is likely real
- effect size tells whether the difference matters

## 5. Make the Decision

Treat the optimization as valid only when all four conditions hold:
- the core metric meets the target and has business value
- the difference is statistically defensible or clearly reproducible when significance testing is not available
- supporting and safety metrics do not regress
- repeated runs or repeated periods tell the same story

When writing the conclusion, always include:
- what improved
- by how much
- under what load and environment
- what tradeoff remains

## 6. Common Failure Modes

- Environment mismatch invalidates the comparison.
- Average latency hides tail regression.
- Small sample size or short duration turns noise into fake wins.
- Shared cluster resources create interference between groups.
- Missing significance or reproducibility checks turns random variation into false certainty.
- Cold-start data pollutes steady-state conclusions.

## 7. Example

Goal: verify whether an order-query algorithm optimization reduces latency without raising CPU cost.

Checklist:
- Baseline and candidate run on equivalent hardware.
- Both sides use the same JVM options, same database, same container limits.
- Requests are replayed from real production traffic.
- Warmup runs before collection.
- Three repeated 30-minute steady-state runs are collected.

A valid final statement looks like this:
- P99 latency dropped from `200ms` to `140ms`, a `30%` reduction.
- CPU dropped from `35%` to `28%`.
- Error rate and timeout rate remained `0`.
- Repeated runs were consistent.
- Therefore the optimization is both operationally safe and user-visible.
