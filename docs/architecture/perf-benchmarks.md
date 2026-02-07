# 性能基准归档

## 2026-02-08 Phase2A（AnalyzeWithVL）耗时对比

### 1) 测试目标与范围
- 目标：验证本轮改造后，`AnalyzeWithVL` 的关键耗时是否下降，尤其是：
  - 路由截图阶段（process short 路由到 CV coarse-fine）
  - VL处理阶段（含 VL 内部 screenshot optimization）
- 范围：仅比较本次改造涉及路径，不比较下载/转写/Stage1/Phase2B。

### 2) 测试对象与输入数据
- 视频与语义单元：`task_hash=99efb7c15a9121f4e29113821d5c9c73`
- 输入文件：
  - 视频：`storage/99efb7c15a9121f4e29113821d5c9c73/video.mp4`
  - 语义单元：`storage/99efb7c15a9121f4e29113821d5c9c73/semantic_units_phase2a.json`
- 输出目录：`storage/99efb7c15a9121f4e29113821d5c9c73`

### 3) 数据来源（可追溯）
- 改造前（Baseline）：用户提供完整日志（时间窗：`2026-02-08 00:54:52` ~ `2026-02-08 01:00:36`）。
  - 关键行（人工基准）：
    - `路由截图 完成: units=10, screenshots=113, ms=660215.7, concurrency=4, batch_size=16`
    - `VL 处理完成: units=3, screenshots=24, clips=17, ms=660216.1`
    - `AnalyzeWithVL 混合结果: total_units=14, vl_units=3, screenshots=137, clips=25`
- 改造后（After）：本地复测日志 `storage/bench_after_vl_clean.log`
  - 关键行：
    - `路由截图完成: units=10, screenshots=113, ms=276035.9, mode=process_streaming, workers=6, queue_maxsize=16, pids=[...]`
    - `VL 处理完成: units=3, screenshots=24, clips=17, ms=276036.3`
    - `AnalyzeWithVL 混合结果: total_units=14, vl_units=3, screenshots=137, clips=25`
- 结构化汇总：`storage/bench_compare_summary.json`

### 4) 测试方式与执行步骤
1. 准备复测脚本：`storage/bench_analyze_with_vl.py`
2. 使用同输入运行一次 `AnalyzeWithVL` 并落日志：

```bash
cmd /c "set PYTHONPATH=D:\videoToMarkdownTest2&& python storage\bench_analyze_with_vl.py > storage\bench_after_vl_clean.log 2>&1"
```

3. 从日志提取以下指标并对比：
   - `路由截图完成` 的 `ms`
   - `VL 处理完成` 的 `ms`
   - 同时间区间 `Coarse-Fine selection [x-y]` 的耗时
   - 输出一致性（screenshots / clips 总量）

### 5) 统计口径说明
- 路由截图阶段耗时：取 `python_grpc_server` 日志行 `路由截图完成 ... ms=...`。
- VL处理阶段耗时：取 `python_grpc_server` 日志行 `VL 处理完成 ... ms=...`。
- Coarse-Fine 对比：仅对“改造前后都出现的同时间区间”做逐区间对比，避免口径漂移。
- 输出一致性：以 `AnalyzeWithVL 混合结果` 的 `screenshots` / `clips` 总数为准。

### 6) 对比结果（阶段耗时）

| 阶段 | 改造前 | 改造后 | 变化 |
|---|---:|---:|---:|
| 路由截图阶段（units=10, screenshots=113） | 660,215.7 ms | 276,035.9 ms | **-58.19%** |
| VL处理阶段（units=3, screenshots=24, clips=17） | 660,216.1 ms | 276,036.3 ms | **-58.19%** |
| Coarse-Fine 长窗口平均（7个同区间） | 235.023 s | 130.459 s | **-44.49%** |
| Coarse-Fine 长窗口最大（同区间） | 344.48 s | 169.12 s | **-50.91%** |
| 路由并发形态 | legacy batch（concurrency=4） | process_streaming（workers=6, active pids=6） | 并发利用提升 |
| 输出一致性 | screenshots=137, clips=25 | screenshots=137, clips=25 | 一致 |

### 7) 同区间 Coarse-Fine 明细（改造前后）

| 时间区间(s) | 改造前(s) | 改造后(s) | 提升 |
|---|---:|---:|---:|
| 248.0-288.0 | 344.48 | 169.12 | -50.91% |
| 318.0-359.0 | 255.13 | 155.16 | -39.18% |
| 359.0-391.0 | 215.98 | 125.84 | -41.74% |
| 391.0-416.0 | 157.40 | 105.77 | -32.80% |
| 517.0-554.0 | 226.89 | 129.73 | -42.82% |
| 633.0-674.0 | 256.91 | 135.82 | -47.13% |
| 674.0-700.0 | 188.37 | 91.77 | -51.28% |

### 8) 本次测试中的异常样本说明
- 在 PowerShell 直接通过 stdin 执行脚本时，出现过 `BrokenProcessPool`，该样本已剔除。
- 原因：Windows 下 `ProcessPool` 与 stdin 启动方式组合时稳定性较差，非业务逻辑回归。
- 处理：改为 `cmd + 文件脚本` 方式复测（即本归档使用的 `bench_after_vl_clean.log`）。

### 9) 结论
- 在输出口径保持一致（`screenshots=137`, `clips=25`）前提下，本次改造显著降低了核心耗时：
  - 路由截图与 VL阶段耗时均下降约 **58%**。
  - 关键长窗口的 Coarse-Fine 单任务耗时平均下降约 **44%**。
- 并发可观测性明确提升：日志可直接看到 `process_streaming`、`workers`、`active_pids`、`pids` 列表。
