# Phase2 Runtime-State-First Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 Phase2A / Phase2B 在正常链路与恢复链路中都以 Stage1 runtime payload 为主传输通道，不再依赖 `step2/step6/sentence_timestamps` 中间 JSON 作为主通道。

**Architecture:** 保留现有 `output_dir + runtime store` 模式，不在本轮引入新的 proto 结构化字段；Python 侧优先读取 runtime payload，恢复链路不再回灌 JSON，Java 侧将 path 字段降级为可空兼容字段。历史任务如果只有 legacy JSON，则只在兼容入口处读取一次并转换为统一 runtime payload，再走同一条下游链路。

**Tech Stack:** Python gRPC (`grpc_service_impl.py` / `runtime_recovery_context.py`), Python Phase2B assembly (`rich_text_pipeline.py`, `subtitle_repository.py`), Java orchestrator (`VideoProcessingOrchestrator`, `TaskProcessingWorker`), pytest, Maven, architecture docs.

---

### Task 1: 收紧 Python 恢复契约，停止从 runtime state 回灌 Stage1 JSON

**Files:**
- Modify: `services/python_grpc/src/server/runtime_recovery_context.py`
- Modify: `services/python_grpc/src/server/tests/test_runtime_recovery_context.py`
- Modify: `services/python_grpc/src/server/tests/test_recover_runtime_context.py`

- [ ] **Step 1: 写失败测试，锁定 runtime-state-first 恢复行为**

在 `services/python_grpc/src/server/tests/test_runtime_recovery_context.py` 中把现有 “materialize stage1 recovery artifacts” 断言改成“runtime state 存在时不写这三类 JSON，返回空 path”，并保留一个 legacy JSON-only 兼容场景。

```python
artifact_paths = resolver.materialize_stage1_recovery_artifacts(
    output_dir=str(tmp_path),
    runtime_state=runtime_state,
)

assert artifact_paths == {
    "step2_json_path": "",
    "step6_json_path": "",
    "sentence_timestamps_path": "",
}
```

- [ ] **Step 2: 运行测试，确认先红**

Run:

```bash
pytest services/python_grpc/src/server/tests/test_runtime_recovery_context.py -q -k "materialize_stage1_recovery_artifacts or legacy"
```

Expected: 至少一个断言仍在期待生成 JSON 路径，测试失败。

- [ ] **Step 3: 最小实现 `runtime_recovery_context.py`**

实现原则：

- 如果 `runtime_state` 已经包含 `step2_subtitles / step6_paragraphs / sentence_timestamps`，则：
  - 不创建 `intermediates/step2_correction_output.json`
  - 不创建 `intermediates/step6_merge_cross_output.json`
  - 不创建 `intermediates/sentence_timestamps.json`
  - 直接返回空 path
- 只有历史兼容分支才去读已有 JSON

建议收口代码形态：

```python
if has_runtime_stage1_payload(runtime_views):
    return {
        "step2_json_path": "",
        "step6_json_path": "",
        "sentence_timestamps_path": "",
    }
```

- [ ] **Step 4: 扩展 `RecoverRuntimeContext` 响应测试**

在 `services/python_grpc/src/server/tests/test_recover_runtime_context.py` 中把当前“恢复时会落出 step2/step6/sentence_timestamps 文件”的断言改成：

- `stage1_ready is True`
- `step2_json_path == ""`
- `step6_json_path == ""`
- `sentence_timestamps_path == ""`

同时保留历史兼容测试，验证已有 JSON 仍可被识别。

- [ ] **Step 5: 运行恢复侧测试，确认转绿**

Run:

```bash
pytest services/python_grpc/src/server/tests/test_runtime_recovery_context.py services/python_grpc/src/server/tests/test_recover_runtime_context.py -q
```

Expected: 通过，且新的 runtime-state-first 断言生效。

- [ ] **Step 6: Commit**

```bash
git add services/python_grpc/src/server/runtime_recovery_context.py services/python_grpc/src/server/tests/test_runtime_recovery_context.py services/python_grpc/src/server/tests/test_recover_runtime_context.py
git commit -m "refactor: stop regenerating stage1 json during recovery"
```

### Task 2: 去掉 Python 主链路中的 Stage1 JSON 兜底写盘

**Files:**
- Modify: `services/python_grpc/src/server/grpc_service_impl.py`
- Create: `services/python_grpc/src/server/tests/test_runtime_state_first_phase2_flow.py`

- [ ] **Step 1: 写失败测试，锁定正常链路不再依赖 JSON**

创建 `services/python_grpc/src/server/tests/test_runtime_state_first_phase2_flow.py`，覆盖两类场景：

1. `AnalyzeSemanticUnits` 命中 runtime Stage1 payload 时，即使 `step2_json_path/step6_json_path/sentence_timestamps_path` 为空，也继续执行。
2. `AssembleRichText` 命中 runtime Stage1 payload 时，不再尝试补 `intermediates/sentence_timestamps.json`。

```python
assert request_snapshot["step2_json_path"] == ""
assert request_snapshot["step6_json_path"] == ""
assert request_snapshot["sentence_timestamps_path"] == ""
assert runtime_payload_used is True
```

- [ ] **Step 2: 运行测试，确认先红**

Run:

```bash
pytest services/python_grpc/src/server/tests/test_runtime_state_first_phase2_flow.py -q
```

Expected: 当前逻辑仍会补 path / 试图找文件，测试失败。

- [ ] **Step 3: 最小实现 `grpc_service_impl.py`**

收口以下分支：

- `AnalyzeSemanticUnits`
- `AssembleRichText`
- 与 `sentence_timestamps_path` 补齐有关的 fallback 逻辑

实现要求：

- 一旦 runtime payload 可用，就不再生成或补齐三类 Stage1 JSON path
- 对下游 `RichTextPipeline` 只传内存对象
- 保留 legacy file 输入兼容，但不主动 materialize

```python
pipeline = pipeline_cls(
    video_path=video_path,
    step2_path="" if runtime_step2_subtitles else step2_json_path,
    step6_path="" if runtime_step6_paragraphs else step6_json_path,
    sentence_timestamps_path="" if runtime_sentence_timestamps else sentence_timestamps_path,
    step2_subtitles=runtime_step2_subtitles,
    step6_paragraphs=runtime_step6_paragraphs,
    sentence_timestamps=runtime_sentence_timestamps,
)
```

并删除“为了统一读取而先写回 JSON”的主链路代码。

- [ ] **Step 4: 运行新测试和相关回归**

Run:

```bash
pytest services/python_grpc/src/server/tests/test_runtime_state_first_phase2_flow.py services/python_grpc/src/server/tests/test_phase2a_runtime_cache.py -q
```

Expected: 通过，正常链路只依赖 runtime payload。

- [ ] **Step 5: Commit**

```bash
git add services/python_grpc/src/server/grpc_service_impl.py services/python_grpc/src/server/tests/test_runtime_state_first_phase2_flow.py
git commit -m "refactor: make phase2 python flow runtime-state-first"
```

### Task 3: 将 SubtitleRepository / Phase2B 组装明确收口到内存态主路径

**Files:**
- Modify: `services/python_grpc/src/content_pipeline/shared/subtitle/subtitle_repository.py`
- Modify: `services/python_grpc/src/content_pipeline/phase2b/assembly/rich_text_pipeline.py`
- Modify: `services/python_grpc/src/content_pipeline/tests/test_subtitle_repository.py`
- Modify: `services/python_grpc/src/content_pipeline/tests/test_rich_text_pipeline_asset_naming.py`
- Modify: `services/python_grpc/src/content_pipeline/tests/test_phase2b_material_resilience.py`

- [ ] **Step 1: 写失败测试，锁定“内存态优先、文件仅兼容”**

新增或改造测试，覆盖：

1. 已传 `step2_subtitles` 时，不再要求 `step2_path` 存在。
2. 已传 `step6_paragraphs` 时，不再要求 `step6_path` 存在。
3. 已传 `sentence_timestamps` 时，`build_sentence_timestamps()` 直接返回内存值。
4. 仅在 legacy JSON-only 场景下，仍能从文件导入。

```python
repo.set_raw_subtitles([...], clear_sentence_timestamps=False)
repo.set_raw_paragraphs([...])
repo.set_raw_sentence_timestamps({"S001": {"start_sec": 0.0, "end_sec": 1.0}})

assert repo.load_step2_subtitles() == [...]
assert repo.load_step6_paragraphs() == [...]
assert repo.build_sentence_timestamps(prefer_external=True)["S001"]["start_sec"] == 0.0
```

- [ ] **Step 2: 运行测试，确认先红**

Run:

```bash
pytest services/python_grpc/src/content_pipeline/tests/test_subtitle_repository.py services/python_grpc/src/content_pipeline/tests/test_rich_text_pipeline_asset_naming.py -q -k "runtime or memory or sentence"
```

Expected: 至少一个场景仍暴露文件优先或 path 语义。

- [ ] **Step 3: 最小实现**

实现要求：

- `SubtitleRepository` 中 `_subtitles / _paragraphs / _sentence_timestamps` 一旦已注入，优先级高于文件路径
- `RichTextPipeline` 初始化后不再把 `sentence_timestamps_path` 当成主状态来源
- 文件读取逻辑保留，但只作为 compatibility adapter

```python
if self._sentence_timestamps is not None:
    return self._sentence_timestamps
if prefer_external and self.sentence_timestamps_path and Path(self.sentence_timestamps_path).exists():
    ...
```

- [ ] **Step 4: 运行 Phase2B 相关回归**

Run:

```bash
pytest services/python_grpc/src/content_pipeline/tests/test_subtitle_repository.py services/python_grpc/src/content_pipeline/tests/test_rich_text_pipeline_asset_naming.py services/python_grpc/src/content_pipeline/tests/test_phase2b_material_resilience.py -q
```

Expected: 通过，且运行时输入不再隐式依赖中间 JSON。

- [ ] **Step 5: Commit**

```bash
git add services/python_grpc/src/content_pipeline/shared/subtitle/subtitle_repository.py services/python_grpc/src/content_pipeline/phase2b/assembly/rich_text_pipeline.py services/python_grpc/src/content_pipeline/tests/test_subtitle_repository.py services/python_grpc/src/content_pipeline/tests/test_rich_text_pipeline_asset_naming.py services/python_grpc/src/content_pipeline/tests/test_phase2b_material_resilience.py
git commit -m "refactor: make subtitle repository prefer runtime payloads"
```

### Task 4: 对齐 Java 恢复消费逻辑，允许 Stage1 path 全空

**Files:**
- Modify: `services/java-orchestrator/src/main/java/com/mvp/module2/fusion/worker/TaskProcessingWorker.java`
- Modify: `services/java-orchestrator/src/main/java/com/mvp/module2/fusion/service/VideoProcessingOrchestrator.java`
- Modify: `services/java-orchestrator/src/main/java/com/mvp/module2/fusion/grpc/PythonGrpcClient.java`
- Modify: `services/java-orchestrator/src/test/java/com/mvp/module2/fusion/service/VideoProcessingOrchestratorPhase2RecoveryTest.java`
- Modify: `services/java-orchestrator/src/test/java/com/mvp/module2/fusion/worker/TaskProcessingWorkerRecoveryStatusTest.java`

- [ ] **Step 1: 写失败测试，锁定“stage1Ready=true 但 path 全空”仍可恢复**

在 Java 测试中新增场景：

- `RecoverRuntimeContextResult.stage1Ready = true`
- `step2JsonPath / step6JsonPath / sentenceTimestampsPath = ""`

预期：

- `reconcileRecoveredPhase2Context(...)` 后 `ioResult.stage1Result.success == true`
- 不因为 path 为空而阻断 Phase2A / Phase2B 调用

```java
assertNotNull(ioResult.stage1Result);
assertTrue(ioResult.stage1Result.success);
assertEquals("", ioResult.stage1Result.step2JsonPath);
assertEquals("", ioResult.stage1Result.step6JsonPath);
assertEquals("", ioResult.stage1Result.sentenceTimestampsPath);
```

- [ ] **Step 2: 运行测试，确认先红**

Run:

```bash
mvn -f services/java-orchestrator/pom.xml -Dtest=VideoProcessingOrchestratorPhase2RecoveryTest,TaskProcessingWorkerRecoveryStatusTest test -q
```

Expected: 现有逻辑至少有一处仍把 path 非空当成创建 `stage1Result` 的前提。

- [ ] **Step 3: 最小实现**

实现要求：

- `TaskProcessingWorker.buildRecoveredIoPhaseResult(...)` 不能只在 path 非空时创建 `Stage1Result`
- `VideoProcessingOrchestrator.reconcileRecoveredPhase2Context(...)` 保持 `stage1Ready` 为主判断
- `PythonGrpcClient` 保留 path 字段，但本轮不新增 proto 字段

```java
if (resumeDecision != null && resumeDecision.findBoolean("stage1_ready")) {
    PythonGrpcClient.Stage1Result stage1Result = new PythonGrpcClient.Stage1Result();
    stage1Result.success = true;
    stage1Result.step2JsonPath = step2JsonPath;
    stage1Result.step6JsonPath = step6JsonPath;
    stage1Result.sentenceTimestampsPath = sentenceTimestampsPath;
    ioResult.stage1Result = stage1Result;
}
```

- [ ] **Step 4: 运行 Java 恢复回归**

Run:

```bash
mvn -f services/java-orchestrator/pom.xml -Dtest=VideoProcessingOrchestratorPhase2RecoveryTest,TaskProcessingWorkerRecoveryStatusTest test -q
```

Expected: 通过，Java 侧接受空 path 的 Stage1 恢复结果。

- [ ] **Step 5: Commit**

```bash
git add services/java-orchestrator/src/main/java/com/mvp/module2/fusion/worker/TaskProcessingWorker.java services/java-orchestrator/src/main/java/com/mvp/module2/fusion/service/VideoProcessingOrchestrator.java services/java-orchestrator/src/main/java/com/mvp/module2/fusion/grpc/PythonGrpcClient.java services/java-orchestrator/src/test/java/com/mvp/module2/fusion/service/VideoProcessingOrchestratorPhase2RecoveryTest.java services/java-orchestrator/src/test/java/com/mvp/module2/fusion/worker/TaskProcessingWorkerRecoveryStatusTest.java
git commit -m "refactor: allow stage1 recovery without intermediate json paths"
```

### Task 5: 补齐历史兼容与文档收口

**Files:**
- Modify: `services/python_grpc/src/server/tests/test_phase2a_reuse_candidates.py`
- Modify: `docs/architecture/overview.md`
- Modify: `docs/architecture/upgrade-log.md`
- Modify: `docs/architecture/error-fixes.md`（若实现过程中修复到具体 bug）

- [ ] **Step 1: 写兼容测试**

在 `test_phase2a_reuse_candidates.py` 中保留一个 legacy JSON-only 场景，验证：

- 历史任务只有 `step2/step6/sentence_timestamps` 文件时仍可导入
- 导入后不会反向要求新任务继续生成这些文件

- [ ] **Step 2: 运行兼容测试，确认先红或至少覆盖新分支**

Run:

```bash
pytest services/python_grpc/src/server/tests/test_phase2a_reuse_candidates.py -q
```

Expected: 新旧路径切换有测试覆盖。

- [ ] **Step 3: 更新架构文档**

在以下文档中记录本次架构演进：

- `docs/architecture/overview.md`
- `docs/architecture/upgrade-log.md`

要明确写清：

- Stage1 runtime payload 是 canonical downstream transport
- legacy JSON 仅用于兼容导入 / 显式导出
- 恢复链路恢复的是结构化状态，不再回灌临时 JSON

- [ ] **Step 4: 运行最终验证**

Run:

```bash
pytest services/python_grpc/src/server/tests/test_runtime_recovery_context.py services/python_grpc/src/server/tests/test_recover_runtime_context.py services/python_grpc/src/server/tests/test_runtime_state_first_phase2_flow.py services/python_grpc/src/content_pipeline/tests/test_subtitle_repository.py services/python_grpc/src/content_pipeline/tests/test_rich_text_pipeline_asset_naming.py services/python_grpc/src/content_pipeline/tests/test_phase2b_material_resilience.py services/python_grpc/src/server/tests/test_phase2a_reuse_candidates.py -q
```

Run:

```bash
mvn -f services/java-orchestrator/pom.xml -Dtest=VideoProcessingOrchestratorPhase2RecoveryTest,TaskProcessingWorkerRecoveryStatusTest test -q
```

Run:

```bash
mvn -f services/java-orchestrator/pom.xml -DskipTests compile -q
```

Run:

```bash
python -X utf8 tools/architecture/check_docs_encoding.py
```

Expected: 全部通过。

- [ ] **Step 5: Commit**

```bash
git add services/python_grpc/src/server/tests/test_phase2a_reuse_candidates.py docs/architecture/overview.md docs/architecture/upgrade-log.md docs/architecture/error-fixes.md
git commit -m "docs: record runtime-state-first phase2 architecture"
```

## Notes

- 本轮实现不主动扩展 `contracts/proto/video_processing.proto`。
- 现有 `step2_json_path / step6_json_path / sentence_timestamps_path` 字段继续保留，但语义降级为：
  - 历史兼容输入
  - 显式导出输出
  - 默认可空
- 如果 Task 4 结束后仍发现 Java 侧存在 path 硬依赖，再单独补一轮 proto / DTO 增强计划，而不是在本轮提前扩大改动面。
