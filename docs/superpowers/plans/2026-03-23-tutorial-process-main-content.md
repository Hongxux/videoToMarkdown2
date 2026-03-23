# Tutorial Process Main Content Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让教程型 `process` 在 Phase2B 中以步骤级 `main_content` 优先、`main_operation` 回退的方式进入统一结构化链路。

**Architecture:** 复用现有 `MarkdownEnhancer` 与 `Phase2bStructuredUnitPipeline` 主链路，不新增 tutorial 专用分支。通过补齐教程步骤 canonical 输入与移除短路逻辑，让教程型 `process` 与普通 `process/concrete` 在结构化阶段对齐。

**Tech Stack:** Python, pytest, MarkdownEnhancer, Phase2B structured unit pipeline

---

### Task 1: Write The Failing Regression Test

**Files:**
- Modify: `services/python_grpc/src/content_pipeline/tests/test_markdown_enhancer_rich_text.py`

- [ ] **Step 1: Write the failing test**

新增教程型 `process` 回归测试，断言：
- 会进入 `skill_pipeline`
- 输入渲染优先使用 `main_content`
- 缺失时回退 `main_operation`
- 最终渲染使用结构化输出

- [ ] **Step 2: Run test to verify it fails**

Run: `python -X utf8 -m pytest services/python_grpc/src/content_pipeline/tests/test_markdown_enhancer_rich_text.py -k tutorial_process_skill_pipeline_uses_main_content_priority_and_renders_structured_output -q`

Expected: FAIL，旧逻辑会卡在 tutorial passthrough，不会进入 pipeline。

### Task 2: Implement Tutorial Process Canonical Input

**Files:**
- Modify: `services/python_grpc/src/content_pipeline/markdown_enhancer.py`

- [ ] **Step 1: Extend tutorial step normalization**

在 `_load_tutorial_steps(...)` 中标准化 `main_content` 字段，并纳入 raw/manifest 合并。

- [ ] **Step 2: Build canonical tutorial process body**

新增或补齐 `process` canonical 输入解析，统一输出教程步骤骨架与正文来源优先级。

- [ ] **Step 3: Remove tutorial passthrough**

删除 `_process_one(...)` 中教程型 `process` 的提前返回，使其进入统一结构化主链路。

- [ ] **Step 4: Respect structured tutorial output when rendering**

调整 `_render_section(...)`，优先渲染结构化结果，仅在结构化结果为空时回退旧教程渲染。

### Task 3: Verify And Record Architecture Change

**Files:**
- Modify: `docs/architecture/upgrade-log.md`

- [ ] **Step 1: Run targeted verification**

Run:
- `python -X utf8 -m pytest services/python_grpc/src/content_pipeline/tests/test_markdown_enhancer_rich_text.py -k "tutorial_process_skill_pipeline_uses_main_content_priority_and_renders_structured_output or process_multistep_renders_ordered_steps_with_assets or tutorial_step_type_renders_note_warning_without_consuming_main_flow_index" -q`
- `python -X utf8 -m pytest services/python_grpc/src/content_pipeline/tests/test_phase2b_unit_pipeline.py -q`

- [ ] **Step 2: Run compile-level sanity check**

Run: `python -X utf8 -m py_compile services/python_grpc/src/content_pipeline/markdown_enhancer.py services/python_grpc/src/content_pipeline/phase2b/pipeline_service.py`

- [ ] **Step 3: Update architecture log**

在 `docs/architecture/upgrade-log.md` 记录教程型 `process` 从 passthrough 改为统一结构化输入链路的原因、决策与验证结果。
