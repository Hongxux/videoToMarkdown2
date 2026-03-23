# Terminal Reconcile Notification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 Web/PWA 在终态对账把任务收敛到 `COMPLETED/FAILED` 后，补发此前因离线窗口丢失的系统通知。

**Architecture:** 继续复用 `/api/mobile/tasks/reconcile-terminal` 作为终态真相源，`reconcileTerminalTaskSnapshots(...)` 在合并终态快照后，对本次收敛到终态的任务调用现有 `maybeShowTaskTerminalNotification(...)`。幂等、权限、前后台判断与通知点击跳转全部保持现有逻辑，不新增后端接口或第二套通知系统。

**Tech Stack:** `services/java-orchestrator/src/main/resources/static/index.html`, Node built-in test runner, `node --check`, Maven, docs encoding checker.

---

### Task 1: 锁定终态对账后的通知补偿行为

**Files:**
- Create: `tests/frontend/test_task_terminal_notification_reconcile.js`
- Test: `tests/frontend/test_task_terminal_notification_reconcile.js`

- [ ] **Step 1: Write the failing test**

新增前端回归测试，至少覆盖：

- 对账把任务从 `PROCESSING` 收敛到 `COMPLETED` 时会补发通知
- 对账把任务从 `PROCESSING` 收敛到 `FAILED` 时会补发通知
- `taskNotificationLedger` 已记录相同终态时不重复通知
- 页面可见且有焦点时不补发通知

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test tests/frontend/test_task_terminal_notification_reconcile.js`
Expected: FAIL，当前 `reconcileTerminalTaskSnapshots(...)` 不会触发任何通知补偿。

- [ ] **Step 3: Write minimal implementation**

在 `reconcileTerminalTaskSnapshots(...)` 中：

- 保存对账前任务快照
- 合并 `/tasks/reconcile-terminal` 返回的终态任务
- 对每个本次返回的终态任务，比较旧状态与新状态
- 命中 `非终态/未知 -> COMPLETED/FAILED` 时复用 `maybeShowTaskTerminalNotification(...)`

- [ ] **Step 4: Run test to verify it passes**

Run: `node --test tests/frontend/test_task_terminal_notification_reconcile.js`
Expected: PASS

### Task 2: 保证对账补偿与现有渲染链共存

**Files:**
- Modify: `services/java-orchestrator/src/main/resources/static/index.html`
- Test: `tests/frontend/test_task_list_refresh_policy.js`
- Test: `tests/frontend/test_task_terminal_notification_reconcile.js`

- [ ] **Step 1: Re-run existing task refresh regression**

Run: `node --test tests/frontend/test_task_list_refresh_policy.js`
Expected: PASS，原有“阅读返回列表触发对账”行为不回退。

- [ ] **Step 2: Validate frontend syntax**

Run: `node --check services/java-orchestrator/src/main/resources/static/index.html`
Expected: PASS

- [ ] **Step 3: Verify both frontend regressions together**

Run: `node --test tests/frontend/test_task_list_refresh_policy.js tests/frontend/test_task_terminal_notification_reconcile.js`
Expected: PASS

### Task 3: 沉淀错误修复并做仓库级校验

**Files:**
- Modify: `docs/architecture/error-fixes.md`

- [ ] **Step 1: Record symptom/root cause/fix/prevention**

在 `docs/architecture/error-fixes.md` 记录：

- 现象：终态对账已收敛状态，但完成/失败通知未补偿
- 根因：通知副作用只挂在 `taskTerminalEvent` 实时分支，没有复用对账分支
- 修复：`reconcileTerminalTaskSnapshots(...)` 合并终态后复用 `maybeShowTaskTerminalNotification(...)`
- 预防：保留前端回归测试，覆盖 `COMPLETED/FAILED`、ledger 去重和前台抑制

- [ ] **Step 2: Run verification**

Run: `node --test tests/frontend/test_task_list_refresh_policy.js tests/frontend/test_task_terminal_notification_reconcile.js`
Expected: PASS

Run: `mvn -f services/java-orchestrator/pom.xml -DskipTests compile -q`
Expected: PASS

Run: `python -X utf8 tools/architecture/check_docs_encoding.py`
Expected: PASS
