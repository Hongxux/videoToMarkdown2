# Reading Return Task Reconcile Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让任务阅读页每次返回任务列表时都强制补一次列表对账，即使当前没有活跃任务。

**Architecture:** 复用现有 `taskListRefreshPolicy` 作为唯一任务列表对账入口，不新增独立轮询器或导航分支。改动集中在 `index.html` 的任务列表刷新策略；阅读返回列表时走一次无 loading 的强制增量对账，其余自动刷新边界保持不变。

**Tech Stack:** `services/java-orchestrator/src/main/resources/static/index.html`, Node built-in test runner, `node --check`, Maven, docs encoding checker.

---

### Task 1: 锁定“阅读返回列表”回归场景

**Files:**
- Create: `tests/frontend/test_task_list_refresh_policy.js`
- Test: `tests/frontend/test_task_list_refresh_policy.js`

- [ ] **Step 1: 写失败测试**

添加两个断言：
- `content -> tasks` 且 `shouldAutoRefreshTaskState()` 为 `false` 时，仍会触发一次 `refreshTaskListIncrementally(...)`
- 非阅读返回路径不会误触发这次强制对账

- [ ] **Step 2: 跑测试确认先红**

Run: `node --test tests/frontend/test_task_list_refresh_policy.js`
Expected: FAIL，显示 `content -> tasks` 未触发刷新。

- [ ] **Step 3: 提交**

```bash
git add tests/frontend/test_task_list_refresh_policy.js
git commit -m "test: cover reading return task reconcile"
```

### Task 2: 在统一刷新策略中补阅读返回列表对账

**Files:**
- Modify: `services/java-orchestrator/src/main/resources/static/index.html`
- Test: `tests/frontend/test_task_list_refresh_policy.js`

- [ ] **Step 1: 最小实现**

在 `createTaskListRefreshPolicy().onViewChange(...)` 中增加：
- 当 `nextView === 'tasks'`
- 且 `previousView === 'content' || previousView === 'outline'`
- 走一次 `refresh(...)`，参数为 `force: true` 与 `showLoading: false`

- [ ] **Step 2: 跑测试确认转绿**

Run: `node --test tests/frontend/test_task_list_refresh_policy.js`
Expected: PASS。

- [ ] **Step 3: 做前端语法校验**

Run:
- `node --check services/java-orchestrator/src/main/resources/static/lib/mobile-view-navigation.js`
- 抽取 `services/java-orchestrator/src/main/resources/static/index.html` 主内联脚本后执行 `node --check`

Expected: 全部通过。

- [ ] **Step 4: 提交**

```bash
git add services/java-orchestrator/src/main/resources/static/index.html tests/frontend/test_task_list_refresh_policy.js
git commit -m "fix: reconcile task list when leaving reader"
```

### Task 3: 补充错误修复沉淀并做仓库级验证

**Files:**
- Modify: `docs/architecture/error-fixes.md`

- [ ] **Step 1: 记录现象 / 根因 / 修复 / 预防**

补充“阅读返回任务列表未触发对账”的错误修复记录，明确调用链从 `mobile-view-navigation -> taskListRefreshPolicy.onViewChange -> refreshTaskListIncrementally`。

- [ ] **Step 2: 运行验证**

Run:
- `node --test tests/frontend/test_task_list_refresh_policy.js`
- `mvn -f services/java-orchestrator/pom.xml -DskipTests compile -q`
- `python -X utf8 tools/architecture/check_docs_encoding.py`

Expected: 全部通过。

- [ ] **Step 3: 提交**

```bash
git add docs/architecture/error-fixes.md tests/frontend/test_task_list_refresh_policy.js services/java-orchestrator/src/main/resources/static/index.html
git commit -m "docs: record reader return reconcile fix"
```
