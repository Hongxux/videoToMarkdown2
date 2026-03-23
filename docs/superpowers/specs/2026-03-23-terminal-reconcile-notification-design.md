# 终态对账通知补偿设计

## 背景

当前 Web/PWA 任务链路里，终态通知只绑定在 `taskTerminalEvent` 实时消息上：

- WebSocket 收到 `taskTerminalEvent`
- `mergeLiveTaskUpdateIntoState(...)` 合并状态
- `maybeShowTaskTerminalNotification(...)` 判断并弹系统通知

但终态对账链路 `reconcileTerminalTaskSnapshots(...)` 只会：

- 调用 `/api/mobile/tasks/reconcile-terminal`
- `mergeTaskRecordsIntoState(...)` 合并终态真相
- 渲染列表

这意味着一旦终态实时事件在离线窗口丢失，后续虽然能靠对账把任务状态收敛为 `COMPLETED/FAILED`，系统通知仍然不会补发。

## 目标

让 Web/PWA 在终态对账完成后，能够对 `COMPLETED` 和 `FAILED` 两类终态执行通知补偿。

补偿必须满足：

- 只在任务从非终态/未知收敛到终态时触发
- 继续复用现有 `maybeShowTaskTerminalNotification(...)`
- 继续复用 `taskNotificationLedger` 做幂等去重
- 继续沿用“页面当前可见且有焦点时不弹系统通知”的现有行为

## 非目标

- 不改 Android 通知链路
- 不新增后端通知补偿接口
- 不把通知副作用塞进底层通用 merge 函数
- 不引入新的持久化通知账本

## 现有杠杆

1. 真相源：`POST /api/mobile/tasks/reconcile-terminal`
2. 状态合并：`mergeTaskRecordsIntoState(...)`
3. 通知判定：`maybeShowTaskTerminalNotification(...)`
4. 幂等去重：`state.taskNotificationLedger`

## 设计决策

采用“对账后补副作用”的最小改动方案。

具体做法：

1. `reconcileTerminalTaskSnapshots(...)` 在对账前保存一份旧任务快照
2. 拉取 `/tasks/reconcile-terminal`
3. 先复用现有 `mergeTaskRecordsIntoState(...)` 合并终态真相
4. 再逐个比较本次对账返回任务的旧状态与新状态
5. 若命中 `非终态/未知 -> COMPLETED/FAILED`，调用 `maybeShowTaskTerminalNotification(...)`

这样做的原因：

- 不破坏现有状态合并层的纯度
- 不重复实现通知权限、前后台判断、点击跳转和 ledger 幂等
- 让 WebSocket 实时收敛与 REST 对账收敛共享同一套通知语义

## 数据流

### 改造前

`taskTerminalEvent -> mergeLiveTaskUpdateIntoState -> maybeShowTaskTerminalNotification`

`reconcileTerminalTaskSnapshots -> mergeTaskRecordsIntoState -> render`

### 改造后

`taskTerminalEvent -> mergeLiveTaskUpdateIntoState -> maybeShowTaskTerminalNotification`

`reconcileTerminalTaskSnapshots -> mergeTaskRecordsIntoState -> compare previousStatus/nextStatus -> maybeShowTaskTerminalNotification -> render`

## 幂等边界

补偿通知必须同时满足以下条件：

- `taskId` 非空
- 新状态是 `COMPLETED` 或 `FAILED`
- 旧状态不是同一终态
- `taskNotificationLedger[taskId]` 里还没有相同终态
- `Notification.permission === 'granted'`
- 页面不处于“可见且有焦点”的前台状态

## 测试策略

新增前端回归测试，直接抽取 `index.html` 内联函数进行验证：

1. 对账把任务从 `PROCESSING` 收敛到 `COMPLETED` 时，补发一次通知
2. 对账把任务从 `PROCESSING` 收敛到 `FAILED` 时，补发一次通知
3. 若 `taskNotificationLedger` 已记录该终态，不重复通知
4. 若页面当前可见且有焦点，不补发系统通知

## 影响文件

- `services/java-orchestrator/src/main/resources/static/index.html`
- `tests/frontend/test_task_terminal_notification_reconcile.js`
- `docs/architecture/error-fixes.md`

## 风险与约束

- 当前工作区已存在未提交改动，本次只触碰通知补偿相关区域，不能覆盖用户现有工作。
- `index.html` 是大文件，必须避免把通知副作用扩散进其他无关分支。
- 通知补偿只能建立在本次候选对账任务集合上，不能在全量刷新时对历史终态普发通知。
