const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

function extractFunctionSource(source, functionName) {
    const marker = `function ${functionName}`;
    const functionStart = source.indexOf(marker);
    if (functionStart < 0) {
        throw new Error(`missing function: ${functionName}`);
    }
    const asyncPrefix = 'async ';
    const start = source.slice(Math.max(0, functionStart - asyncPrefix.length), functionStart) === asyncPrefix
        ? functionStart - asyncPrefix.length
        : functionStart;
    let braceStart = -1;
    let parenDepth = 0;
    let signatureStarted = false;
    for (let index = start; index < source.length; index += 1) {
        const current = source[index];
        if (current === '(') {
            parenDepth += 1;
            signatureStarted = true;
            continue;
        }
        if (current === ')') {
            parenDepth = Math.max(0, parenDepth - 1);
            continue;
        }
        if (current === '{' && signatureStarted && parenDepth === 0) {
            braceStart = index;
            break;
        }
    }
    if (braceStart < 0) {
        throw new Error(`missing body for function: ${functionName}`);
    }
    let depth = 0;
    let inSingle = false;
    let inDouble = false;
    let inTemplate = false;
    let inLineComment = false;
    let inBlockComment = false;
    for (let index = braceStart; index < source.length; index += 1) {
        const current = source[index];
        const next = source[index + 1];
        const previous = source[index - 1];
        if (inLineComment) {
            if (current === '\n') {
                inLineComment = false;
            }
            continue;
        }
        if (inBlockComment) {
            if (previous === '*' && current === '/') {
                inBlockComment = false;
            }
            continue;
        }
        if (!inSingle && !inDouble && !inTemplate) {
            if (current === '/' && next === '/') {
                inLineComment = true;
                index += 1;
                continue;
            }
            if (current === '/' && next === '*') {
                inBlockComment = true;
                index += 1;
                continue;
            }
        }
        if (!inDouble && !inTemplate && current === '\'' && previous !== '\\') {
            inSingle = !inSingle;
            continue;
        }
        if (!inSingle && !inTemplate && current === '"' && previous !== '\\') {
            inDouble = !inDouble;
            continue;
        }
        if (!inSingle && !inDouble && current === '`' && previous !== '\\') {
            inTemplate = !inTemplate;
            continue;
        }
        if (inSingle || inDouble || inTemplate) {
            continue;
        }
        if (current === '{') {
            depth += 1;
            continue;
        }
        if (current === '}') {
            depth -= 1;
            if (depth === 0) {
                return source.slice(start, index + 1);
            }
        }
    }
    throw new Error(`unterminated function: ${functionName}`);
}

function loadReconcileTerminalTaskSnapshotsSource() {
    const filePath = path.resolve(__dirname, '../../services/java-orchestrator/src/main/resources/static/index.html');
    const html = fs.readFileSync(filePath, 'utf8');
    return extractFunctionSource(html, 'reconcileTerminalTaskSnapshots');
}

function buildHarness(options = {}) {
    const notificationCalls = [];
    const initialTasks = Array.isArray(options.initialTasks)
        ? options.initialTasks.map((task) => ({ ...task }))
        : [];
    const sandbox = {
        state: {
            taskTerminalReconcilePending: options.pending !== false,
            taskNotificationLedger: { ...(options.taskNotificationLedger || {}) },
            tasks: initialTasks,
            currentTaskId: '',
            currentTaskStorageKey: '',
        },
        collectTerminalReconcileCandidateTaskIds: () => (
            Array.isArray(options.candidateTaskIds) ? options.candidateTaskIds.slice() : ['task-1']
        ),
        fetchTerminalTaskSnapshots: async () => ({
            tasks: Array.isArray(options.reconciledTasks)
                ? options.reconciledTasks.map((task) => ({ ...task }))
                : [],
        }),
        mergeTaskRecordsIntoState(incomingTasks) {
            const byTaskId = new Map(
                (Array.isArray(sandbox.state.tasks) ? sandbox.state.tasks : [])
                    .map((task) => [String(task && task.taskId || '').trim(), { ...task }])
            );
            (Array.isArray(incomingTasks) ? incomingTasks : []).forEach((task) => {
                const taskId = String(task && task.taskId || '').trim();
                if (!taskId) {
                    return;
                }
                const previousTask = byTaskId.get(taskId) || {};
                byTaskId.set(taskId, {
                    ...previousTask,
                    ...task,
                });
            });
            sandbox.state.tasks = Array.from(byTaskId.values());
        },
        maybeShowTaskTerminalNotification: async (task, payload, previousStatus) => {
            notificationCalls.push({
                task,
                payload,
                previousStatus,
            });
            return true;
        },
        normalizeTaskId(taskId) {
            return String(taskId || '').trim();
        },
        findTaskById(taskId) {
            const normalizedTaskId = String(taskId || '').trim();
            return (Array.isArray(sandbox.state.tasks) ? sandbox.state.tasks : [])
                .find((task) => String(task && task.taskId || '').trim() === normalizedTaskId) || null;
        },
        applyTaskListMotionDiff() {},
        renderTaskList() {},
        syncCurrentTaskRealtimeStateAfterTaskReload: async () => {},
        setTaskSummary() {},
        console,
    };
    const source = `${loadReconcileTerminalTaskSnapshotsSource()}\nthis.reconcileTerminalTaskSnapshots = reconcileTerminalTaskSnapshots;`;
    vm.runInNewContext(source, sandbox, { filename: 'task-terminal-notification-reconcile.vm.js' });
    return {
        state: sandbox.state,
        reconcile: sandbox.reconcileTerminalTaskSnapshots,
        notificationCalls,
    };
}

test('reconcile terminal snapshots compensates completed notification', async () => {
    const harness = buildHarness({
        initialTasks: [
            {
                taskId: 'task-1',
                title: 'Example task',
                status: 'PROCESSING',
            },
        ],
        reconciledTasks: [
            {
                taskId: 'task-1',
                title: 'Example task',
                status: 'COMPLETED',
            },
        ],
    });

    await harness.reconcile({ force: true, render: false });

    assert.equal(harness.notificationCalls.length, 1);
    assert.equal(harness.notificationCalls[0].previousStatus, 'PROCESSING');
    assert.equal(harness.notificationCalls[0].payload.status, 'COMPLETED');
});

test('reconcile terminal snapshots compensates failed notification', async () => {
    const harness = buildHarness({
        initialTasks: [
            {
                taskId: 'task-2',
                title: 'Example task',
                status: 'PROCESSING',
            },
        ],
        candidateTaskIds: ['task-2'],
        reconciledTasks: [
            {
                taskId: 'task-2',
                title: 'Example task',
                status: 'FAILED',
            },
        ],
    });

    await harness.reconcile({ force: true, render: false });

    assert.equal(harness.notificationCalls.length, 1);
    assert.equal(harness.notificationCalls[0].previousStatus, 'PROCESSING');
    assert.equal(harness.notificationCalls[0].payload.status, 'FAILED');
});

test('reconcile terminal snapshots do not compensate when nothing reconciles', async () => {
    const harness = buildHarness({
        initialTasks: [
            {
                taskId: 'task-3',
                title: 'Example task',
                status: 'PROCESSING',
            },
        ],
        candidateTaskIds: ['task-3'],
        reconciledTasks: [],
    });

    await harness.reconcile({ force: true, render: false });

    assert.equal(harness.notificationCalls.length, 0);
});
