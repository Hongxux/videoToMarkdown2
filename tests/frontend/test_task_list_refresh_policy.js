const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

function extractFunctionSource(source, functionName) {
    const marker = `function ${functionName}`;
    const start = source.indexOf(marker);
    if (start < 0) {
        throw new Error(`missing function: ${functionName}`);
    }
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

function loadCreateTaskListRefreshPolicy() {
    const filePath = path.resolve(__dirname, '../../services/java-orchestrator/src/main/resources/static/index.html');
    const html = fs.readFileSync(filePath, 'utf8');
    return extractFunctionSource(html, 'createTaskListRefreshPolicy');
}

function buildPolicyHarness(options = {}) {
    const refreshCalls = [];
    const timers = new Map();
    let nextTimerId = 1;
    const sandbox = {
        TASK_LIST_AUTO_REFRESH_HEALTHY_MS: 180000,
        TASK_LIST_AUTO_REFRESH_RECOVERY_MS: 25000,
        shouldAutoRefreshTaskState: options.shouldAutoRefreshTaskState || (() => false),
        isTaskUpdatesSocketHealthy: options.isTaskUpdatesSocketHealthy || (() => true),
        refreshTaskListIncrementally: async (args) => {
            refreshCalls.push(args);
            return true;
        },
        console,
        setTimeout(callback, delay) {
            const timerId = nextTimerId;
            nextTimerId += 1;
            timers.set(timerId, { callback, delay });
            return timerId;
        },
        clearTimeout(timerId) {
            timers.delete(timerId);
        },
    };
    const source = `${loadCreateTaskListRefreshPolicy()}\nthis.createTaskListRefreshPolicy = createTaskListRefreshPolicy;`;
    vm.runInNewContext(source, sandbox, { filename: 'task-list-refresh-policy.vm.js' });
    return {
        policy: sandbox.createTaskListRefreshPolicy(),
        refreshCalls,
        timers,
    };
}

async function testReadingReturnRefreshesEvenWithoutActiveTasks() {
    const harness = buildPolicyHarness({
        shouldAutoRefreshTaskState: () => false,
    });

    harness.policy.onViewChange('tasks', 'content');
    await Promise.resolve();

    assert.equal(harness.refreshCalls.length, 1);
    assert.equal(harness.refreshCalls[0].showLoading, false);
    assert.equal(harness.timers.size, 0);
}

async function testNonReadingPathsDoNotForceRefresh() {
    const harness = buildPolicyHarness({
        shouldAutoRefreshTaskState: () => false,
    });

    harness.policy.onViewChange('tasks', 'tasks');
    harness.policy.onViewChange('tasks', 'composer');
    await Promise.resolve();

    assert.equal(harness.refreshCalls.length, 0);
}

async function main() {
    const cases = [
        ['content 返回 tasks 时即使没有活跃任务也会触发一次对账', testReadingReturnRefreshesEvenWithoutActiveTasks],
        ['非阅读返回路径不会误触发强制对账', testNonReadingPathsDoNotForceRefresh],
    ];
    let failed = 0;
    for (const [name, run] of cases) {
        try {
            await run();
            process.stdout.write(`PASS ${name}\n`);
        } catch (error) {
            failed += 1;
            process.stderr.write(`FAIL ${name}\n`);
            process.stderr.write(`${error && error.stack ? error.stack : error}\n`);
        }
    }
    if (failed > 0) {
        process.exitCode = 1;
    }
}

main().catch((error) => {
    process.stderr.write(`${error && error.stack ? error.stack : error}\n`);
    process.exitCode = 1;
});
