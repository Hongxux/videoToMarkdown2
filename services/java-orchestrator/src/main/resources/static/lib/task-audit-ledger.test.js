import assert from 'node:assert/strict';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const {
    buildTaskAuditLedgerCards,
    renderTaskAuditLedgerCardsHtml,
} = require('./task-audit-ledger.js');

function testBuildTaskAuditLedgerCardsSortsNewestFirst() {
    const rawText = [
        JSON.stringify({
            schema_version: 'runtime_error_record_v1',
            record_type: 'chunk_error',
            updated_at_ms: 1000,
            stage: 'phase2a',
            chunk_id: 'chunk-old',
            error_message: 'old decode error',
            action_hint: 'retry old',
        }),
        JSON.stringify({
            schema_version: 'runtime_error_record_v1',
            record_type: 'llm_attempt_error',
            updated_at_ms: 2000,
            stage: 'phase2b',
            llm_call_id: 'llm-new',
            error_message: 'latest upstream timeout',
            action_hint: 'switch provider',
        }),
    ].join('\n');

    const cards = buildTaskAuditLedgerCards({ ledgerKey: 'error', rawText });
    assert.equal(cards.length, 2);
    assert.equal(cards[0].rawErrorText, 'latest upstream timeout');
    assert.equal(cards[0].scopeLabel, 'LLM 调用 llm-new');
    assert.equal(cards[1].rawErrorText, 'old decode error');
    assert.equal(cards[1].scopeLabel, '分块 chunk-old');
}

function testRenderTaskAuditLedgerCardsHtmlShowsRawErrorBlock() {
    const rawText = JSON.stringify({
        schema_version: 'llm_fallback_event_v1',
        timestamp: '2026-03-23T09:30:00',
        stage: 'phase2a',
        step_name: 'phase2a_vl',
        unit_id: 'SU100',
        fallback: {
            is_fallback: true,
            fallback_kind: 'vl_analysis_failed',
            fallback_reason: 'Qwen timeout while analyzing screenshots',
        },
        extra: {
            stack_trace: 'Traceback line 1\nTraceback line 2',
        },
    });

    const html = renderTaskAuditLedgerCardsHtml({ ledgerKey: 'fallback', rawText, emptyText: 'empty' });
    assert.match(html, /原始错误信息/);
    assert.match(html, /Qwen timeout while analyzing screenshots/);
    assert.match(html, /展开原始 JSON/);
    assert.match(html, /堆栈/);
}

function testBuildTaskAuditLedgerCardsKeepsInvalidJsonLines() {
    const cards = buildTaskAuditLedgerCards({
        ledgerKey: 'manual_retry_required',
        rawText: '{"record_type":"stage_manual_retry_required","updated_at_ms":3000,"error_message":"need manual retry"}\nnot-json-line',
    });

    assert.equal(cards.length, 2);
    assert.equal(cards[0].rawErrorText, 'need manual retry');
    assert.equal(cards[1].statusTone, 'warning');
    assert.match(cards[1].title, /无法解析/);
    assert.match(cards[1].rawJsonText, /not-json-line/);
}

testBuildTaskAuditLedgerCardsSortsNewestFirst();
testRenderTaskAuditLedgerCardsHtmlShowsRawErrorBlock();
testBuildTaskAuditLedgerCardsKeepsInvalidJsonLines();
console.log('task-audit-ledger tests passed');