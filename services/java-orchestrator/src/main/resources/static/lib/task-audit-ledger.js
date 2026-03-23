(function (global, factory) {
    const api = factory();
    if (typeof module === 'object' && module && module.exports) {
        module.exports = api;
    }
    if (global && typeof global === 'object') {
        global.TaskAuditLedger = api;
    }
})(typeof globalThis !== 'undefined' ? globalThis : this, function () {
    function toText(value) {
        if (value === null || value === undefined) {
            return '';
        }
        return String(value).trim();
    }

    function escapeHtml(value) {
        return toText(value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    function normalizeObject(value) {
        return value && typeof value === 'object' && !Array.isArray(value) ? value : {};
    }

    function stageLabel(rawStage) {
        const stage = toText(rawStage).toLowerCase();
        if (!stage) {
            return '未知阶段';
        }
        if (stage === 'phase2a') {
            return 'Phase2A';
        }
        if (stage === 'phase2b') {
            return 'Phase2B';
        }
        if (stage === 'stage1') {
            return 'Stage1';
        }
        if (stage === 'transcribe') {
            return '转写';
        }
        return toText(rawStage);
    }

    function formatTimeText(record) {
        const timestampMs = Number(record.updated_at_ms || record.recorded_at_ms || 0);
        if (Number.isFinite(timestampMs) && timestampMs > 0) {
            const date = new Date(timestampMs);
            if (!Number.isNaN(date.getTime())) {
                return date.toLocaleString('zh-CN', { hour12: false });
            }
        }
        const iso = toText(record.timestamp || record.recorded_at || record.lastModifiedAt);
        if (!iso) {
            return '';
        }
        const date = new Date(iso);
        if (!Number.isNaN(date.getTime())) {
            return date.toLocaleString('zh-CN', { hour12: false });
        }
        return iso;
    }

    function sortTimestampMs(record, lineIndex) {
        const timestampMs = Number(record.updated_at_ms || record.recorded_at_ms || 0);
        if (Number.isFinite(timestampMs) && timestampMs > 0) {
            return timestampMs;
        }
        const iso = Date.parse(toText(record.timestamp || record.recorded_at || ''));
        if (Number.isFinite(iso) && iso > 0) {
            return iso;
        }
        return lineIndex + 1;
    }

    function extractRawErrorText(record) {
        const fallback = normalizeObject(record.fallback);
        const extra = normalizeObject(record.extra);
        return toText(
            record.error_message
            || fallback.fallback_reason
            || record.message
            || record.readError
            || extra.error_message
            || record.reason
        );
    }

    function extractStackTrace(record) {
        const extra = normalizeObject(record.extra);
        return toText(record.stack_trace || extra.stack_trace);
    }

    function deriveScopeLabel(record) {
        if (toText(record.llm_call_id)) {
            return `LLM 调用 ${toText(record.llm_call_id)}`;
        }
        if (toText(record.chunk_id)) {
            return `分块 ${toText(record.chunk_id)}`;
        }
        if (toText(record.unit_id)) {
            return `语义单元 ${toText(record.unit_id)}`;
        }
        if (toText(record.scope_ref)) {
            return `作用域 ${toText(record.scope_ref)}`;
        }
        return '';
    }

    function deriveStatusTone(ledgerKey, record) {
        if (record.__parseError) {
            return 'warning';
        }
        if (ledgerKey === 'error') {
            return 'danger';
        }
        return 'warning';
    }

    function deriveStatusLabel(ledgerKey, record) {
        if (record.__parseError) {
            return '解析失败';
        }
        if (ledgerKey === 'error') {
            return '失败';
        }
        if (ledgerKey === 'manual_retry_required') {
            return '需人工处理';
        }
        return '已降级';
    }

    function deriveActionText(record) {
        const fallback = normalizeObject(record.fallback);
        return toText(
            record.action_hint
            || record.operator_action
            || record.required_action
            || record.retry_strategy
            || fallback.fallback_kind
        );
    }

    function deriveTitle(ledgerKey, record, lineNumber) {
        if (record.__parseError) {
            return `第 ${lineNumber} 条记录无法解析`;
        }
        const fallback = normalizeObject(record.fallback);
        const stage = stageLabel(record.stage || fallback.repair_stage);
        const recordType = toText(record.record_type).toLowerCase();
        const stepName = toText(record.step_name);
        const fallbackKind = toText(fallback.fallback_kind).toLowerCase();
        if (ledgerKey === 'fallback') {
            if (fallbackKind === 'vl_analysis_failed') {
                return `${stage} ${stepName ? `${stepName} ` : ''}失败，已触发降级`;
            }
            if (stepName) {
                return `${stage} ${stepName} 触发降级`;
            }
            return `${stage} 触发降级`;
        }
        if (recordType === 'llm_attempt_error') {
            return `${stage} LLM 调用失败`;
        }
        if (recordType === 'chunk_error') {
            return `${stage} 分块处理失败`;
        }
        if (recordType === 'stage_error') {
            return `${stage} 阶段执行失败`;
        }
        if (recordType === 'llm_attempt_manual_retry_required') {
            return `${stage} LLM 调用失败，需要人工重试`;
        }
        if (recordType === 'chunk_manual_retry_required') {
            return `${stage} 分块失败，需要人工重试`;
        }
        if (recordType === 'stage_manual_retry_required') {
            return `${stage} 阶段失败，需要人工重试`;
        }
        if (ledgerKey === 'manual_retry_required') {
            return `${stage} 需要人工重试`;
        }
        if (ledgerKey === 'error') {
            return `${stage} 发生错误`;
        }
        return `${stage} 审计记录`;
    }

    function buildMetaItems(ledgerKey, record) {
        const items = [];
        const fallback = normalizeObject(record.fallback);
        const stage = stageLabel(record.stage || fallback.repair_stage);
        if (stage) {
            items.push(stage);
        }
        if (toText(record.record_type)) {
            items.push(`类型 ${toText(record.record_type)}`);
        }
        if (toText(record.step_name)) {
            items.push(`步骤 ${toText(record.step_name)}`);
        }
        if (toText(record.status)) {
            items.push(`状态 ${toText(record.status)}`);
        }
        const scopeLabel = deriveScopeLabel(record);
        if (scopeLabel) {
            items.push(scopeLabel);
        }
        if (ledgerKey === 'fallback' && toText(fallback.fallback_kind)) {
            items.push(`降级 ${toText(fallback.fallback_kind)}`);
        }
        return items;
    }

    function prettyJsonText(record, rawJsonText) {
        if (record.__parseError) {
            return rawJsonText;
        }
        try {
            return JSON.stringify(record, null, 2);
        } catch (_error) {
            return rawJsonText;
        }
    }

    function buildCardModel(ledgerKey, rawLine, parsedRecord, lineIndex) {
        const record = parsedRecord || { __parseError: true, raw_line: rawLine };
        return {
            lineNumber: lineIndex + 1,
            title: deriveTitle(ledgerKey, record, lineIndex + 1),
            statusTone: deriveStatusTone(ledgerKey, record),
            statusLabel: deriveStatusLabel(ledgerKey, record),
            scopeLabel: deriveScopeLabel(record),
            rawErrorText: extractRawErrorText(record),
            stackTrace: extractStackTrace(record),
            actionText: deriveActionText(record),
            timeText: formatTimeText(record),
            sortMs: sortTimestampMs(record, lineIndex),
            metaItems: buildMetaItems(ledgerKey, record),
            rawJsonText: prettyJsonText(record, rawLine),
        };
    }

    function buildTaskAuditLedgerCards(options) {
        const ledgerKey = toText(options && options.ledgerKey) || 'fallback';
        const rawText = toText(options && options.rawText);
        if (!rawText) {
            return [];
        }
        const cards = rawText
            .split(/\r?\n/)
            .map((line) => String(line || ''))
            .filter((line) => line.trim())
            .map((line, lineIndex) => {
                let parsedRecord = null;
                try {
                    parsedRecord = JSON.parse(line);
                } catch (_error) {
                    parsedRecord = null;
                }
                return buildCardModel(ledgerKey, line, parsedRecord, lineIndex);
            });
        cards.sort((left, right) => {
            if (right.sortMs !== left.sortMs) {
                return right.sortMs - left.sortMs;
            }
            return right.lineNumber - left.lineNumber;
        });
        return cards;
    }

    function renderMetaItems(metaItems) {
        if (!Array.isArray(metaItems) || !metaItems.length) {
            return '';
        }
        return `<div class="audit-ledger-record-meta">${metaItems.map((item) => (
            `<span class="audit-ledger-record-chip">${escapeHtml(item)}</span>`
        )).join('')}</div>`;
    }

    function renderTaskAuditLedgerCardsHtml(options) {
        const emptyText = toText(options && options.emptyText) || '当前账本暂无记录';
        const cards = buildTaskAuditLedgerCards(options);
        if (!cards.length) {
            return `<div class="audit-ledger-record-empty">${escapeHtml(emptyText)}</div>`;
        }
        return `<div class="audit-ledger-record-list">${cards.map((card) => {
            const rawErrorBlock = card.rawErrorText
                ? `
                    <section class="audit-ledger-record-section audit-ledger-record-section-error">
                        <div class="audit-ledger-record-section-title">原始错误信息</div>
                        <pre class="audit-ledger-record-error">${escapeHtml(card.rawErrorText)}</pre>
                    </section>
                `
                : '';
            const stackTraceBlock = card.stackTrace
                ? `
                    <details class="audit-ledger-record-disclosure">
                        <summary>堆栈</summary>
                        <pre class="audit-ledger-record-json">${escapeHtml(card.stackTrace)}</pre>
                    </details>
                `
                : '';
            const actionBlock = card.actionText
                ? `
                    <div class="audit-ledger-record-action">
                        <span class="audit-ledger-record-action-label">建议动作</span>
                        <span class="audit-ledger-record-action-text">${escapeHtml(card.actionText)}</span>
                    </div>
                `
                : '';
            const timeBlock = card.timeText
                ? `<div class="audit-ledger-record-time">${escapeHtml(card.timeText)}</div>`
                : '';
            return `
                <article class="audit-ledger-record-card" data-tone="${escapeHtml(card.statusTone)}">
                    <div class="audit-ledger-record-head">
                        <div class="audit-ledger-record-head-main">
                            <span class="audit-ledger-record-badge" data-tone="${escapeHtml(card.statusTone)}">${escapeHtml(card.statusLabel)}</span>
                            <div class="audit-ledger-record-title">${escapeHtml(card.title)}</div>
                        </div>
                        ${timeBlock}
                    </div>
                    ${renderMetaItems(card.metaItems)}
                    ${actionBlock}
                    ${rawErrorBlock}
                    ${stackTraceBlock}
                    <details class="audit-ledger-record-disclosure">
                        <summary>展开原始 JSON</summary>
                        <pre class="audit-ledger-record-json">${escapeHtml(card.rawJsonText)}</pre>
                    </details>
                </article>
            `;
        }).join('')}</div>`;
    }

    return {
        buildTaskAuditLedgerCards: buildTaskAuditLedgerCards,
        renderTaskAuditLedgerCardsHtml: renderTaskAuditLedgerCardsHtml,
    };
});