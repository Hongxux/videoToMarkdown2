(function (global) {
    const INDENT_UNIT = '    ';

    function clampOffset(offsetLike, maxLike) {
        const max = Math.max(0, Number(maxLike) || 0);
        const offset = Number(offsetLike);
        if (!Number.isFinite(offset)) {
            return 0;
        }
        return Math.max(0, Math.min(max, Math.floor(offset)));
    }

    function normalizeSelection(valueLike, startLike, endLike) {
        const value = String(valueLike || '');
        const max = value.length;
        const start = clampOffset(startLike, max);
        const endRaw = clampOffset(endLike, max);
        return {
            value,
            start: Math.min(start, endRaw),
            end: Math.max(start, endRaw),
        };
    }

    function consumeKeyEvent(event) {
        if (!event) {
            return false;
        }
        if (typeof event.preventDefault === 'function') {
            event.preventDefault();
        }
        if (typeof event.stopPropagation === 'function') {
            event.stopPropagation();
        }
        if (typeof event.stopImmediatePropagation === 'function') {
            event.stopImmediatePropagation();
        }
        return true;
    }

    function shiftLineIndent(lineLike, options = {}) {
        const source = String(lineLike || '');
        const outdent = !!options.outdent;
        const indentUnit = String(options.indentUnit || INDENT_UNIT);
        if (!outdent) {
            return {
                text: `${indentUnit}${source}`,
                added: indentUnit.length,
                removed: 0,
                delta: indentUnit.length,
            };
        }
        if (!source.startsWith(indentUnit)) {
            return {
                text: source,
                added: 0,
                removed: 0,
                delta: 0,
            };
        }
        return {
            text: source.slice(indentUnit.length),
            added: 0,
            removed: indentUnit.length,
            delta: -indentUnit.length,
        };
    }

    function resolveParagraphRangeByOffset(valueLike, offsetLike) {
        const value = String(valueLike || '');
        const max = value.length;
        if (!max) {
            return { start: 0, end: 0 };
        }
        const offset = clampOffset(offsetLike, max);
        const lineStart = value.lastIndexOf('\n', Math.max(0, offset - 1)) + 1;
        let lineEnd = value.indexOf('\n', offset);
        if (lineEnd < 0) {
            lineEnd = max;
        }
        const currentLine = value.slice(lineStart, lineEnd).replace(/\r$/, '');
        if (!currentLine.trim()) {
            return { start: lineStart, end: lineEnd };
        }
        let paragraphStart = lineStart;
        while (paragraphStart > 0) {
            const prevLineEnd = paragraphStart - 1;
            const prevLineStart = value.lastIndexOf('\n', Math.max(0, prevLineEnd - 1)) + 1;
            const prevLine = value.slice(prevLineStart, prevLineEnd).replace(/\r$/, '');
            if (!prevLine.trim()) {
                break;
            }
            paragraphStart = prevLineStart;
        }
        let paragraphEnd = lineEnd;
        while (paragraphEnd < max) {
            const nextLineStart = paragraphEnd + 1;
            let nextLineEnd = value.indexOf('\n', nextLineStart);
            if (nextLineEnd < 0) {
                nextLineEnd = max;
            }
            const nextLine = value.slice(nextLineStart, nextLineEnd).replace(/\r$/, '');
            if (!nextLine.trim()) {
                break;
            }
            paragraphEnd = nextLineEnd;
        }
        return {
            start: paragraphStart,
            end: paragraphEnd,
        };
    }

    function mapParagraphLines(textLike, mapper) {
        const lines = String(textLike || '').split('\n');
        if (typeof mapper !== 'function') {
            return lines.join('\n');
        }
        return lines
            .map((line, index) => mapper(String(line || ''), index, lines.length))
            .join('\n');
    }

    function normalizeMarkdownLineBody(lineLike) {
        const line = String(lineLike || '');
        const indentMatch = line.match(/^(\s*)/);
        const indent = indentMatch ? indentMatch[1] : '';
        const body = line
            .slice(indent.length)
            .replace(/^(?:#{1,6}\s+|[-*+]\s+|\d+\.\s+)/, '')
            .trimStart();
        return { indent, body };
    }

    function rewriteSelectedLines(args = {}, mapper) {
        if (typeof mapper !== 'function') {
            return null;
        }
        const selection = normalizeSelection(args.value, args.start, args.end);
        const { value, start, end } = selection;
        const blockStart = value.lastIndexOf('\n', Math.max(0, start - 1)) + 1;
        let blockEnd = value.indexOf('\n', end);
        if (blockEnd < 0) {
            blockEnd = value.length;
        }
        const segment = value.slice(blockStart, blockEnd);
        const lines = segment.split('\n');
        const nextSegment = lines.map((line, index) => mapper(String(line || ''), index, lines.length)).join('\n');
        return {
            value: `${value.slice(0, blockStart)}${nextSegment}${value.slice(blockEnd)}`,
            start: blockStart,
            end: blockStart + nextSegment.length,
        };
    }

    function applyParagraphIndentMutation(args = {}) {
        const selection = normalizeSelection(args.value, args.start, args.end);
        const { value, start } = selection;
        const outdent = !!args.outdent;
        const indentUnit = String(args.indentUnit || INDENT_UNIT);
        const range = resolveParagraphRangeByOffset(value, start);
        const paragraph = value.slice(range.start, range.end);
        const localCaret = Math.max(0, Math.min(paragraph.length, start - range.start));
        const lines = paragraph.split('\n');
        let consumed = 0;
        let caretLineIndex = 0;
        let caretColumn = 0;
        for (let index = 0; index < lines.length; index += 1) {
            const lineLength = String(lines[index] || '').length;
            const lineEnd = consumed + lineLength;
            if (localCaret <= lineEnd || index === lines.length - 1) {
                caretLineIndex = index;
                caretColumn = Math.max(0, Math.min(lineLength, localCaret - consumed));
                break;
            }
            consumed = lineEnd + 1;
        }
        const transformedLines = lines.map((line) => shiftLineIndent(line, { outdent, indentUnit }).text);
        let nextCaretLocal = 0;
        for (let index = 0; index < transformedLines.length; index += 1) {
            if (index < caretLineIndex) {
                nextCaretLocal += transformedLines[index].length + 1;
                continue;
            }
            const currentLine = String(lines[index] || '');
            let nextColumn = caretColumn;
            if (outdent) {
                if (currentLine.startsWith(indentUnit)) {
                    nextColumn = Math.max(0, caretColumn - indentUnit.length);
                }
            } else {
                nextColumn = caretColumn + indentUnit.length;
            }
            nextCaretLocal += Math.max(0, Math.min(transformedLines[index].length, nextColumn));
            break;
        }
        const nextParagraph = transformedLines.join('\n');
        const nextValue = `${value.slice(0, range.start)}${nextParagraph}${value.slice(range.end)}`;
        const nextCaret = range.start + nextCaretLocal;
        return {
            value: nextValue,
            start: nextCaret,
            end: nextCaret,
        };
    }

    function applyHeadingMutation(args = {}) {
        const level = Math.max(1, Math.min(6, Number(args.level) || 1));
        const marker = `${'#'.repeat(level)} `;
        return rewriteSelectedLines(args, (line) => {
            const normalized = normalizeMarkdownLineBody(line);
            return `${normalized.indent}${marker}${normalized.body}`;
        });
    }

    function bindCaptureKeydown(target, handler, options = {}) {
        if (!target || typeof target.addEventListener !== 'function' || typeof handler !== 'function') {
            return false;
        }
        const marker = String(options.marker || 'mobileShortcutBound').trim() || 'mobileShortcutBound';
        const storeKey = `__${marker}`;
        if (target[storeKey]) {
            return false;
        }
        target[storeKey] = true;
        target.addEventListener('keydown', handler, true);
        return true;
    }

    function normalizeKeyToken(tokenLike) {
        return String(tokenLike || '').trim().toLowerCase();
    }

    function normalizeComboSpec(comboLike) {
        const combo = String(comboLike || '').trim();
        if (!combo) {
            return null;
        }
        const tokens = combo.split('+').map((part) => normalizeKeyToken(part)).filter(Boolean);
        if (!tokens.length) {
            return null;
        }
        const keyToken = tokens[tokens.length - 1];
        return {
            combo,
            key: keyToken,
            ctrl: tokens.includes('ctrl') || tokens.includes('control'),
            meta: tokens.includes('meta') || tokens.includes('cmd') || tokens.includes('command'),
            shift: tokens.includes('shift'),
            alt: tokens.includes('alt') || tokens.includes('option'),
        };
    }

    function matchesKeyCombo(event, comboLike) {
        if (!event) {
            return false;
        }
        const spec = typeof comboLike === 'object' && comboLike && comboLike.key
            ? comboLike
            : normalizeComboSpec(comboLike);
        if (!spec) {
            return false;
        }
        const eventKey = normalizeKeyToken(event.key);
        if (eventKey !== spec.key) {
            return false;
        }
        return !!event.ctrlKey === !!spec.ctrl
            && !!event.metaKey === !!spec.meta
            && !!event.shiftKey === !!spec.shift
            && !!event.altKey === !!spec.alt;
    }

    function resolveKeymapEntries(keymapLike) {
        if (Array.isArray(keymapLike)) {
            return keymapLike;
        }
        return keymapLike ? [keymapLike] : [];
    }

    function runKeymap(event, keymapLike, context = {}) {
        const entries = resolveKeymapEntries(keymapLike);
        for (let index = 0; index < entries.length; index += 1) {
            const entry = entries[index] && typeof entries[index] === 'object' ? entries[index] : null;
            if (!entry) {
                continue;
            }
            const combos = Array.isArray(entry.combo) ? entry.combo : [entry.combo];
            const matched = combos.some((combo) => matchesKeyCombo(event, combo));
            if (!matched) {
                continue;
            }
            if (typeof entry.when === 'function' && !entry.when(context, event, entry)) {
                continue;
            }
            if (entry.consume !== false) {
                consumeKeyEvent(event);
            }
            if (typeof entry.run !== 'function') {
                return true;
            }
            const result = entry.run(context, event, entry);
            if (result !== false) {
                return true;
            }
        }
        return false;
    }

    function bindKeymap(target, keymapLike, options = {}) {
        if (!target || typeof target.addEventListener !== 'function') {
            return false;
        }
        const resolveContext = typeof options.resolveContext === 'function'
            ? options.resolveContext
            : () => (options.context && typeof options.context === 'object' ? options.context : {});
        return bindCaptureKeydown(target, (event) => {
            const context = resolveContext(event) || {};
            runKeymap(event, keymapLike, context);
        }, { marker: options.marker || 'mobileKeymapBound' });
    }

    global.MobileEditorShortcuts = Object.freeze({
        INDENT_UNIT,
        normalizeSelection,
        consumeKeyEvent,
        shiftLineIndent,
        resolveParagraphRangeByOffset,
        mapParagraphLines,
        normalizeMarkdownLineBody,
        rewriteSelectedLines,
        applyParagraphIndentMutation,
        applyHeadingMutation,
        bindCaptureKeydown,
        normalizeComboSpec,
        matchesKeyCombo,
        runKeymap,
        bindKeymap,
    });
})(window);
