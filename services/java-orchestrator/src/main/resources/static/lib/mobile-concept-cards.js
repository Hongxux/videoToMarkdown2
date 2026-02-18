(function (global) {
    'use strict';

    const BLOCK_SELECTOR = 'p, li, blockquote, h1, h2, h3, h4, h5, h6, pre';
    const SKIP_HIGHLIGHT_SELECTOR = 'code, pre, a, script, style, .katex, .card-fissure, .concept-term, .inline-sticky-note';
    const CARD_CLOSE_ANIMATION_MS = 380;
    const POST_SAVE_HIGHLIGHT_MS = 1200;
    const TEAR_SPLIT_RATIO = 0.46;
    const WHISPER_VISIBLE_CLASS = 'is-visible';
    const WIKILINK_TRIGGER_REGEX = /\[\[([^\]\n|]*)$/;
    const WIKILINK_SUGGEST_MAX_ITEMS = 8;
    const WIKILINK_KEYUP_SKIP_KEYS = new Set(['ArrowUp', 'ArrowDown', 'Enter', 'Tab', 'Escape']);
    const STORAGE_TITLE_ILLEGAL_CHARS = /[\\/:*?"<>|\u0000-\u001f]/g;
    const STORAGE_TITLE_TRAILING_DOTS_OR_SPACE = /[.\s]+$/;
    const STORAGE_TITLE_WINDOWS_RESERVED = /^(con|prn|aux|nul|com[1-9]|lpt[1-9])(?:\..*)?$/i;
    const SELECTION_TRIGGER_HIDE_DELAY_MS = 160;
    const SELECTION_SNIPPET_MAX_CHARS = 220;
    const SELECTION_TERM_MAX_CHARS = 120;
    const THOUGHT_CARD_TYPE = 'thought';
    const TEAR_OPEN_SWIPE_THRESHOLD_PX = 30;
    const TEAR_OPEN_SWIPE_MAX_DX_PX = 52;
    const TEAR_OPEN_SNAP_RATIO = 0.3;
    const TEAR_CLOSE_SNAP_RATIO = 0.3;
    const TEAR_HAPTIC_TAP_RATIO = 0.96;
    const TEAR_OPEN_FULL_PULL_PX = 104;
    const TEAR_CLOSE_FULL_PULL_PX = 92;
    const TEAR_REBOUND_MS = 280;
    const PAPER_FIBER_FILTER_ID = 'paper-fiber-distortion';
    const SEGMENTER_MAX_TOKENS = 4;
    const SEGMENTER_MAX_CHARS = 30;
    const SEGMENT_BOUNDARY_REGEX = /^[\s,.;:!?\'\"(){}\[\]<>|\/\\\u3000\u3001\u3002\uff01\uff1f\uff1b\uff1a\u201c\u201d\u2018\u2019\uff08\uff09\u3010\u3011\u300a\u300b\u3008\u3009\u3014\u3015\uff3b\uff3d\uff5b\uff5d]+$/;
    const SEGMENT_WORD_FALLBACK_REGEX = /[A-Za-z0-9_\-\u4e00-\u9fff]+/g;

    function createMobileConceptCards(options = {}) {
        const config = Object.assign({
            apiBase: `${global.location.origin}/api/mobile`,
            holdDelayMs: 380,
            maxHighlightTerms: 10000,
            maxTermsPerBucket: 0,
            highlightInitialBlockLimit: 36,
            highlightFrameBudgetMs: 8,
            highlightObserverRootMargin: '120% 0px 120% 0px',
            highlightCandidatesTopK: 1200,
            highlightCandidatesContextChars: 18000,
            highlightCandidatesEndpoint: `${global.location.origin}/api/mobile/cards/titles/candidates`,
            highlightWorkerUrl: '/lib/mobile-highlight-worker.js',
            highlightWorkerMinTerms: 1200,
            contextChars: 320,
            tearOpenSwipeThresholdPx: TEAR_OPEN_SWIPE_THRESHOLD_PX,
            tearOpenSwipeMaxDxPx: TEAR_OPEN_SWIPE_MAX_DX_PX,
            tearOpenSnapRatio: TEAR_OPEN_SNAP_RATIO,
            tearCloseSnapRatio: TEAR_CLOSE_SNAP_RATIO,
            segmenterLocale: String(global.navigator && global.navigator.language ? global.navigator.language : 'zh-CN'),
            notify: null,
            getContext: null,
        }, options || {});

        const highlightModule = global.mobileHighlightEngine;
        const state = {
            container: null,
            titles: [],
            highlightTerms: [],
            titlesLoaded: false,
            activeCard: null,
            touchGesture: null,
            lastTouchOpenAt: 0,
            bound: false,
            highlightEngine: null,
            selectionTrigger: null,
            selectionPayload: null,
            selectionChangeTimer: 0,
            focusedTermNode: null,
            segmenter: createWordSegmenter(config.segmenterLocale),
            advicePrefetches: new Map(),
        };

        async function refresh(params = {}) {
            const nextContainer = params.container || state.container;
            if (!nextContainer) return;
            bindContainer(nextContainer);
            await closeActiveCard({ save: true, silent: true });
            clearFocusedTermNode();
            if (!state.titlesLoaded) {
                await loadTitles();
            }
            await loadHighlightCandidates(params.markdownText);
            resetHighlightRuntime();
            unwrapHighlights(state.container);
            applyHighlights(state.container);
        }

        function destroy() {
            resetHighlightRuntime();
            closeActiveCard({ save: false, silent: true }).catch(() => null);
            unbindContainer();
            clearSelectionChangeTimer();
            hideSelectionTrigger({ immediate: true });
            clearFocusedTermNode();
            if (state.selectionTrigger && state.selectionTrigger.parentNode) {
                state.selectionTrigger.parentNode.removeChild(state.selectionTrigger);
            }
            state.selectionTrigger = null;
            state.selectionPayload = null;
            if (state.highlightEngine && typeof state.highlightEngine.destroy === 'function') {
                state.highlightEngine.destroy();
            }
            state.highlightEngine = null;
            state.advicePrefetches.clear();
        }

        async function loadTitles() {
            try {
                const response = await fetch(`${config.apiBase}/cards/titles`);
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}`);
                }
                const payload = await response.json();
                const rawTitles = Array.isArray(payload.titles) ? payload.titles : [];
                state.titles = rawTitles
                    .map((item) => String(item || '').trim())
                    .filter(Boolean)
                    .filter((title) => title.length >= 2)
                    .slice(0, config.maxHighlightTerms)
                    .sort((a, b) => b.length - a.length);
                updateHighlightTerms(state.titles);
                state.titlesLoaded = true;
            } catch (error) {
                state.titles = [];
                updateHighlightTerms([]);
                state.titlesLoaded = true;
                emitNotice(`姒傚康璇嶅姞杞藉け璐ワ細${normalizeError(error)}`, 'error');
            }
        }

        async function loadHighlightCandidates(markdownText) {
            if (!state.titlesLoaded) return;
            const context = buildHighlightCandidateContext(markdownText);
            if (!context) {
                updateHighlightTerms(state.titles);
                return;
            }
            const endpoint = String(config.highlightCandidatesEndpoint || '').trim() || `${config.apiBase}/cards/titles/candidates`;
            try {
                const response = await fetch(endpoint, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        context,
                        topK: Number(config.highlightCandidatesTopK) || 1200,
                    }),
                });
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}`);
                }
                const payload = await response.json();
                const candidates = Array.isArray(payload.titles) ? payload.titles : [];
                if (candidates.length) {
                    updateHighlightTerms(candidates);
                    return;
                }
            } catch (_error) {
                // 候选词预过滤失败时回退全量标题，保证高亮功能可用。
            }
            updateHighlightTerms(state.titles);
        }

        function buildHighlightCandidateContext(markdownText) {
            const fallbackContext = typeof config.getContext === 'function' ? (config.getContext() || {}) : {};
            const raw = String(markdownText || fallbackContext.markdown || '').trim();
            if (!raw) return '';
            const maxChars = Math.max(256, Number(config.highlightCandidatesContextChars) || 18000);
            return raw.slice(0, maxChars);
        }

        function ensureHighlightEngine() {
            if (state.highlightEngine) {
                return state.highlightEngine;
            }
            if (!highlightModule || typeof highlightModule.create !== 'function') {
                return null;
            }
            state.highlightEngine = highlightModule.create({
                blockSelector: BLOCK_SELECTOR,
                skipSelector: SKIP_HIGHLIGHT_SELECTOR,
                maxHighlightTerms: config.maxHighlightTerms,
                maxTermsPerBucket: config.maxTermsPerBucket,
                highlightInitialBlockLimit: config.highlightInitialBlockLimit,
                highlightFrameBudgetMs: config.highlightFrameBudgetMs,
                highlightObserverRootMargin: config.highlightObserverRootMargin,
                workerUrl: config.highlightWorkerUrl,
                workerMinTerms: config.highlightWorkerMinTerms,
            });
            if (state.highlightTerms.length && typeof state.highlightEngine.setTerms === 'function') {
                state.highlightEngine.setTerms(state.highlightTerms);
            }
            return state.highlightEngine;
        }

        function updateHighlightTerms(rawTitles) {
            const normalized = (Array.isArray(rawTitles) ? rawTitles : [])
                .map((item) => String(item || '').trim())
                .filter(Boolean)
                .filter((title) => title.length >= 2)
                .slice(0, config.maxHighlightTerms)
                .sort((a, b) => b.length - a.length);
            state.highlightTerms = normalized;
            const engine = ensureHighlightEngine();
            if (engine && typeof engine.setTerms === 'function') {
                engine.setTerms(state.highlightTerms);
            }
        }

        function resetHighlightRuntime() {
            const engine = ensureHighlightEngine();
            if (engine && typeof engine.resetRuntime === 'function') {
                engine.resetRuntime(state.container);
            } else if (state.container) {
                state.container.querySelectorAll('[data-concept-highlighted="1"]').forEach((node) => {
                    node.removeAttribute('data-concept-highlighted');
                });
            }
        }

        function bindContainer(container) {
            if (state.container === container && state.bound) {
                return;
            }
            unbindContainer();
            state.container = container;
            state.container.addEventListener('click', onContainerClick, true);
            state.container.addEventListener('touchstart', onTouchStart, { capture: true, passive: false });
            state.container.addEventListener('touchmove', onTouchMove, { capture: true, passive: false });
            state.container.addEventListener('touchend', onTouchEnd, { capture: true, passive: false });
            state.container.addEventListener('touchcancel', onTouchCancel, { capture: true, passive: true });
            document.addEventListener('pointerdown', onDocumentPointerDown, true);
            window.addEventListener('scroll', onWindowScroll, true);
            window.addEventListener('resize', onWindowResize, true);
            state.bound = true;
        }

        function unbindContainer() {
            resetHighlightRuntime();
            clearSelectionChangeTimer();
            hideSelectionTrigger({ immediate: true });
            clearFocusedTermNode();
            if (!state.container || !state.bound) {
                state.container = null;
                state.bound = false;
                return;
            }
            state.container.removeEventListener('click', onContainerClick, true);
            state.container.removeEventListener('touchstart', onTouchStart, true);
            state.container.removeEventListener('touchmove', onTouchMove, true);
            state.container.removeEventListener('touchend', onTouchEnd, true);
            state.container.removeEventListener('touchcancel', onTouchCancel, true);
            document.removeEventListener('pointerdown', onDocumentPointerDown, true);
            window.removeEventListener('scroll', onWindowScroll, true);
            window.removeEventListener('resize', onWindowResize, true);
            state.container = null;
            state.bound = false;
        }

        function onDocumentSelectionChange() {
            clearSelectionChangeTimer();
            state.selectionChangeTimer = window.setTimeout(() => {
                state.selectionChangeTimer = 0;
                refreshSelectionTrigger();
            }, SELECTION_TRIGGER_HIDE_DELAY_MS);
        }

        function onWindowResize() {
            if (!state.selectionPayload) {
                hideSelectionTrigger({ immediate: true });
                return;
            }
            showSelectionTrigger(state.selectionPayload);
        }

        function refreshSelectionTrigger() {
            const payload = resolveSelectionPayload();
            if (!payload) {
                hideSelectionTrigger({ immediate: false });
                return;
            }
            showSelectionTrigger(payload);
        }

        function onContainerClick(event) {
            let termNode = null;
            if (event.detail >= 2) {
                const sentence = resolveSentenceFromPoint(event.clientX, event.clientY);
                termNode = ensureTermNodeForPhrase(sentence);
            }
            if (!termNode) {
                termNode = resolveTermNode(event.target);
            }
            if (!termNode) {
                const phrase = resolvePhraseFromPoint(event.clientX, event.clientY);
                termNode = ensureTermNodeForPhrase(phrase);
            }
            if (!termNode) return;
            if (Date.now() - state.lastTouchOpenAt < 420) {
                event.preventDefault();
                event.stopPropagation();
                return;
            }
            event.preventDefault();
            event.stopPropagation();
            focusTermNode(termNode);
        }

        function onTouchStart(event) {
            if (!event.touches || event.touches.length !== 1) return;
            const touch = event.touches[0];
            let termNode = resolveTermNode(event.target);
            if (!termNode) {
                const phrase = resolvePhraseFromPoint(touch.clientX, touch.clientY);
                termNode = ensureTermNodeForPhrase(phrase);
            }
            if (!termNode) {
                clearTouchGesture();
                return;
            }
            const anchor = resolveAnchorBlock(termNode);
            focusTermNode(termNode);
            state.touchGesture = {
                termNode,
                anchor,
                startX: touch.clientX,
                startY: touch.clientY,
                progress: 0,
                hapticFired: false,
                advicePrefetchKey: '',
            };
        }

        function onTouchMove(event) {
            if (!state.touchGesture || !event.touches || event.touches.length !== 1) return;
            const touch = event.touches[0];
            const dx = touch.clientX - state.touchGesture.startX;
            const dy = touch.clientY - state.touchGesture.startY;
            if (dy < -8 || Math.abs(dx) > Number(config.tearOpenSwipeMaxDxPx || TEAR_OPEN_SWIPE_MAX_DX_PX)) {
                finalizeOpenProbe(state.touchGesture, false);
                clearTouchGesture();
                return;
            }
            if (dy <= 0) {
                return;
            }
            const resisted = applySwipeResistance(dy);
            const fullPull = Math.max(60, TEAR_OPEN_FULL_PULL_PX, Number(config.tearOpenSwipeThresholdPx || TEAR_OPEN_SWIPE_THRESHOLD_PX) * 3.2);
            const progress = Math.max(0, Math.min(1.25, resisted / fullPull));
            state.touchGesture.progress = progress;
            updateOpenProbe(state.touchGesture, progress, touch.clientX);
            if (progress >= 0.08) {
                primeAdviceForGesture(state.touchGesture);
            }
            if (progress >= TEAR_HAPTIC_TAP_RATIO && !state.touchGesture.hapticFired) {
                state.touchGesture.hapticFired = true;
                fireHapticTap();
            }
            event.preventDefault();
            event.stopPropagation();
        }

        function onTouchEnd(event) {
            if (!state.touchGesture) return;
            const gesture = state.touchGesture;
            event.stopPropagation();
            const snapRatio = Number(config.tearOpenSnapRatio || TEAR_OPEN_SNAP_RATIO);
            const shouldOpen = Number(gesture.progress) >= snapRatio;
            if (shouldOpen) {
                finalizeOpenProbe(gesture, true);
                state.lastTouchOpenAt = Date.now();
                openCard(gesture.termNode.dataset.term, gesture.termNode, {
                    advicePrefetchKey: gesture.advicePrefetchKey,
                    selectionSnippet: gesture.termNode.textContent || '',
                }).catch((error) => {
                    emitNotice(`Open card failed: ${normalizeError(error)}`, 'error');
                });
                event.preventDefault();
            } else {
                finalizeOpenProbe(gesture, false);
            }
            clearTouchGesture();
        }

        function onTouchCancel() {
            if (state.touchGesture) {
                finalizeOpenProbe(state.touchGesture, false);
            }
            clearTouchGesture();
        }

        function clearTouchGesture() {
            if (!state.touchGesture) return;
            state.touchGesture = null;
        }

        function applySwipeResistance(distancePx) {
            const raw = Math.max(0, Number(distancePx) || 0);
            if (!raw) return 0;
            return (raw * 0.82) / (1 + (raw / 220));
        }

        function updateOpenProbe(gesture, progress, clientX) {
            if (!gesture || !gesture.anchor) return;
            const anchor = gesture.anchor;
            const safeProgress = Math.max(0, Math.min(1, Number(progress) || 0));
            anchor.classList.add('concept-open-probe');
            anchor.classList.remove('is-probe-rebound');
            anchor.style.setProperty('--tear-open-ratio', safeProgress.toFixed(4));
            anchor.style.setProperty('--tear-shadow-depth', (0.18 + (safeProgress * 0.54)).toFixed(4));
            const rect = anchor.getBoundingClientRect();
            const ratioX = rect && rect.width > 0
                ? Math.max(0, Math.min(1, (Number(clientX) - rect.left) / rect.width))
                : 0.5;
            anchor.style.setProperty('--tear-shadow-focal-x', `${(ratioX * 100).toFixed(2)}%`);
        }

        function finalizeOpenProbe(gesture, opened) {
            if (!gesture || !gesture.anchor) return;
            const anchor = gesture.anchor;
            if (opened) {
                anchor.classList.remove('concept-open-probe', 'is-probe-rebound');
                anchor.style.removeProperty('--tear-open-ratio');
                anchor.style.removeProperty('--tear-shadow-depth');
                anchor.style.removeProperty('--tear-shadow-focal-x');
                return;
            }
            anchor.classList.add('is-probe-rebound');
            anchor.style.setProperty('--tear-open-ratio', '0');
            window.setTimeout(() => {
                anchor.classList.remove('concept-open-probe', 'is-probe-rebound');
                anchor.style.removeProperty('--tear-open-ratio');
                anchor.style.removeProperty('--tear-shadow-depth');
                anchor.style.removeProperty('--tear-shadow-focal-x');
            }, TEAR_REBOUND_MS);
        }

        function fireHapticTap() {
            if (!global.navigator || typeof global.navigator.vibrate !== 'function') return;
            try {
                global.navigator.vibrate(10);
            } catch (_error) {
                // ignore haptic errors
            }
        }

        function primeAdviceForGesture(gesture) {
            if (!gesture || !gesture.termNode || !gesture.anchor) return;
            if (gesture.advicePrefetchKey) return;
            const term = String(gesture.termNode.dataset.term || '').trim();
            if (!term) return;
            const selectionSnippet = normalizeSelectionSnippet(gesture.termNode.textContent || '');
            const contextInfo = resolveContextInfo(term, gesture.anchor, selectionSnippet);
            const key = createAdvicePrefetchKey(term, contextInfo);
            if (!state.advicePrefetches.has(key)) {
                const promise = requestAdviceResult(term, contextInfo);
                state.advicePrefetches.set(key, { promise, createdAt: Date.now() });
                trimAdvicePrefetchCache();
            }
            gesture.advicePrefetchKey = key;
        }

        function createAdvicePrefetchKey(term, contextInfo) {
            const t = String(term || '').trim();
            const context = contextInfo && contextInfo.context ? String(contextInfo.context).slice(0, 180) : '';
            const example = contextInfo && contextInfo.example ? String(contextInfo.example).slice(0, 120) : '';
            return `${t}::${context}::${example}`;
        }

        function trimAdvicePrefetchCache() {
            const items = Array.from(state.advicePrefetches.entries());
            if (items.length <= 12) return;
            items
                .sort((a, b) => Number(a[1].createdAt || 0) - Number(b[1].createdAt || 0))
                .slice(0, Math.max(0, items.length - 12))
                .forEach(([key]) => state.advicePrefetches.delete(key));
        }

        function consumeAdvicePrefetch(prefetchKey) {
            const key = String(prefetchKey || '').trim();
            if (!key) return null;
            const entry = state.advicePrefetches.get(key);
            if (!entry) return null;
            state.advicePrefetches.delete(key);
            return entry.promise || null;
        }

        function onDocumentPointerDown(event) {
            if (state.selectionTrigger && state.selectionTrigger.contains(event.target)) {
                return;
            }
            hideSelectionTrigger({ immediate: true });
            clearFocusedTermNode();
            if (!state.activeCard) return;
            const root = state.activeCard.root;
            if (root && root.contains(event.target)) {
                return;
            }
            if (resolveTermNode(event.target)) {
                return;
            }
            closeActiveCard({ save: true }).catch((error) => {
                emitNotice(`淇濆瓨姒傚康鍗＄墖澶辫触锛?{normalizeError(error)}`, 'error');
            });
        }

        function onWindowScroll() {
            hideSelectionTrigger({ immediate: true });
            clearFocusedTermNode();
            if (!state.activeCard) return;
            closeActiveCard({ save: true, silent: true }).catch((error) => {
                emitNotice(`淇濆瓨姒傚康鍗＄墖澶辫触锛?{normalizeError(error)}`, 'error');
            });
        }

        function clearSelectionChangeTimer() {
            if (!state.selectionChangeTimer) return;
            clearTimeout(state.selectionChangeTimer);
            state.selectionChangeTimer = 0;
        }

        function ensureSelectionTrigger() {
            if (state.selectionTrigger && state.selectionTrigger.isConnected) {
                return state.selectionTrigger;
            }
            const trigger = document.createElement('button');
            trigger.type = 'button';
            trigger.className = 'concept-selection-trigger';
            trigger.textContent = '新建卡片';
            trigger.hidden = true;
            trigger.addEventListener('pointerdown', (event) => {
                event.preventDefault();
                event.stopPropagation();
            });
            trigger.addEventListener('click', (event) => {
                event.preventDefault();
                event.stopPropagation();
                openCardFromSelection().catch((error) => {
                    emitNotice(`新建卡片失败：${normalizeError(error)}`, 'error');
                });
            });
            document.body.appendChild(trigger);
            state.selectionTrigger = trigger;
            return trigger;
        }

        function showSelectionTrigger(payload) {
            if (!payload || !payload.rect) return;
            const trigger = ensureSelectionTrigger();
            const viewportW = Math.max(320, Number(global.innerWidth) || 0);
            const viewportH = Math.max(320, Number(global.innerHeight) || 0);
            const targetX = payload.rect.left + (payload.rect.width / 2);
            const targetY = payload.rect.top - 14;
            const clampedX = Math.max(56, Math.min(viewportW - 56, targetX));
            const clampedY = Math.max(16, Math.min(viewportH - 18, targetY));
            trigger.style.left = `${clampedX}px`;
            trigger.style.top = `${clampedY}px`;
            trigger.hidden = false;
            trigger.classList.add('is-visible');
            state.selectionPayload = payload;
        }

        function hideSelectionTrigger(options = {}) {
            state.selectionPayload = null;
            const trigger = state.selectionTrigger;
            if (!trigger) return;
            trigger.classList.remove('is-visible');
            if (options.immediate) {
                trigger.hidden = true;
                return;
            }
            window.setTimeout(() => {
                if (!trigger.classList.contains('is-visible')) {
                    trigger.hidden = true;
                }
            }, 150);
        }

        function resolveSelectionPayload() {
            if (!state.container || !global.getSelection) return null;
            const selection = global.getSelection();
            if (!selection || selection.isCollapsed || selection.rangeCount < 1) return null;
            const term = normalizeSelectionTerm(selection.toString());
            if (!term) return null;
            const range = selection.getRangeAt(0);
            if (!range) return null;
            if (!isNodeInsideContainer(range.commonAncestorContainer)) return null;
            const endpointNode = resolveRangeEndpointNode(range);
            if (!endpointNode) return null;
            const endpointElement = endpointNode.nodeType === Node.TEXT_NODE
                ? endpointNode.parentElement
                : endpointNode;
            if (!endpointElement || !state.container.contains(endpointElement)) return null;
            if (endpointElement.closest('.card-fissure, .concept-tear-scene')) return null;
            const rect = resolveSelectionRect(range);
            if (!rect) return null;
            return {
                term,
                rect,
                anchorNode: endpointNode,
                selectionSnippet: normalizeSelectionSnippet(selection.toString()),
            };
        }

        function resolveRangeEndpointNode(range) {
            if (!range) return null;
            return range.startContainer || range.commonAncestorContainer || null;
        }

        function isNodeInsideContainer(node) {
            if (!state.container || !node) return false;
            if (node.nodeType === Node.TEXT_NODE) {
                return !!(node.parentNode && state.container.contains(node.parentNode));
            }
            return state.container.contains(node);
        }

        function resolveSelectionRect(range) {
            if (!range) return null;
            const firstRect = range.getClientRects && range.getClientRects().length
                ? range.getClientRects()[0]
                : null;
            const fallbackRect = range.getBoundingClientRect ? range.getBoundingClientRect() : null;
            const rect = firstRect || fallbackRect;
            if (!rect) return null;
            if (!Number.isFinite(rect.left) || !Number.isFinite(rect.top)) return null;
            return rect;
        }

        function normalizeSelectionTerm(rawText) {
            let text = String(rawText || '').replace(/\s+/g, ' ').trim();
            if (!text) return '';
            text = text.replace(/^[\s"'“”‘’`~!@#$%^&*()_+\-=[\]{}|;:,.<>/?，。！？；：、（）【】《》]+/, '');
            text = text.replace(/[\s"'“”‘’`~!@#$%^&*()_+\-=[\]{}|;:,.<>/?，。！？；：、（）【】《》]+$/, '');
            if (!text) return '';
            if (text.length > SELECTION_TERM_MAX_CHARS) {
                text = text.slice(0, SELECTION_TERM_MAX_CHARS).trim();
            }
            return text;
        }

        function normalizeSelectionSnippet(rawText) {
            const normalized = String(rawText || '')
                .replace(/\r\n?/g, '\n')
                .split('\n')
                .map((line) => line.trim())
                .filter(Boolean)
                .join('\n')
                .trim();
            if (!normalized) return '';
            if (normalized.length <= SELECTION_SNIPPET_MAX_CHARS) {
                return normalized;
            }
            return `${normalized.slice(0, SELECTION_SNIPPET_MAX_CHARS).trim()}…`;
        }

        function clearNativeSelection() {
            if (!global.getSelection) return;
            const selection = global.getSelection();
            if (!selection || !selection.removeAllRanges) return;
            selection.removeAllRanges();
        }

        async function openCardFromSelection(options = {}) {
            if (state.focusedTermNode && state.focusedTermNode.dataset && state.focusedTermNode.dataset.term) {
                await openCard(state.focusedTermNode.dataset.term, state.focusedTermNode, {
                    allowToggleClose: false,
                    anchorNode: state.focusedTermNode,
                });
                return true;
            }
            const payload = options.payload || state.selectionPayload || resolveSelectionPayload();
            if (!payload) return false;
            hideSelectionTrigger({ immediate: true });
            clearNativeSelection();
            await openCard(payload.term, payload.anchorNode, {
                allowToggleClose: false,
                anchorNode: payload.anchorNode,
                selectionSnippet: payload.selectionSnippet,
            });
            return true;
        }

        function resolveTermNode(target) {
            if (!target || !target.closest || !state.container) return null;
            const node = target.closest('.concept-term');
            if (!node || !state.container.contains(node)) return null;
            return node;
        }

        function focusTermNode(node) {
            if (!node || !node.classList || !state.container || !state.container.contains(node)) return;
            if (state.focusedTermNode && state.focusedTermNode !== node && state.focusedTermNode.classList) {
                state.focusedTermNode.classList.remove('is-selected');
            }
            state.focusedTermNode = node;
            state.focusedTermNode.classList.add('is-selected');
        }

        function clearFocusedTermNode() {
            if (state.focusedTermNode && state.focusedTermNode.classList) {
                state.focusedTermNode.classList.remove('is-selected');
            }
            state.focusedTermNode = null;
        }

        function ensureTermNodeForPhrase(phrase) {
            if (!phrase || !phrase.textNode) return null;
            const textNode = phrase.textNode;
            if (textNode.nodeType !== Node.TEXT_NODE) return null;
            const parent = textNode.parentElement;
            if (!parent || !state.container || !state.container.contains(parent)) return null;
            if (parent.closest('.card-fissure, .concept-tear-scene')) return null;

            const source = String(textNode.nodeValue || '');
            const start = Math.max(0, Math.min(source.length, Number(phrase.startOffset) || 0));
            const end = Math.max(start, Math.min(source.length, Number(phrase.endOffset) || 0));
            if (end <= start) return null;

            const exactTerm = source.slice(start, end);
            const safeTerm = normalizeSelectionTerm(phrase.term || exactTerm);
            if (!safeTerm) return null;

            const fragment = document.createDocumentFragment();
            if (start > 0) {
                fragment.appendChild(document.createTextNode(source.slice(0, start)));
            }
            const span = document.createElement('span');
            span.className = 'concept-term concept-highlight concept-manual-term';
            span.dataset.term = safeTerm;
            span.textContent = exactTerm;
            fragment.appendChild(span);
            if (end < source.length) {
                fragment.appendChild(document.createTextNode(source.slice(end)));
            }
            textNode.replaceWith(fragment);
            return span;
        }

        function resolvePhraseFromPoint(clientX, clientY) {
            const caret = resolveCaretFromPoint(clientX, clientY);
            if (!caret || !caret.node) return null;
            const textNode = coerceTextNode(caret.node, caret.offset);
            if (!textNode || textNode.nodeType !== Node.TEXT_NODE) return null;
            const parent = textNode.parentElement;
            if (!parent || !state.container || !state.container.contains(parent)) return null;
            if (parent.closest('.card-fissure, .concept-tear-scene, .concept-term')) return null;
            const source = String(textNode.nodeValue || '');
            if (!source.trim()) return null;
            const segmented = resolveSegmentedTermFromText(source, Number(caret.offset) || 0);
            if (!segmented || !segmented.term) return null;
            return {
                term: segmented.term,
                textNode,
                startOffset: segmented.start,
                endOffset: segmented.end,
            };
        }

        function resolveSentenceFromPoint(clientX, clientY) {
            const caret = resolveCaretFromPoint(clientX, clientY);
            if (!caret || !caret.node) return null;
            const textNode = coerceTextNode(caret.node, caret.offset);
            if (!textNode || textNode.nodeType !== Node.TEXT_NODE) return null;
            const parent = textNode.parentElement;
            if (!parent || !state.container || !state.container.contains(parent)) return null;
            if (parent.closest('.card-fissure, .concept-tear-scene, .concept-term')) return null;
            const source = String(textNode.nodeValue || '');
            if (!source.trim()) return null;
            const offset = Math.max(0, Math.min(source.length, Number(caret.offset) || 0));
            const sentence = resolveSentenceOffsets(source, offset);
            if (!sentence) return null;
            const term = normalizeSelectionTerm(source.slice(sentence.start, sentence.end));
            if (!term) return null;
            return {
                term,
                textNode,
                startOffset: sentence.start,
                endOffset: sentence.end,
            };
        }

        function resolveSentenceOffsets(source, offset) {
            const text = String(source || '');
            if (!text) return null;
            let start = Math.max(0, Math.min(text.length, Number(offset) || 0));
            let end = start;
            while (start > 0) {
                const ch = text[start - 1];
                if (/[.!?;\n\u3002\uff01\uff1f\uff1b]/.test(ch)) break;
                start -= 1;
            }
            while (end < text.length) {
                const ch = text[end];
                if (/[.!?;\n\u3002\uff01\uff1f\uff1b]/.test(ch)) break;
                end += 1;
            }
            if (end <= start) return null;
            return { start, end };
        }

        function resolveCaretFromPoint(clientX, clientY) {
            const x = Number(clientX);
            const y = Number(clientY);
            if (!Number.isFinite(x) || !Number.isFinite(y)) return null;
            if (document.caretRangeFromPoint) {
                const range = document.caretRangeFromPoint(x, y);
                if (!range) return null;
                return {
                    node: range.startContainer || range.commonAncestorContainer || null,
                    offset: Number(range.startOffset) || 0,
                };
            }
            if (document.caretPositionFromPoint) {
                const caret = document.caretPositionFromPoint(x, y);
                if (!caret) return null;
                return {
                    node: caret.offsetNode || null,
                    offset: Number(caret.offset) || 0,
                };
            }
            return null;
        }

        function coerceTextNode(node, offset) {
            if (!node) return null;
            if (node.nodeType === Node.TEXT_NODE) return node;
            if (node.nodeType !== Node.ELEMENT_NODE || !node.childNodes || !node.childNodes.length) {
                return null;
            }
            const index = Math.max(0, Math.min(node.childNodes.length - 1, Number(offset) || 0));
            const direct = node.childNodes[index];
            if (direct && direct.nodeType === Node.TEXT_NODE) {
                return direct;
            }
            const walker = document.createTreeWalker(node, NodeFilter.SHOW_TEXT);
            return walker.nextNode();
        }

        function resolveSegmentedTermFromText(source, caretOffset) {
            const text = String(source || '');
            if (!text.trim()) return null;
            const offset = Math.max(0, Math.min(text.length, Number(caretOffset) || 0));
            const intlMatched = state.segmenter ? resolveTermBySegmenter(text, offset) : null;
            if (intlMatched && intlMatched.term) return intlMatched;
            return resolveTermByRegex(text, offset);
        }

        function resolveTermBySegmenter(source, caretOffset) {
            const segments = [];
            let fallbackIndex = 0;
            for (const part of state.segmenter.segment(source)) {
                const segment = String(part && part.segment ? part.segment : '');
                if (!segment) continue;
                const start = Number.isFinite(part.index) ? Number(part.index) : fallbackIndex;
                const end = start + segment.length;
                segments.push({
                    segment,
                    start,
                    end,
                    isWordLike: part.isWordLike !== false,
                });
                fallbackIndex = end;
            }
            if (!segments.length) return null;

            const targetOffset = Math.max(0, Math.min(source.length, caretOffset));
            let center = segments.findIndex((item) => targetOffset >= item.start && targetOffset < item.end);
            if (center < 0 && targetOffset === source.length) {
                center = segments.length - 1;
            }
            if (center < 0 || !canUseSegment(segments[center])) {
                center = findNearestUsableSegment(segments, targetOffset);
            }
            if (center < 0) return null;

            let left = center;
            let right = center;
            let tokenCount = 1;
            let charCount = segments[center].segment.length;
            while (tokenCount < SEGMENTER_MAX_TOKENS) {
                const leftCandidate = left > 0 ? segments[left - 1] : null;
                const rightCandidate = right < (segments.length - 1) ? segments[right + 1] : null;
                const canTakeLeft = !!(leftCandidate && canUseSegment(leftCandidate) && (charCount + leftCandidate.segment.length) <= SEGMENTER_MAX_CHARS);
                const canTakeRight = !!(rightCandidate && canUseSegment(rightCandidate) && (charCount + rightCandidate.segment.length) <= SEGMENTER_MAX_CHARS);
                if (!canTakeLeft && !canTakeRight) break;
                if (canTakeRight && (!canTakeLeft || rightCandidate.segment.length >= leftCandidate.segment.length)) {
                    right += 1;
                    tokenCount += 1;
                    charCount += segments[right].segment.length;
                } else {
                    left -= 1;
                    tokenCount += 1;
                    charCount += segments[left].segment.length;
                }
            }

            const start = segments[left].start;
            const end = segments[right].end;
            const term = normalizeSelectionTerm(source.slice(start, end));
            if (!term) return null;
            return { term, start, end };
        }

        function canUseSegment(segment) {
            if (!segment) return false;
            const raw = String(segment.segment || '');
            if (!raw.trim()) return false;
            if (SEGMENT_BOUNDARY_REGEX.test(raw)) return false;
            if (segment.isWordLike === false && !/[A-Za-z0-9\u4e00-\u9fff]/.test(raw)) return false;
            return true;
        }

        function findNearestUsableSegment(segments, targetOffset) {
            if (!Array.isArray(segments) || !segments.length) return -1;
            let bestIndex = -1;
            let bestDistance = Number.POSITIVE_INFINITY;
            segments.forEach((item, index) => {
                if (!canUseSegment(item)) return;
                const center = item.start + ((item.end - item.start) / 2);
                const distance = Math.abs(center - targetOffset);
                if (distance < bestDistance) {
                    bestDistance = distance;
                    bestIndex = index;
                }
            });
            return bestIndex;
        }

        function resolveTermByRegex(source, caretOffset) {
            const text = String(source || '');
            SEGMENT_WORD_FALLBACK_REGEX.lastIndex = 0;
            let match = null;
            while ((match = SEGMENT_WORD_FALLBACK_REGEX.exec(text))) {
                const term = String(match[0] || '');
                if (!term) continue;
                const start = match.index;
                const end = start + term.length;
                if (caretOffset >= start && caretOffset <= end) {
                    const normalized = normalizeSelectionTerm(term);
                    if (!normalized) return null;
                    return { term: normalized, start, end };
                }
            }
            return null;
        }

        function createWordSegmenter(locale) {
            if (!global.Intl || typeof global.Intl.Segmenter !== 'function') return null;
            try {
                return new global.Intl.Segmenter(String(locale || 'zh-CN'), { granularity: 'word' });
            } catch (_error) {
                try {
                    return new global.Intl.Segmenter('zh-CN', { granularity: 'word' });
                } catch (_innerError) {
                    return null;
                }
            }
        }

        async function openCard(term, triggerNode, options = {}) {
            const safeTerm = String(term || '').trim();
            if (!safeTerm || !state.container) return;
            hideSelectionTrigger({ immediate: true });
            clearFocusedTermNode();
            if (state.activeCard && state.activeCard.term === safeTerm) {
                if (options.allowToggleClose === false) {
                    if (state.activeCard.textarea && typeof state.activeCard.textarea.focus === 'function') {
                        state.activeCard.textarea.focus({ preventScroll: true });
                    }
                    return;
                }
                await closeActiveCard({ save: true });
                return;
            }
            await closeActiveCard({ save: true, silent: true });

            const anchor = resolveAnchorBlock(options.anchorNode || triggerNode);
            if (!anchor) return;
            const seedSelectionSnippet = normalizeSelectionSnippet(options.selectionSnippet);
            const contextInfo = resolveContextInfo(safeTerm, anchor, seedSelectionSnippet);
            const cardRoot = buildCardRoot(safeTerm);
            const tearScene = mountTearScene(anchor, cardRoot);
            if (!tearScene) {
                anchor.insertAdjacentElement('afterend', cardRoot);
            }
            requestAnimationFrame(() => {
                cardRoot.classList.add('is-open');
                if (tearScene && tearScene.wrapper) {
                    tearScene.wrapper.classList.add('is-open');
                }
            });

            const activeCard = {
                term: safeTerm,
                root: cardRoot,
                anchor,
                tearScene,
                seedSelectionSnippet,
                contextInfo,
                advicePrefetchKey: String(options.advicePrefetchKey || ''),
                textarea: cardRoot.querySelector('[data-card-editor]'),
                whisper: cardRoot.querySelector('[data-card-whisper]'),
                fog: cardRoot.querySelector('[data-card-fog]'),
                backlinksPanel: cardRoot.querySelector('[data-card-backlinks]'),
                newCard: false,
                wikilinkPanel: cardRoot.querySelector('[data-wikilink-suggest]'),
                wikilinkSuggestions: [],
                wikilinkActiveIndex: -1,
                wikilinkTriggerStart: -1,
            };
            state.activeCard = activeCard;
            wireCardEvents(activeCard);
            if (tearScene && tearScene.wrapper) {
                tearScene.wrapper.classList.add('concept-anchor-active');
            } else {
                anchor.classList.add('concept-anchor-active');
            }

            const existing = await loadCard(safeTerm);
            if (existing.exists && existing.markdown) {
                activeCard.textarea.value = String(existing.markdown);
            } else {
                activeCard.newCard = true;
                activeCard.textarea.value = buildNewCardTemplate(
                    safeTerm,
                    activeCard.seedSelectionSnippet,
                    contextInfo
                );
            }
            void loadAdvice(activeCard);
            await loadBacklinks(activeCard);

            activeCard.textarea.focus({ preventScroll: true });
        }

        function resolveAnchorBlock(startNode) {
            if (!startNode) return null;
            const element = startNode.nodeType === Node.TEXT_NODE
                ? startNode.parentElement
                : startNode;
            if (!element || !element.closest) return null;
            return element.closest(BLOCK_SELECTOR) || element.closest('*');
        }

        function buildNewCardTemplate(term, selectionSnippet, contextInfo) {
            const safeTerm = String(term || '').trim();
            const safeSnippet = normalizeSelectionSnippet(selectionSnippet);
            const fallbackExample = contextInfo ? extractContextExample(contextInfo.example) : '';
            const quoteSource = safeSnippet || fallbackExample;
            const quote = quoteSource
                .split('\n')
                .map((line) => `> ${line}`)
                .join('\n');
            const quoteBlock = quoteSource
                ? `> 语境例子（仅作例子，不是定义）\n${quote}\n\n`
                : '> 语境例子（请引用当前段落原文，不要写成名词解释）\n\n';
            return `## ${safeTerm}\n\n- 主张：\n- 机制（为什么成立）：\n${quoteBlock}- 边界/反例：\n`;
        }

        function buildCardRoot(term) {
            const root = document.createElement('section');
            root.className = 'card-fissure';
            root.innerHTML = `
                <div class="card-fissure-shell" role="group" aria-label="Concept card ${escapeHtml(term)}">
                    <header class="card-fissure-header">
                        <span class="card-fissure-title">${escapeHtml(term)}</span>
                        <span class="card-fissure-seam-tip">Swipe up to seal</span>
                    </header>
                    <textarea class="card-fissure-editor" data-card-editor placeholder="Write your interpretation in this context, not just a definition."></textarea>
                    <div class="card-fissure-wikilink-panel" data-wikilink-suggest hidden></div>
                    <div class="card-fissure-backlinks" data-card-backlinks hidden></div>
                    <div class="ai-fog-layer" data-card-fog hidden>
                        <div class="ai-fog-line"></div>
                        <div class="ai-fog-line"></div>
                        <div class="ai-fog-line short"></div>
                        <div class="ai-fog-status"></div>
                    </div>
                    <div class="ai-whisper" data-card-whisper hidden></div>
                </div>
            `;
            return root;
        }

        function wireCardEvents(card) {
            if (!card || !card.root) return;
            card.textarea.addEventListener('input', () => {
                refreshWikilinkSuggestions(card);
            });
            card.textarea.addEventListener('click', () => {
                refreshWikilinkSuggestions(card);
            });
            card.textarea.addEventListener('keyup', (event) => {
                if (WIKILINK_KEYUP_SKIP_KEYS.has(event.key)) return;
                refreshWikilinkSuggestions(card);
            });
            card.textarea.addEventListener('blur', () => {
                window.setTimeout(() => {
                    hideWikilinkSuggestions(card);
                }, 120);
            });
            card.textarea.addEventListener('keydown', (event) => {
                if (!hasVisibleWikilinkSuggestions(card)) return;
                if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
                    event.preventDefault();
                    moveWikilinkActiveIndex(card, event.key === 'ArrowDown' ? 1 : -1);
                    return;
                }
                if (event.key === 'Enter' || event.key === 'Tab') {
                    event.preventDefault();
                    applyWikilinkSuggestion(card, card.wikilinkActiveIndex);
                    return;
                }
                if (event.key === 'Escape') {
                    hideWikilinkSuggestions(card);
                }
            });
            if (card.wikilinkPanel) {
                card.wikilinkPanel.addEventListener('mousedown', (event) => {
                    event.preventDefault();
                    const item = event.target.closest('[data-wikilink-index]');
                    if (!item) return;
                    const index = Number(item.getAttribute('data-wikilink-index'));
                    applyWikilinkSuggestion(card, index);
                });
            }
            if (card.backlinksPanel) {
                card.backlinksPanel.addEventListener('click', (event) => {
                    const trigger = event.target.closest('[data-backlink-open]');
                    if (!trigger) return;
                    const nextTitle = String(trigger.getAttribute('data-backlink-open') || '').trim();
                    if (!nextTitle || nextTitle === card.term) return;
                    openCard(nextTitle, card.anchor).catch((error) => {
                        emitNotice(`Open backlink card failed: ${normalizeError(error)}`, 'error');
                    });
                });
            }
            card.whisper.addEventListener('click', () => {
                applyAdviceToEditor(card, false);
            });
            card.whisper.addEventListener('dblclick', (event) => {
                event.preventDefault();
                applyAdviceToEditor(card, true);
            });
            card.root.addEventListener('touchmove', (event) => {
                if (!event.touches || event.touches.length !== 1) return;
                const touch = event.touches[0];
                updateTearShadowFocalFromPoint(card, touch.clientX, touch.clientY);
            }, { passive: true });
            wireCardSwipeSeal(card);
        }

        function wireCardSwipeSeal(card) {
            if (!card || !card.root) return;
            const shell = card.root.querySelector('.card-fissure-shell');
            if (!shell) return;
            card.root.addEventListener('touchstart', (event) => {
                if (state.activeCard !== card) return;
                if (!event.touches || event.touches.length !== 1) return;
                if (event.target && event.target.closest('.card-fissure-editor, .card-fissure-wikilink-panel, .card-fissure-backlinks, .ai-whisper')) {
                    return;
                }
                const touch = event.touches[0];
                const rect = shell.getBoundingClientRect();
                const localY = touch.clientY - rect.top;
                if (localY > 82 && !event.target.closest('.card-fissure-header')) {
                    return;
                }
                card.sealGesture = {
                    startX: touch.clientX,
                    startY: touch.clientY,
                    progress: 0,
                    hapticFired: false,
                };
            }, { passive: true });
            card.root.addEventListener('touchmove', (event) => {
                if (state.activeCard !== card || !card.sealGesture) return;
                if (!event.touches || event.touches.length !== 1) return;
                const touch = event.touches[0];
                const dx = Math.abs(touch.clientX - card.sealGesture.startX);
                const dyUp = card.sealGesture.startY - touch.clientY;
                if (dx > 64) {
                    return;
                }
                if (dyUp <= 0) {
                    updateTearSceneOpenRatio(card, 1);
                    return;
                }
                const resisted = applySwipeResistance(dyUp);
                const progress = Math.max(0, Math.min(1.2, resisted / TEAR_CLOSE_FULL_PULL_PX));
                card.sealGesture.progress = progress;
                updateTearSceneOpenRatio(card, Math.max(0, 1 - progress));
                updateTearShadowFocalFromPoint(card, touch.clientX, touch.clientY);
                if (progress >= TEAR_HAPTIC_TAP_RATIO && !card.sealGesture.hapticFired) {
                    card.sealGesture.hapticFired = true;
                    fireHapticTap();
                }
                event.preventDefault();
            }, { passive: false });
            card.root.addEventListener('touchend', () => {
                if (state.activeCard !== card || !card.sealGesture) return;
                const snapRatio = Number(config.tearCloseSnapRatio || TEAR_CLOSE_SNAP_RATIO);
                const shouldClose = Number(card.sealGesture.progress) >= snapRatio;
                card.sealGesture = null;
                if (shouldClose) {
                    closeBySwipeSeal(card);
                    return;
                }
                updateTearSceneOpenRatio(card, 1);
                if (card.tearScene && card.tearScene.wrapper) {
                    card.tearScene.wrapper.classList.add('is-seal-rebound');
                    window.setTimeout(() => {
                        if (card.tearScene && card.tearScene.wrapper) {
                            card.tearScene.wrapper.classList.remove('is-seal-rebound');
                        }
                    }, TEAR_REBOUND_MS);
                }
            }, { passive: true });
            card.root.addEventListener('touchcancel', () => {
                if (!card.sealGesture) return;
                card.sealGesture = null;
                updateTearSceneOpenRatio(card, 1);
            }, { passive: true });
        }

        function closeBySwipeSeal(card) {
            if (!card || state.activeCard !== card) return;
            playSealClickSound();
            fireHapticTap();
            closeActiveCard({ save: true, fromSwipeSeal: true }).catch((error) => {
                emitNotice(`Seal close failed: ${normalizeError(error)}`, 'error');
            });
        }

        function updateTearSceneOpenRatio(card, ratio) {
            if (!card || !card.tearScene || !card.tearScene.wrapper) return;
            const safeRatio = Math.max(0, Math.min(1, Number(ratio) || 0));
            card.tearScene.wrapper.style.setProperty('--tear-open-ratio', safeRatio.toFixed(4));
            card.tearScene.wrapper.style.setProperty('--tear-shadow-depth', (0.14 + (safeRatio * 0.66)).toFixed(4));
        }

        function updateTearShadowFocalFromPoint(card, clientX, clientY) {
            if (!card || !card.tearScene || !card.tearScene.wrapper) return;
            const wrapper = card.tearScene.wrapper;
            const rect = wrapper.getBoundingClientRect();
            if (!rect || rect.width <= 0 || rect.height <= 0) return;
            const ratioX = Math.max(0, Math.min(1, (Number(clientX) - rect.left) / rect.width));
            const ratioY = Math.max(0, Math.min(1, (Number(clientY) - rect.top) / rect.height));
            wrapper.style.setProperty('--tear-shadow-focal-x', `${(ratioX * 100).toFixed(2)}%`);
            wrapper.style.setProperty('--tear-shadow-focal-y', `${(ratioY * 100).toFixed(2)}%`);
        }

        function playSealClickSound() {
            const AudioCtor = global.AudioContext || global.webkitAudioContext;
            if (!AudioCtor) return;
            try {
                const ctx = new AudioCtor();
                const now = ctx.currentTime;
                const gain = ctx.createGain();
                gain.gain.setValueAtTime(0.0001, now);
                gain.gain.exponentialRampToValueAtTime(0.12, now + 0.008);
                gain.gain.exponentialRampToValueAtTime(0.0001, now + 0.09);
                gain.connect(ctx.destination);

                const osc = ctx.createOscillator();
                osc.type = 'triangle';
                osc.frequency.setValueAtTime(1320, now);
                osc.frequency.exponentialRampToValueAtTime(540, now + 0.09);
                osc.connect(gain);
                osc.start(now);
                osc.stop(now + 0.1);

                const noise = ctx.createBufferSource();
                const buffer = ctx.createBuffer(1, Math.max(1, Math.floor(ctx.sampleRate * 0.05)), ctx.sampleRate);
                const data = buffer.getChannelData(0);
                for (let i = 0; i < data.length; i += 1) {
                    data[i] = (Math.random() * 2) - 1;
                }
                const noiseGain = ctx.createGain();
                noiseGain.gain.setValueAtTime(0.0001, now);
                noiseGain.gain.exponentialRampToValueAtTime(0.05, now + 0.004);
                noiseGain.gain.exponentialRampToValueAtTime(0.0001, now + 0.05);
                noise.buffer = buffer;
                noise.connect(noiseGain);
                noiseGain.connect(ctx.destination);
                noise.start(now);
                noise.stop(now + 0.05);

                window.setTimeout(() => {
                    if (ctx.state !== 'closed') {
                        void ctx.close().catch(() => null);
                    }
                }, 320);
            } catch (_error) {
                // ignore audio errors
            }
        }

        async function loadBacklinks(card) {
            if (!card || !card.backlinksPanel) return;
            const panel = card.backlinksPanel;
            panel.hidden = false;
            panel.innerHTML = '<div class="card-fissure-backlinks-loading">正在读取反向链接...</div>';
            try {
                const storageTitle = normalizeStorageTitle(card.term);
                if (!storageTitle) {
                    panel.hidden = true;
                    return;
                }
                const response = await fetch(`${config.apiBase}/cards/concept/${encodeURIComponent(storageTitle)}/backlinks`);
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}`);
                }
                const payload = await response.json();
                if (state.activeCard !== card) {
                    return;
                }
                const items = Array.isArray(payload && payload.items) ? payload.items : [];
                renderBacklinks(card, items);
            } catch (_error) {
                if (state.activeCard !== card) {
                    return;
                }
                panel.innerHTML = '<div class="card-fissure-backlinks-empty">反向链接读取失败</div>';
            }
        }

        function renderBacklinks(card, items) {
            if (!card || !card.backlinksPanel) return;
            const panel = card.backlinksPanel;
            const normalizedItems = (items || [])
                .map((item) => ({
                    sourceTitle: String(item && item.sourceTitle ? item.sourceTitle : '').trim(),
                    count: Number(item && item.count) || 0,
                }))
                .filter((item) => item.sourceTitle);
            if (!normalizedItems.length) {
                panel.innerHTML = '<div class="card-fissure-backlinks-empty">暂无反向链接</div>';
                panel.hidden = false;
                return;
            }
            panel.innerHTML = `
                <div class="card-fissure-backlinks-title">反向链接</div>
                ${normalizedItems.map((item) => `
                    <button
                        type="button"
                        class="card-fissure-backlink-item"
                        data-backlink-open="${escapeHtml(item.sourceTitle)}"
                    >
                        <span class="card-fissure-backlink-title">${escapeHtml(item.sourceTitle)}</span>
                        <span class="card-fissure-backlink-count">x${Math.max(1, item.count)}</span>
                    </button>
                `).join('')}
            `;
            panel.hidden = false;
        }

        function refreshWikilinkSuggestions(card) {
            if (!card || !card.textarea) return;
            const textarea = card.textarea;
            const cursor = Number(textarea.selectionStart);
            if (!Number.isFinite(cursor) || cursor < 0) {
                hideWikilinkSuggestions(card);
                return;
            }
            const beforeCursor = String(textarea.value || '').slice(0, cursor);
            const triggerMatch = beforeCursor.match(WIKILINK_TRIGGER_REGEX);
            if (!triggerMatch) {
                hideWikilinkSuggestions(card);
                return;
            }
            const query = String(triggerMatch[1] || '');
            const triggerLength = triggerMatch[0].length;
            card.wikilinkTriggerStart = cursor - triggerLength;
            const suggestions = collectWikilinkSuggestions(query, card.term);
            if (!suggestions.length) {
                hideWikilinkSuggestions(card);
                return;
            }
            showWikilinkSuggestions(card, suggestions);
        }

        function collectWikilinkSuggestions(query, activeTerm) {
            const normalizedQuery = String(query || '').trim().toLowerCase();
            const candidates = [];
            const seen = new Set();
            const pushUnique = (rawValue) => {
                const value = String(rawValue || '').trim();
                if (!value) return;
                const key = value.toLowerCase();
                if (seen.has(key)) return;
                seen.add(key);
                candidates.push(value);
            };
            pushUnique(activeTerm);
            (state.titles || []).forEach(pushUnique);

            const startsWith = [];
            const contains = [];
            candidates.forEach((title) => {
                if (!normalizedQuery) {
                    startsWith.push(title);
                    return;
                }
                const lowerTitle = title.toLowerCase();
                const idx = lowerTitle.indexOf(normalizedQuery);
                if (idx < 0) return;
                if (idx === 0) {
                    startsWith.push(title);
                } else {
                    contains.push(title);
                }
            });
            return startsWith.concat(contains).slice(0, WIKILINK_SUGGEST_MAX_ITEMS);
        }

        function hasVisibleWikilinkSuggestions(card) {
            return !!(card && card.wikilinkPanel && !card.wikilinkPanel.hidden && card.wikilinkSuggestions.length > 0);
        }

        function showWikilinkSuggestions(card, suggestions) {
            if (!card || !card.wikilinkPanel) return;
            card.wikilinkSuggestions = Array.isArray(suggestions) ? suggestions : [];
            card.wikilinkActiveIndex = card.wikilinkSuggestions.length ? 0 : -1;
            if (card.textarea) {
                const panelTop = Number(card.textarea.offsetTop) + Number(card.textarea.offsetHeight) + 4;
                card.wikilinkPanel.style.top = `${Math.max(0, panelTop)}px`;
            }
            card.wikilinkPanel.innerHTML = card.wikilinkSuggestions.map((item, index) => `
                <button type="button" class="card-fissure-wikilink-item${index === card.wikilinkActiveIndex ? ' is-active' : ''}" data-wikilink-index="${index}">
                    ${escapeHtml(item)}
                </button>
            `).join('');
            card.wikilinkPanel.hidden = card.wikilinkSuggestions.length === 0;
        }

        function hideWikilinkSuggestions(card) {
            if (!card) return;
            card.wikilinkSuggestions = [];
            card.wikilinkActiveIndex = -1;
            card.wikilinkTriggerStart = -1;
            if (!card.wikilinkPanel) return;
            card.wikilinkPanel.hidden = true;
            card.wikilinkPanel.innerHTML = '';
        }

        function moveWikilinkActiveIndex(card, direction) {
            if (!hasVisibleWikilinkSuggestions(card)) return;
            const total = card.wikilinkSuggestions.length;
            if (!total) return;
            const baseIndex = Number(card.wikilinkActiveIndex);
            const current = Number.isFinite(baseIndex) && baseIndex >= 0 ? baseIndex : 0;
            const next = (current + direction + total) % total;
            card.wikilinkActiveIndex = next;
            card.wikilinkPanel.querySelectorAll('.card-fissure-wikilink-item').forEach((node, index) => {
                node.classList.toggle('is-active', index === next);
            });
        }

        function applyWikilinkSuggestion(card, index) {
            if (!card || !card.textarea) return;
            const textarea = card.textarea;
            const suggestions = card.wikilinkSuggestions || [];
            const nextIndex = Number(index);
            const resolvedIndex = Number.isFinite(nextIndex) && nextIndex >= 0 ? nextIndex : 0;
            const selected = suggestions[resolvedIndex];
            if (!selected) {
                hideWikilinkSuggestions(card);
                return;
            }
            const cursor = Number(textarea.selectionStart);
            const start = Number(card.wikilinkTriggerStart);
            if (!Number.isFinite(cursor) || !Number.isFinite(start) || start < 0 || start > cursor) {
                hideWikilinkSuggestions(card);
                return;
            }
            const before = textarea.value.slice(0, start);
            const after = textarea.value.slice(cursor);
            const insert = `[[${selected}]]`;
            textarea.value = `${before}${insert}${after}`;
            const caret = before.length + insert.length;
            textarea.setSelectionRange(caret, caret);
            hideWikilinkSuggestions(card);
            textarea.focus({ preventScroll: true });
        }

        function applyAdviceToEditor(card, asQuote) {
            if (!card || !card.whisper || !card.textarea) return;
            const advice = String(card.whisper.textContent || '').trim();
            if (!advice) return;
            const line = asQuote ? `> ${advice}` : advice;
            const current = String(card.textarea.value || '');
            const suffix = current.endsWith('\n') || current.length === 0 ? '' : '\n';
            card.textarea.value = `${current}${suffix}${line}\n`;
            toggleVisibilityWithTransition(card.whisper, false, { visibleClass: WHISPER_VISIBLE_CLASS });
            card.textarea.focus({ preventScroll: true });
        }

        async function loadAdvice(card) {
            if (!card || !card.whisper) return;
            const contextInfo = card.contextInfo || resolveContextInfo(card.term, card.anchor, card.seedSelectionSnippet);
            card.contextInfo = contextInfo;
            showAdviceFog(card, { offline: false, status: '' });
            const prefetched = consumeAdvicePrefetch(card.advicePrefetchKey);
            try {
                const result = prefetched || await requestAdviceResult(card.term, contextInfo);
                if (state.activeCard !== card) return;
                const advice = String(result && result.advice ? result.advice : '').trim();
                if (!advice) {
                    if (result && result.offline) {
                        showAdviceFog(card, {
                            offline: true,
                            status: '\u7eb8\u5f20\u5df2\u7834\uff0c\u4f46\u601d\u7eea\u6682\u65ad\u3002',
                        });
                    } else {
                        hideAdviceFog(card);
                        toggleVisibilityWithTransition(card.whisper, false, { visibleClass: WHISPER_VISIBLE_CLASS });
                    }
                    return;
                }
                hideAdviceFog(card);
                card.whisper.textContent = advice;
                card.whisper.classList.remove('is-ink-reveal');
                card.whisper.offsetWidth;
                card.whisper.classList.add('is-ink-reveal');
                toggleVisibilityWithTransition(card.whisper, true, { visibleClass: WHISPER_VISIBLE_CLASS });
            } catch (error) {
                if (state.activeCard !== card) return;
                const offline = isLikelyOfflineError(error);
                if (offline) {
                    showAdviceFog(card, {
                        offline: true,
                        status: '\u7eb8\u5f20\u5df2\u7834\uff0c\u4f46\u601d\u7eea\u6682\u65ad\u3002',
                    });
                    return;
                }
                hideAdviceFog(card);
            }
        }

        async function requestAdviceResult(term, contextInfo) {
            try {
                const response = await fetch(`${config.apiBase}/cards/ai-advice`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        term,
                        context: contextInfo.context,
                        contextExample: contextInfo.example,
                        isContextDependent: contextInfo.isContextDependent,
                    }),
                });
                if (!response.ok) {
                    return { advice: '', offline: response.status === 0 };
                }
                const payload = await response.json();
                return {
                    advice: String(payload && payload.advice ? payload.advice : '').trim(),
                    offline: false,
                };
            } catch (error) {
                return {
                    advice: '',
                    offline: isLikelyOfflineError(error),
                };
            }
        }

        function showAdviceFog(card, options = {}) {
            if (!card || !card.fog) return;
            const offline = !!options.offline;
            const status = String(options.status || '').trim();
            card.fog.hidden = false;
            card.fog.classList.toggle('is-offline', offline);
            const statusNode = card.fog.querySelector('.ai-fog-status');
            if (statusNode) {
                statusNode.textContent = status;
                statusNode.hidden = !status;
            }
        }

        function hideAdviceFog(card) {
            if (!card || !card.fog) return;
            card.fog.hidden = true;
            card.fog.classList.remove('is-offline');
            const statusNode = card.fog.querySelector('.ai-fog-status');
            if (statusNode) {
                statusNode.hidden = true;
                statusNode.textContent = '';
            }
        }

        function isLikelyOfflineError(error) {
            if (global.navigator && global.navigator.onLine === false) {
                return true;
            }
            const message = normalizeError(error).toLowerCase();
            return message.includes('network')
                || message.includes('failed to fetch')
                || message.includes('offline')
                || message.includes('fetch');
        }

        function toggleVisibilityWithTransition(node, visible, options = {}) {
            if (!node) return;
            const visibleClass = String(options.visibleClass || 'is-visible');
            if (!visible) {
                node.classList.remove(visibleClass);
                node.hidden = true;
                return;
            }
            node.hidden = false;
            node.classList.remove(visibleClass);
            if (isReducedMotionPreferred()) {
                node.classList.add(visibleClass);
                return;
            }
            // 先移除再下一帧恢复可见态，确保同一节点重复展示时过渡仍会触发。
            requestAnimationFrame(() => {
                if (!node.hidden) {
                    node.classList.add(visibleClass);
                }
            });
        }

        function isReducedMotionPreferred() {
            if (!global.matchMedia) return false;
            return global.matchMedia('(prefers-reduced-motion: reduce)').matches;
        }

        function resolveContextInfo(term, anchor, selectionSnippet) {
            const context = typeof config.getContext === 'function' ? (config.getContext() || {}) : {};
            const markdown = String(context.markdown || '');
            const anchorText = anchor ? String(anchor.textContent || '').trim() : '';
            const seedSnippet = normalizeSelectionSnippet(selectionSnippet);
            const mergedContext = [anchorText, seedSnippet, markdown]
                .filter((part) => String(part || '').trim())
                .join('\n')
                .slice(0, config.contextChars);
            const example = extractContextExample(seedSnippet || anchorText || mergedContext);
            return {
                context: mergedContext,
                example,
                isContextDependent: true,
                type: THOUGHT_CARD_TYPE,
            };
        }

        function extractContextExample(rawContext) {
            const normalized = String(rawContext || '').replace(/\s+/g, ' ').trim();
            if (!normalized) return '';
            if (normalized.length <= 160) {
                return normalized;
            }
            return `${normalized.slice(0, 160).trim()}...`;
        }

        async function closeActiveCard(options = {}) {
            const active = state.activeCard;
            if (!active) return;
            clearTouchGesture();
            hideWikilinkSuggestions(active);
            hideAdviceFog(active);
            const saveResult = options.save !== false ? await saveCard(active, options) : null;
            state.activeCard = null;
            if (active.tearScene && active.tearScene.wrapper) {
                active.tearScene.wrapper.classList.remove('concept-anchor-active');
                if (options.fromSwipeSeal) {
                    active.tearScene.wrapper.classList.add('is-seal-impact');
                }
            } else if (active.anchor) {
                active.anchor.classList.remove('concept-anchor-active');
            }
            if (active.root) {
                active.root.classList.remove('is-open');
                active.root.classList.add('is-closing');
            }
            if (active.tearScene && active.tearScene.wrapper) {
                active.tearScene.wrapper.classList.remove('is-open');
                active.tearScene.wrapper.classList.add('is-closing');
            }
            window.setTimeout(() => {
                if (active.tearScene) {
                    unmountTearScene(active.tearScene);
                    return;
                }
                if (active.root && active.root.parentNode) {
                    active.root.parentNode.removeChild(active.root);
                }
                if (saveResult) {
                    enterPostSaveState(active, saveResult);
                }
            }, CARD_CLOSE_ANIMATION_MS);
        }

        // 通过“上下半片文本 + 中间卡片”的结构模拟纸张撕裂效果，保持原文可读性。
        function ensurePaperFiberFilter() {
            if (!document || !document.body) return;
            if (document.getElementById(PAPER_FIBER_FILTER_ID)) return;
            const svgNs = 'http://www.w3.org/2000/svg';
            const svg = document.createElementNS(svgNs, 'svg');
            svg.setAttribute('aria-hidden', 'true');
            svg.setAttribute('width', '0');
            svg.setAttribute('height', '0');
            svg.style.position = 'absolute';
            svg.style.width = '0';
            svg.style.height = '0';
            const defs = document.createElementNS(svgNs, 'defs');
            const filter = document.createElementNS(svgNs, 'filter');
            filter.setAttribute('id', PAPER_FIBER_FILTER_ID);
            filter.setAttribute('x', '-15%');
            filter.setAttribute('y', '-30%');
            filter.setAttribute('width', '130%');
            filter.setAttribute('height', '180%');
            const turbulence = document.createElementNS(svgNs, 'feTurbulence');
            turbulence.setAttribute('type', 'fractalNoise');
            turbulence.setAttribute('baseFrequency', '0.95');
            turbulence.setAttribute('numOctaves', '1');
            turbulence.setAttribute('seed', '7');
            turbulence.setAttribute('result', 'noise');
            const displace = document.createElementNS(svgNs, 'feDisplacementMap');
            displace.setAttribute('in', 'SourceGraphic');
            displace.setAttribute('in2', 'noise');
            displace.setAttribute('scale', '5');
            displace.setAttribute('xChannelSelector', 'R');
            displace.setAttribute('yChannelSelector', 'A');
            filter.appendChild(turbulence);
            filter.appendChild(displace);
            defs.appendChild(filter);
            svg.appendChild(defs);
            svg.id = PAPER_FIBER_FILTER_ID;
            document.body.appendChild(svg);
        }

        function mountTearScene(anchor, cardRoot) {
            if (!anchor || !anchor.parentNode || !cardRoot) return null;
            if (String(anchor.tagName || '').toUpperCase() === 'LI') return null;
            ensurePaperFiberFilter();
            const sourceParent = anchor.parentNode;
            const sourceNextSibling = anchor.nextSibling;

            const wrapper = document.createElement('div');
            wrapper.className = 'concept-tear-scene';
            wrapper.style.setProperty('--tear-open-ratio', '1');
            wrapper.style.setProperty('--tear-shadow-depth', '0.72');
            wrapper.style.setProperty('--tear-shadow-focal-x', '50%');
            wrapper.style.setProperty('--tear-shadow-focal-y', '42%');

            const sourceRect = anchor.getBoundingClientRect();
            const sourceHeight = Math.max(Math.round(anchor.offsetHeight || sourceRect.height || 0), 28);
            const splitTop = Math.min(sourceHeight - 8, Math.max(18, Math.round(sourceHeight * TEAR_SPLIT_RATIO)));
            const splitBottom = Math.max(8, sourceHeight - splitTop);
            wrapper.style.setProperty('--tear-top-height', `${splitTop}px`);
            wrapper.style.setProperty('--tear-bottom-height', `${splitBottom}px`);

            const computed = window.getComputedStyle(anchor);
            wrapper.style.marginTop = computed.marginTop;
            wrapper.style.marginBottom = computed.marginBottom;
            wrapper.style.marginLeft = computed.marginLeft;
            wrapper.style.marginRight = computed.marginRight;

            const topHalf = document.createElement('div');
            topHalf.className = 'concept-tear-half concept-tear-half-top';
            const bottomHalf = document.createElement('div');
            bottomHalf.className = 'concept-tear-half concept-tear-half-bottom';

            const topClone = cloneAnchorForTear(anchor, 'concept-tear-copy');
            const bottomClone = cloneAnchorForTear(anchor, 'concept-tear-copy');
            topHalf.appendChild(topClone);
            bottomHalf.appendChild(bottomClone);

            sourceParent.insertBefore(wrapper, anchor);
            sourceParent.removeChild(anchor);
            wrapper.appendChild(topHalf);
            wrapper.appendChild(cardRoot);
            wrapper.appendChild(bottomHalf);

            return {
                wrapper,
                source: anchor,
                sourceParent,
                sourceNextSibling,
            };
        }

        function unmountTearScene(scene) {
            if (!scene || !scene.sourceParent || !scene.source) return;
            const wrapper = scene.wrapper;
            if (wrapper && wrapper.parentNode) {
                wrapper.parentNode.removeChild(wrapper);
            }
            if (scene.sourceNextSibling && scene.sourceNextSibling.parentNode === scene.sourceParent) {
                scene.sourceParent.insertBefore(scene.source, scene.sourceNextSibling);
            } else {
                scene.sourceParent.appendChild(scene.source);
            }
        }

        function cloneAnchorForTear(anchor, extraClass) {
            const clone = anchor.cloneNode(true);
            if (extraClass) {
                clone.classList.add(extraClass);
            }
            clone.removeAttribute('id');
            clone.querySelectorAll('[id]').forEach((node) => node.removeAttribute('id'));
            clone.querySelectorAll('.concept-term').forEach((node) => {
                node.removeAttribute('data-term');
            });
            clone.setAttribute('aria-hidden', 'true');
            return clone;
        }

        async function saveCard(activeCard, options) {
            if (!activeCard || !activeCard.textarea) return;
            const markdown = String(activeCard.textarea.value || '');
            try {
                const storageTitle = normalizeStorageTitle(activeCard.term);
                if (!storageTitle) {
                    throw new Error('概念标题非法，无法保存');
                }
                const contextInfo = activeCard.contextInfo
                    || resolveContextInfo(activeCard.term, activeCard.anchor, activeCard.seedSelectionSnippet);
                const query = new URLSearchParams();
                query.set('isContextDependent', String(contextInfo.isContextDependent));
                query.set('type', contextInfo.type);
                const endpoint = `${config.apiBase}/cards/concept/${encodeURIComponent(storageTitle)}?${query.toString()}`;
                const response = await fetch(endpoint, {
                    method: 'POST',
                    headers: { 'Content-Type': 'text/plain;charset=UTF-8' },
                    body: markdown,
                });
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}`);
                }
                let payload = null;
                if (isJsonResponse(response)) {
                    payload = await response.json();
                }
                appendLocalTitle(storageTitle);
                if (!options || options.silent !== true) {
                    emitNotice(`Saved card: ${activeCard.term}`, 'success');
                }
                return payload;
            } catch (error) {
                if (!options || options.silent !== true) {
                    throw error;
                }
                return null;
            }
        }

        function appendLocalTitle(title) {
            const safe = String(title || '').trim();
            if (!safe) return;
            if (!state.titles.includes(safe)) {
                state.titles.push(safe);
                state.titles.sort((a, b) => b.length - a.length);
                if (!state.highlightTerms.includes(safe)) {
                    const next = state.highlightTerms.concat(safe);
                    updateHighlightTerms(next);
                }
            }
        }

        async function loadCard(term) {
            try {
                const storageTitle = normalizeStorageTitle(term);
                if (!storageTitle) {
                    return { exists: false, markdown: '' };
                }
                const response = await fetch(`${config.apiBase}/cards/concept/${encodeURIComponent(storageTitle)}`);
                if (response.status === 404) {
                    return { exists: false, markdown: '' };
                }
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}`);
                }
                const payload = await response.json();
                return {
                    exists: true,
                    markdown: String(payload && payload.markdown ? payload.markdown : ''),
                };
            } catch (error) {
                emitNotice(`璇诲彇姒傚康鍗＄墖澶辫触锛?{normalizeError(error)}`, 'error');
                return { exists: false, markdown: '' };
            }
        }

        function isJsonResponse(response) {
            if (!response || !response.headers) return false;
            const contentType = String(response.headers.get('content-type') || '').toLowerCase();
            return contentType.includes('application/json');
        }

        function enterPostSaveState(activeCard, saveResult) {
            const target = resolvePostSaveTarget(activeCard, saveResult);
            if (!target) return;
            const behavior = isReducedMotionPreferred() ? 'auto' : 'smooth';
            target.scrollIntoView({ behavior, block: 'center', inline: 'nearest' });
            target.classList.add('concept-save-highlight');
            window.setTimeout(() => {
                target.classList.remove('concept-save-highlight');
            }, POST_SAVE_HIGHLIGHT_MS);
        }

        function resolvePostSaveTarget(activeCard, saveResult) {
            const locator = saveResult && typeof saveResult === 'object' ? saveResult.locator : null;
            const locatorKind = locator && typeof locator.kind === 'string' ? locator.kind.trim().toLowerCase() : '';
            const locatorTitle = locator && typeof locator.value === 'string' ? locator.value.trim() : '';
            if (locatorKind === 'title' && locatorTitle && state.container && global.CSS && typeof global.CSS.escape === 'function') {
                const termNode = state.container.querySelector(`.concept-term[data-term="${global.CSS.escape(locatorTitle)}"]`);
                if (termNode) {
                    return resolveAnchorBlock(termNode) || termNode;
                }
            }
            if (activeCard && activeCard.tearScene && activeCard.tearScene.source && state.container && state.container.contains(activeCard.tearScene.source)) {
                return activeCard.tearScene.source;
            }
            if (activeCard && activeCard.anchor && state.container && state.container.contains(activeCard.anchor)) {
                return activeCard.anchor;
            }
            return null;
        }

        function unwrapHighlights(container) {
            container.querySelectorAll('span.concept-term').forEach((node) => {
                const textNode = document.createTextNode(node.textContent || '');
                node.replaceWith(textNode);
            });
            container.querySelectorAll('[data-concept-highlighted="1"]').forEach((node) => {
                node.removeAttribute('data-concept-highlighted');
            });
        }

        function applyHighlights(container) {
            if (!container || !state.highlightTerms.length) return;
            const engine = ensureHighlightEngine();
            if (!engine || typeof engine.applyHighlights !== 'function') return;
            engine.applyHighlights(container);
        }

        function countOccurrences(text, keyword) {
            if (!text || !keyword) return 0;
            let count = 0;
            let start = 0;
            while (start >= 0) {
                const idx = text.indexOf(keyword, start);
                if (idx < 0) break;
                count += 1;
                start = idx + keyword.length;
            }
            return count;
        }

        function normalizeStorageTitle(rawTitle) {
            let title = String(rawTitle || '').trim();
            if (!title) return '';
            title = title.replace(STORAGE_TITLE_ILLEGAL_CHARS, '_');
            title = title.replace(/\s+/g, ' ').trim();
            title = title.replace(STORAGE_TITLE_TRAILING_DOTS_OR_SPACE, '');
            if (!title || title === '.' || title === '..') {
                return '';
            }
            if (STORAGE_TITLE_WINDOWS_RESERVED.test(title)) {
                title = `_${title}`;
            }
            return title.slice(0, 120).trim().replace(STORAGE_TITLE_TRAILING_DOTS_OR_SPACE, '');
        }

        function normalizeError(error) {
            if (!error) return 'Unknown error';
            if (typeof error === 'string') return error;
            if (error && typeof error.message === 'string') return error.message;
            return String(error);
        }

        function emitNotice(message, type) {
            if (typeof config.notify === 'function') {
                config.notify(String(message || ''), type || 'info');
            } else if (type === 'error') {
                console.warn('[concept-cards]', message);
            }
        }

        function escapeHtml(raw) {
            return String(raw || '')
                .replace(/&/g, '&amp;')
                .replace(/</g, '&lt;')
                .replace(/>/g, '&gt;')
                .replace(/"/g, '&quot;')
                .replace(/'/g, '&#39;');
        }

        return Object.freeze({
            refresh,
            destroy,
            closeActiveCard,
            openFromSelection: openCardFromSelection,
        });
    }

    global.mobileConceptCards = Object.freeze({
        create: createMobileConceptCards,
    });
})(window);

