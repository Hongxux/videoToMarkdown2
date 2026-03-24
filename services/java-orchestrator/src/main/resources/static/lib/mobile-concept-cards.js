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
    const SELECTION_SNIPPET_MAX_CHARS = 220;
    const SELECTION_TERM_MAX_CHARS = 120;
    const THOUGHT_CARD_TYPE = 'thought';
    const TEAR_OPEN_SWIPE_THRESHOLD_PX = 34;
    const TEAR_OPEN_PROBE_START_PX = 12;
    const TEAR_OPEN_SWIPE_MAX_DX_PX = 52;
    const TEAR_OPEN_SNAP_RATIO = 0.34;
    const TEAR_CLOSE_SNAP_RATIO = 0.52;
    const TEAR_CLOSE_MIN_SWIPE_PX = 88;
    const TEAR_HAPTIC_TAP_RATIO = 0.96;
    const TEAR_OPEN_FULL_PULL_PX = 104;
    const TEAR_CLOSE_FULL_PULL_PX = 120;
    const TEAR_REBOUND_MS = 280;
    const TEAR_SCENE_TARGET_CENTER_RATIO = 0.52;
    const TEAR_SCENE_MIN_VISIBLE_TOP_PX = 10;
    const TEAR_SCENE_MIN_CARD_TOP_PX = 56;
    const TEAR_SCENE_MIN_CARD_TOP_RATIO = 0.16;
    const CARD_OPEN_MIN_HEIGHT_PX = 180;
    const CARD_OPEN_FIXED_VIEWPORT_RATIO = 0.62;
    const CARD_OPEN_FIXED_MAX_HEIGHT_PX = 640;
    const CARD_TAIL_SPACE_RATIO = 1 / 3;
    const WINDOW_SCROLL_CLOSE_DELTA_PX = 320;
    const WINDOW_SCROLL_CLOSE_GRACE_MS = 420;
    const OUTSIDE_SWIPE_CLOSE_ENABLED = false;
    const OUTSIDE_CLOSE_MIN_SWIPE_PX = 78;
    const OUTSIDE_CLOSE_MAX_HORIZONTAL_PX = 92;
    const OUTSIDE_CLOSE_MAX_DURATION_MS = 1100;
    const CARD_INSIDE_TAP_MAX_TRAVEL_PX = 14;
    const CARD_INSIDE_DOUBLE_TAP_WINDOW_MS = 320;
    const CARD_INSIDE_DOUBLE_TAP_MAX_DISTANCE_PX = 26;
    const TOUCH_SELECTION_DOUBLE_TAP_WINDOW_MS = 420;
    const TOUCH_SELECTION_DOUBLE_TAP_MAX_DISTANCE_PX = 26;
    const REOPEN_SELECTED_DOUBLE_TAP_ARM_WINDOW_MS = 3000;
    const PULL_DOWN_OPEN_THRESHOLD_PX = 40;
    const PULL_DOWN_MAX_DX_PX = 30;
    const PULL_DOWN_OPEN_ENABLED = false;
    const PAPER_FIBER_FILTER_ID = 'paper-fiber-distortion';
    const SINGLE_CLICK_COMMIT_DELAY_MS = 240;
    const STRICT_SHORT_TAP_MAX_DISTANCE_PX = 12;
    const STRICT_SHORT_TAP_MAX_DURATION_MS = 420;
    const SEGMENTER_MAX_TOKENS = 6;
    const SEGMENTER_MAX_CHARS = 36;
    const TERM_WINDOW_MAX_TOKENS = 6;
    const TERM_WINDOW_MAX_CHARS = 24;
    const TERM_DICTIONARY_MATCH_LIMIT = 1800;
    const TERM_DICTIONARY_MATCH_RADIUS = 64;
    const ADVICE_MIN_CHARS = 10;
    const ADVICE_KEYWORD_MAX = 14;
    const ADVICE_KEYWORD_REGEX = /[A-Za-z0-9_\-\u4e00-\u9fff]{2,}/g;
    const ADVICE_PREFETCH_PROGRESS_RATIO = 0.08;
    const ADVICE_MARKDOWN_MAX_CHARS = 18000;
    const AI_ADVICE_BLOCK_START = '<!-- ai-advice:start -->';
    const AI_ADVICE_BLOCK_END = '<!-- ai-advice:end -->';
    const LEARNED_TERMS_STORAGE_KEY = 'mobile.concept.learnedTerms.v1';
    const LEARNED_TERMS_MAX_ITEMS = 800;
    const LEGACY_PLACEHOLDER_LINE_REGEX = /^\s*[-*]\s*(?:主张|机制(?:（为什么成立）|\(为什么成立\))?|边界\s*[\/／]\s*反例)\s*[:：]?\s*$/gim;
    const LEGACY_PLACEHOLDER_HEADING_REGEX = /^\s{0,3}#{1,6}\s*(?:主张|机制(?:（为什么成立）|\(为什么成立\))?|边界\s*[\/／]\s*反例)\s*$/gim;
    const LEGACY_PLACEHOLDER_QUOTE_BLOCK_REGEX = /^\s*>\s*语境例子[^\n]*(?:\n\s*>[^\n]*)*/gim;
    const ADVICE_PLACEHOLDER_REGEX = /\{[a-z][a-z0-9_]{1,40}\}/i;
    const ADVICE_PROMPT_LEAK_REGEX = /\b(?:system|user|assistant)\s*prompt\b|context_block|example_block|output\s*format|role\s*[:：]\s*(?:system|user|assistant)/i;
    const ADVICE_STOPWORD_SET = new Set([
        'this', 'that', 'these', 'those', 'with', 'from', 'into', 'about', 'have',
        'will', 'would', 'should', 'could', 'their', 'there', 'which', 'where',
        'when', 'while', 'context', 'example', 'based', 'using',
        '\u8fd9\u4e2a', '\u90a3\u4e2a', '\u8fd9\u4e9b', '\u90a3\u4e9b', '\u6211\u4eec',
        '\u4f60\u4eec', '\u4ed6\u4eec', '\u5176\u4e2d', '\u4ee5\u53ca', '\u5982\u679c',
        '\u6240\u4ee5', '\u56e0\u4e3a',
    ]);
    const TERM_EDGE_PARTICLE_SET = new Set([
        '\u7684', '\u5730', '\u5f97', '\u5728', '\u4e8e', '\u7740', '\u8fc7',
        '\u548c', '\u4e0e', '\u662f', '\u88ab', '\u628a', '\u5c06', '\u5bf9', '\u53ca',
    ]);
    const TERM_STOPWORD_SET = new Set([
        '\u8fd9\u4e2a', '\u90a3\u4e2a', '\u8fd9\u4e9b', '\u90a3\u4e9b', '\u8fd9\u91cc', '\u90a3\u91cc',
        '\u6211\u4eec', '\u4f60\u4eec', '\u4ed6\u4eec', '\u5979\u4eec', '\u5b83\u4eec',
        '\u4e00\u4e2a', '\u4e00\u79cd', '\u4e00\u4e9b', '\u53ef\u4ee5', '\u9700\u8981', '\u5e94\u8be5', '\u53ef\u80fd',
    ]);
    const TERM_MODIFIER_CONNECTOR_SET = new Set(['\u7684', '\u5730', '\u5f97', '\u4e4b']);
    const TERM_MODIFIER_NOUN_PATTERN = /[\u4e00-\u9fffA-Za-z0-9]{1,8}(?:\u7684|\u5730|\u5f97|\u4e4b)[\u4e00-\u9fffA-Za-z0-9]{1,8}/g;
    const SEGMENT_BOUNDARY_REGEX = /^[\s,.;:!?\'\"(){}\[\]<>|\/\\\u3000\u3001\u3002\uff01\uff1f\uff1b\uff1a\u201c\u201d\u2018\u2019\uff08\uff09\u3010\u3011\u300a\u300b\u3008\u3009\u3014\u3015\uff3b\uff3d\uff5b\uff5d]+$/;
    const SEGMENT_WORD_FALLBACK_REGEX = /[A-Za-z0-9_\-\u4e00-\u9fff]+/g;
    const JIEBA_SCRIPT_URLS = Object.freeze([
        '/lib/vendor/jieba.js',
        'https://cdn.jsdelivr.net/npm/jieba-js@1.0.0/jieba.js',
        'https://unpkg.com/jieba-js@1.0.0/jieba.js',
    ]);
    const JIEBA_NOUN_TAGS = new Set(['n', 'nr', 'ns', 'nt', 'nz', 'nl', 'ng', 'vn']);
    const JIEBA_MODIFIER_TAGS = new Set(['a', 'ad', 'an', 'b', 'm', 'mq', 'q', 'r', 'f', 't', 's', 'j', 'l', 'i']);
    const JIEBA_CONNECTOR_WORDS = new Set(['\u7684', '\u5730', '\u5f97', '\u4e4b']);
    const JIEBA_LOOKAHEAD_TOKENS = 4;
    const SELECTION_REFINE_MAX_SOURCE_CHARS = 560;

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
            selectionRefineEndpoint: `${global.location.origin}/api/mobile/cards/selection-refine`,
            selectionRefineEnabled: true,
            selectionRefineTimeoutMs: 4800,
            selectionRefineMinGainChars: 1,
            jiebaEnabled: true,
            jiebaScriptUrls: JIEBA_SCRIPT_URLS.slice(),
            contextChars: 320,
            tearOpenSwipeThresholdPx: TEAR_OPEN_SWIPE_THRESHOLD_PX,
            tearOpenProbeStartPx: TEAR_OPEN_PROBE_START_PX,
            tearOpenSwipeMaxDxPx: TEAR_OPEN_SWIPE_MAX_DX_PX,
            tearOpenSnapRatio: TEAR_OPEN_SNAP_RATIO,
            tearCloseSnapRatio: TEAR_CLOSE_SNAP_RATIO,
            tearCloseMinSwipePx: TEAR_CLOSE_MIN_SWIPE_PX,
            closeByOutsideSwipeOnly: true,
            outsideSwipeCloseEnabled: OUTSIDE_SWIPE_CLOSE_ENABLED,
            closeCardOnWindowScroll: false,
            closeCardOnWindowScrollDeltaPx: WINDOW_SCROLL_CLOSE_DELTA_PX,
            closeCardOnWindowScrollGraceMs: WINDOW_SCROLL_CLOSE_GRACE_MS,
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
            focusedTermNode: null,
            segmenter: createWordSegmenter(config.segmenterLocale),
            advicePrefetches: new Map(),
            singleClickTimer: 0,
            adviceMarkdownEngine: null,
            lastTouchEndAt: 0,
            lastTouchTapAt: 0,
            lastTouchTapX: 0,
            lastTouchTapY: 0,
            lastTouchTapNode: null,
            lastTouchTapPhase: '',
            outsideCloseGesture: null,
            learnedTermKeys: loadLearnedTermKeys(),
            jiebaLoadPromise: null,
            jiebaLoaded: false,
            jiebaDisabled: false,
            manualTermMeta: new WeakMap(),
            selectionRefineSeq: 0,
            selectedArmTarget: null,
            selectedArmType: '',
            selectedArmedAt: 0,
            selectedArmExpiresAt: 0,
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
            syncLearnedTermDecorations(state.container);
        }

        function destroy() {
            resetHighlightRuntime();
            closeActiveCard({ save: false, silent: true }).catch(() => null);
            unbindContainer();
            clearSingleClickTimer();
            clearFocusedTermNode();
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
                state.titles = normalizeTitleCollection(rawTitles, config.maxHighlightTerms);
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
            const normalized = normalizeTitleCollection(rawTitles, config.maxHighlightTerms);
            state.highlightTerms = normalized;
            const engine = ensureHighlightEngine();
            if (engine && typeof engine.setTerms === 'function') {
                engine.setTerms(state.highlightTerms);
            }
        }

        function normalizeTitleCollection(rawTitles, maxCount) {
            const bucket = new Map();
            (Array.isArray(rawTitles) ? rawTitles : []).forEach((item) => {
                const candidate = normalizeTitleCandidate(item);
                if (!candidate || candidate.length < 2) return;
                const key = normalizeTitleLookupKey(candidate);
                if (!key) return;
                const prev = bucket.get(key);
                bucket.set(key, pickPreferredCanonicalTitle(candidate, prev));
            });
            return Array.from(bucket.values())
                .filter(Boolean)
                .sort((a, b) => b.length - a.length)
                .slice(0, Math.max(0, Number(maxCount) || 0));
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
            state.container.addEventListener('dblclick', onContainerDoubleClick, true);
            state.container.addEventListener('touchstart', onTouchStart, { capture: true, passive: false });
            state.container.addEventListener('touchmove', onTouchMove, { capture: true, passive: false });
            state.container.addEventListener('touchend', onTouchEnd, { capture: true, passive: false });
            state.container.addEventListener('touchcancel', onTouchCancel, { capture: true, passive: true });
            document.addEventListener('pointerdown', onDocumentPointerDown, true);
            window.addEventListener('scroll', onWindowScroll, true);
            state.bound = true;
            void ensureJiebaReady();
        }

        function unbindContainer() {
            resetHighlightRuntime();
            clearFocusedTermNode();
            clearSingleClickTimer();
            if (!state.container || !state.bound) {
                state.container = null;
                state.bound = false;
                return;
            }
            state.container.removeEventListener('click', onContainerClick, true);
            state.container.removeEventListener('dblclick', onContainerDoubleClick, true);
            state.container.removeEventListener('touchstart', onTouchStart, true);
            state.container.removeEventListener('touchmove', onTouchMove, true);
            state.container.removeEventListener('touchend', onTouchEnd, true);
            state.container.removeEventListener('touchcancel', onTouchCancel, true);
            document.removeEventListener('pointerdown', onDocumentPointerDown, true);
            window.removeEventListener('scroll', onWindowScroll, true);
            state.container = null;
            state.bound = false;
        }

        function onContainerClick(event) {
            if (event.detail >= 2) {
                clearSingleClickTimer();
                return;
            }
            if (Date.now() - Number(state.lastTouchEndAt || 0) < 420) {
                clearSingleClickTimer();
                return;
            }
            if (Date.now() - state.lastTouchOpenAt < 420) {
                clearSingleClickTimer();
                return;
            }
            const target = event.target;
            const clickX = Number(event.clientX);
            const clickY = Number(event.clientY);
            clearSingleClickTimer();
            state.singleClickTimer = window.setTimeout(() => {
                state.singleClickTimer = 0;
                if (Date.now() - state.lastTouchOpenAt < 420) {
                    return;
                }
                const termNode = resolveOrCreateTermNodeFromPoint(target, clickX, clickY);
                if (!termNode) return;
                focusTermNode(termNode, { selectionType: 'phrase' });
            }, SINGLE_CLICK_COMMIT_DELAY_MS);
        }

        function onContainerDoubleClick(event) {
            clearSingleClickTimer();
            if (Date.now() - Number(state.lastTouchEndAt || 0) < 520) {
                event.preventDefault();
                event.stopPropagation();
                return;
            }
            if (Date.now() - state.lastTouchOpenAt < 420) {
                event.preventDefault();
                event.stopPropagation();
                return;
            }
            const reopenTarget = resolveReopenSelectionTarget(event.target, event.clientX, event.clientY);
            if (reopenTarget) {
                event.preventDefault();
                event.stopPropagation();
                openCardFromSelectionNode(reopenTarget);
                return;
            }
            const sentence = resolveSentenceFromPoint(event.clientX, event.clientY);
            const termNode = ensureTermNodeForPhrase(sentence);
            if (!termNode) return;
            event.preventDefault();
            event.stopPropagation();
            // 首次双击用于选中句子；进入选中后可在 3 秒窗口内“再双击”打开卡片。
            focusTermNode(termNode, { selectionType: 'sentence' });
        }

        function clearSingleClickTimer() {
            if (!state.singleClickTimer) return;
            clearTimeout(state.singleClickTimer);
            state.singleClickTimer = 0;
        }

        function onTouchStart(event) {
            if (!event.touches || event.touches.length !== 1) return;
            const touch = event.touches[0];
            if (beginOutsideCloseGestureIfNeeded(event.target, touch)) {
                clearTouchGesture();
                return;
            }
            if (isTargetInsideActiveCard(event.target)) {
                clearTouchGesture();
                return;
            }
            const directTermNode = resolveTermNode(event.target);
            const pendingPhrase = directTermNode ? null : resolvePhraseFromPoint(touch.clientX, touch.clientY);
            if (!directTermNode && !pendingPhrase) {
                clearTouchGesture();
                return;
            }
            const anchorNode = directTermNode || (pendingPhrase && pendingPhrase.textNode) || null;
            const anchor = resolveAnchorBlock(anchorNode);
            const gesture = {
                termNode: directTermNode || null,
                pendingPhrase,
                anchor,
                startX: touch.clientX,
                startY: touch.clientY,
                startTime: Date.now(),
                isScrolling: false,
                pressTimerId: 0,
                pullDownActive: false,
                pullDownCommitted: false,
            };
            state.touchGesture = gesture;
            if (gesture.termNode) {
                applyPressActive(gesture.termNode);
                gesture.pressTimerId = window.setTimeout(() => {
                    if (state.touchGesture !== gesture) return;
                    removePressActive(gesture.termNode);
                }, STRICT_SHORT_TAP_MAX_DURATION_MS);
            }
        }

        function onTouchMove(event) {
            if (state.outsideCloseGesture && event.touches && event.touches.length === 1) {
                const touch = event.touches[0];
                const outside = state.outsideCloseGesture;
                const dy = touch.clientY - outside.startY;
                if (dy > outside.maxDyDown) {
                    outside.maxDyDown = dy;
                }
                return;
            }
            if (!state.touchGesture || !event.touches || event.touches.length !== 1) return;
            const gesture = state.touchGesture;
            const touch = event.touches[0];
            const dx = Math.abs(touch.clientX - gesture.startX);
            const dyDown = touch.clientY - gesture.startY;
            const delta = resolveTouchTravel(
                gesture.startX,
                gesture.startY,
                touch.clientX,
                touch.clientY
            );
            if (delta <= STRICT_SHORT_TAP_MAX_DISTANCE_PX) {
                return;
            }

            // 兼容保留下拉探测逻辑；当前策略关闭下拉开卡，仅保留“再双击开卡”。
            const touchOnFocused = gesture.termNode
                && state.focusedTermNode
                && gesture.termNode === state.focusedTermNode;
            if (PULL_DOWN_OPEN_ENABLED && touchOnFocused && dyDown > 0 && dx <= PULL_DOWN_MAX_DX_PX) {
                // 在 5px~40px 之间提前锁定触摸事件，阻止浏览器接管滚动
                clearTouchPressTimer(gesture);
                removePressActive(gesture.termNode);
                event.preventDefault();

                if (!gesture.pullDownActive && dyDown >= PULL_DOWN_OPEN_THRESHOLD_PX) {
                    gesture.pullDownActive = true;
                    // 触发撕裂动画反馈
                    if (gesture.anchor) {
                        const progress = Math.min(1, (dyDown - PULL_DOWN_OPEN_THRESHOLD_PX) / (TEAR_OPEN_FULL_PULL_PX || 104));
                        updateOpenProbe(gesture, progress, touch.clientX);
                    }
                    fireHapticTap();
                    return;
                }
                if (gesture.pullDownActive) {
                    // 更新撕裂进度
                    const progress = Math.min(1, (dyDown - PULL_DOWN_OPEN_THRESHOLD_PX) / (TEAR_OPEN_FULL_PULL_PX || 104));
                    updateOpenProbe(gesture, progress, touch.clientX);
                }
                return;
            }

            // 默认滚动行为
            gesture.isScrolling = true;
            clearTouchPressTimer(gesture);
            removePressActive(gesture.termNode);
        }

        function onTouchEnd(event) {
            if (state.outsideCloseGesture) {
                const gesture = state.outsideCloseGesture;
                const touch = event.changedTouches && event.changedTouches.length ? event.changedTouches[0] : null;
                const endX = touch ? touch.clientX : gesture.startX;
                const endY = touch ? touch.clientY : gesture.startY;
                const dx = Math.abs(endX - gesture.startX);
                const dyDown = Math.max(0, endY - gesture.startY);
                const duration = Date.now() - Number(gesture.startTime || 0);
                state.outsideCloseGesture = null;
                state.lastTouchEndAt = Date.now();
                if (dyDown >= OUTSIDE_CLOSE_MIN_SWIPE_PX
                    && dx <= OUTSIDE_CLOSE_MAX_HORIZONTAL_PX
                    && duration <= OUTSIDE_CLOSE_MAX_DURATION_MS) {
                    closeActiveCard({ save: true, fromSwipeSeal: true, silent: true }).catch((error) => {
                        emitNotice(`卡片收起失败：${normalizeError(error)}`, 'error');
                    });
                    event.preventDefault();
                    event.stopPropagation();
                }
                return;
            }
            if (!state.touchGesture) return;
            const gesture = state.touchGesture;
            clearTouchPressTimer(gesture);
            removePressActive(gesture.termNode);
            const touch = event.changedTouches && event.changedTouches.length ? event.changedTouches[0] : null;
            const endX = touch ? touch.clientX : gesture.startX;
            const endY = touch ? touch.clientY : gesture.startY;
            const delta = resolveTouchTravel(gesture.startX, gesture.startY, endX, endY);
            const nowAt = Date.now();
            const duration = nowAt - Number(gesture.startTime || 0);
            // 下拉打开手势完成：松手后打开卡片
            if (PULL_DOWN_OPEN_ENABLED && gesture.pullDownActive) {
                const dyDown = Math.max(0, endY - gesture.startY);
                finalizeOpenProbe(gesture, dyDown >= PULL_DOWN_OPEN_THRESHOLD_PX);
                if (dyDown >= PULL_DOWN_OPEN_THRESHOLD_PX && gesture.termNode) {
                    state.lastTouchOpenAt = Date.now();
                    void triggerSelectionRefineForManualTerm(gesture.termNode);
                    openCard(gesture.termNode.dataset.term, gesture.termNode, {
                        selectionSnippet: gesture.termNode.textContent || '',
                    }).catch((error) => {
                        emitNotice(`Open card failed: ${normalizeError(error)}`, 'error');
                    });
                    event.preventDefault();
                    event.stopPropagation();
                }
                state.lastTouchEndAt = Date.now();
                clearTouchGesture();
                return;
            }

            // 短点仅选词，不打开卡片
            const isShortTap = !gesture.isScrolling
                && delta < STRICT_SHORT_TAP_MAX_DISTANCE_PX
                && duration < STRICT_SHORT_TAP_MAX_DURATION_MS;
            if (isShortTap) {
                const termNode = gesture.termNode || ensureTermNodeForPhrase(gesture.pendingPhrase);
                if (termNode) {
                    gesture.termNode = termNode;
                    const wasDoubleTap = isTouchDoubleTap(termNode, endX, endY, nowAt);
                    const reopenTarget = resolveReopenSelectionTarget(event.target, endX, endY);
                    if (wasDoubleTap) {
                        clearTouchTapMemory();
                        if (reopenTarget && reopenTarget === termNode) {
                            openCardFromSelectionNode(reopenTarget);
                        } else {
                            // 双击第一职责是“选句”，只有“再双击”才触发开卡。
                            const sentence = resolveSentenceFromPoint(endX, endY);
                            const sentenceNode = ensureTermNodeForPhrase(sentence);
                            if (sentenceNode) {
                                focusTermNode(sentenceNode, { selectionType: 'sentence' });
                                void triggerSelectionRefineForManualTerm(sentenceNode);
                            } else {
                                focusTermNode(termNode, { selectionType: 'phrase' });
                                void triggerSelectionRefineForManualTerm(termNode);
                            }
                        }
                    } else if (reopenTarget && reopenTarget === termNode) {
                        focusTermNode(termNode, { selectionType: state.selectedArmType || 'phrase' });
                        void triggerSelectionRefineForManualTerm(termNode);
                        rememberTouchTap(termNode, endX, endY, 'reopen-prime');
                    } else {
                        focusTermNode(termNode, { selectionType: 'phrase' });
                        void triggerSelectionRefineForManualTerm(termNode);
                        rememberTouchTap(termNode, endX, endY, 'phrase-select');
                    }
                } else {
                    clearTouchTapMemory();
                }
                event.preventDefault();
                event.stopPropagation();
            }
            state.lastTouchEndAt = Date.now();
            clearTouchGesture();
        }

        function onTouchCancel() {
            state.lastTouchEndAt = Date.now();
            state.outsideCloseGesture = null;
            clearTouchGesture();
        }

        function clearTouchGesture() {
            const gesture = state.touchGesture;
            if (!gesture) return;
            clearTouchPressTimer(gesture);
            removePressActive(gesture.termNode);
            state.touchGesture = null;
        }

        function clearTouchPressTimer(gesture) {
            if (!gesture || !gesture.pressTimerId) return;
            clearTimeout(gesture.pressTimerId);
            gesture.pressTimerId = 0;
        }

        function applyPressActive(termNode) {
            if (!termNode || !termNode.classList) return;
            termNode.classList.add('is-press-active');
        }

        function removePressActive(termNode) {
            if (!termNode || !termNode.classList) return;
            termNode.classList.remove('is-press-active');
        }

        function resolveTouchTravel(startX, startY, endX, endY) {
            const dx = Number(endX || 0) - Number(startX || 0);
            const dy = Number(endY || 0) - Number(startY || 0);
            return Math.hypot(dx, dy);
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
            clearFocusedTermNode();
            if (!state.activeCard) return;
            if (event && event.pointerType === 'touch') {
                return;
            }
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
            clearFocusedTermNode();
            if (!state.activeCard) return;
            if (config.closeCardOnWindowScroll !== true) return;
            const openedAt = Number(state.activeCard.openedAt) || 0;
            const closeGraceMs = Math.max(120, Number(config.closeCardOnWindowScrollGraceMs) || WINDOW_SCROLL_CLOSE_GRACE_MS);
            if (openedAt > 0 && (Date.now() - openedAt) < closeGraceMs) {
                return;
            }
            const currentY = resolveWindowScrollY();
            const openY = Number(state.activeCard.openScrollY) || 0;
            const closeDelta = Math.max(96, Number(config.closeCardOnWindowScrollDeltaPx) || WINDOW_SCROLL_CLOSE_DELTA_PX);
            if (Math.abs(currentY - openY) < closeDelta) {
                return;
            }
            closeActiveCard({ save: true, silent: true }).catch((error) => {
                emitNotice(`淇濆瓨姒傚康鍗＄墖澶辫触锛?{normalizeError(error)}`, 'error');
            });
        }

        function resolveWindowScrollY() {
            const byWindow = Number(global.scrollY);
            if (Number.isFinite(byWindow)) return byWindow;
            const byPage = Number(global.pageYOffset);
            if (Number.isFinite(byPage)) return byPage;
            const root = document.documentElement;
            const top = root ? Number(root.scrollTop) : 0;
            return Number.isFinite(top) ? top : 0;
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

        function resolveTermNode(target) {
            if (!target || !target.closest || !state.container) return null;
            const node = target.closest('.concept-term');
            if (!node || !state.container.contains(node)) return null;
            return node;
        }

        function resolveOrCreateTermNodeFromPoint(target, clientX, clientY) {
            const existing = resolveTermNode(target);
            if (existing) {
                void triggerSelectionRefineForManualTerm(existing);
                return existing;
            }
            const phrase = resolvePhraseFromPoint(clientX, clientY);
            const created = ensureTermNodeForPhrase(phrase);
            void triggerSelectionRefineForManualTerm(created);
            return created;
        }

        function beginOutsideCloseGestureIfNeeded(target, touch) {
            if (config.outsideSwipeCloseEnabled !== true) return false;
            if (!state.activeCard) return false;
            if (isTargetInsideActiveCard(target)) return false;
            if (resolveTermNode(target)) return false;
            state.outsideCloseGesture = {
                startX: touch.clientX,
                startY: touch.clientY,
                startTime: Date.now(),
                maxDyDown: 0,
            };
            return true;
        }

        function isTargetInsideActiveCard(target) {
            if (!state.activeCard || !target) return false;
            const activeRoot = state.activeCard.root;
            if (activeRoot && typeof activeRoot.contains === 'function' && activeRoot.contains(target)) {
                return true;
            }
            const sceneWrapper = state.activeCard.tearScene && state.activeCard.tearScene.wrapper;
            if (sceneWrapper && typeof sceneWrapper.contains === 'function' && sceneWrapper.contains(target)) {
                return true;
            }
            return false;
        }

        function clearTouchTapMemory() {
            state.lastTouchTapAt = 0;
            state.lastTouchTapX = 0;
            state.lastTouchTapY = 0;
            state.lastTouchTapNode = null;
            state.lastTouchTapPhase = '';
        }

        function rememberTouchTap(node, clientX, clientY, phase) {
            state.lastTouchTapAt = Date.now();
            state.lastTouchTapX = Number(clientX) || 0;
            state.lastTouchTapY = Number(clientY) || 0;
            state.lastTouchTapNode = node || null;
            state.lastTouchTapPhase = String(phase || '');
        }

        function clearSelectionReopenArm() {
            state.selectedArmTarget = null;
            state.selectedArmType = '';
            state.selectedArmedAt = 0;
            state.selectedArmExpiresAt = 0;
        }

        function armSelectionForReopen(node, selectionType) {
            if (!node || !node.classList || !state.container || !state.container.contains(node)) {
                clearSelectionReopenArm();
                return;
            }
            const now = Date.now();
            state.selectedArmTarget = node;
            state.selectedArmType = selectionType === 'sentence' ? 'sentence' : 'phrase';
            state.selectedArmedAt = now;
            state.selectedArmExpiresAt = now + REOPEN_SELECTED_DOUBLE_TAP_ARM_WINDOW_MS;
        }

        function resolveActiveSelectionArmTarget() {
            const node = state.selectedArmTarget;
            if (!node || !node.classList || !state.container || !state.container.contains(node)) {
                clearSelectionReopenArm();
                return null;
            }
            const expiresAt = Number(state.selectedArmExpiresAt || 0);
            if (!expiresAt || Date.now() > expiresAt) {
                clearSelectionReopenArm();
                return null;
            }
            return node;
        }

        function isTargetInsideNode(target, node) {
            if (!node || !target) return false;
            if (node === target) return true;
            if (typeof node.contains === 'function') {
                return node.contains(target);
            }
            return false;
        }

        function isPointerInsideNode(node, target, clientX, clientY) {
            if (!node) return false;
            if (isTargetInsideNode(target, node)) {
                return true;
            }
            const x = Number(clientX);
            const y = Number(clientY);
            if (!Number.isFinite(x) || !Number.isFinite(y) || !document || typeof document.elementFromPoint !== 'function') {
                return false;
            }
            const el = document.elementFromPoint(x, y);
            return isTargetInsideNode(el, node);
        }

        function resolveReopenSelectionTarget(target, clientX, clientY) {
            const armedNode = resolveActiveSelectionArmTarget();
            if (!armedNode) return null;
            if (!state.focusedTermNode || state.focusedTermNode !== armedNode) {
                return null;
            }
            if (!isPointerInsideNode(armedNode, target, clientX, clientY)) {
                return null;
            }
            return armedNode;
        }

        function isTouchDoubleTap(node, endX, endY, nowAt) {
            const lastAt = Number(state.lastTouchTapAt || 0);
            if (!lastAt || (nowAt - lastAt) > TOUCH_SELECTION_DOUBLE_TAP_WINDOW_MS) {
                return false;
            }
            const dx = (Number(endX) || 0) - (Number(state.lastTouchTapX) || 0);
            const dy = (Number(endY) || 0) - (Number(state.lastTouchTapY) || 0);
            if (Math.hypot(dx, dy) > TOUCH_SELECTION_DOUBLE_TAP_MAX_DISTANCE_PX) {
                return false;
            }
            const prevNode = state.lastTouchTapNode;
            if (!prevNode || !node || !prevNode.classList || !node.classList) {
                return false;
            }
            if (prevNode === node) {
                return true;
            }
            return isTargetInsideNode(prevNode, node) || isTargetInsideNode(node, prevNode);
        }

        function openCardFromSelectionNode(node) {
            if (!node || !node.classList) return;
            state.lastTouchOpenAt = Date.now();
            clearSelectionReopenArm();
            clearTouchTapMemory();
            void triggerSelectionRefineForManualTerm(node);
            openCard(node.dataset.term, node, {
                selectionSnippet: node.textContent || '',
            }).catch((error) => {
                emitNotice(`Open card failed: ${normalizeError(error)}`, 'error');
            });
        }

        function focusTermNode(node, options = {}) {
            if (!node || !node.classList || !state.container || !state.container.contains(node)) return;
            if (state.focusedTermNode && state.focusedTermNode !== node && state.focusedTermNode.classList) {
                state.focusedTermNode.classList.remove('is-selected');
            }
            state.focusedTermNode = node;
            state.focusedTermNode.classList.add('is-selected');
            if (options.armReopen !== false) {
                armSelectionForReopen(node, options.selectionType);
            }
        }

        function clearFocusedTermNode() {
            if (state.focusedTermNode && state.focusedTermNode.classList) {
                state.focusedTermNode.classList.remove('is-selected');
            }
            state.focusedTermNode = null;
            clearSelectionReopenArm();
            clearTouchTapMemory();
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
            const safeTerm = normalizeCardTerm(phrase.term || exactTerm);
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
            registerManualTermMeta(span, {
                sourceText: source,
                startOffset: start,
                endOffset: end,
                cursorOffset: Math.max(0, Math.min(source.length, Number(phrase.cursorOffset) || start)),
            });
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
                sourceText: source,
                cursorOffset: Math.max(0, Math.min(source.length, Number(caret.offset) || 0)),
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
            const jiebaMatched = resolveTermByJiebaPos(text, offset);
            if (jiebaMatched && jiebaMatched.term) return jiebaMatched;
            if (config.jiebaEnabled !== false && !state.jiebaLoaded && !state.jiebaDisabled) {
                void ensureJiebaReady();
            }
            const dictionaryMatched = resolveTermByHighlightDictionary(text, offset);
            if (dictionaryMatched && dictionaryMatched.term) return dictionaryMatched;
            const modifierNounMatched = resolveModifierNounByRegex(text, offset);
            if (modifierNounMatched && modifierNounMatched.term) return modifierNounMatched;
            const intlMatched = state.segmenter ? resolveTermBySegmenter(text, offset) : null;
            if (intlMatched && intlMatched.term) return intlMatched;
            return resolveTermByRegex(text, offset);
        }

        function registerManualTermMeta(termNode, meta) {
            if (!termNode || !meta || !state.manualTermMeta) return;
            const sourceText = String(meta.sourceText || '');
            if (!sourceText) return;
            const startOffset = Math.max(0, Math.min(sourceText.length, Number(meta.startOffset) || 0));
            const endOffset = Math.max(startOffset, Math.min(sourceText.length, Number(meta.endOffset) || 0));
            if (endOffset <= startOffset) return;
            const cursorOffset = Math.max(startOffset, Math.min(endOffset, Number(meta.cursorOffset) || startOffset));
            state.manualTermMeta.set(termNode, {
                sourceText,
                startOffset,
                endOffset,
                cursorOffset,
                pendingSeq: 0,
                pendingAt: 0,
                lastRequestedAt: 0,
            });
        }

        function resolveManualTermMeta(termNode) {
            if (!termNode || !state.manualTermMeta) return null;
            return state.manualTermMeta.get(termNode) || null;
        }

        function triggerSelectionRefineForManualTerm(termNode) {
            if (!termNode || !termNode.classList || !termNode.classList.contains('concept-manual-term')) return;
            const meta = resolveManualTermMeta(termNode);
            if (!meta) return;
            const now = Date.now();
            const pendingSeq = Number(meta.pendingSeq || 0);
            const pendingAt = Number(meta.pendingAt || 0);
            if (pendingSeq > 0 && (now - pendingAt) < 9000) {
                return;
            }
            const lastRequestedAt = Number(meta.lastRequestedAt || 0);
            if ((now - lastRequestedAt) < 1200) {
                return;
            }
            meta.lastRequestedAt = now;
            meta.pendingAt = now;
            state.manualTermMeta.set(termNode, meta);
            void requestSelectionRefineForTermNode(termNode);
        }

        async function requestSelectionRefineForTermNode(termNode) {
            if (!termNode || !termNode.classList || !termNode.classList.contains('concept-manual-term')) return;
            if (config.selectionRefineEnabled === false) return;
            const endpoint = String(config.selectionRefineEndpoint || '').trim();
            if (!endpoint) return;
            const meta = resolveManualTermMeta(termNode);
            if (!meta || !meta.sourceText) return;
            const payloadWindow = buildSelectionRefinePayloadWindow(meta);
            if (!payloadWindow || !payloadWindow.sourceText) return;

            const seq = Number(state.selectionRefineSeq || 0) + 1;
            state.selectionRefineSeq = seq;
            meta.pendingSeq = seq;
            meta.pendingAt = Date.now();
            state.manualTermMeta.set(termNode, meta);

            const timeoutMs = Math.max(1200, Number(config.selectionRefineTimeoutMs) || 4800);
            const abortController = typeof AbortController === 'function' ? new AbortController() : null;
            let timeoutId = 0;
            if (abortController) {
                timeoutId = window.setTimeout(() => abortController.abort(), timeoutMs);
            }

            try {
                const response = await fetch(endpoint, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        sourceText: payloadWindow.sourceText,
                        cursorOffset: payloadWindow.cursorOffset,
                        currentTerm: payloadWindow.currentTerm,
                        currentStartOffset: payloadWindow.currentStartOffset,
                        currentEndOffset: payloadWindow.currentEndOffset,
                    }),
                    signal: abortController ? abortController.signal : undefined,
                });
                if (!response.ok) return;

                const payload = await response.json();
                if (!payload || payload.improved !== true) return;

                const latestMeta = resolveManualTermMeta(termNode);
                if (!latestMeta || Number(latestMeta.pendingSeq || 0) !== seq) return;

                const rawStart = Number(payload.startOffset);
                const rawEnd = Number(payload.endOffset);
                if (!Number.isFinite(rawStart) || !Number.isFinite(rawEnd)) return;

                const nextStart = Math.max(
                    0,
                    Math.min(latestMeta.sourceText.length, payloadWindow.windowStart + Math.floor(rawStart))
                );
                const nextEnd = Math.max(
                    nextStart,
                    Math.min(latestMeta.sourceText.length, payloadWindow.windowStart + Math.floor(rawEnd))
                );
                if (!isSelectionRefineUpgrade(latestMeta, nextStart, nextEnd)) return;
                if (latestMeta.cursorOffset < nextStart || latestMeta.cursorOffset > nextEnd) return;

                applyRefinedSelectionRange(
                    termNode,
                    latestMeta,
                    nextStart,
                    nextEnd,
                    String(payload.term || '')
                );
            } catch (_error) {
                // 静默纠偏失败时不打断即时选词体验。
            } finally {
                if (timeoutId) {
                    clearTimeout(timeoutId);
                }
                const latestMeta = resolveManualTermMeta(termNode);
                if (latestMeta && Number(latestMeta.pendingSeq || 0) === seq) {
                    latestMeta.pendingSeq = 0;
                    latestMeta.pendingAt = 0;
                    state.manualTermMeta.set(termNode, latestMeta);
                }
            }
        }
        function buildSelectionRefinePayloadWindow(meta) {
            if (!meta || !meta.sourceText) return null;
            const sourceText = String(meta.sourceText || '');
            if (!sourceText) return null;
            const sourceLen = sourceText.length;
            const currentStart = Math.max(0, Math.min(sourceLen, Number(meta.startOffset) || 0));
            const currentEnd = Math.max(currentStart, Math.min(sourceLen, Number(meta.endOffset) || 0));
            const cursorOffset = Math.max(currentStart, Math.min(currentEnd, Number(meta.cursorOffset) || currentStart));
            const currentTerm = sourceText.slice(currentStart, currentEnd);
            const maxWindowChars = Math.max(160, Math.min(SELECTION_REFINE_MAX_SOURCE_CHARS, sourceLen));
            if (sourceLen <= maxWindowChars) {
                return {
                    sourceText,
                    windowStart: 0,
                    cursorOffset,
                    currentTerm,
                    currentStartOffset: currentStart,
                    currentEndOffset: currentEnd,
                };
            }
            const focusCenter = Math.floor((currentStart + currentEnd + cursorOffset) / 3);
            let windowStart = Math.max(0, focusCenter - Math.floor(maxWindowChars / 2));
            let windowEnd = Math.min(sourceLen, windowStart + maxWindowChars);
            if (windowEnd - windowStart < maxWindowChars) {
                windowStart = Math.max(0, windowEnd - maxWindowChars);
            }
            if (currentStart < windowStart) {
                windowStart = currentStart;
                windowEnd = Math.min(sourceLen, windowStart + maxWindowChars);
            }
            if (currentEnd > windowEnd) {
                windowEnd = currentEnd;
                windowStart = Math.max(0, windowEnd - maxWindowChars);
            }
            return {
                sourceText: sourceText.slice(windowStart, windowEnd),
                windowStart,
                cursorOffset: cursorOffset - windowStart,
                currentTerm,
                currentStartOffset: currentStart - windowStart,
                currentEndOffset: currentEnd - windowStart,
            };
        }

        function isSelectionRefineUpgrade(meta, nextStart, nextEnd) {
            if (!meta) return false;
            const currentStart = Math.max(0, Number(meta.startOffset) || 0);
            const currentEnd = Math.max(currentStart, Number(meta.endOffset) || 0);
            const currentLen = currentEnd - currentStart;
            const nextLen = Math.max(0, Number(nextEnd) - Number(nextStart));
            if (nextLen <= 0) return false;
            const minGain = Math.max(0, Number(config.selectionRefineMinGainChars) || 1);
            const boundaryExpanded = nextStart < currentStart || nextEnd > currentEnd;
            if (!boundaryExpanded) return false;
            if ((nextLen - currentLen) >= minGain) return true;
            return (nextStart < currentStart && nextEnd >= currentEnd)
                || (nextStart <= currentStart && nextEnd > currentEnd);
        }

        function applyRefinedSelectionRange(termNode, meta, nextStart, nextEnd, refinedTermText) {
            if (!termNode || !meta || !meta.sourceText || !termNode.parentNode) return false;
            const sourceText = String(meta.sourceText || '');
            const sourceLen = sourceText.length;
            const currentStart = Math.max(0, Math.min(sourceLen, Number(meta.startOffset) || 0));
            const currentEnd = Math.max(currentStart, Math.min(sourceLen, Number(meta.endOffset) || 0));
            const safeStart = Math.max(0, Math.min(sourceLen, Number(nextStart) || 0));
            const safeEnd = Math.max(safeStart, Math.min(sourceLen, Number(nextEnd) || 0));
            if (safeEnd <= safeStart) return false;

            const beforeNode = termNode.previousSibling && termNode.previousSibling.nodeType === Node.TEXT_NODE
                ? termNode.previousSibling
                : null;
            const afterNode = termNode.nextSibling && termNode.nextSibling.nodeType === Node.TEXT_NODE
                ? termNode.nextSibling
                : null;
            const expectedBefore = sourceText.slice(0, currentStart);
            const expectedMiddle = sourceText.slice(currentStart, currentEnd);
            const expectedAfter = sourceText.slice(currentEnd);
            const currentBefore = beforeNode ? String(beforeNode.nodeValue || '') : '';
            const currentMiddle = String(termNode.textContent || '');
            const currentAfter = afterNode ? String(afterNode.nodeValue || '') : '';
            if (currentBefore !== expectedBefore || currentMiddle !== expectedMiddle || currentAfter !== expectedAfter) {
                return false;
            }

            const nextBefore = sourceText.slice(0, safeStart);
            const nextMiddle = sourceText.slice(safeStart, safeEnd);
            const nextAfter = sourceText.slice(safeEnd);
            if (!nextMiddle.trim()) return false;

            const parent = termNode.parentNode;
            if (nextBefore) {
                if (beforeNode) {
                    beforeNode.nodeValue = nextBefore;
                } else {
                    parent.insertBefore(document.createTextNode(nextBefore), termNode);
                }
            } else if (beforeNode) {
                beforeNode.remove();
            }

            termNode.textContent = nextMiddle;
            const fallbackTerm = normalizeCardTerm(nextMiddle);
            const refinedTerm = normalizeCardTerm(refinedTermText);
            termNode.dataset.term = refinedTerm || fallbackTerm || normalizeCardTerm(termNode.dataset.term || '');

            const latestAfterNode = termNode.nextSibling && termNode.nextSibling.nodeType === Node.TEXT_NODE
                ? termNode.nextSibling
                : null;
            if (nextAfter) {
                if (latestAfterNode) {
                    latestAfterNode.nodeValue = nextAfter;
                } else {
                    parent.insertBefore(document.createTextNode(nextAfter), termNode.nextSibling);
                }
            } else if (latestAfterNode) {
                latestAfterNode.remove();
            }

            const nextMeta = {
                sourceText,
                startOffset: safeStart,
                endOffset: safeEnd,
                cursorOffset: Math.max(safeStart, Math.min(safeEnd, Number(meta.cursorOffset) || safeStart)),
                pendingSeq: 0,
            };
            state.manualTermMeta.set(termNode, nextMeta);
            if (state.focusedTermNode === termNode) {
                termNode.classList.add('is-selected');
            }
            return true;
        }

        function resolveTermByJiebaPos(source, caretOffset) {
            if (config.jiebaEnabled === false || state.jiebaDisabled) return null;
            const jiebaApi = resolveJiebaApi();
            if (!jiebaApi || typeof jiebaApi.tag !== 'function') return null;
            let tagged = null;
            try {
                tagged = jiebaApi.tag(String(source || ''));
            } catch (_error) {
                return null;
            }
            const tokens = normalizeJiebaTokens(tagged, source);
            if (!tokens.length) return null;
            const offset = Math.max(0, Math.min(String(source || '').length, Number(caretOffset) || 0));
            let anchorIndex = tokens.findIndex((token) => offset >= token.start && offset < token.end);
            if (anchorIndex < 0 && offset === String(source || '').length) {
                anchorIndex = tokens.length - 1;
            }
            if (anchorIndex < 0) {
                anchorIndex = findNearestJiebaToken(tokens, offset);
            }
            if (anchorIndex < 0) return null;

            const anchor = tokens[anchorIndex];
            const nounIndex = resolveJiebaAnchorNoun(tokens, anchorIndex, anchor);
            if (nounIndex < 0) return null;
            let left = nounIndex;
            for (let i = nounIndex - 1; i >= 0; i -= 1) {
                if (isJiebaModifierToken(tokens[i]) || isJiebaConnectorToken(tokens[i])) {
                    left = i;
                    continue;
                }
                break;
            }
            let right = nounIndex;
            for (let i = nounIndex + 1; i < tokens.length; i += 1) {
                const token = tokens[i];
                if (isJiebaNounToken(token)) {
                    right = i;
                    continue;
                }
                if (isJiebaConnectorToken(token) && i + 1 < tokens.length && isJiebaNounToken(tokens[i + 1])) {
                    right = i + 1;
                    i += 1;
                    continue;
                }
                break;
            }
            while (left < right && isJiebaConnectorToken(tokens[left])) {
                left += 1;
            }
            while (right > left && isJiebaConnectorToken(tokens[right])) {
                right -= 1;
            }
            if (right < left) return null;

            const start = tokens[left].start;
            const end = tokens[right].end;
            if (offset < start || offset > end) return null;
            const clamped = clampRegexFallbackSpan(source, start, end, offset);
            const term = normalizeSelectionTerm(String(source || '').slice(clamped.start, clamped.end));
            if (!term || TERM_STOPWORD_SET.has(term)) return null;
            return { term, start: clamped.start, end: clamped.end };
        }

        function resolveJiebaAnchorNoun(tokens, anchorIndex, anchorToken) {
            if (isJiebaNounToken(anchorToken)) return anchorIndex;
            if (isJiebaModifierToken(anchorToken) || isJiebaConnectorToken(anchorToken)) {
                for (let i = anchorIndex + 1; i < tokens.length && (i - anchorIndex) <= JIEBA_LOOKAHEAD_TOKENS; i += 1) {
                    if (isJiebaNounToken(tokens[i])) return i;
                    if (!isJiebaModifierToken(tokens[i]) && !isJiebaConnectorToken(tokens[i])) break;
                }
                for (let i = anchorIndex - 1; i >= 0 && (anchorIndex - i) <= 2; i -= 1) {
                    if (isJiebaNounToken(tokens[i])) return i;
                    if (!isJiebaModifierToken(tokens[i]) && !isJiebaConnectorToken(tokens[i])) break;
                }
            }
            return -1;
        }

        function normalizeJiebaTokens(tagged, source) {
            if (!Array.isArray(tagged) || !tagged.length) return [];
            const text = String(source || '');
            const tokens = [];
            let cursor = 0;
            for (let i = 0; i < tagged.length; i += 1) {
                const normalized = normalizeSingleJiebaToken(tagged[i]);
                if (!normalized || !normalized.word) continue;
                const word = normalized.word;
                let start = cursor;
                const matchedAt = text.indexOf(word, cursor);
                if (matchedAt >= 0) {
                    start = matchedAt;
                }
                const end = Math.max(start, Math.min(text.length, start + word.length));
                if (end <= start) continue;
                tokens.push({
                    word,
                    tag: normalized.tag,
                    start,
                    end,
                });
                cursor = end;
                if (cursor >= text.length) {
                    break;
                }
            }
            return tokens;
        }

        function normalizeSingleJiebaToken(rawToken) {
            if (!rawToken) return null;
            if (Array.isArray(rawToken)) {
                const word = String(rawToken[0] || '');
                const tag = String(rawToken[1] || '').toLowerCase();
                return word ? { word, tag } : null;
            }
            if (typeof rawToken === 'object') {
                const word = String(rawToken.word || rawToken.term || rawToken.text || '');
                const tag = String(rawToken.tag || rawToken.pos || rawToken.flag || '').toLowerCase();
                return word ? { word, tag } : null;
            }
            return null;
        }

        function findNearestJiebaToken(tokens, offset) {
            if (!Array.isArray(tokens) || !tokens.length) return -1;
            let bestIndex = -1;
            let bestDistance = Number.POSITIVE_INFINITY;
            tokens.forEach((token, index) => {
                const center = token.start + ((token.end - token.start) / 2);
                const distance = Math.abs(center - offset);
                if (distance < bestDistance) {
                    bestDistance = distance;
                    bestIndex = index;
                }
            });
            return bestIndex;
        }

        function isJiebaNounToken(token) {
            if (!token) return false;
            const tag = String(token.tag || '').toLowerCase();
            if (!tag) return false;
            return JIEBA_NOUN_TAGS.has(tag) || tag.startsWith('n');
        }

        function isJiebaModifierToken(token) {
            if (!token) return false;
            const tag = String(token.tag || '').toLowerCase();
            if (!tag) return false;
            if (JIEBA_MODIFIER_TAGS.has(tag)) return true;
            return tag.startsWith('a') || tag.startsWith('m') || tag.startsWith('q') || tag === 'uj';
        }

        function isJiebaConnectorToken(token) {
            if (!token) return false;
            const word = String(token.word || '');
            return JIEBA_CONNECTOR_WORDS.has(word);
        }

        function resolveJiebaApi() {
            if (global.jieba && typeof global.jieba === 'object') {
                return global.jieba;
            }
            return null;
        }

        async function ensureJiebaReady() {
            if (config.jiebaEnabled === false || state.jiebaDisabled) return null;
            const directApi = resolveJiebaApi();
            if (directApi && typeof directApi.tag === 'function') {
                state.jiebaLoaded = true;
                return directApi;
            }
            if (state.jiebaLoadPromise) {
                return state.jiebaLoadPromise;
            }
            state.jiebaLoadPromise = (async () => {
                try {
                    const candidates = normalizeJiebaScriptUrls(config.jiebaScriptUrls);
                    for (let i = 0; i < candidates.length; i += 1) {
                        await loadScriptByUrl(candidates[i]);
                        const api = resolveJiebaApi();
                        if (!api) continue;
                        if (typeof api.load === 'function') {
                            await Promise.resolve(api.load());
                        }
                        if (typeof api.tag === 'function') {
                            state.jiebaLoaded = true;
                            return api;
                        }
                    }
                    state.jiebaDisabled = true;
                    return null;
                } catch (_error) {
                    state.jiebaDisabled = true;
                    return null;
                } finally {
                    state.jiebaLoadPromise = null;
                }
            })();
            return state.jiebaLoadPromise;
        }

        function normalizeJiebaScriptUrls(rawUrls) {
            const list = Array.isArray(rawUrls) ? rawUrls : [];
            const normalized = list
                .map((item) => String(item || '').trim())
                .filter(Boolean);
            if (normalized.length) return normalized;
            return JIEBA_SCRIPT_URLS.slice();
        }

        function loadScriptByUrl(url) {
            const src = String(url || '').trim();
            if (!src) return Promise.reject(new Error('empty script url'));
            const marker = encodeURIComponent(src);
            const existing = document.querySelector(`script[data-jieba-script="${marker}"]`);
            if (existing) {
                return Promise.resolve(existing);
            }
            return new Promise((resolve, reject) => {
                const script = document.createElement('script');
                script.async = true;
                script.src = src;
                script.setAttribute('data-jieba-script', marker);
                script.onload = () => resolve(script);
                script.onerror = () => reject(new Error(`failed to load script: ${src}`));
                document.head.appendChild(script);
            });
        }

        function resolveModifierNounByRegex(source, caretOffset) {
            const text = String(source || '');
            if (!text.trim()) return null;
            const offset = Math.max(0, Math.min(text.length, Number(caretOffset) || 0));
            TERM_MODIFIER_NOUN_PATTERN.lastIndex = 0;
            let best = null;
            let match = null;
            while ((match = TERM_MODIFIER_NOUN_PATTERN.exec(text))) {
                const raw = String(match[0] || '');
                if (!raw) continue;
                const start = match.index;
                const end = start + raw.length;
                if (offset < start || offset > end) continue;
                const term = normalizeSelectionTerm(raw);
                if (!term) continue;
                const connectorCount = countModifierConnectors(term);
                const spanCenter = start + ((end - start) / 2);
                const score = (term.length * 4) - Math.abs(offset - spanCenter) - (Math.max(0, connectorCount - 1) * 3);
                if (!best || score > best.score || (score === best.score && term.length > best.term.length)) {
                    best = { term, start, end, score };
                }
            }
            if (!best) return null;
            return { term: best.term, start: best.start, end: best.end };
        }

        function resolveTermByHighlightDictionary(source, caretOffset) {
            const text = String(source || '');
            if (!text.trim()) return null;
            const terms = Array.isArray(state.highlightTerms) ? state.highlightTerms : [];
            if (!terms.length) return null;
            const offset = Math.max(0, Math.min(text.length, Number(caretOffset) || 0));
            const scanStart = Math.max(0, offset - TERM_DICTIONARY_MATCH_RADIUS);
            const scanEnd = Math.min(text.length, offset + TERM_DICTIONARY_MATCH_RADIUS);
            const scanText = text.slice(scanStart, scanEnd);
            if (!scanText) return null;
            const maxTerms = Math.min(TERM_DICTIONARY_MATCH_LIMIT, terms.length);
            let best = null;
            for (let i = 0; i < maxTerms; i += 1) {
                const raw = String(terms[i] || '').trim();
                if (!raw || raw.length < 2 || raw.length > TERM_WINDOW_MAX_CHARS) continue;
                let localIndex = scanText.indexOf(raw);
                while (localIndex !== -1) {
                    const start = scanStart + localIndex;
                    const end = start + raw.length;
                    if (offset >= start && offset <= end) {
                        const term = normalizeSelectionTerm(raw);
                        if (term) {
                            const center = start + ((end - start) / 2);
                            const score = (term.length * 4) - Math.abs(offset - center);
                            if (!best || score > best.score || (score === best.score && term.length > best.term.length)) {
                                best = { term, start, end, score };
                            }
                        }
                    }
                    localIndex = scanText.indexOf(raw, localIndex + 1);
                }
            }
            if (!best) return null;
            return { term: best.term, start: best.start, end: best.end };
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
            const best = resolveBestSegmentWindow(source, segments, center, targetOffset);
            if (!best) return null;
            return best;
        }

        function resolveBestSegmentWindow(source, segments, centerIndex, targetOffset) {
            if (!Array.isArray(segments) || !segments.length) return null;
            const maxTokens = Math.max(1, Math.min(TERM_WINDOW_MAX_TOKENS, SEGMENTER_MAX_TOKENS));
            let best = null;
            for (let left = centerIndex; left >= 0 && (centerIndex - left + 1) <= maxTokens; left -= 1) {
                if (!canUseSegment(segments[left])) break;
                for (let right = centerIndex; right < segments.length && (right - left + 1) <= maxTokens; right += 1) {
                    if (!canUseSegment(segments[right])) break;
                    const spanChars = segments[right].end - segments[left].start;
                    if (spanChars > TERM_WINDOW_MAX_CHARS || spanChars > SEGMENTER_MAX_CHARS) break;
                    const start = segments[left].start;
                    const end = segments[right].end;
                    if (targetOffset < start || targetOffset > end) continue;
                    const term = normalizeSelectionTerm(source.slice(start, end));
                    if (!term) continue;
                    const score = scoreSegmentWindow(source, segments, left, right, centerIndex, targetOffset, term);
                    if (!best || score > best.score || (score === best.score && term.length > best.term.length)) {
                        best = { term, start, end, score };
                    }
                }
            }
            if (!best) return null;
            return { term: best.term, start: best.start, end: best.end };
        }

        function scoreSegmentWindow(source, segments, left, right, centerIndex, targetOffset, term) {
            const tokenCount = right - left + 1;
            const termLength = term.length;
            let score = 0;
            if (tokenCount === 1) {
                score += 2;
            } else if (tokenCount <= 4) {
                score += 9;
            } else {
                score += 6;
            }
            const hasCjk = /[\u4e00-\u9fff]/.test(term);
            if (hasCjk && tokenCount >= 2 && tokenCount <= 3) {
                score += 5;
            }
            if (termLength >= 2 && termLength <= 12) {
                score += 8;
            } else if (termLength <= 18) {
                score += 4;
            } else {
                score -= 4;
            }
            if (hasCjk) {
                score += 3;
            }
            if (/[\u4e00-\u9fff](?:\u7684|\u5730|\u5f97)[\u4e00-\u9fff]/.test(term)) {
                score += 2;
            }
            const connectorCount = countModifierConnectors(term);
            if (connectorCount > 0) {
                score += 2;
                if (connectorCount > 1) {
                    score -= (connectorCount - 1) * 4;
                }
            }
            if (TERM_STOPWORD_SET.has(term)) {
                score -= 10;
            }
            const leadChar = term.charAt(0);
            const tailChar = term.charAt(term.length - 1);
            if (TERM_EDGE_PARTICLE_SET.has(leadChar)) {
                score -= 5;
            }
            if (TERM_EDGE_PARTICLE_SET.has(tailChar)) {
                score -= 5;
            }
            if (TERM_MODIFIER_CONNECTOR_SET.has(leadChar)) {
                score -= 7;
            }
            if (TERM_MODIFIER_CONNECTOR_SET.has(tailChar)) {
                score -= 7;
            }
            if (Array.isArray(state.highlightTerms) && state.highlightTerms.includes(term)) {
                score += 14;
            }
            const spanCenter = segments[left].start + ((segments[right].end - segments[left].start) / 2);
            const centerDistancePenalty = Math.abs(targetOffset - spanCenter) / Math.max(1, termLength);
            score -= centerDistancePenalty;
            if (centerIndex >= left && centerIndex <= right) {
                score += 1;
            }
            return score;
        }

        function countModifierConnectors(term) {
            const text = String(term || '');
            if (!text) return 0;
            let count = 0;
            for (let i = 0; i < text.length; i += 1) {
                if (TERM_MODIFIER_CONNECTOR_SET.has(text.charAt(i))) {
                    count += 1;
                }
            }
            return count;
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
                    const span = clampRegexFallbackSpan(text, start, end, caretOffset);
                    const normalized = normalizeSelectionTerm(text.slice(span.start, span.end));
                    if (!normalized) return null;
                    return { term: normalized, start: span.start, end: span.end };
                }
            }
            return null;
        }

        function clampRegexFallbackSpan(source, start, end, caretOffset) {
            const rawStart = Math.max(0, Number(start) || 0);
            const rawEnd = Math.max(rawStart, Number(end) || 0);
            if ((rawEnd - rawStart) <= TERM_WINDOW_MAX_CHARS) {
                return { start: rawStart, end: rawEnd };
            }
            const text = String(source || '');
            const offset = Math.max(rawStart, Math.min(rawEnd, Number(caretOffset) || rawStart));
            const leftRoom = Math.floor(TERM_WINDOW_MAX_CHARS * 0.6);
            let nextStart = Math.max(rawStart, offset - leftRoom);
            let nextEnd = Math.min(rawEnd, nextStart + TERM_WINDOW_MAX_CHARS);
            if ((nextEnd - nextStart) < TERM_WINDOW_MAX_CHARS) {
                nextStart = Math.max(rawStart, nextEnd - TERM_WINDOW_MAX_CHARS);
            }
            while (nextStart > rawStart) {
                const prev = text.charAt(nextStart - 1);
                if (!/[A-Za-z0-9_\-\u4e00-\u9fff]/.test(prev)) break;
                nextStart -= 1;
            }
            while (nextEnd < rawEnd) {
                const ch = text.charAt(nextEnd);
                if (!/[A-Za-z0-9_\-\u4e00-\u9fff]/.test(ch)) break;
                nextEnd += 1;
            }
            return { start: nextStart, end: nextEnd };
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
            const safeTerm = normalizeCardTerm(term);
            if (!safeTerm || !state.container) return;
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
                syncCardOpenHeight(state.activeCard);
                scheduleCardViewportAlignment(state.activeCard);
            });

            const activeCard = {
                term: safeTerm,
                root: cardRoot,
                anchor,
                tearScene,
                openScrollY: resolveWindowScrollY(),
                openedAt: Date.now(),
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
                cachedAdvice: '',
                insideTapProbe: null,
                lastInsideTap: null,
                doubleTapClosePending: false,
            };
            state.activeCard = activeCard;
            wireCardEvents(activeCard);
            rememberOpenedTerm(safeTerm);
            syncCardOpenHeight(activeCard);
            scheduleCardViewportAlignment(activeCard);
            if (tearScene && tearScene.wrapper) {
                tearScene.wrapper.classList.add('concept-anchor-active');
            } else {
                anchor.classList.add('concept-anchor-active');
            }

            const existing = await loadCard(safeTerm);
            if (existing.exists && existing.markdown) {
                const sanitizedExistingMarkdown = sanitizeCardMarkdown(safeTerm, existing.markdown);
                activeCard.textarea.value = sanitizedExistingMarkdown;
                activeCard.cachedAdvice = extractAdviceFromMarkdown(sanitizedExistingMarkdown);
                if (normalizeMarkdownForCompare(existing.markdown) !== normalizeMarkdownForCompare(sanitizedExistingMarkdown)) {
                    void saveCard(activeCard, { silent: true, contextInfo }).catch(() => null);
                }
            } else {
                activeCard.newCard = true;
                activeCard.textarea.value = buildNewCardTemplate(
                    safeTerm,
                    activeCard.seedSelectionSnippet,
                    contextInfo
                );
                activeCard.cachedAdvice = '';
            }
            syncCardOpenHeight(activeCard);
            scheduleCardViewportAlignment(activeCard);
            void loadAdvice(activeCard);
            await loadBacklinks(activeCard);
            syncCardOpenHeight(activeCard);
            scheduleCardViewportAlignment(activeCard);

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
            if (!safeTerm) return '';
            return `## ${safeTerm}\n\n`;
        }

        function buildCardRoot(term) {
            const root = document.createElement('section');
            root.className = 'card-fissure';
            root.innerHTML = `
                <div class="card-fissure-shell" role="group" aria-label="Concept card ${escapeHtml(term)}">
                    <header class="card-fissure-header">
                        <span class="card-fissure-title">${escapeHtml(term)}</span>
                        <span class="card-fissure-seam-tip">卡片内双击可收起</span>
                    </header>
                    <textarea class="card-fissure-editor" data-card-editor placeholder="写下你的想法..."></textarea>
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
                syncCardOpenHeight(card);
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
            wireCardDoubleTapClose(card);
            card.root.addEventListener('touchmove', (event) => {
                if (!event.touches || event.touches.length !== 1) return;
                const touch = event.touches[0];
                updateTearShadowFocalFromPoint(card, touch.clientX, touch.clientY);
            }, { passive: true });
            wireCardSwipeSeal(card);
        }

        function wireCardDoubleTapClose(card) {
            if (!card || !card.root) return;
            card.root.addEventListener('dblclick', (event) => {
                if (state.activeCard !== card) return;
                requestCardCloseByDoubleTap(card, event);
            }, true);
            card.root.addEventListener('touchstart', (event) => {
                if (state.activeCard !== card) return;
                if (!event.touches || event.touches.length !== 1) {
                    card.insideTapProbe = null;
                    return;
                }
                const touch = event.touches[0];
                card.insideTapProbe = {
                    startX: touch.clientX,
                    startY: touch.clientY,
                    startTime: Date.now(),
                    moved: false,
                };
            }, { passive: true });
            card.root.addEventListener('touchmove', (event) => {
                if (state.activeCard !== card || !card.insideTapProbe) return;
                if (!event.touches || event.touches.length !== 1) {
                    card.insideTapProbe.moved = true;
                    return;
                }
                const touch = event.touches[0];
                const travel = resolveTouchTravel(
                    card.insideTapProbe.startX,
                    card.insideTapProbe.startY,
                    touch.clientX,
                    touch.clientY
                );
                if (travel > CARD_INSIDE_TAP_MAX_TRAVEL_PX) {
                    card.insideTapProbe.moved = true;
                }
            }, { passive: true });
            card.root.addEventListener('touchend', (event) => {
                if (state.activeCard !== card) return;
                const probe = card.insideTapProbe;
                card.insideTapProbe = null;
                if (!probe || probe.moved) {
                    card.lastInsideTap = null;
                    return;
                }
                const duration = Date.now() - Number(probe.startTime || 0);
                if (duration > STRICT_SHORT_TAP_MAX_DURATION_MS) {
                    card.lastInsideTap = null;
                    return;
                }
                const touch = event.changedTouches && event.changedTouches.length ? event.changedTouches[0] : null;
                const tapX = touch ? touch.clientX : probe.startX;
                const tapY = touch ? touch.clientY : probe.startY;
                const now = Date.now();
                state.lastTouchEndAt = now;
                const previous = card.lastInsideTap;
                card.lastInsideTap = { at: now, x: tapX, y: tapY };
                if (!previous) return;
                const interval = now - Number(previous.at || 0);
                if (interval > CARD_INSIDE_DOUBLE_TAP_WINDOW_MS) {
                    return;
                }
                const distance = resolveTouchTravel(previous.x, previous.y, tapX, tapY);
                if (distance > CARD_INSIDE_DOUBLE_TAP_MAX_DISTANCE_PX) {
                    return;
                }
                card.lastInsideTap = null;
                requestCardCloseByDoubleTap(card, event);
            }, { capture: true, passive: false });
            card.root.addEventListener('touchcancel', () => {
                card.insideTapProbe = null;
                card.lastInsideTap = null;
            }, { passive: true });
        }

        function requestCardCloseByDoubleTap(card, event) {
            if (!card || state.activeCard !== card) return;
            if (card.doubleTapClosePending === true) return;
            card.doubleTapClosePending = true;
            if (event && typeof event.preventDefault === 'function') {
                event.preventDefault();
            }
            if (event && typeof event.stopPropagation === 'function') {
                event.stopPropagation();
            }
            closeActiveCard({ save: true, silent: true }).catch((error) => {
                emitNotice(`Close card failed: ${normalizeError(error)}`, 'error');
            }).finally(() => {
                card.doubleTapClosePending = false;
            });
        }

        function wireCardSwipeSeal(card) {
            if (!card || !card.root) return;
            if (config.closeByOutsideSwipeOnly === true) return;
            card.root.addEventListener('touchstart', (event) => {
                if (state.activeCard !== card) return;
                if (!event.touches || event.touches.length !== 1) return;
                if (event.target && event.target.closest('.card-fissure-editor, .card-fissure-wikilink-panel, .card-fissure-backlinks')) {
                    return;
                }
                const touch = event.touches[0];
                card.sealGesture = {
                    startX: touch.clientX,
                    startY: touch.clientY,
                    progress: 0,
                    maxDyDown: 0,
                    hapticFired: false,
                };
            }, { passive: true });
            card.root.addEventListener('touchmove', (event) => {
                if (state.activeCard !== card || !card.sealGesture) return;
                if (!event.touches || event.touches.length !== 1) return;
                const touch = event.touches[0];
                const dx = Math.abs(touch.clientX - card.sealGesture.startX);
                const dyDown = touch.clientY - card.sealGesture.startY;
                if (dx > 64) {
                    return;
                }
                if (dyDown <= 0) {
                    updateTearSceneOpenRatio(card, 1);
                    return;
                }
                if (dyDown > card.sealGesture.maxDyDown) {
                    card.sealGesture.maxDyDown = dyDown;
                }
                const resisted = applySwipeResistance(dyDown);
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
                const minSwipePx = Math.max(18, Number(config.tearCloseMinSwipePx) || TEAR_CLOSE_MIN_SWIPE_PX);
                const reachedDistance = Number(card.sealGesture.maxDyDown || 0) >= minSwipePx;
                const shouldClose = Number(card.sealGesture.progress) >= snapRatio && reachedDistance;
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
            panel.innerHTML = '<div class="card-fissure-backlinks-loading">Loading backlinks...</div>';
            try {
                const storageTitle = normalizeStorageTitle(card.term);
                if (!storageTitle) {
                    panel.hidden = true;
                    return;
                }
                const runtimeContext = typeof config.getContext === 'function' ? (config.getContext() || {}) : {};
                const query = new URLSearchParams();
                if (runtimeContext && runtimeContext.taskId) {
                    query.set('taskId', String(runtimeContext.taskId));
                }
                const endpoint = `${config.apiBase}/cards/concept/${encodeURIComponent(storageTitle)}/backlinks${query.toString() ? `?${query.toString()}` : ''}`;
                const response = await fetch(endpoint);
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
                panel.innerHTML = '<div class="card-fissure-backlinks-empty">Backlinks unavailable</div>';
                panel.hidden = false;
            }
            syncCardOpenHeight(card);
            scheduleCardViewportAlignment(card);
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
                panel.innerHTML = '<div class="card-fissure-backlinks-empty">No backlinks</div>';
                panel.hidden = false;
                syncCardOpenHeight(card);
                scheduleCardViewportAlignment(card);
                return;
            }
            panel.innerHTML = `
                <div class="card-fissure-backlinks-title">Backlinks</div>
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
            syncCardOpenHeight(card);
            scheduleCardViewportAlignment(card);
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

        async function loadAdvice(card) {
            if (!card || !card.whisper) return;
            const contextInfo = card.contextInfo || resolveContextInfo(card.term, card.anchor, card.seedSelectionSnippet);
            card.contextInfo = contextInfo;
            const cachedAdvice = normalizeAdviceForDisplay(
                extractAdviceFromMarkdown(card.textarea ? card.textarea.value : '') || card.cachedAdvice || ''
            );
            if (cachedAdvice) {
                card.cachedAdvice = cachedAdvice;
                hideAdviceFog(card);
                renderAdviceWhisperInk(card, cachedAdvice);
                toggleVisibilityWithTransition(card.whisper, true, { visibleClass: WHISPER_VISIBLE_CLASS });
                syncCardOpenHeight(card);
                scheduleCardViewportAlignment(card);
                return;
            }
            const prefetched = consumeAdvicePrefetch(card.advicePrefetchKey);
            const advicePromise = prefetched || requestAdviceResult(card.term, contextInfo);
            showAdviceFog(card, { offline: false, status: '' });
            try {
                const result = await advicePromise;
                const advice = normalizeAdviceForDisplay(result && result.advice ? result.advice : '');
                if (!advice) {
                    if (state.activeCard !== card) return;
                    if (result && result.offline) {
                        showAdviceFog(card, {
                            offline: true,
                            status: '\u7eb8\u5f20\u5df2\u7834\uff0c\u4f46\u601d\u7eea\u6682\u65ad\u3002',
                        });
                    } else {
                        hideAdviceFog(card);
                        toggleVisibilityWithTransition(card.whisper, false, { visibleClass: WHISPER_VISIBLE_CLASS });
                        syncCardOpenHeight(card);
                        scheduleCardViewportAlignment(card);
                    }
                    return;
                }
                const canDisplayAdvice = card.newCard
                    ? (advice.length >= ADVICE_MIN_CHARS && !ADVICE_PLACEHOLDER_REGEX.test(advice) && !ADVICE_PROMPT_LEAK_REGEX.test(advice))
                    : shouldDisplayAdvice(advice, card.term, contextInfo, result);
                if (!canDisplayAdvice) {
                    if (state.activeCard !== card) return;
                    hideAdviceFog(card);
                    toggleVisibilityWithTransition(card.whisper, false, { visibleClass: WHISPER_VISIBLE_CLASS });
                    syncCardOpenHeight(card);
                    scheduleCardViewportAlignment(card);
                    return;
                }
                persistAdviceToCardMarkdownAsync(card, advice, contextInfo);
                if (state.activeCard !== card) return;
                hideAdviceFog(card);
                renderAdviceWhisperInk(card, advice);
                toggleVisibilityWithTransition(card.whisper, true, { visibleClass: WHISPER_VISIBLE_CLASS });
                syncCardOpenHeight(card);
                scheduleCardViewportAlignment(card);
            } catch (error) {
                if (state.activeCard !== card) return;
                const offline = isLikelyOfflineError(error);
                if (offline) {
                    showAdviceFog(card, {
                        offline: true,
                        status: '\u7eb8\u5f20\u5df2\u7834\uff0c\u4f46\u601d\u7eea\u6682\u65ad\u3002',
                    });
                    syncCardOpenHeight(card);
                    scheduleCardViewportAlignment(card);
                    return;
                }
                hideAdviceFog(card);
                syncCardOpenHeight(card);
                scheduleCardViewportAlignment(card);
            }
        }

        function persistAdviceToCardMarkdownAsync(card, advice, contextInfo) {
            if (!card || !card.textarea) return;
            const currentMarkdown = String(card.textarea.value || '');
            const nextMarkdown = composeCardMarkdownWithAdvice(card.term, currentMarkdown, advice);
            if (!nextMarkdown) return;
            card.cachedAdvice = normalizeAdviceForDisplay(advice);
            const normalizedCurrent = normalizeMarkdownForCompare(currentMarkdown);
            const normalizedNext = normalizeMarkdownForCompare(nextMarkdown);
            if (normalizedCurrent === normalizedNext) return;
            card.textarea.value = nextMarkdown;
            syncCardOpenHeight(card);
            scheduleCardViewportAlignment(card);
            void saveCard(card, { silent: true, contextInfo }).catch((error) => {
                emitNotice(`AI 建议回写失败：${normalizeError(error)}`, 'error');
            });
        }

        function composeCardMarkdownWithAdvice(term, existingMarkdown, advice) {
            const safeTerm = String(term || '').trim();
            const normalizedAdvice = normalizeAdviceForDisplay(advice);
            if (!safeTerm || !normalizedAdvice) return '';
            let base = sanitizeCardMarkdown(safeTerm, existingMarkdown).trim();
            if (!base) {
                base = `## ${safeTerm}`;
            }
            const adviceBlock = [
                AI_ADVICE_BLOCK_START,
                '### AI 建议',
                '',
                normalizedAdvice,
                AI_ADVICE_BLOCK_END,
            ].join('\n');
            const startIdx = base.indexOf(AI_ADVICE_BLOCK_START);
            const endIdx = base.indexOf(AI_ADVICE_BLOCK_END);
            if (startIdx >= 0 && endIdx > startIdx) {
                const head = base.slice(0, startIdx).trimEnd();
                const tail = base.slice(endIdx + AI_ADVICE_BLOCK_END.length).trimStart();
                const merged = [head, adviceBlock, tail].filter(Boolean).join('\n\n').trim();
                return `${merged}\n`;
            }
            if (normalizeMarkdownForCompare(base).includes(normalizeMarkdownForCompare(normalizedAdvice))) {
                return `${base.trim()}\n`;
            }
            return `${base}\n\n${adviceBlock}\n`;
        }

        function extractAdviceFromMarkdown(markdown) {
            const source = String(markdown || '').replace(/\r\n?/g, '\n');
            const startIdx = source.indexOf(AI_ADVICE_BLOCK_START);
            const endIdx = source.indexOf(AI_ADVICE_BLOCK_END);
            if (startIdx >= 0 && endIdx > startIdx) {
                const block = source
                    .slice(startIdx + AI_ADVICE_BLOCK_START.length, endIdx)
                    .replace(/^\s*#{1,6}\s*AI\s*建议\s*$/im, '')
                    .trim();
                return normalizeAdviceForDisplay(block);
            }
            const headingMatch = source.match(/^\s*#{1,6}\s*AI\s*建议\s*$/im);
            if (!headingMatch) return '';
            const contentStart = Number(headingMatch.index || 0) + headingMatch[0].length;
            const tail = source.slice(contentStart).replace(/^\s+/, '');
            if (!tail) return '';
            const nextHeadingIndex = tail.search(/^\s*#{1,6}\s+\S/m);
            const section = nextHeadingIndex >= 0 ? tail.slice(0, nextHeadingIndex) : tail;
            return normalizeAdviceForDisplay(section.trim());
        }

        function sanitizeCardMarkdown(term, markdown) {
            const safeTerm = String(term || '').trim();
            const raw = String(markdown || '').replace(/\r\n?/g, '\n');
            const stripped = stripLegacyPlaceholderLines(raw).trim();
            if (stripped) {
                return `${stripped}\n`;
            }
            if (!safeTerm) {
                return '';
            }
            return `## ${safeTerm}\n`;
        }

        function stripLegacyPlaceholderLines(markdown) {
            return String(markdown || '')
                .replace(LEGACY_PLACEHOLDER_LINE_REGEX, '')
                .replace(LEGACY_PLACEHOLDER_HEADING_REGEX, '')
                .replace(LEGACY_PLACEHOLDER_QUOTE_BLOCK_REGEX, '')
                .replace(/\n{3,}/g, '\n\n')
                .trim();
        }

        function normalizeMarkdownForCompare(markdown) {
            return String(markdown || '')
                .replace(/\r\n?/g, '\n')
                .replace(/[ \t]+\n/g, '\n')
                .replace(/\n{3,}/g, '\n\n')
                .trim();
        }

        async function requestAdviceResult(term, contextInfo) {
            try {
                const response = await fetch(`${config.apiBase}/cards/ai-advice`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        term,
                        context: contextInfo.context,
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
                    term: String(payload && payload.term ? payload.term : '').trim(),
                    source: String(payload && payload.source ? payload.source : '').trim(),
                };
            } catch (error) {
                return {
                    advice: '',
                    offline: isLikelyOfflineError(error),
                    term: '',
                    source: '',
                };
            }
        }

        function normalizeAdviceForDisplay(rawAdvice) {
            let normalized = String(rawAdvice || '').trim();
            if (!normalized) return '';
            normalized = normalized
                .replace(/^```[\w-]*\s*/i, '')
                .replace(/\s*```$/, '')
                .trim();
            normalized = normalized.replace(/^(?:advice|suggestion)\s*[:：]\s*/i, '').trim();
            return normalized;
        }

        function renderAdviceWhisperInk(card, advice) {
            if (!card || !card.whisper) return;
            const whisper = card.whisper;
            const raw = String(advice || '');
            whisper.style.removeProperty('--ink-total-duration');
            whisper.classList.remove('is-ink-reveal');
            if (!raw) {
                whisper.innerHTML = '';
                syncCardOpenHeight(card);
                return;
            }
            whisper.innerHTML = renderAdviceMarkdownHtml(raw);
            if (!isReducedMotionPreferred()) {
                whisper.offsetWidth;
                whisper.classList.add('is-ink-reveal');
            }
            syncCardOpenHeight(card);
        }

        function normalizeAdviceKeyword(rawKeyword) {
            return String(rawKeyword || '')
                .trim()
                .replace(/^[^A-Za-z0-9\u4e00-\u9fff]+|[^A-Za-z0-9\u4e00-\u9fff]+$/g, '')
                .toLowerCase();
        }

        function collectAdviceKeywords(rawText, maxCount = ADVICE_KEYWORD_MAX) {
            const source = String(rawText || '').replace(/\s+/g, ' ').trim();
            if (!source) return [];
            const candidates = [];
            if (state.segmenter && typeof state.segmenter.segment === 'function') {
                for (const part of state.segmenter.segment(source)) {
                    const segment = normalizeAdviceKeyword(part && part.segment ? part.segment : '');
                    if (segment) {
                        candidates.push(segment);
                    }
                    if (candidates.length >= maxCount * 4) {
                        break;
                    }
                }
            } else {
                const matches = source.match(ADVICE_KEYWORD_REGEX) || [];
                matches.forEach((token) => candidates.push(normalizeAdviceKeyword(token)));
            }

            const deduped = [];
            const seen = new Set();
            for (let i = 0; i < candidates.length; i += 1) {
                const token = String(candidates[i] || '');
                if (!token || token.length < 2 || token.length > 20) continue;
                if (/^\d+$/.test(token)) continue;
                if (ADVICE_STOPWORD_SET.has(token)) continue;
                if (seen.has(token)) continue;
                seen.add(token);
                deduped.push(token);
                if (deduped.length >= maxCount) {
                    break;
                }
            }
            return deduped;
        }

        function normalizeAdviceTerm(rawTerm) {
            return normalizeAdviceKeyword(String(rawTerm || '').replace(/\s+/g, ''));
        }

        function shouldDisplayAdvice(advice, term, contextInfo, result) {
            const resolvedAdvice = normalizeAdviceForDisplay(advice);
            if (!resolvedAdvice || resolvedAdvice.length < ADVICE_MIN_CHARS) {
                return false;
            }
            if (ADVICE_PLACEHOLDER_REGEX.test(resolvedAdvice)) {
                return false;
            }
            if (ADVICE_PROMPT_LEAK_REGEX.test(resolvedAdvice)) {
                return false;
            }

            const requestTerm = normalizeAdviceTerm(term);
            const responseTerm = normalizeAdviceTerm(result && result.term ? result.term : '');
            if (responseTerm && requestTerm && responseTerm !== requestTerm) {
                return false;
            }

            const adviceLower = resolvedAdvice.toLowerCase();
            if (requestTerm && requestTerm.length >= 2 && adviceLower.includes(requestTerm)) {
                return true;
            }

            let anchorKeywords = collectAdviceKeywords([
                term,
                contextInfo && contextInfo.example ? contextInfo.example : '',
            ].join('\n'));
            if (!anchorKeywords.length) {
                anchorKeywords = collectAdviceKeywords(contextInfo && contextInfo.context ? contextInfo.context : '');
            }
            if (!anchorKeywords.length) {
                return false;
            }

            const minOverlap = responseTerm ? 1 : 2;
            let overlapCount = 0;
            for (let i = 0; i < anchorKeywords.length; i += 1) {
                if (adviceLower.includes(anchorKeywords[i])) {
                    overlapCount += 1;
                    if (overlapCount >= minOverlap) {
                        return true;
                    }
                }
            }
            return false;
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

        function ensureAdviceMarkdownEngine() {
            if (state.adviceMarkdownEngine) {
                return state.adviceMarkdownEngine;
            }
            if (typeof global.markdownit !== 'function') {
                return null;
            }
            try {
                state.adviceMarkdownEngine = global.markdownit({
                    html: false,
                    linkify: true,
                    breaks: true,
                    typographer: false,
                });
                return state.adviceMarkdownEngine;
            } catch (_error) {
                state.adviceMarkdownEngine = null;
                return null;
            }
        }

        function renderAdviceMarkdownHtml(rawAdvice) {
            const source = String(rawAdvice || '').slice(0, ADVICE_MARKDOWN_MAX_CHARS);
            if (!source.trim()) {
                return '';
            }
            const engine = ensureAdviceMarkdownEngine();
            let html = '';
            if (engine && typeof engine.render === 'function') {
                try {
                    html = String(engine.render(source) || '');
                } catch (_error) {
                    html = '';
                }
            }
            if (!html) {
                html = `<p>${escapeHtml(source).replace(/\r?\n/g, '<br>')}</p>`;
            }
            if (global.DOMPurify && typeof global.DOMPurify.sanitize === 'function') {
                html = global.DOMPurify.sanitize(html, {
                    ALLOWED_ATTR: ['href', 'title', 'target', 'rel'],
                });
            }
            return html;
        }

        function syncCardOpenHeight(card) {
            if (!card || !card.root) return;
            const shell = card.root.querySelector('.card-fissure-shell');
            if (!shell) return;
            const whisperHeight = card.whisper && !card.whisper.hidden
                ? Math.max(0, Number(card.whisper.scrollHeight) || 0)
                : 0;
            const tailSpace = whisperHeight > 0
                ? Math.round(whisperHeight * CARD_TAIL_SPACE_RATIO)
                : 0;
            shell.style.setProperty('--card-tail-space', `${Math.max(0, tailSpace)}px`);
            const viewportHeight = Math.max(
                1,
                Number(global.innerHeight) || Number(document.documentElement && document.documentElement.clientHeight) || 0
            );
            const nextHeight = Math.max(
                CARD_OPEN_MIN_HEIGHT_PX,
                Math.min(
                    CARD_OPEN_FIXED_MAX_HEIGHT_PX,
                    Math.round(viewportHeight * CARD_OPEN_FIXED_VIEWPORT_RATIO)
                )
            );
            card.root.style.setProperty('--card-open-max-height', `${nextHeight}px`);
        }

        function scheduleCardViewportAlignment(card) {
            if (!card || !card.tearScene || !card.tearScene.wrapper) return;
            const run = () => {
                if (state.activeCard !== card) return;
                alignCardSceneToViewport(card);
            };
            requestAnimationFrame(run);
            window.setTimeout(run, 180);
        }

        function alignCardSceneToViewport(card) {
            if (!card || !card.root || !card.tearScene || !card.tearScene.wrapper) return;
            const wrapper = card.tearScene.wrapper;
            const cardRect = card.root.getBoundingClientRect();
            const sceneRect = wrapper.getBoundingClientRect();
            if (cardRect.height <= 1 || sceneRect.height <= 1) {
                return;
            }
            const viewportHeight = Math.max(
                1,
                Number(global.innerHeight) || Number(document.documentElement && document.documentElement.clientHeight) || 0
            );
            const targetCenterY = viewportHeight * TEAR_SCENE_TARGET_CENTER_RATIO;
            const cardCenterY = cardRect.top + (cardRect.height / 2);
            let shiftY = targetCenterY - cardCenterY;
            if (shiftY > 0) {
                shiftY = 0;
            }
            const maxUpBySceneTop = Math.max(0, sceneRect.top - TEAR_SCENE_MIN_VISIBLE_TOP_PX);
            shiftY = Math.max(shiftY, -maxUpBySceneTop);
            const minCardTop = Math.max(TEAR_SCENE_MIN_CARD_TOP_PX, viewportHeight * TEAR_SCENE_MIN_CARD_TOP_RATIO);
            const projectedCardTop = cardRect.top + shiftY;
            if (projectedCardTop < minCardTop) {
                shiftY += (minCardTop - projectedCardTop);
            }
            wrapper.style.setProperty('--tear-scene-shift-y', `${Math.round(shiftY)}px`);
        }

        function resolveContextInfo(term, anchor, selectionSnippet) {
            const context = typeof config.getContext === 'function' ? (config.getContext() || {}) : {};
            const markdown = String(context.markdown || '');
            const scopedContext = resolveScopedContextByAnchor(anchor, markdown, config.contextChars);
            const seedSnippet = normalizeSelectionSnippet(selectionSnippet);
            const mergedContext = scopedContext || seedSnippet;
            return {
                context: mergedContext,
                example: '',
                isContextDependent: true,
                type: THOUGHT_CARD_TYPE,
            };
        }

        function resolveScopedContextByAnchor(anchor, markdown, maxChars) {
            const raw = String(markdown || '').replace(/\r\n?/g, '\n');
            if (!raw) return '';
            const lines = raw.split('\n');
            if (!lines.length || !anchor || !anchor.getAttribute) return '';
            const baseRange = resolveElementLineRange(anchor);
            if (!baseRange) return '';
            const scopedLineNumbers = collectScopedLineNumbers(anchor, baseRange, lines.length);
            if (!scopedLineNumbers.length) return '';
            const merged = scopedLineNumbers
                .map((lineNo) => String(lines[lineNo - 1] || '').trimEnd())
                .join('\n')
                .trim();
            if (!merged) return '';
            const maxLen = Math.max(120, Number(maxChars) || 0);
            if (merged.length <= maxLen) return merged;
            return `${merged.slice(0, maxLen).trim()}`;
        }

        function collectScopedLineNumbers(anchor, baseRange, totalLines) {
            const lineSet = new Set();
            const appendRange = (range) => {
                if (!range) return;
                const start = Math.max(1, Number(range.start) || 0);
                const end = Math.max(start, Number(range.end) || start);
                for (let lineNo = start; lineNo <= end && lineNo <= totalLines; lineNo += 1) {
                    lineSet.add(lineNo);
                }
            };

            appendRange(baseRange);

            let parent = anchor.parentElement;
            while (parent) {
                appendRange(resolveElementLineRange(parent));
                parent = parent.parentElement;
            }

            if (anchor.querySelectorAll) {
                anchor.querySelectorAll('[data-line]').forEach((node) => {
                    appendRange(resolveElementLineRange(node));
                });
            }

            return Array.from(lineSet).sort((a, b) => a - b);
        }

        function resolveElementLineRange(element) {
            if (!element || !element.getAttribute) return null;
            const start = parsePositiveLineNo(element.getAttribute('data-line'));
            if (!start) return null;
            const end = parsePositiveLineNo(element.getAttribute('data-line-end')) || start;
            return {
                start,
                end: Math.max(start, end),
            };
        }

        function parsePositiveLineNo(rawLine) {
            const lineNo = Number.parseInt(String(rawLine || '').trim(), 10);
            if (!Number.isFinite(lineNo) || lineNo <= 0) {
                return 0;
            }
            return lineNo;
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
                active.tearScene.wrapper.style.setProperty('--tear-scene-shift-y', '0px');
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
            wrapper.style.setProperty('--tear-scene-shift-y', '0px');

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
                const contextInfo = (options && options.contextInfo)
                    || activeCard.contextInfo
                    || resolveContextInfo(activeCard.term, activeCard.anchor, activeCard.seedSelectionSnippet);
                const query = new URLSearchParams();
                query.set('isContextDependent', String(contextInfo.isContextDependent));
                query.set('type', contextInfo.type);
                const runtimeContext = typeof config.getContext === 'function' ? (config.getContext() || {}) : {};
                if (runtimeContext && runtimeContext.taskId) {
                    query.set('sourceTaskId', String(runtimeContext.taskId));
                }
                if (runtimeContext && runtimeContext.path) {
                    query.set('sourcePath', String(runtimeContext.path));
                }
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
            const safe = normalizeTitleCandidate(title);
            if (!safe) return;
            const key = normalizeTitleLookupKey(safe);
            if (!key) return;
            let replaced = false;
            for (let i = 0; i < state.titles.length; i += 1) {
                const current = String(state.titles[i] || '');
                if (normalizeTitleLookupKey(current) !== key) continue;
                const preferred = pickPreferredCanonicalTitle(safe, current);
                if (preferred !== current) {
                    state.titles[i] = preferred;
                }
                replaced = true;
                break;
            }
            if (!replaced) {
                state.titles.push(safe);
            }
            state.titles = normalizeTitleCollection(state.titles, config.maxHighlightTerms);
            updateHighlightTerms(state.titles);
        }

        function rememberOpenedTerm(rawTerm) {
            const safeTerm = normalizeCardTerm(rawTerm);
            if (!safeTerm) return;
            const key = normalizeTitleLookupKey(safeTerm);
            if (!key) return;
            if (!state.learnedTermKeys.has(key)) {
                state.learnedTermKeys.add(key);
                persistLearnedTermKeys();
            }
            appendLocalTitle(safeTerm);
            if (!state.container) return;
            resetHighlightRuntime();
            applyHighlights(state.container);
            syncLearnedTermDecorations(state.container);
        }

        function syncLearnedTermDecorations(container) {
            if (!container) return;
            container.querySelectorAll('.concept-term').forEach((node) => {
                if (!node || !node.classList) return;
                const source = String(node.getAttribute('data-term') || node.textContent || '').trim();
                const key = normalizeTitleLookupKey(source);
                node.classList.toggle('is-learned-term', !!(key && state.learnedTermKeys.has(key)));
            });
        }

        function loadLearnedTermKeys() {
            const keys = new Set();
            try {
                if (!global.localStorage) return keys;
                const raw = String(global.localStorage.getItem(LEARNED_TERMS_STORAGE_KEY) || '').trim();
                if (!raw) return keys;
                const items = JSON.parse(raw);
                if (!Array.isArray(items)) return keys;
                items.slice(0, LEARNED_TERMS_MAX_ITEMS).forEach((item) => {
                    const key = normalizeTitleLookupKey(item);
                    if (key) {
                        keys.add(key);
                    }
                });
            } catch (_error) {
                return keys;
            }
            return keys;
        }

        function persistLearnedTermKeys() {
            try {
                if (!global.localStorage) return;
                const payload = Array.from(state.learnedTermKeys || []).slice(0, LEARNED_TERMS_MAX_ITEMS);
                global.localStorage.setItem(LEARNED_TERMS_STORAGE_KEY, JSON.stringify(payload));
            } catch (_error) {
                // ignore storage errors
            }
        }

        async function loadCard(term) {
            try {
                const storageTitle = normalizeStorageTitle(term);
                if (!storageTitle) {
                    return { exists: false, markdown: '' };
                }
                const runtimeContext = typeof config.getContext === 'function' ? (config.getContext() || {}) : {};
                const query = new URLSearchParams();
                if (runtimeContext && runtimeContext.taskId) {
                    query.set('taskId', String(runtimeContext.taskId));
                }
                const endpoint = `${config.apiBase}/cards/concept/${encodeURIComponent(storageTitle)}${query.toString() ? `?${query.toString()}` : ''}`;
                const response = await fetch(endpoint);
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
            syncLearnedTermDecorations(container);
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
            let title = normalizeCardTerm(rawTitle);
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

        function normalizeCardTerm(rawTerm) {
            const normalized = String(rawTerm || '').replace(/\s+/g, ' ').trim();
            if (!normalized) return '';
            const recovered = recoverSuspiciousDuplicatedCjk(normalized);
            const knownDirect = resolveKnownTitle(normalized);
            const knownRecovered = resolveKnownTitle(recovered);
            const knownPreferred = pickPreferredCanonicalTitle(knownRecovered, knownDirect);
            if (knownPreferred) return knownPreferred;
            const recoveredPreferred = pickPreferredCanonicalTitle(recovered, normalized);
            return recoveredPreferred || normalized;
        }

        function resolveKnownTitle(rawTerm) {
            const key = normalizeTitleLookupKey(rawTerm);
            if (!key || !Array.isArray(state.titles) || !state.titles.length) return '';
            let matched = '';
            for (let i = 0; i < state.titles.length; i += 1) {
                const candidate = String(state.titles[i] || '');
                if (!candidate) continue;
                if (normalizeTitleLookupKey(candidate) === key) {
                    matched = pickPreferredCanonicalTitle(candidate, matched);
                }
            }
            return matched;
        }

        function normalizeTitleLookupKey(rawTitle) {
            return recoverSuspiciousDuplicatedCjk(String(rawTitle || '')
                .replace(/\s+/g, ' ')
                .trim()
                .toLowerCase());
        }

        function normalizeTitleCandidate(rawTitle) {
            return recoverSuspiciousDuplicatedCjk(String(rawTitle || '')
                .replace(/\s+/g, ' ')
                .trim());
        }

        function pickPreferredCanonicalTitle(primary, secondary) {
            const first = String(primary || '').trim();
            const second = String(secondary || '').trim();
            if (first && !second) return first;
            if (second && !first) return second;
            if (!first && !second) return '';
            const firstNoise = looksLikeDuplicatedCjkNoise(first);
            const secondNoise = looksLikeDuplicatedCjkNoise(second);
            if (firstNoise !== secondNoise) {
                return firstNoise ? second : first;
            }
            if (first.length !== second.length) {
                return first.length < second.length ? first : second;
            }
            return first.localeCompare(second, 'zh-Hans-CN') <= 0 ? first : second;
        }

        function recoverSuspiciousDuplicatedCjk(rawText) {
            const source = String(rawText || '');
            if (!looksLikeDuplicatedCjkNoise(source)) {
                return source.replace(/\s+/g, ' ').trim();
            }
            return collapseConsecutiveCjkDuplicates(source).replace(/\s+/g, ' ').trim();
        }

        function looksLikeDuplicatedCjkNoise(rawText) {
            const text = String(rawText || '');
            if (!text || text.length < 8) return false;
            const chars = Array.from(text);
            let cjkCount = 0;
            let significantCount = 0;
            let duplicatedPairCount = 0;
            const uniqueCjk = new Set();
            for (let i = 0; i < chars.length;) {
                const ch = chars[i];
                let runLen = 1;
                while ((i + runLen) < chars.length && chars[i + runLen] === ch) {
                    runLen += 1;
                }
                if (/[\u4e00-\u9fff]/.test(ch)) {
                    cjkCount += runLen;
                    significantCount += runLen;
                    uniqueCjk.add(ch);
                    if (runLen >= 2) {
                        duplicatedPairCount += Math.floor(runLen / 2);
                    }
                } else if (/[A-Za-z0-9]/.test(ch)) {
                    significantCount += runLen;
                }
                i += runLen;
            }
            if (cjkCount < 6) return false;
            if (uniqueCjk.size < 3) return false;
            if (significantCount > 0 && cjkCount < Math.ceil(significantCount * 0.5)) return false;
            return duplicatedPairCount >= 3 && duplicatedPairCount >= Math.ceil(cjkCount * 0.35);
        }

        function collapseConsecutiveCjkDuplicates(rawText) {
            return String(rawText || '').replace(/([\u4e00-\u9fff])\1+/g, '$1');
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
        });
    }

    global.mobileConceptCards = Object.freeze({
        create: createMobileConceptCards,
    });
})(window);
