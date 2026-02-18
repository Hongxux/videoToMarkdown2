(function (global) {
    'use strict';

    const DEFAULT_BLOCK_SELECTOR = 'p, li, blockquote, h1, h2, h3, h4, h5, h6, pre';
    const DEFAULT_SKIP_SELECTOR = 'code, pre, a, script, style, .katex, .card-fissure, .concept-term, .inline-sticky-note';
    const DEFAULT_ROOT_MARGIN = '120% 0px 120% 0px';

    function createMobileHighlightEngine(options = {}) {
        const config = Object.assign({
            blockSelector: DEFAULT_BLOCK_SELECTOR,
            skipSelector: DEFAULT_SKIP_SELECTOR,
            maxHighlightTerms: 10000,
            maxTermsPerBucket: 0,
            highlightInitialBlockLimit: 36,
            highlightFrameBudgetMs: 8,
            highlightObserverRootMargin: DEFAULT_ROOT_MARGIN,
            workerUrl: '/lib/mobile-highlight-worker.js',
            workerMinTerms: 1200,
        }, options || {});

        const state = {
            container: null,
            terms: [],
            termIndex: null,
            regexCache: new Map(),
            blockObserver: null,
            highlightQueue: [],
            queuedBlocks: new Set(),
            highlightIdleTask: 0,
            highlightFrameTask: 0,
            drainInProgress: false,
            worker: null,
            workerEnabled: false,
            workerReady: false,
            workerSeq: 0,
            workerGeneration: 0,
            workerPending: new Map(),
        };

        function setTerms(rawTerms) {
            const nextTerms = sanitizeTerms(rawTerms, config.maxHighlightTerms);
            state.terms = nextTerms;
            state.termIndex = buildTermIndex(nextTerms, config.maxTermsPerBucket);
            state.regexCache.clear();
            syncWorkerIndex();
        }

        function applyHighlights(container) {
            if (!container || !state.termIndex) return;
            state.container = container;
            const blocks = collectHighlightBlocks(container, config.blockSelector, config.skipSelector);
            if (!blocks.length) return;

            if (typeof global.IntersectionObserver === 'function') {
                state.blockObserver = new global.IntersectionObserver((entries) => {
                    const pending = [];
                    entries.forEach((entry) => {
                        if (entry && entry.isIntersecting && entry.target) {
                            pending.push(entry.target);
                        }
                    });
                    enqueueHighlightBlocks(pending);
                }, {
                    root: null,
                    rootMargin: String(config.highlightObserverRootMargin || DEFAULT_ROOT_MARGIN),
                    threshold: 0,
                });
                blocks.forEach((block) => state.blockObserver.observe(block));
            }

            const seeded = pickInitialHighlightBlocks(blocks, config.highlightInitialBlockLimit);
            enqueueHighlightBlocks(seeded);
            if (!state.blockObserver) {
                enqueueHighlightBlocks(blocks);
            }
        }

        function resetRuntime(container) {
            if (state.blockObserver) {
                state.blockObserver.disconnect();
                state.blockObserver = null;
            }
            if (state.highlightIdleTask && typeof global.cancelIdleCallback === 'function') {
                global.cancelIdleCallback(state.highlightIdleTask);
            }
            if (state.highlightFrameTask && typeof global.cancelAnimationFrame === 'function') {
                global.cancelAnimationFrame(state.highlightFrameTask);
            }
            state.highlightIdleTask = 0;
            state.highlightFrameTask = 0;
            state.highlightQueue.length = 0;
            state.queuedBlocks.clear();
            state.drainInProgress = false;

            const target = container || state.container;
            if (target && target.querySelectorAll) {
                target.querySelectorAll('[data-concept-highlighted="1"]').forEach((node) => {
                    node.removeAttribute('data-concept-highlighted');
                });
            }

            state.workerGeneration += 1;
            clearPendingWorkerRequests();
        }

        function destroy() {
            resetRuntime(state.container);
            destroyWorker();
            state.container = null;
            state.termIndex = null;
            state.terms = [];
            state.regexCache.clear();
        }

        function enqueueHighlightBlocks(blocks) {
            if (!Array.isArray(blocks) || !blocks.length) return;
            blocks.forEach((block) => {
                if (!block || !state.container || !state.container.contains(block)) return;
                if (block.getAttribute('data-concept-highlighted') === '1') return;
                if (state.queuedBlocks.has(block)) return;
                state.queuedBlocks.add(block);
                state.highlightQueue.push(block);
            });
            scheduleHighlightDrain();
        }

        function scheduleHighlightDrain() {
            if (state.highlightIdleTask || state.highlightFrameTask) return;
            if (typeof global.requestIdleCallback === 'function') {
                state.highlightIdleTask = global.requestIdleCallback((deadline) => {
                    state.highlightIdleTask = 0;
                    void drainHighlightQueue(deadline);
                }, { timeout: 180 });
                return;
            }
            if (typeof global.requestAnimationFrame === 'function') {
                state.highlightFrameTask = global.requestAnimationFrame(() => {
                    state.highlightFrameTask = 0;
                    void drainHighlightQueue(null);
                });
                return;
            }
            void drainHighlightQueue(null);
        }

        async function drainHighlightQueue(deadline) {
            if (state.drainInProgress) return;
            state.drainInProgress = true;
            try {
                const budget = Math.max(3, Number(config.highlightFrameBudgetMs) || 8);
                const start = getNowMs();
                while (state.highlightQueue.length) {
                    const block = state.highlightQueue.shift();
                    if (block) {
                        state.queuedBlocks.delete(block);
                    }
                    if (!block || !state.container || !state.container.contains(block)) {
                        continue;
                    }
                    if (block.getAttribute('data-concept-highlighted') === '1') {
                        continue;
                    }
                    await highlightBlock(block);
                    block.setAttribute('data-concept-highlighted', '1');
                    if (state.blockObserver) {
                        state.blockObserver.unobserve(block);
                    }
                    if (shouldYield(deadline, start, budget)) {
                        break;
                    }
                }
            } finally {
                state.drainInProgress = false;
            }
            if (state.highlightQueue.length) {
                scheduleHighlightDrain();
            }
        }

        async function highlightBlock(block) {
            if (!block || !state.termIndex) return;
            const walker = document.createTreeWalker(
                block,
                NodeFilter.SHOW_TEXT,
                {
                    acceptNode(node) {
                        if (!node || !node.parentElement) return NodeFilter.FILTER_REJECT;
                        const text = String(node.nodeValue || '');
                        if (!text.trim()) return NodeFilter.FILTER_REJECT;
                        if (node.parentElement.closest(config.skipSelector)) {
                            return NodeFilter.FILTER_REJECT;
                        }
                        return NodeFilter.FILTER_ACCEPT;
                    },
                }
            );
            const nodes = [];
            let current = walker.nextNode();
            while (current) {
                nodes.push(current);
                current = walker.nextNode();
            }
            for (const node of nodes) {
                await highlightTextNode(node);
            }
        }

        async function highlightTextNode(node) {
            const source = String(node.nodeValue || '');
            if (!source.trim()) return;
            const matches = await findMatches(source);
            if (!matches.length) return;

            const fragment = document.createDocumentFragment();
            let lastIndex = 0;
            matches.forEach((match) => {
                const term = String(match.term || '');
                const start = Number(match.index);
                if (!term || !Number.isFinite(start)) return;
                if (start > lastIndex) {
                    fragment.appendChild(document.createTextNode(source.slice(lastIndex, start)));
                }
                const span = document.createElement('span');
                span.className = 'concept-term concept-highlight';
                span.dataset.term = term;
                span.textContent = term;
                fragment.appendChild(span);
                lastIndex = start + term.length;
            });
            if (lastIndex < source.length) {
                fragment.appendChild(document.createTextNode(source.slice(lastIndex)));
            }
            node.replaceWith(fragment);
        }

        function findMatches(source) {
            if (!source || !state.termIndex) return Promise.resolve([]);
            if (shouldUseWorker()) {
                return findMatchesByWorker(source);
            }
            return Promise.resolve(findMatchesByIndex(source, state.termIndex, state.regexCache));
        }

        function shouldUseWorker() {
            return !!(state.workerEnabled && state.workerReady && state.worker && state.terms.length >= Number(config.workerMinTerms || 0));
        }

        function findMatchesByWorker(source) {
            if (!state.worker || !state.workerReady) {
                return Promise.resolve(findMatchesByIndex(source, state.termIndex, state.regexCache));
            }
            const requestId = ++state.workerSeq;
            const generation = state.workerGeneration;
            return new Promise((resolve) => {
                state.workerPending.set(requestId, { resolve, generation });
                try {
                    state.worker.postMessage({
                        type: 'findMatches',
                        id: requestId,
                        generation,
                        source,
                    });
                } catch (_error) {
                    state.workerPending.delete(requestId);
                    resolve(findMatchesByIndex(source, state.termIndex, state.regexCache));
                }
            });
        }

        function syncWorkerIndex() {
            const canUseWorker = typeof global.Worker === 'function'
                && !!String(config.workerUrl || '').trim();
            if (!canUseWorker || !state.termIndex) {
                destroyWorker();
                return;
            }
            if (!state.worker) {
                try {
                    state.worker = new global.Worker(String(config.workerUrl));
                } catch (_error) {
                    state.worker = null;
                    state.workerEnabled = false;
                    state.workerReady = false;
                    return;
                }
                state.workerEnabled = true;
                state.workerReady = false;
                state.worker.onmessage = handleWorkerMessage;
                state.worker.onerror = () => {
                    state.workerEnabled = false;
                    state.workerReady = false;
                    clearPendingWorkerRequests();
                };
            }
            if (state.worker && state.workerEnabled) {
                const payload = serializeTermIndex(state.termIndex);
                state.worker.postMessage({
                    type: 'initIndex',
                    generation: state.workerGeneration,
                    termIndex: payload,
                });
            }
        }

        function handleWorkerMessage(event) {
            const payload = event && event.data ? event.data : {};
            const type = String(payload.type || '');
            if (type === 'ready') {
                state.workerReady = true;
                return;
            }
            if (type !== 'matches') return;
            const id = Number(payload.id);
            if (!Number.isFinite(id)) return;
            const pending = state.workerPending.get(id);
            if (!pending) return;
            state.workerPending.delete(id);
            if (pending.generation !== state.workerGeneration) {
                pending.resolve([]);
                return;
            }
            const matches = Array.isArray(payload.matches) ? payload.matches : [];
            pending.resolve(matches);
        }

        function clearPendingWorkerRequests() {
            state.workerPending.forEach((pending) => {
                if (pending && typeof pending.resolve === 'function') {
                    pending.resolve([]);
                }
            });
            state.workerPending.clear();
        }

        function destroyWorker() {
            clearPendingWorkerRequests();
            if (state.worker) {
                try {
                    state.worker.terminate();
                } catch (_error) {
                    // noop
                }
            }
            state.worker = null;
            state.workerEnabled = false;
            state.workerReady = false;
        }

        return Object.freeze({
            setTerms,
            applyHighlights,
            resetRuntime,
            destroy,
        });
    }

    function sanitizeTerms(rawTerms, maxTerms) {
        const limit = Math.max(1, Number(maxTerms) || 10000);
        return (Array.isArray(rawTerms) ? rawTerms : [])
            .map((item) => String(item || '').trim())
            .filter(Boolean)
            .filter((title) => title.length >= 2)
            .slice(0, limit)
            .sort((a, b) => b.length - a.length);
    }

    function collectHighlightBlocks(container, blockSelector, skipSelector) {
        const blockNodes = Array.from(container.querySelectorAll(blockSelector)).filter((node) => {
            if (!node || !(node instanceof Element)) return false;
            if (node.closest(skipSelector)) return false;
            return true;
        });
        if (blockNodes.length) return blockNodes;
        if (container instanceof Element && !container.closest(skipSelector)) return [container];
        return [];
    }

    function pickInitialHighlightBlocks(blocks, limit) {
        const maxCount = Math.max(1, Number(limit) || 36);
        if (!Array.isArray(blocks) || !blocks.length) return [];
        if (typeof global.innerHeight !== 'number' || typeof global.scrollY !== 'number') {
            return blocks.slice(0, maxCount);
        }
        const viewportTop = global.scrollY - global.innerHeight;
        const viewportBottom = global.scrollY + global.innerHeight * 2;
        const nearby = blocks.filter((block) => {
            const rect = block.getBoundingClientRect();
            const top = rect.top + global.scrollY;
            const height = Math.max(rect.height || 0, 1);
            const bottom = top + height;
            return bottom >= viewportTop && top <= viewportBottom;
        });
        return (nearby.length ? nearby : blocks).slice(0, maxCount);
    }

    function shouldYield(deadline, start, budget) {
        if (deadline && typeof deadline.timeRemaining === 'function') {
            return deadline.timeRemaining() <= 1;
        }
        return (getNowMs() - start) >= budget;
    }

    function getNowMs() {
        if (global.performance && typeof global.performance.now === 'function') {
            return global.performance.now();
        }
        return Date.now();
    }

    function findMatchesByIndex(source, termIndex, regexCache) {
        if (!source || !termIndex || !termIndex.bucketTerms) return [];
        const keys = collectBucketKeys(source, termIndex.bucketTerms);
        if (!keys.length) return [];
        const matches = [];
        keys.forEach((key) => {
            const regex = resolveBucketRegex(key, termIndex, regexCache);
            if (!regex) return;
            regex.lastIndex = 0;
            let match = regex.exec(source);
            while (match) {
                const value = String(match[0] || '');
                if (value) {
                    matches.push({ index: match.index, term: value });
                }
                match = regex.exec(source);
            }
        });
        if (!matches.length) return [];
        matches.sort((a, b) => (a.index - b.index) || (b.term.length - a.term.length));
        return mergeOverlappingMatches(matches);
    }

    function collectBucketKeys(source, bucketTerms) {
        const keys = new Set();
        for (let index = 0; index < source.length; index += 1) {
            const key = normalizeBucketKey(source.charAt(index));
            if (!key || keys.has(key)) continue;
            if (bucketTerms.has(key)) {
                keys.add(key);
            }
        }
        return Array.from(keys);
    }

    function resolveBucketRegex(key, termIndex, regexCache) {
        if (regexCache.has(key)) {
            return regexCache.get(key) || null;
        }
        const terms = termIndex.bucketTerms.get(key);
        if (!terms || !terms.length) {
            regexCache.set(key, null);
            return null;
        }
        const regex = new RegExp(`(${terms.map(escapeRegex).join('|')})`, 'g');
        regexCache.set(key, regex);
        return regex;
    }

    function mergeOverlappingMatches(matches) {
        const merged = [];
        let index = 0;
        let lastEnd = 0;
        while (index < matches.length) {
            const start = matches[index].index;
            if (start < lastEnd) {
                index += 1;
                continue;
            }
            let best = matches[index];
            index += 1;
            while (index < matches.length && matches[index].index === start) {
                if (matches[index].term.length > best.term.length) {
                    best = matches[index];
                }
                index += 1;
            }
            merged.push(best);
            lastEnd = best.index + best.term.length;
        }
        return merged;
    }

    function buildTermIndex(titles, maxTermsPerBucket) {
        if (!Array.isArray(titles) || !titles.length) return null;
        const bucketTerms = new Map();
        titles.forEach((term) => {
            const key = normalizeBucketKey(term.charAt(0));
            if (!key) return;
            if (!bucketTerms.has(key)) {
                bucketTerms.set(key, []);
            }
            const terms = bucketTerms.get(key);
            if (terms) {
                terms.push(term);
            }
        });
        const maxPerBucket = Math.max(0, Number(maxTermsPerBucket) || 0);
        bucketTerms.forEach((terms, key) => {
            terms.sort((a, b) => b.length - a.length);
            bucketTerms.set(key, maxPerBucket > 0 ? terms.slice(0, maxPerBucket) : terms);
        });
        return Object.freeze({ bucketTerms });
    }

    function serializeTermIndex(termIndex) {
        const payload = {};
        if (!termIndex || !termIndex.bucketTerms) return payload;
        termIndex.bucketTerms.forEach((terms, key) => {
            payload[key] = Array.isArray(terms) ? terms.slice() : [];
        });
        return payload;
    }

    function normalizeBucketKey(input) {
        const value = String(input || '');
        if (!value) return '';
        return value.charAt(0).toLocaleLowerCase();
    }

    function escapeRegex(input) {
        return String(input).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    }

    global.mobileHighlightEngine = Object.freeze({
        create: createMobileHighlightEngine,
    });
})(window);
