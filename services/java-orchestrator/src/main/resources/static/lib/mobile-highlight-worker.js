/* eslint-disable no-restricted-globals */
(function () {
    'use strict';

    const state = {
        generation: 0,
        bucketTerms: new Map(),
        regexCache: new Map(),
    };

    self.onmessage = function onMessage(event) {
        const payload = event && event.data ? event.data : {};
        const type = String(payload.type || '');
        if (type === 'initIndex') {
            state.generation = Number(payload.generation) || 0;
            state.bucketTerms = deserializeBuckets(payload.termIndex);
            state.regexCache.clear();
            self.postMessage({ type: 'ready', generation: state.generation });
            return;
        }
        if (type === 'findMatches') {
            const id = Number(payload.id);
            const generation = Number(payload.generation) || 0;
            const source = String(payload.source || '');
            if (!Number.isFinite(id) || generation !== state.generation) {
                self.postMessage({ type: 'matches', id, generation, matches: [] });
                return;
            }
            const matches = findMatchesByIndex(source);
            self.postMessage({ type: 'matches', id, generation, matches });
        }
    };

    function deserializeBuckets(raw) {
        const map = new Map();
        if (!raw || typeof raw !== 'object') return map;
        Object.keys(raw).forEach((key) => {
            const items = Array.isArray(raw[key]) ? raw[key] : [];
            const safe = items
                .map((item) => String(item || '').trim())
                .filter(Boolean);
            if (safe.length) {
                map.set(String(key || ''), safe);
            }
        });
        return map;
    }

    function findMatchesByIndex(source) {
        if (!source || !state.bucketTerms.size) return [];
        const keys = collectBucketKeys(source);
        if (!keys.length) return [];
        const matches = [];
        keys.forEach((key) => {
            const regex = resolveBucketRegex(key);
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

    function collectBucketKeys(source) {
        const keys = new Set();
        for (let index = 0; index < source.length; index += 1) {
            const key = normalizeBucketKey(source.charAt(index));
            if (!key || keys.has(key)) continue;
            if (state.bucketTerms.has(key)) {
                keys.add(key);
            }
        }
        return Array.from(keys);
    }

    function resolveBucketRegex(key) {
        if (state.regexCache.has(key)) {
            return state.regexCache.get(key) || null;
        }
        const terms = state.bucketTerms.get(key);
        if (!terms || !terms.length) {
            state.regexCache.set(key, null);
            return null;
        }
        const regex = new RegExp(`(${terms.map(escapeRegex).join('|')})`, 'g');
        state.regexCache.set(key, regex);
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

    function normalizeBucketKey(input) {
        const value = String(input || '');
        if (!value) return '';
        return value.charAt(0).toLocaleLowerCase();
    }

    function escapeRegex(input) {
        return String(input).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    }
})();
