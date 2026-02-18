(function (global) {
    'use strict';

    function toFiniteNumber(value, fallback) {
        const next = Number(value);
        if (Number.isFinite(next)) {
            return next;
        }
        return Number(fallback) || 0;
    }

    // 双帧调度：把“写后读”拆到后续帧，降低同帧布局抖动概率。
    function scheduleDoubleAnimationFrame(callback) {
        let rafA = 0;
        let rafB = 0;
        rafA = requestAnimationFrame(() => {
            rafA = 0;
            rafB = requestAnimationFrame(() => {
                rafB = 0;
                if (typeof callback === 'function') {
                    callback();
                }
            });
        });
        return function cancelSchedule() {
            if (rafA) {
                cancelAnimationFrame(rafA);
                rafA = 0;
            }
            if (rafB) {
                cancelAnimationFrame(rafB);
                rafB = 0;
            }
        };
    }

    // JSON Worker 池：把大 payload 解析迁移到 Worker，避免长时间占用主线程。
    function createJsonParseWorkerPool(options) {
        const settings = options || {};
        const timeoutMs = Math.max(1000, Math.round(toFiniteNumber(settings.timeoutMs, 8000)));
        const supported = typeof Worker === 'function' && typeof Blob === 'function' && typeof URL.createObjectURL === 'function';
        let seq = 0;
        let worker = null;
        let pending = new Map();

        function clearPending(error) {
            if (!pending || !pending.size) {
                return;
            }
            pending.forEach((entry) => {
                clearTimeout(entry.timerId);
                entry.reject(error);
            });
            pending.clear();
        }

        function ensureWorker() {
            if (!supported) {
                return null;
            }
            if (worker) {
                return worker;
            }
            const workerScript = [
                'self.onmessage = function(event) {',
                '  var data = event.data || {};',
                '  var id = data.id;',
                '  var text = typeof data.text === "string" ? data.text : "";',
                '  try {',
                '    var payload = JSON.parse(text);',
                '    self.postMessage({ id: id, ok: true, payload: payload });',
                '  } catch (error) {',
                '    self.postMessage({ id: id, ok: false, error: error && error.message ? error.message : "json parse failed" });',
                '  }',
                '};',
            ].join('\n');
            const blobUrl = URL.createObjectURL(new Blob([workerScript], { type: 'application/javascript' }));
            worker = new Worker(blobUrl);
            URL.revokeObjectURL(blobUrl);

            worker.onmessage = (event) => {
                const data = event && event.data ? event.data : {};
                const id = Number(data.id);
                if (!Number.isFinite(id) || !pending.has(id)) {
                    return;
                }
                const entry = pending.get(id);
                pending.delete(id);
                clearTimeout(entry.timerId);
                if (data.ok) {
                    entry.resolve(data.payload || {});
                    return;
                }
                entry.reject(new Error(data.error || 'JSON parse failed'));
            };
            worker.onerror = () => {
                const error = new Error('JSON worker failed');
                clearPending(error);
                if (worker) {
                    worker.terminate();
                    worker = null;
                }
            };
            return worker;
        }

        function parse(text) {
            const raw = typeof text === 'string' ? text : String(text || '');
            return new Promise((resolve, reject) => {
                const targetWorker = ensureWorker();
                if (!targetWorker) {
                    reject(new Error('JSON worker unavailable'));
                    return;
                }
                const id = seq + 1;
                seq = id;
                const timerId = setTimeout(() => {
                    if (!pending.has(id)) {
                        return;
                    }
                    pending.delete(id);
                    reject(new Error('JSON worker timeout'));
                }, timeoutMs);
                pending.set(id, { resolve, reject, timerId });
                targetWorker.postMessage({ id, text: raw });
            });
        }

        function dispose() {
            clearPending(new Error('JSON worker disposed'));
            if (worker) {
                worker.terminate();
                worker = null;
            }
        }

        return {
            parse,
            dispose,
            isAvailable: function () {
                return supported;
            },
        };
    }

    // 纸张滚动动效绑定器：每帧仅写一次 transform，减少样式系统压力。
    function createPaperScrollMotionBinder(options) {
        const settings = options || {};
        const minDelta = Math.max(0.1, toFiniteNumber(settings.minDelta, 0.5));
        const relaxDelayMs = Math.max(16, Math.round(toFiniteNumber(settings.relaxDelayMs, 96)));
        const perspectivePx = Math.max(200, toFiniteNumber(settings.perspectivePx, 920));
        const shiftFactor = toFiniteNumber(settings.shiftFactor, 0.24);
        const tiltFactor = toFiniteNumber(settings.tiltFactor, 0.05);
        const intensityDivisor = Math.max(1, toFiniteNumber(settings.intensityDivisor, 34));
        const maxShiftPx = Math.max(1, toFiniteNumber(settings.maxShiftPx, 6));
        const maxTiltDeg = Math.max(0.1, toFiniteNumber(settings.maxTiltDeg, 1.2));
        const maxScaleLoss = Math.max(0.0001, toFiniteNumber(settings.maxScaleLoss, 0.004));

        return function bindPaperScrollMotion(node) {
            if (!node || node.dataset.paperScrollBound === '1') return;
            node.dataset.paperScrollBound = '1';
            node.classList.add('paper-scroll-surface');

            let lastTop = node.scrollTop;
            let rafId = 0;
            let releaseTimer = 0;
            let pendingDelta = 0;

            const relax = () => {
                node.style.removeProperty('transform');
                node.classList.remove('is-paper-scrolling');
            };

            const apply = () => {
                rafId = 0;
                const delta = pendingDelta;
                pendingDelta = 0;

                const intensity = Math.min(1, Math.abs(delta) / intensityDivisor);
                const shiftY = Math.max(-maxShiftPx, Math.min(maxShiftPx, -delta * shiftFactor));
                const tiltX = Math.max(-maxTiltDeg, Math.min(maxTiltDeg, delta * tiltFactor));
                const scale = 1 - intensity * maxScaleLoss;

                node.style.transform = `perspective(${perspectivePx}px) translate3d(0, ${shiftY.toFixed(2)}px, 0) rotateX(${tiltX.toFixed(2)}deg) scale(${scale.toFixed(4)})`;
                node.classList.add('is-paper-scrolling');
            };

            node.addEventListener('scroll', () => {
                const nowTop = node.scrollTop;
                const delta = nowTop - lastTop;
                lastTop = nowTop;
                if (Math.abs(delta) < minDelta) return;

                pendingDelta = delta;
                if (!rafId) {
                    rafId = requestAnimationFrame(apply);
                }
                if (releaseTimer) {
                    clearTimeout(releaseTimer);
                }
                releaseTimer = setTimeout(relax, relaxDelayMs);
            }, { passive: true });
        };
    }

    global.MobilePerformanceUtils = Object.freeze({
        scheduleDoubleAnimationFrame,
        createJsonParseWorkerPool,
        createPaperScrollMotionBinder,
    });
})(window);
