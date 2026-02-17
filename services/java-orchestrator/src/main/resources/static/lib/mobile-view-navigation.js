(function (global) {
    'use strict';

    function toFiniteNumber(value, fallback) {
        const num = Number(value);
        if (Number.isFinite(num)) {
            return num;
        }
        return Number(fallback) || 0;
    }

    function createMobileViewNavigation(options) {
        const settings = options || {};
        const state = settings.state;
        if (!state || typeof state !== 'object') {
            throw new Error('[mobile-view-navigation] state is required');
        }

        const viewHistoryMax = Math.max(8, Math.round(toFiniteNumber(settings.viewHistoryMax, 24)));
        const edgeBackMotion = settings.edgeBackMotion || {};
        const edgeBackTransition = String(settings.edgeBackTransition || '');
        const edgeBackCommitSettleDurationMs = Math.max(120, Math.round(toFiniteNumber(settings.edgeBackCommitSettleDurationMs, 250)));

        const onViewChanged = typeof settings.onViewChanged === 'function' ? settings.onViewChanged : function () {};
        const onReadingViewLeave = typeof settings.onReadingViewLeave === 'function' ? settings.onReadingViewLeave : function () {};
        const onViewPushed = typeof settings.onViewPushed === 'function' ? settings.onViewPushed : function () {};
        const onBlockedView = typeof settings.onBlockedView === 'function' ? settings.onBlockedView : function () {};

        function normalizeViewId(viewId) {
            const id = String(viewId || '').toLowerCase();
            if (id === 'tasks' || id === 'reading' || id === 'outline') {
                return id;
            }
            return 'tasks';
        }

        function pushViewHistory(viewId) {
            const normalized = normalizeViewId(viewId);
            const stack = state.viewHistoryStack || (state.viewHistoryStack = []);
            if (stack.length > 0 && stack[stack.length - 1] === normalized) {
                return;
            }
            stack.push(normalized);
            if (stack.length > viewHistoryMax) {
                stack.splice(0, stack.length - viewHistoryMax);
            }
        }

        function peekViewHistoryTarget(currentView) {
            const normalizedCurrent = normalizeViewId(currentView);
            const stack = state.viewHistoryStack || [];
            for (let i = stack.length - 1; i >= 0; i -= 1) {
                const candidate = normalizeViewId(stack[i]);
                if (candidate && candidate !== normalizedCurrent) {
                    return candidate;
                }
            }
            return '';
        }

        function consumeViewHistoryTarget(currentView, expectedTarget) {
            const normalizedCurrent = normalizeViewId(currentView);
            const normalizedTarget = normalizeViewId(expectedTarget);
            const stack = state.viewHistoryStack || [];
            while (stack.length > 0) {
                const candidate = normalizeViewId(stack.pop());
                if (!candidate || candidate === normalizedCurrent) {
                    continue;
                }
                if (candidate === normalizedTarget) {
                    return;
                }
            }
        }

        function pushView(viewId, options) {
            const requestedView = normalizeViewId(viewId);
            const previousView = normalizeViewId(state.currentView);
            const blockedByMissingTask = (requestedView === 'reading' || requestedView === 'outline') && !state.currentTaskId;
            const nextView = blockedByMissingTask ? 'tasks' : requestedView;
            const historyMode = blockedByMissingTask
                ? 'none'
                : (options && options.historyMode ? String(options.historyMode) : 'push');

            if (nextView !== previousView) {
                if (historyMode === 'pop') {
                    consumeViewHistoryTarget(previousView, nextView);
                } else if (historyMode !== 'none') {
                    pushViewHistory(previousView);
                }
            }

            state.currentView = nextView;
            document.querySelectorAll('.app-view[data-view]').forEach((view) => {
                const active = view.getAttribute('data-view') === nextView;
                view.classList.toggle('active', active);
            });

            if (blockedByMissingTask) {
                onBlockedView(requestedView);
            }

            onViewChanged(nextView, previousView);

            if (nextView !== 'reading') {
                onReadingViewLeave();
            }

            onViewPushed(nextView, previousView);
        }

        function resolveEdgeBackTarget(fromView) {
            if (fromView === 'reading') {
                // 阅读态右滑返回固定回任务列表，避免被历史栈中的临时页面干扰。
                return 'tasks';
            }
            if (fromView !== 'outline') {
                return '';
            }
            return peekViewHistoryTarget(fromView);
        }

        function clampEdgeMotion(value, min, max) {
            return Math.max(min, Math.min(max, value));
        }

        function buildEdgeBackTransition(velocityX) {
            const safeVelocity = clampEdgeMotion(Number(velocityX) || 0, 0, 1.2);
            const duration = Math.round(168 - (safeVelocity * 42));
            const safeDuration = clampEdgeMotion(duration, 122, 168);
            return {
                text: `transform ${safeDuration}ms cubic-bezier(0.16, 0.86, 0.18, 1.12), opacity ${safeDuration}ms cubic-bezier(0.2, 0.8, 0.2, 1), filter ${safeDuration}ms cubic-bezier(0.2, 0.8, 0.2, 1)`,
                cleanupMs: safeDuration + 40,
            };
        }

        function applyEdgeBackCommitSettle(viewId, velocityX) {
            const view = document.querySelector(`.app-view[data-view="${viewId}"]`);
            if (!view || typeof view.animate !== 'function') return;
            const safeVelocity = clampEdgeMotion(Number(velocityX) || 0, 0, 1.2);
            const backShift = -Math.round(6 + (safeVelocity * 8));
            const forwardShift = Math.max(1, Math.round(1 + (safeVelocity * 2.8)));
            const duration = Math.round(edgeBackCommitSettleDurationMs - (safeVelocity * 34));
            // 进入目标页时做一次极轻的“超调回弹”，避免机械的直线到位。
            view.animate([
                { transform: `translateX(${backShift}px) translateY(0)`, filter: 'brightness(0.965)' },
                { transform: `translateX(${forwardShift}px) translateY(0)`, filter: 'brightness(1.008)', offset: 0.72 },
                { transform: 'translateX(0) translateY(0)', filter: 'brightness(1)' },
            ], {
                duration: clampEdgeMotion(duration, 190, 250),
                easing: 'cubic-bezier(0.24, 0.84, 0.2, 1.12)',
                fill: 'none',
            });
        }

        function computeEdgeBackOffset(dx, velocityX) {
            const safeDx = Math.max(0, dx);
            const safeVelocity = clampEdgeMotion(Number(velocityX) || 0, 0, 1.2);
            const velocityBoost = safeVelocity * 20;
            // 指尖建压阶段故意稍慢，随后快速跟随；velocityBoost 用于“甩动感”。
            const eased = Number(edgeBackMotion.maxOffsetPx || 132) * (1 - Math.exp(-(safeDx + velocityBoost) / 76));
            return clampEdgeMotion(eased, 0, Number(edgeBackMotion.maxOffsetPx || 132));
        }

        function computeEdgeBackProgress(offsetX, velocityX) {
            const raw = Math.max(0, Math.min(1, offsetX / Number(edgeBackMotion.progressDistancePx || 128)));
            // 非线性映射：起步慢一点，随后更快接近完成态，减少“机器匀速感”。
            const curved = Math.pow(raw, 1.24);
            const safeVelocity = clampEdgeMotion(Number(velocityX) || 0, 0, 1.2);
            const velocityBias = 0.075 * safeVelocity * (1 - curved);
            return clampEdgeMotion(curved + velocityBias, 0, 1);
        }

        function applyEdgeBackPreview(viewId, offsetX, progress) {
            const view = document.querySelector(`.app-view[data-view="${viewId}"]`);
            if (!view) return;
            const safeProgress = Math.max(0, Math.min(1, progress));
            view.style.transition = 'none';
            view.style.setProperty('--nav-gesture-x', `${Math.max(0, offsetX).toFixed(1)}px`);
            const activeOpacityFloor = Number(edgeBackMotion.activeOpacityFloor || 0.9);
            const activeOpacityDrop = Number(edgeBackMotion.activeOpacityDrop || 0.1);
            const opacity = Math.max(activeOpacityFloor, 1 - (safeProgress * activeOpacityDrop));
            view.style.opacity = String(opacity);
        }

        function applyEdgeBackUnderlayPreview(viewId, progress) {
            const view = document.querySelector(`.app-view[data-view="${viewId}"]`);
            if (!view) return;
            const safeProgress = Math.max(0, Math.min(1, progress));
            const underlayMaxDim = Number(edgeBackMotion.underlayMaxDim || 0.18);
            const underlayMaxParallaxPx = Number(edgeBackMotion.underlayMaxParallaxPx || 14);
            const dim = underlayMaxDim * (1 - safeProgress);
            const parallaxX = -underlayMaxParallaxPx * (1 - safeProgress);
            view.classList.add('edge-underlay');
            view.style.transition = 'none';
            view.style.setProperty('--nav-underlay-dim', dim.toFixed(3));
            view.style.setProperty('--nav-underlay-x', `${parallaxX.toFixed(1)}px`);
            view.style.opacity = '1';
        }

        function resetEdgeBackUnderlayPreview(viewId, options) {
            const view = document.querySelector(`.app-view[data-view="${viewId}"]`);
            if (!view) return;
            const animated = !(options && options.immediate);
            const velocityX = options && Number(options.velocityX) ? Number(options.velocityX) : 0;
            const transitionInfo = buildEdgeBackTransition(velocityX);
            if (!view.classList.contains('edge-underlay')) {
                view.style.removeProperty('--nav-underlay-dim');
                view.style.removeProperty('--nav-underlay-x');
                view.style.removeProperty('opacity');
                view.style.removeProperty('transition');
                return;
            }
            if (!animated) {
                view.classList.remove('edge-underlay');
                view.style.removeProperty('--nav-underlay-dim');
                view.style.removeProperty('--nav-underlay-x');
                view.style.removeProperty('opacity');
                view.style.removeProperty('transition');
                return;
            }
            const underlayMaxDim = Number(edgeBackMotion.underlayMaxDim || 0.18);
            const underlayMaxParallaxPx = Number(edgeBackMotion.underlayMaxParallaxPx || 14);
            view.style.transition = transitionInfo.text || edgeBackTransition;
            view.style.setProperty('--nav-underlay-dim', String(underlayMaxDim));
            view.style.setProperty('--nav-underlay-x', `${-underlayMaxParallaxPx}px`);
            view.style.opacity = '0';
            setTimeout(() => {
                view.classList.remove('edge-underlay');
                view.style.removeProperty('--nav-underlay-dim');
                view.style.removeProperty('--nav-underlay-x');
                view.style.removeProperty('opacity');
                view.style.removeProperty('transition');
            }, transitionInfo.cleanupMs);
        }

        function resetEdgeBackPreview(viewId, options) {
            const view = document.querySelector(`.app-view[data-view="${viewId}"]`);
            if (!view) return;
            const velocityX = options && Number(options.velocityX) ? Number(options.velocityX) : 0;
            const transitionInfo = buildEdgeBackTransition(velocityX);
            view.style.transition = transitionInfo.text || edgeBackTransition;
            view.style.setProperty('--nav-gesture-x', '0px');
            view.style.removeProperty('opacity');
            setTimeout(() => {
                view.style.removeProperty('transition');
            }, transitionInfo.cleanupMs);
        }

        function finalizeEdgeBackGesture(cancelled) {
            const gesture = state.edgeBackGesture;
            if (!gesture) return;
            state.edgeBackGesture = null;
            const dx = gesture.lastX - gesture.startX;
            const dy = gesture.lastY - gesture.startY;
            const velocityX = Math.max(0, Number(gesture.velocityX) || 0);
            const previewProgress = Number(gesture.previewProgress) || 0;
            const projectedDx = dx + (velocityX * Number(edgeBackMotion.projectVelocityWindowMs || 56));
            const flickCommit = velocityX >= Number(edgeBackMotion.flickVelocityPxPerMs || 0.5)
                && dx >= Number(edgeBackMotion.flickMinDxPx || 18);
            const shouldCommit = !cancelled
                && gesture.active
                && (
                    dx >= Number(edgeBackMotion.commitDistancePx || 84)
                    || projectedDx >= Number(edgeBackMotion.commitDistancePx || 84)
                    || previewProgress >= Number(edgeBackMotion.commitProgress || 0.72)
                    || flickCommit
                )
                && Math.abs(dx) > (Math.abs(dy) * Number(edgeBackMotion.commitDirectionRatio || 1.08));
            resetEdgeBackPreview(gesture.fromView, { velocityX });
            if (shouldCommit) {
                pushView(gesture.toView, { historyMode: 'pop' });
                if (gesture.toView) {
                    resetEdgeBackUnderlayPreview(gesture.toView, { immediate: true });
                    applyEdgeBackCommitSettle(gesture.toView, velocityX);
                }
                return;
            }
            if (gesture.toView) {
                resetEdgeBackUnderlayPreview(gesture.toView, { immediate: false, velocityX });
            }
        }

        function bindEdgeBackNavigation() {
            if (document.body.dataset.edgeBackBound === '1') return;
            document.body.dataset.edgeBackBound = '1';

            document.addEventListener('touchstart', (event) => {
                if (!event.touches || event.touches.length !== 1) return;
                const fromView = state.currentView;
                const toView = resolveEdgeBackTarget(fromView);
                if (!toView) return;
                const touch = event.touches[0];
                if (!touch || touch.clientX > Number(edgeBackMotion.hotZonePx || 24)) return;
                const target = event.target;
                if (target && target.closest && target.closest('input, textarea, select, video, .comment-modal')) {
                    return;
                }
                state.edgeBackGesture = {
                    fromView,
                    toView,
                    startX: touch.clientX,
                    startY: touch.clientY,
                    lastX: touch.clientX,
                    lastY: touch.clientY,
                    active: false,
                    previewProgress: 0,
                    velocityX: 0,
                    lastSampleAt: (window.performance && typeof window.performance.now === 'function')
                        ? window.performance.now()
                        : Date.now(),
                    lastSampleDx: 0,
                };
            }, { passive: true });

            document.addEventListener('touchmove', (event) => {
                const gesture = state.edgeBackGesture;
                if (!gesture || !event.touches || event.touches.length !== 1) return;
                const touch = event.touches[0];
                if (!touch) return;
                gesture.lastX = touch.clientX;
                gesture.lastY = touch.clientY;
                const dx = gesture.lastX - gesture.startX;
                const dy = gesture.lastY - gesture.startY;
                if (!gesture.active) {
                    if (dx < Number(edgeBackMotion.activateMinDxPx || 8)) return;
                    if (Math.abs(dx) < Math.abs(dy) * Number(edgeBackMotion.activateDirectionRatio || 1.2)) return;
                    gesture.active = true;
                }
                if (!gesture.active) return;
                event.preventDefault();
                const now = (window.performance && typeof window.performance.now === 'function')
                    ? window.performance.now()
                    : Date.now();
                const dt = Math.max(8, now - (gesture.lastSampleAt || now));
                const sampleDx = Math.max(0, dx);
                const instVelocity = (sampleDx - (gesture.lastSampleDx || 0)) / dt;
                const smoothPrev = Number(gesture.velocityX) || 0;
                gesture.velocityX = (smoothPrev * 0.72) + (instVelocity * 0.28);
                gesture.lastSampleAt = now;
                gesture.lastSampleDx = sampleDx;
                const offsetX = computeEdgeBackOffset(dx, gesture.velocityX);
                const progress = computeEdgeBackProgress(offsetX, gesture.velocityX);
                gesture.previewProgress = progress;
                applyEdgeBackPreview(gesture.fromView, offsetX, progress);
                applyEdgeBackUnderlayPreview(gesture.toView, progress);
            }, { passive: false });

            document.addEventListener('touchend', () => finalizeEdgeBackGesture(false), { passive: true });
            document.addEventListener('touchcancel', () => finalizeEdgeBackGesture(true), { passive: true });
        }

        return {
            normalizeViewId,
            pushViewHistory,
            peekViewHistoryTarget,
            consumeViewHistoryTarget,
            pushView,
            resolveEdgeBackTarget,
            finalizeEdgeBackGesture,
            bindEdgeBackNavigation,
        };
    }

    global.mobileViewNavigation = Object.freeze({
        create: createMobileViewNavigation,
    });
})(window);
