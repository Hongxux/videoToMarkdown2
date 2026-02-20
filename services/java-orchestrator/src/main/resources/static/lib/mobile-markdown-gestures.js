(function (global) {
    'use strict';

    const DEFAULT_CONFIG = Object.freeze({
        holdTriggerMs: 1000,
        moveCancelPx: 10,
        tapTolerancePx: 12,
        doubleTapWindowMs: 330,
    });

    // 手势动作映射：集中定义语义，后续改手势只需改这里。
    const DEFAULT_GESTURE_ACTION_MAP = Object.freeze({
        doubleTap: null,
        longPress: 'copy',
    });

    // 滑动操作手感参数：阈值、阻尼、速度判定等集中配置。
    const SWIPE_PHYSICS = Object.freeze({
        activateMinDxPx: 26,
        activateDirectionRatio: 2.2,
        commitRatio: 0.35,
        deleteCommitRatio: 0.43,
        annotateCommitRatio: 0.35,
        flickVelocityPxPerMs: 0.4,
        flickMinDxPx: 20,
        deleteFlickVelocityPxPerMs: 0.52,
        deleteFlickMinDxPx: 36,
        flickCommitMinProgressRatio: 0.72,
        // 橡皮筋阻尼核心参数：limit 控制上限，curve 控制阻尼增长速度。
        rubberBandLimit: 120,
        rubberBandCurve: 0.64,
        rubberBandTensionPx: 7.2,
        rubberBandBreathPx: 2.8,
        rubberBandBreathHz: 2.4,
        // 左滑删除专用“纸感阻尼”：越接近撕裂点，阻力越明显。
        tearResistanceStartRatio: 0.56,
        tearResistanceCurve: 1.18,
        tearSnapBoostPx: 12,
        springBackMs: 500,
        settleBreathMs: 420,
        // 删除动效延长到“揉皱+扫落”所需时长。
        slideOutMs: 470,
        collapseMs: 320,
    });

    const ICON_DELETE_SVG = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.3" stroke-linecap="round" stroke-linejoin="round"><path d="M3 6h18"/><path d="M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6"/><path d="M10 11v6"/><path d="M14 11v6"/></svg>';
    const ICON_ANNOTATE_SVG = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.3" stroke-linecap="round" stroke-linejoin="round"><path d="M12 20h9"/><path d="M16.5 3.5a2.121 2.121 0 0 1 3 3L7 19l-4 1 1-4 12.5-12.5z"/></svg>';
    const SWIPE_ICON_BASE = Object.freeze({
        scale: 0.84,
        opacity: 0.58,
    });
    const SWIPE_ICON_COMMITTED = Object.freeze({
        scale: 1.24,
        opacity: 1,
    });
    const SWIPE_ICON_RESET_TRANSITION_MS = 320;
    const SWIPE_PREWARM_IDLE_TIMEOUT_MS = 120;
    const SWIPE_PREWARM_NEIGHBOR_OFFSETS = Object.freeze([-1, 1]);
    const SWIPE_DOM_CACHE_LIMIT = 28;
    const SWIPE_DOM_PRUNE_IDLE_TIMEOUT_MS = 180;

    function resolveConfig(rawConfig) {
        const next = Object.assign({}, DEFAULT_CONFIG, rawConfig || {});
        next.holdTriggerMs = Math.max(120, Number(next.holdTriggerMs) || DEFAULT_CONFIG.holdTriggerMs);
        next.moveCancelPx = Math.max(4, Number(next.moveCancelPx) || DEFAULT_CONFIG.moveCancelPx);
        next.tapTolerancePx = Math.max(4, Number(next.tapTolerancePx) || DEFAULT_CONFIG.tapTolerancePx);
        next.doubleTapWindowMs = Math.max(120, Number(next.doubleTapWindowMs) || DEFAULT_CONFIG.doubleTapWindowMs);
        return next;
    }

    // ─── 滑动 DOM 基础设施 ────────────────────────────────────

    function ensureSwipeCellDom(card) {
        if (!card || card.dataset.swipeCellReady === '1') {
            return card;
        }
        let contentWrap = card.querySelector('.swipe-content');
        if (!contentWrap) {
            contentWrap = document.createElement('div');
            contentWrap.className = 'swipe-content';
            while (card.firstChild) {
                contentWrap.appendChild(card.firstChild);
            }
            card.appendChild(contentWrap);
        }

        if (!card.querySelector('.swipe-backdrop.swipe-delete')) {
            const delBg = document.createElement('div');
            delBg.className = 'swipe-backdrop swipe-delete';
            delBg.innerHTML = `<svg class="swipe-backdrop-icon is-live" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.3" stroke-linecap="round" stroke-linejoin="round">${ICON_DELETE_SVG.replace(/<\/?svg[^>]*>/g, '')}</svg>`;
            card.insertBefore(delBg, contentWrap);
        }
        if (!card.querySelector('.swipe-backdrop.swipe-annotate')) {
            const annBg = document.createElement('div');
            annBg.className = 'swipe-backdrop swipe-annotate';
            annBg.innerHTML = `<svg class="swipe-backdrop-icon is-live" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.3" stroke-linecap="round" stroke-linejoin="round">${ICON_ANNOTATE_SVG.replace(/<\/?svg[^>]*>/g, '')}</svg>`;
            card.insertBefore(annBg, contentWrap);
        }

        card.dataset.swipeCellReady = '1';
        return card;
    }

    function getSwipeContent(card) {
        return card && card.querySelector('.swipe-content');
    }

    // 图标视觉由 CSS 负责渲染，JS 仅写变量与状态类，便于统一调参。
    function applyBackdropIconVisual(icon, scale, opacity, options = {}) {
        if (!icon) return;
        icon.style.setProperty('--swipe-icon-scale', Number(scale).toFixed(3));
        icon.style.setProperty('--swipe-icon-opacity', Number(opacity).toFixed(2));
        icon.classList.toggle('is-resetting', !!options.resetting);
        icon.classList.toggle('is-committed', !!options.committed);
        icon.classList.add('is-live');
    }

    function resetBackdropIconVisual(icon, options = {}) {
        applyBackdropIconVisual(icon, SWIPE_ICON_BASE.scale, SWIPE_ICON_BASE.opacity, {
            resetting: !!options.animate,
            committed: false,
        });
    }

    function commitBackdropIconVisual(icon) {
        applyBackdropIconVisual(icon, SWIPE_ICON_COMMITTED.scale, SWIPE_ICON_COMMITTED.opacity, {
            resetting: false,
            committed: true,
        });
    }

    function resetBackdropIcons(card, options = {}) {
        if (!card) return;
        card.querySelectorAll('.swipe-backdrop-icon').forEach((icon) => {
            resetBackdropIconVisual(icon, options);
        });
    }

    function clearBackdropIconResetState(card) {
        if (!card) return;
        card.querySelectorAll('.swipe-backdrop-icon').forEach((icon) => {
            icon.classList.remove('is-resetting');
        });
    }

    // 更新 Backdrop 状态（图标缩放、透明度）
    function updateBackdropVisuals(card, direction, progress, isCommitted) {
        if (!card) return;
        const delBd = card.querySelector('.swipe-backdrop.swipe-delete');
        const annBd = card.querySelector('.swipe-backdrop.swipe-annotate');
        const delIcon = delBd ? delBd.querySelector('.swipe-backdrop-icon') : null;
        const annIcon = annBd ? annBd.querySelector('.swipe-backdrop-icon') : null;
        
        // 激活/隐藏背景
        if (delBd) delBd.classList.toggle('active', direction === 'left');
        if (annBd) annBd.classList.toggle('active', direction === 'right');
        if (delBd) delBd.classList.toggle('committed', direction === 'left' && !!isCommitted);
        if (annBd) annBd.classList.toggle('committed', direction === 'right' && !!isCommitted);

        // 右滑批注使用“便签伸出”隐喻：根据进度驱动纸片位置与透明度。
        if (annBd) {
            const noteProgress = direction === 'right' ? Math.min(1.2, Math.max(0, progress)) : 0;
            annBd.style.setProperty('--swipe-note-progress', noteProgress.toFixed(3));
        }

        // 动态调整图标
        const targetIcon = direction === 'left' ? delIcon : annIcon;
        const inactiveIcon = direction === 'left' ? annIcon : delIcon;
        resetBackdropIconVisual(inactiveIcon, { animate: false });
        if (!targetIcon) return;
        const safeProgress = Math.min(1.5, Math.max(0, progress));
        let scale = SWIPE_ICON_BASE.scale + (0.34 * Math.min(1, safeProgress));
        if (isCommitted) {
            scale = SWIPE_ICON_COMMITTED.scale;
        } else if (safeProgress > 1) {
            scale += (safeProgress - 1) * 0.12;
        }
        const opacity = SWIPE_ICON_BASE.opacity + (0.42 * Math.min(1, safeProgress));
        applyBackdropIconVisual(targetIcon, scale, opacity, { resetting: false, committed: isCommitted });
    }

    function hideAllBackdrops(card, options = {}) {
        if (!card) return;
        card.querySelectorAll('.swipe-backdrop').forEach((bd) => {
            bd.classList.remove('active', 'committed');
        });
        const annBd = card.querySelector('.swipe-backdrop.swipe-annotate');
        if (annBd) {
            annBd.classList.remove('slap');
            annBd.style.setProperty('--swipe-note-progress', '0');
        }
        resetBackdropIcons(card, { animate: !!options.animateIcons });
    }

    function cleanSwipeClasses(contentEl) {
        if (!contentEl) return;
        contentEl.classList.remove('swiping', 'spring-back', 'slide-out-left', 'slide-out-right', 'breath-settle', 'paper-crumple');
        contentEl.style.removeProperty('--swipe-start-x');
        contentEl.style.removeProperty('--swipe-exit-x');
        contentEl.style.removeProperty('--swipe-exit-rot');
        contentEl.style.transform = '';
    }

    // ─── 缩合指捏快捷键 (保持不变) ────────────────────────────────
    function bindMarkdownPinchOutlineShortcut(markdownBody, deps) {
        // ... (与之前相同，略微简化代码量，只保留核心逻辑) ...
        // 为了完整性，这里必须包含全量代码，因为是 write_to_file 覆盖。
        // 复用之前的逻辑：
        if (!markdownBody || markdownBody.dataset.pinchOutlineBound === '1') return;
        markdownBody.dataset.pinchOutlineBound = '1';
        const { state, touchDistance, switchTab, closeTransientUi } = deps;

        markdownBody.addEventListener('touchstart', (e) => {
            if (!e.touches || e.touches.length !== 2) return;
            state.pinchStartDistance = touchDistance(e.touches[0], e.touches[1]);
            state.pinchTriggered = false;
        }, { passive: true });

        markdownBody.addEventListener('touchmove', (e) => {
            if (!e.touches || e.touches.length !== 2 || state.pinchStartDistance <= 0 || state.pinchTriggered) return;
            state.pinchLatestDistance = touchDistance(e.touches[0], e.touches[1]);
            if (!state.pinchMoveRafId) {
                state.pinchMoveRafId = requestAnimationFrame(() => {
                    state.pinchMoveRafId = 0;
                    if (state.pinchTriggered || state.pinchStartDistance <= 0) return;
                    if (state.pinchLatestDistance <= state.pinchStartDistance * 0.78) {
                        state.pinchTriggered = true;
                        switchTab('outline');
                        closeTransientUi();
                    }
                });
            }
        }, { passive: true });

        const resetPinch = () => {
            if (state.pinchMoveRafId) cancelAnimationFrame(state.pinchMoveRafId);
            state.pinchMoveRafId = 0;
            state.pinchStartDistance = 0;
            state.pinchTriggered = false;
        };
        markdownBody.addEventListener('touchend', resetPinch, { passive: true });
        markdownBody.addEventListener('touchcancel', resetPinch, { passive: true });
    }

    // ─── 段落手势（高保真物理重写） ────────────────────────────────

    function bindMarkdownParagraphGestures(markdownBody, deps) {
        if (!markdownBody || markdownBody.dataset.paragraphGestureBound === '1') return;
        markdownBody.dataset.paragraphGestureBound = '1';
        const {
            state, config, edgeBackHotZonePx,
            isParagraphActionAllowed, isParagraphGestureTargetBlocked,
            resolveGestureParagraphTarget, resolveGestureParagraphIndex,
            copyParagraphText, clearParagraphHoldCue, getParagraphCardByIndex,
            triggerParagraphHoldActivated, toggleParagraphFavorite,
            canHandleParagraphSwipeGesture, openInlineStickyNote, deleteLineAtIndex,
            onDeleteSwipeCommitted
        } = deps;

        // 手势动作映射支持外部覆盖，默认语义由 DEFAULT_GESTURE_ACTION_MAP 提供。
        const gestureActionMap = Object.assign({}, DEFAULT_GESTURE_ACTION_MAP, deps && deps.gestureActionMap ? deps.gestureActionMap : {});
        // 收藏切换去重状态：用于抑制 touch 双击与浏览器合成 dblclick 的重复触发。
        // 取舍：仅按“同一段落 + 短时间窗口”拦截，避免误伤用户连续手动切换。
        let lastFavoriteToggle = { index: -1, at: 0, source: '' };
        const favoriteCrossSourceGuardMs = Math.max((Number(config.doubleTapWindowMs) || DEFAULT_CONFIG.doubleTapWindowMs) + 80, 420);
        const favoriteSameSourceGuardMs = 120;
        const defaultGestureBlockSelector = [
            'button',
            'textarea',
            'input',
            'select',
            'option',
            'label',
            'a[href]',
            '[contenteditable=\"true\"]',
            '[data-concept-term]',
            '.concept-term',
            '.concept-card',
            '.inline-sticky-editor',
            '.inline-sticky-sheet',
        ].join(', ');

        function resolveGestureAction(trigger) {
            const action = gestureActionMap[trigger];
            return action === 'copy' || action === 'favorite' ? action : null;
        }

        function isGestureTargetBlocked(eventTarget) {
            if (typeof isParagraphGestureTargetBlocked === 'function') {
                return !!isParagraphGestureTargetBlocked(eventTarget);
            }
            return !!(eventTarget && eventTarget.closest && eventTarget.closest(defaultGestureBlockSelector));
        }

        function resolveGestureTarget(eventTarget) {
            if (typeof resolveGestureParagraphTarget === 'function') {
                const target = resolveGestureParagraphTarget(eventTarget);
                if (target && Number.isFinite(target.index) && target.index >= 0) {
                    const span = Number(target.lineSpan);
                    return {
                        index: target.index,
                        lineSpan: Number.isFinite(span) && span > 0 ? Math.floor(span) : 1,
                    };
                }
            }
            const index = typeof resolveGestureParagraphIndex === 'function'
                ? resolveGestureParagraphIndex(eventTarget)
                : -1;
            if (!Number.isFinite(index) || index < 0) {
                return null;
            }
            return { index, lineSpan: 1 };
        }

        function shouldSkipFavoriteToggle(index, source) {
            const now = Date.now();
            const safeSource = source || 'unknown';
            const prev = lastFavoriteToggle;
            const sameIndex = prev && prev.index === index;
            if (sameIndex && prev.at > 0) {
                const delta = now - prev.at;
                const sameSource = prev.source === safeSource;
                if ((sameSource && delta <= favoriteSameSourceGuardMs)
                    || (!sameSource && delta <= favoriteCrossSourceGuardMs)) {
                    return true;
                }
            }
            lastFavoriteToggle = { index, at: now, source: safeSource };
            return false;
        }

        async function executeParagraphGestureAction(action, index, options = {}) {
            if (!Number.isFinite(index) || index < 0 || !action) return;
            if (action === 'favorite') {
                if (shouldSkipFavoriteToggle(index, options.source)) {
                    return;
                }
                await toggleParagraphFavorite(index, { anchorX: options.anchorX, anchorY: options.anchorY });
                return;
            }
            if (action === 'copy') {
                if (options.gesture) state.touchGesture = options.gesture;
                await copyParagraphText(index);
            }
        }

        // 核心手势状态机
        let activeGesture = null;
        let swipeNeighborPrewarmCancel = null;
        let swipeDomPruneCancel = null;
        const GESTURE_NO_SELECT_CLASS = 'gesture-no-select';

        function setGestureSelectionSuppressed(enabled) {
            if (!markdownBody || !markdownBody.classList) return;
            markdownBody.classList.toggle(GESTURE_NO_SELECT_CLASS, !!enabled);
        }

        function clearSwipeNeighborPrewarm() {
            if (typeof swipeNeighborPrewarmCancel === 'function') {
                swipeNeighborPrewarmCancel();
                swipeNeighborPrewarmCancel = null;
            }
        }

        function clearSwipeDomPrune() {
            if (typeof swipeDomPruneCancel === 'function') {
                swipeDomPruneCancel();
                swipeDomPruneCancel = null;
            }
        }

        function stampSwipeCellUsage(card) {
            if (!card || !card.dataset) return;
            card.dataset.swipeCellLastUsedAt = String(Date.now());
        }

        function teardownSwipeCellDom(card) {
            if (!card || card.dataset.swipeCellReady !== '1') return;
            const contentWrap = card.querySelector('.swipe-content');
            if (contentWrap) {
                while (contentWrap.firstChild) {
                    card.appendChild(contentWrap.firstChild);
                }
                contentWrap.remove();
            }
            card.querySelectorAll('.swipe-backdrop').forEach((node) => node.remove());
            card.classList.remove('swipe-interaction-ready');
            delete card.dataset.swipeCellReady;
            delete card.dataset.swipeCellLastUsedAt;
        }

        function scheduleSwipeDomPrune(excludeCard) {
            clearSwipeDomPrune();
            const run = () => {
                swipeDomPruneCancel = null;
                const readyCards = Array.from(markdownBody.querySelectorAll('.paragraph-card[data-swipe-cell-ready="1"]'));
                if (readyCards.length <= SWIPE_DOM_CACHE_LIMIT) {
                    return;
                }
                const protectedCards = new Set();
                if (excludeCard) protectedCards.add(excludeCard);
                if (activeGesture && activeGesture.prewarmedCard) protectedCards.add(activeGesture.prewarmedCard);
                if (activeGesture && activeGesture.swipeCard) protectedCards.add(activeGesture.swipeCard);
                if (activeGesture && activeGesture.holdCard) protectedCards.add(activeGesture.holdCard);
                const candidates = readyCards
                    .filter((card) => !protectedCards.has(card))
                    .sort((a, b) => (Number(a.dataset.swipeCellLastUsedAt) || 0) - (Number(b.dataset.swipeCellLastUsedAt) || 0));
                const evictCount = Math.max(0, readyCards.length - SWIPE_DOM_CACHE_LIMIT);
                for (let i = 0; i < evictCount && i < candidates.length; i += 1) {
                    teardownSwipeCellDom(candidates[i]);
                }
            };
            if (typeof window.requestIdleCallback === 'function'
                && typeof window.cancelIdleCallback === 'function') {
                const idleId = window.requestIdleCallback(run, { timeout: SWIPE_DOM_PRUNE_IDLE_TIMEOUT_MS });
                swipeDomPruneCancel = () => window.cancelIdleCallback(idleId);
                return;
            }
            const timeoutId = window.setTimeout(run, 36);
            swipeDomPruneCancel = () => window.clearTimeout(timeoutId);
        }

        function scheduleSwipeNeighborPrewarm(index) {
            clearSwipeNeighborPrewarm();
            const run = () => {
                swipeNeighborPrewarmCancel = null;
                SWIPE_PREWARM_NEIGHBOR_OFFSETS.forEach((offset) => {
                    const card = getParagraphCardByIndex(index + offset);
                    if (!card) return;
                    ensureSwipeCellDom(card);
                    stampSwipeCellUsage(card);
                });
                scheduleSwipeDomPrune(null);
            };
            if (typeof window.requestIdleCallback === 'function'
                && typeof window.cancelIdleCallback === 'function') {
                const idleId = window.requestIdleCallback(run, { timeout: SWIPE_PREWARM_IDLE_TIMEOUT_MS });
                swipeNeighborPrewarmCancel = () => window.cancelIdleCallback(idleId);
                return;
            }
            const timeoutId = window.setTimeout(run, 24);
            swipeNeighborPrewarmCancel = () => window.clearTimeout(timeoutId);
        }

        function prewarmSwipeInteractionCard(card) {
            if (!card) return null;
            const wasReady = card.dataset.swipeCellReady === '1';
            card.classList.add('swipe-interaction-ready');
            ensureSwipeCellDom(card);
            stampSwipeCellUsage(card);
            if (!wasReady) {
                scheduleSwipeDomPrune(card);
            }
            return card;
        }

        function clearSwipeInteractionCard(gesture) {
            if (!gesture) return;
            const primaryCard = gesture.prewarmedCard;
            if (primaryCard && primaryCard.classList) {
                primaryCard.classList.remove('swipe-interaction-ready');
            }
            if (gesture.swipeCard && gesture.swipeCard !== primaryCard && gesture.swipeCard.classList) {
                gesture.swipeCard.classList.remove('swipe-interaction-ready');
            }
            scheduleSwipeDomPrune(null);
        }

        function hasActiveTextSelection() {
            const selection = window.getSelection && window.getSelection();
            return !!(selection && !selection.isCollapsed && String(selection.toString() || '').trim());
        }

        function isTouchInCardMargin(card, clientX, clientY) {
            if (!card || !card.getBoundingClientRect) return false;
            const rect = card.getBoundingClientRect();
            if (!Number.isFinite(rect.left) || !Number.isFinite(rect.right)) return false;
            if (clientX < rect.left || clientX > rect.right || clientY < rect.top || clientY > rect.bottom) return false;
            const marginPx = 20;
            return (
                (clientX - rect.left) <= marginPx
                || (rect.right - clientX) <= marginPx
                || (clientY - rect.top) <= marginPx
                || (rect.bottom - clientY) <= marginPx
            );
        }

        function canTriggerLongPressCopy(gesture) {
            if (!gesture) return false;
            // 仅在“触摸开始前已存在选区”时执行保护逻辑。
            // 原因：移动端长按过程中浏览器可能自动创建选区，若按当前时刻判断会误拦截复制。
            if (!gesture.selectionPresentAtStart) return true;
            return isTouchInCardMargin(gesture.holdCard, gesture.startX, gesture.startY);
        }

        function canTriggerLongPressAction(action, gesture) {
            if (action !== 'copy') return true;
            return canTriggerLongPressCopy(gesture);
        }

        function resolveSwipeCommitRatio(direction) {
            if (direction === 'left') {
                return SWIPE_PHYSICS.deleteCommitRatio;
            }
            if (direction === 'right') {
                return SWIPE_PHYSICS.annotateCommitRatio;
            }
            return SWIPE_PHYSICS.commitRatio;
        }

        function resolveSwipeCommitThreshold(cardWidth, direction) {
            return Math.max(1, (Number(cardWidth) || 0) * resolveSwipeCommitRatio(direction));
        }

        const updateSwipeFrame = () => {
            if (!activeGesture || !activeGesture.swipeActive) return;
            
            const g = activeGesture;
            const dx = g.currentX - g.startX; // 原始位移
            const absX = Math.abs(dx);
            const direction = dx < 0 ? 'left' : 'right';
            g.swipeDirection = direction;

            const commitThreshold = resolveSwipeCommitThreshold(g.swipeCardWidth, direction);
            const sign = dx >= 0 ? 1 : -1;
            const isDeleteDirection = direction === 'left';
            let displayDx = 0;

            if (absX <= commitThreshold) {
                const progress = Math.min(1, absX / Math.max(1, commitThreshold));
                // 阈值内保持跟手，同时略微提前降速，形成“橡皮筋拉紧”感。
                const eased = 1 - Math.pow(1 - progress, isDeleteDirection ? 0.8 : 0.88);
                // 左滑删除靠近提交阈值时额外增加阻力，模拟“纸张将要被撕开”前的拉扯。
                let nearCommitResistance = 0;
                if (isDeleteDirection && progress > SWIPE_PHYSICS.tearResistanceStartRatio) {
                    const tailProgress = (progress - SWIPE_PHYSICS.tearResistanceStartRatio)
                        / Math.max(0.001, 1 - SWIPE_PHYSICS.tearResistanceStartRatio);
                    nearCommitResistance = Math.pow(tailProgress, SWIPE_PHYSICS.tearResistanceCurve)
                        * SWIPE_PHYSICS.rubberBandTensionPx * 0.44;
                }
                displayDx = sign * ((commitThreshold * eased) - nearCommitResistance);
            } else {
                const excess = absX - commitThreshold;
                // 阈值外采用橡皮筋阻尼：拉得越远，单位位移反馈越小。
                const limit = isDeleteDirection
                    ? SWIPE_PHYSICS.rubberBandLimit * 0.86
                    : SWIPE_PHYSICS.rubberBandLimit;
                const curve = isDeleteDirection
                    ? SWIPE_PHYSICS.rubberBandCurve * 1.14
                    : SWIPE_PHYSICS.rubberBandCurve;
                const denom = limit + (excess * curve);
                const dampedExcess = (limit * excess) / Math.max(1, denom);
                const overRatio = Math.min(1, excess / Math.max(1, limit));
                const tensionKick = Math.sin(overRatio * Math.PI) * SWIPE_PHYSICS.rubberBandTensionPx;
                const tearSnap = isDeleteDirection
                    ? Math.sin(overRatio * Math.PI) * SWIPE_PHYSICS.tearSnapBoostPx * 0.34
                    : 0;
                displayDx = sign * (commitThreshold + dampedExcess + (tensionKick * 0.42) + tearSnap);
                if (overRatio > 0.08) {
                    const elapsedMs = performance.now() - (g.breathEpochMs || 0);
                    const wave = Math.sin((elapsedMs / 1000) * Math.PI * 2 * SWIPE_PHYSICS.rubberBandBreathHz);
                    displayDx += sign * wave * SWIPE_PHYSICS.rubberBandBreathPx * overRatio;
                }
            }
            g.swipeDx = displayDx;

            // 渲染位移
            if (g.swipeContentEl) {
                g.swipeContentEl.style.transform = `translateX(${displayDx.toFixed(2)}px)`;
            }

            // 计算进度供图标动画使用
            const progress = absX / commitThreshold;
            const isCommitted = absX >= commitThreshold;
            g.swipeCommitted = isCommitted;

            updateBackdropVisuals(g.swipeCard, direction, progress, isCommitted);

            g.rafId = requestAnimationFrame(updateSwipeFrame);
        };

        markdownBody.addEventListener('touchstart', (event) => {
            const longPressAction = resolveGestureAction('longPress');
            const touchStartDoubleTapAction = resolveGestureAction('doubleTap');
            const hasReadonlyGestureAction = (
                (longPressAction && isParagraphActionAllowed(longPressAction))
                || (touchStartDoubleTapAction && isParagraphActionAllowed(touchStartDoubleTapAction))
            );
            if (!hasReadonlyGestureAction) return;
            if (event.touches.length !== 1) return;
            if (isGestureTargetBlocked(event.target)) return;
            
            const touch = event.touches[0];
            const edgeGuard = (Number(edgeBackHotZonePx) || 24) + 6;
            if (state.currentView === 'content' && touch.clientX <= edgeGuard) return;

            const target = resolveGestureTarget(event.target);
            if (!target) return;
            const index = target.index;
            const touchCard = prewarmSwipeInteractionCard(getParagraphCardByIndex(index));
            scheduleSwipeNeighborPrewarm(index);

            // 清理旧状态
            if (state.touchGesture && state.touchGesture.longPressTimer) clearTimeout(state.touchGesture.longPressTimer);
            clearParagraphHoldCue(state.touchGesture);
            setGestureSelectionSuppressed(true);

            activeGesture = {
                index,
                lineSpan: target.lineSpan,
                startX: touch.clientX,
                startY: touch.clientY,
                currentX: touch.clientX,
                currentY: touch.clientY,
                lastX: touch.clientX,
                lastY: touch.clientY,
                // 记录触摸起点时的选区状态，避免长按过程中系统自动选区误伤复制手势。
                selectionPresentAtStart: hasActiveTextSelection(),
                // 速度采样
                history: [{ t: Date.now(), x: touch.clientX }],
                // 状态位
                hasMoved: false,
                longPressTriggered: false,
                holdArmedAt: 0,
                swipeActive: false,
                swipeCard: null,
                swipeContentEl: null,
                swipeCardWidth: 0,
                rafId: 0,
                breathEpochMs: performance.now(),
                prewarmedCard: touchCard,
            };
            
            // 长按计时器
            activeGesture.holdCard = touchCard || getParagraphCardByIndex(index);
            if (activeGesture.holdCard) activeGesture.holdCard.classList.add('touch-hold-cue');
            
            activeGesture.longPressTimer = setTimeout(() => {
                if (!activeGesture) return;
                activeGesture.longPressTimer = null;
                // 长按达到阈值只进入“待决态”，真正动作在 touchend 统一结算。
                if (hasActiveTextSelection()) {
                    return;
                }
                if (!(longPressAction && isParagraphActionAllowed(longPressAction)
                    && canTriggerLongPressAction(longPressAction, activeGesture))) {
                    return;
                }
                activeGesture.longPressTriggered = true;
                activeGesture.holdArmedAt = Date.now();
                if (longPressAction === 'copy') {
                    triggerParagraphHoldActivated(activeGesture.holdCard);
                }
            }, config.holdTriggerMs);

            state.touchGesture = activeGesture;
            state.touchScrollActive = true;
        }, { passive: true });

        markdownBody.addEventListener('selectstart', (event) => {
            if (!activeGesture) return;
            if (event.cancelable) event.preventDefault();
        });

        // TouchMove: 这里的关键是只更新数据，渲染交给 rAF
        markdownBody.addEventListener('touchmove', (event) => {
            if (!activeGesture) return;
            const touch = event.touches[0];
            if (!touch) return;
            activeGesture.currentX = touch.clientX;
            activeGesture.currentY = touch.clientY;
            activeGesture.lastX = touch.clientX;
            activeGesture.lastY = touch.clientY;

            // 更新速度历史（保留最近 100ms）
            const now = Date.now();
            activeGesture.history.push({ t: now, x: touch.clientX });
            activeGesture.history = activeGesture.history.filter(h => now - h.t < 150);

            // 判定逻辑
            const dx = activeGesture.currentX - activeGesture.startX;
            const dy = activeGesture.currentY - activeGesture.startY;
            const absX = Math.abs(dx);
            const absY = Math.abs(dy);
            const hasCrossedMoveCancel = absX > config.moveCancelPx || absY > config.moveCancelPx;
            const hasHorizontalIntent = absX > config.moveCancelPx && absX > absY * 0.72;

            // 1. 长按未达阈值就出现明显移动：取消本次手势，交还给滚动。
            if (!activeGesture.longPressTriggered && !activeGesture.swipeActive && hasCrossedMoveCancel) {
                activeGesture.hasMoved = true;
                if (activeGesture.longPressTimer) {
                    clearTimeout(activeGesture.longPressTimer);
                    activeGesture.longPressTimer = null;
                }
                clearParagraphHoldCue(activeGesture);
                clearSwipeNeighborPrewarm();
                clearSwipeDomPrune();
                clearSwipeInteractionCard(activeGesture);
                state.touchGesture = null;
                activeGesture = null;
                state.touchScrollActive = false;
                setGestureSelectionSuppressed(false);
                return;
            }

            if ((activeGesture.swipeActive || (activeGesture.longPressTriggered && hasHorizontalIntent)) && event.cancelable) {
                event.preventDefault();
            }

            // 2. 激活滑动 (Lock)
            if (!activeGesture.swipeActive && activeGesture.longPressTriggered) {
                // Scroll Priority: 检查水平滚动容器
                const isScrollable = (function(el) {
                    while (el && el !== markdownBody && el !== document.body) {
                        const style = window.getComputedStyle(el);
                        if ((style.overflowX === 'auto' || style.overflowX === 'scroll') && el.scrollWidth > el.clientWidth) {
                            return true;
                        }
                        el = el.parentElement;
                    }
                    return false;
                })(event.target);

                if (isScrollable) {
                    return;
                }

                const internalCanActivate = absX >= SWIPE_PHYSICS.activateMinDxPx
                    && absX > absY * SWIPE_PHYSICS.activateDirectionRatio;
                const externalCanActivate = typeof canHandleParagraphSwipeGesture === 'function'
                    ? !!canHandleParagraphSwipeGesture(absX, absY)
                    : true;
                const canActivateSwipe = internalCanActivate && externalCanActivate;
                if (canActivateSwipe) {
                    const card = getParagraphCardByIndex(activeGesture.index);
                    if (card) {
                        prewarmSwipeInteractionCard(card);
                        const contentEl = getSwipeContent(card);
                        if (contentEl) {
                            activeGesture.swipeActive = true;
                            activeGesture.swipeCard = card;
                            activeGesture.swipeContentEl = contentEl;
                            activeGesture.swipeCardWidth = card.offsetWidth || 300;
                            contentEl.classList.add('swiping');
                            // 启动渲染循环
                            activeGesture.rafId = requestAnimationFrame(updateSwipeFrame);
                        }
                    }
                }
            }

            // 3. 屏蔽默认滚动（如果已激活滑动）
            if (activeGesture.swipeActive) {
                if (event.cancelable) event.preventDefault();
            }
        }, { passive: false });

        markdownBody.addEventListener('touchend', async (event) => {
            if (!activeGesture) return;
            const g = activeGesture;
            
            // 清理
            if (g.longPressTimer) clearTimeout(g.longPressTimer);
            if (g.rafId) cancelAnimationFrame(g.rafId);
            clearParagraphHoldCue(g);
            state.touchGesture = null;
            activeGesture = null;
            state.touchScrollActive = false;
            setGestureSelectionSuppressed(false);
            clearSwipeNeighborPrewarm();
            clearSwipeDomPrune();

            if (!g.longPressTriggered) {
                clearSwipeInteractionCard(g);
                return;
            }

            // ─── 结算滑动 ───
            if (g.swipeActive) {
                // 计算最终速度 (线性回归或简单差分)
                let velocity = 0;
                if (g.history.length >= 2) {
                    const latest = g.history[g.history.length - 1];
                    const first = g.history[0];
                    const dt = latest.t - first.t;
                    if (dt > 10) {
                        velocity = (latest.x - first.x) / dt; // 有方向的速度 px/ms
                    }
                }
                const absVel = Math.abs(velocity);
                const absX = Math.abs(g.currentX - g.startX);
                const direction = g.swipeDirection || ((g.currentX - g.startX) < 0 ? 'left' : 'right');
                const directionSign = direction === 'left' ? -1 : 1;
                const commitThreshold = resolveSwipeCommitThreshold(g.swipeCardWidth, direction);
                let isFlick = false;
                if (direction === 'left') {
                    const minDeleteFlickDistance = Math.max(
                        SWIPE_PHYSICS.deleteFlickMinDxPx,
                        commitThreshold * SWIPE_PHYSICS.flickCommitMinProgressRatio
                    );
                    isFlick = absVel >= SWIPE_PHYSICS.deleteFlickVelocityPxPerMs
                        && absX >= minDeleteFlickDistance;
                } else {
                    isFlick = absVel >= SWIPE_PHYSICS.flickVelocityPxPerMs
                        && absX >= SWIPE_PHYSICS.flickMinDxPx;
                }
                // 复用 visual feedback 的判断逻辑：如果已经拉过了阈值，或者速度够快
                const shouldCommit = absX >= commitThreshold || (isFlick && Math.sign(velocity) === directionSign);

                if (shouldCommit && direction === 'left') {
                    await commitDeleteSwipe(g);
                } else if (shouldCommit && direction === 'right') {
                    await commitAnnotateSwipe(g);
                } else {
                    springBack(g);
                    const longPressAction = resolveGestureAction('longPress');
                    if (longPressAction && isParagraphActionAllowed(longPressAction)
                        && canTriggerLongPressAction(longPressAction, g)
                        && !hasActiveTextSelection()) {
                        await executeParagraphGestureAction(longPressAction, g.index, {
                            anchorX: g.currentX,
                            anchorY: g.currentY,
                            gesture: g,
                        });
                    }
                }
                return;
            }

            const longPressAction = resolveGestureAction('longPress');
            if (longPressAction && isParagraphActionAllowed(longPressAction)
                && canTriggerLongPressAction(longPressAction, g)
                && !hasActiveTextSelection()) {
                await executeParagraphGestureAction(longPressAction, g.index, {
                    anchorX: g.currentX,
                    anchorY: g.currentY,
                    gesture: g,
                });
            }
            clearSwipeInteractionCard(g);
        }, { passive: true });

        markdownBody.addEventListener('touchcancel', () => {
            if (!activeGesture) return;
            clearSwipeNeighborPrewarm();
            clearSwipeDomPrune();
            if (activeGesture.rafId) cancelAnimationFrame(activeGesture.rafId);
            if (activeGesture.longPressTimer) clearTimeout(activeGesture.longPressTimer);
            if (activeGesture.swipeActive) {
                springBack(activeGesture);
            } else {
                clearSwipeInteractionCard(activeGesture);
            }
            clearParagraphHoldCue(activeGesture);
            state.touchGesture = null;
            activeGesture = null;
            state.touchScrollActive = false;
            setGestureSelectionSuppressed(false);
        }, { passive: true });

        // ─── 动画实现 ───

        function springBack(g) {
            const el = g.swipeContentEl;
            if (!el) return;
            el.classList.remove('swiping');
            el.classList.add('spring-back');
            el.style.transform = 'translateX(0)';
            
            // 图标复位
            hideAllBackdrops(g.swipeCard, { animateIcons: true });
            el.classList.remove('breath-settle');
            void el.offsetWidth;
            el.classList.add('breath-settle');
            setTimeout(() => {
                el.classList.remove('breath-settle');
            }, SWIPE_PHYSICS.settleBreathMs);
            setTimeout(() => {
                clearBackdropIconResetState(g.swipeCard);
            }, SWIPE_ICON_RESET_TRANSITION_MS + 34);

            setTimeout(() => {
                cleanSwipeClasses(el);
                clearSwipeInteractionCard(g);
            }, SWIPE_PHYSICS.springBackMs);
        }

        async function commitDeleteSwipe(g) {
            const el = g.swipeContentEl;
            const card = g.swipeCard;
            if (!el || !card) {
                clearSwipeInteractionCard(g);
                return;
            }
             
            // 只要确认，就把图标设为高亮状态
            const delBd = card.querySelector('.swipe-backdrop.swipe-delete');
            if (delBd) {
                const icon = delBd.querySelector('.swipe-backdrop-icon');
                commitBackdropIconVisual(icon);
            }

            el.classList.remove('swiping');
            const commitThreshold = resolveSwipeCommitThreshold(g.swipeCardWidth, 'left');
            const startX = Number.isFinite(g.swipeDx) ? g.swipeDx : (g.currentX - g.startX);
            const swipeDistance = Math.abs(startX);
            const overshoot = Math.max(0, swipeDistance - commitThreshold);
            // 撕纸强度：同时考虑“拉过阈值多少”和“超出阈值多少”。
            const tearStrength = Math.min(
                1.9,
                (swipeDistance / Math.max(1, commitThreshold)) * 0.84
                + (overshoot / Math.max(1, SWIPE_PHYSICS.rubberBandLimit)) * 0.52
            );
            const exitTravelFactor = 1.28 + Math.min(0.26, tearStrength * 0.14);
            const exitX = -g.swipeCardWidth * exitTravelFactor;
            const exitRotDeg = -17 - Math.min(4, tearStrength * 2.2);
            el.style.setProperty('--swipe-start-x', `${startX.toFixed(2)}px`);
            el.style.setProperty('--swipe-exit-x', `${exitX.toFixed(2)}px`);
            el.style.setProperty('--swipe-exit-rot', `${exitRotDeg.toFixed(2)}deg`);
            el.classList.add('slide-out-left', 'paper-crumple');
            el.style.transform = '';

            if (typeof onDeleteSwipeCommitted === 'function') {
                try {
                    onDeleteSwipeCommitted({
                        index: g.index,
                        lineSpan: Number.isFinite(g.lineSpan) && g.lineSpan > 0 ? g.lineSpan : 1,
                        tearStrength,
                        swipeDistancePx: swipeDistance,
                        thresholdPx: commitThreshold,
                    });
                } catch (_hookError) {
                    // 删除主流程优先：回调异常不应中断删除。
                }
            }

            // 揉皱发生瞬间再触发震动，手感会更像“纸张被捏皱”。
            if (typeof navigator !== 'undefined' && typeof navigator.vibrate === 'function') {
                navigator.vibrate([10, 16, 20]);
            }

            await waitMs(SWIPE_PHYSICS.slideOutMs);

            // 折叠动画
            card.style.maxHeight = card.offsetHeight + 'px';
            void card.offsetHeight; // force reflow
            card.classList.add('swipe-collapse');

            await waitMs(SWIPE_PHYSICS.collapseMs);

            cleanSwipeClasses(el);
            hideAllBackdrops(card);
            card.classList.remove('swipe-collapse');
            card.style.maxHeight = '';
            clearSwipeInteractionCard(g);

            await deleteLineAtIndex(g.index, {
                confirmDelete: false,
                withUndo: true,
                lineSpan: Number.isFinite(g.lineSpan) && g.lineSpan > 0 ? g.lineSpan : 1,
            });
        }

        async function commitAnnotateSwipe(g) {
            const el = g.swipeContentEl;
            const card = g.swipeCard;
            if (!el || !card) {
                clearSwipeInteractionCard(g);
                return;
            }
             
            // 确认视觉反馈
            const annBd = card.querySelector('.swipe-backdrop.swipe-annotate');
            if (annBd) {
                const icon = annBd.querySelector('.swipe-backdrop-icon');
                commitBackdropIconVisual(icon);
                annBd.classList.add('committed', 'slap');
                annBd.style.setProperty('--swipe-note-progress', '1');
                setTimeout(() => annBd.classList.remove('slap'), 260);
            }
            if (typeof navigator !== 'undefined' && typeof navigator.vibrate === 'function') {
                navigator.vibrate(8);
            }

            // 弹回
            springBack(g);
            
            // 立即触发行内便签，无需等待完全弹回。
            // 这样感觉更连贯："松手 -> 便签贴上来"。
            setTimeout(() => openInlineStickyNote(g.index, {
                anchorX: Number.isFinite(g.currentX) ? g.currentX : g.startX,
                anchorY: Number.isFinite(g.currentY) ? g.currentY : g.startY,
            }), 50);
        }

        function waitMs(ms) {
            return new Promise(r => setTimeout(r, ms));
        }

    }

    function bindMarkdownBodyInteractions(deps) {
        if (!deps || !deps.markdownBody || !deps.state) return false;
        const markdownBody = deps.markdownBody;
        const mergedDeps = Object.assign({}, deps, {
            config: resolveConfig(deps.config),
            closeTransientUi: typeof deps.closeTransientUi === 'function' ? deps.closeTransientUi : function () {},
        });
        bindMarkdownPinchOutlineShortcut(markdownBody, mergedDeps);
        bindMarkdownParagraphGestures(markdownBody, mergedDeps);
        
        // Draft sync bind
        if (!markdownBody.dataset.paragraphDraftBound) {
            markdownBody.dataset.paragraphDraftBound = '1';
            markdownBody.addEventListener('input', (e) => {
                const editor = e.target.closest('[data-p-editor]');
                if (editor && Number(editor.getAttribute('data-p-editor')) === deps.state.paragraphEditingIndex) {
                    deps.state.paragraphDraftText = editor.value;
                }
            });
        }
        return true;
    }

    global.mobileMarkdownGestures = Object.freeze({
        DEFAULT_CONFIG,
        bindMarkdownBodyInteractions,
    });
})(window);



