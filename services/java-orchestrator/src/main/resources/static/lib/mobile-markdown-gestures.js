(function (global) {
    'use strict';

    const DEFAULT_CONFIG = Object.freeze({
        holdTriggerMs: 560,
        moveCancelPx: 10,
        tapTolerancePx: 12,
        doubleTapWindowMs: 330,
    });

    function resolveConfig(rawConfig) {
        const next = Object.assign({}, DEFAULT_CONFIG, rawConfig || {});
        next.holdTriggerMs = Math.max(120, Number(next.holdTriggerMs) || DEFAULT_CONFIG.holdTriggerMs);
        next.moveCancelPx = Math.max(4, Number(next.moveCancelPx) || DEFAULT_CONFIG.moveCancelPx);
        next.tapTolerancePx = Math.max(4, Number(next.tapTolerancePx) || DEFAULT_CONFIG.tapTolerancePx);
        next.doubleTapWindowMs = Math.max(120, Number(next.doubleTapWindowMs) || DEFAULT_CONFIG.doubleTapWindowMs);
        return next;
    }

    function bindMarkdownPinchOutlineShortcut(markdownBody, deps) {
        if (!markdownBody || markdownBody.dataset.pinchOutlineBound === '1') {
            return;
        }
        markdownBody.dataset.pinchOutlineBound = '1';
        const {
            state,
            touchDistance,
            switchTab,
            closeTransientUi,
        } = deps;

        markdownBody.addEventListener('touchstart', (event) => {
            if (!event.touches || event.touches.length !== 2) return;
            state.pinchStartDistance = touchDistance(event.touches[0], event.touches[1]);
            state.pinchTriggered = false;
        }, { passive: true });

        markdownBody.addEventListener('touchmove', (event) => {
            if (!event.touches || event.touches.length !== 2 || state.pinchStartDistance <= 0 || state.pinchTriggered) return;
            state.pinchLatestDistance = touchDistance(event.touches[0], event.touches[1]);
            if (state.pinchMoveRafId) return;
            state.pinchMoveRafId = requestAnimationFrame(() => {
                state.pinchMoveRafId = 0;
                if (state.pinchTriggered || state.pinchStartDistance <= 0) return;
                if (state.pinchLatestDistance <= state.pinchStartDistance * 0.78) {
                    state.pinchTriggered = true;
                    switchTab('outline');
                    closeTransientUi();
                }
            });
        }, { passive: true });

        markdownBody.addEventListener('touchend', (event) => {
            if (event.touches && event.touches.length >= 2) return;
            if (state.pinchMoveRafId) {
                cancelAnimationFrame(state.pinchMoveRafId);
                state.pinchMoveRafId = 0;
            }
            state.pinchStartDistance = 0;
            state.pinchTriggered = false;
            state.pinchLatestDistance = 0;
        }, { passive: true });

        markdownBody.addEventListener('touchcancel', () => {
            if (state.pinchMoveRafId) {
                cancelAnimationFrame(state.pinchMoveRafId);
                state.pinchMoveRafId = 0;
            }
            state.pinchStartDistance = 0;
            state.pinchTriggered = false;
            state.pinchLatestDistance = 0;
        }, { passive: true });
    }

    function bindMarkdownParagraphGestures(markdownBody, deps) {
        if (!markdownBody || markdownBody.dataset.paragraphGestureBound === '1') {
            return;
        }
        markdownBody.dataset.paragraphGestureBound = '1';
        const {
            state,
            config,
            edgeBackHotZonePx,
            isParagraphActionAllowed,
            isParagraphGestureTargetBlocked,
            resolveGestureParagraphIndex,
            copyParagraphText,
            clearParagraphHoldCue,
            getParagraphCardByIndex,
            triggerParagraphHoldActivated,
            commitParagraphEditing,
            startParagraphEditing,
            toggleParagraphFavorite,
            canHandleParagraphSwipeGesture,
            openCommentModal,
            deleteLineAtIndex,
        } = deps;

        function activateParagraphEditing(index) {
            if (!state.editMode) {
                return false;
            }
            if (!Number.isFinite(index) || index < 0) {
                return false;
            }
            if (state.paragraphEditingIndex === index) {
                return true;
            }
            if (state.paragraphEditingIndex >= 0) {
                commitParagraphEditing(state.paragraphEditingIndex, { leaveEditing: true });
            }
            return !!startParagraphEditing(index);
        }

        // 统一段落手势：双击复制、长按收藏/编辑、横滑批注/删除。
        markdownBody.addEventListener('dblclick', async (event) => {
            if (!isParagraphActionAllowed('copy')) return;
            if (isParagraphGestureTargetBlocked(event.target)) return;
            const index = resolveGestureParagraphIndex(event.target);
            if (!Number.isFinite(index) || index < 0) return;
            await copyParagraphText(index);
        });

        markdownBody.addEventListener('touchstart', (event) => {
            if (!isParagraphActionAllowed('copy') && !isParagraphActionAllowed('favorite') && !state.editMode) return;
            if (!event.touches || event.touches.length !== 1) return;
            if (isParagraphGestureTargetBlocked(event.target)) return;
            const touch = event.touches[0];
            if (!touch) return;
            const edgeHotZone = Math.max(0, Number(edgeBackHotZonePx) || 24);
            const annotationEdgeGuard = edgeHotZone + 6;
            // 阅读态在左边缘优先让路给系统返回手势，避免右滑批注与返回冲突。
            if (state.currentView === 'reading' && touch.clientX <= annotationEdgeGuard) return;
            const index = resolveGestureParagraphIndex(event.target);
            if (!Number.isFinite(index) || index < 0) return;

            if (state.touchGesture && state.touchGesture.longPressTimer) {
                clearTimeout(state.touchGesture.longPressTimer);
            }
            clearParagraphHoldCue(state.touchGesture);
            const gesture = {
                index,
                startX: touch.clientX,
                startY: touch.clientY,
                lastX: touch.clientX,
                lastY: touch.clientY,
                latestX: touch.clientX,
                latestY: touch.clientY,
                moved: false,
                longPressTriggered: false,
                longPressTimer: null,
                moveRafId: 0,
                holdCard: null,
            };
            gesture.holdCard = getParagraphCardByIndex(index);
            if (gesture.holdCard) {
                gesture.holdCard.classList.add('touch-hold-cue');
            }
            gesture.longPressTimer = setTimeout(() => {
                gesture.longPressTriggered = true;
                triggerParagraphHoldActivated(gesture.holdCard);
                if (state.editMode) {
                    activateParagraphEditing(index);
                    return;
                }
                if (isParagraphActionAllowed('favorite')) {
                    toggleParagraphFavorite(index, { anchorX: gesture.startX, anchorY: gesture.startY })
                        .catch((error) => alert(`收藏失败：${error.message || error}`));
                }
            }, config.holdTriggerMs);
            if (state.editMode) {
                state.touchScrollActive = true;
            }
            state.touchGesture = gesture;
        }, { passive: true });

        markdownBody.addEventListener('touchmove', (event) => {
            const gesture = state.touchGesture;
            if (!gesture) return;
            const touch = event.touches && event.touches[0];
            if (!touch) return;
            gesture.latestX = touch.clientX;
            gesture.latestY = touch.clientY;
            if (state.editMode) {
                state.touchScrollLastMoveAt = Date.now();
            }
            if (gesture.moveRafId) return;
            gesture.moveRafId = requestAnimationFrame(() => {
                gesture.moveRafId = 0;
                gesture.lastX = gesture.latestX;
                gesture.lastY = gesture.latestY;
                const dx = gesture.lastX - gesture.startX;
                const dy = gesture.lastY - gesture.startY;
                if (Math.abs(dx) > config.moveCancelPx || Math.abs(dy) > config.moveCancelPx) {
                    gesture.moved = true;
                    if (gesture.longPressTimer) {
                        clearTimeout(gesture.longPressTimer);
                        gesture.longPressTimer = null;
                    }
                    clearParagraphHoldCue(gesture);
                }
            });
        }, { passive: true });

        markdownBody.addEventListener('touchcancel', () => {
            const gesture = state.touchGesture;
            if (!gesture) return;
            if (gesture.longPressTimer) {
                clearTimeout(gesture.longPressTimer);
            }
            if (gesture.moveRafId) {
                cancelAnimationFrame(gesture.moveRafId);
                gesture.moveRafId = 0;
            }
            if (state.editMode) {
                state.touchScrollActive = false;
                state.touchScrollLastMoveAt = Date.now();
            }
            clearParagraphHoldCue(gesture);
            state.touchGesture = null;
        }, { passive: true });

        markdownBody.addEventListener('touchend', async () => {
            const gesture = state.touchGesture;
            if (!gesture) return;
            if (gesture.longPressTimer) {
                clearTimeout(gesture.longPressTimer);
                gesture.longPressTimer = null;
            }
            if (gesture.moveRafId) {
                cancelAnimationFrame(gesture.moveRafId);
                gesture.moveRafId = 0;
            }
            if (state.editMode) {
                state.touchScrollActive = false;
                state.touchScrollLastMoveAt = Date.now();
            }
            clearParagraphHoldCue(gesture);
            state.touchGesture = null;
            if (gesture.longPressTriggered) {
                return;
            }

            const dx = gesture.lastX - gesture.startX;
            const dy = gesture.lastY - gesture.startY;
            const absX = Math.abs(dx);
            const absY = Math.abs(dy);

            if (canHandleParagraphSwipeGesture(absX, absY)) {
                if (dx > 0) {
                    openCommentModal(gesture.index);
                    return;
                }
                if (state.editMode) {
                    await deleteLineAtIndex(gesture.index, { confirmDelete: false, withUndo: true });
                }
                return;
            }

            if (absX < config.tapTolerancePx && absY < config.tapTolerancePx) {
                if (state.editMode) {
                    activateParagraphEditing(gesture.index);
                    return;
                }
            }

            if (!state.editMode && absX < config.tapTolerancePx && absY < config.tapTolerancePx && isParagraphActionAllowed('copy')) {
                const now = Date.now();
                if (state.lastTapIndex === gesture.index && now - state.lastTapAt <= config.doubleTapWindowMs) {
                    await copyParagraphText(gesture.index);
                    state.lastTapAt = 0;
                    state.lastTapIndex = -1;
                } else {
                    state.lastTapAt = now;
                    state.lastTapIndex = gesture.index;
                }
            }
        }, { passive: true });

        markdownBody.addEventListener('click', (event) => {
            if (!state.editMode) {
                return;
            }
            if (event.detail && event.detail > 1) {
                return;
            }
            if (isParagraphGestureTargetBlocked(event.target)) {
                return;
            }
            const index = resolveGestureParagraphIndex(event.target);
            if (!Number.isFinite(index) || index < 0) {
                return;
            }
            activateParagraphEditing(index);
        });
    }

    function bindParagraphDraftSync(markdownBody, deps) {
        if (!markdownBody || markdownBody.dataset.paragraphDraftBound === '1') {
            return;
        }
        markdownBody.dataset.paragraphDraftBound = '1';
        const { state } = deps;
        markdownBody.addEventListener('input', (event) => {
            const editor = event.target.closest('[data-p-editor]');
            if (!editor || !state.editMode) return;
            const idx = Number(editor.getAttribute('data-p-editor'));
            if (idx === state.paragraphEditingIndex) {
                state.paragraphDraftText = editor.value;
            }
        });
    }

    function bindMarkdownBodyInteractions(deps) {
        if (!deps || !deps.markdownBody || !deps.state) {
            return false;
        }
        const markdownBody = deps.markdownBody;
        const mergedDeps = Object.assign({}, deps, {
            config: resolveConfig(deps.config),
            closeTransientUi: typeof deps.closeTransientUi === 'function' ? deps.closeTransientUi : function () {},
        });
        bindMarkdownPinchOutlineShortcut(markdownBody, mergedDeps);
        bindMarkdownParagraphGestures(markdownBody, mergedDeps);
        bindParagraphDraftSync(markdownBody, mergedDeps);
        return true;
    }

    global.mobileMarkdownGestures = Object.freeze({
        DEFAULT_CONFIG,
        bindMarkdownBodyInteractions,
    });
})(window);
