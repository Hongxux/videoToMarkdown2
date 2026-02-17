(function (global) {
    'use strict';

    const DEFAULT_CONFIG = Object.freeze({
        holdTriggerMs: 560,
        moveCancelPx: 10,
        tapTolerancePx: 12,
        doubleTapWindowMs: 330,
    });

    // 滑动操作手感参数：阈值、阻尼、速度判定等集中配置。
    const SWIPE_PHYSICS = Object.freeze({
        // 手指横向移动多少 px 后开始判定为滑动（而非纵向滚动）
        activateMinDxPx: 14,
        // 横向/纵向比例必须超过此值才激活滑动
        activateDirectionRatio: 1.3,
        // 滑过宽度的 35% 即视为"超过阈值"
        commitRatio: 0.35,
        // 快速甩动速度（px/ms）
        flickVelocityPxPerMs: 0.4,
        // 快速甩动最小位移
        flickMinDxPx: 24,
        // 阻尼：超出内容宽度后施加橡皮筋阻力
        rubberBandFactor: 0.35,
        // 弹回动画时长
        springBackMs: 380,
        // 滑出动画时长
        slideOutMs: 280,
        // 折叠动画时长
        collapseMs: 320,
    });

    // SVG 图标：垃圾桶（删除）
    const ICON_DELETE_SVG = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 6h18"/><path d="M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6"/><path d="M10 11v6"/><path d="M14 11v6"/></svg>';
    // SVG 图标：钢笔（批注）
    const ICON_ANNOTATE_SVG = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 20h9"/><path d="M16.5 3.5a2.121 2.121 0 0 1 3 3L7 19l-4 1 1-4 12.5-12.5z"/></svg>';

    function resolveConfig(rawConfig) {
        const next = Object.assign({}, DEFAULT_CONFIG, rawConfig || {});
        next.holdTriggerMs = Math.max(120, Number(next.holdTriggerMs) || DEFAULT_CONFIG.holdTriggerMs);
        next.moveCancelPx = Math.max(4, Number(next.moveCancelPx) || DEFAULT_CONFIG.moveCancelPx);
        next.tapTolerancePx = Math.max(4, Number(next.tapTolerancePx) || DEFAULT_CONFIG.tapTolerancePx);
        next.doubleTapWindowMs = Math.max(120, Number(next.doubleTapWindowMs) || DEFAULT_CONFIG.doubleTapWindowMs);
        return next;
    }

    // ─── 滑动 DOM 基础设施 ────────────────────────────────────

    /**
     * 确保 paragraph-card 内部有 swipe-content 包裹层和 backdrop 层。
     * 只在首次滑动时（lazy）注入，避免渲染阶段增加开销。
     */
    function ensureSwipeCellDom(card) {
        if (!card || card.dataset.swipeCellReady === '1') {
            return card;
        }
        // 检查是否已有 swipe-content 子元素
        let contentWrap = card.querySelector('.swipe-content');
        if (!contentWrap) {
            contentWrap = document.createElement('div');
            contentWrap.className = 'swipe-content';
            // 把 card 的所有子节点移入 contentWrap
            while (card.firstChild) {
                contentWrap.appendChild(card.firstChild);
            }
            card.appendChild(contentWrap);
        }

        // 添加两个 backdrop（删除、批注）
        if (!card.querySelector('.swipe-backdrop.swipe-delete')) {
            const delBg = document.createElement('div');
            delBg.className = 'swipe-backdrop swipe-delete';
            delBg.innerHTML = `<svg class="swipe-backdrop-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">${ICON_DELETE_SVG.replace(/<\/?svg[^>]*>/g, '')}</svg>`;
            card.insertBefore(delBg, contentWrap);
        }
        if (!card.querySelector('.swipe-backdrop.swipe-annotate')) {
            const annBg = document.createElement('div');
            annBg.className = 'swipe-backdrop swipe-annotate';
            annBg.innerHTML = `<svg class="swipe-backdrop-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">${ICON_ANNOTATE_SVG.replace(/<\/?svg[^>]*>/g, '')}</svg>`;
            card.insertBefore(annBg, contentWrap);
        }

        card.dataset.swipeCellReady = '1';
        return card;
    }

    function getSwipeContent(card) {
        return card && card.querySelector('.swipe-content');
    }

    function showBackdrop(card, direction) {
        if (!card) return;
        const delBd = card.querySelector('.swipe-backdrop.swipe-delete');
        const annBd = card.querySelector('.swipe-backdrop.swipe-annotate');
        if (delBd) {
            delBd.classList.toggle('active', direction === 'left');
            delBd.classList.remove('committed');
        }
        if (annBd) {
            annBd.classList.toggle('active', direction === 'right');
            annBd.classList.remove('committed');
        }
    }

    function setBackdropCommitted(card, direction, committed) {
        if (!card) return;
        const selector = direction === 'left' ? '.swipe-delete' : '.swipe-annotate';
        const bd = card.querySelector(`.swipe-backdrop${selector}`);
        if (bd) {
            bd.classList.toggle('committed', committed);
        }
    }

    function hideAllBackdrops(card) {
        if (!card) return;
        card.querySelectorAll('.swipe-backdrop').forEach((bd) => {
            bd.classList.remove('active', 'committed');
        });
    }

    function cleanSwipeClasses(contentEl) {
        if (!contentEl) return;
        contentEl.classList.remove('swiping', 'spring-back', 'slide-out-left', 'slide-out-right');
        contentEl.style.transform = '';
    }

    // ─── 缩合指捏快捷键 ────────────────────────────────────

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

    // ─── 段落手势（含滑动物理） ────────────────────────────────

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

        // 当前正在进行的滑动手势状态（null = 无）
        let activeSwipe = null;

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

        // 统一段落手势：双击复制
        markdownBody.addEventListener('dblclick', async (event) => {
            if (!isParagraphActionAllowed('copy')) return;
            if (isParagraphGestureTargetBlocked(event.target)) return;
            const index = resolveGestureParagraphIndex(event.target);
            if (!Number.isFinite(index) || index < 0) return;
            await copyParagraphText(index);
        });

        // ▸ touchstart：初始化手势
        markdownBody.addEventListener('touchstart', (event) => {
            if (!isParagraphActionAllowed('copy') && !isParagraphActionAllowed('favorite') && !state.editMode) return;
            if (!event.touches || event.touches.length !== 1) return;
            if (isParagraphGestureTargetBlocked(event.target)) return;
            const touch = event.touches[0];
            if (!touch) return;
            const edgeHotZone = Math.max(0, Number(edgeBackHotZonePx) || 24);
            const annotationEdgeGuard = edgeHotZone + 6;
            // 阅读态在左边缘优先让路给系统返回手势
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
                // 滑动物理状态
                swipeActive: false,
                swipeDirection: null, // 'left' | 'right'
                swipeCard: null,
                swipeContentEl: null,
                swipeCardWidth: 0,
                swipeDx: 0,
                swipeCommitted: false,
                // 速度采样
                velocityX: 0,
                lastSampleAt: typeof performance !== 'undefined' ? performance.now() : Date.now(),
                lastSampleDx: 0,
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

        // ▸ touchmove：1:1 跟手追踪
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

            // 速度采样
            const now = typeof performance !== 'undefined' ? performance.now() : Date.now();
            const dt = Math.max(4, now - gesture.lastSampleAt);
            const rawDx = gesture.latestX - gesture.startX;
            const instV = (Math.abs(rawDx) - Math.abs(gesture.lastSampleDx)) / dt;
            gesture.velocityX = gesture.velocityX * 0.65 + Math.max(0, instV) * 0.35;
            gesture.lastSampleAt = now;
            gesture.lastSampleDx = rawDx;

            const dx = gesture.latestX - gesture.startX;
            const dy = gesture.latestY - gesture.startY;
            const absX = Math.abs(dx);
            const absY = Math.abs(dy);

            // 判断是否取消长按
            if (!gesture.swipeActive && (absX > config.moveCancelPx || absY > config.moveCancelPx)) {
                gesture.moved = true;
                if (gesture.longPressTimer) {
                    clearTimeout(gesture.longPressTimer);
                    gesture.longPressTimer = null;
                }
                clearParagraphHoldCue(gesture);
            }

            // 判断是否激活滑动
            if (!gesture.swipeActive && !gesture.longPressTriggered && state.editMode) {
                if (absX >= SWIPE_PHYSICS.activateMinDxPx && absX > absY * SWIPE_PHYSICS.activateDirectionRatio) {
                    const card = getParagraphCardByIndex(gesture.index);
                    if (card) {
                        ensureSwipeCellDom(card);
                        const contentEl = getSwipeContent(card);
                        if (contentEl) {
                            gesture.swipeActive = true;
                            gesture.swipeCard = card;
                            gesture.swipeContentEl = contentEl;
                            gesture.swipeCardWidth = card.offsetWidth || 300;
                            contentEl.classList.add('swiping');
                            activeSwipe = gesture;
                        }
                    }
                }
            }

            // 1:1 跟手更新
            if (gesture.swipeActive && gesture.swipeContentEl) {
                if (event.cancelable) {
                    event.preventDefault();
                }
                const direction = dx < 0 ? 'left' : 'right';
                gesture.swipeDirection = direction;

                // 应用橡皮筋阻尼：超过宽度的 commitRatio 后增加阻力感
                const maxComfort = gesture.swipeCardWidth * SWIPE_PHYSICS.commitRatio;
                let displayDx;
                if (absX <= maxComfort) {
                    displayDx = dx;
                } else {
                    const excess = absX - maxComfort;
                    const dampedExcess = excess * SWIPE_PHYSICS.rubberBandFactor;
                    displayDx = (dx > 0 ? 1 : -1) * (maxComfort + dampedExcess);
                }
                gesture.swipeDx = displayDx;

                // 更新 DOM
                gesture.swipeContentEl.style.transform = `translateX(${displayDx.toFixed(1)}px)`;
                showBackdrop(gesture.swipeCard, direction);

                // 判断是否达到阈值
                const commitThreshold = gesture.swipeCardWidth * SWIPE_PHYSICS.commitRatio;
                const isCommitted = absX >= commitThreshold || gesture.velocityX >= SWIPE_PHYSICS.flickVelocityPxPerMs;
                gesture.swipeCommitted = isCommitted;
                setBackdropCommitted(gesture.swipeCard, direction, isCommitted);

                return; // 滑动激活后不再处理其他 move 逻辑
            }

            if (gesture.moveRafId) return;
            gesture.moveRafId = requestAnimationFrame(() => {
                gesture.moveRafId = 0;
                gesture.lastX = gesture.latestX;
                gesture.lastY = gesture.latestY;
            });
        }, { passive: false }); // passive: false 以便 preventDefault

        // ▸ touchcancel：清理
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
            // 如果正在滑动，弹回
            if (gesture.swipeActive) {
                springBack(gesture);
            }
            if (state.editMode) {
                state.touchScrollActive = false;
                state.touchScrollLastMoveAt = Date.now();
            }
            clearParagraphHoldCue(gesture);
            state.touchGesture = null;
            activeSwipe = null;
        }, { passive: true });

        // ▸ touchend：判定提交/取消
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
            activeSwipe = null;

            if (gesture.longPressTriggered) {
                return;
            }

            // ── 滑动判定 ──
            if (gesture.swipeActive) {
                const dx = gesture.latestX - gesture.startX;
                const absX = Math.abs(dx);
                const direction = gesture.swipeDirection;
                const commitThreshold = gesture.swipeCardWidth * SWIPE_PHYSICS.commitRatio;
                const isFlick = gesture.velocityX >= SWIPE_PHYSICS.flickVelocityPxPerMs && absX >= SWIPE_PHYSICS.flickMinDxPx;
                const shouldCommit = absX >= commitThreshold || isFlick;

                if (shouldCommit && direction === 'left') {
                    // ──── 确认删除 ────
                    await commitDeleteSwipe(gesture);
                } else if (shouldCommit && direction === 'right') {
                    // ──── 确认批注 ────
                    await commitAnnotateSwipe(gesture);
                } else {
                    // ──── 取消：弹簧回弹 ────
                    springBack(gesture);
                }
                return;
            }

            // ── 非滑动手势（点击逻辑） ──
            const dx = gesture.lastX - gesture.startX;
            const dy = gesture.lastY - gesture.startY;
            const absX = Math.abs(dx);
            const absY = Math.abs(dy);

            // 老的滑动检测（兼容非编辑模式）
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

        // ─── 弹簧回弹 ───
        function springBack(gesture) {
            const contentEl = gesture.swipeContentEl;
            const card = gesture.swipeCard;
            if (!contentEl) return;
            contentEl.classList.remove('swiping');
            contentEl.classList.add('spring-back');
            contentEl.style.transform = 'translateX(0)';
            hideAllBackdrops(card);
            const cleanup = () => {
                cleanSwipeClasses(contentEl);
            };
            contentEl.addEventListener('transitionend', cleanup, { once: true });
            setTimeout(cleanup, SWIPE_PHYSICS.springBackMs + 50);
        }

        // ─── 确认删除：滑出 → 折叠 → 数据删除 ───
        async function commitDeleteSwipe(gesture) {
            const contentEl = gesture.swipeContentEl;
            const card = gesture.swipeCard;
            const index = gesture.index;
            if (!contentEl || !card) return;

            // 滑出屏幕
            contentEl.classList.remove('swiping');
            contentEl.classList.add('slide-out-left');
            contentEl.style.transform = `translateX(${-gesture.swipeCardWidth - 20}px)`;

            // 等待滑出动画完成
            await waitTransitionEnd(contentEl, SWIPE_PHYSICS.slideOutMs + 60);

            // 折叠
            const cardHeight = card.offsetHeight;
            card.style.maxHeight = cardHeight + 'px';
            // 强制重排
            void card.offsetHeight;
            card.classList.add('swipe-collapse');

            // 等待折叠动画
            await new Promise((r) => setTimeout(r, SWIPE_PHYSICS.collapseMs + 40));

            // 清理并执行数据层删除
            cleanSwipeClasses(contentEl);
            hideAllBackdrops(card);
            card.classList.remove('swipe-collapse');
            card.style.maxHeight = '';

            await deleteLineAtIndex(index, { confirmDelete: false, withUndo: true });
        }

        // ─── 确认批注：弹回 + 打开评论框 ───
        async function commitAnnotateSwipe(gesture) {
            const contentEl = gesture.swipeContentEl;
            const card = gesture.swipeCard;
            const index = gesture.index;

            // 弹回原位
            springBack(gesture);

            // 稍等弹回后打开评论框
            setTimeout(() => {
                openCommentModal(index);
            }, 160);
        }

        function waitTransitionEnd(el, maxMs) {
            return new Promise((resolve) => {
                const done = () => resolve();
                el.addEventListener('transitionend', done, { once: true });
                setTimeout(done, maxMs);
            });
        }

        // ─── 点击激活编辑（兼容） ───
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
