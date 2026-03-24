(function (global) {
    'use strict';

    const DEFAULT_INVITATION_SELECTOR = '.empty-invitation';
    const DEFAULT_PULSE_CLASS = 'invitation-pulse';
    const DEFAULT_PULSE_DURATION_MS = 760;

    function resolveNode(target) {
        if (!target) return null;
        if (typeof target === 'string') {
            return document.querySelector(target);
        }
        return target;
    }

    // 内容空态文案同步：统一“标题提示 + 空态提示”的更新逻辑，避免主脚本分散判断。
    function applyStaticCopy(options) {
        const settings = options || {};
        const hasCurrentTask = !!settings.hasCurrentTask;
        if (hasCurrentTask) {
            return true;
        }
        const viewerTitleNode = resolveNode(settings.viewerTitleNode);
        if (viewerTitleNode && typeof settings.viewerHintOpen === 'string') {
            viewerTitleNode.textContent = settings.viewerHintOpen;
        }

        const emptyRoot = resolveNode(settings.emptyRoot);
        if (!emptyRoot) {
            return false;
        }
        const invitationTextNode = emptyRoot.querySelector
            ? emptyRoot.querySelector('.empty-text')
            : null;
        if (invitationTextNode && typeof settings.viewerHintOpenContent === 'string') {
            invitationTextNode.textContent = settings.viewerHintOpenContent;
            return true;
        }
        if (typeof settings.viewerHintOpenContent === 'string') {
            emptyRoot.textContent = settings.viewerHintOpenContent;
        }
        return true;
    }

    // 列表与创作入口脉冲提示：点击空态后给用户可感知的“下一步方向”。
    function pulseTargets(targets, options) {
        const settings = options || {};
        const pulseClass = String(settings.pulseClass || DEFAULT_PULSE_CLASS);
        const durationMs = Math.max(120, Number(settings.durationMs) || DEFAULT_PULSE_DURATION_MS);
        const list = Array.isArray(targets) ? targets : [];
        list.forEach((target) => {
            const node = resolveNode(target);
            if (!node || !node.classList) return;
            node.classList.remove(pulseClass);
            // 强制重排，保证连续触发时动画可重播。
            void node.offsetWidth;
            node.classList.add(pulseClass);
            global.setTimeout(function () {
                node.classList.remove(pulseClass);
            }, durationMs);
        });
    }

    // 空态点击绑定：把“邀请文案 -> 回到列表 -> 提示目标”封装成独立能力。
    function bindInvitationClick(options) {
        const settings = options || {};
        const container = resolveNode(settings.container);
        if (!container || typeof container.addEventListener !== 'function') {
            return false;
        }
        if (container.dataset.contentInvitationBound === '1') {
            return true;
        }
        container.dataset.contentInvitationBound = '1';

        const invitationSelector = String(settings.invitationSelector || DEFAULT_INVITATION_SELECTOR);
        container.addEventListener('click', function (event) {
            if (typeof settings.isTaskOpened === 'function' && settings.isTaskOpened()) {
                return;
            }
            const source = event && event.target;
            const invitationNode = source && source.closest
                ? source.closest(invitationSelector)
                : null;
            if (!invitationNode) {
                return;
            }
            if (event && typeof event.preventDefault === 'function') {
                event.preventDefault();
            }
            if (typeof settings.onOpenTaskList === 'function') {
                settings.onOpenTaskList(invitationNode);
            }
            if (typeof settings.onInvoked === 'function') {
                settings.onInvoked(invitationNode);
            }
        });
        return true;
    }

    global.mobileContentInvitation = Object.freeze({
        applyStaticCopy,
        pulseTargets,
        bindInvitationClick,
    });
})(window);
