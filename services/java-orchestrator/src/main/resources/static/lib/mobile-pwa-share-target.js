(function (global) {
    'use strict';

    const DEFAULT_SHARE_KEYS = Object.freeze(['title', 'text', 'url']);

    function normalizeShareKeys(inputKeys) {
        if (!Array.isArray(inputKeys) || inputKeys.length === 0) {
            return DEFAULT_SHARE_KEYS;
        }
        const normalized = inputKeys
            .map((item) => String(item || '').trim())
            .filter((item) => item.length > 0);
        if (!normalized.length) {
            return DEFAULT_SHARE_KEYS;
        }
        return normalized;
    }

    // 从分享文案中提取首个 URL，避免把整段描述误当作可解析地址。
    // 取舍：仅提取 http(s) 以保持稳定性，不尝试兼容全部非标准链接格式。
    function extractShareUrlCandidate(rawValue) {
        const text = String(rawValue || '').trim();
        if (!text) return '';
        const match = text.match(/https?:\/\/[^\s]+/i);
        if (!match || !match[0]) {
            return '';
        }
        return String(match[0]).replace(/[)\],.!?;]+$/, '');
    }

    // 消费并清理分享参数，避免页面刷新后重复触发自动提交流程。
    function consumeShareTargetPayloadFromUrl(options) {
        const settings = options || {};
        const targetWindow = settings.window || global;
        if (!targetWindow || !targetWindow.location || !targetWindow.history) {
            return null;
        }

        let currentUrl;
        try {
            currentUrl = new URL(targetWindow.location.href);
        } catch (_error) {
            return null;
        }

        const shareKeys = normalizeShareKeys(settings.keys);
        const payload = {};
        shareKeys.forEach((key) => {
            payload[key] = String(currentUrl.searchParams.get(key) || '').trim();
        });
        const hasSharePayload = shareKeys.some((key) => {
            return String(payload[key] || '').trim().length > 0;
        });
        if (!hasSharePayload) {
            return null;
        }

        shareKeys.forEach((key) => {
            currentUrl.searchParams.delete(key);
        });
        const nextQuery = currentUrl.searchParams.toString();
        const nextHref = `${currentUrl.pathname}${nextQuery ? `?${nextQuery}` : ''}${currentUrl.hash}`;
        targetWindow.history.replaceState(targetWindow.history.state, '', nextHref);
        return payload;
    }

    // 将分享参数写入现有输入框，并触发现有页面回调完成视图与提示同步。
    function prefillComposerFromShareTarget(options) {
        const settings = options || {};
        const payload = settings.payload;
        if (!payload || typeof payload !== 'object') return false;

        const targetDocument = settings.document || (global && global.document);
        if (!targetDocument) return false;
        const inputId = String(settings.inputId || 'mobileVideoUrl');
        const urlInput = targetDocument.getElementById(inputId);
        if (!urlInput) return false;

        const sharedUrl = extractShareUrlCandidate(payload.url) || String(payload.url || '').trim();
        const sharedTextUrl = extractShareUrlCandidate(payload.text);
        const sharedText = String(payload.text || '').trim();
        const sharedTitle = String(payload.title || '').trim();
        const candidate = sharedUrl || sharedTextUrl || sharedText || sharedTitle;
        if (!candidate) return false;

        urlInput.value = candidate;
        if (typeof settings.onRequireTasksView === 'function') {
            settings.onRequireTasksView();
        }
        if (typeof settings.onOpenComposer === 'function') {
            settings.onOpenComposer();
        }
        if (typeof settings.onSetSubmitTip === 'function') {
            settings.onSetSubmitTip(String(settings.shareDetectedTip || '已读取分享内容，正在提交...'));
        }
        return true;
    }

    // 注册 Service Worker：维持 PWA 可安装性，并为 Share Target 提供运行前提。
    function registerServiceWorker(options) {
        const settings = options || {};
        const targetWindow = settings.window || global;
        const targetNavigator = settings.navigator || (targetWindow ? targetWindow.navigator : null);
        if (!targetWindow || !targetNavigator || !('serviceWorker' in targetNavigator)) {
            return false;
        }
        const swUrl = String(settings.swUrl || '/sw.js');
        const onError = typeof settings.onError === 'function'
            ? settings.onError
            : function (error) {
                if (typeof console !== 'undefined' && console.warn) {
                    console.warn('Service Worker 注册失败', error);
                }
            };
        targetWindow.addEventListener('load', function () {
            targetNavigator.serviceWorker.register(swUrl).then(function (registration) {
                if (registration && typeof registration.update === 'function') {
                    return registration.update();
                }
                return null;
            }).catch(onError);
        });
        return true;
    }

    global.mobilePwaShareTarget = Object.freeze({
        registerServiceWorker,
        consumeShareTargetPayloadFromUrl,
        prefillComposerFromShareTarget,
    });
})(window);
