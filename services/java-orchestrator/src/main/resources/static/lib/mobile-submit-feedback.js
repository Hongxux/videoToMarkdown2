(function (global) {
    'use strict';

    var DEFAULT_BUSY_MESSAGE = '系统繁忙，请稍后重试';

    function textOrEmpty(input) {
        return String(input || '').trim();
    }

    // 做什么：从异常对象中抽取用户可见文本；为什么：前端与后端返回形态不统一，需要单点兼容。
    // 取舍：优先取 serverMessage，避免 message 被二次包装后丢失具体上下文。
    function extractRawErrorMessage(error) {
        if (!error) return '';
        if (typeof error === 'string') {
            return textOrEmpty(error);
        }
        if (error && typeof error.serverMessage === 'string' && textOrEmpty(error.serverMessage)) {
            return textOrEmpty(error.serverMessage);
        }
        if (error && typeof error.message === 'string' && textOrEmpty(error.message)) {
            return textOrEmpty(error.message);
        }
        return textOrEmpty(error);
    }

    function normalizeMessage(errorOrMessage, options) {
        var opts = options || {};
        var normalizeErrorMessage = typeof opts.normalizeErrorMessage === 'function'
            ? opts.normalizeErrorMessage
            : null;
        var fallback = textOrEmpty(opts.busyMessage) || DEFAULT_BUSY_MESSAGE;
        if (normalizeErrorMessage) {
            try {
                return textOrEmpty(normalizeErrorMessage(errorOrMessage, { fallback: fallback }));
            } catch (_error) {
                // ignore and continue with local fallback
            }
        }
        return textOrEmpty(errorOrMessage) || fallback;
    }

    function isBusyLikeMessage(message) {
        var lower = textOrEmpty(message).toLowerCase();
        if (!lower) return false;
        return (
            lower.indexOf('429') >= 0 ||
            lower.indexOf('503') >= 0 ||
            lower.indexOf('too many requests') >= 0 ||
            lower.indexOf('service unavailable') >= 0 ||
            lower.indexOf('rate limit') >= 0 ||
            lower.indexOf('busy') >= 0 ||
            lower.indexOf('overload') >= 0 ||
            lower.indexOf('timeout') >= 0 ||
            lower.indexOf('繁忙') >= 0 ||
            lower.indexOf('过载') >= 0 ||
            lower.indexOf('限流') >= 0 ||
            lower.indexOf('超时') >= 0 ||
            lower.indexOf('暂时不可用') >= 0
        );
    }

    function classifySubmitError(input, options) {
        var payload = input && typeof input === 'object' ? input : { error: input };
        var opts = options || {};
        var error = payload.error;
        var hasFile = !!payload.hasFile;
        if (typeof opts.hasFile === 'boolean') {
            hasFile = !!opts.hasFile;
        }
        var status = Number(payload.httpStatus) || Number(error && (error.httpStatus || error.status)) || 0;
        var rawMessage = textOrEmpty(payload.rawMessage) || extractRawErrorMessage(error);
        var normalizedMessage = textOrEmpty(payload.normalizedMessage) || normalizeMessage(rawMessage || error, opts);
        var lower = String(rawMessage || normalizedMessage || '').toLowerCase();

        var isNetwork = (
            error && error.name === 'TypeError'
        ) || /network|failed to fetch|load failed|econn|net::|connection|offline|aborterror/.test(lower);
        var isBusy = status === 429 || status === 503 || status >= 500 || isBusyLikeMessage(rawMessage) || isBusyLikeMessage(normalizedMessage);
        var isTooLarge = status === 413
            || /上传文件过大|too large|payload too large|max[- ]?file|max[- ]?request|request entity too large|超过|2gb|2048mb/.test(lower);
        var isUnsupported = /仅支持|不支持|unsupported|mime|file type|格式|extension/.test(lower);
        var isMissingFile = /videofile\s*不能为空|缺少视频|empty file|missing file/.test(lower);
        var isLinkIssue = !hasFile
            && (/videourl\s*不能为空|invalid url|malformed url|bad url|链接|url/.test(lower));

        if (isTooLarge) {
            return {
                reason: 'too_large',
                message: '这个视频有点大，我们这次没能抱住它。试试截短一点再来。',
                highlightLink: false,
                allowRetry: false,
                rawMessage: rawMessage,
                normalizedMessage: normalizedMessage,
                httpStatus: status,
            };
        }
        if (isUnsupported) {
            return {
                reason: 'unsupported',
                message: '这一类文件对我们来说还是个谜。试试 mp4、mov、mkv、avi、webm 或 m4v。',
                highlightLink: false,
                allowRetry: false,
                rawMessage: rawMessage,
                normalizedMessage: normalizedMessage,
                httpStatus: status,
            };
        }
        if (isMissingFile) {
            return {
                reason: 'missing_file',
                message: '我们还没拿到视频文件。再拖一次，或点一下选择文件。',
                highlightLink: false,
                allowRetry: false,
                rawMessage: rawMessage,
                normalizedMessage: normalizedMessage,
                httpStatus: status,
            };
        }
        if (isLinkIssue) {
            return {
                reason: 'link_issue',
                message: '这个链接暂时无法处理，试试换一条？',
                highlightLink: true,
                allowRetry: false,
                rawMessage: rawMessage,
                normalizedMessage: normalizedMessage,
                httpStatus: status,
            };
        }
        if (isNetwork || isBusy) {
            return {
                reason: isNetwork ? 'network' : 'busy',
                message: '网络有点不稳定，请再试一次。',
                highlightLink: false,
                allowRetry: true,
                rawMessage: rawMessage,
                normalizedMessage: normalizedMessage,
                httpStatus: status,
            };
        }
        var unknownMessage = textOrEmpty(normalizedMessage) || textOrEmpty(rawMessage);
        if (!unknownMessage) {
            unknownMessage = '这次处理没成功，请再试一次。';
        }
        return {
            reason: 'unknown',
            message: unknownMessage,
            highlightLink: false,
            allowRetry: true,
            rawMessage: rawMessage,
            normalizedMessage: normalizedMessage,
            httpStatus: status,
        };
    }

    global.MobileSubmitFeedback = {
        extractRawErrorMessage: extractRawErrorMessage,
        classifySubmitError: classifySubmitError,
    };
})(window);
