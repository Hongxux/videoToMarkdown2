(function (global) {
    'use strict';

    const DEFAULT_TOKENS = Object.freeze({
        lineHeight: '1.8',
        letterSpacing: '0.01em',
        columnWidth: '68ch',
    });

    function resolveRoot(root) {
        if (root && root.style) {
            return root;
        }
        if (global.document && global.document.documentElement) {
            return global.document.documentElement;
        }
        return null;
    }

    // 阅读排版 token 单点初始化：后续调参只需改模块配置，不再散落在主脚本。
    function applyDefaults(options) {
        const settings = options || {};
        const root = resolveRoot(settings.root);
        if (!root) {
            return { applied: false, tokens: DEFAULT_TOKENS };
        }

        const tokens = Object.assign({}, DEFAULT_TOKENS, settings.tokens || {});
        root.style.setProperty('--content-line-height', String(tokens.lineHeight || DEFAULT_TOKENS.lineHeight));
        root.style.setProperty('--content-letter-spacing', String(tokens.letterSpacing || DEFAULT_TOKENS.letterSpacing));
        root.style.setProperty('--content-column-width', String(tokens.columnWidth || DEFAULT_TOKENS.columnWidth));
        return { applied: true, tokens };
    }

    global.mobileReaderTypography = Object.freeze({
        DEFAULT_TOKENS,
        applyDefaults,
    });
})(window);

