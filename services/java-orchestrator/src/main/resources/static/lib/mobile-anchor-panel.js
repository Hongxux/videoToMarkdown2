(() => {
    if (window.__mobileAnchorPanelReady) {
        return;
    }
    window.__mobileAnchorPanelReady = true;

    const TEXT = Object.freeze({
        noAnchors: '当前页面暂无可挂载锚点',
        noMounted: '暂无已挂载内容，请先选择锚点并上传。',
        noSelection: '请先选择一个锚点。',
        quotePlaceholder: '点击锚点后，这里会显示定位片段。',
        hintPlaceholder: '手机端填写的简短备注会展示在这里。',
        previewEmpty: '这个锚点暂无内容，可从右侧输入或拖拽文件。',
        previewLoading: '正在加载挂载预览...',
        choosing: '已选择锚点，准备挂载内容。',
        uploadFailed: '挂载失败，请稍后重试。',
        uploadSuccess: '挂载完成，已写入新 revision。',
        chooseAtLeastOne: '请至少选择一个文件或输入备注。',
    });

    const runtime = {
        ctxKey: '',
        ctx: { taskId: '', pathHint: '', apiBase: '/api/mobile' },
        anchors: new Map(),
        candidates: [],
        activeId: '',
        pendingByAnchor: new Map(),
        pendingMainByAnchor: new Map(),
        mountedPayloadByAnchor: new Map(),
        mountedNoteByAnchor: new Map(),
        observer: null,
        rematchTimer: 0,
        ctxTimer: 0,
        metaSeq: 0,
        mountedSeq: 0,
        dropId: '',
        dragDepth: 0,
        localNotesByAnchor: new Map(),
        activeLocalNoteIdByAnchor: new Map(),
        localNoteFilterByAnchor: new Map(),
        localNoteLayoutModeByAnchor: new Map(),
        obsidianGraphCacheByAnchor: new Map(),
        localVaultSnapshotByAnchor: new Map(),
        localSyncShadowByAnchor: new Map(),
        localSyncDirtyByAnchor: new Set(),
        syncInFlightByAnchor: new Set(),
        syncOperationSeq: 0,
        syncTimer: 0,
        vditorLoadPromise: null,
        vditorReady: false,
        vditorInstance: null,
        vditorSyncing: false,
        manualFullscreen: false,
        linkHoverHideTimer: 0,
        obsidianContextCollapsed: true,
        obsidianFocusMode: true,
        obsidianSettingsExpanded: false,
        editorActive: false,
        editorDirty: false,
        readerTheme: null,
        wikilinkSuggest: {
            open: false,
            anchorId: '',
            mode: 'slash',
            start: -1,
            end: -1,
            range: null,
            query: '',
            options: [],
            activeIndex: 0,
        },
        phase2b: {
            expanded: false,
            mode: 'idle',
            dragging: false,
            moving: false,
            movePointerId: -1,
            moveOffsetX: 0,
            moveOffsetY: 0,
            moveStartClientX: 0,
            moveStartClientY: 0,
            moveDidDrag: false,
            moveCaptureTarget: null,
            suppressNextOpen: false,
            moveX: null,
            moveY: null,
            resizing: false,
            resizePointerId: -1,
            resizeStartX: 0,
            resizeStartY: 0,
            resizeStartWidth: 0,
            resizeStartHeight: 0,
            canvasWidth: null,
            canvasHeight: null,
            layoutLoaded: false,
            inputCollapsed: false,
            inputValue: '',
            resultValue: '',
            feedback: '',
            error: '',
            processing: false,
            inFlightRequestSeq: 0,
            progressText: '',
            requestSeq: 0,
            currentRequestId: '',
            progressChannel: '',
            ws: null,
            wsConnectPromise: null,
            copied: false,
            attachedFiles: [],
            linkItems: [],
            linkPrefetchInFlight: new Set(),
            streamActive: false,
            streamChunkCount: 0,
            noticeText: '',
            noticeTimer: 0,
        },
    };

    const NODE_SELECTOR = 'h1,h2,h3,h4,h5,h6,p,li,blockquote,pre,td,th,figcaption';
    const LOCAL_NOTE_STORE_PREFIX = 'mobile.anchor.obsidian.local.v1';
    const LOCAL_NOTE_LAYOUT_PREFIX = 'mobile.anchor.obsidian.layout.v1';
    const LOCAL_THEME_STORE_KEY = 'mobile.anchor.obsidian.theme.v1';
    const PHASE2B_LAYOUT_STORE_KEY = 'mobile.anchor.phase2b.layout.v1';
    const ENABLE_OBSIDIAN_GRAPH = false;
    const WIKILINK_CAPTURE_REGEX = /\[\[([^\]\n]+)\]\]/g;
    const BLOCK_ID_MARKER_REGEX = /\^([A-Za-z0-9_-]{1,80})\s*$/;
    const PHASE2B_ARTICLE_LINK_REGEX = /https?:\/\/(?:(?:zhuanlan\.zhihu\.com\/p\/\d+)|(?:(?:www\.)?zhihu\.com\/question\/\d+\/answer\/\d+)|(?:juejin\.cn\/post\/\d+))(?:[^\s)\]}]*)/gi;
    const PHASE2B_PROGRESS_FALLBACK_TEXT = '正在等待后端处理进度...';
    const PHASE2B_ENDPOINT_SUFFIX = '/cards/phase2b/structured-markdown';
    const PHASE2B_LINK_METADATA_ENDPOINT_SUFFIX = '/cards/phase2b/link-metadata';

    function t(value) {
        return String(value || '').replace(/[\u200b\u200c\u200d\ufeff]/g, '').replace(/\s+/g, ' ').trim();
    }

    function h(value) {
        return String(value || '')
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    function elementFromEventTarget(target) {
        if (target instanceof Element) {
            return target;
        }
        if (target && target.parentElement instanceof Element) {
            return target.parentElement;
        }
        return null;
    }

    function closestFromEventTarget(target, selector) {
        const element = elementFromEventTarget(target);
        if (!element || typeof element.closest !== 'function') {
            return null;
        }
        return element.closest(selector);
    }

    function short(value, len) {
        const text = t(value);
        return text.length > len ? `${text.slice(0, len)}...` : text;
    }

    function normalizePath(value) {
        return String(value || '').trim().replace(/\\/g, '/').replace(/\/+/g, '/').replace(/^\/+/, '');
    }

    function normalizePhase2bArticleLink(urlLike) {
        const raw = String(urlLike || '').trim();
        if (!raw) {
            return '';
        }
        let cleaned = raw.replace(/[)\],.;!?]+$/g, '');
        if (!/^https?:\/\//i.test(cleaned)) {
            cleaned = `https://${cleaned.replace(/^\/+/, '')}`;
        }
        let parsed;
        try {
            parsed = new URL(cleaned);
        } catch (_e) {
            return '';
        }
        parsed.hash = '';
        parsed.search = '';
        const host = parsed.hostname.toLowerCase();
        const path = parsed.pathname.replace(/\/+$/, '');
        if (host === 'zhuanlan.zhihu.com' && /^\/p\/\d+$/.test(path)) {
            return `https://zhuanlan.zhihu.com${path}`;
        }
        if ((host === 'www.zhihu.com' || host === 'zhihu.com')) {
            const answerMatch = path.match(/^\/question\/(\d+)\/answer\/(\d+)$/);
            if (answerMatch) {
                return `https://www.zhihu.com/question/${answerMatch[1]}/answer/${answerMatch[2]}`;
            }
        }
        if ((host === 'juejin.cn' || host === 'www.juejin.cn') && /^\/post\/\d+$/.test(path)) {
            return `https://juejin.cn${path}`;
        }
        return '';
    }

    function extractPhase2bArticleLinks(textLike) {
        const source = String(textLike || '');
        const links = [];
        const seen = new Set();
        let match;
        PHASE2B_ARTICLE_LINK_REGEX.lastIndex = 0;
        while ((match = PHASE2B_ARTICLE_LINK_REGEX.exec(source)) !== null) {
            const normalized = normalizePhase2bArticleLink(match[0]);
            if (!normalized || seen.has(normalized)) {
                continue;
            }
            seen.add(normalized);
            links.push(normalized);
        }
        PHASE2B_ARTICLE_LINK_REGEX.lastIndex = 0;
        return links;
    }

    function stripPhase2bArticleLinks(textLike) {
        const source = String(textLike || '');
        const stripped = source.replace(PHASE2B_ARTICLE_LINK_REGEX, ' ').replace(/\s{2,}/g, ' ');
        PHASE2B_ARTICLE_LINK_REGEX.lastIndex = 0;
        return stripped.trim();
    }

    function inferPhase2bLinkSite(urlLike) {
        const normalized = normalizePhase2bArticleLink(urlLike);
        if (!normalized) {
            return '';
        }
        if (normalized.includes('zhuanlan.zhihu.com')) {
            return 'zhihu';
        }
        if (normalized.includes('zhihu.com/question/')) {
            return 'zhihu';
        }
        if (normalized.includes('juejin.cn')) {
            return 'juejin';
        }
        return '';
    }

    function buildPhase2bLinkChipLabel(linkItem) {
        const source = linkItem && typeof linkItem === 'object' ? linkItem : {};
        const status = t(source.status).toLowerCase();
        if (status === 'fetching') {
            return '正在获取文章标题...';
        }
        if (status === 'failed') {
            return '标题获取失败（提交时继续解析）';
        }
        const title = t(source.title);
        if (title) {
            return title;
        }
        const site = inferPhase2bLinkSite(source.url);
        if (site === 'zhihu') {
            return '知乎文章（等待标题）';
        }
        if (site === 'juejin') {
            return '掘金文章（等待标题）';
        }
        return '链接（等待标题）';
    }

    function upsertPhase2bLinkItems(urlsLike, options = {}) {
        const urls = Array.isArray(urlsLike) ? urlsLike : [urlsLike];
        const phase2b = runtime.phase2b;
        const existing = Array.isArray(phase2b.linkItems) ? phase2b.linkItems : [];
        const next = existing.slice();
        const fromResolved = options && options.resolvedMap && typeof options.resolvedMap === 'object'
            ? options.resolvedMap
            : {};
        let changed = false;
        for (let i = 0; i < urls.length; i += 1) {
            const normalized = normalizePhase2bArticleLink(urls[i]);
            if (!normalized) {
                continue;
            }
            const idx = next.findIndex((item) => normalizePhase2bArticleLink(item && item.url) === normalized);
            const resolved = fromResolved[normalized] && typeof fromResolved[normalized] === 'object'
                ? fromResolved[normalized]
                : null;
            const nextTitle = t(resolved && resolved.title) || t(options && options.title);
            const nextStatus = t(resolved && resolved.status) || t(options && options.status) || 'queued';
            const nextSite = t(resolved && resolved.site) || inferPhase2bLinkSite(normalized);
            if (idx >= 0) {
                const prev = next[idx] || {};
                const merged = {
                    url: normalized,
                    title: nextTitle || t(prev.title),
                    site: nextSite || t(prev.site),
                    status: nextStatus || t(prev.status),
                };
                if (JSON.stringify(prev) !== JSON.stringify(merged)) {
                    next[idx] = merged;
                    changed = true;
                }
                continue;
            }
            next.push({
                url: normalized,
                title: nextTitle,
                site: nextSite,
                status: nextStatus,
            });
            changed = true;
        }
        if (changed) {
            phase2b.linkItems = next;
        }
        return changed;
    }

    function applyPhase2bResolvedLinkTitles(linksLike) {
        const links = Array.isArray(linksLike) ? linksLike : [];
        if (!links.length) {
            return;
        }
        const resolvedMap = {};
        links.forEach((item) => {
            const url = normalizePhase2bArticleLink(item && item.url);
            if (!url) {
                return;
            }
            resolvedMap[url] = {
                title: t(item && item.title),
                site: t(item && item.siteType),
                status: 'resolved',
            };
        });
        if (upsertPhase2bLinkItems(Object.keys(resolvedMap), { resolvedMap })) {
            renderPhase2bFloatingUi();
        }
    }

    function clampNumber(value, min, max, fallback) {
        const n = Number(value);
        if (!Number.isFinite(n)) {
            return fallback;
        }
        if (n < min) return min;
        if (n > max) return max;
        return n;
    }

    function normalizeThemeMode(value) {
        const mode = t(value).toLowerCase();
        if (mode === 'light' || mode === 'dark' || mode === 'auto') {
            return mode;
        }
        return 'auto';
    }

    function normalizeFontPreset(value) {
        const preset = t(value).toLowerCase();
        if (preset === 'system' || preset === 'serif' || preset === 'mono') {
            return preset;
        }
        return 'system';
    }

    function resolveReaderFontFamily(preset) {
        const normalized = normalizeFontPreset(preset);
        if (normalized === 'serif') {
            return '"Noto Serif SC", "Source Han Serif SC", "Songti SC", "Times New Roman", serif';
        }
        if (normalized === 'mono') {
            return '"JetBrains Mono", "SFMono-Regular", "Consolas", "Liberation Mono", monospace';
        }
        return '-apple-system, BlinkMacSystemFont, "Segoe UI", Inter, "Helvetica Neue", Arial, sans-serif';
    }

    function normalizeReaderThemeSettings(raw) {
        const source = raw && typeof raw === 'object' ? raw : {};
        return {
            themeMode: normalizeThemeMode(source.themeMode),
            fontPreset: normalizeFontPreset(source.fontPreset),
            fontSize: clampNumber(source.fontSize, 12, 22, 15),
            lineHeight: clampNumber(source.lineHeight, 1.3, 2.2, 1.72),
            maxWidthCh: clampNumber(source.maxWidthCh, 52, 120, 78),
            pagePaddingPx: clampNumber(source.pagePaddingPx, 8, 40, 18),
            paragraphGapEm: clampNumber(source.paragraphGapEm, 0.3, 2.2, 0.88),
            listIndentEm: clampNumber(source.listIndentEm, 0.8, 2.6, 1.25),
        };
    }

    function readReaderThemeSettings() {
        try {
            const raw = localStorage.getItem(LOCAL_THEME_STORE_KEY);
            if (!raw) {
                return normalizeReaderThemeSettings({});
            }
            return normalizeReaderThemeSettings(JSON.parse(raw));
        } catch (_e) {
            return normalizeReaderThemeSettings({});
        }
    }

    function writeReaderThemeSettings(settings) {
        try {
            localStorage.setItem(LOCAL_THEME_STORE_KEY, JSON.stringify(normalizeReaderThemeSettings(settings)));
        } catch (_e) {
        }
    }

    function applyReaderThemeSettings(settingsLike) {
        const settings = normalizeReaderThemeSettings(settingsLike);
        runtime.readerTheme = settings;
        const panel = document.getElementById('anchorMountPanel');
        if (panel) {
            panel.setAttribute('data-obs-theme', settings.themeMode);
            panel.style.setProperty('--obs-font-family', resolveReaderFontFamily(settings.fontPreset));
            panel.style.setProperty('--obs-font-size', `${settings.fontSize}px`);
            panel.style.setProperty('--obs-line-height', `${settings.lineHeight}`);
            panel.style.setProperty('--obs-max-width-ch', `${settings.maxWidthCh}ch`);
            panel.style.setProperty('--obs-page-padding', `${settings.pagePaddingPx}px`);
            panel.style.setProperty('--obs-paragraph-gap', `${settings.paragraphGapEm}em`);
            panel.style.setProperty('--obs-list-indent', `${settings.listIndentEm}`);
        }
        return settings;
    }

    function ensureReaderThemeSettingsApplied() {
        if (!runtime.readerTheme) {
            runtime.readerTheme = readReaderThemeSettings();
        }
        return applyReaderThemeSettings(runtime.readerTheme);
    }

    function exposeReaderThemeApi() {
        if (window.__mobileObsidianThemeApi) {
            return;
        }
        window.__mobileObsidianThemeApi = {
            get() {
                return Object.assign({}, runtime.readerTheme || ensureReaderThemeSettingsApplied());
            },
            set(partial) {
                const base = runtime.readerTheme || ensureReaderThemeSettingsApplied();
                const next = normalizeReaderThemeSettings(Object.assign({}, base, partial || {}));
                writeReaderThemeSettings(next);
                applyReaderThemeSettings(next);
                return Object.assign({}, next);
            },
            reset() {
                const defaults = normalizeReaderThemeSettings({});
                writeReaderThemeSettings(defaults);
                applyReaderThemeSettings(defaults);
                return Object.assign({}, defaults);
            },
        };
    }

    function updateSettingsPanelDisplays(scope, settingsLike) {
        const root = scope instanceof Element ? scope : document;
        const settings = normalizeReaderThemeSettings(settingsLike || runtime.readerTheme || {});
        root.querySelectorAll('[data-setting-output]').forEach((node) => {
            const key = t(node.getAttribute('data-setting-output'));
            if (key === 'fontSize') node.textContent = `${settings.fontSize}px`;
            if (key === 'lineHeight') node.textContent = `${settings.lineHeight.toFixed(2)}`;
            if (key === 'maxWidthCh') node.textContent = `${Math.round(settings.maxWidthCh)}ch`;
            if (key === 'pagePaddingPx') node.textContent = `${Math.round(settings.pagePaddingPx)}px`;
            if (key === 'paragraphGapEm') node.textContent = `${settings.paragraphGapEm.toFixed(2)}em`;
            if (key === 'listIndentEm') node.textContent = `${settings.listIndentEm.toFixed(2)}em`;
        });
    }

    function renderObsidianSettingsPanel() {
        const menu = document.getElementById('anchorObsidianCommandMenu');
        if (!menu) {
            return;
        }
        const settings = runtime.readerTheme || ensureReaderThemeSettingsApplied();
        const expanded = !!runtime.obsidianSettingsExpanded;
        menu.innerHTML = `
            <div class="anchor-obsidian-settings-panel">
                <button class="anchor-obsidian-settings-toggle" type="button" data-settings-action="toggle-expanded" aria-expanded="${expanded ? 'true' : 'false'}">
                    <span>阅读与排版</span>
                    <span class="anchor-obsidian-settings-toggle-icon">${expanded ? '▾' : '▸'}</span>
                </button>
                <div class="anchor-obsidian-settings-body${expanded ? ' is-open' : ''}" ${expanded ? '' : 'hidden'}>
                    <label class="anchor-obsidian-settings-row">
                        <span>主题模式</span>
                        <select data-setting="themeMode">
                            <option value="auto" ${settings.themeMode === 'auto' ? 'selected' : ''}>跟随系统</option>
                            <option value="light" ${settings.themeMode === 'light' ? 'selected' : ''}>浅色</option>
                            <option value="dark" ${settings.themeMode === 'dark' ? 'selected' : ''}>深色</option>
                        </select>
                    </label>
                    <label class="anchor-obsidian-settings-row">
                        <span>字体方案</span>
                        <select data-setting="fontPreset">
                            <option value="system" ${settings.fontPreset === 'system' ? 'selected' : ''}>系统无衬线</option>
                            <option value="serif" ${settings.fontPreset === 'serif' ? 'selected' : ''}>阅读衬线</option>
                            <option value="mono" ${settings.fontPreset === 'mono' ? 'selected' : ''}>等宽</option>
                        </select>
                    </label>
                    <label class="anchor-obsidian-settings-row">
                        <span>字号 <b data-setting-output="fontSize"></b></span>
                        <input data-setting="fontSize" type="range" min="12" max="22" step="1" value="${settings.fontSize}">
                    </label>
                    <label class="anchor-obsidian-settings-row">
                        <span>行高 <b data-setting-output="lineHeight"></b></span>
                        <input data-setting="lineHeight" type="range" min="1.3" max="2.2" step="0.01" value="${settings.lineHeight}">
                    </label>
                    <label class="anchor-obsidian-settings-row">
                        <span>最大行宽 <b data-setting-output="maxWidthCh"></b></span>
                        <input data-setting="maxWidthCh" type="range" min="52" max="120" step="1" value="${settings.maxWidthCh}">
                    </label>
                    <label class="anchor-obsidian-settings-row">
                        <span>页边距 <b data-setting-output="pagePaddingPx"></b></span>
                        <input data-setting="pagePaddingPx" type="range" min="8" max="40" step="1" value="${settings.pagePaddingPx}">
                    </label>
                    <label class="anchor-obsidian-settings-row">
                        <span>段落间距 <b data-setting-output="paragraphGapEm"></b></span>
                        <input data-setting="paragraphGapEm" type="range" min="0.3" max="2.2" step="0.01" value="${settings.paragraphGapEm}">
                    </label>
                    <label class="anchor-obsidian-settings-row">
                        <span>列表缩进 <b data-setting-output="listIndentEm"></b></span>
                        <input data-setting="listIndentEm" type="range" min="0.8" max="2.6" step="0.05" value="${settings.listIndentEm}">
                    </label>
                    <div class="anchor-obsidian-settings-actions">
                        <button class="btn btn-ghost-icon" type="button" data-theme-action="reset">重置</button>
                    </div>
                </div>
            </div>
        `;
        updateSettingsPanelDisplays(menu, settings);
    }

    function applyReaderThemePatch(patch) {
        const base = runtime.readerTheme || ensureReaderThemeSettingsApplied();
        const next = normalizeReaderThemeSettings(Object.assign({}, base, patch || {}));
        writeReaderThemeSettings(next);
        applyReaderThemeSettings(next);
        const menu = document.getElementById('anchorObsidianCommandMenu');
        if (menu) {
            updateSettingsPanelDisplays(menu, next);
        }
        return next;
    }

    const VDITOR_CSS_CANDIDATES = [
        '/lib/vditor/index.css',
        'https://cdn.jsdelivr.net/npm/vditor@3.10.9/dist/index.css',
        'https://unpkg.com/vditor@3.10.9/dist/index.css',
    ];
    const VDITOR_JS_CANDIDATES = [
        '/lib/vditor/index.min.js',
        'https://cdn.jsdelivr.net/npm/vditor@3.10.9/dist/index.min.js',
        'https://unpkg.com/vditor@3.10.9/dist/index.min.js',
    ];

    function isVditorReady() {
        return !!(runtime.vditorReady && runtime.vditorInstance && typeof runtime.vditorInstance.getValue === 'function');
    }

    function readEditorValue() {
        if (isVditorReady()) {
            return String(runtime.vditorInstance.getValue() || '');
        }
        const editor = document.getElementById('anchorQuickNoteInput');
        return String(editor && editor.value || '');
    }

    function writeEditorValue(value, options = {}) {
        const nextValue = String(value || '');
        const editor = document.getElementById('anchorQuickNoteInput');
        if (editor && String(editor.value || '') !== nextValue) {
            editor.value = nextValue;
        }
        if (isVditorReady()) {
            const current = String(runtime.vditorInstance.getValue() || '');
            if (current !== nextValue) {
                runtime.vditorSyncing = true;
                runtime.vditorInstance.setValue(nextValue, !!options.clearHistory);
                runtime.vditorSyncing = false;
            }
        }
    }

    function focusEditorInput() {
        if (isVditorReady() && typeof runtime.vditorInstance.focus === 'function') {
            runtime.vditorInstance.focus();
            return;
        }
        const editor = document.getElementById('anchorQuickNoteInput');
        if (editor && typeof editor.focus === 'function') {
            editor.focus();
        }
    }

    function isAnchorPanelDetailMode() {
        const panel = document.getElementById('anchorMountPanel');
        return !!(panel && panel.classList.contains('is-detail-mode'));
    }

    function isEditorFocusTarget(target) {
        const textarea = document.getElementById('anchorQuickNoteInput');
        const vditorHost = document.getElementById('anchorQuickNoteVditor');
        const active = document.activeElement;
        const probe = target instanceof Element ? target : active;
        if (!(probe instanceof Element)) {
            return false;
        }
        if (textarea && (probe === textarea || textarea.contains(probe))) {
            return true;
        }
        if (vditorHost && vditorHost.contains(probe)) {
            return true;
        }
        if (!(active instanceof Element)) {
            return false;
        }
        if (textarea && (active === textarea || textarea.contains(active))) {
            return true;
        }
        return !!(vditorHost && vditorHost.contains(active));
    }

    function dispatchTextareaInputEvent() {
        const editor = document.getElementById('anchorQuickNoteInput');
        if (!(editor instanceof HTMLTextAreaElement)) {
            return;
        }
        editor.dispatchEvent(new Event('input', { bubbles: true }));
    }

    function syncTextareaFromVditor() {
        if (!isVditorReady()) {
            return false;
        }
        const editor = document.getElementById('anchorQuickNoteInput');
        if (!(editor instanceof HTMLTextAreaElement)) {
            return false;
        }
        const value = String(runtime.vditorInstance.getValue() || '');
        if (String(editor.value || '') !== value) {
            editor.value = value;
        }
        dispatchTextareaInputEvent();
        return true;
    }

    function runVditorExecCommand(command, value) {
        if (!isVditorReady()) {
            return false;
        }
        if (typeof runtime.vditorInstance.focus === 'function') {
            runtime.vditorInstance.focus();
        }
        try {
            document.execCommand(String(command || ''), false, value);
        } catch (_e) {
            return false;
        }
        syncTextareaFromVditor();
        return true;
    }

    function normalizeMarkdownLineBody(lineLike) {
        const line = String(lineLike || '');
        const indentMatch = line.match(/^(\s*)/);
        const indent = indentMatch ? indentMatch[1] : '';
        const body = line
            .slice(indent.length)
            .replace(/^(?:#{1,6}\s+|[-*+]\s+|\d+\.\s+)/, '')
            .trimStart();
        return { indent, body };
    }

    function transformSelectedLinesInTextarea(mapper) {
        const editor = document.getElementById('anchorQuickNoteInput');
        if (!(editor instanceof HTMLTextAreaElement) || typeof mapper !== 'function') {
            return false;
        }
        const value = String(editor.value || '');
        const selectionStart = Number.isInteger(Number(editor.selectionStart)) ? Number(editor.selectionStart) : 0;
        const selectionEndRaw = Number.isInteger(Number(editor.selectionEnd)) ? Number(editor.selectionEnd) : selectionStart;
        const selectionEnd = Math.max(selectionStart, selectionEndRaw);
        const blockStart = value.lastIndexOf('\n', Math.max(0, selectionStart - 1)) + 1;
        let blockEnd = value.indexOf('\n', selectionEnd);
        if (blockEnd < 0) {
            blockEnd = value.length;
        }
        const segment = value.slice(blockStart, blockEnd);
        const lines = segment.split('\n');
        const nextSegment = lines.map((line, index) => mapper(line, index, lines.length)).join('\n');
        editor.value = `${value.slice(0, blockStart)}${nextSegment}${value.slice(blockEnd)}`;
        editor.setSelectionRange(blockStart, blockStart + nextSegment.length);
        dispatchTextareaInputEvent();
        return true;
    }

    function applyHeadingShortcut() {
        if (isVditorReady()) {
            if (runVditorExecCommand('formatBlock', '<h1>')) {
                return true;
            }
            return runVditorExecCommand('formatBlock', 'h1');
        }
        return transformSelectedLinesInTextarea((line) => {
            const normalized = normalizeMarkdownLineBody(line);
            return `${normalized.indent}# ${normalized.body}`;
        });
    }

    function applyUnorderedListShortcut() {
        if (isVditorReady()) {
            return runVditorExecCommand('insertUnorderedList');
        }
        return transformSelectedLinesInTextarea((line) => {
            const normalized = normalizeMarkdownLineBody(line);
            return `${normalized.indent}- ${normalized.body}`;
        });
    }

    function applyOrderedListShortcut() {
        if (isVditorReady()) {
            return runVditorExecCommand('insertOrderedList');
        }
        return transformSelectedLinesInTextarea((line, index) => {
            const normalized = normalizeMarkdownLineBody(line);
            return `${normalized.indent}${index + 1}. ${normalized.body}`;
        });
    }

    function applyBoldShortcut() {
        if (isVditorReady()) {
            return runVditorExecCommand('bold');
        }
        const editor = document.getElementById('anchorQuickNoteInput');
        if (!(editor instanceof HTMLTextAreaElement)) {
            return false;
        }
        const value = String(editor.value || '');
        const start = Number.isInteger(Number(editor.selectionStart)) ? Number(editor.selectionStart) : 0;
        const endRaw = Number.isInteger(Number(editor.selectionEnd)) ? Number(editor.selectionEnd) : start;
        const end = Math.max(start, endRaw);
        if (start === end) {
            editor.value = `${value.slice(0, start)}****${value.slice(end)}`;
            editor.setSelectionRange(start + 2, start + 2);
        } else {
            editor.value = `${value.slice(0, start)}**${value.slice(start, end)}**${value.slice(end)}`;
            editor.setSelectionRange(start + 2, end + 2);
        }
        dispatchTextareaInputEvent();
        return true;
    }

    function setObsidianWorkspaceState(options = {}) {
        if (typeof options.contextCollapsed === 'boolean') {
            runtime.obsidianContextCollapsed = options.contextCollapsed;
        }
        if (typeof options.focusMode === 'boolean') {
            runtime.obsidianFocusMode = options.focusMode;
        }
        const mountPanel = document.getElementById('anchorMountPanel');
        const quickNote = document.querySelector('#anchorComposerShell .anchor-quick-note');
        if (mountPanel) {
            mountPanel.classList.toggle('is-obsidian-focus', !!runtime.obsidianFocusMode);
        }
        if (quickNote) {
            quickNote.classList.toggle('is-obsidian-context-collapsed', !!runtime.obsidianContextCollapsed);
        }
    }

    function loadCssWithFallback(urls) {
        const candidates = Array.isArray(urls) ? urls.filter(Boolean) : [];
        if (!candidates.length) {
            return Promise.resolve(false);
        }
        let chain = Promise.resolve(false);
        candidates.forEach((url) => {
            chain = chain.then((loaded) => {
                if (loaded) {
                    return true;
                }
                return new Promise((resolve) => {
                    const existing = document.querySelector(`link[data-vditor-css="${url}"]`);
                    if (existing) {
                        resolve(true);
                        return;
                    }
                    const link = document.createElement('link');
                    link.rel = 'stylesheet';
                    link.href = url;
                    link.setAttribute('data-vditor-css', url);
                    link.onload = () => resolve(true);
                    link.onerror = () => {
                        link.remove();
                        resolve(false);
                    };
                    document.head.appendChild(link);
                });
            });
        });
        return chain;
    }

    function loadScriptWithFallback(urls) {
        const candidates = Array.isArray(urls) ? urls.filter(Boolean) : [];
        if (!candidates.length) {
            return Promise.resolve(false);
        }
        let chain = Promise.resolve(false);
        candidates.forEach((url) => {
            chain = chain.then((loaded) => {
                if (loaded || window.Vditor) {
                    return true;
                }
                return new Promise((resolve) => {
                    const existing = document.querySelector(`script[data-vditor-js="${url}"]`);
                    if (existing) {
                        const done = !!window.Vditor;
                        resolve(done);
                        return;
                    }
                    const script = document.createElement('script');
                    script.async = true;
                    script.src = url;
                    script.setAttribute('data-vditor-js', url);
                    script.onload = () => resolve(!!window.Vditor);
                    script.onerror = () => {
                        script.remove();
                        resolve(false);
                    };
                    document.head.appendChild(script);
                });
            });
        });
        return chain;
    }

    async function ensureVditorRuntime() {
        if (window.Vditor) {
            runtime.vditorReady = true;
            return true;
        }
        if (runtime.vditorLoadPromise) {
            return runtime.vditorLoadPromise;
        }
        runtime.vditorLoadPromise = (async () => {
            await loadCssWithFallback(VDITOR_CSS_CANDIDATES);
            const jsReady = await loadScriptWithFallback(VDITOR_JS_CANDIDATES);
            runtime.vditorReady = !!(jsReady && window.Vditor);
            return runtime.vditorReady;
        })();
        return runtime.vditorLoadPromise;
    }

    function getPhase2bElements() {
        return {
            panel: document.getElementById('anchorMountPanel'),
            dock: document.getElementById('anchorPhase2bDock'),
            capsuleBtn: document.getElementById('anchorPhase2bCapsuleBtn'),
            capsuleIndicator: document.getElementById('anchorPhase2bCapsuleIndicator'),
            canvas: document.getElementById('anchorPhase2bCanvas'),
            actionLayer: document.getElementById('anchorPhase2bActionLayer'),
            fileChips: document.getElementById('anchorPhase2bFileChips'),
            inputWrap: document.getElementById('anchorPhase2bInputWrap'),
            input: document.getElementById('anchorPhase2bInput'),
            submitBtn: document.getElementById('anchorPhase2bSubmitBtn'),
            headSubmitBtn: document.getElementById('anchorPhase2bHeadSubmitBtn'),
            headCopyBtn: document.getElementById('anchorPhase2bHeadCopyBtn'),
            toggleInputBtn: document.getElementById('anchorPhase2bToggleInputBtn'),
            processingWrap: document.getElementById('anchorPhase2bProcessing'),
            processingText: document.getElementById('anchorPhase2bProcessingText'),
            resultWrap: document.getElementById('anchorPhase2bResult'),
            resultPreview: document.getElementById('anchorPhase2bResultPreview'),
            copyBtn: document.getElementById('anchorPhase2bCopyBtn'),
            feedback: document.getElementById('anchorPhase2bFeedback'),
            resizer: document.getElementById('anchorPhase2bResizer'),
        };
    }

    function normalizePhase2bCanvasSize(rawWidth, rawHeight) {
        const width = Number(rawWidth);
        const height = Number(rawHeight);
        if (!Number.isFinite(width) || !Number.isFinite(height)) {
            return null;
        }
        return {
            width: Math.round(Math.max(320, Math.min(920, width))),
            height: Math.round(Math.max(260, Math.min(860, height))),
        };
    }

    function normalizePhase2bDockOffset(rawX, rawY) {
        const x = Number(rawX);
        const y = Number(rawY);
        if (!Number.isFinite(x) || !Number.isFinite(y)) {
            return null;
        }
        return {
            x: Math.round(Math.max(0, x)),
            y: Math.round(Math.max(0, y)),
        };
    }

    function readPhase2bLayoutState() {
        try {
            const raw = localStorage.getItem(PHASE2B_LAYOUT_STORE_KEY);
            if (!raw) {
                return null;
            }
            const parsed = JSON.parse(raw);
            const normalizedSize = normalizePhase2bCanvasSize(parsed && parsed.width, parsed && parsed.height);
            const normalizedDock = normalizePhase2bDockOffset(parsed && parsed.x, parsed && parsed.y);
            if (!normalizedSize && !normalizedDock) {
                return null;
            }
            return {
                width: normalizedSize ? normalizedSize.width : null,
                height: normalizedSize ? normalizedSize.height : null,
                x: normalizedDock ? normalizedDock.x : null,
                y: normalizedDock ? normalizedDock.y : null,
            };
        } catch (_e) {
            return null;
        }
    }

    function writePhase2bLayoutState() {
        const phase2b = runtime.phase2b;
        const normalizedSize = normalizePhase2bCanvasSize(phase2b.canvasWidth, phase2b.canvasHeight);
        const normalizedDock = normalizePhase2bDockOffset(phase2b.moveX, phase2b.moveY);
        if (!normalizedSize && !normalizedDock) {
            return;
        }
        const payload = {};
        if (normalizedSize) {
            payload.width = normalizedSize.width;
            payload.height = normalizedSize.height;
        }
        if (normalizedDock) {
            payload.x = normalizedDock.x;
            payload.y = normalizedDock.y;
        }
        try {
            localStorage.setItem(PHASE2B_LAYOUT_STORE_KEY, JSON.stringify(payload));
        } catch (_e) {
            // ignore localStorage write error
        }
    }

    function ensurePhase2bLayoutStateLoaded() {
        const phase2b = runtime.phase2b;
        if (phase2b.layoutLoaded) {
            return;
        }
        phase2b.layoutLoaded = true;
        const saved = readPhase2bLayoutState();
        if (!saved) {
            return;
        }
        if (Number.isFinite(Number(saved.width)) && Number.isFinite(Number(saved.height))) {
            phase2b.canvasWidth = Number(saved.width);
            phase2b.canvasHeight = Number(saved.height);
        }
        if (Number.isFinite(Number(saved.x)) && Number.isFinite(Number(saved.y))) {
            phase2b.moveX = Number(saved.x);
            phase2b.moveY = Number(saved.y);
        }
    }

    function clearPhase2bProgressState() {
        const phase2b = runtime.phase2b;
        phase2b.progressText = '';
        phase2b.streamActive = false;
        phase2b.streamChunkCount = 0;
    }

    function clearPhase2bCompletionNotice() {
        const phase2b = runtime.phase2b;
        phase2b.noticeText = '';
        if (phase2b.noticeTimer) {
            clearTimeout(phase2b.noticeTimer);
            phase2b.noticeTimer = 0;
        }
    }

    function derivePhase2bCompletionNoticeText() {
        const links = Array.isArray(runtime.phase2b.linkItems) ? runtime.phase2b.linkItems : [];
        if (!links.length) {
            return 'Phase2B 重构完成';
        }
        const hasJuejin = links.some((item) => inferPhase2bLinkSite(item && item.url) === 'juejin');
        const hasZhihu = links.some((item) => inferPhase2bLinkSite(item && item.url) === 'zhihu');
        if (hasJuejin && !hasZhihu) {
            return '掘金文章重写完毕';
        }
        if (hasZhihu && !hasJuejin) {
            return '知乎文章重写完毕';
        }
        return '多源文章重写完毕';
    }

    function playPhase2bNoticeTone() {
        const AudioContextImpl = window.AudioContext || window.webkitAudioContext;
        if (typeof AudioContextImpl !== 'function') {
            return;
        }
        try {
            const ctx = new AudioContextImpl();
            const osc = ctx.createOscillator();
            const gain = ctx.createGain();
            osc.type = 'sine';
            osc.frequency.setValueAtTime(880, ctx.currentTime);
            gain.gain.setValueAtTime(0.0001, ctx.currentTime);
            gain.gain.exponentialRampToValueAtTime(0.03, ctx.currentTime + 0.015);
            gain.gain.exponentialRampToValueAtTime(0.0001, ctx.currentTime + 0.12);
            osc.connect(gain);
            gain.connect(ctx.destination);
            osc.start();
            osc.stop(ctx.currentTime + 0.13);
            setTimeout(() => {
                try {
                    ctx.close();
                } catch (_e) {
                    // ignore close error
                }
            }, 220);
        } catch (_e) {
            // ignore audio permission errors
        }
    }

    function showPhase2bCompletionNotice(textLike) {
        const text = t(textLike);
        if (!text) {
            return;
        }
        const phase2b = runtime.phase2b;
        clearPhase2bCompletionNotice();
        phase2b.noticeText = text;
        phase2b.noticeTimer = setTimeout(() => {
            clearPhase2bCompletionNotice();
            renderPhase2bFloatingUi();
        }, 3000);
        playPhase2bNoticeTone();
        renderPhase2bFloatingUi();
    }

    function isPhase2bRequestInFlight() {
        const phase2b = runtime.phase2b;
        const inFlightSeq = Number(phase2b.inFlightRequestSeq);
        const requestSeq = Number(phase2b.requestSeq);
        return !!phase2b.processing
            && Number.isFinite(inFlightSeq)
            && inFlightSeq > 0
            && inFlightSeq === requestSeq;
    }

    function getPhase2bProgressChannel() {
        const phase2b = runtime.phase2b;
        if (t(phase2b.progressChannel)) {
            return phase2b.progressChannel;
        }
        phase2b.progressChannel = `phase2b_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`.replace(/[^A-Za-z0-9:_\-.]/g, '');
        return phase2b.progressChannel;
    }

    function normalizePhase2bRequestId(requestLike) {
        const raw = t(requestLike).replace(/[^A-Za-z0-9:_\-.]/g, '');
        if (!raw) {
            return '';
        }
        return raw.slice(0, 120);
    }

    function resolvePhase2bWebSocketUrl() {
        const fromWindow = t(window.__mobileTaskWebSocketUrl || window.__mobilePhase2bWsUrl);
        if (fromWindow) {
            return fromWindow;
        }
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        return `${protocol}//${window.location.host}/ws/tasks`;
    }

    function ensurePhase2bWebSocketConnected() {
        const phase2b = runtime.phase2b;
        const current = phase2b.ws;
        if (current && current.readyState === WebSocket.OPEN) {
            return Promise.resolve(true);
        }
        if (phase2b.wsConnectPromise) {
            return phase2b.wsConnectPromise;
        }
        phase2b.wsConnectPromise = new Promise((resolve) => {
            const wsUrl = resolvePhase2bWebSocketUrl();
            let ws;
            try {
                ws = new WebSocket(wsUrl);
            } catch (_e) {
                phase2b.ws = null;
                phase2b.wsConnectPromise = null;
                resolve(false);
                return;
            }
            phase2b.ws = ws;
            const finish = (ok) => {
                if (phase2b.wsConnectPromise) {
                    phase2b.wsConnectPromise = null;
                }
                resolve(ok);
            };
            const openTimer = setTimeout(() => {
                if (ws.readyState === WebSocket.CONNECTING) {
                    try {
                        ws.close();
                    } catch (_e) {
                        // ignore close error
                    }
                }
            }, 2200);
            ws.addEventListener('open', () => {
                clearTimeout(openTimer);
                const channel = getPhase2bProgressChannel();
                try {
                    ws.send(JSON.stringify({ action: 'subscribePhase2b', channel }));
                } catch (_e) {
                    finish(false);
                    return;
                }
                finish(true);
            }, { once: true });
            ws.addEventListener('message', (event) => {
                let payload = null;
                try {
                    payload = JSON.parse(String(event && event.data || ''));
                } catch (_e) {
                    payload = null;
                }
                if (!payload) {
                    return;
                }
                const payloadType = t(payload.type);
                if (payloadType !== 'phase2bProgress'
                    && payloadType !== 'phase2bMarkdownChunk'
                    && payloadType !== 'phase2bMarkdownFinal') {
                    return;
                }
                if (t(payload.channel) !== t(runtime.phase2b.progressChannel)) {
                    return;
                }
                const incomingRequestId = normalizePhase2bRequestId(payload.requestId);
                const currentRequestId = normalizePhase2bRequestId(runtime.phase2b.currentRequestId);
                if (currentRequestId && incomingRequestId && currentRequestId !== incomingRequestId) {
                    return;
                }
                if (payloadType === 'phase2bProgress') {
                    const nextText = t(payload.message);
                    if (nextText) {
                        runtime.phase2b.progressText = nextText;
                    }
                    const done = payload.done === true;
                    const success = payload.success === true;
                    if (done && success && !runtime.phase2b.expanded) {
                        showPhase2bCompletionNotice(derivePhase2bCompletionNoticeText());
                    }
                    renderPhase2bFloatingUi();
                    return;
                }
                if (payloadType === 'phase2bMarkdownFinal') {
                    const finalMarkdown = String(payload.markdown || '');
                    runtime.phase2b.streamActive = false;
                    if (finalMarkdown || Number(runtime.phase2b.streamChunkCount || 0) > 0) {
                        runtime.phase2b.inputValue = finalMarkdown;
                        runtime.phase2b.resultValue = finalMarkdown;
                    }
                    renderPhase2bFloatingUi();
                    return;
                }
                const chunkText = String(payload.chunk || '');
                const isDone = payload.done === true;
                if (chunkText) {
                    if (!runtime.phase2b.streamActive && Number(runtime.phase2b.streamChunkCount || 0) === 0) {
                        runtime.phase2b.inputValue = '';
                        runtime.phase2b.resultValue = '';
                    }
                    runtime.phase2b.streamActive = true;
                    runtime.phase2b.streamChunkCount = Number(runtime.phase2b.streamChunkCount || 0) + 1;
                    runtime.phase2b.inputValue = `${String(runtime.phase2b.inputValue || '')}${chunkText}`;
                    runtime.phase2b.resultValue = runtime.phase2b.inputValue;
                }
                if (isDone) {
                    runtime.phase2b.streamActive = false;
                }
                renderPhase2bFloatingUi();
            });
            ws.addEventListener('close', () => {
                if (runtime.phase2b.ws === ws) {
                    runtime.phase2b.ws = null;
                }
                if (runtime.phase2b.wsConnectPromise) {
                    runtime.phase2b.wsConnectPromise = null;
                }
            });
            ws.addEventListener('error', () => {
                clearTimeout(openTimer);
                finish(false);
            }, { once: true });
        });
        return phase2b.wsConnectPromise;
    }

    function closePhase2bWebSocket() {
        const phase2b = runtime.phase2b;
        const ws = phase2b.ws;
        if (!ws) {
            return;
        }
        try {
            if (ws.readyState === WebSocket.OPEN && t(phase2b.progressChannel)) {
                ws.send(JSON.stringify({ action: 'unsubscribePhase2b', channel: phase2b.progressChannel }));
            }
        } catch (_e) {
            // ignore unsubscribe failure on page teardown
        }
        try {
            ws.close();
        } catch (_e) {
            // ignore close failure
        }
        phase2b.ws = null;
        phase2b.wsConnectPromise = null;
    }

    function resolvePhase2bEndpoint() {
        const fromWindow = t(window.__mobilePhase2bEndpoint);
        if (fromWindow) {
            return fromWindow;
        }
        const base = t(runtime.ctx && runtime.ctx.apiBase) || '/api/mobile';
        return `${base.replace(/\/+$/, '')}${PHASE2B_ENDPOINT_SUFFIX}`;
    }

    function resolvePhase2bLinkMetadataEndpoint() {
        const fromWindow = t(window.__mobilePhase2bLinkMetadataEndpoint);
        if (fromWindow) {
            return fromWindow;
        }
        const base = t(runtime.ctx && runtime.ctx.apiBase) || '/api/mobile';
        return `${base.replace(/\/+$/, '')}${PHASE2B_LINK_METADATA_ENDPOINT_SUFFIX}`;
    }

    async function callPhase2bLinkMetadata(urlsLike) {
        const urls = (Array.isArray(urlsLike) ? urlsLike : [urlsLike])
            .map((item) => normalizePhase2bArticleLink(item))
            .filter((item, index, arr) => !!item && arr.indexOf(item) === index);
        if (!urls.length) {
            return [];
        }
        const endpoint = resolvePhase2bLinkMetadataEndpoint();
        const resp = await fetch(endpoint, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ linkUrls: urls }),
        });
        const raw = await resp.text();
        let parsed = null;
        if (raw) {
            try {
                parsed = JSON.parse(raw);
            } catch (_e) {
                parsed = null;
            }
        }
        if (!resp.ok) {
            throw new Error(t(parsed && parsed.message) || t(raw) || `HTTP ${resp.status}`);
        }
        const candidates = [
            parsed && parsed.links,
            parsed && parsed.data && parsed.data.links,
            parsed && parsed.payload && parsed.payload.links,
        ];
        for (let i = 0; i < candidates.length; i += 1) {
            if (Array.isArray(candidates[i])) {
                return candidates[i];
            }
        }
        return [];
    }

    async function prefetchPhase2bLinkTitles(urlsLike) {
        const urls = (Array.isArray(urlsLike) ? urlsLike : [urlsLike])
            .map((item) => normalizePhase2bArticleLink(item))
            .filter((item, index, arr) => !!item && arr.indexOf(item) === index);
        if (!urls.length) {
            return;
        }
        const phase2b = runtime.phase2b;
        const inFlightSet = phase2b.linkPrefetchInFlight instanceof Set
            ? phase2b.linkPrefetchInFlight
            : new Set();
        phase2b.linkPrefetchInFlight = inFlightSet;
        const requestUrls = [];
        urls.forEach((url) => {
            if (inFlightSet.has(url)) {
                return;
            }
            inFlightSet.add(url);
            requestUrls.push(url);
        });
        if (!requestUrls.length) {
            return;
        }
        if (upsertPhase2bLinkItems(requestUrls, { status: 'fetching' })) {
            renderPhase2bFloatingUi();
        }
        try {
            const metadataLinks = await callPhase2bLinkMetadata(requestUrls);
            const resolvedMap = {};
            requestUrls.forEach((url) => {
                resolvedMap[url] = {
                    title: '',
                    site: inferPhase2bLinkSite(url),
                    status: 'failed',
                };
            });
            metadataLinks.forEach((item) => {
                const normalizedUrl = normalizePhase2bArticleLink(item && item.url);
                if (!normalizedUrl || !resolvedMap[normalizedUrl]) {
                    return;
                }
                const title = t(item && item.title);
                const statusRaw = t(item && item.status).toLowerCase();
                const status = statusRaw === 'pending' && !title ? 'failed' : statusRaw;
                resolvedMap[normalizedUrl] = {
                    title,
                    site: t(item && (item.siteType || item.site)) || inferPhase2bLinkSite(normalizedUrl),
                    status: status || (title ? 'resolved' : 'failed'),
                };
            });
            if (upsertPhase2bLinkItems(requestUrls, { resolvedMap })) {
                renderPhase2bFloatingUi();
            }
        } catch (_e) {
            const fallbackMap = {};
            requestUrls.forEach((url) => {
                fallbackMap[url] = {
                    title: '',
                    site: inferPhase2bLinkSite(url),
                    status: 'failed',
                };
            });
            if (upsertPhase2bLinkItems(requestUrls, { resolvedMap: fallbackMap })) {
                renderPhase2bFloatingUi();
            }
        } finally {
            requestUrls.forEach((url) => inFlightSet.delete(url));
        }
    }

    function updatePhase2bInputHeight() {
        const input = document.getElementById('anchorPhase2bInput');
        const panel = document.getElementById('anchorMountPanel');
        const canvas = document.getElementById('anchorPhase2bCanvas');
        if (!(input instanceof HTMLTextAreaElement)) {
            return;
        }
        const panelHeight = panel ? panel.clientHeight : window.innerHeight;
        const canvasHeight = canvas ? canvas.clientHeight : panelHeight;
        const maxHeight = Math.max(120, Math.min(Math.floor(panelHeight * 0.6), Math.floor(canvasHeight * 0.42)));
        input.style.maxHeight = `${maxHeight}px`;
        input.style.height = 'auto';
        input.style.height = `${Math.min(input.scrollHeight, maxHeight)}px`;
    }

    function syncPhase2bInputFromTextarea(textareaLike) {
        const textarea = textareaLike instanceof HTMLTextAreaElement ? textareaLike : document.getElementById('anchorPhase2bInput');
        if (!(textarea instanceof HTMLTextAreaElement)) {
            return false;
        }
        runtime.phase2b.inputValue = String(textarea.value || '');
        runtime.phase2b.mode = 'input';
        runtime.phase2b.error = '';
        updatePhase2bInputHeight();
        renderPhase2bFloatingUi();
        return true;
    }

    function withPhase2bTextareaSelection(editorLike, handler) {
        const editor = editorLike instanceof HTMLTextAreaElement ? editorLike : document.getElementById('anchorPhase2bInput');
        if (!(editor instanceof HTMLTextAreaElement) || typeof handler !== 'function') {
            return false;
        }
        const value = String(editor.value || '');
        const start = Number.isInteger(Number(editor.selectionStart)) ? Number(editor.selectionStart) : 0;
        const endRaw = Number.isInteger(Number(editor.selectionEnd)) ? Number(editor.selectionEnd) : start;
        const end = Math.max(start, endRaw);
        const next = handler({ value, start, end });
        if (!next || typeof next.value !== 'string') {
            return false;
        }
        editor.value = next.value;
        const nextStart = Number.isInteger(Number(next.start)) ? Number(next.start) : start;
        const nextEnd = Number.isInteger(Number(next.end)) ? Number(next.end) : nextStart;
        editor.setSelectionRange(nextStart, nextEnd);
        return syncPhase2bInputFromTextarea(editor);
    }

    function applyPhase2bBoldShortcut(editorLike) {
        return withPhase2bTextareaSelection(editorLike, ({ value, start, end }) => {
            if (start === end) {
                return {
                    value: `${value.slice(0, start)}****${value.slice(end)}`,
                    start: start + 2,
                    end: start + 2,
                };
            }
            return {
                value: `${value.slice(0, start)}**${value.slice(start, end)}**${value.slice(end)}`,
                start: start + 2,
                end: end + 2,
            };
        });
    }

    function prefixPhase2bSelectedLines(editorLike, mapLine) {
        return withPhase2bTextareaSelection(editorLike, ({ value, start, end }) => {
            const blockStart = value.lastIndexOf('\n', Math.max(0, start - 1)) + 1;
            let blockEnd = value.indexOf('\n', end);
            if (blockEnd < 0) {
                blockEnd = value.length;
            }
            const segment = value.slice(blockStart, blockEnd);
            const lines = segment.split('\n');
            const nextSegment = lines.map((line, index) => mapLine(String(line || ''), index)).join('\n');
            return {
                value: `${value.slice(0, blockStart)}${nextSegment}${value.slice(blockEnd)}`,
                start: blockStart,
                end: blockStart + nextSegment.length,
            };
        });
    }

    function normalizePhase2bLineBody(lineLike) {
        const line = String(lineLike || '');
        const indent = (line.match(/^(\s*)/) || ['', ''])[1];
        const body = line
            .slice(indent.length)
            .replace(/^(?:#{1,6}\s+|[-*+]\s+|\d+\.\s+)/, '')
            .trimStart();
        return { indent, body };
    }

    function applyPhase2bHeadingShortcut(editorLike, levelLike) {
        const level = Math.max(1, Math.min(6, Number(levelLike) || 1));
        const marker = `${'#'.repeat(level)} `;
        return prefixPhase2bSelectedLines(editorLike, (line) => {
            const normalized = normalizePhase2bLineBody(line);
            return `${normalized.indent}${marker}${normalized.body}`;
        });
    }

    function applyPhase2bUnorderedListShortcut(editorLike) {
        return prefixPhase2bSelectedLines(editorLike, (line) => {
            const normalized = normalizePhase2bLineBody(line);
            return `${normalized.indent}- ${normalized.body}`;
        });
    }

    function applyPhase2bOrderedListShortcut(editorLike) {
        return prefixPhase2bSelectedLines(editorLike, (line, index) => {
            const normalized = normalizePhase2bLineBody(line);
            return `${normalized.indent}${index + 1}. ${normalized.body}`;
        });
    }

    function applyPhase2bDockPosition(dockLike) {
        const dock = dockLike instanceof HTMLElement ? dockLike : document.getElementById('anchorPhase2bDock');
        if (!(dock instanceof HTMLElement)) {
            return;
        }
        const panel = document.getElementById('anchorMountPanel');
        if (!(panel instanceof HTMLElement)) {
            return;
        }
        const phase2b = runtime.phase2b;
        const x = Number(phase2b.moveX);
        const y = Number(phase2b.moveY);
        const hasManualPosition = Number.isFinite(x) && Number.isFinite(y);
        if (!hasManualPosition) {
            dock.style.left = '';
            dock.style.top = '';
            dock.style.right = '';
            dock.style.bottom = '';
            return;
        }
        const maxX = Math.max(0, panel.clientWidth - dock.offsetWidth);
        const maxY = Math.max(0, panel.clientHeight - dock.offsetHeight);
        const clampedX = Math.min(maxX, Math.max(0, x));
        const clampedY = Math.min(maxY, Math.max(0, y));
        phase2b.moveX = clampedX;
        phase2b.moveY = clampedY;
        dock.style.left = `${Math.round(clampedX)}px`;
        dock.style.top = `${Math.round(clampedY)}px`;
        dock.style.right = 'auto';
        dock.style.bottom = 'auto';
    }

    function applyPhase2bCanvasSize(canvasLike) {
        const canvas = canvasLike instanceof HTMLElement ? canvasLike : document.getElementById('anchorPhase2bCanvas');
        if (!(canvas instanceof HTMLElement)) {
            return;
        }
        const panel = document.getElementById('anchorMountPanel');
        if (!(panel instanceof HTMLElement)) {
            return;
        }
        const phase2b = runtime.phase2b;
        const preferred = normalizePhase2bCanvasSize(phase2b.canvasWidth, phase2b.canvasHeight);
        if (!preferred) {
            canvas.style.width = '';
            canvas.style.height = '';
            return;
        }
        const maxWidth = Math.max(320, panel.clientWidth - 10);
        const maxHeight = Math.max(260, panel.clientHeight - 36);
        const clampedWidth = Math.max(320, Math.min(maxWidth, preferred.width));
        const clampedHeight = Math.max(260, Math.min(maxHeight, preferred.height));
        phase2b.canvasWidth = Math.round(clampedWidth);
        phase2b.canvasHeight = Math.round(clampedHeight);
        canvas.style.width = `${Math.round(clampedWidth)}px`;
        canvas.style.height = `${Math.round(clampedHeight)}px`;
    }

    function ensurePhase2bFloatingStyle() {
        if (document.getElementById('anchorPhase2bStyle')) {
            return;
        }
        const style = document.createElement('style');
        style.id = 'anchorPhase2bStyle';
        style.textContent = `
            #anchorMountPanel { position: relative; }
            #anchorMountPanel .anchor-phase2b-dock { position: absolute; right: clamp(10px, 2vw, 24px); bottom: clamp(10px, 2vh, 24px); z-index: 42; display: flex; flex-direction: column; align-items: flex-end; pointer-events: none; max-width: calc(100% - 10px); }
            #anchorMountPanel .anchor-phase2b-dock > * { pointer-events: auto; }
            #anchorMountPanel .anchor-phase2b-capsule { width: 44px; height: 44px; border-radius: 999px; border: 1px solid rgba(148,163,184,.34); background: rgba(255,255,255,.74); color: #0f172a; backdrop-filter: blur(20px) saturate(180%); box-shadow: 0 12px 30px rgba(15,23,42,.12); display: inline-flex; align-items: center; justify-content: center; cursor: grab; transition: transform .22s cubic-bezier(.2,.8,.2,1), box-shadow .22s cubic-bezier(.2,.8,.2,1), opacity .2s ease; touch-action: none; user-select: none; }
            #anchorMountPanel .anchor-phase2b-capsule:hover { transform: translateY(-1px) scale(1.04); box-shadow: 0 16px 38px rgba(15,23,42,.2); }
            #anchorMountPanel .anchor-phase2b-dock.is-moving .anchor-phase2b-capsule { cursor: grabbing; }
            #anchorMountPanel .anchor-phase2b-capsule-icon { font-size: 15px; color: #334155; line-height: 1; }
            #anchorMountPanel .anchor-phase2b-capsule-label { display: none !important; }
            #anchorMountPanel .anchor-phase2b-capsule-indicator { width: 8px; height: 8px; border-radius: 999px; background: transparent; box-shadow: 0 0 0 0 transparent; transition: all .2s ease; }
            #anchorMountPanel .anchor-phase2b-dock.is-processing .anchor-phase2b-capsule-indicator { background: #16a34a; animation: anchorPhase2bPulse 1.5s ease-in-out infinite; }
            #anchorMountPanel .anchor-phase2b-dock.is-ready:not(.is-processing) .anchor-phase2b-capsule-indicator { background: #2563eb; box-shadow: 0 0 0 5px rgba(37,99,235,.17); }
            #anchorMountPanel .anchor-phase2b-toast { margin-top: 8px; padding: 6px 10px; border-radius: 9px; background: rgba(15,23,42,.86); color: #f8fafc; font-size: 11px; line-height: 1.3; letter-spacing: .01em; opacity: 0; transform: translateY(6px) scale(.98); transition: opacity .22s ease, transform .22s ease; pointer-events: none; max-width: min(260px, 72vw); box-shadow: 0 12px 30px rgba(15,23,42,.24); }
            #anchorMountPanel .anchor-phase2b-dock.is-notice .anchor-phase2b-toast { opacity: 1; transform: translateY(0) scale(1); }
            #anchorMountPanel .anchor-phase2b-dock.is-notice .anchor-phase2b-capsule { animation: anchorPhase2bNotify .32s ease 1; }
            #anchorMountPanel .anchor-phase2b-canvas { width: min(460px, calc(100vw - 56px), calc(100% - 8px)); max-height: min(74vh, 760px, calc(100% - 64px)); border-radius: 16px; border: 1px solid rgba(148,163,184,.36); background: rgba(255,255,255,.86); backdrop-filter: blur(24px) saturate(175%); box-shadow: 0 24px 54px rgba(15,23,42,.26); padding: 12px 12px 16px 12px; display: grid; gap: 10px; grid-template-rows: auto auto auto minmax(0,1fr) auto; transform-origin: right bottom; transform: translateY(14px) scale(.9); opacity: 0; pointer-events: none; transition: transform .34s cubic-bezier(.2,.8,.2,1), opacity .24s ease; position: relative; min-height: 320px; }
            #anchorMountPanel .anchor-phase2b-dock.is-open .anchor-phase2b-canvas { transform: translateY(0) scale(1); opacity: 1; pointer-events: auto; }
            #anchorMountPanel .anchor-phase2b-dock.is-moving .anchor-phase2b-canvas { transition: none; }
            #anchorMountPanel .anchor-phase2b-dock.is-open .anchor-phase2b-capsule { opacity: 0; transform: translateY(8px) scale(.88); pointer-events: none; }
            #anchorMountPanel .anchor-phase2b-canvas-head { display: flex; align-items: center; justify-content: space-between; gap: 8px; cursor: move; user-select: none; touch-action: none; }
            #anchorMountPanel .anchor-phase2b-canvas-title { font-size: 13px; font-weight: 700; letter-spacing: .01em; color: #0f172a; }
            #anchorMountPanel .anchor-phase2b-canvas-actions { display: inline-flex; align-items: center; gap: 6px; }
            #anchorMountPanel .anchor-phase2b-canvas-actions .btn { min-width: 30px; min-height: 28px; border-radius: 8px; padding: 0 9px; font-size: 12px; cursor: pointer; touch-action: auto; }
            #anchorMountPanel .anchor-phase2b-chips { display: flex; align-items: center; flex-wrap: wrap; gap: 6px; }
            #anchorMountPanel .anchor-phase2b-chip { border: 1px solid rgba(99,102,241,.34); background: rgba(224,231,255,.62); color: #3730a3; border-radius: 999px; padding: 2px 9px; font-size: 11px; line-height: 1.55; max-width: 100%; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
            #anchorMountPanel .anchor-phase2b-chip.is-link { display: inline-flex; align-items: center; gap: 6px; border-color: rgba(148,163,184,.4); background: rgba(241,245,249,.9); color: #0f172a; max-width: min(100%, 360px); }
            #anchorMountPanel .anchor-phase2b-chip-site { width: 16px; height: 16px; border-radius: 999px; background: rgba(148,163,184,.22); color: #334155; font-size: 10px; font-weight: 700; display: inline-flex; align-items: center; justify-content: center; flex: 0 0 auto; }
            #anchorMountPanel .anchor-phase2b-input-wrap { display: grid; gap: 8px; }
            #anchorMountPanel .anchor-phase2b-input-wrap.is-collapsed { display: none; }
            #anchorMountPanel .anchor-phase2b-input-shell { position: relative; border: 1px solid rgba(148,163,184,.4); border-radius: 12px; background: rgba(255,255,255,.92); transition: border-color .2s ease, box-shadow .2s ease; }
            #anchorMountPanel .anchor-phase2b-dock.is-dragging .anchor-phase2b-input-shell { border-color: rgba(37,99,235,.72); box-shadow: 0 0 0 2px rgba(37,99,235,.14), 0 14px 30px rgba(59,130,246,.2); }
            #anchorMountPanel .anchor-phase2b-input { width: 100%; resize: none; border: 0; outline: none; background: transparent; color: #0f172a; padding: 12px 46px 12px 12px; font-size: 13px; line-height: 1.55; min-height: 86px; max-height: 42vh; overflow-y: auto; }
            #anchorMountPanel .anchor-phase2b-input::placeholder { color: #94a3b8; }
            #anchorMountPanel .anchor-phase2b-input::-webkit-scrollbar { width: 6px; }
            #anchorMountPanel .anchor-phase2b-input::-webkit-scrollbar-thumb { border-radius: 999px; background: rgba(148,163,184,.48); }
            #anchorMountPanel .anchor-phase2b-submit { position: absolute; right: 8px; bottom: 8px; min-width: 30px; min-height: 30px; border-radius: 999px; border: 1px solid rgba(59,130,246,.44); background: rgba(37,99,235,.95); color: #fff; font-size: 13px; line-height: 1; cursor: pointer; transition: transform .18s ease, opacity .2s ease; }
            #anchorMountPanel .anchor-phase2b-submit:disabled { opacity: .5; cursor: not-allowed; }
            #anchorMountPanel .anchor-phase2b-processing { border: 1px solid rgba(148,163,184,.34); border-radius: 12px; min-height: 120px; padding: 14px; position: relative; overflow: hidden; background: rgba(248,250,252,.92); display: grid; align-content: center; gap: 8px; }
            #anchorMountPanel .anchor-phase2b-processing::before { content: none; }
            #anchorMountPanel .anchor-phase2b-processing-text { position: relative; font-size: 12px; color: #334155; font-weight: 600; line-height: 1.6; }
            #anchorMountPanel .anchor-phase2b-result { display: grid; gap: 8px; min-height: 0; align-content: start; }
            #anchorMountPanel .anchor-phase2b-result-head { display: flex; align-items: center; justify-content: flex-end; }
            #anchorMountPanel .anchor-phase2b-copy-btn { min-height: 30px; padding: 0 12px; border-radius: 8px; border: 1px solid rgba(59,130,246,.42); background: #2563eb; color: #fff; font-size: 12px; font-weight: 600; cursor: pointer; transition: transform .18s ease, background-color .2s ease; }
            #anchorMountPanel .anchor-phase2b-copy-btn.is-copied { background: #059669; border-color: rgba(5,150,105,.45); }
            #anchorMountPanel .anchor-phase2b-copy-btn:disabled { opacity: .5; cursor: not-allowed; }
            #anchorMountPanel .anchor-phase2b-preview { max-height: min(40vh, 360px); overflow: auto; border-radius: 10px; border: 1px solid rgba(148,163,184,.33); background: rgba(248,250,252,.9); padding: 12px; font-size: 12px; line-height: 1.58; color: #0f172a; white-space: pre-wrap; word-break: break-word; }
            #anchorMountPanel .anchor-phase2b-preview :is(p,ul,ol,blockquote,pre,table,h1,h2,h3,h4,h5,h6) { margin: 0 0 .72em; }
            #anchorMountPanel .anchor-phase2b-preview p { margin-block: .55em; }
            #anchorMountPanel .anchor-phase2b-preview :is(p,li,blockquote,td,th) { white-space: pre-wrap; }
            #anchorMountPanel .anchor-phase2b-preview code { background: rgba(15,23,42,.08); padding: .06em .34em; border-radius: 4px; }
            #anchorMountPanel .anchor-phase2b-preview pre { background: rgba(15,23,42,.94); color: #e2e8f0; padding: 10px 12px; border-radius: 8px; overflow: auto; white-space: pre; }
            #anchorMountPanel .anchor-phase2b-preview.is-streaming > * { animation: anchorPhase2bChunkIn .24s ease; }
            #anchorMountPanel .anchor-phase2b-feedback { font-size: 11px; line-height: 1.45; color: #475569; min-height: 16px; }
            #anchorMountPanel .anchor-phase2b-feedback.is-error { color: #b91c1c; }
            #anchorMountPanel .anchor-phase2b-resizer { position: absolute; width: 14px; height: 14px; right: 2px; bottom: 2px; cursor: nwse-resize; border-right: 2px solid rgba(71,85,105,.55); border-bottom: 2px solid rgba(71,85,105,.55); border-radius: 0 0 8px 0; touch-action: none; }
            #anchorMountPanel .anchor-phase2b-dock.is-resizing .anchor-phase2b-canvas { transition: none; }
            .viewer-layout.is-center-right-stacked #anchorMountPanel .anchor-phase2b-dock { right: 10px; bottom: 10px; }
            .viewer-layout.is-center-right-stacked #anchorMountPanel .anchor-phase2b-canvas { width: min(420px, calc(100% - 4px)); max-height: min(62vh, calc(100% - 52px)); }
            @media (max-width: 960px) { #anchorMountPanel .anchor-phase2b-dock { right: 10px; bottom: 10px; } #anchorMountPanel .anchor-phase2b-canvas { width: min(420px, calc(100vw - 28px), calc(100% - 4px)); } }
            @keyframes anchorPhase2bPulse { 0% { box-shadow: 0 0 0 0 rgba(34,197,94,.5); } 70% { box-shadow: 0 0 0 10px rgba(34,197,94,0); } 100% { box-shadow: 0 0 0 0 rgba(34,197,94,0); } }
            @keyframes anchorPhase2bNotify { 0% { transform: translateY(0) scale(1); } 35% { transform: translateY(-2px) scale(1.05); } 100% { transform: translateY(0) scale(1); } }
            @keyframes anchorPhase2bChunkIn { 0% { opacity: .16; transform: translateY(4px); } 100% { opacity: 1; transform: translateY(0); } }
        `;
        document.head.appendChild(style);
    }

    function ensurePhase2bFloatingUi() {
        ensurePhase2bFloatingStyle();
        ensurePhase2bLayoutStateLoaded();
        const panel = document.getElementById('anchorMountPanel');
        if (!panel) {
            return;
        }
        let dock = document.getElementById('anchorPhase2bDock');
        if (!dock) {
            dock = document.createElement('div');
            dock.id = 'anchorPhase2bDock';
            dock.className = 'anchor-phase2b-dock';
            dock.innerHTML = `
                <button class="anchor-phase2b-capsule" id="anchorPhase2bCapsuleBtn" type="button" data-phase2b-action="open" aria-label="打开 Phase2B 结构化输入面板">
                    <span class="anchor-phase2b-capsule-icon" aria-hidden="true">✦</span>
                    <span class="anchor-phase2b-capsule-indicator" id="anchorPhase2bCapsuleIndicator" aria-hidden="true"></span>
                </button>
                <div class="anchor-phase2b-toast" id="anchorPhase2bNotice" hidden></div>
                <section class="anchor-phase2b-canvas" id="anchorPhase2bCanvas" hidden aria-hidden="true">
                    <div class="anchor-phase2b-canvas-head">
                        <div class="anchor-phase2b-canvas-title">Phase2B 提示词结构化</div>
                        <div class="anchor-phase2b-canvas-actions">
                            <button class="btn btn-ghost-icon" id="anchorPhase2bHeadSubmitBtn" type="button" data-phase2b-action="submit" title="提交处理">↗</button>
                            <button class="btn btn-ghost-icon" id="anchorPhase2bHeadCopyBtn" type="button" data-phase2b-action="copy" title="一键复制">⧉</button>
                            <button class="btn btn-ghost-icon" id="anchorPhase2bToggleInputBtn" type="button" data-phase2b-action="toggle-input" title="收起输入">⌨</button>
                            <button class="btn btn-ghost-icon" type="button" data-phase2b-action="collapse" title="收起">×</button>
                        </div>
                    </div>
                    <div class="anchor-phase2b-chips" id="anchorPhase2bFileChips" hidden></div>
                    <div class="anchor-phase2b-input-wrap" id="anchorPhase2bInputWrap">
                        <div class="anchor-phase2b-input-shell" id="anchorPhase2bActionLayer">
                            <textarea id="anchorPhase2bInput" class="anchor-phase2b-input" placeholder="粘贴文本、知乎/掘金链接，或将 .md / .txt 拖拽至此..." spellcheck="false"></textarea>
                            <button id="anchorPhase2bSubmitBtn" class="anchor-phase2b-submit" type="button" data-phase2b-action="submit" aria-label="提交处理">↑</button>
                        </div>
                    </div>
                    <div class="anchor-phase2b-processing" id="anchorPhase2bProcessing" hidden></div>
                    <div class="anchor-phase2b-result" id="anchorPhase2bResult" hidden>
                        <div class="anchor-phase2b-result-head">
                            <button id="anchorPhase2bCopyBtn" class="anchor-phase2b-copy-btn" type="button" data-phase2b-action="copy">一键复制</button>
                        </div>
                        <div class="anchor-phase2b-preview" id="anchorPhase2bResultPreview"></div>
                    </div>
                    <div class="anchor-phase2b-feedback" id="anchorPhase2bFeedback" hidden></div>
                    <div class="anchor-phase2b-resizer" id="anchorPhase2bResizer" aria-hidden="true"></div>
                </section>
            `;
            panel.appendChild(dock);
        }
        renderPhase2bFloatingUi();
    }

    function renderPhase2bFloatingUi() {
        const phase2b = runtime.phase2b;
        const refs = getPhase2bElements();
        if (!refs.dock) {
            return;
        }
        const hasResult = !!t(phase2b.resultValue || phase2b.inputValue);
        const requestInFlight = isPhase2bRequestInFlight();
        const hasStreamPreview = requestInFlight && Number(phase2b.streamChunkCount || 0) > 0;
        const mode = requestInFlight ? 'processing' : 'input';
        const inputCollapsed = mode === 'input' && !!phase2b.inputCollapsed;
        const hasArticleLinks = Array.isArray(phase2b.linkItems) && phase2b.linkItems.length > 0;
        const submitDisabled = requestInFlight || (!t(phase2b.inputValue) && !hasArticleLinks);
        const copyText = String(phase2b.inputValue || phase2b.resultValue || '');
        const copyDisabled = !copyText.trim();
        refs.dock.classList.toggle('is-open', !!phase2b.expanded);
        refs.dock.classList.toggle('is-processing', requestInFlight);
        refs.dock.classList.toggle('is-ready', !requestInFlight && hasResult);
        refs.dock.classList.toggle('is-notice', !!t(phase2b.noticeText));
        refs.dock.classList.toggle('is-dragging', !!phase2b.dragging);
        refs.dock.classList.toggle('is-moving', !!phase2b.moving);
        refs.dock.classList.toggle('is-resizing', !!phase2b.resizing);
        if (refs.canvas) {
            refs.canvas.hidden = !phase2b.expanded;
            refs.canvas.setAttribute('aria-hidden', phase2b.expanded ? 'false' : 'true');
            applyPhase2bCanvasSize(refs.canvas);
        }
        if (refs.capsuleBtn) {
            const capsuleText = requestInFlight
                ? 'Phase2B 处理中'
                : (hasResult && !phase2b.expanded ? 'Phase2B 结果就绪' : '打开 Phase2B 面板');
            refs.capsuleBtn.setAttribute('title', capsuleText);
            refs.capsuleBtn.setAttribute('aria-label', capsuleText);
        }
        if (refs.inputWrap) {
            refs.inputWrap.hidden = mode !== 'input' || inputCollapsed;
            refs.inputWrap.classList.toggle('is-collapsed', inputCollapsed);
        }
        if (refs.processingWrap) {
            refs.processingWrap.hidden = mode !== 'processing';
            if (mode === 'processing') {
                refs.processingWrap.innerHTML = `
                    <div class="anchor-phase2b-processing-text" id="anchorPhase2bProcessingText"></div>
                `;
            } else {
                refs.processingWrap.innerHTML = '';
            }
        }
        if (refs.resultWrap) {
            refs.resultWrap.hidden = mode === 'processing' && !hasStreamPreview;
        }
        const processingTextNode = document.getElementById('anchorPhase2bProcessingText');
        if (processingTextNode) {
            processingTextNode.textContent = requestInFlight
                ? (t(phase2b.progressText) || PHASE2B_PROGRESS_FALLBACK_TEXT)
                : '';
        }
        if (refs.fileChips) {
            const files = Array.isArray(phase2b.attachedFiles) ? phase2b.attachedFiles : [];
            const links = Array.isArray(phase2b.linkItems) ? phase2b.linkItems : [];
            refs.fileChips.hidden = files.length === 0 && links.length === 0;
            const fileChips = files.map((item) => {
                const name = t(item && item.name) || 'untitled.md';
                const chars = Number.isFinite(Number(item && item.chars)) ? Number(item.chars) : 0;
                return `<span class="anchor-phase2b-chip" title="${h(name)}">${h(name)}${chars > 0 ? ` · ${chars} chars` : ''}</span>`;
            });
            const linkChips = links.map((item) => {
                const url = normalizePhase2bArticleLink(item && item.url);
                const title = buildPhase2bLinkChipLabel(item);
                const site = inferPhase2bLinkSite(url);
                const badge = site === 'zhihu' ? '知' : (site === 'juejin' ? '掘' : '链');
                return `<span class="anchor-phase2b-chip is-link" title="${h(url || title)}"><span class="anchor-phase2b-chip-site">${h(badge)}</span>${h(title)}</span>`;
            });
            refs.fileChips.innerHTML = fileChips.concat(linkChips).join('');
        }
        if (refs.input && document.activeElement !== refs.input && String(refs.input.value || '') !== String(phase2b.inputValue || '')) {
            refs.input.value = String(phase2b.inputValue || '');
        }
        if (refs.input) {
            refs.input.disabled = requestInFlight;
        }
        if (refs.submitBtn) {
            refs.submitBtn.disabled = submitDisabled;
        }
        if (refs.headSubmitBtn) {
            refs.headSubmitBtn.hidden = mode !== 'input';
            refs.headSubmitBtn.disabled = submitDisabled;
        }
        if (refs.headCopyBtn) {
            refs.headCopyBtn.hidden = false;
            refs.headCopyBtn.disabled = copyDisabled;
        }
        if (refs.toggleInputBtn) {
            refs.toggleInputBtn.hidden = mode !== 'input';
            refs.toggleInputBtn.textContent = inputCollapsed ? '⌨' : '▾';
            refs.toggleInputBtn.setAttribute('title', inputCollapsed ? '展开输入' : '收起输入');
            refs.toggleInputBtn.setAttribute('aria-label', inputCollapsed ? '展开输入' : '收起输入');
        }
        if (refs.resultPreview) {
            const previewSource = String(phase2b.inputValue || '');
            refs.resultPreview.innerHTML = previewSource
                ? renderPhase2bPreviewMarkdown(previewSource)
                : '<div class="anchor-obsidian-empty">暂无可预览的 Markdown 内容</div>';
            refs.resultPreview.classList.toggle('is-streaming', hasStreamPreview);
        }
        if (refs.copyBtn) {
            refs.copyBtn.classList.toggle('is-copied', !!phase2b.copied);
            refs.copyBtn.textContent = phase2b.copied ? '已复制' : '一键复制';
            refs.copyBtn.disabled = copyDisabled;
        }
        const feedbackText = phase2b.error ? `处理失败：${phase2b.error}` : t(phase2b.feedback);
        if (refs.feedback) {
            refs.feedback.hidden = !feedbackText;
            refs.feedback.classList.toggle('is-error', !!phase2b.error);
            refs.feedback.textContent = feedbackText;
        }
        const noticeNode = document.getElementById('anchorPhase2bNotice');
        if (noticeNode) {
            const noticeText = t(phase2b.noticeText);
            noticeNode.hidden = !noticeText || !!phase2b.expanded;
            noticeNode.textContent = noticeText;
        }
        applyPhase2bDockPosition(refs.dock);
        updatePhase2bInputHeight();
    }

    function setPhase2bExpanded(expanded, options = {}) {
        const phase2b = runtime.phase2b;
        const requestInFlight = isPhase2bRequestInFlight();
        phase2b.expanded = !!expanded;
        if (phase2b.expanded) {
            clearPhase2bCompletionNotice();
            phase2b.mode = requestInFlight ? 'processing' : 'input';
        } else if (!requestInFlight) {
            phase2b.mode = 'idle';
            phase2b.dragging = false;
            phase2b.moving = false;
            phase2b.movePointerId = -1;
            phase2b.moveStartClientX = 0;
            phase2b.moveStartClientY = 0;
            phase2b.moveDidDrag = false;
            phase2b.moveCaptureTarget = null;
            phase2b.suppressNextOpen = false;
            phase2b.resizing = false;
            phase2b.resizePointerId = -1;
        }
        renderPhase2bFloatingUi();
        if (phase2b.expanded && options.focusInput !== false && phase2b.mode === 'input' && !phase2b.inputCollapsed) {
            const input = document.getElementById('anchorPhase2bInput');
            if (input && typeof input.focus === 'function') {
                input.focus();
                const len = String(input.value || '').length;
                if (typeof input.setSelectionRange === 'function') {
                    input.setSelectionRange(len, len);
                }
            }
        }
    }

    function hidePhase2bFloatingUi() {
        setPhase2bExpanded(false, { focusInput: false });
    }

    async function callPhase2bStructuredMarkdown(rawText, requestId) {
        const endpoint = resolvePhase2bEndpoint();
        const payload = {
            bodyText: String(rawText || ''),
            sourceText: String(rawText || ''),
            taskId: t(runtime.ctx && runtime.ctx.taskId),
            anchorId: t(runtime.activeId),
            pathHint: normalizePath(runtime.ctx && runtime.ctx.pathHint),
            source: 'web_anchor_phase2b_capsule',
            progressChannel: getPhase2bProgressChannel(),
            requestId: normalizePhase2bRequestId(requestId),
            linkUrls: (Array.isArray(runtime.phase2b.linkItems) ? runtime.phase2b.linkItems : [])
                .map((item) => normalizePhase2bArticleLink(item && item.url))
                .filter((item, idx, arr) => !!item && arr.indexOf(item) === idx),
        };
        const resp = await fetch(endpoint, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(payload),
        });
        const raw = await resp.text();
        let parsed = null;
        if (raw) {
            try {
                parsed = JSON.parse(raw);
            } catch (_e) {
                parsed = null;
            }
        }
        if (!resp.ok) {
            throw new Error(t(parsed && parsed.message) || t(raw) || `HTTP ${resp.status}`);
        }
        if (parsed && parsed.success === false) {
            throw new Error(t(parsed.message) || 'phase2b failed');
        }
        const resolvedLinks = Array.isArray(parsed && parsed.links) ? parsed.links : [];
        const candidates = [
            parsed && parsed.markdown,
            parsed && parsed.structuredMarkdown,
            parsed && parsed.result,
            parsed && parsed.output,
            parsed && parsed.content,
            parsed && parsed.text,
            parsed && parsed.data && parsed.data.markdown,
            parsed && parsed.data && parsed.data.result,
            parsed && parsed.payload && parsed.payload.markdown,
        ];
        for (let i = 0; i < candidates.length; i += 1) {
            const item = candidates[i];
            if (typeof item === 'string' && t(item)) {
                return {
                    markdown: String(item),
                    links: resolvedLinks,
                };
            }
        }
        if (raw && !parsed) {
            return {
                markdown: String(raw),
                links: [],
            };
        }
        throw new Error('接口返回为空，请检查后端提示词调用结果。');
    }

    async function submitPhase2bContent() {
        const phase2b = runtime.phase2b;
        if (isPhase2bRequestInFlight()) {
            return;
        }
        const rawInputText = String(phase2b.inputValue || '');
        const inlineLinks = extractPhase2bArticleLinks(rawInputText);
        if (inlineLinks.length) {
            upsertPhase2bLinkItems(inlineLinks, { status: 'queued' });
            prefetchPhase2bLinkTitles(inlineLinks);
            phase2b.inputValue = stripPhase2bArticleLinks(rawInputText);
            phase2b.inputCollapsed = true;
        }
        const payloadText = String(phase2b.inputValue || '').trim();
        const hasArticleLinks = (Array.isArray(phase2b.linkItems) && phase2b.linkItems.length > 0) || inlineLinks.length > 0;
        if (!payloadText && !hasArticleLinks) {
            phase2b.error = '请先输入文本，拖入 .md/.txt，或粘贴知乎/掘金文章链接';
            phase2b.feedback = '';
            renderPhase2bFloatingUi();
            return;
        }
        const requestSeq = Number(phase2b.requestSeq || 0) + 1;
        const requestId = normalizePhase2bRequestId(`phase2b_${requestSeq}_${Date.now().toString(36)}`);
        clearPhase2bCompletionNotice();
        phase2b.requestSeq = requestSeq;
        phase2b.currentRequestId = requestId;
        phase2b.processing = true;
        phase2b.inFlightRequestSeq = requestSeq;
        phase2b.mode = 'processing';
        phase2b.streamActive = false;
        phase2b.streamChunkCount = 0;
        phase2b.error = '';
        phase2b.feedback = '';
        phase2b.copied = false;
        phase2b.progressText = '请求已发出，等待后端接收...';
        try {
            await ensurePhase2bWebSocketConnected();
        } catch (_e) {
            // allow request to proceed even when websocket is unavailable
        }
        renderPhase2bFloatingUi();
        try {
            const phase2bResult = await callPhase2bStructuredMarkdown(payloadText, requestId);
            if (requestSeq !== runtime.phase2b.requestSeq) {
                return;
            }
            const markdown = String(phase2bResult && phase2bResult.markdown || '');
            phase2b.resultValue = markdown;
            phase2b.inputValue = markdown;
            phase2b.mode = 'input';
            phase2b.processing = false;
            phase2b.inFlightRequestSeq = 0;
            phase2b.streamActive = false;
            phase2b.streamChunkCount = 0;
            phase2b.progressText = '';
            phase2b.feedback = '结构化完成，可继续编辑并实时预览。';
            phase2b.error = '';
            applyPhase2bResolvedLinkTitles(phase2bResult && phase2bResult.links);
            if (!phase2b.expanded && !t(phase2b.noticeText)) {
                showPhase2bCompletionNotice(derivePhase2bCompletionNoticeText());
            }
        } catch (error) {
            if (requestSeq !== runtime.phase2b.requestSeq) {
                return;
            }
            phase2b.processing = false;
            phase2b.inFlightRequestSeq = 0;
            phase2b.mode = 'input';
            phase2b.streamActive = false;
            phase2b.streamChunkCount = 0;
            phase2b.progressText = '';
            phase2b.feedback = '';
            phase2b.error = t(error && error.message) || '请求失败';
        } finally {
            if (requestSeq === runtime.phase2b.requestSeq) {
                if (runtime.phase2b.inFlightRequestSeq === requestSeq) {
                    runtime.phase2b.inFlightRequestSeq = 0;
                }
                renderPhase2bFloatingUi();
            }
        }
    }

    async function attachPhase2bFiles(filesLike) {
        const fileList = Array.from(filesLike || []);
        const supported = fileList.filter((file) => {
            const name = t(file && file.name).toLowerCase();
            return name.endsWith('.md') || name.endsWith('.markdown') || name.endsWith('.txt');
        });
        if (!supported.length) {
            runtime.phase2b.feedback = '仅支持 .md / .markdown / .txt 文件';
            runtime.phase2b.error = '';
            renderPhase2bFloatingUi();
            return;
        }
        const chunks = [];
        const files = [];
        for (let i = 0; i < supported.length; i += 1) {
            const file = supported[i];
            const text = String(await file.text());
            if (!t(text)) {
                continue;
            }
            chunks.push(`\n\n<!-- ${file.name} -->\n${text.trim()}`);
            files.push({ name: file.name, chars: text.length });
        }
        if (!chunks.length) {
            runtime.phase2b.feedback = '文件内容为空，未导入。';
            runtime.phase2b.error = '';
            renderPhase2bFloatingUi();
            return;
        }
        const prev = String(runtime.phase2b.inputValue || '').trim();
        const merged = `${prev}${chunks.join('')}`.trim();
        runtime.phase2b.inputValue = merged;
        runtime.phase2b.attachedFiles = files;
        runtime.phase2b.feedback = `已导入 ${files.length} 个文件`;
        runtime.phase2b.error = '';
        runtime.phase2b.mode = 'input';
        renderPhase2bFloatingUi();
        setPhase2bExpanded(true);
    }

    async function copyPhase2bResultToClipboard() {
        const text = String(runtime.phase2b.inputValue || runtime.phase2b.resultValue || '');
        if (!text.trim()) {
            return;
        }
        try {
            await navigator.clipboard.writeText(text);
            runtime.phase2b.copied = true;
            runtime.phase2b.error = '';
            runtime.phase2b.feedback = '复制成功';
            renderPhase2bFloatingUi();
            setTimeout(() => {
                runtime.phase2b.copied = false;
                renderPhase2bFloatingUi();
            }, 1400);
        } catch (error) {
            runtime.phase2b.error = `复制失败：${t(error && error.message) || 'clipboard denied'}`;
            runtime.phase2b.feedback = '';
            renderPhase2bFloatingUi();
        }
    }

    function resetPhase2bForContextChange() {
        clearPhase2bProgressState();
        clearPhase2bCompletionNotice();
        runtime.phase2b.expanded = false;
        runtime.phase2b.mode = 'idle';
        runtime.phase2b.dragging = false;
        runtime.phase2b.moving = false;
        runtime.phase2b.movePointerId = -1;
        runtime.phase2b.moveOffsetX = 0;
        runtime.phase2b.moveOffsetY = 0;
        runtime.phase2b.moveStartClientX = 0;
        runtime.phase2b.moveStartClientY = 0;
        runtime.phase2b.moveDidDrag = false;
        runtime.phase2b.moveCaptureTarget = null;
        runtime.phase2b.suppressNextOpen = false;
        runtime.phase2b.inputCollapsed = false;
        runtime.phase2b.inputValue = '';
        runtime.phase2b.resultValue = '';
        runtime.phase2b.feedback = '';
        runtime.phase2b.error = '';
        runtime.phase2b.processing = false;
        runtime.phase2b.inFlightRequestSeq = 0;
        runtime.phase2b.currentRequestId = '';
        runtime.phase2b.requestSeq = Number(runtime.phase2b.requestSeq || 0) + 1;
        runtime.phase2b.copied = false;
        runtime.phase2b.attachedFiles = [];
        runtime.phase2b.linkItems = [];
        runtime.phase2b.linkPrefetchInFlight = new Set();
        runtime.phase2b.streamActive = false;
        runtime.phase2b.streamChunkCount = 0;
        renderPhase2bFloatingUi();
    }

    function decodePath(value) {
        const raw = String(value || '').trim();
        if (!raw) return '';
        try {
            return decodeURIComponent(raw);
        } catch (_e) {
            return raw;
        }
    }

    function isMarkdown(pathLike) {
        const p = normalizePath(pathLike).toLowerCase();
        return p.endsWith('.md') || p.endsWith('.markdown');
    }

    function localNoteStoreKey(anchorId) {
        const taskId = encodeURIComponent(t(runtime.ctx && runtime.ctx.taskId));
        const pathHint = encodeURIComponent(normalizePath(runtime.ctx && runtime.ctx.pathHint));
        const key = encodeURIComponent(t(anchorId));
        return `${LOCAL_NOTE_STORE_PREFIX}:${taskId}:${pathHint}:${key}`;
    }

    function localLayoutStoreKey(anchorId) {
        const taskId = encodeURIComponent(t(runtime.ctx && runtime.ctx.taskId));
        const pathHint = encodeURIComponent(normalizePath(runtime.ctx && runtime.ctx.pathHint));
        const key = encodeURIComponent(t(anchorId));
        return `${LOCAL_NOTE_LAYOUT_PREFIX}:${taskId}:${pathHint}:${key}`;
    }

    function normalizeEditorLayoutMode(modeLike) {
        const mode = t(modeLike).toLowerCase();
        if (mode === 'edit') {
            return 'edit';
        }
        return 'edit';
    }

    function normalizeLocalNoteTitle(content, fallback) {
        const raw = String(content || '');
        const lines = raw.split(/\r?\n/).map((line) => t(line)).filter(Boolean);
        const heading = lines.find((line) => line.startsWith('#'));
        if (heading) {
            return short(heading.replace(/^#+\s*/, ''), 24) || fallback;
        }
        if (lines.length > 0) {
            return short(lines[0], 24) || fallback;
        }
        return fallback;
    }

    function normalizeLocalNoteItem(raw, fallbackIndex) {
        const source = raw && typeof raw === 'object' ? raw : {};
        const content = String(source.content || '');
        const fallbackTitle = `Note ${Math.max(1, Number(fallbackIndex) + 1)}`;
        const fileName = normalizePath(source.fileName || '');
        const title = t(source.title) || normalizeLocalNoteTitle(content, fileName.replace(/\.markdown?$/i, '') || fallbackTitle);
        const id = t(source.id) || `local_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
        const updatedAt = Number.isFinite(Number(source.updatedAt)) ? Number(source.updatedAt) : Date.now();
        return {
            id,
            title: title || fallbackTitle,
            content,
            fileName: fileName || '',
            updatedAt,
        };
    }

    function readLocalNotesFromStorage(anchorId) {
        const storageKey = localNoteStoreKey(anchorId);
        try {
            const raw = localStorage.getItem(storageKey);
            if (!raw) {
                return [];
            }
            const parsed = JSON.parse(raw);
            if (!Array.isArray(parsed)) {
                return [];
            }
            return parsed
                .map((item, index) => normalizeLocalNoteItem(item, index))
                .filter((item) => !!item.id);
        } catch (_e) {
            return [];
        }
    }

    function writeLocalNotesToStorage(anchorId, notes) {
        const storageKey = localNoteStoreKey(anchorId);
        try {
            localStorage.setItem(storageKey, JSON.stringify(Array.isArray(notes) ? notes : []));
        } catch (_e) {
            // ignore local storage quota issue in degraded mode
        }
    }

    function ensureLocalNotes(anchorId) {
        const key = t(anchorId);
        if (!key) {
            return [];
        }
        if (!runtime.localNotesByAnchor.has(key)) {
            const restored = readLocalNotesFromStorage(key);
            const normalized = restored.length
                ? restored
                : [normalizeLocalNoteItem({
                    id: `local_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
                    title: 'Note 1',
                    content: '',
                    fileName: '',
                }, 0)];
            runtime.localNotesByAnchor.set(key, normalized);
            writeLocalNotesToStorage(key, normalized);
        }
        return runtime.localNotesByAnchor.get(key) || [];
    }

    function getActiveLocalNoteId(anchorId) {
        const key = t(anchorId);
        if (!key) {
            return '';
        }
        const notes = ensureLocalNotes(key);
        const activeId = t(runtime.activeLocalNoteIdByAnchor.get(key));
        if (activeId && notes.some((item) => item.id === activeId)) {
            return activeId;
        }
        const fallback = notes[0] ? notes[0].id : '';
        if (fallback) {
            runtime.activeLocalNoteIdByAnchor.set(key, fallback);
        }
        return fallback;
    }

    function setActiveLocalNoteId(anchorId, noteId) {
        const key = t(anchorId);
        const normalizedNoteId = t(noteId);
        if (!key || !normalizedNoteId) {
            return;
        }
        runtime.activeLocalNoteIdByAnchor.set(key, normalizedNoteId);
    }

    function markAnchorLocalSyncDirty(anchorId) {
        const key = t(anchorId);
        if (!key) {
            return;
        }
        runtime.localSyncDirtyByAnchor.add(key);
    }

    function clearAnchorLocalSyncDirty(anchorId) {
        const key = t(anchorId);
        if (!key) {
            return;
        }
        runtime.localSyncDirtyByAnchor.delete(key);
    }

    function getActiveLocalNote(anchorId) {
        const key = t(anchorId);
        if (!key) {
            return null;
        }
        const notes = ensureLocalNotes(key);
        const noteId = getActiveLocalNoteId(key);
        return notes.find((item) => item.id === noteId) || null;
    }

    function safeMarkdownFileName(rawName, fallback) {
        const cleaned = String(rawName || '')
            .replace(/[<>:"/\\|?*\u0000-\u001f]/g, ' ')
            .replace(/\s+/g, '_')
            .replace(/^_+|_+$/g, '');
        const base = cleaned || fallback || `anchor_note_${Date.now()}`;
        return /\.markdown?$/i.test(base) ? base : `${base}.md`;
    }

    function stripMarkdownExt(pathLike) {
        return normalizePath(pathLike).replace(/\.markdown?$/i, '');
    }

    function noteNameFromPath(pathLike) {
        const normalized = normalizePath(pathLike);
        if (!normalized) {
            return '';
        }
        return normalized.replace(/^.*\//, '').replace(/\.markdown?$/i, '');
    }

    function normalizeTagToken(rawTag) {
        const tag = String(rawTag || '').trim().replace(/^#+/, '').trim();
        return tag ? `#${tag}` : '';
    }

    function normalizeHeadingToken(rawHeading) {
        return String(rawHeading || '')
            .trim()
            .toLowerCase()
            .replace(/[`*_~]/g, '')
            .replace(/[^\p{L}\p{N}\s_-]/gu, '')
            .replace(/\s+/g, '-')
            .replace(/-+/g, '-')
            .replace(/^-+|-+$/g, '');
    }

    function parseWikilinkToken(rawToken) {
        const raw = String(rawToken || '').trim();
        if (!raw) {
            return null;
        }
        const aliasSplit = raw.split('|');
        const targetRaw = t(aliasSplit[0]);
        const alias = t(aliasSplit.slice(1).join('|'));
        const hashIndex = targetRaw.indexOf('#');
        const pathPart = hashIndex >= 0 ? t(targetRaw.slice(0, hashIndex)) : targetRaw;
        const anchorPart = hashIndex >= 0 ? t(targetRaw.slice(hashIndex + 1)) : '';
        return {
            raw,
            pathPart,
            alias,
            anchorPart,
            label: alias || pathPart || anchorPart || raw,
        };
    }

    function extractWikilinkTokens(markdownText) {
        const source = String(markdownText || '');
        const tokens = [];
        let match = null;
        WIKILINK_CAPTURE_REGEX.lastIndex = 0;
        while ((match = WIKILINK_CAPTURE_REGEX.exec(source)) !== null) {
            const parsed = parseWikilinkToken(match[1]);
            if (!parsed) {
                continue;
            }
            tokens.push({
                ...parsed,
                index: match.index,
                isEmbed: match.index > 0 && source.charAt(match.index - 1) === '!',
            });
        }
        return tokens;
    }

    function extractTagTokens(markdownText) {
        const source = String(markdownText || '');
        const result = new Set();
        source.replace(/(^|[\s(])#([\p{L}\p{N}_/-]{1,64})/gu, (_m, _prefix, body) => {
            const normalized = normalizeTagToken(body);
            if (normalized) {
                result.add(normalized);
            }
            return _m;
        });
        return Array.from(result);
    }

    function normalizeReferencePathCandidate(pathPart) {
        const value = normalizePath(pathPart || '');
        if (!value) {
            return '';
        }
        return isMarkdown(value) ? value : `${value}.md`;
    }

    function invalidateObsidianModel(anchorId) {
        const key = t(anchorId);
        if (!key) {
            return;
        }
        runtime.obsidianGraphCacheByAnchor.delete(key);
    }

    function computeObsidianModelVersion(anchorId) {
        const key = t(anchorId);
        if (!key) {
            return '';
        }
        const notes = ensureLocalNoteFileNames(key);
        return notes
            .map((note) => `${t(note.id)}|${normalizePath(note.fileName)}|${Number(note.updatedAt) || 0}|${String(note.content || '').length}`)
            .join('||');
    }

    function resolveWikilinkTargetForModel(model, sourceNote, wikilink) {
        if (!model || !wikilink) {
            return null;
        }
        const pathCandidate = normalizeReferencePathCandidate(wikilink.pathPart);
        if (!pathCandidate && wikilink.anchorPart && sourceNote) {
            return {
                note: sourceNote,
                relativePath: normalizePath(sourceNote.fileName),
                anchorPart: wikilink.anchorPart,
                label: wikilink.label,
            };
        }
        if (pathCandidate) {
            const direct = model.byPathLower.get(pathCandidate.toLowerCase());
            if (direct) {
                return {
                    note: direct,
                    relativePath: normalizePath(direct.fileName),
                    anchorPart: wikilink.anchorPart,
                    label: wikilink.label || noteNameFromPath(direct.fileName),
                };
            }
        }
        const lookupName = pathCandidate
            ? noteNameFromPath(pathCandidate).toLowerCase()
            : t(wikilink.pathPart).toLowerCase();
        if (lookupName) {
            const byName = model.byTitleLower.get(lookupName);
            if (byName) {
                return {
                    note: byName,
                    relativePath: normalizePath(byName.fileName),
                    anchorPart: wikilink.anchorPart,
                    label: wikilink.label || byName.title || noteNameFromPath(byName.fileName),
                };
            }
        }
        return null;
    }

    function buildNoteBlockIndex(note) {
        if (!note || typeof note !== 'object') {
            return { blockById: new Map(), headingByKey: new Map() };
        }
        if (note.__blockIndexCache && note.__blockIndexCacheVersion === String(note.content || '')) {
            return note.__blockIndexCache;
        }
        const blockById = new Map();
        const headingByKey = new Map();
        const lines = String(note.content || '').split(/\r?\n/);
        let chunk = [];
        const flushChunk = () => {
            if (!chunk.length) {
                return;
            }
            const joined = chunk.join('\n').trim();
            if (!joined) {
                chunk = [];
                return;
            }
            const markerMatch = joined.match(BLOCK_ID_MARKER_REGEX);
            if (markerMatch && markerMatch[1]) {
                const blockId = t(markerMatch[1]);
                const cleaned = joined.replace(new RegExp(`\\^${blockId}\\s*$`), '').trim();
                if (blockId && cleaned) {
                    blockById.set(blockId, cleaned);
                }
            }
            chunk = [];
        };
        lines.forEach((line) => {
            const headingMatch = String(line || '').match(/^\s{0,3}#{1,6}\s+(.+)$/);
            if (headingMatch) {
                const headingText = t(headingMatch[1]);
                const key = normalizeHeadingToken(headingText);
                if (key && headingText) {
                    headingByKey.set(key, headingText);
                }
            }
            if (!String(line || '').trim()) {
                flushChunk();
            } else {
                chunk.push(line);
            }
        });
        flushChunk();
        const result = { blockById, headingByKey };
        note.__blockIndexCache = result;
        note.__blockIndexCacheVersion = String(note.content || '');
        return result;
    }

    function resolveBlockOrHeadingExcerpt(note, anchorPart) {
        const anchor = t(anchorPart);
        if (!note || !anchor) {
            return '';
        }
        const index = buildNoteBlockIndex(note);
        if (anchor.startsWith('^')) {
            const block = index.blockById.get(anchor.slice(1));
            return block || '';
        }
        const normalizedHeading = normalizeHeadingToken(anchor.replace(/^#/, ''));
        if (!normalizedHeading) {
            return '';
        }
        const headingText = index.headingByKey.get(normalizedHeading);
        return headingText ? `# ${headingText}` : '';
    }

    function buildObsidianModel(anchorId) {
        const key = t(anchorId);
        const notes = ensureLocalNoteFileNames(key);
        const byId = new Map();
        const byPathLower = new Map();
        const byTitleLower = new Map();
        const backlinksByTargetId = new Map();
        const unresolvedBySourceId = new Map();
        const tagsByNoteId = new Map();
        const edges = [];
        notes.forEach((note) => {
            byId.set(note.id, note);
            const pathLower = normalizePath(note.fileName).toLowerCase();
            if (pathLower) {
                byPathLower.set(pathLower, note);
                byTitleLower.set(noteNameFromPath(pathLower).toLowerCase(), note);
            }
            const titleLower = t(note.title).toLowerCase();
            if (titleLower) {
                byTitleLower.set(titleLower, note);
            }
            tagsByNoteId.set(note.id, extractTagTokens(note.content));
        });
        notes.forEach((note) => {
            const links = extractWikilinkTokens(note.content);
            const unresolved = [];
            links.forEach((link) => {
                const resolved = resolveWikilinkTargetForModel({
                    byPathLower,
                    byTitleLower,
                }, note, link);
                if (!resolved || !resolved.note) {
                    if (!link.isEmbed) {
                        unresolved.push(link);
                    }
                    return;
                }
                const targetId = t(resolved.note.id);
                if (!targetId) {
                    return;
                }
                if (!backlinksByTargetId.has(targetId)) {
                    backlinksByTargetId.set(targetId, []);
                }
                backlinksByTargetId.get(targetId).push({
                    sourceId: note.id,
                    sourceTitle: note.title || noteNameFromPath(note.fileName) || note.id,
                    label: resolved.label || link.label,
                    raw: link.raw,
                    anchorPart: resolved.anchorPart || '',
                    isEmbed: !!link.isEmbed,
                });
                edges.push({
                    sourceId: note.id,
                    targetId,
                    isEmbed: !!link.isEmbed,
                });
            });
            unresolvedBySourceId.set(note.id, unresolved);
        });
        return {
            notes,
            byId,
            byPathLower,
            byTitleLower,
            backlinksByTargetId,
            unresolvedBySourceId,
            tagsByNoteId,
            edges,
        };
    }

    function getObsidianModel(anchorId) {
        const key = t(anchorId);
        if (!key) {
            return buildObsidianModel('');
        }
        const version = computeObsidianModelVersion(key);
        const cached = runtime.obsidianGraphCacheByAnchor.get(key);
        if (cached && cached.version === version && cached.model) {
            return cached.model;
        }
        const model = buildObsidianModel(key);
        runtime.obsidianGraphCacheByAnchor.set(key, { version, model });
        return model;
    }

    function syncEditorFromActiveLocalNote(anchorId) {
        const editor = document.getElementById('anchorQuickNoteInput');
        if (!editor) {
            return;
        }
        const key = t(anchorId);
        applyLocalEditorLayoutMode(key);
        const active = getActiveLocalNote(anchorId);
        if (!active) {
            writeEditorValue('');
            editor.removeAttribute('data-local-note-id');
            renderLocalNoteLivePreview(anchorId);
            refreshObsidianStatusBar(anchorId);
            closeWikilinkSuggest();
            return;
        }
        editor.setAttribute('data-local-note-id', active.id);
        editor.setAttribute('placeholder', `Editing local markdown: ${active.title}`);
        writeEditorValue(String(active.content || ''), { clearHistory: true });
        renderLocalNoteLivePreview(anchorId);
        refreshObsidianStatusBar(anchorId);
    }

    function persistActiveLocalNoteFromEditor(anchorId, options = {}) {
        const key = t(anchorId);
        if (!key) {
            return;
        }
        const editor = document.getElementById('anchorQuickNoteInput');
        if (!editor) {
            return;
        }
        const notes = ensureLocalNotes(key);
        const activeId = getActiveLocalNoteId(key);
        const target = notes.find((item) => item.id === activeId);
        if (!target) {
            return;
        }
        const content = readEditorValue();
        const previousContent = String(target.content || '');
        const contentChanged = previousContent !== content;
        const previousTitle = String(target.title || '');
        let nextTitle = previousTitle;
        if (options.retitle !== false) {
            const fallbackTitle = t(target.fileName).replace(/\.markdown?$/i, '') || previousTitle || 'Note';
            nextTitle = normalizeLocalNoteTitle(content, fallbackTitle);
        }
        const titleChanged = nextTitle !== previousTitle;
        if (!contentChanged && !titleChanged) {
            return;
        }
        target.content = content;
        target.title = nextTitle;
        target.updatedAt = Date.now();
        writeLocalNotesToStorage(key, notes);
        invalidateObsidianModel(key);
        if (contentChanged) {
            markAnchorLocalSyncDirty(key);
        }
        renderLocalNoteLivePreview(key);
        refreshObsidianStatusBar(key);
    }

    function createLocalNote(anchorId) {
        const key = t(anchorId);
        if (!key) {
            return;
        }
        const notes = ensureLocalNotes(key);
        const note = normalizeLocalNoteItem({
            id: `local_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
            title: `Note ${notes.length + 1}`,
            content: '',
            fileName: '',
        }, notes.length);
        notes.push(note);
        runtime.localNotesByAnchor.set(key, notes);
        setActiveLocalNoteId(key, note.id);
        writeLocalNotesToStorage(key, notes);
        invalidateObsidianModel(key);
        markAnchorLocalSyncDirty(key);
    }

    function upsertLocalNoteFromMarkdownFile(anchorId, fileName, content) {
        const key = t(anchorId);
        if (!key) {
            return;
        }
        const normalizedFileName = normalizePath(fileName || '');
        if (!isMarkdown(normalizedFileName)) {
            return;
        }
        const notes = ensureLocalNotes(key);
        const lowerName = normalizedFileName.toLowerCase();
        let target = notes.find((item) => normalizePath(item.fileName || '').toLowerCase() === lowerName);
        if (!target) {
            target = normalizeLocalNoteItem({
                id: `local_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
                title: normalizedFileName.replace(/^.*\//, '').replace(/\.markdown?$/i, ''),
                content: '',
                fileName: normalizedFileName,
            }, notes.length);
            notes.push(target);
        }
        target.fileName = normalizedFileName;
        target.content = String(content || '');
        target.title = normalizeLocalNoteTitle(target.content, normalizedFileName.replace(/^.*\//, '').replace(/\.markdown?$/i, '') || target.title || 'Note');
        target.updatedAt = Date.now();
        runtime.localNotesByAnchor.set(key, notes);
        setActiveLocalNoteId(key, target.id);
        writeLocalNotesToStorage(key, notes);
        invalidateObsidianModel(key);
        markAnchorLocalSyncDirty(key);
    }

    function deleteLocalNote(anchorId, noteId) {
        const key = t(anchorId);
        const targetNoteId = t(noteId);
        if (!key || !targetNoteId) {
            return;
        }
        const notes = ensureLocalNotes(key);
        const index = notes.findIndex((item) => item.id === targetNoteId);
        if (index < 0) {
            return;
        }
        notes.splice(index, 1);
        if (!notes.length) {
            notes.push(normalizeLocalNoteItem({
                id: `local_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
                title: 'Note 1',
                content: '',
                fileName: '',
            }, 0));
        }
        runtime.localNotesByAnchor.set(key, notes);
        const nextActive = notes[Math.max(0, Math.min(index, notes.length - 1))];
        if (nextActive) {
            setActiveLocalNoteId(key, nextActive.id);
        }
        writeLocalNotesToStorage(key, notes);
        invalidateObsidianModel(key);
        markAnchorLocalSyncDirty(key);
    }

    function ensureLocalNoteFileNames(anchorId) {
        const key = t(anchorId);
        if (!key) {
            return [];
        }
        const notes = ensureLocalNotes(key);
        let changed = false;
        notes.forEach((note, index) => {
            const currentPath = normalizePath(note.fileName);
            if (currentPath) {
                note.fileName = currentPath;
                return;
            }
            const fallbackBase = note.title || `note_${index + 1}`;
            const fileName = safeMarkdownFileName(fallbackBase, `note_${index + 1}`);
            note.fileName = normalizePath(`cards/${fileName}`);
            changed = true;
        });
        if (changed) {
            runtime.localNotesByAnchor.set(key, notes);
            writeLocalNotesToStorage(key, notes);
            invalidateObsidianModel(key);
            markAnchorLocalSyncDirty(key);
        }
        return notes;
    }

    function buildLocalSyncSnapshot(anchorId) {
        const notes = ensureLocalNoteFileNames(anchorId);
        const snapshot = new Map();
        notes.forEach((note) => {
            const relativePath = normalizePath(note.fileName);
            if (!relativePath) {
                return;
            }
            snapshot.set(relativePath, String(note.content || ''));
        });
        return snapshot;
    }

    function buildLocalSyncOperations(currentSnapshot, shadowSnapshot) {
        const operations = [];
        shadowSnapshot.forEach((_content, relativePath) => {
            if (!currentSnapshot.has(relativePath)) {
                operations.push({
                    op: 'delete',
                    relativePath,
                });
            }
        });
        currentSnapshot.forEach((content, relativePath) => {
            if (!shadowSnapshot.has(relativePath)) {
                operations.push({
                    op: 'add',
                    relativePath,
                    content,
                });
                return;
            }
            if (String(shadowSnapshot.get(relativePath)) !== String(content)) {
                operations.push({
                    op: 'replace',
                    relativePath,
                    content,
                });
            }
        });
        return operations;
    }

    function canIncrementalSyncAnchor(anchorId) {
        const anchor = runtime.anchors.get(anchorId) || candidateOf(anchorId) || null;
        if (!anchor) {
            return false;
        }
        if (normalizeRevisionDir(anchor)) {
            return true;
        }
        return !!normalizePath(anchor.mountedPath);
    }

    async function syncAnchorLocalNotesIncremental(anchorId) {
        const normalizedAnchorId = t(anchorId);
        if (!normalizedAnchorId || !runtime.ctx.taskId) {
            return;
        }
        if (!canIncrementalSyncAnchor(normalizedAnchorId)) {
            return;
        }
        if (runtime.syncInFlightByAnchor.has(normalizedAnchorId)) {
            return;
        }
        const currentSnapshot = buildLocalSyncSnapshot(normalizedAnchorId);
        const shadowSnapshot = runtime.localSyncShadowByAnchor.get(normalizedAnchorId) || new Map();
        const operations = buildLocalSyncOperations(currentSnapshot, shadowSnapshot);
        if (!operations.length) {
            clearAnchorLocalSyncDirty(normalizedAnchorId);
            return;
        }
        runtime.syncInFlightByAnchor.add(normalizedAnchorId);
        try {
            const activeNote = getActiveLocalNote(normalizedAnchorId);
            const preferredMainNotePath = normalizePath(activeNote && activeNote.fileName);
            const resp = await fetch(`${runtime.ctx.apiBase}/tasks/${encodeURIComponent(runtime.ctx.taskId)}/anchors/${encodeURIComponent(normalizedAnchorId)}/sync`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    path: runtime.ctx.pathHint || '',
                    mainNotePath: preferredMainNotePath || '',
                    operations,
                }),
            });
            await parseResp(resp);
            runtime.localSyncShadowByAnchor.set(normalizedAnchorId, new Map(currentSnapshot));
            clearAnchorLocalSyncDirty(normalizedAnchorId);
        } catch (error) {
            setPreview(`Sync failed: ${t(error && error.message)}`);
        } finally {
            runtime.syncInFlightByAnchor.delete(normalizedAnchorId);
        }
    }

    function flushIncrementalLocalNoteSync() {
        if (!runtime.ctx.taskId) {
            return;
        }
        if (runtime.activeId && runtime.editorDirty) {
            persistActiveLocalNoteFromEditor(runtime.activeId, { retitle: true });
            renderLocalNoteCards(runtime.activeId);
            runtime.editorDirty = false;
        }
        const dirtyAnchorIds = Array.from(runtime.localSyncDirtyByAnchor.values());
        if (!dirtyAnchorIds.length) {
            return;
        }
        dirtyAnchorIds.forEach((anchorId) => {
            syncAnchorLocalNotesIncremental(anchorId);
        });
    }

    function renderLocalNoteCards(anchorId) {
        const container = document.getElementById('anchorLocalNoteCards');
        if (!container) {
            return;
        }
        const key = t(anchorId);
        if (!key) {
            container.innerHTML = '';
            return;
        }
        const notes = ensureLocalNotes(key);
        const activeId = getActiveLocalNoteId(key);
        container.innerHTML = `
            <div class="anchor-local-note-tabs">
                ${notes.map((note) => `
                    <button class="anchor-local-note-card${note.id === activeId ? ' is-active' : ''}" type="button" data-local-note-id="${h(note.id)}" title="${h(note.title)}">
                        <span class="anchor-local-note-card-title">${h(short(note.title, 32))}</span>
                        <span class="anchor-local-note-card-delete" data-local-note-delete="${h(note.id)}" role="button" aria-label="Delete note">×</span>
                    </button>
                `).join('')}
                <button class="anchor-local-note-card is-create" type="button" data-local-note-create="1" aria-label="Create note">+</button>
            </div>
        `;
    }

    function ensureObsidianNoteUi() {
        if (!document.getElementById('anchorComposerShell')) {
            return;
        }
        if (!document.getElementById('anchorObsidianUiStyle')) {
            const style = document.createElement('style');
            style.id = 'anchorObsidianUiStyle';
            style.textContent = `
                .anchor-quick-note-composer {
                    grid-template-columns: 1fr;
                }
                .anchor-local-note-cards {
                    display: block;
                    margin-bottom: 0;
                    border-bottom: 0;
                    overflow-x: auto;
                    overflow-y: hidden;
                    scrollbar-width: thin;
                    white-space: nowrap;
                    padding-right: 0;
                    position: relative;
                    z-index: 7;
                    border: 0;
                    box-shadow: none;
                    background: transparent;
                }
                .anchor-local-note-tabs {
                    display: inline-flex;
                    align-items: flex-end;
                    gap: 4px;
                    min-width: 100%;
                    padding-bottom: 0;
                    position: relative;
                    z-index: 7;
                    border: 0;
                    box-shadow: none;
                    background: transparent;
                }
                .anchor-local-note-card {
                    border: 1px solid rgba(100,116,139,.34);
                    border-bottom: 0;
                    background: #f8fafc;
                    color: #334155;
                    border-radius: 10px 10px 0 0;
                    padding: 7px 10px 6px;
                    display: inline-flex;
                    align-items: center;
                    gap: 8px;
                    min-width: 120px;
                    max-width: 240px;
                    text-align: left;
                    cursor: pointer;
                    position: relative;
                    top: 0;
                }
                .anchor-local-note-card.is-active {
                    border-color: #2563eb;
                    background: #ffffff;
                    color: #0f172a;
                    box-shadow: 0 -1px 0 0 rgba(37,99,235,.28);
                }
                .anchor-local-note-card.is-create {
                    min-width: 36px;
                    justify-content: center;
                    font-weight: 600;
                    color: #1d4ed8;
                    padding-inline: 0;
                }
                .anchor-local-note-card-title {
                    font-size: 12px;
                    line-height: 1.25;
                    overflow: hidden;
                    text-overflow: ellipsis;
                    white-space: nowrap;
                }
                .anchor-local-note-card-delete {
                    font-size: 13px;
                    line-height: 1;
                    color: #94a3b8;
                    cursor: pointer;
                    flex: 0 0 auto;
                }
                .anchor-local-note-card-delete:hover {
                    color: #b91c1c;
                }
                .anchor-composer-cloud-btn,
                .anchor-composer-attach-btn,
                .anchor-composer-send-btn {
                    display: none !important;
                }
                @media (max-width: 1080px) {
                    .anchor-quick-note-composer {
                        grid-template-columns: 1fr;
                    }
                }
            `;
            document.head.appendChild(style);
        }
        const composerShell = document.getElementById('anchorComposerShell');
        if (!composerShell) {
            return;
        }
        let quickNote = composerShell.querySelector('.anchor-quick-note');
        if (!quickNote) {
            quickNote = document.createElement('div');
            quickNote.className = 'anchor-quick-note';
            while (composerShell.firstChild) {
                quickNote.appendChild(composerShell.firstChild);
            }
            composerShell.appendChild(quickNote);
        }
        let composer = quickNote.querySelector('.anchor-quick-note-composer');
        if (!composer) {
            composer = document.createElement('div');
            composer.className = 'anchor-quick-note-composer';
            const footer = quickNote.querySelector('.obsidian-footer-actions');
            quickNote.insertBefore(composer, footer || quickNote.firstChild || null);
            const textarea = document.getElementById('anchorQuickNoteInput');
            const preview = document.getElementById('anchorPreview');
            const fileList = document.getElementById('anchorFileList');
            if (textarea && textarea.parentElement !== composer) {
                composer.appendChild(textarea);
            }
            if (preview && preview.parentElement !== composer) {
                composer.appendChild(preview);
            }
            if (fileList && fileList.parentElement !== composer) {
                composer.appendChild(fileList);
            }
        }
        Array.from(quickNote.children || []).forEach((node) => {
            if (!(node instanceof HTMLDivElement)) {
                return;
            }
            if (node.id || node.className || t(node.textContent)) {
                return;
            }
            const styleText = String(node.getAttribute('style') || '').toLowerCase();
            if (styleText.includes('justify-content') && styleText.includes('margin-bottom')) {
                node.remove();
            }
        });
        const sendBtn = document.getElementById('anchorUploadBtn');
        if (quickNote && !document.getElementById('anchorLocalNoteCards')) {
            const cards = document.createElement('div');
            cards.id = 'anchorLocalNoteCards';
            cards.className = 'anchor-local-note-cards';
            quickNote.insertBefore(cards, composer || null);
        }
        const pickBtn = document.getElementById('anchorPickBtn');
        if (pickBtn) {
            pickBtn.hidden = true;
            pickBtn.style.display = 'none';
        }
        if (sendBtn) {
            sendBtn.hidden = true;
            sendBtn.style.display = 'none';
        }
        const cloudBtn = document.getElementById('anchorCloudUploadBtn');
        if (cloudBtn) {
            cloudBtn.hidden = true;
            cloudBtn.style.display = 'none';
        }
    }

    function getLocalNoteFilter(anchorId) {
        const key = t(anchorId);
        if (!key) {
            return '';
        }
        return t(runtime.localNoteFilterByAnchor.get(key)).toLowerCase();
    }

    function setLocalNoteFilter(anchorId, query) {
        const key = t(anchorId);
        if (!key) {
            return;
        }
        const normalized = t(query).toLowerCase();
        if (!normalized) {
            runtime.localNoteFilterByAnchor.delete(key);
            return;
        }
        runtime.localNoteFilterByAnchor.set(key, normalized);
    }

    function getFilteredLocalNotes(anchorId, notes) {
        const filter = getLocalNoteFilter(anchorId);
        if (!filter) {
            return Array.isArray(notes) ? notes : [];
        }
        return (Array.isArray(notes) ? notes : []).filter((note) => {
            const title = t(note && note.title).toLowerCase();
            const fileName = normalizePath(note && note.fileName).toLowerCase();
            const content = String(note && note.content || '').toLowerCase();
            return title.includes(filter) || fileName.includes(filter) || content.includes(filter);
        });
    }

    function buildObsidianNoteLinkHref(targetNoteId, anchorPart) {
        const noteId = encodeURIComponent(t(targetNoteId));
        const anchor = t(anchorPart);
        if (!noteId) {
            return '#';
        }
        return anchor
            ? `/__obsidian_note__/${noteId}?anchor=${encodeURIComponent(anchor)}`
            : `/__obsidian_note__/${noteId}`;
    }

    function buildObsidianCreateLinkHref(rawWikilink) {
        return `/__obsidian_create__/${encodeURIComponent(String(rawWikilink || '').trim())}`;
    }

    function buildEmbedSnippetMarkdown(targetNote, anchorPart) {
        const title = t(targetNote && targetNote.title) || noteNameFromPath(targetNote && targetNote.fileName) || 'Embedded note';
        const excerpt = resolveBlockOrHeadingExcerpt(targetNote, anchorPart)
            || buildLocalPreviewExcerpt(targetNote && targetNote.content, 5)
            || '(empty)';
        const lines = String(excerpt || '')
            .split(/\r?\n/)
            .filter((line) => line.trim().length > 0)
            .slice(0, 8);
        const quoted = lines.map((line) => `> ${line}`).join('\n');
        return `> [!quote] ${title}\n${quoted || '> (empty)'}`;
    }

    function transformObsidianPreviewMarkdown(markdownText, anchorId, currentNoteId) {
        const key = t(anchorId);
        const model = getObsidianModel(key);
        const sourceNote = model.byId.get(t(currentNoteId)) || null;
        let transformed = String(markdownText || '');
        transformed = transformed.replace(/!\[\[([^\]\n]+)\]\]/g, (_m, inner) => {
            const parsed = parseWikilinkToken(inner);
            if (!parsed) {
                return _m;
            }
            const resolved = resolveWikilinkTargetForModel(model, sourceNote, parsed);
            if (!resolved || !resolved.note) {
                return `> Missing embed: [[${parsed.raw}]]`;
            }
            return buildEmbedSnippetMarkdown(resolved.note, resolved.anchorPart);
        });
        transformed = transformed.replace(/(^|[^!])\[\[([^\]\n]+)\]\]/g, (_m, prefix, inner) => {
            const parsed = parseWikilinkToken(inner);
            if (!parsed) {
                return _m;
            }
            const resolved = resolveWikilinkTargetForModel(model, sourceNote, parsed);
            if (!resolved || !resolved.note) {
                return `${prefix}[${parsed.label}](${buildObsidianCreateLinkHref(parsed.raw)})`;
            }
            return `${prefix}[${resolved.label}](${buildObsidianNoteLinkHref(resolved.note.id, resolved.anchorPart)})`;
        });
        transformed = transformed.replace(/==([^=\n][^=\n]*?)==/g, '<mark>$1</mark>');
        return transformed;
    }

    function renderAnchorPreviewMarkdown(markdownText, anchorId, currentNoteId) {
        const transformed = transformObsidianPreviewMarkdown(markdownText, anchorId, currentNoteId);
        let html = '';
        if (typeof window.renderMarkdownFragment === 'function') {
            try {
                html = String(window.renderMarkdownFragment(
                    String(transformed || ''),
                    t(runtime.ctx && runtime.ctx.taskId)
                ) || '');
            } catch (_e) {
                html = '';
            }
        }
        if (!html) {
            html = window.markdownit
                ? window.markdownit({ html: true, breaks: true, linkify: true }).render(String(transformed || ''))
                : `<pre>${h(String(transformed || ''))}</pre>`;
        }
        return window.DOMPurify ? window.DOMPurify.sanitize(html) : html;
    }

    function renderPhase2bPreviewMarkdown(markdownText) {
        const source = String(markdownText || '');
        let html = '';
        if (window.markdownit) {
            html = window.markdownit({ html: true, breaks: true, linkify: true }).render(source);
        } else if (typeof window.renderMarkdownFragment === 'function') {
            try {
                html = String(window.renderMarkdownFragment(
                    source,
                    t(runtime.ctx && runtime.ctx.taskId)
                ) || '');
            } catch (_e) {
                html = '';
            }
        }
        if (!html) {
            html = `<pre>${h(source)}</pre>`;
        }
        return window.DOMPurify ? window.DOMPurify.sanitize(html) : html;
    }

    function isHoverPreviewableHref(hrefRaw) {
        const href = String(hrefRaw || '').trim();
        if (!href || href.startsWith('#')) {
            return false;
        }
        if (href.startsWith('/__obsidian_note__/')) {
            return true;
        }
        const schemeMatch = href.match(/^([a-zA-Z][a-zA-Z0-9+\-.]*):/);
        if (schemeMatch) {
            return false;
        }
        const decoded = decodePath(href.split('#')[0].split('?')[0]);
        const notePath = normalizePath(decoded);
        return !!(notePath && isMarkdown(notePath));
    }

    function resolveHoverPreviewTarget(anchorId, hrefRaw) {
        const key = t(anchorId);
        if (!key) {
            return null;
        }
        const href = String(hrefRaw || '').trim();
        if (!isHoverPreviewableHref(href)) {
            return null;
        }
        const model = getObsidianModel(key);
        if (href.startsWith('/__obsidian_note__/')) {
            const pathWithoutHash = href.split('#')[0];
            const idPart = pathWithoutHash.replace(/^\/__obsidian_note__\//, '');
            const encodedId = idPart.split('?')[0];
            const queryRaw = idPart.includes('?') ? idPart.slice(idPart.indexOf('?') + 1) : '';
            const query = new URLSearchParams(queryRaw);
            const noteId = decodePath(encodedId);
            const note = model.byId.get(noteId);
            if (!note) {
                return null;
            }
            return {
                note,
                anchorPart: t(query.get('anchor')),
            };
        }
        const decoded = decodePath(href.split('#')[0].split('?')[0]);
        const notePath = normalizePath(decoded);
        if (!notePath) {
            return null;
        }
        const note = model.notes.find((item) => normalizePath(item.fileName).toLowerCase() === notePath.toLowerCase()) || null;
        if (!note) {
            return null;
        }
        return {
            note,
            anchorPart: '',
        };
    }

    function ensureObsidianHoverCard() {
        let card = document.getElementById('anchorObsidianHoverCard');
        if (card) {
            return card;
        }
        card = document.createElement('div');
        card.id = 'anchorObsidianHoverCard';
        card.className = 'anchor-obsidian-hover-card';
        card.hidden = true;
        card.setAttribute('aria-hidden', 'true');
        card.innerHTML = `
            <div class="anchor-obsidian-hover-card-head">
                <div class="anchor-obsidian-hover-card-title"></div>
                <div class="anchor-obsidian-hover-card-meta"></div>
            </div>
            <div class="anchor-obsidian-hover-card-body"></div>
        `;
        card.addEventListener('mouseenter', () => {
            if (runtime.linkHoverHideTimer) {
                clearTimeout(runtime.linkHoverHideTimer);
                runtime.linkHoverHideTimer = 0;
            }
        });
        card.addEventListener('mouseleave', () => {
            scheduleHideObsidianHoverCard(120);
        });
        document.body.appendChild(card);
        return card;
    }

    function scheduleHideObsidianHoverCard(delay) {
        if (runtime.linkHoverHideTimer) {
            clearTimeout(runtime.linkHoverHideTimer);
            runtime.linkHoverHideTimer = 0;
        }
        const timeout = Number.isFinite(Number(delay)) ? Number(delay) : 100;
        runtime.linkHoverHideTimer = setTimeout(() => {
            runtime.linkHoverHideTimer = 0;
            const card = document.getElementById('anchorObsidianHoverCard');
            if (card) {
                card.hidden = true;
                card.setAttribute('aria-hidden', 'true');
            }
        }, Math.max(40, timeout));
    }

    function hideObsidianHoverCardNow() {
        if (runtime.linkHoverHideTimer) {
            clearTimeout(runtime.linkHoverHideTimer);
            runtime.linkHoverHideTimer = 0;
        }
        const card = document.getElementById('anchorObsidianHoverCard');
        if (!card) {
            return;
        }
        card.hidden = true;
        card.setAttribute('aria-hidden', 'true');
    }

    function positionObsidianHoverCard(card, linkElement) {
        if (!(card instanceof HTMLElement) || !(linkElement instanceof HTMLElement)) {
            return;
        }
        const rect = linkElement.getBoundingClientRect();
        card.style.left = '12px';
        card.style.top = '12px';
        card.hidden = false;
        card.setAttribute('aria-hidden', 'false');
        const cardRect = card.getBoundingClientRect();
        let left = rect.right + 12;
        if (left + cardRect.width > window.innerWidth - 12) {
            left = rect.left - cardRect.width - 12;
        }
        left = Math.max(12, Math.min(left, window.innerWidth - cardRect.width - 12));
        let top = rect.top + 4;
        if (top + cardRect.height > window.innerHeight - 12) {
            top = window.innerHeight - cardRect.height - 12;
        }
        top = Math.max(12, top);
        card.style.left = `${left}px`;
        card.style.top = `${top}px`;
    }

    function showObsidianHoverCard(anchorId, linkElement) {
        if (!(linkElement instanceof HTMLElement)) {
            return;
        }
        const hrefRaw = String(linkElement.getAttribute('href') || '').trim();
        const resolved = resolveHoverPreviewTarget(anchorId, hrefRaw);
        if (!resolved || !resolved.note) {
            scheduleHideObsidianHoverCard(80);
            return;
        }
        if (runtime.linkHoverHideTimer) {
            clearTimeout(runtime.linkHoverHideTimer);
            runtime.linkHoverHideTimer = 0;
        }
        const card = ensureObsidianHoverCard();
        const title = t(resolved.note.title) || noteNameFromPath(resolved.note.fileName) || resolved.note.id;
        const meta = normalizePath(resolved.note.fileName || '') || `id:${t(resolved.note.id)}`;
        const bodyHtml = renderAnchorPreviewMarkdown(String(resolved.note.content || ''), anchorId, resolved.note.id);
        const titleNode = card.querySelector('.anchor-obsidian-hover-card-title');
        const metaNode = card.querySelector('.anchor-obsidian-hover-card-meta');
        const bodyNode = card.querySelector('.anchor-obsidian-hover-card-body');
        if (titleNode) {
            titleNode.textContent = title;
        }
        if (metaNode) {
            metaNode.textContent = meta;
        }
        if (bodyNode) {
            bodyNode.innerHTML = bodyHtml || '<div class="anchor-obsidian-empty">No markdown content</div>';
        }
        positionObsidianHoverCard(card, linkElement);
    }

    function renderLocalNoteLivePreview(anchorId) {
        const preview = document.getElementById('anchorLocalNoteLivePreview');
        if (!preview) {
            return;
        }
        const key = t(anchorId);
        if (!key) {
            preview.innerHTML = '<div class="anchor-obsidian-empty">Select an anchor to edit local markdown</div>';
            return;
        }
        const active = getActiveLocalNote(key);
        if (!active) {
            preview.innerHTML = '<div class="anchor-obsidian-empty">No local note selected</div>';
            return;
        }
        const html = renderAnchorPreviewMarkdown(active.content || '', key, active.id);
        preview.innerHTML = html || '<div class="anchor-obsidian-empty">No markdown content</div>';
    }

    function refreshObsidianStatusBar(anchorId) {
        const status = document.getElementById('anchorObsidianStatusBar');
        if (!status) {
            return;
        }
        const key = t(anchorId);
        const active = key ? getActiveLocalNote(key) : null;
        const content = readEditorValue();
        const words = t(content).split(/\s+/).filter(Boolean).length;
        const chars = content.length;
        const mode = readLocalEditorLayoutMode(key || runtime.activeId);
        const fileName = normalizePath(active && active.fileName);
        const left = fileName ? short(fileName, 40) : short(t(active && active.title) || 'Local note', 40);
        status.innerHTML = `
            <span class="anchor-obsidian-status-item">${h(left)}</span>
            <span class="anchor-obsidian-status-item">Words ${words}</span>
            <span class="anchor-obsidian-status-item">Chars ${chars}</span>
            <span class="anchor-obsidian-status-item">Mode ${h(mode)}</span>
        `;
    }

    async function ensureVditorEditor(anchorId) {
        const container = document.getElementById('anchorQuickNoteVditor');
        const textarea = document.getElementById('anchorQuickNoteInput');
        const columns = document.getElementById('anchorObsidianEditorColumns');
        if (!container || !textarea) {
            return false;
        }
        const ready = await ensureVditorRuntime();
        if (!ready || !window.Vditor) {
            container.hidden = true;
            textarea.hidden = false;
            textarea.setAttribute('aria-hidden', 'false');
            if (columns) {
                columns.classList.remove('is-vditor-ready');
            }
            return false;
        }
        if (runtime.vditorInstance) {
            container.hidden = false;
            textarea.hidden = true;
            textarea.setAttribute('aria-hidden', 'true');
            if (columns) {
                columns.classList.add('is-vditor-ready');
            }
            return true;
        }
        container.hidden = false;
        textarea.hidden = true;
        textarea.setAttribute('aria-hidden', 'true');
        if (columns) {
            columns.classList.add('is-vditor-ready');
        }
        runtime.vditorInstance = new window.Vditor('anchorQuickNoteVditor', {
            mode: 'ir',
            width: '100%',
            minHeight: 220,
            placeholder: textarea.getAttribute('placeholder') || 'Write markdown...',
            cache: { enable: false },
            toolbar: [
                'emoji',
                'headings',
                'bold',
                'italic',
                'strike',
                '|',
                'list',
                'ordered-list',
                'check',
                '|',
                'link',
                'quote',
                'line',
                'code',
                'inline-code',
                '|',
                'undo',
                'redo',
                '|',
                'outline',
                'fullscreen',
            ],
            toolbarConfig: {
                pin: false,
            },
            preview: {
                delay: 120,
                mode: 'editor',
                markdown: {
                    toc: true,
                },
                hljs: {
                    style: 'github',
                },
            },
            after: () => {
                runtime.vditorReady = true;
                const initValue = String(textarea.value || '');
                runtime.vditorSyncing = true;
                runtime.vditorInstance.setValue(initValue, true);
                runtime.vditorSyncing = false;
                refreshObsidianStatusBar(anchorId || runtime.activeId);
            },
            input: (value) => {
                if (runtime.vditorSyncing) {
                    return;
                }
                textarea.value = String(value || '');
                textarea.dispatchEvent(new Event('input', { bubbles: true }));
                refreshObsidianStatusBar(runtime.activeId);
            },
            blur: (value) => {
                textarea.value = String(value || '');
                textarea.dispatchEvent(new Event('blur', { bubbles: true }));
                refreshObsidianStatusBar(runtime.activeId);
            },
            focus: () => {
                runtime.editorActive = true;
                textarea.dispatchEvent(new Event('focus', { bubbles: true }));
                refreshObsidianStatusBar(runtime.activeId);
            },
        });
        return true;
    }

    function readLocalEditorLayoutMode(anchorId) {
        const key = t(anchorId);
        if (!key) {
            return 'split';
        }
        if (runtime.localNoteLayoutModeByAnchor.has(key)) {
            return normalizeEditorLayoutMode(runtime.localNoteLayoutModeByAnchor.get(key));
        }
        try {
            const modeRaw = localStorage.getItem(localLayoutStoreKey(key));
            const mode = normalizeEditorLayoutMode(modeRaw);
            runtime.localNoteLayoutModeByAnchor.set(key, mode);
            return mode;
        } catch (_e) {
            return 'split';
        }
    }

    function writeLocalEditorLayoutMode(anchorId, modeLike) {
        const key = t(anchorId);
        if (!key) {
            return;
        }
        const mode = normalizeEditorLayoutMode(modeLike);
        runtime.localNoteLayoutModeByAnchor.set(key, mode);
        try {
            localStorage.setItem(localLayoutStoreKey(key), mode);
        } catch (_e) {
        }
    }

    function applyLocalEditorLayoutMode(anchorId) {
        const mode = readLocalEditorLayoutMode(anchorId);
        const columns = document.getElementById('anchorObsidianEditorColumns');
        if (columns) {
            columns.setAttribute('data-layout', mode);
        }
        const layoutSelect = document.getElementById('anchorObsidianLayoutMode');
        if (layoutSelect && layoutSelect.value !== mode) {
            layoutSelect.value = mode;
        }
    }

    function closeWikilinkSuggest() {
        const panel = document.getElementById('anchorWikilinkSuggest');
        if (panel) {
            panel.hidden = true;
            panel.innerHTML = '';
            panel.style.left = '';
            panel.style.top = '';
        }
        runtime.wikilinkSuggest.open = false;
        runtime.wikilinkSuggest.anchorId = '';
        runtime.wikilinkSuggest.mode = 'slash';
        runtime.wikilinkSuggest.start = -1;
        runtime.wikilinkSuggest.end = -1;
        runtime.wikilinkSuggest.range = null;
        runtime.wikilinkSuggest.query = '';
        runtime.wikilinkSuggest.options = [];
        runtime.wikilinkSuggest.activeIndex = 0;
    }

    function captureEditorSelectionForSuggest() {
        const editor = document.getElementById('anchorQuickNoteInput');
        runtime.wikilinkSuggest.range = null;
        if (isVditorReady()) {
            const host = document.getElementById('anchorQuickNoteVditor');
            const selection = window.getSelection ? window.getSelection() : null;
            if (!host || !selection || selection.rangeCount <= 0) {
                runtime.wikilinkSuggest.start = -1;
                runtime.wikilinkSuggest.end = -1;
                return;
            }
            const current = selection.getRangeAt(0);
            if (!host.contains(current.startContainer)) {
                runtime.wikilinkSuggest.start = -1;
                runtime.wikilinkSuggest.end = -1;
                return;
            }
            runtime.wikilinkSuggest.range = current.cloneRange();
            runtime.wikilinkSuggest.start = -1;
            runtime.wikilinkSuggest.end = -1;
            return;
        }
        if (!(editor instanceof HTMLTextAreaElement)) {
            runtime.wikilinkSuggest.start = -1;
            runtime.wikilinkSuggest.end = -1;
            return;
        }
        const start = Number(editor.selectionStart);
        const end = Number(editor.selectionEnd);
        runtime.wikilinkSuggest.start = Number.isInteger(start) ? start : -1;
        runtime.wikilinkSuggest.end = Number.isInteger(end) ? end : runtime.wikilinkSuggest.start;
    }

    function restoreEditorSelectionForSuggest() {
        const editor = document.getElementById('anchorQuickNoteInput');
        if (isVditorReady()) {
            const range = runtime.wikilinkSuggest.range;
            if (!(range instanceof Range)) {
                return;
            }
            const host = document.getElementById('anchorQuickNoteVditor');
            if (!host || !host.contains(range.startContainer)) {
                return;
            }
            const selection = window.getSelection ? window.getSelection() : null;
            if (!selection) {
                return;
            }
            selection.removeAllRanges();
            selection.addRange(range.cloneRange());
            return;
        }
        if (!(editor instanceof HTMLTextAreaElement)) {
            return;
        }
        const start = Number(runtime.wikilinkSuggest.start);
        const end = Number(runtime.wikilinkSuggest.end);
        const nextStart = Number.isInteger(start) && start >= 0 ? start : Number(editor.selectionStart);
        const nextEnd = Number.isInteger(end) && end >= nextStart ? end : nextStart;
        editor.focus();
        editor.setSelectionRange(nextStart, nextEnd);
    }

    function insertTextIntoEditorSelection(textLike) {
        const text = String(textLike || '');
        if (!text) {
            return false;
        }
        const editor = document.getElementById('anchorQuickNoteInput');
        if (isVditorReady() && runtime.vditorInstance && typeof runtime.vditorInstance.insertValue === 'function') {
            runtime.vditorInstance.insertValue(text);
            if (editor instanceof HTMLTextAreaElement) {
                editor.value = String(runtime.vditorInstance.getValue() || '');
                editor.dispatchEvent(new Event('input', { bubbles: true }));
            }
            return true;
        }
        if (!(editor instanceof HTMLTextAreaElement)) {
            return false;
        }
        const start = Number(editor.selectionStart);
        const end = Number(editor.selectionEnd);
        const safeStart = Number.isInteger(start) ? start : 0;
        const safeEnd = Number.isInteger(end) ? end : safeStart;
        const left = String(editor.value || '').slice(0, safeStart);
        const right = String(editor.value || '').slice(safeEnd);
        editor.value = `${left}${text}${right}`;
        const caret = left.length + text.length;
        editor.setSelectionRange(caret, caret);
        editor.dispatchEvent(new Event('input', { bubbles: true }));
        return true;
    }

    function readEditorTextBeforeCaret(maxChars) {
        const limit = Math.max(1, Number(maxChars) || 1);
        const editor = document.getElementById('anchorQuickNoteInput');
        if (isVditorReady()) {
            const host = document.getElementById('anchorQuickNoteVditor');
            const selection = window.getSelection ? window.getSelection() : null;
            if (!host || !selection || selection.rangeCount <= 0) {
                return '';
            }
            const range = selection.getRangeAt(0);
            if (!host.contains(range.startContainer)) {
                return '';
            }
            const probe = range.cloneRange();
            probe.selectNodeContents(host);
            probe.setEnd(range.startContainer, range.startOffset);
            return String(probe.toString() || '').slice(-limit);
        }
        if (!(editor instanceof HTMLTextAreaElement)) {
            return '';
        }
        const start = Number(editor.selectionStart);
        if (!Number.isInteger(start) || start < 0) {
            return '';
        }
        return String(editor.value || '').slice(Math.max(0, start - limit), start);
    }

    function resolveTextareaCaretRect(textarea) {
        if (!(textarea instanceof HTMLTextAreaElement)) {
            return null;
        }
        const rect = textarea.getBoundingClientRect();
        const style = window.getComputedStyle(textarea);
        const lineHeight = parseFloat(style.lineHeight) || 20;
        const paddingLeft = parseFloat(style.paddingLeft) || 0;
        const paddingTop = parseFloat(style.paddingTop) || 0;
        const value = String(textarea.value || '');
        const caret = Number.isInteger(Number(textarea.selectionStart)) ? Number(textarea.selectionStart) : value.length;
        const leftText = value.slice(0, Math.max(0, caret));
        const lineCount = Math.max(1, leftText.split('\n').length);
        const top = rect.top + paddingTop + (lineCount - 1) * lineHeight - textarea.scrollTop;
        const left = rect.left + paddingLeft + 8;
        return {
            left,
            right: left + 1,
            top,
            bottom: top + lineHeight,
            width: 1,
            height: lineHeight,
        };
    }

    function resolveEditorCaretRect() {
        if (isVditorReady()) {
            const host = document.getElementById('anchorQuickNoteVditor');
            const selection = window.getSelection ? window.getSelection() : null;
            if (host && selection && selection.rangeCount > 0) {
                const range = selection.getRangeAt(0).cloneRange();
                if (host.contains(range.startContainer)) {
                    range.collapse(true);
                    const rect = range.getBoundingClientRect();
                    if (rect && Number.isFinite(rect.left)) {
                        return rect;
                    }
                }
            }
            return host ? host.getBoundingClientRect() : null;
        }
        const editor = document.getElementById('anchorQuickNoteInput');
        return resolveTextareaCaretRect(editor);
    }

    function positionWikilinkSuggestPanel(panel, caretRect) {
        if (!(panel instanceof HTMLElement)) {
            return;
        }
        const rect = caretRect && Number.isFinite(caretRect.left)
            ? caretRect
            : { left: window.innerWidth / 2, bottom: window.innerHeight / 2, top: window.innerHeight / 2 };
        panel.style.left = '12px';
        panel.style.top = '12px';
        panel.hidden = false;
        const panelRect = panel.getBoundingClientRect();
        let left = rect.left;
        let top = rect.bottom + 10;
        if (left + panelRect.width > window.innerWidth - 12) {
            left = window.innerWidth - panelRect.width - 12;
        }
        if (left < 12) {
            left = 12;
        }
        if (top + panelRect.height > window.innerHeight - 12) {
            top = (rect.top || rect.bottom || 0) - panelRect.height - 10;
        }
        if (top < 12) {
            top = 12;
        }
        panel.style.left = `${left}px`;
        panel.style.top = `${top}px`;
    }

    function buildWikilinkSuggestOptions(anchorId, queryLower) {
        const key = t(anchorId);
        if (!key) {
            return [];
        }
        const notes = ensureLocalNotes(key);
        const activeId = getActiveLocalNoteId(key);
        const dedupe = new Set();
        const query = String(queryLower || '').toLowerCase();
        const options = [];
        notes.forEach((note) => {
            if (!note || !t(note.id)) {
                return;
            }
            const title = t(note.title) || noteNameFromPath(note.fileName) || note.id;
            const fileName = normalizePath(note.fileName || '');
            const id = t(note.id);
            const dedupeKey = title.toLowerCase();
            if (!title || dedupe.has(dedupeKey)) {
                return;
            }
            const target = `${title} ${fileName} ${id}`.toLowerCase();
            if (query && !target.includes(query)) {
                return;
            }
            dedupe.add(dedupeKey);
            let score = 100;
            if (query) {
                if (title.toLowerCase() === query) {
                    score += 300;
                } else if (title.toLowerCase().startsWith(query)) {
                    score += 180;
                } else if (target.includes(query)) {
                    score += 80;
                }
            }
            if (id === activeId) {
                score -= 20;
            }
            options.push({
                kind: 'note',
                noteId: id,
                title,
                fileName,
                updatedAt: Number(note.updatedAt) || 0,
                score,
            });
        });
        runtime.candidates.forEach((candidate) => {
            const anchorIdRaw = t(candidate && candidate.anchorId);
            if (!anchorIdRaw) {
                return;
            }
            const title = t(candidate.displayText) || `锚点 ${short(anchorIdRaw, 12)}`;
            const meta = t(candidate.contextQuote || candidate.quote || candidate.anchorHint || '');
            const target = `${title} ${meta} ${anchorIdRaw}`.toLowerCase();
            if (query && !target.includes(query)) {
                return;
            }
            let score = 90;
            if (query) {
                if (title.toLowerCase() === query) {
                    score += 260;
                } else if (title.toLowerCase().startsWith(query)) {
                    score += 150;
                } else if (target.includes(query)) {
                    score += 70;
                }
            }
            options.push({
                kind: 'anchor',
                anchorId: anchorIdRaw,
                title,
                fileName: meta || anchorIdRaw,
                updatedAt: Date.now(),
                score,
            });
        });
        return options
            .sort((a, b) => {
                if (b.score !== a.score) return b.score - a.score;
                if (b.updatedAt !== a.updatedAt) return b.updatedAt - a.updatedAt;
                return a.title.localeCompare(b.title);
            })
            .slice(0, 12);
    }

    function renderWikilinkSuggest(anchorId, options = {}) {
        const panel = document.getElementById('anchorWikilinkSuggest');
        const key = t(anchorId);
        if (!panel || !key) {
            closeWikilinkSuggest();
            return;
        }
        if (!runtime.wikilinkSuggest.open && !options.force) {
            closeWikilinkSuggest();
            return;
        }
        const queryRaw = t(runtime.wikilinkSuggest.query);
        const suggestOptions = buildWikilinkSuggestOptions(key, queryRaw.toLowerCase());
        runtime.wikilinkSuggest.options = suggestOptions;
        runtime.wikilinkSuggest.open = true;
        runtime.wikilinkSuggest.anchorId = key;
        if (!Number.isInteger(runtime.wikilinkSuggest.activeIndex) || runtime.wikilinkSuggest.activeIndex < 0 || runtime.wikilinkSuggest.activeIndex >= suggestOptions.length) {
            runtime.wikilinkSuggest.activeIndex = 0;
        }
        if (!suggestOptions.length) {
            panel.hidden = false;
            panel.innerHTML = `
                <div class="anchor-obsidian-wikilink-head">链接到文件或锚点</div>
                <input class="anchor-obsidian-wikilink-query" data-wikilink-query type="text" value="${h(queryRaw)}" placeholder="输入关键词筛选">
                <div class="anchor-obsidian-empty">没有匹配项</div>
            `;
            positionWikilinkSuggestPanel(panel, options.caretRect || resolveEditorCaretRect());
            const queryInputEmpty = panel.querySelector('[data-wikilink-query]');
            if (options.focusQuery && queryInputEmpty instanceof HTMLInputElement) {
                queryInputEmpty.focus();
                const caret = queryInputEmpty.value.length;
                queryInputEmpty.setSelectionRange(caret, caret);
            }
            return;
        }
        panel.hidden = false;
        panel.innerHTML = `
            <div class="anchor-obsidian-wikilink-head">链接到文件或锚点</div>
            <input class="anchor-obsidian-wikilink-query" data-wikilink-query type="text" value="${h(queryRaw)}" placeholder="输入关键词筛选">
            <div class="anchor-obsidian-wikilink-list">
                ${suggestOptions.map((item, idx) => `
                    <button class="anchor-obsidian-wikilink-item${idx === runtime.wikilinkSuggest.activeIndex ? ' is-active' : ''}" type="button" data-wikilink-index="${idx}">
                        <span class="anchor-obsidian-wikilink-title">${h(short(item.title, 36))}</span>
                        <span class="anchor-obsidian-wikilink-meta">
                            <span>${h(short(item.fileName || item.noteId || item.anchorId, 42))}</span>
                            <span class="anchor-obsidian-wikilink-badge">${item.kind === 'anchor' ? '锚点' : '文件'}</span>
                        </span>
                    </button>
                `).join('')}
            </div>
        `;
        positionWikilinkSuggestPanel(panel, options.caretRect || resolveEditorCaretRect());
        const queryInput = panel.querySelector('[data-wikilink-query]');
        if (options.focusQuery && queryInput instanceof HTMLInputElement) {
            queryInput.focus();
            const caret = queryInput.value.length;
            queryInput.setSelectionRange(caret, caret);
        }
    }

    function moveWikilinkSuggestActive(step) {
        if (!runtime.wikilinkSuggest.open || !runtime.wikilinkSuggest.options.length) {
            return;
        }
        const total = runtime.wikilinkSuggest.options.length;
        const current = Number.isInteger(runtime.wikilinkSuggest.activeIndex) ? runtime.wikilinkSuggest.activeIndex : 0;
        const next = (current + step + total) % total;
        runtime.wikilinkSuggest.activeIndex = next;
        const panel = document.getElementById('anchorWikilinkSuggest');
        if (!panel) {
            return;
        }
        panel.querySelectorAll('[data-wikilink-index]').forEach((node) => {
            const idx = Number(node.getAttribute('data-wikilink-index'));
            node.classList.toggle('is-active', idx === next);
        });
    }

    function applyWikilinkSuggestSelection(indexLike) {
        if (!runtime.wikilinkSuggest.open) {
            return false;
        }
        const index = Number(indexLike);
        if (!Number.isInteger(index) || index < 0 || index >= runtime.wikilinkSuggest.options.length) {
            return false;
        }
        const option = runtime.wikilinkSuggest.options[index];
        if (!option) {
            closeWikilinkSuggest();
            return false;
        }
        const anchorId = t(runtime.wikilinkSuggest.anchorId || runtime.activeId);
        const mode = t(runtime.wikilinkSuggest.mode || 'slash');
        if (option.kind === 'anchor' && option.anchorId) {
            if (mode === 'double_bracket') {
                restoreEditorSelectionForSuggest();
                insertTextIntoEditorSelection(`[${option.title}]]`);
            }
            closeWikilinkSuggest();
            selectAnchor(option.anchorId, 'suggest_anchor', true);
            return true;
        }
        if (option.kind !== 'note') {
            closeWikilinkSuggest();
            return false;
        }
        const insertedText = mode === 'double_bracket'
            ? `[${option.title}]]`
            : `[[${option.title}]]`;
        restoreEditorSelectionForSuggest();
        const inserted = insertTextIntoEditorSelection(insertedText);
        closeWikilinkSuggest();
        if (!inserted) {
            return false;
        }
        if (anchorId) {
            persistActiveLocalNoteFromEditor(anchorId, { retitle: false });
            renderLocalNoteLivePreview(anchorId);
            renderObsidianKnowledgePanels(anchorId);
            refreshObsidianStatusBar(anchorId);
        }
        focusEditorInput();
        return true;
    }

    function openWikilinkSuggest(anchorId, modeLike, options = {}) {
        const key = t(anchorId);
        if (!key) {
            closeWikilinkSuggest();
            return false;
        }
        runtime.wikilinkSuggest.open = true;
        runtime.wikilinkSuggest.anchorId = key;
        runtime.wikilinkSuggest.mode = t(modeLike) || 'slash';
        runtime.wikilinkSuggest.query = t(options.query || '');
        runtime.wikilinkSuggest.options = [];
        runtime.wikilinkSuggest.activeIndex = 0;
        captureEditorSelectionForSuggest();
        renderWikilinkSuggest(key, { caretRect: resolveEditorCaretRect(), focusQuery: true, force: true });
        return true;
    }

    function renderObsidianKnowledgePanels(anchorId) {
        const panel = document.getElementById('anchorObsidianPanels');
        if (!panel) {
            return;
        }
        const key = t(anchorId);
        if (!key) {
            panel.innerHTML = '';
            return;
        }
        const model = getObsidianModel(key);
        const activeNoteId = getActiveLocalNoteId(key);
        const activeNote = model.byId.get(activeNoteId) || null;
        if (!activeNote) {
            panel.innerHTML = '';
            return;
        }
        const backlinks = model.backlinksByTargetId.get(activeNoteId) || [];
        const unresolved = model.unresolvedBySourceId.get(activeNoteId) || [];
        const tags = model.tagsByNoteId.get(activeNoteId) || [];
        const linkedCount = model.edges.filter((edge) => edge.sourceId === activeNoteId).length;
        panel.innerHTML = `
            <div class="anchor-obsidian-section">
                <div class="anchor-obsidian-section-head">
                    <span>Backlinks</span>
                    <span class="anchor-obsidian-count">${backlinks.length}</span>
                </div>
                <div class="anchor-obsidian-list">
                    ${backlinks.length ? backlinks.map((item) => `
                        <button class="anchor-obsidian-item" type="button" data-open-local-note-id="${h(item.sourceId)}">
                            <span class="anchor-obsidian-item-title">${h(short(item.sourceTitle || item.sourceId, 36))}</span>
                            <span class="anchor-obsidian-item-meta">${h(short(item.label || item.raw || '', 42))}</span>
                        </button>
                    `).join('') : '<div class="anchor-obsidian-empty">No backlinks yet</div>'}
                </div>
            </div>
            <div class="anchor-obsidian-section">
                <div class="anchor-obsidian-section-head">
                    <span>Missing Links</span>
                    <span class="anchor-obsidian-count">${unresolved.length}</span>
                </div>
                <div class="anchor-obsidian-list">
                    ${unresolved.length ? unresolved.map((item) => `
                        <button class="anchor-obsidian-item is-warning" type="button" data-create-note-ref="${h(item.raw)}">
                            <span class="anchor-obsidian-item-title">${h(short(item.label || item.raw, 36))}</span>
                            <span class="anchor-obsidian-item-meta">Create note</span>
                        </button>
                    `).join('') : '<div class="anchor-obsidian-empty">No unresolved links</div>'}
                </div>
            </div>
            <div class="anchor-obsidian-section">
                <div class="anchor-obsidian-section-head">
                    <span>Tags</span>
                    <span class="anchor-obsidian-count">${tags.length}</span>
                </div>
                <div class="anchor-obsidian-tags">
                    ${tags.length ? tags.map((tag) => `
                        <button class="anchor-obsidian-tag" type="button" data-note-filter="${h(tag.replace(/^#/, ''))}">${h(tag)}</button>
                    `).join('') : '<div class="anchor-obsidian-empty">No tags in current note</div>'}
                </div>
            </div>
            <div class="anchor-obsidian-summary">Linked notes: ${linkedCount} · Vault notes: ${model.notes.length}</div>
        `;
    }

    function switchActiveLocalNote(anchorId, noteId, options = {}) {
        const key = t(anchorId);
        const nextNoteId = t(noteId);
        if (!key || !nextNoteId) {
            return;
        }
        persistActiveLocalNoteFromEditor(key, { retitle: true });
        setActiveLocalNoteId(key, nextNoteId);
        renderLocalNoteCards(key);
        syncEditorFromActiveLocalNote(key);
        renderObsidianKnowledgePanels(key);
        refreshObsidianStatusBar(key);
        if (options.focusEditor !== false) {
            focusEditorInput();
        }
    }

    function createLocalNoteFromWikilink(anchorId, rawWikilink) {
        const key = t(anchorId);
        if (!key) {
            return null;
        }
        const parsed = parseWikilinkToken(rawWikilink);
        if (!parsed) {
            return null;
        }
        const model = getObsidianModel(key);
        const sourceNote = getActiveLocalNote(key);
        const existed = resolveWikilinkTargetForModel(model, sourceNote, parsed);
        if (existed && existed.note) {
            switchActiveLocalNote(key, existed.note.id, { focusEditor: true });
            return existed.note;
        }
        const notes = ensureLocalNotes(key);
        const basePath = normalizeReferencePathCandidate(parsed.pathPart);
        const pathCandidate = basePath
            ? basePath
            : normalizePath(`cards/${safeMarkdownFileName(parsed.alias || parsed.label || `note_${Date.now()}`, `note_${Date.now()}`)}`);
        const normalizedPath = isMarkdown(pathCandidate) ? pathCandidate : `${pathCandidate}.md`;
        const title = parsed.alias || noteNameFromPath(normalizedPath) || `Note ${notes.length + 1}`;
        const content = `# ${title}\n\n`;
        const created = normalizeLocalNoteItem({
            id: `local_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
            title,
            content,
            fileName: normalizedPath,
            updatedAt: Date.now(),
        }, notes.length);
        notes.push(created);
        runtime.localNotesByAnchor.set(key, notes);
        setActiveLocalNoteId(key, created.id);
        writeLocalNotesToStorage(key, notes);
        invalidateObsidianModel(key);
        markAnchorLocalSyncDirty(key);
        renderLocalNoteCards(key);
        syncEditorFromActiveLocalNote(key);
        renderObsidianKnowledgePanels(key);
        return created;
    }

    function closeObsidianGraphModal() {
        const modal = document.getElementById('anchorObsidianGraphModal');
        if (modal) {
            modal.hidden = true;
            modal.setAttribute('aria-hidden', 'true');
            modal.style.display = 'none';
            if (!ENABLE_OBSIDIAN_GRAPH && modal.parentElement) {
                modal.remove();
            }
        }
    }

    function renderObsidianGraphModal(anchorId) {
        if (!ENABLE_OBSIDIAN_GRAPH) {
            closeObsidianGraphModal();
            return;
        }
        const key = t(anchorId);
        const modal = document.getElementById('anchorObsidianGraphModal');
        const canvas = document.getElementById('anchorObsidianGraphCanvas');
        if (!key || !modal || !canvas) {
            return;
        }
        const model = getObsidianModel(key);
        const nodes = model.notes.slice();
        if (!nodes.length) {
            canvas.innerHTML = '<div class="anchor-obsidian-empty">No local notes</div>';
            modal.hidden = false;
            modal.setAttribute('aria-hidden', 'false');
            modal.style.display = 'grid';
            return;
        }
        const width = 640;
        const height = 360;
        const centerX = width / 2;
        const centerY = height / 2;
        const radius = Math.max(96, Math.min(width, height) * 0.36);
        const positions = new Map();
        nodes.forEach((note, index) => {
            const angle = (Math.PI * 2 * index) / nodes.length - (Math.PI / 2);
            const x = centerX + radius * Math.cos(angle);
            const y = centerY + radius * Math.sin(angle);
            positions.set(note.id, { x, y });
        });
        const lines = model.edges
            .map((edge) => {
                const from = positions.get(edge.sourceId);
                const to = positions.get(edge.targetId);
                if (!from || !to) {
                    return '';
                }
                const stroke = edge.isEmbed ? 'rgba(220, 38, 38, 0.42)' : 'rgba(37, 99, 235, 0.34)';
                return `<line x1="${from.x.toFixed(2)}" y1="${from.y.toFixed(2)}" x2="${to.x.toFixed(2)}" y2="${to.y.toFixed(2)}" stroke="${stroke}" stroke-width="${edge.isEmbed ? 2.2 : 1.3}" />`;
            })
            .join('');
        const activeId = getActiveLocalNoteId(key);
        const points = nodes.map((note) => {
            const pos = positions.get(note.id);
            if (!pos) {
                return '';
            }
            const active = note.id === activeId;
            const fill = active ? '#1d4ed8' : '#0f172a';
            const textFill = active ? '#1d4ed8' : '#334155';
            const label = h(short(note.title || noteNameFromPath(note.fileName) || note.id, 18));
            return `
                <g class="anchor-obsidian-graph-node" data-graph-note-id="${h(note.id)}" transform="translate(${pos.x.toFixed(2)} ${pos.y.toFixed(2)})">
                    <circle r="${active ? '10' : '8'}" fill="${fill}" />
                    <text x="0" y="-14" text-anchor="middle" fill="${textFill}" font-size="10">${label}</text>
                </g>
            `;
        }).join('');
        canvas.innerHTML = `
            <svg viewBox="0 0 ${width} ${height}" class="anchor-obsidian-graph-svg" role="img" aria-label="Local note graph">
                <rect x="0" y="0" width="${width}" height="${height}" fill="#f8fafc"></rect>
                ${lines}
                ${points}
            </svg>
        `;
        modal.hidden = false;
        modal.setAttribute('aria-hidden', 'false');
        modal.style.display = 'grid';
    }

    async function importLocalVaultMarkdown(anchorId) {
        const key = t(anchorId);
        if (!key) {
            return;
        }
        if (typeof window.showDirectoryPicker !== 'function') {
            setPreview('This browser does not support local vault import.');
            return;
        }
        try {
            const dirHandle = await window.showDirectoryPicker({ mode: 'read' });
            const imported = [];
            const walk = async (handle, prefix) => {
                for await (const [name, child] of handle.entries()) {
                    const relative = normalizePath(prefix ? `${prefix}/${name}` : name);
                    if (child.kind === 'directory') {
                        await walk(child, relative);
                        continue;
                    }
                    if (!isMarkdown(relative)) {
                        continue;
                    }
                    const file = await child.getFile();
                    const content = await file.text();
                    upsertLocalNoteFromMarkdownFile(key, relative, content);
                    imported.push(relative);
                }
            };
            await walk(dirHandle, '');
            runtime.localVaultSnapshotByAnchor.set(key, imported);
            renderLocalNoteCards(key);
            syncEditorFromActiveLocalNote(key);
            renderObsidianKnowledgePanels(key);
            setPreview(imported.length ? `Imported ${imported.length} markdown files.` : 'No markdown files found in selected folder.');
        } catch (error) {
            setPreview(`Vault import failed: ${t(error && error.message)}`);
        }
    }

    const renderLocalNoteCardsBase = renderLocalNoteCards;
    renderLocalNoteCards = function renderLocalNoteCardsEnhanced(anchorId) {
        const key = t(anchorId);
        const container = document.getElementById('anchorLocalNoteCards');
        if (!container || !key) {
            renderLocalNoteCardsBase(anchorId);
            return;
        }
        const notes = ensureLocalNotes(key);
        const activeId = getActiveLocalNoteId(key);
        const filtered = getFilteredLocalNotes(key, notes);
        const hasFilter = !!getLocalNoteFilter(key);
        const visible = filtered.length
            ? filtered
            : (notes.find((note) => note.id === activeId) ? [notes.find((note) => note.id === activeId)] : []);
        container.innerHTML = `
            <div class="anchor-local-note-tabs">
                ${visible.map((note) => `
                    <button class="anchor-local-note-card${note.id === activeId ? ' is-active' : ''}" type="button" data-local-note-id="${h(note.id)}" title="${h(note.title)}">
                        <span class="anchor-local-note-card-title">${h(short(note.title, 32))}</span>
                        <span class="anchor-local-note-card-delete" data-local-note-delete="${h(note.id)}" role="button" aria-label="Delete note">×</span>
                    </button>
                `).join('')}
                <button class="anchor-local-note-card is-create" type="button" data-local-note-create="1" aria-label="Create note">+</button>
            </div>
            ${hasFilter && !filtered.length ? '<div class="anchor-local-note-empty">No matched note</div>' : ''}
        `;
    };

    const ensureObsidianNoteUiBase = ensureObsidianNoteUi;
    ensureObsidianNoteUi = function ensureObsidianNoteUiEnhanced() {
        ensureObsidianNoteUiBase();
        const composerShell = document.getElementById('anchorComposerShell');
        const quickNote = composerShell
            ? (composerShell.querySelector('.anchor-quick-note') || composerShell)
            : null;
        let composer = quickNote && quickNote.querySelector
            ? quickNote.querySelector('.anchor-quick-note-composer')
            : null;
        if (!quickNote) {
            return;
        }
        if (!composer) {
            composer = document.createElement('div');
            composer.className = 'anchor-quick-note-composer';
            const textarea = document.getElementById('anchorQuickNoteInput');
            const preview = document.getElementById('anchorPreview');
            const fileList = document.getElementById('anchorFileList');
            const footer = quickNote.querySelector('.obsidian-footer-actions');
            quickNote.insertBefore(composer, footer || quickNote.firstChild || null);
            if (textarea && textarea.parentElement !== composer) {
                composer.appendChild(textarea);
            }
            if (preview && preview.parentElement !== composer) {
                composer.appendChild(preview);
            }
            if (fileList && fileList.parentElement !== composer) {
                composer.appendChild(fileList);
            }
        }
        let editorColumns = document.getElementById('anchorObsidianEditorColumns');
        if (!editorColumns && composer) {
            editorColumns = document.createElement('div');
            editorColumns.id = 'anchorObsidianEditorColumns';
            editorColumns.className = 'anchor-obsidian-editor-columns';
            const hint = quickNote.querySelector('.anchor-quick-note-hint');
            quickNote.insertBefore(editorColumns, hint || null);
            editorColumns.appendChild(composer);
        } else if (editorColumns && composer && composer.parentElement !== editorColumns) {
            editorColumns.insertBefore(composer, editorColumns.firstChild || null);
        }
        if (editorColumns && !document.getElementById('anchorLocalNoteLivePreview')) {
            const livePreview = document.createElement('div');
            livePreview.id = 'anchorLocalNoteLivePreview';
            livePreview.className = 'anchor-obsidian-live-preview';
            livePreview.innerHTML = '<div class="anchor-obsidian-empty">Select an anchor to edit local markdown</div>';
            editorColumns.appendChild(livePreview);
        }
        if (!document.getElementById('anchorWikilinkSuggest')) {
            const suggest = document.createElement('div');
            suggest.id = 'anchorWikilinkSuggest';
            suggest.className = 'anchor-obsidian-wikilink-suggest';
            suggest.hidden = true;
            const hint = quickNote.querySelector('.anchor-quick-note-hint');
            quickNote.insertBefore(suggest, hint || null);
        }
        if (!document.getElementById('anchorObsidianToolbar')) {
            const toolbar = document.createElement('div');
            toolbar.id = 'anchorObsidianToolbar';
            toolbar.className = 'anchor-obsidian-toolbar';
            toolbar.innerHTML = `
                <input id="anchorLocalNoteSearchInput" type="text" placeholder="Search local notes / tags / text" aria-label="Search local notes">
            `;
            quickNote.insertBefore(toolbar, document.getElementById('anchorObsidianEditorColumns') || composer || null);
        }
        if (!document.getElementById('anchorObsidianPanels')) {
            const panels = document.createElement('div');
            panels.id = 'anchorObsidianPanels';
            panels.className = 'anchor-obsidian-panels';
            quickNote.insertBefore(panels, document.getElementById('anchorObsidianEditorColumns') || composer || null);
        }
        const localCards = document.getElementById('anchorLocalNoteCards');
        const toolbarNode = document.getElementById('anchorObsidianToolbar');
        const panelsNode = document.getElementById('anchorObsidianPanels');
        if (localCards && localCards.parentElement !== quickNote) {
            quickNote.insertBefore(localCards, quickNote.firstChild || null);
        }
        if (!document.getElementById('anchorObsidianHeader')) {
            const header = document.createElement('div');
            header.id = 'anchorObsidianHeader';
            header.className = 'anchor-obsidian-header';
            header.innerHTML = `
                <div class="anchor-obsidian-header-main">
                </div>
                <div class="anchor-obsidian-header-actions">
                    <button class="btn btn-ghost-icon" type="button" data-obsidian-action="toggle-fullscreen" title="Enter fullscreen" aria-label="Enter fullscreen" aria-pressed="false">&#x26F6;</button>
                    <button class="btn btn-ghost-icon" type="button" data-obsidian-action="toggle-command" title="Open command menu">...</button>
                </div>
            `;
            quickNote.insertBefore(header, toolbarNode || quickNote.firstChild || null);
        }
        let workbench = document.getElementById('anchorObsidianWorkbench');
        if (!workbench) {
            workbench = document.createElement('div');
            workbench.id = 'anchorObsidianWorkbench';
            workbench.className = 'anchor-obsidian-workbench';
            const hintNode = quickNote.querySelector('.anchor-quick-note-hint');
            quickNote.insertBefore(workbench, hintNode || null);
        }
        let contextPane = document.getElementById('anchorObsidianContextPane');
        if (!contextPane) {
            contextPane = document.createElement('aside');
            contextPane.id = 'anchorObsidianContextPane';
            contextPane.className = 'anchor-obsidian-context-pane';
        }
        if (editorColumns && editorColumns.parentElement !== workbench) {
            workbench.insertBefore(editorColumns, workbench.firstChild || null);
        }
        if (panelsNode && panelsNode.parentElement !== contextPane) {
            contextPane.appendChild(panelsNode);
        }
        if (contextPane.parentElement !== workbench) {
            workbench.appendChild(contextPane);
        }
        if (!document.getElementById('anchorObsidianStatusBar')) {
            const statusBar = document.createElement('div');
            statusBar.id = 'anchorObsidianStatusBar';
            statusBar.className = 'anchor-obsidian-statusbar';
            quickNote.appendChild(statusBar);
        }
        if (!document.getElementById('anchorObsidianCommandMenu')) {
            const commandMenu = document.createElement('div');
            commandMenu.id = 'anchorObsidianCommandMenu';
            commandMenu.className = 'anchor-obsidian-command-menu';
            commandMenu.hidden = true;
            quickNote.appendChild(commandMenu);
        }
        const commandMenuNode = document.getElementById('anchorObsidianCommandMenu');
        if (commandMenuNode && !commandMenuNode.dataset.ready) {
            commandMenuNode.hidden = true;
            commandMenuNode.dataset.ready = '1';
            renderObsidianSettingsPanel();
        }
        if (composer && !document.getElementById('anchorQuickNoteVditor')) {
            const textarea = document.getElementById('anchorQuickNoteInput');
            const vditorHost = document.createElement('div');
            vditorHost.id = 'anchorQuickNoteVditor';
            vditorHost.className = 'anchor-obsidian-vditor-host';
            vditorHost.hidden = true;
            if (textarea && textarea.parentElement === composer) {
                composer.insertBefore(vditorHost, textarea);
            } else {
                composer.appendChild(vditorHost);
            }
        }
        if (!document.getElementById('anchorObsidianUiEnhancedStyle')) {
            const style = document.createElement('style');
            style.id = 'anchorObsidianUiEnhancedStyle';
            style.textContent = `
                #anchorMountPanel {
                    --obs-font-size: 15px;
                    --obs-line-height: 1.72;
                    --obs-max-width-ch: 78ch;
                    --obs-page-padding: 18px;
                    --obs-paragraph-gap: 0.88em;
                    --obs-list-indent: 1.25;
                    --obs-font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Inter, 'Helvetica Neue', Arial, sans-serif;
                    --obs-code-font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, 'Liberation Mono', monospace;
                    --obs-bg: #f8fafc;
                    --obs-surface: #ffffff;
                    --obs-text: #0f172a;
                    --obs-muted: #64748b;
                    --obs-border: rgba(148,163,184,.32);
                    --obs-link: #3f4fae;
                    background: var(--obs-bg);
                    color: var(--obs-text);
                }
                #anchorMountPanel[data-obs-theme="dark"] {
                    --obs-bg: #0f172a;
                    --obs-surface: #111827;
                    --obs-text: #e5e7eb;
                    --obs-muted: #94a3b8;
                    --obs-border: rgba(71,85,105,.55);
                    --obs-link: #8ea0ff;
                }
                @media (prefers-color-scheme: dark) {
                    #anchorMountPanel[data-obs-theme="auto"] {
                        --obs-bg: #0f172a;
                        --obs-surface: #111827;
                        --obs-text: #e5e7eb;
                        --obs-muted: #94a3b8;
                        --obs-border: rgba(71,85,105,.55);
                        --obs-link: #8ea0ff;
                    }
                }
                .anchor-obsidian-toolbar {
                    display: none !important;
                    grid-template-columns: minmax(0, 1fr);
                    gap: 6px;
                    margin-bottom: 8px;
                }
                .anchor-obsidian-toolbar input {
                    border: 1px solid rgba(100,116,139,.38);
                    border-radius: 8px;
                    min-height: 34px;
                    padding: 0 10px;
                    font-size: 12px;
                    outline: none;
                }
                .anchor-obsidian-toolbar input:focus {
                    border-color: #1d4ed8;
                    box-shadow: 0 0 0 1px rgba(29,78,216,.24);
                }
                .anchor-obsidian-header {
                    display: flex;
                    align-items: center;
                    justify-content: flex-end;
                    gap: 4px;
                    padding: 0;
                    border: 0;
                    border-radius: 0;
                    background: transparent;
                    margin-bottom: 0;
                    position: absolute;
                    top: 0;
                    right: 0;
                    z-index: 4;
                    pointer-events: none;
                }
                .anchor-obsidian-header-main {
                    display: none !important;
                }
                .anchor-obsidian-header-title {
                    font-size: 12px;
                    font-weight: 700;
                    color: #0f172a;
                    line-height: 1.3;
                }
                .anchor-obsidian-header-subtitle {
                    font-size: 10px;
                    color: #64748b;
                    line-height: 1.25;
                    letter-spacing: .02em;
                }
                .anchor-obsidian-header-actions {
                    display: inline-flex;
                    align-items: center;
                    gap: 4px;
                    pointer-events: auto;
                }
                .anchor-obsidian-header-actions .btn {
                    min-width: 34px;
                    min-height: 30px;
                    border-radius: 8px;
                    font-size: 11px;
                    padding: 0 10px;
                    letter-spacing: .06em;
                    pointer-events: auto;
                    border: 0 !important;
                    box-shadow: none !important;
                    background: transparent !important;
                }
                .anchor-obsidian-workbench {
                    display: grid;
                    grid-template-columns: minmax(0, 1fr);
                    gap: 0;
                    align-items: start;
                    margin-bottom: 0;
                }
                .anchor-obsidian-context-pane {
                    min-width: 0;
                    display: none !important;
                }
                .anchor-quick-note.is-obsidian-context-collapsed .anchor-obsidian-workbench {
                    grid-template-columns: minmax(0, 1fr);
                }
                .anchor-quick-note.is-obsidian-context-collapsed .anchor-obsidian-context-pane {
                    display: none;
                }
                .anchor-quick-note:not(.is-obsidian-context-collapsed) .anchor-obsidian-context-pane {
                    display: block;
                }
                .anchor-mount-panel.is-obsidian-focus .anchor-panel-subtitle,
                .anchor-mount-panel.is-obsidian-focus .anchor-context,
                .anchor-mount-panel.is-obsidian-focus #anchorMainNoteWrap,
                .anchor-mount-panel.is-obsidian-focus #anchorFileList,
                .anchor-mount-panel.is-obsidian-focus .anchor-quick-note-hint {
                    display: none !important;
                }
                .anchor-mount-panel.is-obsidian-fullscreen {
                    position: fixed !important;
                    inset: 0 !important;
                    z-index: 80 !important;
                    width: 100vw !important;
                    max-width: 100vw !important;
                    height: 100vh !important;
                    max-height: 100vh !important;
                    border-radius: 0 !important;
                    border: 0 !important;
                    margin: 0 !important;
                    padding: 10px 12px !important;
                    background: #f8fafc !important;
                    overflow: hidden;
                }
                body.anchor-obsidian-fullscreen-open {
                    overflow: hidden;
                }
                .anchor-mount-panel.is-obsidian-fullscreen .anchor-panel-head {
                    position: sticky;
                    top: 0;
                    z-index: 2;
                    background: linear-gradient(180deg,#f8fafc 0%, rgba(248,250,252,.92) 100%);
                    padding-bottom: 6px;
                }
                .anchor-mount-panel.is-obsidian-fullscreen .anchor-composer-shell {
                    display: grid;
                    grid-template-rows: minmax(0, 1fr);
                    min-height: 0;
                    height: calc(100vh - 132px);
                    border-radius: 12px;
                    padding: 8px;
                    background: #ffffff;
                }
                .anchor-mount-panel.is-obsidian-fullscreen .anchor-quick-note {
                    display: grid;
                    grid-template-rows: auto auto minmax(0, 1fr) auto;
                    min-height: 0;
                }
                .anchor-mount-panel.is-obsidian-fullscreen .anchor-obsidian-workbench {
                    min-height: 0;
                    height: 100%;
                }
                .anchor-mount-panel.is-obsidian-fullscreen .anchor-obsidian-editor-columns {
                    min-height: 0;
                    height: 100%;
                    grid-template-columns: 1fr;
                }
                .anchor-mount-panel.is-obsidian-fullscreen .anchor-quick-note-composer {
                    min-height: 0;
                    height: 100%;
                    align-items: stretch;
                }
                .anchor-mount-panel.is-obsidian-fullscreen .anchor-obsidian-editor-columns .anchor-quick-note-composer {
                    height: 100%;
                }
                .anchor-mount-panel.is-obsidian-fullscreen #anchorQuickNoteInput,
                .anchor-mount-panel.is-obsidian-fullscreen #anchorQuickNoteVditor,
                .anchor-mount-panel.is-obsidian-fullscreen .anchor-obsidian-vditor-host {
                    min-height: 0;
                    height: 100%;
                }
                .anchor-mount-panel.is-obsidian-fullscreen #anchorMainNoteWrap,
                .anchor-mount-panel.is-obsidian-fullscreen #anchorFileList {
                    display: none !important;
                }
                .anchor-mount-panel.is-obsidian-fullscreen .anchor-preview {
                    display: none !important;
                }
                .anchor-mount-panel.is-obsidian-fullscreen .anchor-dropzone {
                    z-index: 20;
                }
                .anchor-obsidian-command-menu {
                    position: absolute;
                    right: 14px;
                    top: 54px;
                    z-index: 8;
                    border: 1px solid rgba(148,163,184,.35);
                    border-radius: 10px;
                    background: #fff;
                    box-shadow: 0 14px 28px rgba(15,23,42,.18);
                    padding: 6px;
                    display: grid;
                    gap: 4px;
                    min-width: 184px;
                }
                .anchor-obsidian-command-menu[hidden] {
                    display: none !important;
                }
                .anchor-obsidian-command-menu button {
                    border: 1px solid rgba(148,163,184,.3);
                    border-radius: 8px;
                    background: #fff;
                    color: #0f172a;
                    text-align: left;
                    font-size: 12px;
                    line-height: 1.35;
                    padding: 6px 8px;
                    cursor: pointer;
                }
                .anchor-obsidian-command-menu button:hover {
                    border-color: rgba(37,99,235,.44);
                    background: #eff6ff;
                }
                .anchor-obsidian-settings-panel {
                    display: grid;
                    gap: 8px;
                }
                .anchor-obsidian-settings-toggle {
                    width: 100%;
                    display: flex;
                    align-items: center;
                    justify-content: space-between;
                    gap: 8px;
                    border: 1px solid var(--obs-border);
                    border-radius: 9px;
                    padding: 6px 8px;
                    background: var(--obs-surface);
                    color: var(--obs-text);
                    cursor: pointer;
                    font-size: 12px;
                    font-weight: 700;
                    text-align: left;
                }
                .anchor-obsidian-settings-toggle:hover {
                    border-color: rgba(37,99,235,.44);
                    background: rgba(59,130,246,.08);
                }
                .anchor-obsidian-settings-toggle-icon {
                    font-size: 12px;
                    line-height: 1;
                    color: var(--obs-muted);
                }
                .anchor-obsidian-settings-head {
                    font-size: 12px;
                    font-weight: 700;
                    color: var(--obs-text);
                    padding: 2px 4px;
                }
                .anchor-obsidian-settings-body {
                    display: grid;
                    gap: 8px;
                }
                .anchor-obsidian-settings-body[hidden] {
                    display: none !important;
                }
                .anchor-obsidian-settings-row {
                    display: grid;
                    gap: 5px;
                    border: 1px solid var(--obs-border);
                    border-radius: 9px;
                    padding: 6px 8px;
                    background: var(--obs-surface);
                    color: var(--obs-text);
                }
                .anchor-obsidian-settings-row > span {
                    font-size: 11px;
                    color: var(--obs-muted);
                    display: flex;
                    align-items: center;
                    justify-content: space-between;
                    gap: 6px;
                }
                .anchor-obsidian-settings-row > span b {
                    color: var(--obs-text);
                    font-weight: 700;
                    font-size: 11px;
                }
                .anchor-obsidian-settings-row input[type="range"] {
                    width: 100%;
                }
                .anchor-obsidian-settings-row select {
                    width: 100%;
                    min-height: 30px;
                    border-radius: 8px;
                    border: 1px solid var(--obs-border);
                    background: var(--obs-surface);
                    color: var(--obs-text);
                    font-size: 12px;
                    padding: 0 8px;
                    outline: none;
                }
                .anchor-obsidian-settings-actions {
                    display: flex;
                    justify-content: flex-end;
                    gap: 6px;
                    padding-top: 2px;
                }
                .anchor-obsidian-vditor-host {
                    border: 1px solid var(--obs-border);
                    border-radius: 10px;
                    overflow: hidden;
                    background: var(--obs-surface);
                    min-height: 220px;
                }
                .anchor-obsidian-vditor-host[hidden] {
                    display: none !important;
                }
                .anchor-obsidian-vditor-host .vditor {
                    border: 0 !important;
                    height: 100%;
                }
                .anchor-obsidian-vditor-host .vditor-toolbar {
                    border-bottom: 1px solid rgba(148,163,184,.25);
                }
                .anchor-obsidian-vditor-host .vditor-reset,
                .anchor-obsidian-vditor-host .vditor-content {
                    min-height: 0;
                }
                #anchorMountPanel .anchor-obsidian-vditor-host .vditor-reset,
                #anchorMountPanel .anchor-obsidian-vditor-host .vditor-ir,
                #anchorMountPanel .anchor-obsidian-vditor-host .vditor-wysiwyg,
                #anchorMountPanel .anchor-obsidian-live-preview,
                #anchorMountPanel #anchorQuickNoteInput {
                    font-family: var(--obs-font-family);
                    font-size: var(--obs-font-size);
                    line-height: var(--obs-line-height);
                    color: var(--obs-text);
                }
                #anchorMountPanel .anchor-obsidian-vditor-host .vditor-reset {
                    max-width: var(--obs-max-width-ch);
                    margin: 0 auto;
                    padding-left: var(--obs-page-padding) !important;
                    padding-right: var(--obs-page-padding) !important;
                }
                #anchorMountPanel .anchor-obsidian-vditor-host .vditor-content a,
                #anchorMountPanel .anchor-obsidian-live-preview a {
                    color: var(--obs-link);
                }
                #anchorMountPanel #anchorQuickNoteInput {
                    width: min(100%, calc(var(--obs-max-width-ch) + var(--obs-page-padding) * 2));
                    margin: 0 auto;
                    padding-left: var(--obs-page-padding);
                    padding-right: var(--obs-page-padding);
                }
                #anchorMountPanel .anchor-obsidian-live-preview {
                    background: var(--obs-surface);
                    border-color: var(--obs-border);
                    padding: var(--obs-page-padding);
                }
                #anchorMountPanel .anchor-obsidian-live-preview :is(p,ul,ol,blockquote,pre,table,h1,h2,h3,h4,h5,h6) {
                    max-width: var(--obs-max-width-ch);
                    margin-left: auto;
                    margin-right: auto;
                }
                #anchorMountPanel .anchor-obsidian-live-preview p {
                    margin-top: 0;
                    margin-bottom: var(--obs-paragraph-gap);
                }
                #anchorMountPanel .anchor-obsidian-live-preview ul,
                #anchorMountPanel .anchor-obsidian-live-preview ol,
                #anchorMountPanel .anchor-obsidian-vditor-host .vditor-reset ul,
                #anchorMountPanel .anchor-obsidian-vditor-host .vditor-reset ol {
                    padding-inline-start: calc(var(--obs-list-indent) * 1em);
                }
                #anchorMountPanel .anchor-obsidian-live-preview pre,
                #anchorMountPanel .anchor-obsidian-live-preview code,
                #anchorMountPanel .anchor-obsidian-vditor-host code,
                #anchorMountPanel .anchor-obsidian-vditor-host pre {
                    font-family: var(--obs-code-font-family);
                }
                .anchor-obsidian-hover-card {
                    position: fixed;
                    z-index: 120;
                    width: min(560px, calc(100vw - 24px));
                    max-height: min(74vh, 680px);
                    border: 1px solid var(--obs-border);
                    border-radius: 12px;
                    background: var(--obs-surface);
                    box-shadow: 0 18px 42px rgba(15,23,42,.22);
                    overflow: hidden;
                }
                .anchor-obsidian-hover-card[hidden] {
                    display: none !important;
                }
                .anchor-obsidian-hover-card-head {
                    padding: 10px 12px 8px;
                    border-bottom: 1px solid var(--obs-border);
                    background: var(--obs-surface);
                }
                .anchor-obsidian-hover-card-title {
                    font-size: 13px;
                    line-height: 1.35;
                    font-weight: 700;
                    color: var(--obs-text);
                }
                .anchor-obsidian-hover-card-meta {
                    margin-top: 3px;
                    font-size: 11px;
                    line-height: 1.35;
                    color: var(--obs-muted);
                    word-break: break-all;
                }
                .anchor-obsidian-hover-card-body {
                    max-height: min(66vh, 610px);
                    overflow: auto;
                    padding: 12px;
                    color: var(--obs-text);
                }
                .anchor-obsidian-hover-card-body :is(p,ul,ol,blockquote,pre,table,h1,h2,h3,h4,h5,h6) {
                    max-width: var(--obs-max-width-ch);
                    margin-left: auto;
                    margin-right: auto;
                }
                .anchor-obsidian-statusbar {
                    display: none !important;
                }
                .anchor-obsidian-status-item {
                    border-radius: 999px;
                    border: 1px solid rgba(148,163,184,.32);
                    background: #f8fafc;
                    padding: 3px 8px;
                    color: #334155;
                }
                #anchorMountPanel .obsidian-panel-header,
                #anchorMountPanel #panelContext,
                #anchorMountPanel #panelInbox,
                #anchorMountPanel .obsidian-footer-actions,
                #anchorMountPanel #anchorMainNoteWrap {
                    display: none !important;
                }
                #anchorMountPanel .obsidian-panel-content {
                    display: flex !important;
                    flex-direction: column;
                    height: 100% !important;
                    min-height: 0;
                }
                #anchorMountPanel #panelEditor {
                    display: flex !important;
                    flex-direction: column;
                    flex: 1 1 auto;
                    height: 100% !important;
                    min-height: 0;
                    overflow: hidden;
                }
                #anchorMountPanel .anchor-composer-shell {
                    display: flex !important;
                    flex-direction: column;
                    flex: 1 1 auto;
                    margin-top: 0 !important;
                    padding: 0 !important;
                    border: 0 !important;
                    gap: 0 !important;
                    min-height: 0;
                }
                #anchorMountPanel:not(.is-obsidian-fullscreen) {
                    padding: 8px !important;
                }
                #anchorMountPanel:not(.is-obsidian-fullscreen) .obsidian-panel-content,
                #anchorMountPanel:not(.is-obsidian-fullscreen) #panelEditor {
                    margin: 0 !important;
                    padding: 0 !important;
                }
                #anchorMountPanel .anchor-quick-note {
                    display: flex;
                    flex-direction: column;
                    flex: 1 1 auto;
                    gap: 0;
                    min-height: 0;
                    height: 100%;
                    position: relative;
                }
                #anchorMountPanel #anchorLocalNoteCards {
                    flex: 0 0 auto;
                    margin: 0 !important;
                    padding-bottom: 0 !important;
                    border: 0 !important;
                    box-shadow: none !important;
                    background: transparent !important;
                }
                #anchorMountPanel .anchor-local-note-tabs {
                    margin: 0 !important;
                }
                #anchorMountPanel .anchor-local-note-card {
                    margin-bottom: 0 !important;
                }
                #anchorMountPanel #anchorObsidianWorkbench {
                    flex: 1 1 auto;
                    min-height: 0;
                    margin-top: 0 !important;
                    padding-top: 0 !important;
                    gap: 0 !important;
                }
                #anchorMountPanel .anchor-obsidian-workbench,
                #anchorMountPanel .anchor-obsidian-editor-columns,
                #anchorMountPanel .anchor-quick-note-composer {
                    min-height: 0;
                    height: 100%;
                }
                #anchorMountPanel #anchorObsidianEditorColumns {
                    flex: 1 1 auto;
                    gap: 0 !important;
                    margin-top: 0 !important;
                }
                #anchorMountPanel #anchorQuickNoteVditor,
                #anchorMountPanel .anchor-obsidian-vditor-host,
                #anchorMountPanel .anchor-obsidian-vditor-host .vditor {
                    height: 100% !important;
                }
                .anchor-local-note-empty {
                    width: 100%;
                    border: 1px dashed rgba(148,163,184,.5);
                    border-radius: 8px;
                    padding: 8px;
                    color: #64748b;
                    font-size: 12px;
                    background: #f8fafc;
                }
                .anchor-obsidian-panels {
                    display: grid;
                    grid-template-columns: 1fr;
                    gap: 8px;
                    margin: 0;
                }
                .anchor-obsidian-section {
                    border: 1px solid rgba(148,163,184,.35);
                    border-radius: 10px;
                    background: #f8fafc;
                    padding: 8px;
                    display: grid;
                    gap: 6px;
                }
                .anchor-obsidian-section-head {
                    display: flex;
                    align-items: center;
                    justify-content: space-between;
                    color: #334155;
                    font-size: 12px;
                    font-weight: 700;
                }
                .anchor-obsidian-count {
                    font-size: 11px;
                    color: #64748b;
                    font-weight: 600;
                }
                .anchor-obsidian-list {
                    display: grid;
                    gap: 6px;
                }
                .anchor-obsidian-item {
                    border: 1px solid rgba(148,163,184,.36);
                    border-radius: 8px;
                    background: #fff;
                    text-align: left;
                    padding: 6px 8px;
                    display: grid;
                    gap: 2px;
                    color: #0f172a;
                    cursor: pointer;
                }
                .anchor-obsidian-item.is-warning {
                    border-style: dashed;
                    border-color: rgba(217,119,6,.55);
                    background: #fffbeb;
                }
                .anchor-obsidian-item-title {
                    font-size: 12px;
                    font-weight: 600;
                    line-height: 1.35;
                }
                .anchor-obsidian-item-meta {
                    font-size: 11px;
                    color: #64748b;
                    line-height: 1.35;
                }
                .anchor-obsidian-tags {
                    display: flex;
                    flex-wrap: wrap;
                    gap: 6px;
                }
                .anchor-obsidian-tag {
                    border: 1px solid rgba(30,64,175,.35);
                    background: #eff6ff;
                    color: #1e3a8a;
                    border-radius: 999px;
                    font-size: 11px;
                    line-height: 1;
                    padding: 5px 8px;
                    cursor: pointer;
                }
                .anchor-obsidian-empty {
                    font-size: 11px;
                    color: #64748b;
                }
                .anchor-obsidian-summary {
                    font-size: 11px;
                    color: #64748b;
                    grid-column: 1 / -1;
                }
                .anchor-obsidian-editor-columns {
                    display: grid;
                    grid-template-columns: minmax(0, 1fr);
                    gap: 0;
                    align-items: stretch;
                }
                .anchor-obsidian-editor-columns.is-vditor-ready[data-layout="edit"] .anchor-obsidian-live-preview {
                    display: none;
                }
                .anchor-obsidian-editor-columns[data-layout="preview"] .anchor-quick-note-composer {
                    display: none;
                }
                .anchor-obsidian-live-preview {
                    display: block;
                    border: 1px solid rgba(148,163,184,.35);
                    border-radius: 10px;
                    background: #fff;
                    padding: 8px 10px;
                    font-size: 12px;
                    line-height: 1.55;
                    color: #0f172a;
                    overflow: auto;
                    max-height: 220px;
                    min-height: 110px;
                }
                .anchor-obsidian-live-preview p {
                    margin: 0 0 6px;
                }
                .anchor-obsidian-live-preview :is(h1,h2,h3,h4,h5,h6) {
                    margin: 0 0 6px;
                }
                .anchor-obsidian-live-preview code {
                    font-family: Consolas, 'Courier New', monospace;
                    font-size: 11px;
                }
                .anchor-obsidian-live-preview pre {
                    margin: 0 0 8px;
                    white-space: pre-wrap;
                    word-break: break-word;
                }
                .anchor-obsidian-wikilink-suggest {
                    position: fixed;
                    z-index: 126;
                    width: min(420px, calc(100vw - 24px));
                    max-height: min(56vh, 460px);
                    overflow: auto;
                    border: 1px solid rgba(148,163,184,.42);
                    border-radius: 10px;
                    background: #ffffff;
                    box-shadow: 0 10px 26px rgba(15, 23, 42, 0.12);
                    padding: 7px;
                    display: grid;
                    gap: 6px;
                }
                .anchor-obsidian-wikilink-head {
                    font-size: 11px;
                    font-weight: 700;
                    color: #334155;
                    letter-spacing: .02em;
                }
                .anchor-obsidian-wikilink-query {
                    width: 100%;
                    min-height: 30px;
                    border: 1px solid rgba(148,163,184,.42);
                    border-radius: 8px;
                    background: #fff;
                    color: #0f172a;
                    font-size: 12px;
                    padding: 0 8px;
                    outline: none;
                }
                .anchor-obsidian-wikilink-query:focus {
                    border-color: rgba(29,78,216,.62);
                    box-shadow: 0 0 0 1px rgba(29,78,216,.18);
                }
                .anchor-obsidian-wikilink-list {
                    display: grid;
                    gap: 4px;
                }
                .anchor-obsidian-wikilink-item {
                    border: 1px solid rgba(148,163,184,.28);
                    border-radius: 8px;
                    background: #fff;
                    color: #0f172a;
                    text-align: left;
                    padding: 6px 8px;
                    cursor: pointer;
                    display: grid;
                    gap: 2px;
                }
                .anchor-obsidian-wikilink-item.is-active {
                    border-color: rgba(29,78,216,.52);
                    box-shadow: 0 0 0 1px rgba(29,78,216,.16);
                    background: #eff6ff;
                }
                .anchor-obsidian-wikilink-title {
                    font-size: 12px;
                    line-height: 1.3;
                }
                .anchor-obsidian-wikilink-meta {
                    font-size: 10px;
                    color: #64748b;
                    line-height: 1.35;
                    display: flex;
                    align-items: center;
                    justify-content: space-between;
                    gap: 8px;
                }
                .anchor-obsidian-wikilink-badge {
                    border: 1px solid rgba(59,130,246,.34);
                    background: rgba(219,234,254,.72);
                    color: #1e3a8a;
                    border-radius: 999px;
                    padding: 1px 6px;
                    font-size: 10px;
                    line-height: 1.2;
                    white-space: nowrap;
                }
                @media (max-width: 1180px) {
                    .anchor-obsidian-toolbar {
                        grid-template-columns: minmax(0, 1fr);
                    }
                    .anchor-obsidian-workbench {
                        grid-template-columns: minmax(0, 1fr);
                    }
                }
                @media (max-width: 960px) {
                    .anchor-obsidian-editor-columns {
                        grid-template-columns: 1fr;
                    }
                    .anchor-obsidian-live-preview {
                        max-height: 180px;
                    }
                }
                @media (max-width: 760px) {
                    .anchor-obsidian-header {
                        flex-direction: column;
                        align-items: stretch;
                    }
                    .anchor-obsidian-header-actions {
                        justify-content: flex-end;
                    }
                }
                .anchor-obsidian-graph-modal {
                    position: fixed;
                    inset: 0;
                    background: rgba(2, 6, 23, 0.56);
                    display: grid;
                    place-items: center;
                    z-index: 46;
                    padding: 16px;
                }
                .anchor-obsidian-graph-modal[hidden] {
                    display: none !important;
                }
                .anchor-obsidian-graph-dialog {
                    width: min(720px, 96vw);
                    border-radius: 14px;
                    background: #fff;
                    border: 1px solid rgba(148,163,184,.35);
                    box-shadow: 0 20px 48px rgba(15, 23, 42, 0.28);
                    display: grid;
                    gap: 8px;
                    padding: 10px;
                }
                .anchor-obsidian-graph-head {
                    display: flex;
                    align-items: center;
                    justify-content: space-between;
                    gap: 8px;
                }
                .anchor-obsidian-graph-head-title {
                    font-size: 13px;
                    color: #0f172a;
                    font-weight: 700;
                }
                .anchor-obsidian-graph-svg {
                    width: 100%;
                    max-height: min(58vh, 420px);
                    border-radius: 10px;
                    border: 1px solid rgba(148,163,184,.25);
                    background: #f8fafc;
                }
                .anchor-obsidian-graph-node {
                    cursor: pointer;
                }
            `;
            document.head.appendChild(style);
        }
        if (ENABLE_OBSIDIAN_GRAPH && !document.getElementById('anchorObsidianGraphModal')) {
            const modal = document.createElement('div');
            modal.id = 'anchorObsidianGraphModal';
            modal.className = 'anchor-obsidian-graph-modal';
            modal.hidden = true;
            modal.setAttribute('aria-hidden', 'true');
            modal.style.display = 'none';
            modal.innerHTML = `
                <div class="anchor-obsidian-graph-dialog">
                    <div class="anchor-obsidian-graph-head">
                        <div class="anchor-obsidian-graph-head-title">Local Note Graph</div>
                        <button class="btn" type="button" id="anchorObsidianGraphCloseBtn">Close</button>
                    </div>
                    <div id="anchorObsidianGraphCanvas"></div>
                </div>
            `;
            document.body.appendChild(modal);
        }
        const ensuredModal = document.getElementById('anchorObsidianGraphModal');
        if (ensuredModal && !ENABLE_OBSIDIAN_GRAPH) {
            ensuredModal.remove();
        } else if (ensuredModal) {
            ensuredModal.hidden = true;
            ensuredModal.setAttribute('aria-hidden', 'true');
            ensuredModal.style.display = 'none';
        }
        setObsidianWorkspaceState({
            contextCollapsed: runtime.obsidianContextCollapsed,
            focusMode: runtime.obsidianFocusMode,
        });
        applyLocalEditorLayoutMode(runtime.activeId);
        renderLocalNoteLivePreview(runtime.activeId);
        refreshObsidianStatusBar(runtime.activeId);
        ensureVditorEditor(runtime.activeId).then((enabled) => {
            if (!enabled) {
                return;
            }
            renderLocalNoteLivePreview(runtime.activeId);
            refreshObsidianStatusBar(runtime.activeId);
        }).catch((_error) => {
        });
        ensureReaderThemeSettingsApplied();
        exposeReaderThemeApi();
        refreshFullscreenToggleButton();
        ensurePhase2bFloatingUi();
        renderPhase2bFloatingUi();
    };

    function parseMetaKey(key) {
        const raw = t(key);
        const last = raw.lastIndexOf('::');
        if (last <= 0 || last >= raw.length - 2) return null;
        const mid = raw.lastIndexOf('::', last - 1);
        if (mid <= 0 || mid >= last - 2) return null;
        const blockId = raw.slice(0, mid).trim();
        const start = Number.parseInt(raw.slice(mid + 2, last), 10);
        const end = Number.parseInt(raw.slice(last + 2), 10);
        if (!blockId || !Number.isInteger(start) || !Number.isInteger(end) || start < 0 || end <= start) return null;
        return { blockId, start, end };
    }

    function getCtx() {
        const api = window.__mobileTaskContextApi || null;
        const taskId = t(api && typeof api.getTaskId === 'function' ? api.getTaskId() : '');
        const pathHint = normalizePath(api && typeof api.getPathHint === 'function' ? api.getPathHint() : '');
        const apiBaseRaw = t(api && typeof api.getApiBase === 'function' ? api.getApiBase() : '/api/mobile');
        return { taskId, pathHint, apiBase: apiBaseRaw ? apiBaseRaw.replace(/\/+$/, '') : '/api/mobile' };
    }

    async function parseResp(resp) {
        const raw = await resp.text();
        let body = {};
        try {
            body = raw ? JSON.parse(raw) : {};
        } catch (_e) {
            body = {};
        }
        const ok = !Object.prototype.hasOwnProperty.call(body, 'success') || body.success !== false;
        if (!resp.ok || !ok) {
            throw new Error(t(body.message) || `HTTP ${resp.status}`);
        }
        return body;
    }

    function sanitizeAnchor(anchorId, raw) {
        const parsed = parseMetaKey(anchorId);
        const source = raw && typeof raw === 'object' ? raw : {};
        const revisions = Array.isArray(source.revisions) ? source.revisions : [];
        const lastRevision = revisions.length > 0 ? revisions[revisions.length - 1] : null;
        const mountedPath = normalizePath(source.mountedPath || (lastRevision && lastRevision.notePath) || '');
        const statusRaw = t(source.status).toLowerCase();
        const status = statusRaw === 'mounted' || statusRaw === 'files_uploaded' || statusRaw === 'pending'
            ? statusRaw
            : (mountedPath ? 'mounted' : 'pending');
        const blockId = t(source.blockId) || (parsed ? parsed.blockId : '');
        const startIndex = Number.isInteger(Number(source.startIndex)) ? Number(source.startIndex) : (parsed ? parsed.start : -1);
        const endIndex = Number.isInteger(Number(source.endIndex)) ? Number(source.endIndex) : (parsed ? parsed.end : -1);
        const quote = t(source.quote || source.token);
        const contextQuote = t(source.contextQuote || source.quoteSnapshot);
        const anchorHint = t(source.anchorHint || source.hint);
        return {
            anchorId: t(anchorId),
            blockId,
            startIndex,
            endIndex,
            quote,
            contextQuote,
            anchorHint,
            status,
            mountedPath,
            revisions,
            displayText: quote || contextQuote || anchorHint || t(anchorId),
        };
    }

    function hasMounted(anchor) {
        if (!anchor) return false;
        if (anchor.status === 'mounted' || !!anchor.mountedPath) return true;
        return (anchor.revisions || []).some((r) => !!normalizePath(r && r.notePath));
    }

    function collectNodes() {
        const body = document.getElementById('markdownBody');
        if (!body) return [];
        return Array.from(body.querySelectorAll(NODE_SELECTOR))
            .map((node) => {
                if (!(node instanceof HTMLElement)) return null;
                if (node.closest('#contentInvitation')) return null;
                const text = t(node.innerText || node.textContent || '');
                if (!text) return null;
                const ids = new Set([
                    t(node.id),
                    t(node.getAttribute('data-block-id')),
                    t(node.getAttribute('data-node-id')),
                ].filter(Boolean));
                return { node, text, ids };
            })
            .filter(Boolean);
    }

    function score(anchor, nodeInfo) {
        let s = 0;
        const txt = nodeInfo.text.toLowerCase();
        if (anchor.blockId && nodeInfo.ids.has(anchor.blockId)) s += 180;
        if (anchor.quote && txt.includes(anchor.quote.toLowerCase())) s += 120;
        if (anchor.contextQuote && txt.includes(anchor.contextQuote.replace(/\.\.\./g, ' ').toLowerCase())) s += 40;
        if (anchor.anchorHint && txt.includes(anchor.anchorHint.toLowerCase())) s += 25;
        return s;
    }

    function rebuildCandidates() {
        const body = document.getElementById('markdownBody');
        if (body) {
            body.querySelectorAll('.md-anchor-highlight[data-anchor-id]').forEach((node) => {
                node.classList.remove('md-anchor-highlight', 'is-mounted', 'is-drop-target');
                node.removeAttribute('data-anchor-id');
            });
        }
        const nodes = collectNodes();
        const next = [];
        runtime.anchors.forEach((anchor, anchorId) => {
            let best = null;
            let bestScore = -1;
            nodes.forEach((one) => {
                const s = score(anchor, one);
                if (s > bestScore) {
                    bestScore = s;
                    best = one;
                }
            });
            const candidate = { ...anchor, anchorId, node: bestScore >= 12 && best ? best.node : null, matchText: best ? best.text : '' };
            if (candidate.node) {
                candidate.node.dataset.anchorId = anchorId;
                candidate.node.classList.add('md-anchor-highlight');
                candidate.node.classList.toggle('is-mounted', hasMounted(candidate));
                candidate.node.classList.toggle('is-drop-target', runtime.dropId === anchorId);
            }
            next.push(candidate);
        });
        runtime.candidates = next.sort((a, b) => String(a.anchorId).localeCompare(String(b.anchorId)));
    }

    function candidateOf(anchorId) {
        return runtime.candidates.find((c) => c.anchorId === anchorId) || null;
    }

    function isEditableTarget(target) {
        if (!target) return false;
        if (target instanceof HTMLInputElement || target instanceof HTMLTextAreaElement) {
            return true;
        }
        if (target instanceof HTMLElement && target.isContentEditable) {
            return true;
        }
        if (!(target instanceof Element)) {
            return false;
        }
        return !!target.closest('input, textarea, [contenteditable], [contenteditable=\"\"], [contenteditable=\"true\"]');
    }

    function readSelectedMarkdownText() {
        const selection = window.getSelection ? window.getSelection() : null;
        if (!selection || selection.rangeCount <= 0 || selection.isCollapsed) {
            return '';
        }
        const text = t(selection.toString());
        if (!text) {
            return '';
        }
        const markdownBody = document.getElementById('markdownBody');
        if (!markdownBody) {
            return '';
        }
        for (let i = 0; i < selection.rangeCount; i += 1) {
            const range = selection.getRangeAt(i);
            const root = range && range.commonAncestorContainer;
            const container = root instanceof Element ? root : (root && root.parentElement);
            if (container && markdownBody.contains(container)) {
                return text;
            }
        }
        return '';
    }

    function resolveAnchorIdBySelectionText(selectionText) {
        const needle = t(selectionText).toLowerCase();
        if (!needle) {
            return '';
        }
        let bestId = '';
        let bestScore = 0;
        runtime.candidates.forEach((candidate) => {
            const fields = [candidate.quote, candidate.contextQuote, candidate.anchorHint, candidate.displayText]
                .map((value) => t(value).toLowerCase())
                .filter(Boolean);
            let score = 0;
            fields.forEach((field) => {
                if (field === needle) {
                    score = Math.max(score, 320);
                    return;
                }
                if (field.includes(needle)) {
                    score = Math.max(score, 240);
                    return;
                }
                if (needle.includes(field)) {
                    score = Math.max(score, 180);
                }
            });
            if (!score && candidate.node) {
                const nodeText = t(candidate.node.innerText || candidate.node.textContent || '').toLowerCase();
                if (nodeText && nodeText.includes(needle)) {
                    score = 140;
                }
            }
            if (score > bestScore) {
                bestScore = score;
                bestId = candidate.anchorId;
            }
        });
        return bestScore > 0 ? bestId : '';
    }

    function mountAnchorBySelectionText(selectionText, source, preferredAnchorId) {
        const selectedText = t(selectionText);
        const preferred = t(preferredAnchorId);
        if (preferred && runtime.anchors.has(preferred)) {
            selectAnchor(preferred, t(source) || 'selection_mount', true);
            return true;
        }
        if (!selectedText) {
            return false;
        }
        const matchedAnchorId = resolveAnchorIdBySelectionText(selectedText);
        if (!matchedAnchorId) {
            document.dispatchEvent(new CustomEvent('mobile-anchor-create-request', {
                detail: {
                    text: selectedText,
                    source: t(source) || 'selection_mount',
                },
            }));
            openPanel();
            setPanel(false);
            setPreview(`Creating mount anchor: ${short(selectedText, 40)}`);
            return false;
        }
        selectAnchor(matchedAnchorId, t(source) || 'selection_mount', true);
        return true;
    }

    function setPreview(text) {
        const node = document.getElementById('anchorPreview');
        if (!node) return;
        node.innerHTML = `<div class="empty">${h(text || TEXT.previewEmpty)}</div>`;
    }

    function refreshFullscreenToggleButton() {
        const button = document.querySelector('#anchorObsidianHeader [data-obsidian-action="toggle-fullscreen"]');
        if (!(button instanceof HTMLElement)) {
            return;
        }
        const active = !!runtime.manualFullscreen;
        button.setAttribute('aria-pressed', active ? 'true' : 'false');
        button.setAttribute('title', active ? 'Exit fullscreen' : 'Enter fullscreen');
        button.setAttribute('aria-label', active ? 'Exit fullscreen' : 'Enter fullscreen');
        button.innerHTML = active ? '&#x2922;' : '&#x26F6;';
    }

    function setPanel(detail) {
        const panel = document.getElementById('anchorMountPanel');
        const backBtn = document.getElementById('anchorInboxBackBtn');
        const fullscreen = !!detail && !!runtime.manualFullscreen;
        if (!panel) return;
        panel.classList.add('is-expanded');
        panel.classList.toggle('is-detail-mode', !!detail);
        panel.classList.toggle('is-anchor-active', !!detail);
        panel.classList.toggle('is-obsidian-fullscreen', fullscreen);
        if (document.body) {
            document.body.classList.toggle('anchor-obsidian-fullscreen-open', fullscreen);
        }
        if (backBtn) backBtn.hidden = !detail;
        refreshFullscreenToggleButton();
    }

    function openPanel() {
        const api = window.__viewerLayoutApi;
        if (api && typeof api.setRightExpanded === 'function') {
            api.setRightExpanded(true, true);
        }
    }

    function closePanel() {
        const api = window.__viewerLayoutApi;
        if (api && typeof api.setRightExpanded === 'function') {
            api.setRightExpanded(false, true);
        }
        runtime.manualFullscreen = false;
        hideObsidianHoverCardNow();
        closeObsidianGraphModal();
        closeWikilinkSuggest();
        const commandMenu = document.getElementById('anchorObsidianCommandMenu');
        if (commandMenu) {
            commandMenu.hidden = true;
        }
        hidePhase2bFloatingUi();
        const panel = document.getElementById('anchorMountPanel');
        if (panel) {
            panel.classList.remove('is-expanded', 'is-anchor-active', 'is-detail-mode', 'is-obsidian-fullscreen');
        }
        if (document.body) {
            document.body.classList.remove('anchor-obsidian-fullscreen-open');
        }
        refreshFullscreenToggleButton();
    }

    function renderIndex() {
        const list = document.getElementById('anchorIndexList');
        if (!list) return;
        if (!runtime.candidates.length) {
            list.innerHTML = `<div class="empty">${h(TEXT.noAnchors)}</div>`;
            return;
        }
        list.innerHTML = runtime.candidates.map((c) => {
            const badge = hasMounted(c) ? '已挂载' : (c.status === 'files_uploaded' ? '已上传文件' : '待处理');
            return `
                <button class="anchor-index-item${runtime.activeId === c.anchorId ? ' is-active' : ''}${runtime.dropId === c.anchorId ? ' is-drop-target' : ''}" type="button" data-anchor-id="${h(c.anchorId)}">
                    <span class="anchor-index-item-line">${h(short(c.displayText, 72))}</span>
                    <span class="anchor-index-item-meta">
                        <span>${h(short(c.matchText || c.displayText, 64))}</span>
                        <span class="anchor-index-badge ${hasMounted(c) ? 'is-mounted' : (c.status === 'files_uploaded' ? 'is-files' : 'is-pending')}">${h(badge)}</span>
                    </span>
                </button>
            `;
        }).join('');
    }

    function renderInbox() {
        const box = document.getElementById('anchorInboxList');
        if (!box) return;
        const mounted = runtime.candidates.filter((c) => hasMounted(c));
        if (!mounted.length) {
            box.innerHTML = `<div class="empty">${h(TEXT.noMounted)}</div>`;
            return;
        }
        box.innerHTML = mounted.map((c) => `
            <button class="anchor-inbox-item${runtime.activeId === c.anchorId ? ' is-active' : ''}" type="button" data-anchor-id="${h(c.anchorId)}">
                <span class="anchor-inbox-item-line">${h(short(c.displayText, 64))}</span>
                <span class="anchor-inbox-item-hint">${h(short(c.contextQuote || c.anchorHint || c.quote || c.anchorId, 64))}</span>
                <span class="anchor-inbox-item-meta">已挂载</span>
            </button>
        `).join('');
    }

    function refreshViews() {
        renderIndex();
        renderInbox();
    }

    async function fetchMeta(silent) {
        const ctx = runtime.ctx;
        if (!ctx.taskId) {
            runtime.anchors.clear();
            runtime.candidates = [];
            runtime.activeId = '';
            runtime.dropId = '';
            runtime.pendingByAnchor.clear();
            runtime.pendingMainByAnchor.clear();
            rebuildCandidates();
            refreshViews();
            setPanel(false);
            closePanel();
            return;
        }
        const seq = ++runtime.metaSeq;
        const query = ctx.pathHint ? `?path=${encodeURIComponent(ctx.pathHint)}` : '';
        try {
            const resp = await fetch(`${ctx.apiBase}/tasks/${encodeURIComponent(ctx.taskId)}/meta${query}`);
            const body = await parseResp(resp);
            if (seq !== runtime.metaSeq) return;
            const map = new Map();
            const anchorsObj = body && body.anchors && typeof body.anchors === 'object' ? body.anchors : {};
            Object.keys(anchorsObj).forEach((id) => {
                const key = t(id);
                if (!key) return;
                map.set(key, sanitizeAnchor(key, anchorsObj[id]));
            });
            runtime.anchors = map;
            rebuildCandidates();
            if (runtime.activeId && !candidateOf(runtime.activeId)) {
                runtime.activeId = '';
                setPanel(false);
            }
            refreshViews();
        } catch (e) {
            if (!silent) console.warn('加载锚点元数据失败', e);
        }
    }

    async function emitTelemetry(eventType, anchor, extra) {
        if (!runtime.ctx.taskId) return;
        const payload = Object.assign({
            anchorId: t(anchor && anchor.anchorId),
            quote: t(anchor && anchor.quote),
        }, extra || {});
        try {
            await fetch(`${runtime.ctx.apiBase}/tasks/${encodeURIComponent(runtime.ctx.taskId)}/telemetry`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    path: runtime.ctx.pathHint || '',
                    events: [{
                        nodeId: t(anchor && anchor.blockId),
                        eventType,
                        relevanceScore: 0,
                        timestampMs: Date.now(),
                        payload,
                    }],
                }),
            });
        } catch (_e) {
        }
    }

    async function loadMounted(anchorId, notePath, telemetrySource) {
        const c = candidateOf(anchorId);
        if (!c || !hasMounted(c) || !runtime.ctx.taskId) return null;
        const seq = ++runtime.mountedSeq;
        const query = new URLSearchParams();
        if (runtime.ctx.pathHint) query.set('path', runtime.ctx.pathHint);
        if (notePath) query.set('notePath', notePath);
        setPreview(TEXT.previewLoading);
        try {
            const resp = await fetch(`${runtime.ctx.apiBase}/tasks/${encodeURIComponent(runtime.ctx.taskId)}/anchors/${encodeURIComponent(anchorId)}/mounted?${query.toString()}`);
            const body = await parseResp(resp);
            if (seq !== runtime.mountedSeq) return null;
            runtime.mountedPayloadByAnchor.set(anchorId, body);
            if (body.notePath) runtime.mountedNoteByAnchor.set(anchorId, normalizePath(body.notePath));
            const preview = document.getElementById('anchorPreview');
            if (preview) {
                const html = window.markdownit
                    ? window.markdownit({ html: false, breaks: true, linkify: true }).render(String(body.markdown || ''))
                    : `<pre>${h(String(body.markdown || ''))}</pre>`;
                preview.innerHTML = window.DOMPurify ? window.DOMPurify.sanitize(html) : html;
            }
            renderMainNoteSelector(anchorId);
            if (telemetrySource) {
                emitTelemetry('mounted_note_opened', c, {
                    source: telemetrySource,
                    notePath: t(body.notePath || body.entryNotePath),
                });
            }
            return body;
        } catch (e) {
            setPreview(`${TEXT.uploadFailed}：${t(e && e.message)}`);
            return null;
        }
    }

    function pendingList(anchorId) {
        return runtime.pendingByAnchor.get(anchorId) || [];
    }

    function latestRevision(anchor) {
        if (!anchor || !Array.isArray(anchor.revisions) || !anchor.revisions.length) return null;
        return anchor.revisions[anchor.revisions.length - 1];
    }

    function normalizeRevisionDir(anchor) {
        const revision = latestRevision(anchor);
        return normalizePath(revision && revision.relativeDir);
    }

    function toRevisionRelativePath(pathLike, anchor) {
        const value = normalizePath(pathLike);
        if (!value) return '';
        const revisionDir = normalizeRevisionDir(anchor);
        if (revisionDir && value.startsWith(`${revisionDir}/`)) {
            return value.slice(revisionDir.length + 1);
        }
        return value;
    }

    function collectMarkdownCandidates(anchorId, anchor) {
        const candidates = [];
        const dedupe = new Set();
        const pushOne = (pathLike) => {
            const p = normalizePath(pathLike);
            if (!p || !isMarkdown(p) || dedupe.has(p)) return;
            dedupe.add(p);
            candidates.push(p);
        };

        pendingList(anchorId).forEach((x) => pushOne(x && x.relativePath));
        if (candidates.length > 0) {
            return candidates;
        }

        const revision = latestRevision(anchor);
        const revisionDir = normalizePath(revision && revision.relativeDir);
        const files = Array.isArray(revision && revision.files) ? revision.files : [];
        files.forEach((file) => {
            let p = normalizePath(file && (file.relativePath || file.path || file.name));
            if (!p) return;
            if (revisionDir && p.startsWith(`${revisionDir}/`)) {
                p = p.slice(revisionDir.length + 1);
            }
            pushOne(p);
        });

        pushOne(toRevisionRelativePath(runtime.mountedNoteByAnchor.get(anchorId) || '', anchor));
        pushOne(toRevisionRelativePath(anchor && anchor.mountedPath || '', anchor));
        return candidates;
    }

    function renderMainNoteSelector(anchorId) {
        const wrap = document.getElementById('anchorMainNoteWrap');
        const select = document.getElementById('anchorMainNoteSelect');
        const key = t(anchorId);
        if (wrap) {
            wrap.hidden = true;
            wrap.style.display = 'none';
        }
        if (select) {
            select.innerHTML = '';
        }
        if (!key) {
            return;
        }
        const notes = ensureLocalNoteFileNames(key);
        const firstNotePath = normalizePath(notes[0] && notes[0].fileName);
        if (firstNotePath) {
            runtime.pendingMainByAnchor.set(key, firstNotePath);
            return;
        }
        const anchor = candidateOf(key) || runtime.anchors.get(key) || null;
        const fallback = normalizePath(
            toRevisionRelativePath(runtime.mountedNoteByAnchor.get(key) || '', anchor)
            || toRevisionRelativePath(anchor && anchor.mountedPath || '', anchor)
        );
        if (fallback) {
            runtime.pendingMainByAnchor.set(key, fallback);
            return;
        }
        runtime.pendingMainByAnchor.delete(key);
    }

    function renderPending(anchorId) {
        const list = document.getElementById('anchorFileList');
        if (!list) return;
        const arr = pendingList(anchorId);
        if (!arr.length) {
            list.innerHTML = '';
            renderMainNoteSelector(anchorId);
            return;
        }
        list.innerHTML = arr.map((x, i) => `
            <div class="anchor-file-item">
                <span class="anchor-file-item-name" title="${h(x.relativePath)}">${h(x.relativePath)}</span>
                <span class="anchor-file-item-size">${h(`${Math.max(1, Math.round((x.file.size || 0) / 1024))} KB`)}</span>
                <button class="anchor-file-item-remove" type="button" data-remove-index="${i}" aria-label="移除附件">×</button>
            </div>
        `).join('');
        renderMainNoteSelector(anchorId);
    }

    function renderContext(candidate) {
        const title = document.getElementById('anchorPanelTitle');
        const subtitle = document.getElementById('anchorPanelSubtitle');
        const quote = document.getElementById('anchorQuoteSnapshot');
        const hint = document.getElementById('anchorHintPreview');
        if (title) title.textContent = `为锚点挂载：${short(candidate.displayText, 20)}`;
        if (subtitle) subtitle.textContent = '统一输入：补充说明、添加附件，然后点击发送。';
        if (quote) quote.textContent = candidate.contextQuote || candidate.quote || TEXT.quotePlaceholder;
        if (hint) hint.textContent = candidate.anchorHint || TEXT.hintPlaceholder;
    }

    function selectAnchor(anchorId, source, scrollIntoView) {
        const c = candidateOf(anchorId);
        if (!c) return;
        if (runtime.activeId && runtime.activeId !== anchorId) {
            persistActiveLocalNoteFromEditor(runtime.activeId, { retitle: true });
        }
        runtime.activeId = anchorId;
        openPanel();
        setPanel(true);
        renderContext(c);
        renderPending(anchorId);
        renderLocalNoteCards(anchorId);
        syncEditorFromActiveLocalNote(anchorId);
        const searchInput = document.getElementById('anchorLocalNoteSearchInput');
        if (searchInput) {
            searchInput.value = getLocalNoteFilter(anchorId);
        }
        refreshViews();
        if (c.node && scrollIntoView !== false) c.node.scrollIntoView({ block: 'center', behavior: 'smooth' });
        if (hasMounted(c)) {
            loadMounted(anchorId, runtime.mountedNoteByAnchor.get(anchorId) || '', source || 'anchor');
        } else {
            setPreview(TEXT.choosing);
        }
    }

    async function submitMount(options = {}) {
        const anchorId = runtime.activeId;
        if (!anchorId) {
            setPreview(TEXT.noSelection);
            return;
        }
        const c = candidateOf(anchorId);
        if (!c || !runtime.ctx.taskId) {
            setPreview(TEXT.noSelection);
            return;
        }
        const note = t(readEditorValue());
        const includeQuickInputNote = !options || options.includeQuickInputNote !== false;
        const clearQuickInput = !!(options && options.clearQuickInput);
        const preferredMainNotePath = normalizePath(options && options.preferredMainNotePath);
        const telemetrySource = t(options && options.telemetrySource) || 'web_anchor_panel';
        const externalPending = Array.isArray(options && options.extraPending) ? options.extraPending : [];
        const pending = pendingList(anchorId).slice();
        externalPending.forEach((item) => {
            if (!item || !(item.file instanceof File)) {
                return;
            }
            const relativePath = normalizePath(item.relativePath || item.file.name);
            if (!relativePath) {
                return;
            }
            pending.push({
                file: item.file,
                relativePath,
            });
        });
        if (includeQuickInputNote && note) {
            const ts = Date.now();
            const noteName = `anchor_note_${ts}.md`;
            pending.push({
                file: new File([`${note}\n`], noteName, { type: 'text/markdown;charset=utf-8', lastModified: ts }),
                relativePath: noteName,
            });
        }
        if (!pending.length) {
            setPreview(TEXT.chooseAtLeastOne);
            return;
        }
        const fd = new FormData();
        if (runtime.ctx.pathHint) fd.append('path', runtime.ctx.pathHint);
        pending.forEach((x) => {
            fd.append('files', x.file, x.file.name);
            fd.append('relativePaths', x.relativePath);
        });
        const mdCandidates = Array.from(new Set(
            pending
                .map((x) => normalizePath(x && x.relativePath))
                .filter((p) => isMarkdown(p))
        ));
        if (mdCandidates.length) {
            const preferred = normalizePath(runtime.pendingMainByAnchor.get(anchorId));
            if (mdCandidates.includes(preferred)) {
                fd.append('mainNotePath', preferred);
            } else if (preferredMainNotePath && mdCandidates.includes(preferredMainNotePath)) {
                fd.append('mainNotePath', preferredMainNotePath);
            } else {
                fd.append('mainNotePath', mdCandidates[0]);
            }
        }
        setPreview(TEXT.previewLoading);
        try {
            const resp = await fetch(`${runtime.ctx.apiBase}/tasks/${encodeURIComponent(runtime.ctx.taskId)}/anchors/${encodeURIComponent(anchorId)}/mount`, {
                method: 'POST',
                body: fd,
            });
            const body = await parseResp(resp);
            if (body.anchor && typeof body.anchor === 'object') {
                runtime.anchors.set(anchorId, sanitizeAnchor(anchorId, body.anchor));
            }
            runtime.pendingByAnchor.set(anchorId, []);
            runtime.pendingMainByAnchor.delete(anchorId);
            if (clearQuickInput) {
                writeEditorValue('', { clearHistory: false });
            }
            await emitTelemetry('note_mounted', c, { source: telemetrySource, fileCount: pending.length, noteChars: note.length });
            await fetchMeta(true);
            runtime.localSyncShadowByAnchor.set(anchorId, buildLocalSyncSnapshot(anchorId));
            clearAnchorLocalSyncDirty(anchorId);
            await loadMounted(anchorId, '', '');
            renderPending(anchorId);
            setPreview(TEXT.uploadSuccess);
            const mounted = runtime.mountedPayloadByAnchor.get(anchorId);
            if (mounted && mounted.markdown) loadMounted(anchorId, '', '');
        } catch (e) {
            setPreview(`${TEXT.uploadFailed}：${t(e && e.message)}`);
        }
    }

    function bindEvents() {
        ensureObsidianNoteUi();
        const indexList = document.getElementById('anchorIndexList');
        const inboxList = document.getElementById('anchorInboxList');
        const body = document.getElementById('markdownBody');
        const fileInput = document.getElementById('anchorFileInput');
        const pickBtn = document.getElementById('anchorPickBtn');
        const cloudBtn = document.getElementById('anchorCloudUploadBtn');
        const sendBtn = document.getElementById('anchorUploadBtn');
        const fileList = document.getElementById('anchorFileList');
        const localCards = document.getElementById('anchorLocalNoteCards');
        const closeBtn = document.getElementById('anchorPanelCloseBtn');
        const backBtn = document.getElementById('anchorInboxBackBtn');
        const layout = document.getElementById('viewerLayout');
        const quickInput = document.getElementById('anchorQuickNoteInput');
        const mainNoteSelect = document.getElementById('anchorMainNoteSelect');
        const preview = document.getElementById('anchorPreview');
        const localLivePreview = document.getElementById('anchorLocalNoteLivePreview');
        const searchInput = document.getElementById('anchorLocalNoteSearchInput');
        const obsidianPanels = document.getElementById('anchorObsidianPanels');
        const obsidianHeader = document.getElementById('anchorObsidianHeader');
        const obsidianCommandMenu = document.getElementById('anchorObsidianCommandMenu');
        const layoutModeSelect = document.getElementById('anchorObsidianLayoutMode');
        const wikilinkSuggestPanel = document.getElementById('anchorWikilinkSuggest');
        const dropzone = document.getElementById('anchorDropzone');
        const graphBtn = document.getElementById('anchorObsidianGraphBtn');
        const graphCloseBtn = document.getElementById('anchorObsidianGraphCloseBtn');
        const graphModal = document.getElementById('anchorObsidianGraphModal');
        const graphCanvas = document.getElementById('anchorObsidianGraphCanvas');
        const phase2bDock = document.getElementById('anchorPhase2bDock');
        const phase2bCanvas = document.getElementById('anchorPhase2bCanvas');
        const phase2bInput = document.getElementById('anchorPhase2bInput');
        const phase2bResizer = document.getElementById('anchorPhase2bResizer');
        if (!ENABLE_OBSIDIAN_GRAPH) {
            if (graphBtn) {
                graphBtn.hidden = true;
                graphBtn.style.display = 'none';
            }
            closeObsidianGraphModal();
        }

        const findId = (target) => {
            const n = target && target.closest ? target.closest('[data-anchor-id]') : null;
            return t(n && n.getAttribute('data-anchor-id'));
        };

        const handlePreviewLinkMouseOver = (event) => {
            const anchorForPreview = t(runtime.activeId || (runtime.candidates[0] && runtime.candidates[0].anchorId));
            if (!anchorForPreview) {
                return;
            }
            const link = event && event.target && event.target.closest ? event.target.closest('a[href]') : null;
            if (!(link instanceof HTMLAnchorElement)) {
                return;
            }
            const href = String(link.getAttribute('href') || '').trim();
            if (!isHoverPreviewableHref(href)) {
                return;
            }
            showObsidianHoverCard(anchorForPreview, link);
        };
        const handlePreviewLinkMouseOut = (event) => {
            const link = event && event.target && event.target.closest ? event.target.closest('a[href]') : null;
            if (!(link instanceof HTMLAnchorElement)) {
                return;
            }
            const related = event && event.relatedTarget;
            const card = document.getElementById('anchorObsidianHoverCard');
            if (card && related instanceof Element && card.contains(related)) {
                return;
            }
            scheduleHideObsidianHoverCard(120);
        };

        const ingestMarkdownFilesToLocalNotes = async (anchorId, files) => {
            const targetAnchorId = t(anchorId);
            if (!targetAnchorId) {
                return { importedCount: 0, nonMarkdownCount: 0 };
            }
            let importedCount = 0;
            let nonMarkdownCount = 0;
            const jobs = Array.from(files || []).map(async (file) => {
                if (!(file instanceof File) || !file.name) {
                    return;
                }
                const relativePath = normalizePath(file.webkitRelativePath || file.name);
                if (!isMarkdown(relativePath)) {
                    nonMarkdownCount += 1;
                    return;
                }
                try {
                    const content = await file.text();
                    upsertLocalNoteFromMarkdownFile(targetAnchorId, relativePath, content);
                    importedCount += 1;
                } catch (_e) {
                    // ignore markdown parse error for non-text file fallback
                }
            });
            await Promise.allSettled(jobs);
            if (runtime.activeId === targetAnchorId) {
                renderLocalNoteCards(targetAnchorId);
                syncEditorFromActiveLocalNote(targetAnchorId);
                renderObsidianKnowledgePanels(targetAnchorId);
                refreshObsidianStatusBar(targetAnchorId);
            }
            if (importedCount > 0) {
                setPreview(`Imported ${importedCount} markdown ${importedCount === 1 ? 'note' : 'notes'} via drag and drop.`);
            } else if (nonMarkdownCount > 0) {
                setPreview('Only markdown files are accepted for local note cards.');
            }
            return { importedCount, nonMarkdownCount };
        };

        indexList && indexList.addEventListener('click', (e) => { const id = findId(e.target); if (id) selectAnchor(id, 'index', true); });
        inboxList && inboxList.addEventListener('click', (e) => { const id = findId(e.target); if (id) selectAnchor(id, 'inbox', true); });
        body && body.addEventListener('click', (e) => { const id = findId(e.target); if (id) selectAnchor(id, 'highlight', false); });
        body && body.addEventListener('mouseover', handlePreviewLinkMouseOver);
        body && body.addEventListener('mouseout', handlePreviewLinkMouseOut);

        if (pickBtn) {
            pickBtn.hidden = true;
            pickBtn.style.display = 'none';
        }
        if (sendBtn) {
            sendBtn.hidden = true;
            sendBtn.style.display = 'none';
        }
        if (cloudBtn) {
            cloudBtn.hidden = true;
            cloudBtn.style.display = 'none';
        }
        fileInput && fileInput.addEventListener('change', async (e) => {
            if (!runtime.activeId) { e.target.value = ''; return setPreview(TEXT.noSelection); }
            const selectedFiles = Array.from(e.target.files || []);
            await ingestMarkdownFilesToLocalNotes(runtime.activeId, selectedFiles);
            runtime.pendingByAnchor.set(runtime.activeId, []);
            renderPending(runtime.activeId);
            refreshViews();
            e.target.value = '';
        });
        if (ENABLE_OBSIDIAN_GRAPH) {
            graphBtn && graphBtn.addEventListener('click', (e) => {
                e.preventDefault();
                const anchorId = t(runtime.activeId);
                if (!anchorId) {
                    setPreview(TEXT.noSelection);
                    return;
                }
                renderObsidianGraphModal(anchorId);
            });
            graphCloseBtn && graphCloseBtn.addEventListener('click', (e) => {
                e.preventDefault();
                closeObsidianGraphModal();
            });
            graphModal && graphModal.addEventListener('click', (e) => {
                if (e && e.target === graphModal) {
                    closeObsidianGraphModal();
                }
            });
            graphCanvas && graphCanvas.addEventListener('click', (e) => {
                if (!runtime.activeId) {
                    return;
                }
                const node = e && e.target && e.target.closest ? e.target.closest('[data-graph-note-id]') : null;
                if (!node) {
                    return;
                }
                const noteId = t(node.getAttribute('data-graph-note-id'));
                if (!noteId) {
                    return;
                }
                switchActiveLocalNote(runtime.activeId, noteId, { focusEditor: true });
                closeObsidianGraphModal();
            });
        }
        layoutModeSelect && layoutModeSelect.addEventListener('change', (e) => {
            if (!runtime.activeId) {
                return;
            }
            const mode = normalizeEditorLayoutMode(e && e.target ? e.target.value : '');
            writeLocalEditorLayoutMode(runtime.activeId, mode);
            applyLocalEditorLayoutMode(runtime.activeId);
            if (mode !== 'preview') {
                focusEditorInput();
            }
            refreshObsidianStatusBar(runtime.activeId);
        });
        const closeCommandMenu = () => {
            if (obsidianCommandMenu) {
                obsidianCommandMenu.hidden = true;
            }
        };
        const toggleCommandMenu = () => {
            if (!obsidianCommandMenu) {
                return;
            }
            if (obsidianCommandMenu.hidden) {
                runtime.obsidianSettingsExpanded = false;
                renderObsidianSettingsPanel();
            }
            obsidianCommandMenu.hidden = !obsidianCommandMenu.hidden;
        };
        obsidianHeader && obsidianHeader.addEventListener('click', (e) => {
            const actionBtn = closestFromEventTarget(e && e.target, '[data-obsidian-action]');
            if (!actionBtn) {
                return;
            }
            const action = t(actionBtn.getAttribute('data-obsidian-action'));
            if (action === 'toggle-fullscreen') {
                e.preventDefault();
                if (!runtime.activeId) {
                    return;
                }
                runtime.manualFullscreen = !runtime.manualFullscreen;
                setPanel(true);
                return;
            }
            if (action === 'toggle-command') {
                toggleCommandMenu();
            }
        });
        obsidianCommandMenu && obsidianCommandMenu.addEventListener('click', (e) => {
            const toggleBtn = closestFromEventTarget(e && e.target, '[data-settings-action="toggle-expanded"]');
            if (toggleBtn) {
                runtime.obsidianSettingsExpanded = !runtime.obsidianSettingsExpanded;
                renderObsidianSettingsPanel();
                return;
            }
            const resetBtn = closestFromEventTarget(e && e.target, '[data-theme-action="reset"]');
            if (resetBtn) {
                const defaults = normalizeReaderThemeSettings({});
                writeReaderThemeSettings(defaults);
                applyReaderThemeSettings(defaults);
                renderObsidianSettingsPanel();
            }
        });
        obsidianCommandMenu && obsidianCommandMenu.addEventListener('input', (e) => {
            const target = e && e.target;
            if (!(target instanceof Element)) {
                return;
            }
            const settingNode = target.closest('[data-setting]');
            if (!(settingNode instanceof Element)) {
                return;
            }
            const key = t(settingNode.getAttribute('data-setting'));
            if (!key) {
                return;
            }
            let value = null;
            if (settingNode instanceof HTMLInputElement || settingNode instanceof HTMLSelectElement) {
                value = settingNode.value;
            }
            if (key === 'themeMode') {
                applyReaderThemePatch({ themeMode: normalizeThemeMode(value) });
                return;
            }
            if (key === 'fontPreset') {
                applyReaderThemePatch({ fontPreset: normalizeFontPreset(value) });
                return;
            }
            const n = Number(value);
            if (!Number.isFinite(n)) {
                return;
            }
            if (key === 'fontSize') applyReaderThemePatch({ fontSize: n });
            if (key === 'lineHeight') applyReaderThemePatch({ lineHeight: n });
            if (key === 'maxWidthCh') applyReaderThemePatch({ maxWidthCh: n });
            if (key === 'pagePaddingPx') applyReaderThemePatch({ pagePaddingPx: n });
            if (key === 'paragraphGapEm') applyReaderThemePatch({ paragraphGapEm: n });
            if (key === 'listIndentEm') applyReaderThemePatch({ listIndentEm: n });
        });
        if (phase2bDock && !phase2bDock.dataset.phase2bBound) {
            phase2bDock.dataset.phase2bBound = '1';
            phase2bDock.addEventListener('click', (e) => {
                const actionNode = e && e.target && e.target.closest ? e.target.closest('[data-phase2b-action]') : null;
                if (!(actionNode instanceof Element)) {
                    return;
                }
                const action = t(actionNode.getAttribute('data-phase2b-action'));
                if (action === 'open') {
                    if (runtime.phase2b.suppressNextOpen) {
                        runtime.phase2b.suppressNextOpen = false;
                        return;
                    }
                    setPhase2bExpanded(true);
                    return;
                }
                if (action === 'collapse') {
                    hidePhase2bFloatingUi();
                    return;
                }
                if (action === 'toggle-input') {
                    runtime.phase2b.inputCollapsed = !runtime.phase2b.inputCollapsed;
                    renderPhase2bFloatingUi();
                    if (!runtime.phase2b.inputCollapsed) {
                        const input = document.getElementById('anchorPhase2bInput');
                        if (input && typeof input.focus === 'function') {
                            input.focus();
                            const len = String(input.value || '').length;
                            if (typeof input.setSelectionRange === 'function') {
                                input.setSelectionRange(len, len);
                            }
                        }
                    }
                    return;
                }
                if (action === 'submit') {
                    submitPhase2bContent();
                    return;
                }
                if (action === 'copy') {
                    copyPhase2bResultToClipboard();
                    return;
                }
            });
            const hasDraggedFiles = (event) => {
                const dt = event && event.dataTransfer;
                return !!dt && Array.from(dt.types || []).includes('Files');
            };
            const phase2bCapsuleBtn = document.getElementById('anchorPhase2bCapsuleBtn');
            const phase2bDragHandle = phase2bCanvas && phase2bCanvas.querySelector
                ? phase2bCanvas.querySelector('.anchor-phase2b-canvas-head')
                : null;
            const startPhase2bMove = (pointerEvent, captureTarget) => {
                if (!pointerEvent || !Number.isFinite(Number(pointerEvent.pointerId))) {
                    return false;
                }
                if (Number.isFinite(Number(pointerEvent.button)) && Number(pointerEvent.button) !== 0) {
                    return false;
                }
                if (!(phase2bDock instanceof HTMLElement)) {
                    return false;
                }
                const panelNode = document.getElementById('anchorMountPanel');
                if (!(panelNode instanceof HTMLElement)) {
                    return false;
                }
                const panelRect = panelNode.getBoundingClientRect();
                const dockRect = phase2bDock.getBoundingClientRect();
                const phase2b = runtime.phase2b;
                phase2b.moveX = dockRect.left - panelRect.left;
                phase2b.moveY = dockRect.top - panelRect.top;
                phase2b.moveOffsetX = pointerEvent.clientX - dockRect.left;
                phase2b.moveOffsetY = pointerEvent.clientY - dockRect.top;
                phase2b.moveStartClientX = pointerEvent.clientX;
                phase2b.moveStartClientY = pointerEvent.clientY;
                phase2b.moveDidDrag = false;
                phase2b.movePointerId = pointerEvent.pointerId;
                phase2b.moving = true;
                phase2b.moveCaptureTarget = captureTarget instanceof Element ? captureTarget : null;
                if (captureTarget && typeof captureTarget.setPointerCapture === 'function') {
                    try {
                        captureTarget.setPointerCapture(pointerEvent.pointerId);
                    } catch (_e) {
                        // ignore unsupported setPointerCapture
                    }
                }
                renderPhase2bFloatingUi();
                return true;
            };
            const endPhase2bMove = (pointerId) => {
                const phase2b = runtime.phase2b;
                if (!phase2b.moving) {
                    return;
                }
                if (Number.isFinite(Number(pointerId))
                        && Number.isFinite(Number(phase2b.movePointerId))
                        && Number(phase2b.movePointerId) >= 0
                        && Number(phase2b.movePointerId) !== Number(pointerId)) {
                    return;
                }
                phase2b.moving = false;
                phase2b.movePointerId = -1;
                phase2b.moveOffsetX = 0;
                phase2b.moveOffsetY = 0;
                if (phase2b.moveDidDrag) {
                    phase2b.suppressNextOpen = true;
                }
                phase2b.moveDidDrag = false;
                phase2b.moveStartClientX = 0;
                phase2b.moveStartClientY = 0;
                const captureTarget = phase2b.moveCaptureTarget;
                phase2b.moveCaptureTarget = null;
                if (captureTarget
                        && Number.isFinite(Number(pointerId))
                        && typeof captureTarget.releasePointerCapture === 'function') {
                    try {
                        captureTarget.releasePointerCapture(Number(pointerId));
                    } catch (_e) {
                        // ignore unsupported releasePointerCapture
                    }
                }
                writePhase2bLayoutState();
                renderPhase2bFloatingUi();
            };
            const endPhase2bResize = (pointerId) => {
                const phase2b = runtime.phase2b;
                if (!phase2b.resizing) {
                    return;
                }
                if (Number.isFinite(Number(pointerId))
                        && Number.isFinite(Number(phase2b.resizePointerId))
                        && Number(phase2b.resizePointerId) >= 0
                        && Number(phase2b.resizePointerId) !== Number(pointerId)) {
                    return;
                }
                phase2b.resizing = false;
                phase2b.resizePointerId = -1;
                phase2b.resizeStartX = 0;
                phase2b.resizeStartY = 0;
                phase2b.resizeStartWidth = 0;
                phase2b.resizeStartHeight = 0;
                if (phase2bResizer
                        && Number.isFinite(Number(pointerId))
                        && typeof phase2bResizer.releasePointerCapture === 'function') {
                    try {
                        phase2bResizer.releasePointerCapture(Number(pointerId));
                    } catch (_e) {
                        // ignore unsupported releasePointerCapture
                    }
                }
                writePhase2bLayoutState();
                renderPhase2bFloatingUi();
            };
            const onPhase2bPointerMove = (e) => {
                if (!e || !Number.isFinite(Number(e.pointerId))) {
                    return;
                }
                const phase2b = runtime.phase2b;
                if (phase2b.resizing && Number(phase2b.resizePointerId) === Number(e.pointerId)) {
                    if (!(phase2bCanvas instanceof HTMLElement)) {
                        return;
                    }
                    const panelNode = document.getElementById('anchorMountPanel');
                    if (!(panelNode instanceof HTMLElement)) {
                        return;
                    }
                    const minWidth = 320;
                    const minHeight = 260;
                    const maxWidth = Math.max(minWidth, panelNode.clientWidth - 10);
                    const maxHeight = Math.max(minHeight, panelNode.clientHeight - 36);
                    const deltaX = e.clientX - Number(phase2b.resizeStartX || 0);
                    const deltaY = e.clientY - Number(phase2b.resizeStartY || 0);
                    phase2b.canvasWidth = Math.max(minWidth, Math.min(maxWidth, Number(phase2b.resizeStartWidth || phase2bCanvas.offsetWidth) + deltaX));
                    phase2b.canvasHeight = Math.max(minHeight, Math.min(maxHeight, Number(phase2b.resizeStartHeight || phase2bCanvas.offsetHeight) + deltaY));
                    applyPhase2bCanvasSize(phase2bCanvas);
                    updatePhase2bInputHeight();
                    e.preventDefault();
                    return;
                }
                if (!phase2b.moving || Number(phase2b.movePointerId) !== Number(e.pointerId)) {
                    return;
                }
                if (!(phase2bDock instanceof HTMLElement)) {
                    return;
                }
                const panelNode = document.getElementById('anchorMountPanel');
                if (!(panelNode instanceof HTMLElement)) {
                    return;
                }
                const panelRect = panelNode.getBoundingClientRect();
                phase2b.moveX = e.clientX - panelRect.left - Number(phase2b.moveOffsetX || 0);
                phase2b.moveY = e.clientY - panelRect.top - Number(phase2b.moveOffsetY || 0);
                const deltaX = e.clientX - Number(phase2b.moveStartClientX || 0);
                const deltaY = e.clientY - Number(phase2b.moveStartClientY || 0);
                if (!phase2b.moveDidDrag && ((deltaX * deltaX) + (deltaY * deltaY)) > 16) {
                    phase2b.moveDidDrag = true;
                }
                applyPhase2bDockPosition(phase2bDock);
                e.preventDefault();
            };
            const onPhase2bPointerUp = (e) => {
                if (!e || !Number.isFinite(Number(e.pointerId))) {
                    return;
                }
                endPhase2bMove(e.pointerId);
                endPhase2bResize(e.pointerId);
            };
            window.addEventListener('pointermove', onPhase2bPointerMove);
            window.addEventListener('pointerup', onPhase2bPointerUp);
            window.addEventListener('pointercancel', onPhase2bPointerUp);
            phase2bDragHandle && phase2bDragHandle.addEventListener('pointerdown', (e) => {
                if (!e || !Number.isFinite(Number(e.pointerId))) {
                    return;
                }
                if (Number.isFinite(Number(e.button)) && Number(e.button) !== 0) {
                    return;
                }
                if (!runtime.phase2b.expanded) {
                    return;
                }
                const interactiveTarget = e.target instanceof Element
                    ? e.target.closest('button, input, textarea, select, a, [data-phase2b-action]')
                    : null;
                if (interactiveTarget) {
                    return;
                }
                const started = startPhase2bMove(e, phase2bDragHandle);
                if (!started) {
                    return;
                }
                e.preventDefault();
                e.stopPropagation();
            });
            phase2bCapsuleBtn && phase2bCapsuleBtn.addEventListener('pointerdown', (e) => {
                if (!e || !Number.isFinite(Number(e.pointerId))) {
                    return;
                }
                if (runtime.phase2b.expanded) {
                    return;
                }
                const started = startPhase2bMove(e, phase2bCapsuleBtn);
                if (!started) {
                    return;
                }
                e.preventDefault();
                e.stopPropagation();
            });
            phase2bResizer && phase2bResizer.addEventListener('pointerdown', (e) => {
                if (!e || !Number.isFinite(Number(e.pointerId))) {
                    return;
                }
                if (Number.isFinite(Number(e.button)) && Number(e.button) !== 0) {
                    return;
                }
                if (!runtime.phase2b.expanded || !(phase2bCanvas instanceof HTMLElement)) {
                    return;
                }
                const phase2b = runtime.phase2b;
                const currentWidth = Number(phase2b.canvasWidth);
                const currentHeight = Number(phase2b.canvasHeight);
                phase2b.resizeStartX = e.clientX;
                phase2b.resizeStartY = e.clientY;
                phase2b.resizeStartWidth = Number.isFinite(currentWidth) ? currentWidth : phase2bCanvas.offsetWidth;
                phase2b.resizeStartHeight = Number.isFinite(currentHeight) ? currentHeight : phase2bCanvas.offsetHeight;
                phase2b.resizePointerId = e.pointerId;
                phase2b.resizing = true;
                if (typeof phase2bResizer.setPointerCapture === 'function') {
                    try {
                        phase2bResizer.setPointerCapture(e.pointerId);
                    } catch (_e) {
                        // ignore unsupported setPointerCapture
                    }
                }
                renderPhase2bFloatingUi();
                e.preventDefault();
                e.stopPropagation();
            });
            phase2bCanvas && phase2bCanvas.addEventListener('dragenter', (e) => {
                if (!hasDraggedFiles(e)) {
                    return;
                }
                e.preventDefault();
                e.stopPropagation();
                runtime.phase2b.dragging = true;
                renderPhase2bFloatingUi();
            });
            phase2bCanvas && phase2bCanvas.addEventListener('dragover', (e) => {
                if (!hasDraggedFiles(e)) {
                    return;
                }
                e.preventDefault();
                e.stopPropagation();
                runtime.phase2b.dragging = true;
                renderPhase2bFloatingUi();
            });
            phase2bCanvas && phase2bCanvas.addEventListener('dragleave', (e) => {
                if (!hasDraggedFiles(e)) {
                    return;
                }
                const related = e && e.relatedTarget;
                if (related instanceof Element && phase2bCanvas.contains(related)) {
                    return;
                }
                runtime.phase2b.dragging = false;
                renderPhase2bFloatingUi();
            });
            phase2bCanvas && phase2bCanvas.addEventListener('drop', (e) => {
                if (!hasDraggedFiles(e)) {
                    return;
                }
                e.preventDefault();
                e.stopPropagation();
                runtime.phase2b.dragging = false;
                renderPhase2bFloatingUi();
                const files = Array.from(e.dataTransfer.files || []);
                attachPhase2bFiles(files);
            });
            phase2bInput && phase2bInput.addEventListener('focus', () => {
                runtime.phase2b.mode = isPhase2bRequestInFlight() ? 'processing' : 'input';
            });
            phase2bInput && phase2bInput.addEventListener('paste', (e) => {
                if (!e || !e.clipboardData) {
                    return;
                }
                const raw = String(e.clipboardData.getData('text') || '');
                const links = extractPhase2bArticleLinks(raw);
                if (!links.length) {
                    return;
                }
                e.preventDefault();
                upsertPhase2bLinkItems(links, { status: 'queued' });
                prefetchPhase2bLinkTitles(links);
                const plainText = stripPhase2bArticleLinks(raw);
                if (plainText) {
                    const editor = phase2bInput;
                    const value = String(editor.value || '');
                    const start = Number.isInteger(Number(editor.selectionStart)) ? Number(editor.selectionStart) : value.length;
                    const endRaw = Number.isInteger(Number(editor.selectionEnd)) ? Number(editor.selectionEnd) : start;
                    const end = Math.max(start, endRaw);
                    const inserted = plainText.endsWith('\n') ? plainText : `${plainText}\n`;
                    editor.value = `${value.slice(0, start)}${inserted}${value.slice(end)}`;
                    const caret = start + inserted.length;
                    editor.setSelectionRange(caret, caret);
                }
                syncPhase2bInputFromTextarea(phase2bInput);
                runtime.phase2b.inputCollapsed = true;
                if (typeof phase2bInput.blur === 'function') {
                    phase2bInput.blur();
                }
                renderPhase2bFloatingUi();
            });
            phase2bInput && phase2bInput.addEventListener('input', (e) => {
                syncPhase2bInputFromTextarea(e && e.target);
            });
            phase2bInput && phase2bInput.addEventListener('keydown', (e) => {
                if (!e) {
                    return;
                }
                const key = String(e.key || '').toLowerCase();
                const withCommand = !!(e.ctrlKey || e.metaKey) && !e.altKey;
                if (withCommand && key === 'enter') {
                    e.preventDefault();
                    submitPhase2bContent();
                    return;
                }
                if (withCommand && key === 'b') {
                    e.preventDefault();
                    applyPhase2bBoldShortcut(phase2bInput);
                    return;
                }
                if (withCommand && (key === '1' || key === '2' || key === '3')) {
                    e.preventDefault();
                    applyPhase2bHeadingShortcut(phase2bInput, Number(key));
                    return;
                }
                if (withCommand && key === 'a') {
                    e.preventDefault();
                    applyPhase2bUnorderedListShortcut(phase2bInput);
                    return;
                }
                if (withCommand && key === 'q') {
                    e.preventDefault();
                    applyPhase2bOrderedListShortcut(phase2bInput);
                }
            });
            window.addEventListener('resize', () => {
                renderPhase2bFloatingUi();
            });
        }
        document.addEventListener('click', (e) => {
            const target = elementFromEventTarget(e && e.target);
            if (runtime.phase2b.expanded) {
                if (!target || !phase2bDock || !phase2bDock.contains(target)) {
                    hidePhase2bFloatingUi();
                }
            }
            if (!obsidianCommandMenu || obsidianCommandMenu.hidden) {
                return;
            }
            if (!target) {
                closeCommandMenu();
                return;
            }
            if (obsidianCommandMenu.contains(target) || (obsidianHeader && obsidianHeader.contains(target))) {
                return;
            }
            closeCommandMenu();
        });
        searchInput && searchInput.addEventListener('input', (e) => {
            if (!runtime.activeId) {
                return;
            }
            const query = e && e.target ? e.target.value : '';
            setLocalNoteFilter(runtime.activeId, query);
            renderLocalNoteCards(runtime.activeId);
            renderObsidianKnowledgePanels(runtime.activeId);
            if (runtime.wikilinkSuggest.open) {
                renderWikilinkSuggest(runtime.activeId, { focusQuery: true });
            }
        });
        obsidianPanels && obsidianPanels.addEventListener('click', (e) => {
            if (!runtime.activeId) {
                return;
            }
            const openBtn = e && e.target && e.target.closest ? e.target.closest('[data-open-local-note-id]') : null;
            if (openBtn) {
                const noteId = t(openBtn.getAttribute('data-open-local-note-id'));
                if (noteId) {
                    switchActiveLocalNote(runtime.activeId, noteId, { focusEditor: true });
                }
                return;
            }
            const createBtn = e && e.target && e.target.closest ? e.target.closest('[data-create-note-ref]') : null;
            if (createBtn) {
                const raw = t(createBtn.getAttribute('data-create-note-ref'));
                if (raw) {
                    createLocalNoteFromWikilink(runtime.activeId, raw);
                }
                return;
            }
            const tagBtn = e && e.target && e.target.closest ? e.target.closest('[data-note-filter]') : null;
            if (tagBtn) {
                const tag = t(tagBtn.getAttribute('data-note-filter'));
                setLocalNoteFilter(runtime.activeId, tag);
                if (searchInput) {
                    searchInput.value = tag;
                }
                renderLocalNoteCards(runtime.activeId);
                renderObsidianKnowledgePanels(runtime.activeId);
            }
        });
        localCards && localCards.addEventListener('click', (e) => {
            if (!runtime.activeId) {
                return;
            }
            const deleteBtn = e.target && e.target.closest ? e.target.closest('[data-local-note-delete]') : null;
            if (deleteBtn) {
                const noteId = t(deleteBtn.getAttribute('data-local-note-delete'));
                if (!noteId) {
                    return;
                }
                persistActiveLocalNoteFromEditor(runtime.activeId, { retitle: true });
                deleteLocalNote(runtime.activeId, noteId);
                renderLocalNoteCards(runtime.activeId);
                syncEditorFromActiveLocalNote(runtime.activeId);
                return;
            }
            const createBtn = e.target && e.target.closest ? e.target.closest('[data-local-note-create]') : null;
            if (createBtn) {
                persistActiveLocalNoteFromEditor(runtime.activeId, { retitle: true });
                createLocalNote(runtime.activeId);
                renderLocalNoteCards(runtime.activeId);
                syncEditorFromActiveLocalNote(runtime.activeId);
                focusEditorInput();
                return;
            }
            const noteBtn = e.target && e.target.closest ? e.target.closest('[data-local-note-id]') : null;
            if (!noteBtn) {
                return;
            }
            const noteId = t(noteBtn.getAttribute('data-local-note-id'));
            if (!noteId) {
                return;
            }
            persistActiveLocalNoteFromEditor(runtime.activeId, { retitle: true });
            setActiveLocalNoteId(runtime.activeId, noteId);
            renderLocalNoteCards(runtime.activeId);
            syncEditorFromActiveLocalNote(runtime.activeId);
            focusEditorInput();
        });
        quickInput && quickInput.addEventListener('input', () => {
            if (!runtime.activeId) {
                return;
            }
            runtime.editorActive = true;
            runtime.editorDirty = true;
            renderLocalNoteLivePreview(runtime.activeId);
            if (runtime.wikilinkSuggest.open) {
                renderWikilinkSuggest(runtime.activeId, { focusQuery: true });
            }
            refreshObsidianStatusBar(runtime.activeId);
        });
        quickInput && quickInput.addEventListener('focus', () => {
            runtime.editorActive = true;
            if (!runtime.activeId) {
                closeWikilinkSuggest();
                return;
            }
            if (runtime.wikilinkSuggest.open) {
                renderWikilinkSuggest(runtime.activeId, { focusQuery: true });
            }
        });
        quickInput && quickInput.addEventListener('click', () => {
            if (!runtime.activeId) {
                closeWikilinkSuggest();
                return;
            }
            if (runtime.wikilinkSuggest.open) {
                renderWikilinkSuggest(runtime.activeId, { focusQuery: true });
            }
        });
        quickInput && quickInput.addEventListener('blur', () => {
            const activeAnchorId = t(runtime.activeId);
            if (activeAnchorId) {
                persistActiveLocalNoteFromEditor(activeAnchorId, { retitle: true });
                renderLocalNoteCards(activeAnchorId);
                renderLocalNoteLivePreview(activeAnchorId);
                runtime.editorDirty = false;
            }
            runtime.editorActive = false;
            setTimeout(() => {
                const focused = document.activeElement;
                if (focused && wikilinkSuggestPanel && wikilinkSuggestPanel.contains(focused)) {
                    return;
                }
                closeWikilinkSuggest();
            }, 60);
        });
        wikilinkSuggestPanel && wikilinkSuggestPanel.addEventListener('mousedown', (e) => {
            const target = e && e.target;
            if (target instanceof Element && target.closest('[data-wikilink-query]')) {
                return;
            }
            e.preventDefault();
        });
        wikilinkSuggestPanel && wikilinkSuggestPanel.addEventListener('click', (e) => {
            const target = e && e.target && e.target.closest ? e.target.closest('[data-wikilink-index]') : null;
            if (!target) {
                return;
            }
            const idx = Number(target.getAttribute('data-wikilink-index'));
            if (!Number.isInteger(idx)) {
                return;
            }
            applyWikilinkSuggestSelection(idx);
        });
        wikilinkSuggestPanel && wikilinkSuggestPanel.addEventListener('input', (e) => {
            const input = e && e.target && e.target.closest ? e.target.closest('[data-wikilink-query]') : null;
            if (!(input instanceof HTMLInputElement)) {
                return;
            }
            runtime.wikilinkSuggest.query = t(input.value);
            runtime.wikilinkSuggest.activeIndex = 0;
            renderWikilinkSuggest(runtime.wikilinkSuggest.anchorId || runtime.activeId, { focusQuery: true });
        });
        wikilinkSuggestPanel && wikilinkSuggestPanel.addEventListener('keydown', (e) => {
            const key = String(e && e.key || '');
            if (key === 'ArrowDown') {
                e.preventDefault();
                moveWikilinkSuggestActive(1);
                return;
            }
            if (key === 'ArrowUp') {
                e.preventDefault();
                moveWikilinkSuggestActive(-1);
                return;
            }
            if (key === 'Enter' || key === 'Tab') {
                e.preventDefault();
                applyWikilinkSuggestSelection(runtime.wikilinkSuggest.activeIndex);
                return;
            }
            if (key === 'Escape') {
                e.preventDefault();
                closeWikilinkSuggest();
                focusEditorInput();
            }
        });
        document.addEventListener('keydown', (e) => {
            if (!e || !isAnchorPanelDetailMode() || !isEditorFocusTarget(e.target)) {
                return;
            }
            const keyRaw = String(e.key || '');
            const key = keyRaw.toLowerCase();
            if (!e.ctrlKey && !e.metaKey && !e.altKey && runtime.activeId) {
                if (keyRaw === '/') {
                    const prev = readEditorTextBeforeCaret(1);
                    if (!prev || /\s/.test(prev)) {
                        e.preventDefault();
                        e.stopPropagation();
                        openWikilinkSuggest(runtime.activeId, 'slash');
                        return;
                    }
                }
                if (keyRaw === '[' && readEditorTextBeforeCaret(1) === '[') {
                    e.preventDefault();
                    e.stopPropagation();
                    openWikilinkSuggest(runtime.activeId, 'double_bracket');
                    return;
                }
            }
            if (runtime.wikilinkSuggest.open) {
                if (keyRaw === 'ArrowDown') {
                    e.preventDefault();
                    e.stopPropagation();
                    moveWikilinkSuggestActive(1);
                    return;
                }
                if (keyRaw === 'ArrowUp') {
                    e.preventDefault();
                    e.stopPropagation();
                    moveWikilinkSuggestActive(-1);
                    return;
                }
                if (keyRaw === 'Enter' || keyRaw === 'Tab') {
                    e.preventDefault();
                    e.stopPropagation();
                    applyWikilinkSuggestSelection(runtime.wikilinkSuggest.activeIndex);
                    return;
                }
                if (keyRaw === 'Escape') {
                    e.preventDefault();
                    e.stopPropagation();
                    closeWikilinkSuggest();
                    return;
                }
            }
            if ((!e.ctrlKey && !e.metaKey) || e.altKey) {
                return;
            }
            if (key === 's') {
                if (!runtime.activeId) {
                    return;
                }
                e.preventDefault();
                e.stopPropagation();
                persistActiveLocalNoteFromEditor(runtime.activeId, { retitle: true });
                renderLocalNoteCards(runtime.activeId);
                runtime.editorDirty = false;
                setPreview('Local markdown saved');
                return;
            }
            if (!runtime.activeId) {
                return;
            }
            if (key === 'b') {
                e.preventDefault();
                e.stopPropagation();
                applyBoldShortcut();
                return;
            }
            if (key === '1' || key === '2' || key === '3') {
                e.preventDefault();
                e.stopPropagation();
                applyHeadingShortcut();
                return;
            }
            if (key === 'a') {
                e.preventDefault();
                e.stopPropagation();
                applyUnorderedListShortcut();
                return;
            }
            if (key === 'q') {
                e.preventDefault();
                e.stopPropagation();
                applyOrderedListShortcut();
            }
        }, true);
        document.addEventListener('keydown', (e) => {
            if (!e || !e.ctrlKey || !e.altKey || String(e.key).toLowerCase() !== 'a') {
                return;
            }
            if (isEditableTarget(e.target)) {
                return;
            }
            const selectedText = readSelectedMarkdownText();
            if (!selectedText) {
                return;
            }
            e.preventDefault();
            mountAnchorBySelectionText(selectedText, 'shortcut_ctrl_alt_a');
        }, true);
        document.addEventListener('keydown', (e) => {
            if (!e || String(e.key) !== 'Escape') {
                return;
            }
            if (runtime.phase2b && runtime.phase2b.expanded) {
                hidePhase2bFloatingUi();
                e.preventDefault();
                return;
            }
            const hoverCard = document.getElementById('anchorObsidianHoverCard');
            if (hoverCard && !hoverCard.hidden) {
                hideObsidianHoverCardNow();
                e.preventDefault();
                return;
            }
            const commandMenu = document.getElementById('anchorObsidianCommandMenu');
            if (commandMenu && !commandMenu.hidden) {
                commandMenu.hidden = true;
                e.preventDefault();
                return;
            }
            if (runtime.wikilinkSuggest.open) {
                closeWikilinkSuggest();
                return;
            }
            if (runtime.manualFullscreen && runtime.activeId) {
                runtime.manualFullscreen = false;
                setPanel(true);
                e.preventDefault();
                return;
            }
            const modal = document.getElementById('anchorObsidianGraphModal');
            if (!modal || modal.hidden) {
                return;
            }
            e.preventDefault();
            closeObsidianGraphModal();
        });
        document.addEventListener('mobile-anchor-mount-selection', (e) => {
            const detail = e && e.detail && typeof e.detail === 'object' ? e.detail : {};
            const selectedText = t(detail.text);
            if (!selectedText) {
                return;
            }
            mountAnchorBySelectionText(selectedText, t(detail.source) || 'selection_popover', t(detail.anchorId));
        });
        document.addEventListener('mobile-anchor-upsert', (e) => {
            const detail = e && e.detail && typeof e.detail === 'object' ? e.detail : {};
            const anchorId = t(detail.anchorId);
            const anchorRaw = detail.anchor && typeof detail.anchor === 'object' ? detail.anchor : null;
            if (!anchorId || !anchorRaw) {
                return;
            }
            runtime.anchors.set(anchorId, sanitizeAnchor(anchorId, anchorRaw));
            rebuildCandidates();
            refreshViews();
            const shouldSelect = !!detail.select;
            if (shouldSelect) {
                selectAnchor(anchorId, t(detail.source) || 'anchor_upsert', true);
            }
        });
        mainNoteSelect && mainNoteSelect.addEventListener('change', (e) => {
            if (!runtime.activeId) return;
            const selected = normalizePath(e && e.target ? e.target.value : '');
            if (!selected) return;
            runtime.pendingMainByAnchor.set(runtime.activeId, selected);
        });
        preview && preview.addEventListener('click', (e) => {
            hideObsidianHoverCardNow();
            const link = e && e.target && e.target.closest ? e.target.closest('a[href]') : null;
            if (!link || !runtime.activeId) return;
            const hrefRaw = String(link.getAttribute('href') || '').trim();
            if (!hrefRaw) return;
            if (hrefRaw.startsWith('#')) return;
            const schemeMatch = hrefRaw.match(/^([a-zA-Z][a-zA-Z0-9+\-.]*):/);
            if (schemeMatch) {
                const scheme = schemeMatch[1].toLowerCase();
                if (scheme === 'http' || scheme === 'https' || scheme === 'mailto') {
                    link.setAttribute('target', '_blank');
                    link.setAttribute('rel', 'noopener noreferrer');
                    return;
                }
                return;
            }
            const href = decodePath(hrefRaw.split('#')[0].split('?')[0]);
            const notePath = normalizePath(href);
            if (!isMarkdown(notePath)) return;
            e.preventDefault();
            runtime.pendingMainByAnchor.set(runtime.activeId, notePath);
            loadMounted(runtime.activeId, notePath, 'mounted_link');
        });
        preview && preview.addEventListener('mouseover', handlePreviewLinkMouseOver);
        preview && preview.addEventListener('mouseout', handlePreviewLinkMouseOut);
        localLivePreview && localLivePreview.addEventListener('click', (e) => {
            hideObsidianHoverCardNow();
            const link = e && e.target && e.target.closest ? e.target.closest('a[href]') : null;
            if (!link || !runtime.activeId) return;
            const hrefRaw = String(link.getAttribute('href') || '').trim();
            if (!hrefRaw) return;
            if (hrefRaw.startsWith('/__obsidian_note__/')) {
                e.preventDefault();
                const encodedId = hrefRaw.replace(/^\/__obsidian_note__\//, '').split('?')[0];
                const noteId = decodePath(encodedId);
                if (noteId) {
                    switchActiveLocalNote(runtime.activeId, noteId, { focusEditor: true });
                }
                return;
            }
            if (hrefRaw.startsWith('/__obsidian_create__/')) {
                e.preventDefault();
                const encodedRaw = hrefRaw.replace(/^\/__obsidian_create__\//, '');
                createLocalNoteFromWikilink(runtime.activeId, decodePath(encodedRaw));
                return;
            }
            const schemeMatch = hrefRaw.match(/^([a-zA-Z][a-zA-Z0-9+\-.]*):/);
            if (schemeMatch) {
                const scheme = schemeMatch[1].toLowerCase();
                if (scheme === 'http' || scheme === 'https' || scheme === 'mailto') {
                    link.setAttribute('target', '_blank');
                    link.setAttribute('rel', 'noopener noreferrer');
                    return;
                }
                e.preventDefault();
                return;
            }
            const decoded = decodePath(hrefRaw.split('#')[0].split('?')[0]);
            const notePath = normalizePath(decoded);
            if (!isMarkdown(notePath)) {
                return;
            }
            e.preventDefault();
            const notes = ensureLocalNotes(runtime.activeId);
            const matched = notes.find((note) => normalizePath(note.fileName).toLowerCase() === notePath.toLowerCase());
            if (matched) {
                switchActiveLocalNote(runtime.activeId, matched.id, { focusEditor: true });
                return;
            }
            upsertLocalNoteFromMarkdownFile(runtime.activeId, notePath, `# ${noteNameFromPath(notePath) || 'Note'}\n\n`);
            renderLocalNoteCards(runtime.activeId);
            syncEditorFromActiveLocalNote(runtime.activeId);
            renderObsidianKnowledgePanels(runtime.activeId);
        });
        localLivePreview && localLivePreview.addEventListener('mouseover', handlePreviewLinkMouseOver);
        localLivePreview && localLivePreview.addEventListener('mouseout', handlePreviewLinkMouseOut);
        const vditorHostForHover = document.getElementById('anchorQuickNoteVditor');
        vditorHostForHover && vditorHostForHover.addEventListener('mouseover', handlePreviewLinkMouseOver);
        vditorHostForHover && vditorHostForHover.addEventListener('mouseout', handlePreviewLinkMouseOut);
        fileList && fileList.addEventListener('click', (e) => {
            const btn = e.target && e.target.closest ? e.target.closest('[data-remove-index]') : null;
            if (!btn || !runtime.activeId) return;
            const idx = Number(btn.getAttribute('data-remove-index'));
            if (!Number.isInteger(idx)) return;
            const cur = pendingList(runtime.activeId).slice();
            if (idx >= 0 && idx < cur.length) {
                cur.splice(idx, 1);
                runtime.pendingByAnchor.set(runtime.activeId, cur);
                renderPending(runtime.activeId);
            }
        });
        closeBtn && closeBtn.addEventListener('click', () => { setPanel(false); closePanel(); });
        backBtn && backBtn.addEventListener('click', () => setPanel(false));
        window.addEventListener('resize', () => hideObsidianHoverCardNow());
        window.addEventListener('scroll', () => {
            const card = document.getElementById('anchorObsidianHoverCard');
            if (card && !card.hidden) {
                scheduleHideObsidianHoverCard(60);
            }
        }, { passive: true });

        if (layout) {
            const hasFiles = (e) => {
                const dt = e.dataTransfer;
                return !!dt && Array.from(dt.types || []).includes('Files');
            };
            const setDropzoneVisible = (visible) => {
                if (!dropzone) return;
                dropzone.classList.toggle('is-dragover', !!visible);
                dropzone.setAttribute('aria-hidden', visible ? 'false' : 'true');
            };
            layout.addEventListener('dragenter', (e) => {
                if (!hasFiles(e)) return;
                runtime.dragDepth += 1;
                setDropzoneVisible(true);
            });
            layout.addEventListener('dragover', (e) => {
                if (!hasFiles(e)) return;
                const id = findId(e.target) || runtime.activeId;
                if (!id) return;
                e.preventDefault();
                runtime.dropId = id;
                rebuildCandidates();
                refreshViews();
                setDropzoneVisible(true);
            });
            layout.addEventListener('dragleave', (e) => {
                if (!hasFiles(e)) return;
                runtime.dragDepth = Math.max(0, runtime.dragDepth - 1);
                if (!runtime.dragDepth) {
                    runtime.dropId = '';
                    rebuildCandidates();
                    refreshViews();
                    setDropzoneVisible(false);
                }
            });
            layout.addEventListener('drop', async (e) => {
                if (!hasFiles(e)) return;
                e.preventDefault();
                runtime.dragDepth = 0;
                const id = findId(e.target) || runtime.activeId;
                runtime.dropId = '';
                setDropzoneVisible(false);
                rebuildCandidates();
                refreshViews();
                if (!id) return setPreview(TEXT.noSelection);
                const droppedFiles = Array.from(e.dataTransfer.files || []);
                selectAnchor(id, 'drag_drop', false);
                await ingestMarkdownFilesToLocalNotes(id, droppedFiles);
                runtime.pendingByAnchor.set(id, []);
                renderPending(id);
            });
        }
    }

    function scheduleRematch() {
        if (runtime.rematchTimer) clearTimeout(runtime.rematchTimer);
        runtime.rematchTimer = setTimeout(() => {
            runtime.rematchTimer = 0;
            rebuildCandidates();
            refreshViews();
        }, 120);
    }

    function bindObserver() {
        const body = document.getElementById('markdownBody');
        if (!body) return;
        if (runtime.observer) runtime.observer.disconnect();
        runtime.observer = new MutationObserver(() => scheduleRematch());
        runtime.observer.observe(body, { childList: true, subtree: true, characterData: true });
    }

    function bindContextSync() {
        const sync = async (force) => {
            const next = getCtx();
            const key = `${next.apiBase}|${next.taskId}|${next.pathHint}`;
            if (!force && key === runtime.ctxKey) return;
            runtime.ctx = next;
            runtime.ctxKey = key;
            runtime.activeId = '';
            runtime.pendingByAnchor.clear();
            runtime.pendingMainByAnchor.clear();
            runtime.mountedPayloadByAnchor.clear();
            runtime.mountedNoteByAnchor.clear();
            runtime.localNotesByAnchor.clear();
            runtime.activeLocalNoteIdByAnchor.clear();
            runtime.localNoteLayoutModeByAnchor.clear();
            runtime.localSyncShadowByAnchor.clear();
            runtime.localSyncDirtyByAnchor.clear();
            runtime.syncInFlightByAnchor.clear();
            closeWikilinkSuggest();
            closeObsidianGraphModal();
            setPanel(false);
            closePanel();
            resetPhase2bForContextChange();
            await fetchMeta(true);
        };
        sync(true);
        if (runtime.ctxTimer) clearInterval(runtime.ctxTimer);
        runtime.ctxTimer = setInterval(() => sync(false), 1600);
    }

    function init() {
        if (!document.getElementById('anchorIndexList') || !document.getElementById('anchorMountPanel')) return;
        bindEvents();
        bindObserver();
        bindContextSync();
        flushIncrementalLocalNoteSync();
        if (runtime.syncTimer) clearInterval(runtime.syncTimer);
        runtime.syncTimer = setInterval(() => {
            flushIncrementalLocalNoteSync();
        }, 20000);
        setPreview(TEXT.previewEmpty);
        window.addEventListener('beforeunload', () => {
            flushIncrementalLocalNoteSync();
            clearPhase2bProgressState();
            closePhase2bWebSocket();
            if (runtime.ctxTimer) clearInterval(runtime.ctxTimer);
            if (runtime.syncTimer) clearInterval(runtime.syncTimer);
            if (runtime.observer) runtime.observer.disconnect();
        });
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
