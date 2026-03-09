(() => {
    if (window.__mobileAnchorPanelReady) {
        return;
    }
    window.__mobileAnchorPanelReady = true;
    const ANCHOR_PANEL_BUILD = '20260307-phase2b-toolbar-bottom-row';
    if (typeof console !== 'undefined' && typeof console.info === 'function') {
        console.info(`[mobile-anchor-panel] build=${ANCHOR_PANEL_BUILD}`);
    }

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
        touchMode: false,
        collectionRootOrder: [],
        collections: new Map(),
        collectionByAnchor: new Map(),
        collectionSeq: 0,
        selectionMode: false,
        selectedAnchors: new Set(),
        selectionPivotAnchorId: '',
        selectedCollectionTargetId: '',
        collectionDrag: {
            sourceType: '',
            sourceId: '',
            sourceCollectionId: '',
            batchAnchorIds: [],
            hoverTargetType: '',
            hoverTargetId: '',
            hoverMode: '',
            groupReady: false,
            groupHoldTimer: 0,
            groupHoldKey: '',
            groupHoldStartedAt: 0,
            autoExpandCollectionId: '',
            autoExpandTimer: 0,
            autoScrollTimer: 0,
            autoScrollDelta: 0,
            longPressTimer: 0,
            manualPointerId: -1,
            manualSourceType: '',
            manualSourceId: '',
            manualSourceCollectionId: '',
            manualStartX: 0,
            manualStartY: 0,
            manualArmedAt: 0,
            manualArmed: false,
            manualDragging: false,
            manualRequireHold: false,
            manualHoldReady: false,
            manualHoldTimer: 0,
            manualSuppressClickUntil: 0,
            pulseCollectionId: '',
            pulseUntil: 0,
        },
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
    const COLLECTION_GROUP_HOLD_MS = 300;
    const COLLECTION_GROUP_HOLD_WEB_MS = 500;
    const COLLECTION_AUTO_EXPAND_MS = 260;
    const COLLECTION_AUTOSCROLL_THRESHOLD_PX = 56;
    const COLLECTION_AUTOSCROLL_MAX_STEP = 22;
    const COLLECTION_AUTOSCROLL_INTERVAL_MS = 18;
    const COLLECTION_LONG_PRESS_MS = 420;
    const COLLECTION_WEB_LONG_PRESS_MS = 320;
    const COLLECTION_PREVIEW_LIMIT = 3;
    const COLLECTION_CHILD_PREVIEW_LIMIT = 2;
    const LOCAL_NOTE_SYNC_INTERVAL_MS = 5000;

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


    function getEditorShortcutUtils() {
        return window.MobileEditorShortcuts || null;
    }


    function buildAnchorEditorCommandKeymap() {
        return [
            {
                combo: ['Ctrl+S', 'Meta+S'],
                when: (context) => !!context.activeId,
                run: (context) => {
                    persistActiveLocalNoteFromEditor(context.activeId, { retitle: true });
                    renderLocalNoteCards(context.activeId);
                    runtime.editorDirty = false;
                    setPreview('Local markdown saved');
                    return true;
                },
            },
            {
                combo: ['Ctrl+D', 'Meta+D'],
                when: (context) => !!context.activeId,
                run: () => {
                    applyDeleteParagraphShortcut();
                    return true;
                },
            },
            {
                combo: ['Ctrl+B', 'Meta+B'],
                when: (context) => !!context.activeId,
                run: () => {
                    applyBoldShortcut();
                    return true;
                },
            },
            {
                combo: ['Ctrl+1', 'Meta+1'],
                when: (context) => !!context.activeId,
                run: () => {
                    applyHeadingShortcut(3);
                    return true;
                },
            },
            {
                combo: ['Ctrl+2', 'Meta+2'],
                when: (context) => !!context.activeId,
                run: () => {
                    applyHeadingShortcut(4);
                    return true;
                },
            },
            {
                combo: ['Ctrl+3', 'Meta+3'],
                when: (context) => !!context.activeId,
                run: () => {
                    applyHeadingShortcut(5);
                    return true;
                },
            },
            {
                combo: ['Ctrl+A', 'Meta+A'],
                when: (context) => !!context.activeId,
                run: () => {
                    applyUnorderedListShortcut();
                    return true;
                },
            },
            {
                combo: ['Ctrl+Q', 'Meta+Q'],
                when: (context) => !!context.activeId,
                run: () => {
                    applyOrderedListShortcut();
                    return true;
                },
            },
        ];
    }

    function runAnchorEditorCommandKeymap(event) {
        const shortcutUtils = getEditorShortcutUtils();
        if (!(shortcutUtils && typeof shortcutUtils.runKeymap === 'function')) {
            return false;
        }
        return shortcutUtils.runKeymap(event, buildAnchorEditorCommandKeymap(), {
            activeId: runtime.activeId,
        });
    }


    function handleAnchorEditorIndentKeydown(event) {
        if (!event || event.ctrlKey || event.metaKey || event.altKey || String(event.key || '') !== 'Tab') {
            return false;
        }
        if (!isAnchorPanelDetailMode() || !isEditorFocusTarget(event.target) || !runtime.activeId) {
            return false;
        }
        const shortcutUtils = getEditorShortcutUtils();
        if (shortcutUtils && typeof shortcutUtils.consumeKeyEvent === 'function') {
            shortcutUtils.consumeKeyEvent(event);
        } else {
            event.preventDefault();
            event.stopPropagation();
            if (typeof event.stopImmediatePropagation === 'function') {
                event.stopImmediatePropagation();
            }
        }
        if (runtime.wikilinkSuggest.open) {
            closeWikilinkSuggest();
        }
        applyParagraphIndentShortcut(event.shiftKey);
        return true;
    }

    function bindAnchorEditorIndentInterceptors() {
        const shortcutUtils = getEditorShortcutUtils();
        const targets = [
            document.getElementById('anchorQuickNoteInput'),
            document.getElementById('anchorQuickNoteVditor'),
        ];
        targets.forEach((target) => {
            if (!(target instanceof HTMLElement)) {
                return;
            }
            if (shortcutUtils && typeof shortcutUtils.bindKeymap === 'function') {
                shortcutUtils.bindKeymap(target, [
                    {
                        combo: ['Tab', 'Shift+Tab'],
                        when: () => isAnchorPanelDetailMode() && isEditorFocusTarget(target) && !!runtime.activeId,
                        run: (_context, event) => {
                            if (runtime.wikilinkSuggest.open) {
                                closeWikilinkSuggest();
                            }
                            applyParagraphIndentShortcut(event.shiftKey);
                            return true;
                        },
                    },
                ], { marker: 'anchorIndentInterceptBound' });
                return;
            }
            if (target.dataset.anchorIndentInterceptBound === '1') {
                return;
            }
            target.dataset.anchorIndentInterceptBound = '1';
            target.addEventListener('keydown', (event) => {
                handleAnchorEditorIndentKeydown(event);
            }, true);
        });
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

    function applyHeadingShortcut(levelLike) {
        const level = Math.max(1, Math.min(6, Number(levelLike) || 1));
        if (isVditorReady()) {
            if (runVditorExecCommand('formatBlock', `<h${level}>`)) {
                return true;
            }
            return runVditorExecCommand('formatBlock', `h${level}`);
        }
        const shortcutUtils = getEditorShortcutUtils();
        const editor = document.getElementById('anchorQuickNoteInput');
        if (shortcutUtils && typeof shortcutUtils.applyHeadingMutation === 'function' && editor instanceof HTMLTextAreaElement) {
            const next = shortcutUtils.applyHeadingMutation({
                value: String(editor.value || ''),
                start: editor.selectionStart,
                end: editor.selectionEnd,
                level,
            });
            if (next && typeof next.value === 'string') {
                editor.value = next.value;
                editor.setSelectionRange(Number(next.start) || 0, Number(next.end) || Number(next.start) || 0);
                dispatchTextareaInputEvent();
                return true;
            }
        }
        const marker = `${'#'.repeat(level)} `;
        return transformSelectedLinesInTextarea((line) => {
            const normalized = normalizeMarkdownLineBody(line);
            return `${normalized.indent}${marker}${normalized.body}`;
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

    function clampEditorOffset(offsetLike, maxLike) {
        const max = Math.max(0, Number(maxLike) || 0);
        const offset = Number(offsetLike);
        if (!Number.isFinite(offset)) {
            return 0;
        }
        return Math.max(0, Math.min(max, Math.floor(offset)));
    }

    function resolveVditorEditableRoot() {
        if (!isVditorReady() || !runtime.vditorInstance) {
            return null;
        }
        const core = runtime.vditorInstance.vditor;
        const mode = typeof runtime.vditorInstance.getCurrentMode === 'function'
            ? String(runtime.vditorInstance.getCurrentMode() || 'ir')
            : 'ir';
        if (core && mode === 'sv' && core.sv && core.sv.element instanceof HTMLElement) {
            return core.sv.element;
        }
        if (core && mode === 'wysiwyg' && core.wysiwyg && core.wysiwyg.element instanceof HTMLElement) {
            return core.wysiwyg.element;
        }
        if (core && core.ir && core.ir.element instanceof HTMLElement) {
            return core.ir.element;
        }
        return document.querySelector('#anchorQuickNoteVditor .vditor-ir');
    }

    function resolveVditorSelectionOffsets(valueLike) {
        const value = String(valueLike || '');
        const max = value.length;
        const root = resolveVditorEditableRoot();
        const selection = window.getSelection ? window.getSelection() : null;
        if (!root || !selection || selection.rangeCount <= 0) {
            return null;
        }
        const range = selection.getRangeAt(0);
        if (!root.contains(range.startContainer) || !root.contains(range.endContainer)) {
            return null;
        }
        const startProbe = range.cloneRange();
        startProbe.selectNodeContents(root);
        startProbe.setEnd(range.startContainer, range.startOffset);
        const endProbe = range.cloneRange();
        endProbe.selectNodeContents(root);
        endProbe.setEnd(range.endContainer, range.endOffset);
        const start = clampEditorOffset(String(startProbe.toString() || '').length, max);
        const endRaw = clampEditorOffset(String(endProbe.toString() || '').length, max);
        return {
            start,
            end: Math.max(start, endRaw),
        };
    }

    function readEditorSelectionOffsets(valueLike) {
        const value = String(valueLike || readEditorValue() || '');
        const max = value.length;
        if (isVditorReady()) {
            const vditorOffsets = resolveVditorSelectionOffsets(value);
            if (vditorOffsets) {
                return vditorOffsets;
            }
        }
        const editor = document.getElementById('anchorQuickNoteInput');
        const startRaw = Number(editor && editor.selectionStart);
        const endRaw = Number(editor && editor.selectionEnd);
        const start = clampEditorOffset(Number.isInteger(startRaw) ? startRaw : 0, max);
        const end = clampEditorOffset(Number.isInteger(endRaw) ? endRaw : start, max);
        return {
            start,
            end: Math.max(start, end),
        };
    }

    function resolveVditorPointByOffset(root, offsetLike) {
        const offset = Math.max(0, Number(offsetLike) || 0);
        const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT, null);
        const textNodes = [];
        while (walker.nextNode()) {
            textNodes.push(walker.currentNode);
        }
        if (!textNodes.length) {
            return { node: root, offset: 0 };
        }
        let targetNode = textNodes[textNodes.length - 1];
        let targetOffset = String(targetNode.textContent || '').length;
        for (let i = 0; i < textNodes.length; i += 1) {
            const node = textNodes[i];
            const nodeTextLength = String(node.textContent || '').length;
            const probe = document.createRange();
            probe.selectNodeContents(root);
            probe.setEnd(node, nodeTextLength);
            const measuredLength = String(probe.toString() || '').length;
            if (offset > measuredLength) {
                continue;
            }
            let low = 0;
            let high = nodeTextLength;
            while (low < high) {
                const middle = Math.floor((low + high) / 2);
                const innerProbe = document.createRange();
                innerProbe.selectNodeContents(root);
                innerProbe.setEnd(node, middle);
                const middleLength = String(innerProbe.toString() || '').length;
                if (middleLength < offset) {
                    low = middle + 1;
                } else {
                    high = middle;
                }
            }
            targetNode = node;
            targetOffset = low;
            break;
        }
        return { node: targetNode, offset: targetOffset };
    }

    function setVditorSelectionOffsets(startLike, endLike) {
        const root = resolveVditorEditableRoot();
        const selection = window.getSelection ? window.getSelection() : null;
        if (!root || !selection) {
            return false;
        }
        const start = Math.max(0, Number(startLike) || 0);
        const end = Math.max(start, Number(endLike) || start);
        try {
            const startPoint = resolveVditorPointByOffset(root, start);
            const endPoint = resolveVditorPointByOffset(root, end);
            const range = document.createRange();
            range.setStart(startPoint.node, startPoint.offset);
            range.setEnd(endPoint.node, endPoint.offset);
            selection.removeAllRanges();
            selection.addRange(range);
            return true;
        } catch (_e) {
            return false;
        }
    }

    function applyEditorTextMutation(mutator) {
        if (typeof mutator !== 'function') {
            return false;
        }
        const value = readEditorValue();
        const selection = readEditorSelectionOffsets(value);
        const next = mutator({
            value,
            start: selection.start,
            end: selection.end,
        });
        if (!next || typeof next.value !== 'string') {
            return false;
        }
        const nextValue = String(next.value || '');
        const max = nextValue.length;
        const nextStartRaw = Number(next.start);
        const nextStart = clampEditorOffset(
            Number.isInteger(nextStartRaw) ? nextStartRaw : selection.start,
            max
        );
        const nextEndRaw = Number(next.end);
        const nextEnd = clampEditorOffset(
            Number.isInteger(nextEndRaw) ? nextEndRaw : nextStart,
            max
        );
        writeEditorValue(nextValue);
        const editor = document.getElementById('anchorQuickNoteInput');
        if (editor instanceof HTMLTextAreaElement) {
            editor.value = nextValue;
            editor.setSelectionRange(nextStart, Math.max(nextStart, nextEnd));
        }
        if (isVditorReady()) {
            if (runtime.vditorInstance && typeof runtime.vditorInstance.focus === 'function') {
                runtime.vditorInstance.focus();
            }
            setVditorSelectionOffsets(nextStart, Math.max(nextStart, nextEnd));
        }
        dispatchTextareaInputEvent();
        return true;
    }

    function resolveParagraphRangeByOffset(valueLike, offsetLike) {
        const value = String(valueLike || '');
        const max = value.length;
        if (!max) {
            return { start: 0, end: 0 };
        }
        const offset = clampEditorOffset(offsetLike, max);
        const lineStart = value.lastIndexOf('\n', Math.max(0, offset - 1)) + 1;
        let lineEnd = value.indexOf('\n', offset);
        if (lineEnd < 0) {
            lineEnd = max;
        }
        const currentLine = value.slice(lineStart, lineEnd).replace(/\r$/, '');
        if (!currentLine.trim()) {
            return { start: lineStart, end: lineEnd };
        }
        let paragraphStart = lineStart;
        while (paragraphStart > 0) {
            const prevLineEnd = paragraphStart - 1;
            const prevLineStart = value.lastIndexOf('\n', Math.max(0, prevLineEnd - 1)) + 1;
            const prevLine = value.slice(prevLineStart, prevLineEnd).replace(/\r$/, '');
            if (!prevLine.trim()) {
                break;
            }
            paragraphStart = prevLineStart;
        }
        let paragraphEnd = lineEnd;
        while (paragraphEnd < max) {
            const nextLineStart = paragraphEnd + 1;
            let nextLineEnd = value.indexOf('\n', nextLineStart);
            if (nextLineEnd < 0) {
                nextLineEnd = max;
            }
            const nextLine = value.slice(nextLineStart, nextLineEnd).replace(/\r$/, '');
            if (!nextLine.trim()) {
                break;
            }
            paragraphEnd = nextLineEnd;
        }
        return {
            start: paragraphStart,
            end: paragraphEnd,
        };
    }

    function mapParagraphLines(textLike, mapper) {
        const lines = String(textLike || '').split('\n');
        if (typeof mapper !== 'function') {
            return lines.join('\n');
        }
        return lines
            .map((line, index) => mapper(String(line || ''), index, lines.length))
            .join('\n');
    }

    function applyParagraphIndentShortcut(outdentLike) {
        const outdent = !!outdentLike;
        return applyEditorTextMutation(({ value, start, end }) => {
            const shortcutUtils = getEditorShortcutUtils();
            if (shortcutUtils && typeof shortcutUtils.applyParagraphIndentMutation === 'function') {
                return shortcutUtils.applyParagraphIndentMutation({
                    value,
                    start,
                    end,
                    outdent,
                });
            }
            const indentUnit = '    ';
            const range = resolveParagraphRangeByOffset(value, start);
            const paragraph = value.slice(range.start, range.end);
            const localCaret = Math.max(0, Math.min(paragraph.length, start - range.start));
            const lines = paragraph.split('\n');
            let consumed = 0;
            let caretLineIndex = 0;
            let caretColumn = 0;
            for (let index = 0; index < lines.length; index += 1) {
                const lineLength = String(lines[index] || '').length;
                const lineEnd = consumed + lineLength;
                if (localCaret <= lineEnd || index === lines.length - 1) {
                    caretLineIndex = index;
                    caretColumn = Math.max(0, Math.min(lineLength, localCaret - consumed));
                    break;
                }
                consumed = lineEnd + 1;
            }
            const transformedLines = lines.map((line) => {
                const source = String(line || '');
                if (outdent) {
                    return source.startsWith(indentUnit) ? source.slice(indentUnit.length) : source;
                }
                return `${indentUnit}${source}`;
            });
            let nextCaretLocal = 0;
            for (let index = 0; index < transformedLines.length; index += 1) {
                if (index < caretLineIndex) {
                    nextCaretLocal += transformedLines[index].length + 1;
                    continue;
                }
                const currentLine = String(lines[index] || '');
                let nextColumn = caretColumn;
                if (outdent) {
                    if (currentLine.startsWith(indentUnit)) {
                        nextColumn = Math.max(0, caretColumn - indentUnit.length);
                    }
                } else {
                    nextColumn = caretColumn + indentUnit.length;
                }
                nextCaretLocal += Math.max(0, Math.min(transformedLines[index].length, nextColumn));
                break;
            }
            const nextParagraph = transformedLines.join('\n');
            const nextValue = `${value.slice(0, range.start)}${nextParagraph}${value.slice(range.end)}`;
            const nextCaret = range.start + nextCaretLocal;
            return {
                value: nextValue,
                start: nextCaret,
                end: nextCaret,
            };
        });
    }

    function applyDeleteParagraphShortcut() {
        return applyEditorTextMutation(({ value, start }) => {
            if (!value) {
                return {
                    value: '',
                    start: 0,
                    end: 0,
                };
            }
            const range = resolveParagraphRangeByOffset(value, start);
            const left = value.slice(0, range.start).replace(/\n+$/, '');
            const right = value.slice(range.end).replace(/^\n+/, '');
            const glue = left && right ? '\n\n' : '';
            const nextValue = `${left}${glue}${right}`;
            const nextCaret = left
                ? (right ? left.length + glue.length : left.length)
                : 0;
            return {
                value: nextValue,
                start: nextCaret,
                end: nextCaret,
            };
        });
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
            inputSkeleton: document.getElementById('anchorPhase2bInputSkeleton'),
            pasteSubmitBtn: document.getElementById('anchorPhase2bPasteSubmitBtn'),
            inlineCopyBtn: document.getElementById('anchorPhase2bInlineCopyBtn'),
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
                    runtime.phase2b.streamChunkCount = 0;
                    if (finalMarkdown) {
                        runtime.phase2b.inputValue = finalMarkdown;
                        runtime.phase2b.resultValue = finalMarkdown;
                    }
                    renderPhase2bFloatingUi();
                    return;
                }
                const chunkText = String(payload.chunk || '');
                const isDone = payload.done === true;
                if (chunkText) {
                    runtime.phase2b.streamActive = true;
                }
                if (isDone) {
                    runtime.phase2b.streamActive = false;
                    runtime.phase2b.streamChunkCount = 0;
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

    function getPhase2bDockVisualMetrics(dockLike) {
        const dock = dockLike instanceof HTMLElement ? dockLike : document.getElementById('anchorPhase2bDock');
        if (!(dock instanceof HTMLElement)) {
            return {
                width: 0,
                height: 0,
                offsetX: 0,
                offsetY: 0,
            };
        }
        const phase2b = runtime.phase2b && typeof runtime.phase2b === 'object' ? runtime.phase2b : {};
        const canvas = document.getElementById('anchorPhase2bCanvas');
        const capsuleBtn = document.getElementById('anchorPhase2bCapsuleBtn');
        const visibleNode = phase2b.expanded && canvas instanceof HTMLElement && !canvas.hidden
            ? canvas
            : (capsuleBtn instanceof HTMLElement ? capsuleBtn : dock);
        const width = Math.max(0, Number(visibleNode.offsetWidth || dock.offsetWidth || 0));
        const height = Math.max(0, Number(visibleNode.offsetHeight || dock.offsetHeight || 0));
        if (visibleNode === dock) {
            return {
                width,
                height,
                offsetX: 0,
                offsetY: 0,
            };
        }
        const dockRect = dock.getBoundingClientRect();
        const visibleRect = visibleNode.getBoundingClientRect();
        return {
            width,
            height,
            offsetX: visibleRect.left - dockRect.left,
            offsetY: visibleRect.top - dockRect.top,
        };
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
        const visualMetrics = getPhase2bDockVisualMetrics(dock);
        const clampDockOffset = (rawX, rawY) => {
            const minX = Math.min(0, Math.round(-Number(visualMetrics.offsetX || 0)));
            const minY = Math.min(0, Math.round(-Number(visualMetrics.offsetY || 0)));
            const maxX = Math.max(minX, Math.round(panel.clientWidth - Number(visualMetrics.width || 0) - Number(visualMetrics.offsetX || 0)));
            const maxY = Math.max(minY, Math.round(panel.clientHeight - Number(visualMetrics.height || 0) - Number(visualMetrics.offsetY || 0)));
            return {
                x: Math.min(maxX, Math.max(minX, Math.round(Number(rawX) || 0))),
                y: Math.min(maxY, Math.max(minY, Math.round(Number(rawY) || 0))),
            };
        };
        const resolveDefaultDockOffset = () => {
            const rightMargin = Math.max(10, Math.min(24, Math.round(panel.clientWidth * 0.02)));
            let preferredY = Math.round((panel.clientHeight - Number(visualMetrics.height || 0)) * 0.5);
            const editorShell = document.getElementById('anchorComposerShell');
            if (editorShell instanceof HTMLElement) {
                const panelRect = panel.getBoundingClientRect();
                const editorRect = editorShell.getBoundingClientRect();
                if (editorRect.height > 0 && editorRect.width > 0) {
                    const relativeTop = editorRect.top - panelRect.top;
                    const editorCenterY = relativeTop + (editorRect.height * 0.5);
                    preferredY = Math.round(editorCenterY - (Number(visualMetrics.height || 0) * 0.5));
                }
            }
            return clampDockOffset(
                panel.clientWidth - rightMargin - Number(visualMetrics.width || 0) - Number(visualMetrics.offsetX || 0),
                preferredY - Number(visualMetrics.offsetY || 0),
            );
        };
        const phase2b = runtime.phase2b;
        const x = Number(phase2b.moveX);
        const y = Number(phase2b.moveY);
        const hasManualPosition = Number.isFinite(x) && Number.isFinite(y);
        if (!hasManualPosition) {
            const defaultOffset = resolveDefaultDockOffset();
            dock.style.left = `${defaultOffset.x}px`;
            dock.style.top = `${defaultOffset.y}px`;
            dock.style.right = 'auto';
            dock.style.bottom = 'auto';
            return;
        }
        const clamped = clampDockOffset(x, y);
        const clampedX = clamped.x;
        const clampedY = clamped.y;
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
        const maxWidth = Math.max(320, panel.clientWidth - 48);
        const maxHeight = Math.max(260, panel.clientHeight - 88);
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
                        #anchorMountPanel {
                --p2b-bg: rgba(255, 255, 255, 0.65);
                --p2b-border: rgba(0, 0, 0, 0.08);
                --p2b-border-inner: rgba(255, 255, 255, 0.5);
                --p2b-text-primary: #1e293b;
                --p2b-text-secondary: #64748b;
                --p2b-text-muted: #94a3b8;
                --p2b-input-bg: rgba(0, 0, 0, 0.03);
                --p2b-input-bg-focus: rgba(255, 255, 255, 0.8);
                --p2b-input-border: transparent;
                --p2b-accent: #2563eb;
                --p2b-accent-hover: #1d4ed8;
                --p2b-shadow: rgba(15, 23, 42, 0.16);
                --p2b-shadow-focus: rgba(37, 99, 235, 0.12);
                --p2b-btn-ghost-bg: transparent;
                --p2b-btn-ghost-hover: rgba(0, 0, 0, 0.06);
                --p2b-capsule-bg: rgba(255, 255, 255, 0.74);
            }
            @media (prefers-color-scheme: dark) {
                #anchorMountPanel {
                    --p2b-bg: rgba(30, 30, 30, 0.65);
                    --p2b-border: rgba(255, 255, 255, 0.15);
                    --p2b-border-inner: rgba(255, 255, 255, 0.05);
                    --p2b-text-primary: #f8fafc;
                    --p2b-text-secondary: #94a3b8;
                    --p2b-text-muted: #64748b;
                    --p2b-input-bg: rgba(255, 255, 255, 0.05);
                    --p2b-input-bg-focus: rgba(255, 255, 255, 0.08);
                    --p2b-accent: #3b82f6;
                    --p2b-accent-hover: #60a5fa;
                    --p2b-shadow: rgba(0, 0, 0, 0.4);
                    --p2b-shadow-focus: rgba(59, 130, 246, 0.2);
                    --p2b-btn-ghost-hover: rgba(255, 255, 255, 0.1);
                    --p2b-capsule-bg: rgba(40, 40, 40, 0.74);
                }
            }
            #anchorMountPanel .anchor-phase2b-dock { position: absolute; right: clamp(10px, 2vw, 24px); bottom: clamp(10px, 2vh, 24px); z-index: 42; display: flex; flex-direction: column; align-items: flex-end; pointer-events: none; max-width: calc(100% - 10px); font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif; letter-spacing: -0.01em; }
            #anchorMountPanel .anchor-phase2b-dock > * { pointer-events: auto; }
            #anchorMountPanel .anchor-phase2b-capsule { width: 44px; height: 44px; border-radius: 999px; border: 1px solid var(--p2b-border); background: var(--p2b-capsule-bg); color: var(--p2b-text-primary); backdrop-filter: blur(28px) saturate(180%); -webkit-backdrop-filter: blur(28px) saturate(180%); box-shadow: inset 0 1px 0 var(--p2b-border-inner), 0 12px 30px var(--p2b-shadow); display: inline-flex; align-items: center; justify-content: center; cursor: grab; transition: transform .34s cubic-bezier(0.16, 1, 0.3, 1), box-shadow .34s cubic-bezier(0.16, 1, 0.3, 1), opacity .2s ease; touch-action: none; user-select: none; }
            #anchorMountPanel .anchor-phase2b-capsule:hover { transform: translateY(-2px) scale(1.04); box-shadow: inset 0 1px 0 var(--p2b-border-inner), 0 16px 36px var(--p2b-shadow); }
            #anchorMountPanel .anchor-phase2b-dock.is-moving .anchor-phase2b-capsule { cursor: grabbing; box-shadow: inset 0 1px 0 var(--p2b-border-inner), 0 8px 20px var(--p2b-shadow); opacity: 0.9; }
            #anchorMountPanel .anchor-phase2b-capsule-icon { font-size: 16px; color: var(--p2b-text-primary); line-height: 1; opacity: 0.8; }
            #anchorMountPanel .anchor-phase2b-capsule-label { display: none !important; }
            #anchorMountPanel .anchor-phase2b-capsule-indicator { width: 8px; height: 8px; border-radius: 999px; background: transparent; transition: all .2s ease; position: absolute; right: 8px; top: 8px; pointer-events: none; }
            #anchorMountPanel .anchor-phase2b-dock.is-processing .anchor-phase2b-capsule-indicator { background: #10b981; animation: anchorPhase2bPulse 1.5s cubic-bezier(0.4, 0, 0.6, 1) infinite; }
            #anchorMountPanel .anchor-phase2b-dock.is-ready:not(.is-processing) .anchor-phase2b-capsule-indicator { background: var(--p2b-accent); box-shadow: 0 0 0 4px rgba(59,130,246,.15); }
            #anchorMountPanel .anchor-phase2b-toast { margin-top: 8px; padding: 8px 14px; border-radius: 10px; background: rgba(15,23,42,.88); color: #fff; font-size: 12px; line-height: 1.4; opacity: 0; transform: translateY(6px) scale(.98); transition: opacity .25s ease, transform .25s ease; pointer-events: none; max-width: min(260px, 72vw); box-shadow: 0 12px 30px rgba(0,0,0,.24); text-align: center; }
            #anchorMountPanel .anchor-phase2b-dock.is-notice .anchor-phase2b-toast { opacity: 1; transform: translateY(0) scale(1); }
            #anchorMountPanel .anchor-phase2b-dock.is-notice .anchor-phase2b-capsule { animation: anchorPhase2bNotify .36s cubic-bezier(0.16, 1, 0.3, 1) 1; }
            
            #anchorMountPanel .anchor-phase2b-canvas { width: min(400px, calc(100vw - 72px), calc(100% - 48px)); max-height: min(68vh, 720px, calc(100% - 96px)); border-radius: 18px; border: 1px solid var(--p2b-border); background: var(--p2b-bg); backdrop-filter: blur(40px) saturate(150%); -webkit-backdrop-filter: blur(40px) saturate(150%); box-shadow: inset 0 1px 0 var(--p2b-border-inner), 0 24px 60px var(--p2b-shadow); padding: 16px 18px 18px; display: flex; flex-direction: column; gap: 12px; transform-origin: right bottom; transform: translateY(16px) scale(.92); opacity: 0; pointer-events: none; transition: transform .4s cubic-bezier(0.16, 1, 0.3, 1), opacity .3s ease; position: relative; min-height: 260px; overflow: hidden; box-sizing: border-box; }
            #anchorMountPanel .anchor-phase2b-canvas, #anchorMountPanel .anchor-phase2b-canvas * { box-sizing: border-box; }
            #anchorMountPanel .anchor-phase2b-canvas > * { min-width: 0; max-width: 100%; }
            #anchorMountPanel .anchor-phase2b-dock.is-open .anchor-phase2b-canvas { transform: translateY(0) scale(1); opacity: 1; pointer-events: auto; }
            #anchorMountPanel .anchor-phase2b-dock.is-moving .anchor-phase2b-canvas { transition: none; }
            #anchorMountPanel .anchor-phase2b-dock.is-open .anchor-phase2b-capsule { opacity: 0; transform: translateY(12px) scale(.8); pointer-events: none; position: absolute; right: 0; top: 0; visibility: hidden; }
            
            #anchorMountPanel .anchor-phase2b-canvas-head { display: grid; grid-template-columns: minmax(0, 1fr) auto; align-items: start; column-gap: 10px; row-gap: 8px; cursor: move; user-select: none; touch-action: none; padding-bottom: 4px; }
            #anchorMountPanel .anchor-phase2b-canvas-title { min-width: 0; font-size: 14px; font-weight: 600; line-height: 1.45; color: var(--p2b-text-primary); }
            #anchorMountPanel .anchor-phase2b-canvas-actions { display: inline-flex; align-items: center; justify-content: flex-end; flex-wrap: wrap; gap: 4px; max-width: 100%; }
            #anchorMountPanel .anchor-phase2b-canvas-actions .btn { min-width: 28px; min-height: 28px; border-radius: 8px; padding: 0 6px; font-size: 14px; cursor: pointer; color: var(--p2b-text-secondary); background: transparent; transition: background .2s ease, color .2s ease; border: none; outline: none;}
            #anchorMountPanel .anchor-phase2b-canvas-actions .btn:hover { background: var(--p2b-btn-ghost-hover); color: var(--p2b-text-primary); }
            
            #anchorMountPanel .anchor-phase2b-chips { width: 100%; max-width: 100%; min-width: 0; display: flex; align-items: flex-start; flex-wrap: wrap; gap: 8px; min-height: 0; overflow: hidden; }
            #anchorMountPanel .anchor-phase2b-chip { border: 1px solid rgba(148,163,184,.2); background: rgba(148,163,184,.12); color: var(--p2b-text-secondary); border-radius: 999px; padding: 3px 10px; font-size: 11px; font-weight: 500; line-height: 1.5; max-width: 100%; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
            #anchorMountPanel .anchor-phase2b-chip.is-link { border-color: rgba(59,130,246,.2); background: rgba(59,130,246,.08); color: var(--p2b-accent); max-width: min(100%, 360px); }
            #anchorMountPanel .anchor-phase2b-chip-site { width: 14px; height: 14px; border-radius: 999px; background: rgba(59,130,246,.2); color: var(--p2b-accent); font-size: 9px; font-weight: 700; display: inline-flex; align-items: center; justify-content: center; flex: 0 0 auto; margin-right: 6px; }
            
            #anchorMountPanel .anchor-phase2b-input-wrap { width: 100%; max-width: 100%; min-width: 0; display: grid; gap: 8px; flex: 0 0 auto; min-height: 0; transition: opacity .4s ease, margin .4s cubic-bezier(0.16, 1, 0.3, 1); }
            #anchorMountPanel .anchor-phase2b-input-wrap.is-collapsed { display: none; }
            #anchorMountPanel .anchor-phase2b-dock.is-processing .anchor-phase2b-input-wrap { opacity: 0.3; pointer-events: none; margin-bottom: -20px; transform: scale(0.96); transform-origin: top center; transition: all .4s cubic-bezier(0.16, 1, 0.3, 1); }
            
            #anchorMountPanel .anchor-phase2b-input-shell { width: 100%; max-width: 100%; min-width: 0; position: relative; border-radius: 14px; background: var(--p2b-input-bg); transition: background .3s ease, box-shadow .3s ease; box-shadow: 0 2px 8px rgba(0,0,0,0.02) inset; border: 1px solid transparent; overflow: hidden; }
            #anchorMountPanel .anchor-phase2b-input-shell:focus-within { background: var(--p2b-input-bg-focus); box-shadow: 0 0 0 2px var(--p2b-shadow-focus), 0 4px 12px rgba(0,0,0,0.04); }
            #anchorMountPanel .anchor-phase2b-dock.is-dragging .anchor-phase2b-input-shell { background: var(--p2b-input-bg-focus); box-shadow: 0 0 0 2px var(--p2b-accent), 0 14px 30px var(--p2b-shadow-focus); }
            
            #anchorMountPanel .anchor-phase2b-input { display: block; width: 100%; max-width: 100%; min-width: 0; resize: none; border: 0; outline: none; background: transparent; color: var(--p2b-text-primary); padding: 14px 14px 56px 14px; font-size: 14px; line-height: 1.6; min-height: 112px; max-height: 38vh; overflow-y: auto; overflow-x: hidden; font-family: inherit; transition: opacity .2s ease; }
            #anchorMountPanel .anchor-phase2b-input::placeholder { color: var(--p2b-text-muted); }
            #anchorMountPanel .anchor-phase2b-input::-webkit-scrollbar { width: 6px; }
            #anchorMountPanel .anchor-phase2b-input::-webkit-scrollbar-thumb { border-radius: 999px; background: rgba(148,163,184,.3); }
            #anchorMountPanel .anchor-phase2b-input-shell.is-processing .anchor-phase2b-input { opacity: 0; }
            #anchorMountPanel .anchor-phase2b-input-skeleton { position: absolute; inset: 14px 14px 56px 14px; display: grid; align-content: start; gap: 10px; pointer-events: none; }
            #anchorMountPanel .anchor-phase2b-input-skeleton[hidden] { display: none; }
            #anchorMountPanel .anchor-phase2b-skeleton-line { height: 12px; border-radius: 999px; background: linear-gradient(90deg, rgba(148,163,184,.16) 0%, rgba(148,163,184,.32) 50%, rgba(148,163,184,.16) 100%); background-size: 220% 100%; animation: anchorPhase2bSkeletonShift 1.2s linear infinite; }
            #anchorMountPanel .anchor-phase2b-skeleton-line-lg { width: 92%; }
            #anchorMountPanel .anchor-phase2b-skeleton-line-sm { width: 68%; }

            #anchorMountPanel .anchor-phase2b-clear { position: absolute; right: 8px; top: 8px; width: 24px; height: 24px; border-radius: 50%; background: rgba(148,163,184,.2); color: var(--p2b-text-secondary); border: none; font-size: 14px; display: inline-flex; align-items: center; justify-content: center; cursor: pointer; transition: background .2s, color .2s; }
            #anchorMountPanel .anchor-phase2b-clear:hover { background: rgba(148,163,184,.4); color: var(--p2b-text-primary); }

            #anchorMountPanel .anchor-phase2b-action-row { position: absolute; left: 8px; right: 8px; bottom: 8px; display: inline-flex; align-items: center; justify-content: flex-end; gap: 4px; max-width: none; padding: 4px; border-radius: 999px; background: rgba(255,255,255,.82); box-shadow: inset 0 0 0 1px rgba(148,163,184,.22), 0 8px 18px rgba(15,23,42,.08); backdrop-filter: blur(10px); -webkit-backdrop-filter: blur(10px); overflow-x: auto; overflow-y: hidden; }
            #anchorMountPanel .anchor-phase2b-action-row > button { height: 30px; border-radius: 999px; border: none; display: inline-flex; align-items: center; justify-content: center; white-space: nowrap; cursor: pointer; transition: background .2s ease, opacity .2s ease, transform .2s ease, color .2s ease, box-shadow .2s ease; }
            #anchorMountPanel .anchor-phase2b-inline-copy { min-width: 46px; padding: 0 10px; background: rgba(148,163,184,.16); color: var(--p2b-text-primary); font-size: 12px; font-weight: 600; }
            #anchorMountPanel .anchor-phase2b-inline-copy:hover:not(:disabled) { background: rgba(59,130,246,.14); transform: translateY(-1px); }
            #anchorMountPanel .anchor-phase2b-inline-copy:disabled { opacity: .45; cursor: not-allowed; transform: none; }

            #anchorMountPanel .anchor-phase2b-paste-submit { min-width: 78px; padding: 0 12px; background: rgba(37,99,235,.14); color: var(--p2b-accent); font-size: 12px; font-weight: 700; }
            #anchorMountPanel .anchor-phase2b-paste-submit:hover:not(:disabled) { background: rgba(37,99,235,.22); color: var(--p2b-accent-hover); transform: translateY(-1px); }
            #anchorMountPanel .anchor-phase2b-paste-submit:disabled { opacity: .45; cursor: not-allowed; transform: none; }

            #anchorMountPanel .anchor-phase2b-submit { width: 30px; background: var(--p2b-accent); color: #fff; font-size: 15px; font-weight: 700; outline: none; box-shadow: 0 6px 12px rgba(37,99,235,.18); flex: 0 0 30px; }
            #anchorMountPanel .anchor-phase2b-input-shell:focus-within .anchor-phase2b-submit, #anchorMountPanel .anchor-phase2b-submit.is-active { background: var(--p2b-accent); opacity: 1; box-shadow: 0 4px 12px var(--p2b-shadow-focus); }
            #anchorMountPanel .anchor-phase2b-submit:hover:not(:disabled) { transform: scale(1.08); background: var(--p2b-accent-hover); }
            #anchorMountPanel .anchor-phase2b-submit:active:not(:disabled) { transform: scale(0.95); }
            #anchorMountPanel .anchor-phase2b-submit:disabled { opacity: .4; cursor: not-allowed; background: var(--p2b-text-secondary); box-shadow: none; transform: none; }
            
            #anchorMountPanel .anchor-phase2b-processing { width: 100%; max-width: 100%; min-width: 0; border-radius: 12px; min-height: 100px; padding: 16px; position: relative; overflow: hidden; background: transparent; display: grid; align-content: center; justify-content: center; gap: 12px; text-align: center; opacity: 0; pointer-events: none; transition: opacity .3s ease; transform: translateY(-10px); flex: 0 0 auto; }
            #anchorMountPanel .anchor-phase2b-dock.is-processing .anchor-phase2b-processing { opacity: 1; pointer-events: auto; transform: translateY(0); }
            #anchorMountPanel .anchor-phase2b-processing-text { font-size: 13px; color: var(--p2b-text-secondary); font-weight: 500; line-height: 1.6; animation: anchorPhase2bFadePulse 2s ease-in-out infinite; }
            
            #anchorMountPanel .anchor-phase2b-result { width: 100%; max-width: 100%; min-width: 0; display: flex; flex-direction: column; gap: 12px; flex: 0 0 auto; min-height: 0; overflow: hidden; animation: anchorPhase2bSlideUp .4s cubic-bezier(0.16, 1, 0.3, 1); }
            #anchorMountPanel .anchor-phase2b-result-head { width: 100%; max-width: 100%; min-width: 0; display: flex; align-items: center; justify-content: flex-end; flex-wrap: wrap; gap: 8px; margin-top: 2px; padding-bottom: 0; }
            #anchorMountPanel .anchor-phase2b-copy-btn { min-width: 140px; min-height: 38px; padding: 0 20px; border-radius: 999px; border: none; background: var(--p2b-text-primary); color: var(--p2b-bg); font-size: 13px; font-weight: 600; cursor: pointer; transition: all .25s cubic-bezier(0.16, 1, 0.3, 1); box-shadow: 0 8px 16px var(--p2b-shadow-focus); letter-spacing: 0.02em; display: inline-flex; align-items: center; justify-content: center; gap: 6px; }
            #anchorMountPanel .anchor-phase2b-copy-btn:hover:not(:disabled) { transform: translateY(-1px); box-shadow: 0 12px 24px var(--p2b-shadow-focus); opacity: 0.9; }
            #anchorMountPanel .anchor-phase2b-copy-btn:active:not(:disabled) { transform: scale(0.96); box-shadow: 0 4px 8px var(--p2b-shadow-focus); }
            #anchorMountPanel .anchor-phase2b-copy-btn.is-copied { background: #10b981; color: #fff; box-shadow: 0 8px 16px rgba(16,185,129,.2); }
            #anchorMountPanel .anchor-phase2b-copy-btn:disabled { opacity: .5; cursor: not-allowed; box-shadow: none; }
            
            #anchorMountPanel .anchor-phase2b-preview { width: 100%; max-width: 100%; min-width: 0; flex: 1 1 auto; min-height: 136px; max-height: none; overflow: auto; border-radius: 12px; border: 1px solid var(--p2b-border); background: var(--p2b-input-bg); padding: 14px 16px; font-size: 13px; line-height: 1.6; color: var(--p2b-text-primary); white-space: pre-wrap; word-break: break-word; }
            #anchorMountPanel .anchor-phase2b-preview :is(p,ul,ol,blockquote,pre,table,h1,h2,h3,h4,h5,h6) { margin: 0 0 .8em; }
            #anchorMountPanel .anchor-phase2b-preview p { margin-block: .6em; }
            #anchorMountPanel .anchor-phase2b-preview :is(p,li,blockquote,td,th) { white-space: pre-wrap; }
            #anchorMountPanel .anchor-phase2b-preview code { background: rgba(148,163,184,.15); padding: .1em .4em; border-radius: 4px; font-family: monospace; font-size: 0.9em; }
            #anchorMountPanel .anchor-phase2b-preview pre { background: rgba(15,23,42,.8); color: #e2e8f0; padding: 12px 14px; border-radius: 8px; overflow: auto; white-space: pre; border: 1px solid rgba(255,255,255,0.1); }
            #anchorMountPanel .anchor-phase2b-preview.is-streaming > * { animation: anchorPhase2bChunkIn .3s cubic-bezier(0.16, 1, 0.3, 1) both; }
            
            #anchorMountPanel .anchor-phase2b-feedback { width: 100%; max-width: 100%; min-width: 0; font-size: 12px; line-height: 1.5; color: var(--p2b-text-secondary); min-height: 18px; text-align: center; flex: 0 0 auto; overflow-wrap: anywhere; }
            #anchorMountPanel .anchor-phase2b-feedback.is-error { color: #ef4444; }
            
            #anchorMountPanel .anchor-phase2b-resizer { position: absolute; width: 16px; height: 16px; right: 4px; bottom: 4px; cursor: nwse-resize; opacity: 0.3; transition: opacity .2s; touch-action: none; background: radial-gradient(circle at 70% 70%, var(--p2b-text-secondary) 15%, transparent 16%); background-size: 4px 4px; border-radius: 0 0 16px 0; }
            #anchorMountPanel .anchor-phase2b-canvas:hover .anchor-phase2b-resizer { opacity: 0.6; }
            #anchorMountPanel .anchor-phase2b-dock.is-resizing .anchor-phase2b-canvas { transition: none; }
            
            .viewer-layout.is-center-right-stacked #anchorMountPanel .anchor-phase2b-dock { right: 12px; bottom: 12px; }
            .viewer-layout.is-center-right-stacked #anchorMountPanel .anchor-phase2b-canvas { width: min(380px, calc(100% - 40px)); max-height: min(58vh, calc(100% - 80px)); }
            @media (max-width: 960px) { #anchorMountPanel .anchor-phase2b-dock { right: 12px; bottom: 12px; } #anchorMountPanel .anchor-phase2b-canvas { width: min(380px, calc(100vw - 40px), calc(100% - 24px)); max-height: min(64vh, calc(100% - 64px)); padding: 14px 14px 16px; } }
            @media (max-width: 640px) { #anchorMountPanel .anchor-phase2b-canvas { width: min(360px, calc(100vw - 28px), calc(100% - 16px)); } #anchorMountPanel .anchor-phase2b-canvas-head { grid-template-columns: minmax(0, 1fr); } #anchorMountPanel .anchor-phase2b-canvas-actions { justify-content: flex-start; } }
            
            @keyframes anchorPhase2bPulse { 0% { box-shadow: 0 0 0 0 rgba(16,185,129,.4); } 70% { box-shadow: 0 0 0 8px rgba(16,185,129,0); } 100% { box-shadow: 0 0 0 0 rgba(16,185,129,0); } }
            @keyframes anchorPhase2bNotify { 0% { transform: translateY(0) scale(1); } 35% { transform: translateY(-3px) scale(1.06); } 100% { transform: translateY(0) scale(1); } }
            @keyframes anchorPhase2bChunkIn { 0% { opacity: 0; transform: translateY(6px); } 100% { opacity: 1; transform: translateY(0); } }
            @keyframes anchorPhase2bFadePulse { 0% { opacity: 0.6; } 50% { opacity: 1; } 100% { opacity: 0.6; } }
            @keyframes anchorPhase2bSkeletonShift { 0% { background-position: 100% 0; } 100% { background-position: -100% 0; } }
            @keyframes anchorPhase2bSlideUp { 0% { opacity: 0; transform: translateY(10px); } 100% { opacity: 1; transform: translateY(0); } }
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

                                <button class="anchor-phase2b-capsule" id="anchorPhase2bCapsuleBtn" type="button" data-phase2b-action="open" aria-label="\u6253\u5f00 Phase2B \u7ed3\u6784\u5316\u8f93\u5165\u9762\u677f">
                    <span class="anchor-phase2b-capsule-icon" aria-hidden="true">&#10022;</span>
                    <span class="anchor-phase2b-capsule-indicator" id="anchorPhase2bCapsuleIndicator" aria-hidden="true"></span>
                </button>
                <div class="anchor-phase2b-toast" id="anchorPhase2bNotice" hidden></div>
                <section class="anchor-phase2b-canvas" id="anchorPhase2bCanvas" hidden aria-hidden="true">
                    <div class="anchor-phase2b-canvas-head">
                        <div class="anchor-phase2b-canvas-title">Phase2B \u7ed3\u6784\u5316</div>
                    </div>
                    <div class="anchor-phase2b-chips" id="anchorPhase2bFileChips" hidden></div>
                    <div class="anchor-phase2b-input-wrap" id="anchorPhase2bInputWrap">
                        <div class="anchor-phase2b-input-shell" id="anchorPhase2bActionLayer">
                            <textarea id="anchorPhase2bInput" class="anchor-phase2b-input" placeholder="\u76f4\u63a5\u7c98\u8d34\u65e7\u6587\u672c\uff0c\u8fd4\u56de\u540e\u4f1a\u76f4\u63a5\u66ff\u6362\u4e3a\u5b8c\u6574\u7ed3\u679c\u3002" spellcheck="false"></textarea>
                            <div class="anchor-phase2b-input-skeleton" id="anchorPhase2bInputSkeleton" hidden aria-hidden="true">
                                <span class="anchor-phase2b-skeleton-line anchor-phase2b-skeleton-line-lg"></span>
                                <span class="anchor-phase2b-skeleton-line"></span>
                                <span class="anchor-phase2b-skeleton-line anchor-phase2b-skeleton-line-sm"></span>
                            </div>
                            <button id="anchorPhase2bClearBtn" class="anchor-phase2b-clear" type="button" data-phase2b-action="clear" aria-label="\u6e05\u7a7a\u5185\u5bb9" hidden title="\u6e05\u7a7a\u5185\u5bb9">&times;</button>
                            <div class="anchor-phase2b-action-row">
                                <button id="anchorPhase2bPasteSubmitBtn" class="anchor-phase2b-paste-submit" type="button" data-phase2b-action="paste-submit" aria-label="\u4e00\u952e\u7c98\u8d34\u5e76\u7ed3\u6784\u5316">\u7c98\u8d34\u53d1\u9001</button>
                                <button id="anchorPhase2bInlineCopyBtn" class="anchor-phase2b-inline-copy" type="button" data-phase2b-action="copy" aria-label="\u4e00\u952e\u590d\u5236">\u590d\u5236</button>
                                <button id="anchorPhase2bSubmitBtn" class="anchor-phase2b-submit" type="button" data-phase2b-action="submit" aria-label="\u53d1\u9001">&#8593;</button>
                            </div>
                        </div>
                    </div>
                    <div class="anchor-phase2b-processing" id="anchorPhase2bProcessing"></div>
                    <div class="anchor-phase2b-result" id="anchorPhase2bResult" hidden>
                        <div class="anchor-phase2b-result-head">
                            <button id="anchorPhase2bCopyBtn" class="anchor-phase2b-copy-btn" type="button" data-phase2b-action="copy">
                                <span>&#x29C9;</span> \u4e00\u952e\u590d\u5236
                            </button>
                        </div>
                    </div>
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
        const hasResultValue = !!t(phase2b.resultValue);
        const hasResult = hasResultValue || !!t(phase2b.inputValue);
        const requestInFlight = isPhase2bRequestInFlight();
        const mode = requestInFlight ? 'processing' : 'input';
        const hasArticleLinks = Array.isArray(phase2b.linkItems) && phase2b.linkItems.length > 0;
        const submitDisabled = requestInFlight || (!t(phase2b.inputValue) && !hasArticleLinks);
        const copyText = String(phase2b.inputValue || phase2b.resultValue || '');
        const copyDisabled = !copyText.trim();
        const showCopyAction = !requestInFlight && !copyDisabled;
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
            refs.inputWrap.hidden = false;
            refs.inputWrap.classList.remove('is-collapsed');
        }
        if (refs.processingWrap) {
            refs.processingWrap.hidden = true;
            refs.processingWrap.innerHTML = '';
        }
        if (refs.resultWrap) {
            refs.resultWrap.hidden = !showCopyAction;
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
            refs.fileChips.hidden = true;
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
        if (refs.actionLayer) {
            refs.actionLayer.classList.toggle('is-processing', requestInFlight);
        }
        if (refs.inputSkeleton) {
            refs.inputSkeleton.hidden = !requestInFlight;
        }
        const clearBtn = document.getElementById('anchorPhase2bClearBtn');
        if (clearBtn) {
            clearBtn.hidden = true;
        }
        if (refs.inlineCopyBtn) {
            refs.inlineCopyBtn.disabled = copyDisabled;
        }
        if (refs.pasteSubmitBtn) {
            refs.pasteSubmitBtn.disabled = requestInFlight;
        }
        if (refs.submitBtn) {
            refs.submitBtn.disabled = submitDisabled;
        }
        if (refs.copyBtn) {
            refs.copyBtn.classList.toggle('is-copied', !!phase2b.copied);
            refs.copyBtn.innerHTML = phase2b.copied
                ? '<span>\u2713</span> \u5df2\u590d\u5236'
                : '<span>\u29C9</span> \u4e00\u952e\u590d\u5236';
            refs.copyBtn.disabled = copyDisabled;
        }
        const feedbackText = phase2b.error ? `\u5904\u7406\u5931\u8d25\uff1a${phase2b.error}` : t(phase2b.feedback);
        if (refs.feedback) {
            refs.feedback.hidden = true;
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
        }
        const payloadText = String(phase2b.inputValue || '').trim();
        const hasArticleLinks = (Array.isArray(phase2b.linkItems) && phase2b.linkItems.length > 0) || inlineLinks.length > 0;
        if (!payloadText && !hasArticleLinks) {
            phase2b.error = '\u8bf7\u5148\u8f93\u5165\u6587\u672c\uff0c\u62d6\u5165 .md/.txt\uff0c\u6216\u7c98\u8d34\u77e5\u4e4e/\u6398\u91d1\u6587\u7ae0\u94fe\u63a5';
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
        phase2b.progressText = '\u8bf7\u6c42\u5df2\u53d1\u51fa\uff0c\u7b49\u5f85\u540e\u7aef\u63a5\u6536...';
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
            phase2b.feedback = '';
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
            phase2b.error = t(error && error.message) || '\u8bf7\u6c42\u5931\u8d25';
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
            runtime.phase2b.feedback = '\u4ec5\u652f\u6301 .md / .markdown / .txt \u6587\u4ef6';
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
            runtime.phase2b.feedback = '\u6587\u4ef6\u5185\u5bb9\u4e3a\u7a7a\uff0c\u672a\u5bfc\u5165\u3002';
            runtime.phase2b.error = '';
            renderPhase2bFloatingUi();
            return;
        }
        const prev = String(runtime.phase2b.inputValue || '').trim();
        const merged = `${prev}${chunks.join('')}`.trim();
        runtime.phase2b.inputValue = merged;
        runtime.phase2b.attachedFiles = files;
        runtime.phase2b.feedback = `\u5df2\u5bfc\u5165 ${files.length} \u4e2a\u6587\u4ef6`;
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
            runtime.phase2b.feedback = '';
            renderPhase2bFloatingUi();
            setTimeout(() => {
                runtime.phase2b.copied = false;
                renderPhase2bFloatingUi();
            }, 800);
        } catch (error) {
            runtime.phase2b.error = `\u590d\u5236\u5931\u8d25\uff1a${t(error && error.message) || 'clipboard denied'}`;
            runtime.phase2b.feedback = '';
            renderPhase2bFloatingUi();
        }
    }

    async function pasteAndSubmitPhase2bFromClipboard() {
        if (isPhase2bRequestInFlight()) {
            return;
        }
        let clipboardText = '';
        try {
            clipboardText = String(await navigator.clipboard.readText() || '');
        } catch (error) {
            runtime.phase2b.error = `\u8bfb\u53d6\u526a\u8d34\u677f\u5931\u8d25\uff1a${t(error && error.message) || 'clipboard denied'}`;
            runtime.phase2b.feedback = '';
            renderPhase2bFloatingUi();
            return;
        }
        const normalized = clipboardText.trim();
        if (!normalized) {
            runtime.phase2b.error = '\u526a\u8d34\u677f\u5185\u5bb9\u4e3a\u7a7a';
            runtime.phase2b.feedback = '';
            renderPhase2bFloatingUi();
            return;
        }
        runtime.phase2b.inputValue = normalized;
        runtime.phase2b.resultValue = '';
        runtime.phase2b.error = '';
        runtime.phase2b.feedback = '';
        runtime.phase2b.copied = false;
        renderPhase2bFloatingUi();
        await submitPhase2bContent();
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

    function shouldUseLocalNoteStorage() {
        // 任务阅读态以服务端挂载内容为唯一真源，不从浏览器本地笔记缓存恢复。
        return !t(runtime.ctx && runtime.ctx.taskId);
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
        if (!shouldUseLocalNoteStorage()) {
            return [];
        }
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
        if (!shouldUseLocalNoteStorage()) {
            return;
        }
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

    function findLocalNoteByPath(anchorId, notePathLike) {
        const key = t(anchorId);
        const notePath = normalizePath(notePathLike);
        if (!key || !notePath) {
            return null;
        }
        const targetPath = notePath.toLowerCase();
        const notes = ensureLocalNotes(key);
        return notes.find((note) => normalizePath(note.fileName).toLowerCase() === targetPath) || null;
    }

    function ensureActiveLocalNoteByPath(anchorId, notePathLike, options = {}) {
        const key = t(anchorId);
        const notePath = normalizePath(notePathLike);
        if (!key || !notePath || !isMarkdown(notePath)) {
            return false;
        }
        let target = findLocalNoteByPath(key, notePath);
        if (!target) {
            const fallbackContent = Object.prototype.hasOwnProperty.call(options, 'createContent')
                ? String(options.createContent || '')
                : `# ${noteNameFromPath(notePath) || 'Note'}\n\n`;
            upsertLocalNoteFromMarkdownFile(key, notePath, fallbackContent, {
                skipDirty: !!options.skipDirty,
            });
            target = findLocalNoteByPath(key, notePath);
        }
        if (!target) {
            return false;
        }
        setActiveLocalNoteId(key, target.id);
        if (options.syncEditor !== false) {
            renderLocalNoteCards(key);
            syncEditorFromActiveLocalNote(key);
            renderObsidianKnowledgePanels(key);
            refreshObsidianStatusBar(key);
        }
        return true;
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

    function upsertLocalNoteFromMarkdownFile(anchorId, fileName, content, options = {}) {
        const key = t(anchorId);
        if (!key) {
            return;
        }
        const normalizedFileName = normalizePath(fileName || '');
        if (!isMarkdown(normalizedFileName)) {
            return;
        }
        const shouldSkipDirty = !!(options && options.skipDirty);
        const notes = ensureLocalNotes(key);
        const lowerName = normalizedFileName.toLowerCase();
        let target = notes.find((item) => normalizePath(item.fileName || '').toLowerCase() === lowerName);
        const nextContent = String(content || '');
        if (!target) {
            target = normalizeLocalNoteItem({
                id: `local_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
                title: normalizedFileName.replace(/^.*\//, '').replace(/\.markdown?$/i, ''),
                content: '',
                fileName: normalizedFileName,
            }, notes.length);
            notes.push(target);
        }
        const previousFileName = normalizePath(target.fileName || '');
        const previousContent = String(target.content || '');
        const previousTitle = String(target.title || '');
        const nextTitle = normalizeLocalNoteTitle(
            nextContent,
            normalizedFileName.replace(/^.*\//, '').replace(/\.markdown?$/i, '') || target.title || 'Note'
        );
        const changed = previousFileName !== normalizedFileName
            || previousContent !== nextContent
            || previousTitle !== nextTitle;
        target.fileName = normalizedFileName;
        target.content = nextContent;
        target.title = nextTitle;
        target.updatedAt = changed ? Date.now() : (Number(target.updatedAt) || Date.now());
        runtime.localNotesByAnchor.set(key, notes);
        setActiveLocalNoteId(key, target.id);
        writeLocalNotesToStorage(key, notes);
        invalidateObsidianModel(key);
        if (changed && !shouldSkipDirty) {
            markAnchorLocalSyncDirty(key);
        }
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

    async function syncAnchorLocalNotesIncremental(anchorId, options = {}) {
        const normalizedAnchorId = t(anchorId);
        if (!normalizedAnchorId || !runtime.ctx.taskId) {
            return;
        }
        const keepalive = !!(options && options.keepalive);
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
                keepalive,
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

    function flushIncrementalLocalNoteSync(options = {}) {
        const syncOptions = options && typeof options === 'object' ? options : {};
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
            syncAnchorLocalNotesIncremental(anchorId, syncOptions);
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
        bindAnchorEditorIndentInterceptors();
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

    function createRootAnchorEntry(anchorId) {
        return { kind: 'anchor', id: t(anchorId) };
    }

    function createRootCollectionEntry(collectionId) {
        return { kind: 'collection', id: t(collectionId) };
    }

    function isTouchEnvironment() {
        const nav = window.navigator || {};
        return ('ontouchstart' in window) || (Number(nav.maxTouchPoints) > 0) || (Number(nav.msMaxTouchPoints) > 0);
    }

    function clearCollectionGroupHoldTimer() {
        if (runtime.collectionDrag.groupHoldTimer) {
            clearTimeout(runtime.collectionDrag.groupHoldTimer);
            runtime.collectionDrag.groupHoldTimer = 0;
        }
    }

    function clearCollectionLongPressTimer() {
        if (runtime.collectionDrag.longPressTimer) {
            clearTimeout(runtime.collectionDrag.longPressTimer);
            runtime.collectionDrag.longPressTimer = 0;
        }
    }

    function clearManualCollectionHoldTimer() {
        if (runtime.collectionDrag.manualHoldTimer) {
            clearTimeout(runtime.collectionDrag.manualHoldTimer);
            runtime.collectionDrag.manualHoldTimer = 0;
        }
    }

    function clearCollectionAutoExpandTimer() {
        if (runtime.collectionDrag.autoExpandTimer) {
            clearTimeout(runtime.collectionDrag.autoExpandTimer);
            runtime.collectionDrag.autoExpandTimer = 0;
        }
        runtime.collectionDrag.autoExpandCollectionId = '';
    }

    function scheduleCollectionAutoExpand(collectionIdRaw) {
        const collectionId = t(collectionIdRaw);
        if (!collectionId) {
            clearCollectionAutoExpandTimer();
            return;
        }
        const collection = runtime.collections.get(collectionId);
        if (!collection || collection.expanded !== false) {
            clearCollectionAutoExpandTimer();
            return;
        }
        if (runtime.collectionDrag.autoExpandCollectionId === collectionId && runtime.collectionDrag.autoExpandTimer) {
            return;
        }
        clearCollectionAutoExpandTimer();
        runtime.collectionDrag.autoExpandCollectionId = collectionId;
        runtime.collectionDrag.autoExpandTimer = window.setTimeout(() => {
            runtime.collectionDrag.autoExpandTimer = 0;
            const stillHovering = runtime.collectionDrag.hoverTargetType === 'collection'
                && runtime.collectionDrag.hoverTargetId === collectionId
                && runtime.collectionDrag.hoverMode === 'group';
            if (!stillHovering) {
                runtime.collectionDrag.autoExpandCollectionId = '';
                return;
            }
            const targetCollection = runtime.collections.get(collectionId);
            if (!targetCollection) {
                runtime.collectionDrag.autoExpandCollectionId = '';
                return;
            }
            targetCollection.expanded = true;
            runtime.collectionDrag.autoExpandCollectionId = '';
            renderIndex();
        }, COLLECTION_AUTO_EXPAND_MS);
    }

    function resolveCollectionAutoScrollContainer() {
        const panel = document.getElementById('anchorIndexPanel');
        if (panel instanceof HTMLElement) {
            return panel;
        }
        const list = document.getElementById('anchorIndexList');
        if (list instanceof HTMLElement) {
            return list;
        }
        return null;
    }

    function stopCollectionAutoScroll() {
        if (runtime.collectionDrag.autoScrollTimer) {
            clearInterval(runtime.collectionDrag.autoScrollTimer);
            runtime.collectionDrag.autoScrollTimer = 0;
        }
        runtime.collectionDrag.autoScrollDelta = 0;
    }

    function ensureCollectionAutoScroll(deltaY) {
        const normalizedDelta = Number.isFinite(Number(deltaY)) ? Number(deltaY) : 0;
        if (!normalizedDelta) {
            stopCollectionAutoScroll();
            return;
        }
        runtime.collectionDrag.autoScrollDelta = normalizedDelta;
        if (runtime.collectionDrag.autoScrollTimer) {
            return;
        }
        runtime.collectionDrag.autoScrollTimer = window.setInterval(() => {
            const container = resolveCollectionAutoScrollContainer();
            if (!(container instanceof HTMLElement)) {
                stopCollectionAutoScroll();
                return;
            }
            if (!runtime.collectionDrag.sourceType && !runtime.collectionDrag.manualArmed) {
                stopCollectionAutoScroll();
                return;
            }
            const topBefore = container.scrollTop;
            container.scrollTop = topBefore + runtime.collectionDrag.autoScrollDelta;
            if (container.scrollTop === topBefore) {
                stopCollectionAutoScroll();
            }
        }, COLLECTION_AUTOSCROLL_INTERVAL_MS);
    }

    function updateCollectionAutoScroll(clientYRaw) {
        const container = resolveCollectionAutoScrollContainer();
        if (!(container instanceof HTMLElement)) {
            stopCollectionAutoScroll();
            return;
        }
        const clientY = Number(clientYRaw);
        if (!Number.isFinite(clientY)) {
            stopCollectionAutoScroll();
            return;
        }
        const rect = container.getBoundingClientRect();
        const threshold = Math.max(18, Math.min(COLLECTION_AUTOSCROLL_THRESHOLD_PX, Math.floor(rect.height * 0.2)));
        let delta = 0;
        if (clientY < (rect.top + threshold)) {
            const ratio = Math.max(0, Math.min(1, (rect.top + threshold - clientY) / threshold));
            delta = -Math.max(1, Math.round(COLLECTION_AUTOSCROLL_MAX_STEP * ratio));
        } else if (clientY > (rect.bottom - threshold)) {
            const ratio = Math.max(0, Math.min(1, (clientY - (rect.bottom - threshold)) / threshold));
            delta = Math.max(1, Math.round(COLLECTION_AUTOSCROLL_MAX_STEP * ratio));
        }
        ensureCollectionAutoScroll(delta);
    }

    function resetCollectionDragHoverState() {
        clearCollectionGroupHoldTimer();
        clearCollectionAutoExpandTimer();
        runtime.collectionDrag.hoverTargetType = '';
        runtime.collectionDrag.hoverTargetId = '';
        runtime.collectionDrag.hoverMode = '';
        runtime.collectionDrag.groupReady = false;
        runtime.collectionDrag.groupHoldKey = '';
        runtime.collectionDrag.groupHoldStartedAt = 0;
    }

    function resetCollectionDragState() {
        clearCollectionLongPressTimer();
        clearManualCollectionHoldTimer();
        stopCollectionAutoScroll();
        resetCollectionDragHoverState();
        runtime.collectionDrag.sourceType = '';
        runtime.collectionDrag.sourceId = '';
        runtime.collectionDrag.sourceCollectionId = '';
        runtime.collectionDrag.batchAnchorIds = [];
        runtime.collectionDrag.manualPointerId = -1;
        runtime.collectionDrag.manualSourceType = '';
        runtime.collectionDrag.manualSourceId = '';
        runtime.collectionDrag.manualSourceCollectionId = '';
        runtime.collectionDrag.manualStartX = 0;
        runtime.collectionDrag.manualStartY = 0;
        runtime.collectionDrag.manualArmedAt = 0;
        runtime.collectionDrag.manualArmed = false;
        runtime.collectionDrag.manualDragging = false;
        runtime.collectionDrag.manualRequireHold = false;
        runtime.collectionDrag.manualHoldReady = false;
    }

    function markCollectionPulse(collectionId) {
        const normalized = t(collectionId);
        if (!normalized) {
            return;
        }
        runtime.collectionDrag.pulseCollectionId = normalized;
        runtime.collectionDrag.pulseUntil = Date.now() + 520;
        setTimeout(() => {
            if (runtime.collectionDrag.pulseCollectionId !== normalized) {
                return;
            }
            if (Date.now() >= runtime.collectionDrag.pulseUntil) {
                runtime.collectionDrag.pulseCollectionId = '';
                runtime.collectionDrag.pulseUntil = 0;
                renderIndex();
            }
        }, 560);
    }

    function normalizeCollectionTitle(rawTitle, fallbackAnchorId) {
        const direct = t(rawTitle);
        if (direct) {
            return short(direct, 48);
        }
        const fallback = candidateOf(fallbackAnchorId);
        const firstText = t(fallback && fallback.displayText);
        if (!firstText) {
            return 'New Collection';
        }
        return short(`Group: ${firstText}`, 48);
    }

    function rebuildCollectionLookup() {
        const next = new Map();
        runtime.collections.forEach((collection, collectionId) => {
            const normalizedCollectionId = t(collectionId);
            if (!normalizedCollectionId || !collection || !Array.isArray(collection.anchorIds)) {
                return;
            }
            collection.anchorIds.forEach((anchorIdRaw) => {
                const anchorId = t(anchorIdRaw);
                if (!anchorId || next.has(anchorId)) {
                    return;
                }
                next.set(anchorId, normalizedCollectionId);
            });
        });
        runtime.collectionByAnchor = next;
    }

    function collectionOfAnchor(anchorId) {
        return t(runtime.collectionByAnchor.get(t(anchorId)) || '');
    }

    function findRootEntryIndex(kind, id) {
        const normalizedKind = t(kind);
        const normalizedId = t(id);
        if (!normalizedKind || !normalizedId) {
            return -1;
        }
        return (Array.isArray(runtime.collectionRootOrder) ? runtime.collectionRootOrder : [])
            .findIndex((entry) => entry && entry.kind === normalizedKind && t(entry.id) === normalizedId);
    }

    function removeRootEntry(kind, id) {
        const index = findRootEntryIndex(kind, id);
        if (index < 0) {
            return -1;
        }
        runtime.collectionRootOrder.splice(index, 1);
        return index;
    }

    function insertRootEntry(entry, indexHint) {
        const safeEntry = entry && typeof entry === 'object' ? { kind: t(entry.kind), id: t(entry.id) } : null;
        if (!safeEntry || !safeEntry.kind || !safeEntry.id) {
            return -1;
        }
        const beforeLength = runtime.collectionRootOrder.length;
        const index = Number.isFinite(Number(indexHint))
            ? Math.max(0, Math.min(beforeLength, Number(indexHint)))
            : beforeLength;
        runtime.collectionRootOrder.splice(index, 0, safeEntry);
        return index;
    }

    function removeAnchorFromCollection(anchorId) {
        const normalizedAnchorId = t(anchorId);
        if (!normalizedAnchorId) {
            return '';
        }
        let owner = '';
        runtime.collections.forEach((collection, collectionId) => {
            if (owner || !collection || !Array.isArray(collection.anchorIds)) {
                return;
            }
            const idx = collection.anchorIds.indexOf(normalizedAnchorId);
            if (idx < 0) {
                return;
            }
            collection.anchorIds.splice(idx, 1);
            owner = t(collectionId);
        });
        if (owner) {
            rebuildCollectionLookup();
        }
        return owner;
    }

    function nextCollectionId() {
        runtime.collectionSeq += 1;
        return `collection_${Date.now().toString(36)}_${runtime.collectionSeq.toString(36)}`;
    }

    function createCollectionRecord(anchorIds, insertIndex, preferredTitle) {
        const available = new Set(runtime.candidates.map((candidate) => t(candidate.anchorId)).filter(Boolean));
        const seen = new Set();
        const normalizedAnchors = (Array.isArray(anchorIds) ? anchorIds : [])
            .map((id) => t(id))
            .filter((id) => id && available.has(id) && !seen.has(id) && (seen.add(id) || true));
        if (normalizedAnchors.length < 2) {
            return '';
        }
        const collectionId = nextCollectionId();
        runtime.collections.set(collectionId, {
            id: collectionId,
            title: normalizeCollectionTitle(preferredTitle, normalizedAnchors[0]),
            anchorIds: normalizedAnchors,
            expanded: true,
        });
        rebuildCollectionLookup();
        insertRootEntry(createRootCollectionEntry(collectionId), insertIndex);
        return collectionId;
    }

    function cleanupCollectionState() {
        const toDissolve = [];
        runtime.collections.forEach((collection, collectionId) => {
            const seen = new Set();
            const nextAnchors = (Array.isArray(collection && collection.anchorIds) ? collection.anchorIds : [])
                .map((id) => t(id))
                .filter((id) => id && !seen.has(id) && (seen.add(id) || true));
            collection.anchorIds = nextAnchors;
            if (nextAnchors.length < 2) {
                toDissolve.push({ collectionId: t(collectionId), anchorId: nextAnchors[0] || '' });
            }
        });
        if (!toDissolve.length) {
            rebuildCollectionLookup();
            return;
        }
        toDissolve.forEach(({ collectionId, anchorId }) => {
            const rootIndex = findRootEntryIndex('collection', collectionId);
            removeRootEntry('collection', collectionId);
            runtime.collections.delete(collectionId);
            if (anchorId) {
                removeRootEntry('anchor', anchorId);
                insertRootEntry(createRootAnchorEntry(anchorId), rootIndex >= 0 ? rootIndex : runtime.collectionRootOrder.length);
            }
        });
        rebuildCollectionLookup();
    }

    function syncCollectionStateWithCandidates() {
        const available = new Set(runtime.candidates.map((candidate) => t(candidate.anchorId)).filter(Boolean));
        const collapsedSingles = new Map();
        const nextCollections = new Map();

        runtime.collections.forEach((collectionRaw, rawCollectionId) => {
            const collectionId = t(rawCollectionId);
            if (!collectionId) {
                return;
            }
            const collection = collectionRaw && typeof collectionRaw === 'object' ? collectionRaw : {};
            const seen = new Set();
            const anchorIds = (Array.isArray(collection.anchorIds) ? collection.anchorIds : [])
                .map((id) => t(id))
                .filter((id) => id && available.has(id) && !seen.has(id) && (seen.add(id) || true));
            if (anchorIds.length >= 2) {
                nextCollections.set(collectionId, {
                    id: collectionId,
                    title: normalizeCollectionTitle(collection.title, anchorIds[0]),
                    anchorIds,
                    expanded: collection.expanded !== false,
                });
                return;
            }
            if (anchorIds.length === 1) {
                collapsedSingles.set(collectionId, anchorIds[0]);
            }
        });
        runtime.collections = nextCollections;
        rebuildCollectionLookup();

        const selectedCollectionTargetId = t(runtime.selectedCollectionTargetId);
        if (selectedCollectionTargetId && !runtime.collections.has(selectedCollectionTargetId)) {
            runtime.selectedCollectionTargetId = '';
        }

        Array.from(runtime.selectedAnchors).forEach((anchorIdRaw) => {
            const anchorId = t(anchorIdRaw);
            if (!anchorId || !available.has(anchorId)) {
                runtime.selectedAnchors.delete(anchorIdRaw);
            }
        });

        const nextRoot = [];
        const seenRootAnchors = new Set();
        const seenRootCollections = new Set();
        const pushAnchor = (anchorIdRaw) => {
            const anchorId = t(anchorIdRaw);
            if (!anchorId || seenRootAnchors.has(anchorId) || !available.has(anchorId) || runtime.collectionByAnchor.has(anchorId)) {
                return;
            }
            nextRoot.push(createRootAnchorEntry(anchorId));
            seenRootAnchors.add(anchorId);
        };
        const pushCollection = (collectionIdRaw) => {
            const collectionId = t(collectionIdRaw);
            if (!collectionId || seenRootCollections.has(collectionId) || !runtime.collections.has(collectionId)) {
                return;
            }
            nextRoot.push(createRootCollectionEntry(collectionId));
            seenRootCollections.add(collectionId);
        };

        (Array.isArray(runtime.collectionRootOrder) ? runtime.collectionRootOrder : []).forEach((entry) => {
            if (!entry || typeof entry !== 'object') {
                return;
            }
            if (entry.kind === 'collection') {
                const collectionId = t(entry.id);
                if (runtime.collections.has(collectionId)) {
                    pushCollection(collectionId);
                    return;
                }
                const singleAnchor = collapsedSingles.get(collectionId);
                if (singleAnchor) {
                    pushAnchor(singleAnchor);
                }
                return;
            }
            if (entry.kind === 'anchor') {
                pushAnchor(entry.id);
            }
        });

        runtime.collections.forEach((_collection, collectionId) => {
            if (!seenRootCollections.has(collectionId)) {
                pushCollection(collectionId);
            }
        });
        runtime.candidates.forEach((candidate) => {
            pushAnchor(candidate.anchorId);
        });
        runtime.collectionRootOrder = nextRoot;
    }

    function setSelectionMode(enabled) {
        runtime.selectionMode = !!enabled;
        if (!runtime.selectionMode) {
            runtime.selectedAnchors.clear();
            runtime.selectionPivotAnchorId = '';
            runtime.selectedCollectionTargetId = '';
        }
        updateCollectionActionBar();
    }

    function toggleAnchorSelection(anchorId) {
        const normalizedAnchorId = t(anchorId);
        if (!normalizedAnchorId) {
            return false;
        }
        if (runtime.selectedAnchors.has(normalizedAnchorId)) {
            runtime.selectedAnchors.delete(normalizedAnchorId);
        } else {
            runtime.selectedAnchors.add(normalizedAnchorId);
        }
        runtime.selectionPivotAnchorId = normalizedAnchorId;
        updateCollectionActionBar();
        return true;
    }

    function collectVisibleAnchorOrder() {
        const ordered = [];
        runtime.collectionRootOrder.forEach((entry) => {
            if (!entry || typeof entry !== 'object') {
                return;
            }
            if (entry.kind === 'anchor') {
                const anchorId = t(entry.id);
                if (anchorId) {
                    ordered.push(anchorId);
                }
                return;
            }
            if (entry.kind !== 'collection') {
                return;
            }
            const collection = runtime.collections.get(t(entry.id));
            if (!collection || !Array.isArray(collection.anchorIds)) {
                return;
            }
            collection.anchorIds.forEach((anchorIdRaw) => {
                const anchorId = t(anchorIdRaw);
                if (anchorId) {
                    ordered.push(anchorId);
                }
            });
        });
        return ordered;
    }

    function collectRenderedAnchorOrder() {
        const list = document.getElementById('anchorIndexList');
        if (!(list instanceof HTMLElement)) {
            return collectVisibleAnchorOrder();
        }
        const nodes = list.querySelectorAll('[data-source-type="anchor"][data-anchor-id]');
        const seen = new Set();
        const ordered = [];
        for (let i = 0; i < nodes.length; i += 1) {
            const node = nodes[i];
            if (!(node instanceof HTMLElement)) {
                continue;
            }
            if (node.closest('[hidden]')) {
                continue;
            }
            const style = window.getComputedStyle(node);
            if (style.display === 'none' || style.visibility === 'hidden') {
                continue;
            }
            const anchorId = t(node.getAttribute('data-anchor-id'));
            if (!anchorId || seen.has(anchorId)) {
                continue;
            }
            seen.add(anchorId);
            ordered.push(anchorId);
        }
        return ordered.length ? ordered : collectVisibleAnchorOrder();
    }

    function selectAnchorRange(anchorIdRaw, options = {}) {
        const anchorId = t(anchorIdRaw);
        if (!anchorId) {
            return false;
        }
        const additive = !!options.additive;
        const pivotAnchorId = t(options.pivotAnchorId || runtime.selectionPivotAnchorId || anchorId);
        const ordered = collectRenderedAnchorOrder();
        const startIndex = ordered.indexOf(pivotAnchorId);
        const endIndex = ordered.indexOf(anchorId);
        runtime.selectionMode = true;
        if (!additive) {
            runtime.selectedAnchors.clear();
        }
        if (startIndex < 0 || endIndex < 0) {
            runtime.selectedAnchors.add(anchorId);
            runtime.selectionPivotAnchorId = anchorId;
            updateCollectionActionBar();
            return true;
        }
        const [from, to] = startIndex <= endIndex ? [startIndex, endIndex] : [endIndex, startIndex];
        for (let i = from; i <= to; i += 1) {
            const id = t(ordered[i]);
            if (id) {
                runtime.selectedAnchors.add(id);
            }
        }
        runtime.selectionPivotAnchorId = anchorId;
        updateCollectionActionBar();
        return true;
    }

    function resolveDraggedAnchorIds(sourceTypeRaw, sourceIdRaw) {
        const sourceType = t(sourceTypeRaw);
        if (sourceType !== 'anchor') {
            return [];
        }
        const sourceId = t(sourceIdRaw);
        const batch = Array.isArray(runtime.collectionDrag.batchAnchorIds)
            ? runtime.collectionDrag.batchAnchorIds.map((id) => t(id)).filter(Boolean)
            : [];
        if (batch.length > 1) {
            const seen = new Set();
            return batch.filter((id) => !seen.has(id) && (seen.add(id) || true));
        }
        return sourceId ? [sourceId] : [];
    }

    function collectOrderedCollections() {
        const ordered = [];
        const seen = new Set();
        runtime.collectionRootOrder.forEach((entry) => {
            if (!entry || entry.kind !== 'collection') {
                return;
            }
            const collectionId = t(entry.id);
            const collection = runtime.collections.get(collectionId);
            if (!collection || seen.has(collectionId)) {
                return;
            }
            seen.add(collectionId);
            ordered.push(collection);
        });
        runtime.collections.forEach((collection, collectionIdRaw) => {
            const collectionId = t(collectionIdRaw);
            if (!collection || !collectionId || seen.has(collectionId)) {
                return;
            }
            seen.add(collectionId);
            ordered.push(collection);
        });
        return ordered;
    }

    function groupSelectedAnchors() {
        const orderedVisible = collectVisibleAnchorOrder();
        const selectedOrdered = orderedVisible.filter((anchorId) => runtime.selectedAnchors.has(anchorId));
        if (selectedOrdered.length < 2) {
            return false;
        }
        let insertIndex = runtime.collectionRootOrder.length;
        selectedOrdered.forEach((anchorId) => {
            const ownerCollectionId = collectionOfAnchor(anchorId);
            const rootIndex = ownerCollectionId
                ? findRootEntryIndex('collection', ownerCollectionId)
                : findRootEntryIndex('anchor', anchorId);
            if (rootIndex >= 0) {
                insertIndex = Math.min(insertIndex, rootIndex);
            }
        });
        if (!Number.isFinite(insertIndex)) {
            insertIndex = runtime.collectionRootOrder.length;
        }
        selectedOrdered.forEach((anchorId) => {
            removeRootEntry('anchor', anchorId);
            removeAnchorFromCollection(anchorId);
        });
        const createdCollectionId = createCollectionRecord(selectedOrdered, insertIndex, '');
        cleanupCollectionState();
        syncCollectionStateWithCandidates();
        if (createdCollectionId) {
            markCollectionPulse(createdCollectionId);
        }
        runtime.selectedAnchors.clear();
        runtime.selectionPivotAnchorId = '';
        runtime.selectionMode = false;
        runtime.selectedCollectionTargetId = '';
        updateCollectionActionBar();
        return !!createdCollectionId;
    }

    function moveSelectedAnchorsToCollection(targetCollectionIdRaw) {
        const targetCollectionId = t(targetCollectionIdRaw);
        const targetCollection = runtime.collections.get(targetCollectionId);
        if (!targetCollection) {
            return false;
        }
        const orderedVisible = collectVisibleAnchorOrder();
        const selectedOrdered = orderedVisible.filter((anchorId) => runtime.selectedAnchors.has(anchorId));
        if (!selectedOrdered.length) {
            return false;
        }

        const movingAnchors = selectedOrdered.filter((anchorId) => collectionOfAnchor(anchorId) !== targetCollectionId);
        if (!movingAnchors.length) {
            return false;
        }

        movingAnchors.forEach((anchorId) => {
            removeRootEntry('anchor', anchorId);
            removeAnchorFromCollection(anchorId);
        });

        const seen = new Set((Array.isArray(targetCollection.anchorIds) ? targetCollection.anchorIds : []).map((id) => t(id)).filter(Boolean));
        movingAnchors.forEach((anchorId) => {
            if (!seen.has(anchorId)) {
                seen.add(anchorId);
                targetCollection.anchorIds.push(anchorId);
            }
        });
        targetCollection.expanded = true;

        cleanupCollectionState();
        syncCollectionStateWithCandidates();
        markCollectionPulse(targetCollectionId);
        runtime.selectedAnchors.clear();
        runtime.selectionPivotAnchorId = '';
        runtime.selectionMode = false;
        runtime.selectedCollectionTargetId = '';
        updateCollectionActionBar();
        return true;
    }

    async function deleteSelectedAnchorsPermanently() {
        if (!runtime.ctx.taskId) {
            return false;
        }
        const orderedVisible = collectVisibleAnchorOrder();
        const selectedOrdered = orderedVisible.filter((anchorId) => runtime.selectedAnchors.has(anchorId));
        const selected = selectedOrdered.length
            ? selectedOrdered
            : Array.from(runtime.selectedAnchors).map((anchorIdRaw) => t(anchorIdRaw)).filter(Boolean);
        if (!selected.length) {
            return false;
        }
        const count = selected.length;
        const promptText = count === 1
            ? 'Delete the selected anchor permanently? This will remove anchor metadata and mounted files.'
            : `Delete ${count} anchors permanently? This will remove anchor metadata and mounted files.`;
        if (typeof window !== 'undefined' && typeof window.confirm === 'function') {
            const confirmed = window.confirm(promptText);
            if (!confirmed) {
                return false;
            }
        }
        try {
            const response = await fetch(`${runtime.ctx.apiBase}/tasks/${encodeURIComponent(runtime.ctx.taskId)}/anchors/delete`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    path: runtime.ctx.pathHint || '',
                    anchorIds: selected,
                    removeFiles: true,
                }),
            });
            const body = await parseResp(response);
            const deleted = Array.isArray(body && body.deletedAnchorIds)
                ? body.deletedAnchorIds.map((id) => t(id)).filter(Boolean)
                : selected;
            const deletedSet = new Set(deleted);
            runtime.selectedAnchors.clear();
            runtime.selectionPivotAnchorId = '';
            runtime.selectionMode = false;
            runtime.selectedCollectionTargetId = '';
            if (runtime.activeId && deletedSet.has(runtime.activeId)) {
                runtime.activeId = '';
                setPanel(false);
                closePanel();
            }
            resetCollectionDragState();
            await fetchMeta(true);
            setPreview(deleted.length === 1
                ? 'Anchor deleted permanently.'
                : `${deleted.length} anchors deleted permanently.`);
            return true;
        } catch (error) {
            setPreview(`Delete failed: ${t(error && error.message)}`);
            updateCollectionActionBar();
            return false;
        }
    }

    function resolveRootRefFromTarget(targetInfo) {
        if (!targetInfo || !targetInfo.type || !targetInfo.id) {
            return null;
        }
        if (targetInfo.type === 'collection') {
            return { kind: 'collection', id: t(targetInfo.id) };
        }
        const ownerCollectionId = collectionOfAnchor(targetInfo.id);
        if (ownerCollectionId) {
            return { kind: 'collection', id: ownerCollectionId };
        }
        return { kind: 'anchor', id: t(targetInfo.id) };
    }

    function resolveDragSourceRootRef() {
        const sourceType = t(runtime.collectionDrag.sourceType);
        const sourceId = t(runtime.collectionDrag.sourceId);
        if (!sourceType || !sourceId) {
            return null;
        }
        if (sourceType === 'collection') {
            return { kind: 'collection', id: sourceId };
        }
        const ownerCollectionId = collectionOfAnchor(sourceId);
        if (ownerCollectionId) {
            return { kind: 'collection', id: ownerCollectionId };
        }
        return { kind: 'anchor', id: sourceId };
    }

    function resolveDropMode(event, targetNode) {
        if (!targetNode || typeof targetNode.getBoundingClientRect !== 'function') {
            return 'group';
        }
        const rect = targetNode.getBoundingClientRect();
        const height = Math.max(1, Number(rect.height) || 1);
        const offsetY = Number(event && event.clientY) - Number(rect.top || 0);
        const ratio = Math.max(0, Math.min(1, offsetY / height));
        if (ratio <= 0.25) {
            return 'before';
        }
        if (ratio >= 0.75) {
            return 'after';
        }
        return 'group';
    }

    function canGroupDrop(sourceType, sourceId, targetInfo) {
        if (!targetInfo || !targetInfo.type || !targetInfo.id) {
            return false;
        }
        if (sourceType === 'anchor') {
            const sourceAnchors = resolveDraggedAnchorIds(sourceType, sourceId);
            if (!sourceAnchors.length) {
                return false;
            }
            const sourceAnchorSet = new Set(sourceAnchors);
            if (targetInfo.type === 'anchor') {
                if (sourceAnchorSet.has(t(targetInfo.id))) {
                    return false;
                }
                if (sourceAnchors.length > 1) {
                    const targetCollectionId = collectionOfAnchor(targetInfo.id);
                    if (targetCollectionId) {
                        return sourceAnchors.some((anchorId) => collectionOfAnchor(anchorId) !== targetCollectionId);
                    }
                    return true;
                }
                const sourceCollectionId = collectionOfAnchor(sourceAnchors[0]);
                const targetCollectionId = collectionOfAnchor(targetInfo.id);
                return !(sourceCollectionId && targetCollectionId && sourceCollectionId === targetCollectionId);
            }
            if (targetInfo.type === 'collection') {
                const targetCollectionId = t(targetInfo.id);
                return sourceAnchors.some((anchorId) => collectionOfAnchor(anchorId) !== targetCollectionId);
            }
            return false;
        }
        if (sourceType === 'collection') {
            if (targetInfo.type === 'collection') {
                return sourceId !== targetInfo.id;
            }
            if (targetInfo.type === 'anchor') {
                const targetCollectionId = collectionOfAnchor(targetInfo.id);
                return !targetCollectionId || targetCollectionId !== sourceId;
            }
        }
        return false;
    }

    function canReorderDrop(sourceType, sourceId, targetInfo) {
        if (!targetInfo || !targetInfo.type || !targetInfo.id) {
            return false;
        }
        const sourceAnchors = resolveDraggedAnchorIds(sourceType, sourceId);
        if (sourceType === 'anchor' && sourceAnchors.length > 1) {
            if (targetInfo.type === 'anchor') {
                return !sourceAnchors.includes(t(targetInfo.id));
            }
            return true;
        }
        const sourceRootRef = resolveDragSourceRootRef();
        const targetRootRef = resolveRootRefFromTarget(targetInfo);
        if (!sourceRootRef || !targetRootRef) {
            return false;
        }
        return !(sourceRootRef.kind === targetRootRef.kind && sourceRootRef.id === targetRootRef.id);
    }

    function mergeCollections(targetCollectionIdRaw, sourceCollectionIdRaw) {
        const targetCollectionId = t(targetCollectionIdRaw);
        const sourceCollectionId = t(sourceCollectionIdRaw);
        if (!targetCollectionId || !sourceCollectionId || targetCollectionId === sourceCollectionId) {
            return false;
        }
        const targetCollection = runtime.collections.get(targetCollectionId);
        const sourceCollection = runtime.collections.get(sourceCollectionId);
        if (!targetCollection || !sourceCollection) {
            return false;
        }
        const seen = new Set(targetCollection.anchorIds);
        sourceCollection.anchorIds.forEach((anchorIdRaw) => {
            const anchorId = t(anchorIdRaw);
            if (!anchorId || seen.has(anchorId)) {
                return;
            }
            seen.add(anchorId);
            targetCollection.anchorIds.push(anchorId);
        });
        targetCollection.expanded = true;
        runtime.collections.delete(sourceCollectionId);
        removeRootEntry('collection', sourceCollectionId);
        cleanupCollectionState();
        syncCollectionStateWithCandidates();
        markCollectionPulse(targetCollectionId);
        return true;
    }

    function addAnchorToCollection(anchorIdRaw, targetCollectionIdRaw) {
        const anchorId = t(anchorIdRaw);
        const targetCollectionId = t(targetCollectionIdRaw);
        if (!anchorId || !targetCollectionId) {
            return false;
        }
        const targetCollection = runtime.collections.get(targetCollectionId);
        if (!targetCollection) {
            return false;
        }
        const ownerCollectionId = collectionOfAnchor(anchorId);
        if (ownerCollectionId === targetCollectionId) {
            return false;
        }
        removeRootEntry('anchor', anchorId);
        if (ownerCollectionId) {
            removeAnchorFromCollection(anchorId);
        }
        if (!targetCollection.anchorIds.includes(anchorId)) {
            targetCollection.anchorIds.push(anchorId);
        }
        targetCollection.expanded = true;
        cleanupCollectionState();
        syncCollectionStateWithCandidates();
        markCollectionPulse(targetCollectionId);
        return true;
    }

    function moveAnchorIdsToCollection(anchorIdsRaw, targetCollectionIdRaw) {
        const targetCollectionId = t(targetCollectionIdRaw);
        const targetCollection = runtime.collections.get(targetCollectionId);
        if (!targetCollection) {
            return false;
        }
        const seenInput = new Set();
        const anchorIds = (Array.isArray(anchorIdsRaw) ? anchorIdsRaw : [])
            .map((id) => t(id))
            .filter((id) => id && !seenInput.has(id) && (seenInput.add(id) || true));
        if (!anchorIds.length) {
            return false;
        }
        const movingAnchors = anchorIds.filter((anchorId) => collectionOfAnchor(anchorId) !== targetCollectionId);
        if (!movingAnchors.length) {
            return false;
        }
        movingAnchors.forEach((anchorId) => {
            removeRootEntry('anchor', anchorId);
            removeAnchorFromCollection(anchorId);
        });
        const seen = new Set((Array.isArray(targetCollection.anchorIds) ? targetCollection.anchorIds : []).map((id) => t(id)).filter(Boolean));
        movingAnchors.forEach((anchorId) => {
            if (!seen.has(anchorId)) {
                seen.add(anchorId);
                targetCollection.anchorIds.push(anchorId);
            }
        });
        targetCollection.expanded = true;
        cleanupCollectionState();
        syncCollectionStateWithCandidates();
        markCollectionPulse(targetCollectionId);
        return true;
    }

    function groupAnchorToAnchor(sourceAnchorIdRaw, targetAnchorIdRaw) {
        const sourceAnchorId = t(sourceAnchorIdRaw);
        const targetAnchorId = t(targetAnchorIdRaw);
        if (!sourceAnchorId || !targetAnchorId || sourceAnchorId === targetAnchorId) {
            return false;
        }
        const sourceCollectionId = collectionOfAnchor(sourceAnchorId);
        const targetCollectionId = collectionOfAnchor(targetAnchorId);
        if (sourceCollectionId && targetCollectionId) {
            if (sourceCollectionId === targetCollectionId) {
                return false;
            }
            return mergeCollections(targetCollectionId, sourceCollectionId);
        }
        if (sourceCollectionId && !targetCollectionId) {
            return addAnchorToCollection(targetAnchorId, sourceCollectionId);
        }
        if (!sourceCollectionId && targetCollectionId) {
            return addAnchorToCollection(sourceAnchorId, targetCollectionId);
        }
        let insertIndex = findRootEntryIndex('anchor', targetAnchorId);
        if (insertIndex < 0) {
            insertIndex = findRootEntryIndex('anchor', sourceAnchorId);
        }
        removeRootEntry('anchor', sourceAnchorId);
        removeRootEntry('anchor', targetAnchorId);
        const createdCollectionId = createCollectionRecord([targetAnchorId, sourceAnchorId], insertIndex, '');
        cleanupCollectionState();
        syncCollectionStateWithCandidates();
        if (createdCollectionId) {
            markCollectionPulse(createdCollectionId);
            return true;
        }
        return false;
    }

    function groupCollectionToAnchor(sourceCollectionIdRaw, targetAnchorIdRaw) {
        const sourceCollectionId = t(sourceCollectionIdRaw);
        const targetAnchorId = t(targetAnchorIdRaw);
        if (!sourceCollectionId || !targetAnchorId) {
            return false;
        }
        const targetCollectionId = collectionOfAnchor(targetAnchorId);
        if (targetCollectionId) {
            if (targetCollectionId === sourceCollectionId) {
                return false;
            }
            return mergeCollections(targetCollectionId, sourceCollectionId);
        }
        return addAnchorToCollection(targetAnchorId, sourceCollectionId);
    }

    function applyGroupDrop(targetInfo) {
        const sourceType = t(runtime.collectionDrag.sourceType);
        const sourceId = t(runtime.collectionDrag.sourceId);
        if (!sourceType || !sourceId || !targetInfo || !targetInfo.type || !targetInfo.id) {
            return false;
        }
        if (sourceType === 'anchor') {
            const sourceAnchors = resolveDraggedAnchorIds(sourceType, sourceId);
            if (!sourceAnchors.length) {
                return false;
            }
            if (sourceAnchors.length > 1) {
                if (targetInfo.type === 'collection') {
                    return moveAnchorIdsToCollection(sourceAnchors, targetInfo.id);
                }
                if (targetInfo.type === 'anchor') {
                    const targetAnchorId = t(targetInfo.id);
                    if (!targetAnchorId || sourceAnchors.includes(targetAnchorId)) {
                        return false;
                    }
                    const targetCollectionId = collectionOfAnchor(targetAnchorId);
                    if (targetCollectionId) {
                        return moveAnchorIdsToCollection(sourceAnchors, targetCollectionId);
                    }
                    const mergedAnchors = [targetAnchorId, ...sourceAnchors];
                    let insertIndex = runtime.collectionRootOrder.length;
                    mergedAnchors.forEach((anchorId) => {
                        const ownerCollectionId = collectionOfAnchor(anchorId);
                        const rootIndex = ownerCollectionId
                            ? findRootEntryIndex('collection', ownerCollectionId)
                            : findRootEntryIndex('anchor', anchorId);
                        if (rootIndex >= 0) {
                            insertIndex = Math.min(insertIndex, rootIndex);
                        }
                    });
                    mergedAnchors.forEach((anchorId) => {
                        removeRootEntry('anchor', anchorId);
                        removeAnchorFromCollection(anchorId);
                    });
                    const createdCollectionId = createCollectionRecord(mergedAnchors, insertIndex, '');
                    cleanupCollectionState();
                    syncCollectionStateWithCandidates();
                    if (createdCollectionId) {
                        markCollectionPulse(createdCollectionId);
                        return true;
                    }
                    return false;
                }
                return false;
            }
            if (targetInfo.type === 'anchor') {
                return groupAnchorToAnchor(sourceId, targetInfo.id);
            }
            if (targetInfo.type === 'collection') {
                return addAnchorToCollection(sourceId, targetInfo.id);
            }
            return false;
        }
        if (sourceType === 'collection') {
            if (targetInfo.type === 'collection') {
                return mergeCollections(targetInfo.id, sourceId);
            }
            if (targetInfo.type === 'anchor') {
                return groupCollectionToAnchor(sourceId, targetInfo.id);
            }
        }
        return false;
    }

    function applyReorderDrop(targetInfo, mode) {
        const normalizedMode = mode === 'before' || mode === 'after' ? mode : '';
        if (!normalizedMode) {
            return false;
        }
        const sourceType = t(runtime.collectionDrag.sourceType);
        const sourceId = t(runtime.collectionDrag.sourceId);
        const targetRootRef = resolveRootRefFromTarget(targetInfo);
        if (!sourceType || !sourceId || !targetRootRef) {
            return false;
        }

        if (sourceType === 'anchor') {
            const sourceAnchors = resolveDraggedAnchorIds(sourceType, sourceId);
            const anchorsToMove = sourceAnchors.length ? sourceAnchors : [sourceId];
            anchorsToMove.forEach((anchorId) => {
                removeRootEntry('anchor', anchorId);
                removeAnchorFromCollection(anchorId);
            });
            const targetIndex = findRootEntryIndex(targetRootRef.kind, targetRootRef.id);
            let insertIndex = targetIndex < 0 ? runtime.collectionRootOrder.length : targetIndex;
            if (normalizedMode === 'after') {
                insertIndex += 1;
            }
            anchorsToMove.forEach((anchorId, offset) => {
                insertRootEntry(createRootAnchorEntry(anchorId), insertIndex + offset);
            });
            cleanupCollectionState();
            syncCollectionStateWithCandidates();
            return true;
        }

        if (sourceType === 'collection') {
            if (!runtime.collections.has(sourceId)) {
                return false;
            }
            removeRootEntry('collection', sourceId);
            const targetIndex = findRootEntryIndex(targetRootRef.kind, targetRootRef.id);
            let insertIndex = targetIndex < 0 ? runtime.collectionRootOrder.length : targetIndex;
            if (normalizedMode === 'after') {
                insertIndex += 1;
            }
            insertRootEntry(createRootCollectionEntry(sourceId), insertIndex);
            cleanupCollectionState();
            syncCollectionStateWithCandidates();
            return true;
        }

        return false;
    }

    function applyCollectionDrop(targetInfo, mode) {
        if (!targetInfo || !mode) {
            return false;
        }
        if (mode === 'group') {
            if (runtime.touchMode && !runtime.collectionDrag.groupReady) {
                return false;
            }
            return applyGroupDrop(targetInfo);
        }
        return applyReorderDrop(targetInfo, mode);
    }

    function updateCollectionDragHover(targetInfo, mode, groupAllowed) {
        const drag = runtime.collectionDrag;
        const normalizedMode = mode === 'before' || mode === 'after' || mode === 'group' ? mode : '';
        const normalizedTargetType = t(targetInfo && targetInfo.type);
        const normalizedTargetId = t(targetInfo && targetInfo.id);
        const nextKey = `${normalizedTargetType}:${normalizedTargetId}:${normalizedMode}`;
        let changed = false;

        if (normalizedMode === 'group' && groupAllowed) {
            const now = Date.now();
            if (drag.groupHoldKey !== nextKey) {
                clearCollectionGroupHoldTimer();
                drag.groupHoldKey = nextKey;
                drag.groupHoldStartedAt = now;
                drag.groupReady = false;
                changed = true;
            }
            const holdMs = runtime.touchMode
                ? COLLECTION_GROUP_HOLD_MS
                : (normalizedTargetType === 'anchor' ? COLLECTION_GROUP_HOLD_WEB_MS : 0);
            const shouldReady = (runtime.touchMode && drag.manualDragging)
                || holdMs <= 0
                || (drag.groupHoldStartedAt > 0 && (now - drag.groupHoldStartedAt) >= holdMs);
            if (drag.groupReady !== shouldReady) {
                drag.groupReady = shouldReady;
                changed = true;
            }
            if (normalizedTargetType === 'collection') {
                scheduleCollectionAutoExpand(normalizedTargetId);
            } else {
                clearCollectionAutoExpandTimer();
            }
        } else if (drag.groupReady || drag.groupHoldKey || drag.groupHoldTimer || drag.groupHoldStartedAt) {
            clearCollectionGroupHoldTimer();
            clearCollectionAutoExpandTimer();
            drag.groupReady = false;
            drag.groupHoldKey = '';
            drag.groupHoldStartedAt = 0;
            changed = true;
        } else {
            clearCollectionAutoExpandTimer();
        }

        if (drag.hoverTargetType !== normalizedTargetType) {
            drag.hoverTargetType = normalizedTargetType;
            changed = true;
        }
        if (drag.hoverTargetId !== normalizedTargetId) {
            drag.hoverTargetId = normalizedTargetId;
            changed = true;
        }
        if (drag.hoverMode !== normalizedMode) {
            drag.hoverMode = normalizedMode;
            changed = true;
        }

        if (changed) {
            renderIndex();
        }
    }

    function updateCollectionActionBar() {
        const toggleBtn = document.getElementById('anchorCollectionSelectToggle');
        const fab = document.getElementById('anchorCollectionFab');
        const countNode = document.getElementById('anchorCollectionSelectedCount');
        const groupBtn = document.getElementById('anchorCollectionGroupBtn');
        const moveBtn = document.getElementById('anchorCollectionMoveBtn');
        const deleteBtn = document.getElementById('anchorCollectionDeleteBtn');
        const targetSelect = document.getElementById('anchorCollectionTargetSelect');
        const cancelBtn = document.getElementById('anchorCollectionCancelBtn');
        const count = runtime.selectedAnchors.size;
        const orderedCollections = collectOrderedCollections();
        let selectedCollectionTargetId = t(runtime.selectedCollectionTargetId);

        if (selectedCollectionTargetId && !runtime.collections.has(selectedCollectionTargetId)) {
            selectedCollectionTargetId = '';
        }
        if (!selectedCollectionTargetId && orderedCollections.length && runtime.selectionMode) {
            selectedCollectionTargetId = t(orderedCollections[0] && orderedCollections[0].id);
        }
        runtime.selectedCollectionTargetId = selectedCollectionTargetId;

        if (toggleBtn) {
            if (toggleBtn.getAttribute('data-label-ready') !== '1') {
                toggleBtn.textContent = 'Select';
                toggleBtn.setAttribute('data-label-ready', '1');
            }
            toggleBtn.classList.toggle('is-active', !!runtime.selectionMode);
            toggleBtn.setAttribute('aria-pressed', runtime.selectionMode ? 'true' : 'false');
        }
        if (countNode) {
            countNode.textContent = `${count} selected`;
        }
        if (targetSelect) {
            const optionHtml = orderedCollections.map((collection) => {
                const collectionId = t(collection && collection.id);
                if (!collectionId) {
                    return '';
                }
                const anchorIds = Array.isArray(collection.anchorIds) ? collection.anchorIds : [];
                const title = normalizeCollectionTitle(collection.title, anchorIds[0]);
                return `<option value="${h(collectionId)}">${h(`${title} (${anchorIds.length})`)}</option>`;
            }).join('');
            const nextOptions = `<option value="">Move to...</option>${optionHtml}`;
            if (targetSelect.innerHTML !== nextOptions) {
                targetSelect.innerHTML = nextOptions;
            }
            targetSelect.value = selectedCollectionTargetId || '';
            targetSelect.disabled = !runtime.selectionMode || orderedCollections.length <= 0;
        }
        if (groupBtn) {
            if (groupBtn.getAttribute('data-label-ready') !== '1') {
                groupBtn.textContent = 'Create Collection';
                groupBtn.setAttribute('data-label-ready', '1');
            }
            groupBtn.disabled = count < 2;
        }
        if (moveBtn) {
            if (moveBtn.getAttribute('data-label-ready') !== '1') {
                moveBtn.textContent = 'Move to Collection';
                moveBtn.setAttribute('data-label-ready', '1');
            }
            const movableCount = selectedCollectionTargetId
                ? Array.from(runtime.selectedAnchors).filter((anchorIdRaw) => {
                    const anchorId = t(anchorIdRaw);
                    return anchorId && collectionOfAnchor(anchorId) !== selectedCollectionTargetId;
                }).length
                : 0;
            moveBtn.disabled = !runtime.selectionMode || !selectedCollectionTargetId || count < 1 || movableCount <= 0;
        }
        if (deleteBtn) {
            if (deleteBtn.getAttribute('data-label-ready') !== '1') {
                deleteBtn.textContent = 'Delete Permanently';
                deleteBtn.setAttribute('data-label-ready', '1');
            }
            deleteBtn.disabled = !runtime.selectionMode || count < 1;
        }
        if (cancelBtn) {
            if (cancelBtn.getAttribute('data-label-ready') !== '1') {
                cancelBtn.textContent = 'Cancel';
                cancelBtn.setAttribute('data-label-ready', '1');
            }
            cancelBtn.disabled = !runtime.selectionMode;
        }
        if (fab) {
            fab.hidden = !runtime.selectionMode;
            fab.classList.toggle('is-visible', !!runtime.selectionMode);
        }
    }

    function resolveAnchorBadgeText(candidate) {
        if (hasMounted(candidate)) {
            return 'Mounted';
        }
        if (candidate && candidate.status === 'files_uploaded') {
            return 'Files';
        }
        return 'Pending';
    }

    function buildAnchorCardHtml(candidate, options = {}) {
        if (!candidate || !candidate.anchorId) {
            return '';
        }
        const anchorId = t(candidate.anchorId);
        const parentCollectionId = t(options.parentCollectionId);
        const drag = runtime.collectionDrag;
        const isTarget = drag.hoverTargetType === 'anchor' && drag.hoverTargetId === anchorId;
        const isGroupTarget = isTarget && drag.hoverMode === 'group' && drag.groupReady;
        const isGroupPending = isTarget && drag.hoverMode === 'group' && !drag.groupReady;
        const isInsertBefore = isTarget && drag.hoverMode === 'before';
        const isInsertAfter = isTarget && drag.hoverMode === 'after';
        const isBatchDragging = Array.isArray(drag.batchAnchorIds) && drag.batchAnchorIds.includes(anchorId);
        const isDragging = (drag.sourceType === 'anchor' && drag.sourceId === anchorId) || isBatchDragging;
        const isSelected = false;
        const childClass = options.child ? ' is-child' : '';
        const activeClass = runtime.activeId === anchorId ? ' is-active' : '';
        const dropClass = isGroupTarget ? ' is-drop-target is-dragover-group' : '';
        const pendingClass = isGroupPending ? ' is-group-pending' : '';
        const beforeClass = isInsertBefore ? ' is-insert-before is-dragover-top' : '';
        const afterClass = isInsertAfter ? ' is-insert-after is-dragover-bottom' : '';
        const draggingClass = isDragging ? ' is-dragging' : '';
        const selectedClass = isSelected ? ' is-selected' : '';
        const badgeText = resolveAnchorBadgeText(candidate);
        const badgeClass = hasMounted(candidate) ? 'is-mounted' : (candidate.status === 'files_uploaded' ? 'is-files' : 'is-pending');
        const extractButton = options.child
            ? `<button class="anchor-collection-action is-inline" type="button" data-collection-action="extract-anchor" data-anchor-id="${h(anchorId)}" data-parent-collection-id="${h(parentCollectionId)}">移出</button>`
            : '';
        const selectControl = '';
        const dragHandle = `<span class="anchor-index-drag-handle" draggable="true" data-source-type="anchor" data-anchor-id="${h(anchorId)}" data-parent-collection-id="${h(parentCollectionId)}" title="Drag">⋮⋮</span>`;
        return `
            <div class="anchor-index-item${childClass}${activeClass}${dropClass}${pendingClass}${beforeClass}${afterClass}${draggingClass}${selectedClass}" role="button" tabindex="0" draggable="true" data-dnd-target="item" data-source-type="anchor" data-anchor-id="${h(anchorId)}" data-parent-collection-id="${h(parentCollectionId)}">
                <span class="anchor-index-item-line">${h(short(candidate.displayText, 72))}</span>
                <span class="anchor-index-item-meta">
                    <span>${h(short(candidate.matchText || candidate.displayText, 64))}</span>
                    <span class="anchor-index-badge ${badgeClass}">${h(badgeText)}</span>
                </span>
                <span class="anchor-index-item-tail">
                    ${selectControl}
                    ${extractButton}
                    ${dragHandle}
                </span>
            </div>
        `;
    }

    function buildCollectionCardHtml(collection) {
        if (!collection || !collection.id) {
            return '';
        }
        const collectionId = t(collection.id);
        const expanded = collection.expanded !== false;
        const drag = runtime.collectionDrag;
        const isTarget = drag.hoverTargetType === 'collection' && drag.hoverTargetId === collectionId;
        const isGroupTarget = isTarget && drag.hoverMode === 'group' && drag.groupReady;
        const isGroupPending = isTarget && drag.hoverMode === 'group' && !drag.groupReady;
        const isInsertBefore = isTarget && drag.hoverMode === 'before';
        const isInsertAfter = isTarget && drag.hoverMode === 'after';
        const isDragging = drag.sourceType === 'collection' && drag.sourceId === collectionId;
        const pulseActive = drag.pulseCollectionId === collectionId && Date.now() < drag.pulseUntil;

        const previewItems = collection.anchorIds.slice(0, COLLECTION_PREVIEW_LIMIT).map((anchorIdRaw) => {
            const candidate = candidateOf(anchorIdRaw);
            const text = short((candidate && candidate.displayText) || anchorIdRaw, 26);
            return `<span class="anchor-collection-preview-item">${h(text)}</span>`;
        }).join('');
        const more = Math.max(0, collection.anchorIds.length - COLLECTION_PREVIEW_LIMIT);
        const moreHtml = more > 0 ? `<span class="anchor-collection-preview-item is-more">+${more}</span>` : '';
        const preview = previewItems || '<span class="anchor-collection-preview-item">No preview</span>';
        const childCards = collection.anchorIds.map((anchorIdRaw) => buildAnchorCardHtml(candidateOf(anchorIdRaw), {
            child: true,
            parentCollectionId: collectionId,
        })).join('');
        const childPreview = collection.anchorIds.slice(0, COLLECTION_CHILD_PREVIEW_LIMIT).map((anchorIdRaw) => {
            const candidate = candidateOf(anchorIdRaw);
            return `<span class="anchor-collection-inline-item">${h(short((candidate && candidate.displayText) || anchorIdRaw, 24))}</span>`;
        }).join('');
        return `
            <section class="anchor-collection-block" data-collection-id="${h(collectionId)}">
                <div class="anchor-index-item anchor-collection-item${isGroupTarget ? ' is-drop-target is-dragover-group' : ''}${isGroupPending ? ' is-group-pending' : ''}${isInsertBefore ? ' is-insert-before is-dragover-top' : ''}${isInsertAfter ? ' is-insert-after is-dragover-bottom' : ''}${isDragging ? ' is-dragging' : ''}${pulseActive ? ' is-drop-pulse' : ''}" draggable="true" data-dnd-target="item" data-source-type="collection" data-collection-id="${h(collectionId)}">
                    <div class="anchor-collection-head">
                        <button class="anchor-collection-toggle${expanded ? ' is-expanded' : ''}" type="button" data-collection-action="toggle" data-collection-id="${h(collectionId)}" aria-label="${expanded ? 'Collapse collection' : 'Expand collection'}">${expanded ? '▾' : '▸'}</button>
                        <span class="anchor-collection-title" data-collection-id="${h(collectionId)}" data-collection-title="true" contenteditable="false" spellcheck="false">${h(normalizeCollectionTitle(collection.title, collection.anchorIds[0]))}</span>
                        <span class="anchor-comment-item-count">${h(`${collection.anchorIds.length} 任务`)}</span>
                        <span class="anchor-index-item-tail">
                            <button class="anchor-collection-action" type="button" data-collection-action="rename" data-collection-id="${h(collectionId)}">重命名</button>
                            <button class="anchor-collection-action" type="button" data-collection-action="ungroup" data-collection-id="${h(collectionId)}">解散</button>
                            <span class="anchor-index-drag-handle" draggable="true" data-source-type="collection" data-collection-id="${h(collectionId)}" title="Drag">⋮⋮</span>
                        </span>
                    </div>
                    <div class="anchor-collection-inline-preview">${childPreview || '<span class="anchor-collection-inline-item">Empty</span>'}</div>
                    <div class="anchor-collection-preview">${preview}${moreHtml}</div>
                </div>
                <div class="anchor-collection-children${expanded ? ' is-expanded' : ''}" ${expanded ? '' : 'hidden'}>
                    ${childCards}
                </div>
            </section>
        `;
    }

    function renderIndex() {
        const list = document.getElementById('anchorIndexList');
        if (!list) {
            return;
        }
        syncCollectionStateWithCandidates();
        updateCollectionActionBar();
        list.classList.toggle('is-selection-mode', !!runtime.selectionMode);
        if (!runtime.candidates.length) {
            list.innerHTML = `<div class="empty">${h(TEXT.noAnchors)}</div>`;
            return;
        }
        const html = runtime.collectionRootOrder.map((entry) => {
            if (!entry || typeof entry !== 'object') {
                return '';
            }
            if (entry.kind === 'collection') {
                return buildCollectionCardHtml(runtime.collections.get(t(entry.id)));
            }
            if (entry.kind === 'anchor') {
                return buildAnchorCardHtml(candidateOf(entry.id), {});
            }
            return '';
        }).join('');
        list.innerHTML = html || `<div class="empty">${h(TEXT.noAnchors)}</div>`;
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
            runtime.collectionRootOrder = [];
            runtime.collections.clear();
            runtime.collectionByAnchor.clear();
            runtime.selectedAnchors.clear();
            runtime.selectionPivotAnchorId = '';
            runtime.selectionMode = false;
            runtime.selectedCollectionTargetId = '';
            resetCollectionDragState();
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

    async function loadMounted(anchorId, notePath, telemetrySource, options = {}) {
        const c = candidateOf(anchorId) || runtime.anchors.get(anchorId) || null;
        if (!c || !hasMounted(c) || !runtime.ctx.taskId) return null;
        const silent = !!(options && options.silent);
        const seq = silent ? runtime.mountedSeq : ++runtime.mountedSeq;
        const query = new URLSearchParams();
        if (runtime.ctx.pathHint) query.set('path', runtime.ctx.pathHint);
        if (notePath) query.set('notePath', notePath);
        if (!silent) {
            setPreview(TEXT.previewLoading);
        }
        try {
            const resp = await fetch(`${runtime.ctx.apiBase}/tasks/${encodeURIComponent(runtime.ctx.taskId)}/anchors/${encodeURIComponent(anchorId)}/mounted?${query.toString()}`);
            const body = await parseResp(resp);
            if (!silent && seq !== runtime.mountedSeq) return null;
            runtime.mountedPayloadByAnchor.set(anchorId, body);
            const normalizedMountedPath = normalizePath(
                toRevisionRelativePath(body.notePath || body.entryNotePath || '', c)
            );
            if (normalizedMountedPath) {
                runtime.mountedNoteByAnchor.set(anchorId, normalizedMountedPath);
                const serverRawMarkdown = typeof body.rawMarkdown === 'string'
                    ? body.rawMarkdown
                    : String(body.markdown || '');
                upsertLocalNoteFromMarkdownFile(anchorId, normalizedMountedPath, serverRawMarkdown, {
                    skipDirty: true,
                });
                ensureActiveLocalNoteByPath(anchorId, normalizedMountedPath, {
                    syncEditor: !silent && runtime.activeId === anchorId,
                    createContent: serverRawMarkdown,
                    skipDirty: true,
                });
                runtime.localSyncShadowByAnchor.set(anchorId, buildLocalSyncSnapshot(anchorId));
                clearAnchorLocalSyncDirty(anchorId);
            }
            if (!silent) {
                const preview = document.getElementById('anchorPreview');
                if (preview) {
                    const html = window.markdownit
                        ? window.markdownit({ html: false, breaks: true, linkify: true }).render(String(body.markdown || ''))
                        : `<pre>${h(String(body.markdown || ''))}</pre>`;
                    preview.innerHTML = window.DOMPurify ? window.DOMPurify.sanitize(html) : html;
                }
            }
            if (!silent || runtime.activeId === anchorId) {
                renderMainNoteSelector(anchorId);
            }
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

    async function preloadMountedFromBackend() {
        if (!runtime.ctx.taskId) {
            return;
        }
        const stableCtxKey = runtime.ctxKey;
        const anchorIds = [];
        runtime.anchors.forEach((anchor, anchorId) => {
            if (hasMounted(anchor)) {
                anchorIds.push(anchorId);
            }
        });
        for (let i = 0; i < anchorIds.length; i += 1) {
            if (runtime.ctxKey !== stableCtxKey) {
                return;
            }
            // 静默预加载：回源后端挂载笔记，但不干扰当前预览面板 UI。
            await loadMounted(anchorIds[i], '', '', {
                silent: true,
            });
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
        const anchor = candidateOf(key) || runtime.anchors.get(key) || null;
        const preferredMountedPath = normalizePath(
            toRevisionRelativePath(runtime.mountedNoteByAnchor.get(key) || '', anchor)
            || toRevisionRelativePath(anchor && anchor.mountedPath || '', anchor)
        );
        if (preferredMountedPath) {
            runtime.pendingMainByAnchor.set(key, preferredMountedPath);
            return;
        }
        const notes = ensureLocalNoteFileNames(key);
        const firstNotePath = normalizePath(notes[0] && notes[0].fileName);
        if (firstNotePath) {
            runtime.pendingMainByAnchor.set(key, firstNotePath);
            return;
        }
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

    function resolveAnchorScrollTarget(candidate) {
        if (candidate && candidate.node instanceof HTMLElement) {
            return candidate.node;
        }
        const blockId = t(candidate && candidate.blockId);
        if (!blockId) {
            return null;
        }
        const body = document.getElementById('markdownBody');
        if (!(body instanceof HTMLElement)) {
            return null;
        }
        const nodes = body.querySelectorAll('[data-block-id], [data-node-id], [id]');
        for (let i = 0; i < nodes.length; i += 1) {
            const node = nodes[i];
            if (!(node instanceof HTMLElement)) {
                continue;
            }
            const ids = [
                t(node.getAttribute('data-block-id')),
                t(node.getAttribute('data-node-id')),
                t(node.id),
            ];
            if (ids.includes(blockId)) {
                return node;
            }
        }
        return null;
    }

    function scrollPanelItemIntoView(listId, anchorId) {
        const normalizedAnchorId = t(anchorId);
        if (!normalizedAnchorId) {
            return;
        }
        const list = document.getElementById(listId);
        if (!(list instanceof HTMLElement)) {
            return;
        }
        const node = Array.from(list.querySelectorAll('[data-anchor-id]')).find((item) => {
            if (!(item instanceof HTMLElement)) {
                return false;
            }
            return t(item.getAttribute('data-anchor-id')) === normalizedAnchorId;
        });
        if (node && typeof node.scrollIntoView === 'function') {
            node.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
        }
    }

    async function selectAnchor(anchorId, source, scrollIntoView) {
        const c = candidateOf(anchorId);
        if (!c) return;
        const normalizedSource = t(source) || '';
        const shouldOpenPanel = normalizedSource === 'index_keyboard_enter'
            || normalizedSource === 'index_click'
            || normalizedSource === 'inbox_click';
        if (runtime.activeId && runtime.activeId !== anchorId) {
            persistActiveLocalNoteFromEditor(runtime.activeId, { retitle: true });
        }
        runtime.activeId = anchorId;
        runtime.selectionPivotAnchorId = anchorId;
        if (!shouldOpenPanel) {
            runtime.selectionMode = true;
            runtime.selectedAnchors.clear();
            runtime.selectedAnchors.add(anchorId);
            updateCollectionActionBar();
            refreshViews();
            if (scrollIntoView !== false) {
                const targetNode = resolveAnchorScrollTarget(c);
                if (targetNode && typeof targetNode.scrollIntoView === 'function') {
                    targetNode.scrollIntoView({ block: 'center', behavior: 'smooth' });
                }
            }
            scrollPanelItemIntoView('anchorIndexList', anchorId);
            scrollPanelItemIntoView('anchorInboxList', anchorId);
            setPanel(false);
            closePanel();
            return;
        }
        openPanel();
        setPanel(true);
        const preferredMountedPath = normalizePath(
            toRevisionRelativePath(runtime.mountedNoteByAnchor.get(anchorId) || (c && c.mountedPath) || '', c)
        );
        if (preferredMountedPath) {
            ensureActiveLocalNoteByPath(anchorId, preferredMountedPath, {
                syncEditor: false,
                skipDirty: true,
            });
        }
        renderContext(c);
        renderPending(anchorId);
        if (hasMounted(c)) {
            await loadMounted(anchorId, runtime.mountedNoteByAnchor.get(anchorId) || '', normalizedSource || 'anchor');
        }
        renderLocalNoteCards(anchorId);
        syncEditorFromActiveLocalNote(anchorId);
        const searchInput = document.getElementById('anchorLocalNoteSearchInput');
        if (searchInput) {
            searchInput.value = getLocalNoteFilter(anchorId);
        }
        refreshViews();
        if (scrollIntoView !== false) {
            const targetNode = resolveAnchorScrollTarget(c);
            if (targetNode && typeof targetNode.scrollIntoView === 'function') {
                targetNode.scrollIntoView({ block: 'center', behavior: 'smooth' });
            }
        }
        scrollPanelItemIntoView('anchorIndexList', anchorId);
        scrollPanelItemIntoView('anchorInboxList', anchorId);
        if (!hasMounted(c)) {
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
        const selectToggleBtn = document.getElementById('anchorCollectionSelectToggle');
        const collectionFab = document.getElementById('anchorCollectionFab');
        const groupBtn = document.getElementById('anchorCollectionGroupBtn');
        const moveBtn = document.getElementById('anchorCollectionMoveBtn');
        const deleteBtn = document.getElementById('anchorCollectionDeleteBtn');
        const targetSelect = document.getElementById('anchorCollectionTargetSelect');
        const cancelSelectionBtn = document.getElementById('anchorCollectionCancelBtn');
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

        runtime.touchMode = isTouchEnvironment();

        const resolveIndexItemMeta = (target) => {
            const node = target && target.closest ? target.closest('[data-dnd-target="item"][data-source-type]') : null;
            if (!node) {
                return null;
            }
            const sourceType = t(node.getAttribute('data-source-type'));
            if (sourceType === 'anchor') {
                const anchorId = t(node.getAttribute('data-anchor-id'));
                if (!anchorId) {
                    return null;
                }
                return {
                    node,
                    type: 'anchor',
                    id: anchorId,
                    parentCollectionId: t(node.getAttribute('data-parent-collection-id')),
                };
            }
            if (sourceType === 'collection') {
                const collectionId = t(node.getAttribute('data-collection-id'));
                if (!collectionId) {
                    return null;
                }
                return {
                    node,
                    type: 'collection',
                    id: collectionId,
                    parentCollectionId: '',
                };
            }
            return null;
        };

        const resolveDragSourceMeta = (target) => {
            const handleNode = target && target.closest ? target.closest('.anchor-index-drag-handle[data-source-type]') : null;
            if (handleNode) {
                const sourceType = t(handleNode.getAttribute('data-source-type'));
                if (sourceType === 'anchor') {
                    return {
                        sourceType,
                        id: t(handleNode.getAttribute('data-anchor-id')),
                        parentCollectionId: t(handleNode.getAttribute('data-parent-collection-id')),
                    };
                }
                if (sourceType === 'collection') {
                    return {
                        sourceType,
                        id: t(handleNode.getAttribute('data-collection-id')),
                        parentCollectionId: '',
                    };
                }
            }
            const itemMeta = resolveIndexItemMeta(target);
            if (!itemMeta) {
                return null;
            }
            return {
                sourceType: itemMeta.type,
                id: itemMeta.id,
                parentCollectionId: itemMeta.parentCollectionId,
            };
        };

        const armManualPointerDrag = (dragMeta, event, options = {}) => {
            if (!dragMeta || !dragMeta.sourceType || !dragMeta.id || !event) {
                return;
            }
            const requireHold = !!(options && options.requireHold);
            const holdDuration = Number.isFinite(Number(options && options.holdDurationMs))
                ? Math.max(0, Number(options.holdDurationMs))
                : COLLECTION_WEB_LONG_PRESS_MS;
            clearManualCollectionHoldTimer();
            runtime.collectionDrag.manualPointerId = Number.isFinite(Number(event.pointerId)) ? Number(event.pointerId) : -1;
            runtime.collectionDrag.manualSourceType = dragMeta.sourceType;
            runtime.collectionDrag.manualSourceId = dragMeta.id;
            runtime.collectionDrag.manualSourceCollectionId = dragMeta.parentCollectionId || collectionOfAnchor(dragMeta.id);
            runtime.collectionDrag.manualStartX = Number(event.clientX) || 0;
            runtime.collectionDrag.manualStartY = Number(event.clientY) || 0;
            runtime.collectionDrag.manualArmedAt = Date.now();
            runtime.collectionDrag.manualArmed = true;
            runtime.collectionDrag.manualDragging = false;
            runtime.collectionDrag.manualRequireHold = requireHold;
            runtime.collectionDrag.manualHoldReady = !requireHold;
            runtime.collectionDrag.manualSuppressClickUntil = Math.max(
                Number(runtime.collectionDrag.manualSuppressClickUntil || 0),
                Date.now() + 1200,
            );
            if (requireHold) {
                runtime.collectionDrag.manualHoldTimer = window.setTimeout(() => {
                    runtime.collectionDrag.manualHoldTimer = 0;
                    if (!runtime.collectionDrag.manualArmed || !runtime.collectionDrag.manualRequireHold) {
                        return;
                    }
                    runtime.collectionDrag.manualHoldReady = true;
                    beginManualPointerDrag();
                    runtime.collectionDrag.manualSuppressClickUntil = Date.now() + 320;
                }, holdDuration);
            }
        };

        const beginManualPointerDrag = () => {
            if (!runtime.collectionDrag.manualArmed) {
                return false;
            }
            if (runtime.collectionDrag.manualRequireHold && !runtime.collectionDrag.manualHoldReady) {
                return false;
            }
            const sourceType = t(runtime.collectionDrag.manualSourceType);
            const sourceId = t(runtime.collectionDrag.manualSourceId);
            if (!sourceType || !sourceId) {
                return false;
            }
            runtime.collectionDrag.sourceType = sourceType;
            runtime.collectionDrag.sourceId = sourceId;
            runtime.collectionDrag.sourceCollectionId = t(runtime.collectionDrag.manualSourceCollectionId);
            resetCollectionDragHoverState();
            runtime.collectionDrag.manualDragging = true;
            renderIndex();
            return true;
        };

        const updateManualPointerDrag = (event) => {
            if (!runtime.collectionDrag.manualArmed || !event) {
                return;
            }
            if (runtime.collectionDrag.manualRequireHold && !runtime.collectionDrag.manualHoldReady) {
                const pendingDeltaX = Math.abs((Number(event.clientX) || 0) - runtime.collectionDrag.manualStartX);
                const pendingDeltaY = Math.abs((Number(event.clientY) || 0) - runtime.collectionDrag.manualStartY);
                const pendingDistance = Math.max(pendingDeltaX, pendingDeltaY);
                const elapsed = Date.now() - Number(runtime.collectionDrag.manualArmedAt || 0);
                if (pendingDistance >= 5 && elapsed >= 140) {
                    clearManualCollectionHoldTimer();
                    runtime.collectionDrag.manualHoldReady = true;
                    beginManualPointerDrag();
                    runtime.collectionDrag.manualSuppressClickUntil = Date.now() + 220;
                }
                if (!runtime.collectionDrag.manualHoldReady) {
                    return;
                }
            }
            if (!runtime.collectionDrag.manualDragging) {
                const deltaX = Math.abs((Number(event.clientX) || 0) - runtime.collectionDrag.manualStartX);
                const deltaY = Math.abs((Number(event.clientY) || 0) - runtime.collectionDrag.manualStartY);
                if (Math.max(deltaX, deltaY) < 5) {
                    return;
                }
                if (!beginManualPointerDrag()) {
                    return;
                }
            }
            event.preventDefault();
            updateCollectionAutoScroll(Number(event.clientY) || 0);
            const hit = document.elementFromPoint(Number(event.clientX) || 0, Number(event.clientY) || 0);
            const targetMeta = resolveIndexItemMeta(hit);
            if (!targetMeta) {
                resetCollectionDragHoverState();
                renderIndex();
                return;
            }
            const sourceType = t(runtime.collectionDrag.sourceType);
            const sourceId = t(runtime.collectionDrag.sourceId);
            const mode = resolveDropMode(event, targetMeta.node);
            const groupAllowed = mode === 'group' && canGroupDrop(sourceType, sourceId, targetMeta);
            const reorderAllowed = mode !== 'group' && canReorderDrop(sourceType, sourceId, targetMeta);
            if (!groupAllowed && !reorderAllowed) {
                resetCollectionDragHoverState();
                renderIndex();
                return;
            }
            updateCollectionDragHover(targetMeta, mode, groupAllowed);
        };

        const finishManualPointerDrag = () => {
            if (!runtime.collectionDrag.manualArmed) {
                return;
            }
            const sourceType = t(runtime.collectionDrag.manualSourceType);
            const sourceId = t(runtime.collectionDrag.manualSourceId);
            const shouldCommit = runtime.collectionDrag.manualDragging;
            const shouldTreatAsTap = !shouldCommit
                && runtime.collectionDrag.manualRequireHold
                && !runtime.collectionDrag.manualHoldReady;
            const shouldRender = shouldCommit
                || !!runtime.collectionDrag.sourceType
                || !!runtime.collectionDrag.hoverTargetType
                || !!runtime.collectionDrag.hoverTargetId
                || !!runtime.collectionDrag.hoverMode
                || !!runtime.collectionDrag.groupReady
                || !!runtime.collectionDrag.groupHoldKey
                || !!runtime.collectionDrag.groupHoldTimer
                || !!runtime.collectionDrag.groupHoldStartedAt;
            const targetInfo = (runtime.collectionDrag.hoverTargetType && runtime.collectionDrag.hoverTargetId)
                ? { type: runtime.collectionDrag.hoverTargetType, id: runtime.collectionDrag.hoverTargetId }
                : null;
            const mode = t(runtime.collectionDrag.hoverMode);
            if (shouldCommit && targetInfo && (mode === 'before' || mode === 'after' || mode === 'group')) {
                applyCollectionDrop(targetInfo, mode);
            }
            if (shouldTreatAsTap && sourceId) {
                if (sourceType === 'anchor') {
                    if (runtime.selectionMode) {
                        toggleAnchorSelection(sourceId);
                    } else {
                        runtime.selectionMode = true;
                        runtime.selectedAnchors.clear();
                        runtime.selectedAnchors.add(sourceId);
                        runtime.selectionPivotAnchorId = sourceId;
                        updateCollectionActionBar();
                    }
                } else if (sourceType === 'collection') {
                    const tapCollection = runtime.collections.get(sourceId);
                    if (tapCollection) {
                        tapCollection.expanded = tapCollection.expanded === false;
                    }
                }
            }
            if (shouldCommit || runtime.collectionDrag.manualHoldReady || shouldTreatAsTap) {
                runtime.collectionDrag.manualSuppressClickUntil = Date.now() + 260;
            }
            resetCollectionDragState();
            if (shouldRender && !shouldTreatAsTap) {
                renderIndex();
            } else if (shouldTreatAsTap && (sourceType === 'collection' || sourceType === 'anchor')) {
                renderIndex();
            }
        };

        const commitCollectionTitleEdit = (titleNode) => {
            if (!(titleNode instanceof HTMLElement)) {
                return;
            }
            const collectionId = t(titleNode.getAttribute('data-collection-id'));
            const collection = runtime.collections.get(collectionId);
            if (!collection) {
                renderIndex();
                return;
            }
            collection.title = normalizeCollectionTitle(titleNode.textContent, collection.anchorIds[0]);
            titleNode.removeAttribute('data-editing');
            titleNode.setAttribute('contenteditable', 'false');
            syncCollectionStateWithCandidates();
            renderIndex();
        };

        const beginCollectionTitleEdit = (collectionIdRaw) => {
            const collectionId = t(collectionIdRaw);
            if (!collectionId) {
                return;
            }
            const escapedCollectionId = (window.CSS && typeof window.CSS.escape === 'function')
                ? window.CSS.escape(collectionId)
                : collectionId.replace(/["\\]/g, '\\$&');
            const titleNode = indexList && indexList.querySelector
                ? indexList.querySelector(`[data-collection-title="true"][data-collection-id="${escapedCollectionId}"]`)
                : null;
            if (!(titleNode instanceof HTMLElement)) {
                return;
            }
            titleNode.setAttribute('contenteditable', 'true');
            titleNode.setAttribute('data-editing', '1');
            if (typeof titleNode.focus === 'function') {
                titleNode.focus();
            }
            if (window.getSelection && document.createRange) {
                const selection = window.getSelection();
                if (selection) {
                    const range = document.createRange();
                    range.selectNodeContents(titleNode);
                    range.collapse(false);
                    selection.removeAllRanges();
                    selection.addRange(range);
                }
            }
        };

        const ungroupCollection = (collectionIdRaw) => {
            const collectionId = t(collectionIdRaw);
            const collection = runtime.collections.get(collectionId);
            if (!collection) {
                return false;
            }
            const rootIndex = findRootEntryIndex('collection', collectionId);
            removeRootEntry('collection', collectionId);
            runtime.collections.delete(collectionId);
            collection.anchorIds.forEach((anchorIdRaw, index) => {
                const anchorId = t(anchorIdRaw);
                if (!anchorId) {
                    return;
                }
                removeRootEntry('anchor', anchorId);
                insertRootEntry(createRootAnchorEntry(anchorId), (rootIndex >= 0 ? rootIndex : runtime.collectionRootOrder.length) + index);
            });
            cleanupCollectionState();
            syncCollectionStateWithCandidates();
            return true;
        };

        const extractAnchorFromCollection = (anchorIdRaw, parentCollectionIdRaw) => {
            const anchorId = t(anchorIdRaw);
            const parentCollectionId = t(parentCollectionIdRaw) || collectionOfAnchor(anchorId);
            if (!anchorId || !parentCollectionId) {
                return false;
            }
            const collection = runtime.collections.get(parentCollectionId);
            if (!collection || !Array.isArray(collection.anchorIds)) {
                return false;
            }
            const itemIndex = collection.anchorIds.indexOf(anchorId);
            if (itemIndex < 0) {
                return false;
            }
            collection.anchorIds.splice(itemIndex, 1);
            const rootIndex = findRootEntryIndex('collection', parentCollectionId);
            removeRootEntry('anchor', anchorId);
            insertRootEntry(createRootAnchorEntry(anchorId), rootIndex >= 0 ? rootIndex + 1 : runtime.collectionRootOrder.length);
            cleanupCollectionState();
            syncCollectionStateWithCandidates();
            return true;
        };

        const handleCollectionAction = (actionNode) => {
            const action = t(actionNode && actionNode.getAttribute('data-collection-action'));
            if (!action) {
                return false;
            }
            if (action === 'toggle') {
                const collectionId = t(actionNode.getAttribute('data-collection-id'));
                const collection = runtime.collections.get(collectionId);
                if (!collection) {
                    return false;
                }
                collection.expanded = collection.expanded === false;
                renderIndex();
                return true;
            }
            if (action === 'rename') {
                beginCollectionTitleEdit(actionNode.getAttribute('data-collection-id'));
                return true;
            }
            if (action === 'ungroup') {
                const done = ungroupCollection(actionNode.getAttribute('data-collection-id'));
                if (done) {
                    renderIndex();
                }
                return done;
            }
            if (action === 'extract-anchor') {
                const done = extractAnchorFromCollection(
                    actionNode.getAttribute('data-anchor-id'),
                    actionNode.getAttribute('data-parent-collection-id'),
                );
                if (done) {
                    renderIndex();
                }
                return done;
            }
            return false;
        };

        const resolveOrderedSelectedAnchors = () => {
            const ordered = collectRenderedAnchorOrder();
            const selected = ordered.filter((anchorId) => runtime.selectedAnchors.has(anchorId));
            if (selected.length) {
                return selected;
            }
            return Array.from(runtime.selectedAnchors).map((anchorId) => t(anchorId)).filter(Boolean);
        };

        const createCollectionDragGhost = (count) => {
            const safeCount = Math.max(1, Number(count) || 1);
            const node = document.createElement('div');
            node.className = 'anchor-collection-drag-ghost';
            node.innerHTML = `<span class="anchor-collection-drag-ghost-icon">📦</span><span class="anchor-collection-drag-ghost-count">+${safeCount}</span>`;
            document.body.appendChild(node);
            return node;
        };

        const ensureCollectionContextMenu = () => {
            let menu = document.getElementById('anchorCollectionContextMenu');
            if (menu instanceof HTMLElement) {
                return menu;
            }
            menu = document.createElement('div');
            menu.id = 'anchorCollectionContextMenu';
            menu.className = 'anchor-collection-context-menu';
            menu.hidden = true;
            menu.setAttribute('role', 'menu');
            document.body.appendChild(menu);
            return menu;
        };

        const contextMenuNode = ensureCollectionContextMenu();
        let contextMenuPayload = null;

        const closeCollectionContextMenu = () => {
            contextMenuPayload = null;
            if (contextMenuNode) {
                contextMenuNode.hidden = true;
                contextMenuNode.innerHTML = '';
            }
        };

        const runCollectionContextAction = (actionId, payload) => {
            const action = t(actionId);
            const data = payload && typeof payload === 'object' ? payload : {};
            if (action === 'group_selected') {
                const done = groupSelectedAnchors();
                if (done) {
                    renderIndex();
                } else {
                    updateCollectionActionBar();
                }
                return;
            }
            if (action === 'delete_selected') {
                deleteSelectedAnchorsPermanently().then((done) => {
                    if (done) {
                        renderIndex();
                    } else {
                        updateCollectionActionBar();
                    }
                });
                return;
            }
            if (action === 'rename_collection') {
                const collectionId = t(data.collectionId);
                if (collectionId) {
                    beginCollectionTitleEdit(collectionId);
                }
                return;
            }
            if (action === 'ungroup_collection') {
                const collectionId = t(data.collectionId);
                if (!collectionId) {
                    return;
                }
                const done = ungroupCollection(collectionId);
                if (done) {
                    renderIndex();
                }
            }
        };

        const openCollectionContextMenu = (clientXRaw, clientYRaw, items, payload) => {
            if (!contextMenuNode) {
                return;
            }
            const actionItems = Array.isArray(items) ? items.filter((item) => item && item.id) : [];
            if (!actionItems.length) {
                closeCollectionContextMenu();
                return;
            }
            contextMenuPayload = payload && typeof payload === 'object' ? payload : {};
            contextMenuNode.innerHTML = actionItems.map((item) => {
                const disabled = !!item.disabled;
                const label = t(item.label || item.id);
                return `<button type="button" class="anchor-collection-context-item" data-context-action="${h(item.id)}" ${disabled ? 'disabled' : ''}>${h(label)}</button>`;
            }).join('');
            contextMenuNode.hidden = false;
            const clientX = Number(clientXRaw) || 0;
            const clientY = Number(clientYRaw) || 0;
            const vw = window.innerWidth || document.documentElement.clientWidth || 0;
            const vh = window.innerHeight || document.documentElement.clientHeight || 0;
            const menuRect = contextMenuNode.getBoundingClientRect();
            const left = Math.max(8, Math.min(clientX, Math.max(8, vw - menuRect.width - 8)));
            const top = Math.max(8, Math.min(clientY, Math.max(8, vh - menuRect.height - 8)));
            contextMenuNode.style.left = `${left}px`;
            contextMenuNode.style.top = `${top}px`;
        };

        contextMenuNode && contextMenuNode.addEventListener('click', (event) => {
            const item = closestFromEventTarget(event && event.target, '[data-context-action]');
            if (!(item instanceof HTMLElement)) {
                return;
            }
            event.preventDefault();
            event.stopPropagation();
            const actionId = item.getAttribute('data-context-action');
            runCollectionContextAction(actionId, contextMenuPayload);
            closeCollectionContextMenu();
        });

        document.addEventListener('pointerdown', (event) => {
            if (!contextMenuNode || contextMenuNode.hidden) {
                return;
            }
            const target = elementFromEventTarget(event && event.target);
            if (target && contextMenuNode.contains(target)) {
                return;
            }
            closeCollectionContextMenu();
        }, true);
        window.addEventListener('resize', closeCollectionContextMenu, { passive: true });
        window.addEventListener('blur', closeCollectionContextMenu);
        window.addEventListener('scroll', closeCollectionContextMenu, true);

        if (selectToggleBtn) {
            selectToggleBtn.addEventListener('click', (event) => {
                event.preventDefault();
                setSelectionMode(!runtime.selectionMode);
                renderIndex();
            });
        }
        if (groupBtn) {
            groupBtn.addEventListener('click', (event) => {
                event.preventDefault();
                const done = groupSelectedAnchors();
                if (done) {
                    renderIndex();
                } else {
                    updateCollectionActionBar();
                }
            });
        }
        if (moveBtn) {
            moveBtn.addEventListener('click', (event) => {
                event.preventDefault();
                const done = moveSelectedAnchorsToCollection(runtime.selectedCollectionTargetId);
                if (done) {
                    renderIndex();
                } else {
                    updateCollectionActionBar();
                }
            });
        }
        if (deleteBtn) {
            deleteBtn.addEventListener('click', async (event) => {
                event.preventDefault();
                const done = await deleteSelectedAnchorsPermanently();
                if (done) {
                    renderIndex();
                } else {
                    updateCollectionActionBar();
                }
            });
        }
        if (targetSelect) {
            targetSelect.addEventListener('change', (event) => {
                runtime.selectedCollectionTargetId = t(event && event.target && event.target.value);
                updateCollectionActionBar();
            });
        }
        if (cancelSelectionBtn) {
            cancelSelectionBtn.addEventListener('click', (event) => {
                event.preventDefault();
                setSelectionMode(false);
                renderIndex();
            });
        }
        if (collectionFab) {
            updateCollectionActionBar();
        }

        indexList && indexList.addEventListener('change', (event) => {
            const checkbox = closestFromEventTarget(event && event.target, 'input[type="checkbox"][data-select-anchor-id]');
            if (!(checkbox instanceof HTMLInputElement)) {
                return;
            }
            const anchorId = t(checkbox.getAttribute('data-select-anchor-id'));
            if (!anchorId) {
                return;
            }
            runtime.selectionMode = true;
            if (checkbox.checked) {
                runtime.selectedAnchors.add(anchorId);
                runtime.selectionPivotAnchorId = anchorId;
            } else {
                runtime.selectedAnchors.delete(anchorId);
            }
            updateCollectionActionBar();
            renderIndex();
        });

        indexList && indexList.addEventListener('click', (event) => {
            event.stopPropagation();
            if (Date.now() < Number(runtime.collectionDrag.manualSuppressClickUntil || 0)) {
                event.preventDefault();
                event.stopPropagation();
                return;
            }
            if (runtime.collectionDrag.manualArmed || runtime.collectionDrag.manualDragging || runtime.collectionDrag.manualHoldTimer) {
                event.preventDefault();
                event.stopPropagation();
                return;
            }
            const actionNode = closestFromEventTarget(event && event.target, '[data-collection-action]');
            if (actionNode) {
                event.preventDefault();
                event.stopPropagation();
                handleCollectionAction(actionNode);
                return;
            }
            if (closestFromEventTarget(event && event.target, '[data-select-anchor-id], [data-select-anchor-label]')) {
                return;
            }
            if (closestFromEventTarget(event && event.target, '.anchor-index-drag-handle')) {
                return;
            }
            const titleNode = closestFromEventTarget(event && event.target, '[data-collection-title="true"][data-editing="1"]');
            if (titleNode) {
                return;
            }
            const meta = resolveIndexItemMeta(event && event.target);
            if (!meta) {
                return;
            }
            if (meta.type === 'collection') {
                const collection = runtime.collections.get(meta.id);
                if (!collection) {
                    return;
                }
                collection.expanded = collection.expanded === false;
                renderIndex();
                return;
            }
            const isCommandToggle = !!(event && (event.ctrlKey || event.metaKey));
            const isRangeSelection = !!(event && event.shiftKey);
            if (isRangeSelection) {
                selectAnchorRange(meta.id, { additive: isCommandToggle });
                renderIndex();
                return;
            }
            if (isCommandToggle) {
                if (!runtime.selectionMode) {
                    runtime.selectionMode = true;
                }
                toggleAnchorSelection(meta.id);
                renderIndex();
                return;
            }
            setSelectionMode(false);
            runtime.selectionPivotAnchorId = meta.id;
            selectAnchor(meta.id, 'index_click', true);
        });
        indexList && indexList.addEventListener('contextmenu', (event) => {
            const itemMeta = resolveIndexItemMeta(event && event.target);
            if (!itemMeta) {
                closeCollectionContextMenu();
                return;
            }
            event.preventDefault();
            event.stopPropagation();
            if (itemMeta.type === 'anchor') {
                const anchorId = t(itemMeta.id);
                if (!anchorId) {
                    closeCollectionContextMenu();
                    return;
                }
                if (!runtime.selectionMode) {
                    runtime.selectionMode = true;
                    runtime.selectedAnchors.clear();
                }
                if (!runtime.selectedAnchors.has(anchorId)) {
                    runtime.selectedAnchors.clear();
                    runtime.selectedAnchors.add(anchorId);
                }
                runtime.selectionPivotAnchorId = anchorId;
                updateCollectionActionBar();
                renderIndex();
                openCollectionContextMenu(
                    Number(event.clientX) || 0,
                    Number(event.clientY) || 0,
                    [
                        {
                            id: 'group_selected',
                            label: 'Group into new collection',
                            disabled: runtime.selectedAnchors.size < 2,
                        },
                        {
                            id: 'delete_selected',
                            label: 'Delete permanently',
                            disabled: runtime.selectedAnchors.size < 1,
                        },
                    ],
                    { anchorId },
                );
                return;
            }
            if (itemMeta.type === 'collection') {
                const collectionId = t(itemMeta.id);
                openCollectionContextMenu(
                    Number(event.clientX) || 0,
                    Number(event.clientY) || 0,
                    [
                        { id: 'rename_collection', label: 'Rename' },
                        { id: 'ungroup_collection', label: 'Ungroup' },
                    ],
                    { collectionId },
                );
                return;
            }
            closeCollectionContextMenu();
        });

        indexList && indexList.addEventListener('dblclick', (event) => {
            const titleNode = closestFromEventTarget(event && event.target, '[data-collection-title="true"]');
            if (!titleNode) {
                return;
            }
            event.preventDefault();
            beginCollectionTitleEdit(titleNode.getAttribute('data-collection-id'));
        });

        indexList && indexList.addEventListener('focusout', (event) => {
            const titleNode = closestFromEventTarget(event && event.target, '[data-collection-title="true"][data-editing="1"]');
            if (!titleNode) {
                return;
            }
            commitCollectionTitleEdit(titleNode);
        });

        indexList && indexList.addEventListener('keydown', (event) => {
            if (closestFromEventTarget(event && event.target, 'input[type="checkbox"][data-select-anchor-id], select, option')) {
                return;
            }
            const titleNode = closestFromEventTarget(event && event.target, '[data-collection-title="true"]');
            if (titleNode && titleNode.getAttribute('data-editing') === '1') {
                if (event.key === 'Enter') {
                    event.preventDefault();
                    commitCollectionTitleEdit(titleNode);
                    return;
                }
                if (event.key === 'Escape') {
                    event.preventDefault();
                    renderIndex();
                    return;
                }
                return;
            }

            if (event.key === 'F2') {
                const collectionNode = closestFromEventTarget(event && event.target, '[data-source-type="collection"][data-collection-id]');
                if (!collectionNode) {
                    return;
                }
                event.preventDefault();
                beginCollectionTitleEdit(collectionNode.getAttribute('data-collection-id'));
                return;
            }

            if (event.key !== 'Enter' && event.key !== ' ') {
                return;
            }
            const meta = resolveIndexItemMeta(event && event.target);
            if (!meta) {
                return;
            }
            event.preventDefault();
            if (meta.type === 'collection') {
                const collection = runtime.collections.get(meta.id);
                if (!collection) {
                    return;
                }
                collection.expanded = collection.expanded === false;
                renderIndex();
                return;
            }

            if (event.key === 'Enter') {
                setSelectionMode(false);
                runtime.selectionPivotAnchorId = meta.id;
                selectAnchor(meta.id, 'index_keyboard_enter', true);
                return;
            }

            if (runtime.selectionMode) {
                toggleAnchorSelection(meta.id);
                renderIndex();
                return;
            }
            runtime.selectionMode = true;
            runtime.selectedAnchors.clear();
            runtime.selectedAnchors.add(meta.id);
            runtime.selectionPivotAnchorId = meta.id;
            updateCollectionActionBar();
            renderIndex();
        });

        indexList && indexList.addEventListener('pointerdown', (event) => {
            clearCollectionLongPressTimer();
            const pointerType = String(event && event.pointerType || '').toLowerCase();
            if (pointerType && pointerType !== 'mouse' && pointerType !== 'pen') {
                return;
            }
            if (Number(event && event.button) !== 0) {
                return;
            }
            if (runtime.selectionMode) {
                return;
            }
            if (closestFromEventTarget(event && event.target, 'input, textarea, select, button[data-collection-action], a, [data-collection-title="true"][data-editing="1"]')) {
                return;
            }
            const handleNode = closestFromEventTarget(event && event.target, '.anchor-index-drag-handle[data-source-type]');
            if (!handleNode) {
                return;
            }
            const dragMeta = resolveDragSourceMeta(handleNode);
            if (!dragMeta || !dragMeta.sourceType || !dragMeta.id) {
                return;
            }
            event.preventDefault();
            armManualPointerDrag(dragMeta, event, {
                requireHold: false,
            });
        });
        indexList && indexList.addEventListener('mousedown', (event) => {
            if (runtime.collectionDrag.manualArmed) {
                return;
            }
            if (Number(event && event.button) !== 0) {
                return;
            }
            if (runtime.selectionMode) {
                return;
            }
            if (closestFromEventTarget(event && event.target, 'input, textarea, select, button[data-collection-action], a, [data-collection-title="true"][data-editing="1"]')) {
                return;
            }
            const handleNode = closestFromEventTarget(event && event.target, '.anchor-index-drag-handle[data-source-type]');
            if (!handleNode) {
                return;
            }
            const dragMeta = resolveDragSourceMeta(handleNode);
            if (!dragMeta || !dragMeta.sourceType || !dragMeta.id) {
                return;
            }
            event.preventDefault();
            armManualPointerDrag(dragMeta, event, {
                requireHold: false,
            });
        });
        indexList && indexList.addEventListener('pointermove', (event) => {
            if (!runtime.collectionDrag.manualArmed) {
                return;
            }
            if (runtime.collectionDrag.manualPointerId >= 0 && Number(event.pointerId) !== runtime.collectionDrag.manualPointerId) {
                return;
            }
            updateManualPointerDrag(event);
        });
        indexList && indexList.addEventListener('pointerup', (event) => {
            clearCollectionLongPressTimer();
            if (!runtime.collectionDrag.manualArmed) {
                return;
            }
            if (runtime.collectionDrag.manualPointerId >= 0 && Number(event.pointerId) !== runtime.collectionDrag.manualPointerId) {
                return;
            }
            finishManualPointerDrag();
        });
        indexList && indexList.addEventListener('pointercancel', (event) => {
            clearCollectionLongPressTimer();
            if (!runtime.collectionDrag.manualArmed) {
                return;
            }
            if (runtime.collectionDrag.manualPointerId >= 0 && Number(event.pointerId) !== runtime.collectionDrag.manualPointerId) {
                return;
            }
            finishManualPointerDrag();
        });
        indexList && indexList.addEventListener('pointerleave', (event) => {
            clearCollectionLongPressTimer();
            if (!runtime.collectionDrag.manualArmed) {
                return;
            }
            if (runtime.collectionDrag.manualPointerId >= 0 && Number(event.pointerId) !== runtime.collectionDrag.manualPointerId) {
                return;
            }
            updateManualPointerDrag(event);
        });
        window.addEventListener('pointermove', (event) => {
            if (!runtime.collectionDrag.manualArmed) {
                return;
            }
            if (runtime.collectionDrag.manualPointerId >= 0 && Number(event.pointerId) !== runtime.collectionDrag.manualPointerId) {
                return;
            }
            updateManualPointerDrag(event);
        });
        window.addEventListener('pointerup', (event) => {
            if (!runtime.collectionDrag.manualArmed) {
                return;
            }
            if (runtime.collectionDrag.manualPointerId >= 0 && Number(event.pointerId) !== runtime.collectionDrag.manualPointerId) {
                return;
            }
            finishManualPointerDrag();
        });
        window.addEventListener('pointercancel', (event) => {
            if (!runtime.collectionDrag.manualArmed) {
                return;
            }
            if (runtime.collectionDrag.manualPointerId >= 0 && Number(event.pointerId) !== runtime.collectionDrag.manualPointerId) {
                return;
            }
            finishManualPointerDrag();
        });
        window.addEventListener('mousemove', (event) => {
            if (!runtime.collectionDrag.manualArmed) {
                return;
            }
            if (runtime.collectionDrag.manualPointerId >= 0) {
                return;
            }
            updateManualPointerDrag({
                clientX: Number(event.clientX) || 0,
                clientY: Number(event.clientY) || 0,
                preventDefault: () => {
                    if (event && typeof event.preventDefault === 'function') {
                        event.preventDefault();
                    }
                },
            });
        });
        window.addEventListener('mouseup', () => {
            if (!runtime.collectionDrag.manualArmed) {
                return;
            }
            if (runtime.collectionDrag.manualPointerId >= 0) {
                return;
            }
            finishManualPointerDrag();
        });

        indexList && indexList.addEventListener('dragstart', (event) => {
            clearCollectionLongPressTimer();
            runtime.collectionDrag.manualArmed = false;
            runtime.collectionDrag.manualDragging = false;
            runtime.collectionDrag.manualPointerId = -1;
            const dragMeta = resolveDragSourceMeta(event && event.target);
            if (!dragMeta || !dragMeta.sourceType || !dragMeta.id) {
                return;
            }
            runtime.collectionDrag.sourceType = dragMeta.sourceType;
            runtime.collectionDrag.sourceId = dragMeta.id;
            runtime.collectionDrag.sourceCollectionId = dragMeta.parentCollectionId || collectionOfAnchor(dragMeta.id);
            runtime.collectionDrag.batchAnchorIds = [];
            if (dragMeta.sourceType === 'anchor' && runtime.selectionMode && runtime.selectedAnchors.has(dragMeta.id)) {
                const selectedAnchors = resolveOrderedSelectedAnchors();
                if (selectedAnchors.length > 1) {
                    runtime.collectionDrag.batchAnchorIds = selectedAnchors;
                }
            }
            resetCollectionDragHoverState();
            if (event.dataTransfer) {
                event.dataTransfer.effectAllowed = 'move';
                event.dataTransfer.setData('text/plain', `${dragMeta.sourceType}:${dragMeta.id}`);
                const batchSize = runtime.collectionDrag.batchAnchorIds.length;
                if (batchSize > 1) {
                    const ghost = createCollectionDragGhost(batchSize);
                    event.dataTransfer.setDragImage(ghost, 18, 18);
                    window.setTimeout(() => {
                        if (ghost && ghost.parentNode) {
                            ghost.parentNode.removeChild(ghost);
                        }
                    }, 0);
                }
            }
            renderIndex();
        });

        indexList && indexList.addEventListener('dragover', (event) => {
            const sourceType = t(runtime.collectionDrag.sourceType);
            const sourceId = t(runtime.collectionDrag.sourceId);
            if (!sourceType || !sourceId) {
                stopCollectionAutoScroll();
                return;
            }
            updateCollectionAutoScroll(Number(event && event.clientY) || 0);
            const targetMeta = resolveIndexItemMeta(event && event.target);
            if (!targetMeta) {
                resetCollectionDragHoverState();
                renderIndex();
                return;
            }
            const mode = resolveDropMode(event, targetMeta.node);
            const groupAllowed = mode === 'group' && canGroupDrop(sourceType, sourceId, targetMeta);
            const reorderAllowed = mode !== 'group' && canReorderDrop(sourceType, sourceId, targetMeta);
            if (!groupAllowed && !reorderAllowed) {
                resetCollectionDragHoverState();
                renderIndex();
                return;
            }
            event.preventDefault();
            if (event.dataTransfer) {
                event.dataTransfer.dropEffect = 'move';
            }
            updateCollectionDragHover(targetMeta, mode, groupAllowed);
        });

        indexList && indexList.addEventListener('drop', (event) => {
            const sourceType = t(runtime.collectionDrag.sourceType);
            const sourceId = t(runtime.collectionDrag.sourceId);
            if (!sourceType || !sourceId) {
                stopCollectionAutoScroll();
                return;
            }
            const targetMeta = resolveIndexItemMeta(event && event.target);
            if (!targetMeta) {
                resetCollectionDragState();
                renderIndex();
                return;
            }
            const mode = runtime.collectionDrag.hoverMode || resolveDropMode(event, targetMeta.node);
            const groupAllowed = mode === 'group' && canGroupDrop(sourceType, sourceId, targetMeta);
            const reorderAllowed = mode !== 'group' && canReorderDrop(sourceType, sourceId, targetMeta);
            if (!groupAllowed && !reorderAllowed) {
                resetCollectionDragState();
                renderIndex();
                return;
            }
            event.preventDefault();
            const applied = applyCollectionDrop(targetMeta, mode);
            resetCollectionDragState();
            if (applied) {
                renderIndex();
            } else {
                renderIndex();
            }
        });

        indexList && indexList.addEventListener('dragend', () => {
            if (!runtime.collectionDrag.sourceType && !runtime.collectionDrag.hoverTargetType) {
                stopCollectionAutoScroll();
                return;
            }
            resetCollectionDragState();
            renderIndex();
        });

        indexList && indexList.addEventListener('dragleave', (event) => {
            if (!runtime.collectionDrag.sourceType) {
                stopCollectionAutoScroll();
                return;
            }
            const related = event && event.relatedTarget;
            if (related instanceof Element && indexList.contains(related)) {
                return;
            }
            stopCollectionAutoScroll();
            resetCollectionDragHoverState();
            renderIndex();
        });

        inboxList && inboxList.addEventListener('click', (e) => {
            const id = findId(e.target);
            if (!id) {
                return;
            }
            e.preventDefault();
            e.stopPropagation();
            setSelectionMode(false);
            runtime.selectionPivotAnchorId = id;
            selectAnchor(id, 'inbox_click', true);
        });
        body && body.addEventListener('click', (e) => {
            const id = findId(e.target);
            if (!id) {
                return;
            }
            e.stopPropagation();
        });
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
                if (action === 'clear') {
                    const input = document.getElementById('anchorPhase2bInput');
                    if (input) {
                        input.value = '';
                        runtime.phase2b.inputValue = '';
                        if (typeof input.focus === 'function') {
                            input.focus();
                        }
                    }
                    renderPhase2bFloatingUi();
                    return;
                }
        if (action === 'submit') {
            submitPhase2bContent();
            return;
        }
        if (action === 'paste-submit') {
            pasteAndSubmitPhase2bFromClipboard();
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
                    const maxWidth = Math.max(minWidth, panelNode.clientWidth - 48);
                    const maxHeight = Math.max(minHeight, panelNode.clientHeight - 88);
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
                flushIncrementalLocalNoteSync();
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
            if (key === 'Enter') {
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
            if (handleAnchorEditorIndentKeydown(e)) {
                return;
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
                if (keyRaw === 'Enter') {
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
            if (runAnchorEditorCommandKeymap(e)) {
                return;
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
            runtime.collectionRootOrder = [];
            runtime.collections.clear();
            runtime.collectionByAnchor.clear();
            runtime.selectedAnchors.clear();
            runtime.selectionPivotAnchorId = '';
            runtime.selectionMode = false;
            runtime.selectedCollectionTargetId = '';
            resetCollectionDragState();
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
            await preloadMountedFromBackend();
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
        }, LOCAL_NOTE_SYNC_INTERVAL_MS);
        setPreview(TEXT.previewEmpty);
        const flushSyncOnExit = () => {
            flushIncrementalLocalNoteSync({ keepalive: true });
        };
        window.addEventListener('pagehide', flushSyncOnExit);
        document.addEventListener('visibilitychange', () => {
            if (document.visibilityState === 'hidden') {
                flushSyncOnExit();
            }
        });
        window.addEventListener('beforeunload', () => {
            flushIncrementalLocalNoteSync({ keepalive: true });
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
