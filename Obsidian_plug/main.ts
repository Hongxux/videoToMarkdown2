import {
  App,
  Editor,
  EditorPosition,
  EditorSelectionOrCaret,
  Notice,
  Plugin,
  PluginSettingTab,
  Setting,
  requestUrl,
} from "obsidian";

interface StructuredRewriteSettings {
  backendBaseUrl: string;
  endpointPath: string;
  stripOuterMarkdownFence: boolean;
}

interface StructuredMarkdownResponse {
  success?: boolean;
  markdown?: string;
  message?: string;
  source?: string;
  linkWarnings?: string[];
}

interface LineContext {
  continuationPrefix: string;
  blankLinePrefix: string;
}

interface SelectionSnapshot {
  selectedText: string;
  from: EditorPosition;
  to: EditorPosition;
  fromOffset: number;
  toOffset: number;
  leadingAnchor: string;
  trailingAnchor: string;
  context: LineContext;
}

interface RewriteStatusOptions {
  detail?: string;
  autoResetMs?: number;
  rewriteToken?: number;
}

interface LocatedRange {
  from: EditorPosition;
  to: EditorPosition;
  fromOffset: number;
  toOffset: number;
}

type ApplyReplacementMode = "saved-range" | "offset-fallback" | "anchor-fallback" | "unique-text-fallback";
type RewriteStatusPhase = "idle" | "running" | "received" | "completed" | "failed";

const DEFAULT_SETTINGS: StructuredRewriteSettings = {
  backendBaseUrl: "http://127.0.0.1:8080",
  endpointPath: "/api/mobile/cards/phase2b/structured-markdown",
  stripOuterMarkdownFence: true,
};

const OUTER_MARKDOWN_FENCE = /^\s*```(?:markdown|md)?\s*([\s\S]*?)\s*```\s*$/i;
const STATUS_LABELS: Record<RewriteStatusPhase, string> = {
  idle: "Phase2B：空闲",
  running: "Phase2B：正在执行改写",
  received: "Phase2B：已接收结构化返回文本",
  completed: "Phase2B：完成改写",
  failed: "Phase2B：改写失败",
};
const ANCHOR_CONTEXT_CHARS = 24;

export default class Phase2bStructuredRewritePlugin extends Plugin {
  settings!: StructuredRewriteSettings;
  private statusBarEl!: HTMLElement;
  private latestRewriteToken = 0;
  private statusResetTimer: number | null = null;

  async onload(): Promise<void> {
    await this.loadSettings();

    this.statusBarEl = this.addStatusBarItem();
    this.updateRewriteStatus("idle");

    this.addSettingTab(new StructuredRewriteSettingTab(this.app, this));

    this.addCommand({
      id: "rewrite-selection-with-phase2b",
      name: "结构化改写选中文本",
      hotkeys: [{ modifiers: ["Mod", "Shift"], key: "S" }],
      editorCheckCallback: (checking: boolean, editor: Editor) => {
        const selected = editor.getSelection();
        if (!selected || !selected.trim()) {
          return false;
        }

        if (!checking) {
          void this.rewriteSelection(editor);
        }
        return true;
      },
    });
  }

  onunload(): void {
    this.clearStatusResetTimer();
  }

  async loadSettings(): Promise<void> {
    this.settings = Object.assign({}, DEFAULT_SETTINGS, await this.loadData());
  }

  async saveSettings(): Promise<void> {
    await this.saveData(this.settings);
  }

  private async rewriteSelection(editor: Editor): Promise<void> {
    const snapshot = captureSelectionSnapshot(editor);
    if (!snapshot) {
      new Notice("请先选中需要结构化的文本");
      return;
    }

    const rewriteToken = ++this.latestRewriteToken;
    const requestBody: Record<string, unknown> = {
      bodyText: snapshot.selectedText,
      sourceText: snapshot.selectedText,
    };

    this.updateRewriteStatus("running", {
      detail: `已冻结 ${snapshot.selectedText.length} 个字符的选区`,
      rewriteToken,
    });
    new Notice("正在执行改写，请稍候...");

    try {
      const response = await requestUrl({
        url: buildEndpointUrl(this.settings.backendBaseUrl, this.settings.endpointPath),
        method: "POST",
        contentType: "application/json",
        headers: {
          Accept: "application/json",
        },
        body: JSON.stringify(requestBody),
        throw: false,
      });

      const payload = parseResponsePayload(response.text);
      if (response.status < 200 || response.status >= 300) {
        const serverMessage = typeof payload.message === "string" && payload.message.trim()
          ? payload.message.trim()
          : `接口调用失败，HTTP ${response.status}`;
        throw new Error(serverMessage);
      }

      const normalizedMarkdown = normalizeReturnedMarkdown(
        String(payload.markdown ?? ""),
        this.settings.stripOuterMarkdownFence,
      );

      if (!normalizedMarkdown.trim()) {
        throw new Error("接口返回为空，未生成可替换的 Markdown");
      }

      this.updateRewriteStatus("received", {
        detail: `已接收 ${normalizedMarkdown.length} 个字符的结构化文本`,
        rewriteToken,
      });

      const replacement = adaptMarkdownToSelectionContext(normalizedMarkdown, snapshot.context);
      const applyMode = applyReplacementFromSnapshot(editor, snapshot, replacement);

      const notices: string[] = [];
      if (applyMode !== "saved-range") {
        notices.push(buildFallbackNotice(applyMode));
      }
      if (Array.isArray(payload.linkWarnings) && payload.linkWarnings.length > 0) {
        notices.push(`附带 ${payload.linkWarnings.length} 条链接警告`);
      }

      const completionDetail = applyMode === "saved-range"
        ? "已按原始选区完成回填"
        : `已通过${buildFallbackLabel(applyMode)}完成回填`;
      this.updateRewriteStatus("completed", {
        detail: completionDetail,
        autoResetMs: 5000,
        rewriteToken,
      });

      const suffix = notices.length > 0 ? `（${notices.join("；")}）` : "";
      new Notice(`结构化改写完成${suffix}`);
    } catch (error) {
      const message = error instanceof Error ? error.message : "未知错误";
      this.updateRewriteStatus("failed", {
        detail: message,
        autoResetMs: 8000,
        rewriteToken,
      });
      new Notice(`结构化改写失败：${message}`);
    }
  }

  private updateRewriteStatus(phase: RewriteStatusPhase, options: RewriteStatusOptions = {}): void {
    const { detail = "", autoResetMs, rewriteToken } = options;
    if (typeof rewriteToken === "number" && rewriteToken !== this.latestRewriteToken && phase !== "idle") {
      return;
    }

    this.clearStatusResetTimer();

    const label = buildStatusLabel(phase, detail);
    this.statusBarEl.textContent = label;
    this.statusBarEl.title = label;
    this.statusBarEl.dataset.phase = phase;

    if (autoResetMs && autoResetMs > 0) {
      this.statusResetTimer = window.setTimeout(() => {
        if (typeof rewriteToken === "number" && rewriteToken !== this.latestRewriteToken) {
          return;
        }
        this.updateRewriteStatus("idle");
      }, autoResetMs);
    }
  }

  private clearStatusResetTimer(): void {
    if (this.statusResetTimer !== null) {
      window.clearTimeout(this.statusResetTimer);
      this.statusResetTimer = null;
    }
  }
}

class StructuredRewriteSettingTab extends PluginSettingTab {
  plugin: Phase2bStructuredRewritePlugin;

  constructor(app: App, plugin: Phase2bStructuredRewritePlugin) {
    super(app, plugin);
    this.plugin = plugin;
  }

  display(): void {
    const { containerEl } = this;
    containerEl.empty();

    containerEl.createEl("h2", { text: "Phase2B 结构化改写设置" });

    new Setting(containerEl)
      .setName("后端基础地址")
      .setDesc("示例：http://127.0.0.1:8080")
      .addText((text) =>
        text
          .setPlaceholder(DEFAULT_SETTINGS.backendBaseUrl)
          .setValue(this.plugin.settings.backendBaseUrl)
          .onChange(async (value) => {
            this.plugin.settings.backendBaseUrl = value.trim() || DEFAULT_SETTINGS.backendBaseUrl;
            await this.plugin.saveSettings();
          }),
      );

    new Setting(containerEl)
      .setName("接口路径")
      .setDesc("默认复用现有 /phase2b/structured-markdown")
      .addText((text) =>
        text
          .setPlaceholder(DEFAULT_SETTINGS.endpointPath)
          .setValue(this.plugin.settings.endpointPath)
          .onChange(async (value) => {
            this.plugin.settings.endpointPath = value.trim() || DEFAULT_SETTINGS.endpointPath;
            await this.plugin.saveSettings();
          }),
      );

    new Setting(containerEl)
      .setName("自动剥离外层 Markdown 代码块")
      .setDesc("当模型误返回 fenced markdown 包裹时，自动提取正文")
      .addToggle((toggle) =>
        toggle.setValue(this.plugin.settings.stripOuterMarkdownFence).onChange(async (value) => {
          this.plugin.settings.stripOuterMarkdownFence = value;
          await this.plugin.saveSettings();
        }),
      );
  }
}

function buildEndpointUrl(baseUrl: string, endpointPath: string): string {
  const normalizedBaseUrl = String(baseUrl || DEFAULT_SETTINGS.backendBaseUrl).trim().replace(/\/+$/, "");
  const normalizedPath = String(endpointPath || DEFAULT_SETTINGS.endpointPath).trim();
  if (!normalizedPath) {
    return normalizedBaseUrl + DEFAULT_SETTINGS.endpointPath;
  }
  return normalizedPath.startsWith("/")
    ? `${normalizedBaseUrl}${normalizedPath}`
    : `${normalizedBaseUrl}/${normalizedPath}`;
}

function buildStatusLabel(phase: RewriteStatusPhase, detail: string): string {
  const baseLabel = STATUS_LABELS[phase];
  const normalizedDetail = normalizeStatusDetail(detail);
  return normalizedDetail ? `${baseLabel}｜${normalizedDetail}` : baseLabel;
}

function normalizeStatusDetail(detail: string): string {
  const normalized = String(detail ?? "").replace(/\s+/g, " ").trim();
  if (!normalized) {
    return "";
  }
  return normalized.length > 32 ? `${normalized.slice(0, 32)}…` : normalized;
}

function parseResponsePayload(rawText: string): StructuredMarkdownResponse {
  try {
    return JSON.parse(String(rawText ?? "")) as StructuredMarkdownResponse;
  } catch {
    return {
      message: String(rawText ?? "").trim() || "服务端返回了非 JSON 内容",
    };
  }
}

function normalizeReturnedMarkdown(markdown: string, stripOuterFence: boolean): string {
  const normalized = String(markdown ?? "").replace(/\r\n/g, "\n");
  const withoutFence = stripOuterFence
    ? normalized.replace(OUTER_MARKDOWN_FENCE, "$1")
    : normalized;
  return stripOuterBlankLines(withoutFence);
}

function stripOuterBlankLines(text: string): string {
  return String(text ?? "")
    .replace(/^(?:\s*\n)+/, "")
    .replace(/(?:\n\s*)+$/, "");
}

function captureSelectionSnapshot(editor: Editor): SelectionSnapshot | null {
  const selectedText = editor.getSelection();
  if (!selectedText || !selectedText.trim()) {
    return null;
  }

  const from = cloneEditorPosition(editor.getCursor("from"));
  const to = cloneEditorPosition(editor.getCursor("to"));
  const fromOffset = editor.posToOffset(from);
  const toOffset = editor.posToOffset(to);
  const documentText = editor.getValue();

  return {
    selectedText,
    from,
    to,
    fromOffset,
    toOffset,
    leadingAnchor: buildLeadingAnchor(documentText, fromOffset),
    trailingAnchor: buildTrailingAnchor(documentText, toOffset),
    context: analyzeLineContext(editor, from),
  };
}

function applyReplacementFromSnapshot(
  editor: Editor,
  snapshot: SelectionSnapshot,
  replacement: string,
): ApplyReplacementMode {
  const currentRangeText = editor.getRange(snapshot.from, snapshot.to);
  if (currentRangeText === snapshot.selectedText) {
    applyLocatedReplacement(editor, {
      from: snapshot.from,
      to: snapshot.to,
      fromOffset: snapshot.fromOffset,
      toOffset: snapshot.toOffset,
    }, replacement);
    return "saved-range";
  }

  const offsetRange = findRangeByOffsets(editor, snapshot);
  if (offsetRange) {
    applyLocatedReplacement(editor, offsetRange, replacement);
    return "offset-fallback";
  }

  const anchorRange = findRangeByAnchors(editor.getValue(), snapshot);
  if (anchorRange) {
    applyLocatedReplacement(editor, anchorRange, replacement);
    return "anchor-fallback";
  }

  const uniqueRange = findUniqueTextRange(editor.getValue(), snapshot.selectedText);
  if (uniqueRange) {
    applyLocatedReplacement(editor, uniqueRange, replacement);
    return "unique-text-fallback";
  }

  throw new Error("原选区已变化，且无法重新定位原文；请重新选中后再试");
}

function findRangeByOffsets(editor: Editor, snapshot: SelectionSnapshot): LocatedRange | null {
  const documentText = editor.getValue();
  if (snapshot.fromOffset < 0 || snapshot.toOffset > documentText.length || snapshot.fromOffset > snapshot.toOffset) {
    return null;
  }

  const candidate = documentText.slice(snapshot.fromOffset, snapshot.toOffset);
  if (candidate !== snapshot.selectedText) {
    return null;
  }

  return {
    from: editor.offsetToPos(snapshot.fromOffset),
    to: editor.offsetToPos(snapshot.toOffset),
    fromOffset: snapshot.fromOffset,
    toOffset: snapshot.toOffset,
  };
}

function findRangeByAnchors(documentText: string, snapshot: SelectionSnapshot): LocatedRange | null {
  const targetText = snapshot.selectedText;
  if (!targetText) {
    return null;
  }

  const matches: LocatedRange[] = [];
  let searchStart = 0;
  while (searchStart <= documentText.length) {
    const foundIndex = documentText.indexOf(targetText, searchStart);
    if (foundIndex < 0) {
      break;
    }

    const fromOffset = foundIndex;
    const toOffset = foundIndex + targetText.length;
    if (matchesAnchors(documentText, fromOffset, toOffset, snapshot.leadingAnchor, snapshot.trailingAnchor)) {
      matches.push({
        from: offsetToEditorPosition(documentText, fromOffset),
        to: offsetToEditorPosition(documentText, toOffset),
        fromOffset,
        toOffset,
      });
      if (matches.length > 1) {
        return null;
      }
    }

    searchStart = foundIndex + Math.max(1, targetText.length);
  }

  return matches.length === 1 ? matches[0] : null;
}

function matchesAnchors(
  documentText: string,
  fromOffset: number,
  toOffset: number,
  leadingAnchor: string,
  trailingAnchor: string,
): boolean {
  const currentLeading = documentText.slice(Math.max(0, fromOffset - leadingAnchor.length), fromOffset);
  const currentTrailing = documentText.slice(toOffset, Math.min(documentText.length, toOffset + trailingAnchor.length));
  return currentLeading === leadingAnchor && currentTrailing === trailingAnchor;
}

function findUniqueTextRange(documentText: string, targetText: string): LocatedRange | null {
  if (!targetText) {
    return null;
  }

  const firstIndex = documentText.indexOf(targetText);
  if (firstIndex < 0) {
    return null;
  }

  const secondIndex = documentText.indexOf(targetText, firstIndex + Math.max(1, targetText.length));
  if (secondIndex >= 0) {
    return null;
  }

  const fromOffset = firstIndex;
  const toOffset = firstIndex + targetText.length;
  return {
    from: offsetToEditorPosition(documentText, fromOffset),
    to: offsetToEditorPosition(documentText, toOffset),
    fromOffset,
    toOffset,
  };
}

function applyLocatedReplacement(editor: Editor, range: LocatedRange, replacement: string): void {
  const replacementEnd = editor.offsetToPos(range.fromOffset + replacement.length);
  const selection: EditorSelectionOrCaret = {
    anchor: replacementEnd,
    head: replacementEnd,
  };

  editor.transaction(
    {
      changes: [
        {
          from: range.from,
          to: range.to,
          text: replacement,
        },
      ],
      selections: [selection],
    },
    "phase2b-structured-rewrite",
  );

  editor.scrollIntoView({ from: range.from, to: replacementEnd }, false);
}

function buildFallbackNotice(mode: ApplyReplacementMode): string {
  switch (mode) {
    case "offset-fallback":
      return "已按原始偏移重新定位并替换";
    case "anchor-fallback":
      return "已按原文上下文锚点重新定位并替换";
    case "unique-text-fallback":
      return "已按唯一原文匹配重新定位并替换";
    default:
      return "";
  }
}

function buildFallbackLabel(mode: ApplyReplacementMode): string {
  switch (mode) {
    case "offset-fallback":
      return "原始偏移重定位";
    case "anchor-fallback":
      return "上下文锚点重定位";
    case "unique-text-fallback":
      return "唯一原文匹配重定位";
    default:
      return "原始选区";
  }
}

function buildLeadingAnchor(documentText: string, fromOffset: number): string {
  return documentText.slice(Math.max(0, fromOffset - ANCHOR_CONTEXT_CHARS), fromOffset);
}

function buildTrailingAnchor(documentText: string, toOffset: number): string {
  return documentText.slice(toOffset, Math.min(documentText.length, toOffset + ANCHOR_CONTEXT_CHARS));
}

function offsetToEditorPosition(text: string, offset: number): EditorPosition {
  const safeOffset = Math.max(0, Math.min(text.length, offset));
  let line = 0;
  let lineStart = 0;

  for (let index = 0; index < safeOffset; index += 1) {
    if (text.charCodeAt(index) === 10) {
      line += 1;
      lineStart = index + 1;
    }
  }

  return {
    line,
    ch: safeOffset - lineStart,
  };
}

function cloneEditorPosition(position: EditorPosition): EditorPosition {
  return {
    line: position.line,
    ch: position.ch,
  };
}

function adaptMarkdownToSelectionContext(markdown: string, context: LineContext): string {
  const lines = markdown.split("\n");
  if (lines.length <= 1) {
    return markdown;
  }

  return lines
    .map((line, index) => {
      if (index === 0) {
        return line;
      }
      if (!line.trim()) {
        return context.blankLinePrefix;
      }
      return `${context.continuationPrefix}${line}`;
    })
    .join("\n");
}

function analyzeLineContext(editor: Editor, from: EditorPosition): LineContext {
  const lineText = editor.getLine(from.line);
  const beforeSelection = lineText.slice(0, from.ch);
  const { structuralPrefix, listPrefix } = parseStructuralPrefix(beforeSelection);
  const inlineTail = beforeSelection.slice(structuralPrefix.length);

  let continuationPrefix = structuralPrefix;
  if (listPrefix) {
    continuationPrefix = structuralPrefix.slice(0, structuralPrefix.length - listPrefix.length)
      + " ".repeat(listPrefix.length);
  }

  if (inlineTail.length > 0) {
    continuationPrefix += inlineTail.replace(/[^\t]/g, " ");
  }

  const blankLinePrefix = continuationPrefix.includes(">")
    ? continuationPrefix.trimEnd()
    : continuationPrefix;

  return {
    continuationPrefix,
    blankLinePrefix,
  };
}

function parseStructuralPrefix(beforeSelection: string): { structuralPrefix: string; listPrefix: string } {
  let rest = beforeSelection;
  let structuralPrefix = "";

  const leadingIndent = matchPrefix(rest, /^[ \t]*/);
  structuralPrefix += leadingIndent;
  rest = rest.slice(leadingIndent.length);

  while (rest.startsWith(">")) {
    const quoteToken = matchPrefix(rest, /^>\s?/);
    structuralPrefix += quoteToken;
    rest = rest.slice(quoteToken.length);

    const extraGap = matchPrefix(rest, /^[ \t]*/);
    structuralPrefix += extraGap;
    rest = rest.slice(extraGap.length);
  }

  const listPrefix = matchPrefix(rest, /^(?:[-*+]|\d+[.)])\s+(?:\[[ xX]\]\s+)?/);
  if (listPrefix) {
    structuralPrefix += listPrefix;
  }

  return { structuralPrefix, listPrefix };
}

function matchPrefix(text: string, pattern: RegExp): string {
  const matched = text.match(pattern);
  return matched?.[0] ?? "";
}