"use strict";
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toCommonJS = (mod) => __copyProps(__defProp({}, "__esModule", { value: true }), mod);

// main.ts
var main_exports = {};
__export(main_exports, {
  default: () => Phase2bStructuredRewritePlugin
});
module.exports = __toCommonJS(main_exports);
var import_obsidian = require("obsidian");
var DEFAULT_SETTINGS = {
  backendBaseUrl: "http://127.0.0.1:8080",
  endpointPath: "/api/mobile/cards/phase2b/structured-markdown",
  stripOuterMarkdownFence: true
};
var OUTER_MARKDOWN_FENCE = /^\s*```(?:markdown|md)?\s*([\s\S]*?)\s*```\s*$/i;
var STATUS_LABELS = {
  idle: "Phase2B\uFF1A\u7A7A\u95F2",
  running: "Phase2B\uFF1A\u6B63\u5728\u6267\u884C\u6539\u5199",
  received: "Phase2B\uFF1A\u5DF2\u63A5\u6536\u7ED3\u6784\u5316\u8FD4\u56DE\u6587\u672C",
  completed: "Phase2B\uFF1A\u5B8C\u6210\u6539\u5199",
  failed: "Phase2B\uFF1A\u6539\u5199\u5931\u8D25"
};
var Phase2bStructuredRewritePlugin = class extends import_obsidian.Plugin {
  constructor() {
    super(...arguments);
    this.latestRewriteToken = 0;
    this.statusResetTimer = null;
  }
  async onload() {
    await this.loadSettings();
    this.statusBarEl = this.addStatusBarItem();
    this.updateRewriteStatus("idle");
    this.addSettingTab(new StructuredRewriteSettingTab(this.app, this));
    this.addCommand({
      id: "rewrite-selection-with-phase2b",
      name: "\u7ED3\u6784\u5316\u6539\u5199\u9009\u4E2D\u6587\u672C",
      hotkeys: [{ modifiers: ["Mod", "Shift"], key: "S" }],
      editorCheckCallback: (checking, editor) => {
        const selected = editor.getSelection();
        if (!selected || !selected.trim()) {
          return false;
        }
        if (!checking) {
          void this.rewriteSelection(editor);
        }
        return true;
      }
    });
  }
  onunload() {
    this.clearStatusResetTimer();
  }
  async loadSettings() {
    this.settings = Object.assign({}, DEFAULT_SETTINGS, await this.loadData());
  }
  async saveSettings() {
    await this.saveData(this.settings);
  }
  async rewriteSelection(editor) {
    const snapshot = captureSelectionSnapshot(editor);
    if (!snapshot) {
      new import_obsidian.Notice("\u8BF7\u5148\u9009\u4E2D\u9700\u8981\u7ED3\u6784\u5316\u7684\u6587\u672C");
      return;
    }
    const rewriteToken = ++this.latestRewriteToken;
    const requestBody = {
      bodyText: snapshot.selectedText,
      sourceText: snapshot.selectedText
    };
    this.updateRewriteStatus("running", {
      detail: `\u5DF2\u51BB\u7ED3 ${snapshot.selectedText.length} \u4E2A\u5B57\u7B26\u7684\u9009\u533A`,
      rewriteToken
    });
    new import_obsidian.Notice("\u6B63\u5728\u6267\u884C\u6539\u5199\uFF0C\u8BF7\u7A0D\u5019...");
    try {
      const response = await (0, import_obsidian.requestUrl)({
        url: buildEndpointUrl(this.settings.backendBaseUrl, this.settings.endpointPath),
        method: "POST",
        contentType: "application/json",
        headers: {
          Accept: "application/json"
        },
        body: JSON.stringify(requestBody),
        throw: false
      });
      const payload = parseResponsePayload(response.text);
      if (response.status < 200 || response.status >= 300) {
        const serverMessage = typeof payload.message === "string" && payload.message.trim() ? payload.message.trim() : `\u63A5\u53E3\u8C03\u7528\u5931\u8D25\uFF0CHTTP ${response.status}`;
        throw new Error(serverMessage);
      }
      const normalizedMarkdown = normalizeReturnedMarkdown(
        String(payload.markdown ?? ""),
        this.settings.stripOuterMarkdownFence
      );
      if (!normalizedMarkdown.trim()) {
        throw new Error("\u63A5\u53E3\u8FD4\u56DE\u4E3A\u7A7A\uFF0C\u672A\u751F\u6210\u53EF\u66FF\u6362\u7684 Markdown");
      }
      this.updateRewriteStatus("received", {
        detail: `\u5DF2\u63A5\u6536 ${normalizedMarkdown.length} \u4E2A\u5B57\u7B26\u7684\u7ED3\u6784\u5316\u6587\u672C`,
        rewriteToken
      });
      const replacement = adaptMarkdownToSelectionContext(normalizedMarkdown, snapshot.context);
      const applyMode = applyReplacementFromSnapshot(editor, snapshot, replacement);
      const notices = [];
      if (applyMode === "search-fallback") {
        notices.push("\u9009\u533A\u867D\u5DF2\u53D6\u6D88\uFF0C\u4F46\u5DF2\u6309\u7F13\u5B58\u6587\u672C\u91CD\u65B0\u5B9A\u4F4D\u5E76\u66FF\u6362");
      }
      if (Array.isArray(payload.linkWarnings) && payload.linkWarnings.length > 0) {
        notices.push(`\u9644\u5E26 ${payload.linkWarnings.length} \u6761\u94FE\u63A5\u8B66\u544A`);
      }
      const completionDetail = applyMode === "search-fallback" ? "\u5DF2\u6309\u7F13\u5B58\u6587\u672C\u91CD\u65B0\u5B9A\u4F4D\u5E76\u5B8C\u6210\u56DE\u586B" : "\u5DF2\u6309\u539F\u59CB\u9009\u533A\u5B8C\u6210\u56DE\u586B";
      this.updateRewriteStatus("completed", {
        detail: completionDetail,
        autoResetMs: 5e3,
        rewriteToken
      });
      const suffix = notices.length > 0 ? `\uFF08${notices.join("\uFF1B")}\uFF09` : "";
      new import_obsidian.Notice(`\u7ED3\u6784\u5316\u6539\u5199\u5B8C\u6210${suffix}`);
    } catch (error) {
      const message = error instanceof Error ? error.message : "\u672A\u77E5\u9519\u8BEF";
      this.updateRewriteStatus("failed", {
        detail: message,
        autoResetMs: 8e3,
        rewriteToken
      });
      new import_obsidian.Notice(`\u7ED3\u6784\u5316\u6539\u5199\u5931\u8D25\uFF1A${message}`);
    }
  }
  updateRewriteStatus(phase, options = {}) {
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
  clearStatusResetTimer() {
    if (this.statusResetTimer !== null) {
      window.clearTimeout(this.statusResetTimer);
      this.statusResetTimer = null;
    }
  }
};
var StructuredRewriteSettingTab = class extends import_obsidian.PluginSettingTab {
  constructor(app, plugin) {
    super(app, plugin);
    this.plugin = plugin;
  }
  display() {
    const { containerEl } = this;
    containerEl.empty();
    containerEl.createEl("h2", { text: "Phase2B \u7ED3\u6784\u5316\u6539\u5199\u8BBE\u7F6E" });
    new import_obsidian.Setting(containerEl).setName("\u540E\u7AEF\u57FA\u7840\u5730\u5740").setDesc("\u793A\u4F8B\uFF1Ahttp://127.0.0.1:8080").addText(
      (text) => text.setPlaceholder(DEFAULT_SETTINGS.backendBaseUrl).setValue(this.plugin.settings.backendBaseUrl).onChange(async (value) => {
        this.plugin.settings.backendBaseUrl = value.trim() || DEFAULT_SETTINGS.backendBaseUrl;
        await this.plugin.saveSettings();
      })
    );
    new import_obsidian.Setting(containerEl).setName("\u63A5\u53E3\u8DEF\u5F84").setDesc("\u9ED8\u8BA4\u590D\u7528\u73B0\u6709 /phase2b/structured-markdown").addText(
      (text) => text.setPlaceholder(DEFAULT_SETTINGS.endpointPath).setValue(this.plugin.settings.endpointPath).onChange(async (value) => {
        this.plugin.settings.endpointPath = value.trim() || DEFAULT_SETTINGS.endpointPath;
        await this.plugin.saveSettings();
      })
    );
    new import_obsidian.Setting(containerEl).setName("\u81EA\u52A8\u5265\u79BB\u5916\u5C42 Markdown \u4EE3\u7801\u5757").setDesc("\u5F53\u6A21\u578B\u8BEF\u8FD4\u56DE fenced markdown \u5305\u88F9\u65F6\uFF0C\u81EA\u52A8\u63D0\u53D6\u6B63\u6587").addToggle(
      (toggle) => toggle.setValue(this.plugin.settings.stripOuterMarkdownFence).onChange(async (value) => {
        this.plugin.settings.stripOuterMarkdownFence = value;
        await this.plugin.saveSettings();
      })
    );
  }
};
function buildEndpointUrl(baseUrl, endpointPath) {
  const normalizedBaseUrl = String(baseUrl || DEFAULT_SETTINGS.backendBaseUrl).trim().replace(/\/+$/, "");
  const normalizedPath = String(endpointPath || DEFAULT_SETTINGS.endpointPath).trim();
  if (!normalizedPath) {
    return normalizedBaseUrl + DEFAULT_SETTINGS.endpointPath;
  }
  return normalizedPath.startsWith("/") ? `${normalizedBaseUrl}${normalizedPath}` : `${normalizedBaseUrl}/${normalizedPath}`;
}
function buildStatusLabel(phase, detail) {
  const baseLabel = STATUS_LABELS[phase];
  const normalizedDetail = normalizeStatusDetail(detail);
  return normalizedDetail ? `${baseLabel}\uFF5C${normalizedDetail}` : baseLabel;
}
function normalizeStatusDetail(detail) {
  const normalized = String(detail ?? "").replace(/\s+/g, " ").trim();
  if (!normalized) {
    return "";
  }
  return normalized.length > 32 ? `${normalized.slice(0, 32)}\u2026` : normalized;
}
function parseResponsePayload(rawText) {
  try {
    return JSON.parse(String(rawText ?? ""));
  } catch {
    return {
      message: String(rawText ?? "").trim() || "\u670D\u52A1\u7AEF\u8FD4\u56DE\u4E86\u975E JSON \u5185\u5BB9"
    };
  }
}
function normalizeReturnedMarkdown(markdown, stripOuterFence) {
  const normalized = String(markdown ?? "").replace(/\r\n/g, "\n");
  const withoutFence = stripOuterFence ? normalized.replace(OUTER_MARKDOWN_FENCE, "$1") : normalized;
  return stripOuterBlankLines(withoutFence);
}
function stripOuterBlankLines(text) {
  return String(text ?? "").replace(/^(?:\s*\n)+/, "").replace(/(?:\n\s*)+$/, "");
}
function captureSelectionSnapshot(editor) {
  const selectedText = editor.getSelection();
  if (!selectedText || !selectedText.trim()) {
    return null;
  }
  const from = cloneEditorPosition(editor.getCursor("from"));
  const to = cloneEditorPosition(editor.getCursor("to"));
  return {
    selectedText,
    from,
    to,
    context: analyzeLineContext(editor, from)
  };
}
function applyReplacementFromSnapshot(editor, snapshot, replacement) {
  const currentRangeText = editor.getRange(snapshot.from, snapshot.to);
  if (currentRangeText === snapshot.selectedText) {
    editor.replaceRange(replacement, snapshot.from, snapshot.to);
    return "saved-range";
  }
  const fallbackRange = findUniqueTextRange(editor.getValue(), snapshot.selectedText);
  if (!fallbackRange) {
    throw new Error("\u539F\u9009\u533A\u5DF2\u53D8\u5316\uFF0C\u4E14\u65E0\u6CD5\u552F\u4E00\u5B9A\u4F4D\u7F13\u5B58\u6587\u672C\uFF1B\u8BF7\u91CD\u65B0\u9009\u4E2D\u540E\u518D\u8BD5");
  }
  editor.replaceRange(replacement, fallbackRange.from, fallbackRange.to);
  return "search-fallback";
}
function findUniqueTextRange(documentText, targetText) {
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
  const from = offsetToEditorPosition(documentText, firstIndex);
  const to = offsetToEditorPosition(documentText, firstIndex + targetText.length);
  return { from, to };
}
function offsetToEditorPosition(text, offset) {
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
    ch: safeOffset - lineStart
  };
}
function cloneEditorPosition(position) {
  return {
    line: position.line,
    ch: position.ch
  };
}
function adaptMarkdownToSelectionContext(markdown, context) {
  const lines = markdown.split("\n");
  if (lines.length <= 1) {
    return markdown;
  }
  return lines.map((line, index) => {
    if (index === 0) {
      return line;
    }
    if (!line.trim()) {
      return context.blankLinePrefix;
    }
    return `${context.continuationPrefix}${line}`;
  }).join("\n");
}
function analyzeLineContext(editor, from) {
  const lineText = editor.getLine(from.line);
  const beforeSelection = lineText.slice(0, from.ch);
  const { structuralPrefix, listPrefix } = parseStructuralPrefix(beforeSelection);
  const inlineTail = beforeSelection.slice(structuralPrefix.length);
  let continuationPrefix = structuralPrefix;
  if (listPrefix) {
    continuationPrefix = structuralPrefix.slice(0, structuralPrefix.length - listPrefix.length) + " ".repeat(listPrefix.length);
  }
  if (inlineTail.length > 0) {
    continuationPrefix += inlineTail.replace(/[^\t]/g, " ");
  }
  const blankLinePrefix = continuationPrefix.includes(">") ? continuationPrefix.trimEnd() : continuationPrefix;
  return {
    continuationPrefix,
    blankLinePrefix
  };
}
function parseStructuralPrefix(beforeSelection) {
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
function matchPrefix(text, pattern) {
  const matched = text.match(pattern);
  return matched?.[0] ?? "";
}
