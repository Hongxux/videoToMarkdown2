package com.mvp.module2.fusion.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * 书籍 Markdown 结构保护工具。
 *
 * 目标：
 * 1) 将图片/表格/代码/公式块替换为低碰撞占位符，避免翻译与结构化阶段破坏结构。
 * 2) 保留文本块顺序，后续可按块翻译并按原序回填。
 * 3) 在最终 Markdown 中恢复占位符，保证结构内容不丢失。
 */
public final class BookMarkdownProtectionUtils {

    private static final Pattern IMAGE_INLINE_PATTERN = Pattern.compile("!\\[[^\\]]*\\]\\([^\\)]+\\)|!\\[\\[[^\\]]+\\]\\]");

    private BookMarkdownProtectionUtils() {
    }

    public enum BlockType {
        TEXT,
        PROTECTED
    }

    public static final class ProtectedBlock {
        private final int index;
        private final BlockType type;
        private final String originalText;
        private final String workingText;
        private final String token;

        private ProtectedBlock(int index, BlockType type, String originalText, String workingText, String token) {
            this.index = index;
            this.type = type;
            this.originalText = originalText;
            this.workingText = workingText;
            this.token = token;
        }

        public int getIndex() {
            return index;
        }

        public BlockType getType() {
            return type;
        }

        public String getOriginalText() {
            return originalText;
        }

        public String getWorkingText() {
            return workingText;
        }

        public String getToken() {
            return token;
        }

        public boolean isTranslatable() {
            return type == BlockType.TEXT;
        }
    }

    public static final class ProtectionResult {
        private final List<ProtectedBlock> blocks;
        private final LinkedHashMap<String, String> tokenToOriginalBlock;

        private ProtectionResult(List<ProtectedBlock> blocks, LinkedHashMap<String, String> tokenToOriginalBlock) {
            this.blocks = blocks;
            this.tokenToOriginalBlock = tokenToOriginalBlock;
        }

        public List<ProtectedBlock> getBlocks() {
            return blocks;
        }

        public LinkedHashMap<String, String> getTokenToOriginalBlock() {
            return tokenToOriginalBlock;
        }

        public int getProtectedCount() {
            return tokenToOriginalBlock.size();
        }
    }

    private static final class DraftBlock {
        private final BlockType type;
        private final String text;

        private DraftBlock(BlockType type, String text) {
            this.type = type;
            this.text = text;
        }
    }

    private static final class FenceSpec {
        private final char marker;
        private final int markerLength;

        private FenceSpec(char marker, int markerLength) {
            this.marker = marker;
            this.markerLength = markerLength;
        }
    }

    public static ProtectionResult protectMarkdown(String markdown) {
        String source = normalizeInput(markdown);
        if (source.isBlank()) {
            return new ProtectionResult(Collections.emptyList(), new LinkedHashMap<>());
        }

        List<String> lines = Arrays.asList(source.split("\n", -1));
        List<DraftBlock> drafts = new ArrayList<>();
        StringBuilder textBuffer = new StringBuilder();

        int cursor = 0;
        while (cursor < lines.size()) {
            String line = lines.get(cursor);
            String trimmed = line.trim();

            FenceSpec fenceSpec = parseFenceStart(trimmed);
            if (fenceSpec != null) {
                flushTextBuffer(drafts, textBuffer);
                StringBuilder fenced = new StringBuilder();
                fenced.append(line);
                cursor += 1;
                while (cursor < lines.size()) {
                    String next = lines.get(cursor);
                    fenced.append("\n").append(next);
                    if (isFenceEnd(next.trim(), fenceSpec)) {
                        cursor += 1;
                        break;
                    }
                    cursor += 1;
                }
                drafts.add(new DraftBlock(BlockType.PROTECTED, fenced.toString()));
                continue;
            }

            if ("$$".equals(trimmed)) {
                flushTextBuffer(drafts, textBuffer);
                StringBuilder math = new StringBuilder();
                math.append(line);
                cursor += 1;
                while (cursor < lines.size()) {
                    String next = lines.get(cursor);
                    math.append("\n").append(next);
                    if ("$$".equals(next.trim())) {
                        cursor += 1;
                        break;
                    }
                    cursor += 1;
                }
                drafts.add(new DraftBlock(BlockType.PROTECTED, math.toString()));
                continue;
            }

            if (looksLikeTableLine(trimmed)) {
                flushTextBuffer(drafts, textBuffer);
                StringBuilder table = new StringBuilder();
                while (cursor < lines.size() && looksLikeTableLine(lines.get(cursor).trim())) {
                    if (table.length() > 0) {
                        table.append("\n");
                    }
                    table.append(lines.get(cursor));
                    cursor += 1;
                }
                drafts.add(new DraftBlock(BlockType.PROTECTED, table.toString()));
                continue;
            }

            if (containsImageMarker(line)) {
                flushTextBuffer(drafts, textBuffer);
                drafts.add(new DraftBlock(BlockType.PROTECTED, line));
                cursor += 1;
                continue;
            }

            if (trimmed.isEmpty()) {
                flushTextBuffer(drafts, textBuffer);
                cursor += 1;
                continue;
            }

            if (textBuffer.length() > 0) {
                textBuffer.append("\n");
            }
            textBuffer.append(line);
            cursor += 1;
        }
        flushTextBuffer(drafts, textBuffer);

        Set<String> usedTokens = new LinkedHashSet<>();
        LinkedHashMap<String, String> tokenToOriginal = new LinkedHashMap<>();
        List<ProtectedBlock> finalBlocks = new ArrayList<>();

        for (int i = 0; i < drafts.size(); i++) {
            DraftBlock draft = drafts.get(i);
            if (draft.type == BlockType.TEXT) {
                finalBlocks.add(new ProtectedBlock(i, BlockType.TEXT, draft.text, draft.text, ""));
                continue;
            }
            String token = buildUniqueToken(source, usedTokens);
            usedTokens.add(token);
            tokenToOriginal.put(token, draft.text);
            finalBlocks.add(new ProtectedBlock(i, BlockType.PROTECTED, draft.text, token, token));
        }

        return new ProtectionResult(finalBlocks, tokenToOriginal);
    }

    public static String restoreProtectedBlocks(String markdown, LinkedHashMap<String, String> tokenToOriginalBlock) {
        String content = String.valueOf(markdown == null ? "" : markdown);
        if (content.isBlank()) {
            return content;
        }
        if (tokenToOriginalBlock == null || tokenToOriginalBlock.isEmpty()) {
            return content;
        }

        String restored = content;
        List<String> unresolvedTokens = new ArrayList<>();
        for (var entry : tokenToOriginalBlock.entrySet()) {
            String token = String.valueOf(entry.getKey() == null ? "" : entry.getKey());
            String originalBlock = String.valueOf(entry.getValue() == null ? "" : entry.getValue());
            if (token.isBlank()) {
                continue;
            }
            if (restored.contains(token)) {
                restored = restored.replace(token, originalBlock);
            } else {
                unresolvedTokens.add(token);
            }
        }

        if (unresolvedTokens.isEmpty()) {
            return restored;
        }

        // 占位符未命中时将原始结构块追加到附录，避免结构内容静默丢失。
        StringBuilder appendix = new StringBuilder(restored);
        appendix.append("\n\n## 结构块回填附录\n\n");
        for (String token : unresolvedTokens) {
            String original = tokenToOriginalBlock.get(token);
            if (original == null || original.isBlank()) {
                continue;
            }
            appendix.append(original).append("\n\n");
        }
        return appendix.toString();
    }

    private static String normalizeInput(String markdown) {
        String source = String.valueOf(markdown == null ? "" : markdown);
        source = source.replace("\r\n", "\n").replace('\r', '\n');
        return source;
    }

    private static boolean containsImageMarker(String line) {
        if (line == null || line.isBlank()) {
            return false;
        }
        return IMAGE_INLINE_PATTERN.matcher(line).find();
    }

    private static boolean looksLikeTableLine(String trimmedLine) {
        if (trimmedLine == null || trimmedLine.isBlank()) {
            return false;
        }
        if (!(trimmedLine.startsWith("|") || trimmedLine.endsWith("|"))) {
            return false;
        }
        int pipeCount = 0;
        for (int i = 0; i < trimmedLine.length(); i++) {
            if (trimmedLine.charAt(i) == '|') {
                pipeCount += 1;
            }
        }
        return pipeCount >= 2;
    }

    private static FenceSpec parseFenceStart(String trimmedLine) {
        if (trimmedLine == null || trimmedLine.length() < 3) {
            return null;
        }
        char first = trimmedLine.charAt(0);
        if (first != '`' && first != '~') {
            return null;
        }
        int index = 0;
        while (index < trimmedLine.length() && trimmedLine.charAt(index) == first) {
            index += 1;
        }
        if (index < 3) {
            return null;
        }
        return new FenceSpec(first, index);
    }

    private static boolean isFenceEnd(String trimmedLine, FenceSpec spec) {
        if (trimmedLine == null || spec == null || trimmedLine.length() < spec.markerLength) {
            return false;
        }
        for (int i = 0; i < spec.markerLength; i++) {
            if (trimmedLine.charAt(i) != spec.marker) {
                return false;
            }
        }
        return true;
    }

    private static void flushTextBuffer(List<DraftBlock> drafts, StringBuilder textBuffer) {
        if (textBuffer == null || textBuffer.length() == 0) {
            return;
        }
        String text = textBuffer.toString().trim();
        textBuffer.setLength(0);
        if (text.isBlank()) {
            return;
        }
        drafts.add(new DraftBlock(BlockType.TEXT, text));
    }

    private static String buildUniqueToken(String source, Set<String> usedTokens) {
        String safeSource = String.valueOf(source == null ? "" : source);
        for (int i = 0; i < 16; i++) {
            String id = UUID.randomUUID().toString().replace("-", "")
                    .substring(0, 20)
                    .toUpperCase(Locale.ROOT);
            String token = "[[SYS_MEDIA_" + id + "]]";
            if (safeSource.contains(token)) {
                continue;
            }
            if (usedTokens != null && usedTokens.contains(token)) {
                continue;
            }
            return token;
        }
        String fallback = UUID.randomUUID().toString().replace("-", "").toUpperCase(Locale.ROOT);
        return "[[SYS_MEDIA_" + fallback + "]]";
    }
}
