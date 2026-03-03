package com.mvp.module2.fusion.service;

import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Phase2B 图片占位保护：
 * 1) 先将 Markdown 图片标记替换成稳定 token，避免模型重写时误改路径；
 * 2) 重写后校验 token 拓扑（数量+顺序+唯一性）；
 * 3) 仅在拓扑一致时恢复图片标记，否则返回 null 触发上层安全回退。
 */
public final class Phase2bImageTopologyGuard {

    private static final Pattern IMAGE_MARKER_PATTERN = Pattern.compile("!\\[[^\\]]*\\]\\([^\\)]+\\)|!\\[\\[[^\\]]+\\]\\]");
    private static final Pattern TOKEN_PATTERN = Pattern.compile("\\[\\[PHASE2B_MEDIA_[A-Z0-9]{12,32}\\]\\]");

    private Phase2bImageTopologyGuard() {
    }

    public static final class MaskResult {
        private final String maskedMarkdown;
        private final LinkedHashMap<String, String> tokenToMarker;

        private MaskResult(String maskedMarkdown, LinkedHashMap<String, String> tokenToMarker) {
            this.maskedMarkdown = maskedMarkdown;
            this.tokenToMarker = tokenToMarker;
        }

        public String getMaskedMarkdown() {
            return maskedMarkdown;
        }

        public LinkedHashMap<String, String> getTokenToMarker() {
            return tokenToMarker;
        }

        public boolean hasProtectedMarkers() {
            return tokenToMarker != null && !tokenToMarker.isEmpty();
        }
    }

    public static MaskResult maskImageMarkers(String markdown) {
        String source = String.valueOf(markdown == null ? "" : markdown);
        if (!StringUtils.hasText(source)) {
            return new MaskResult("", new LinkedHashMap<>());
        }
        LinkedHashMap<String, String> tokenToMarker = new LinkedHashMap<>();
        Set<String> usedTokens = new LinkedHashSet<>();
        Matcher matcher = IMAGE_MARKER_PATTERN.matcher(source);
        StringBuffer buffer = new StringBuffer();
        while (matcher.find()) {
            String token = buildUniqueToken(source, usedTokens);
            usedTokens.add(token);
            tokenToMarker.put(token, matcher.group());
            matcher.appendReplacement(buffer, Matcher.quoteReplacement(token));
        }
        matcher.appendTail(buffer);
        return new MaskResult(buffer.toString(), tokenToMarker);
    }

    public static String restoreImageMarkers(String markdown, MaskResult maskResult) {
        String candidate = String.valueOf(markdown == null ? "" : markdown);
        if (maskResult == null || !maskResult.hasProtectedMarkers()) {
            return candidate;
        }
        LinkedHashMap<String, String> tokenToMarker = maskResult.getTokenToMarker();
        List<String> expectedOrder = new ArrayList<>(tokenToMarker.keySet());
        List<String> actualOrder = extractTokenSequence(candidate);
        if (actualOrder.size() != expectedOrder.size()) {
            return null;
        }
        for (int i = 0; i < expectedOrder.size(); i += 1) {
            if (!expectedOrder.get(i).equals(actualOrder.get(i))) {
                return null;
            }
        }
        String restored = candidate;
        for (Map.Entry<String, String> entry : tokenToMarker.entrySet()) {
            String token = entry.getKey();
            if (token == null || token.isBlank()) {
                continue;
            }
            if (countMatches(restored, token) != 1) {
                return null;
            }
            restored = restored.replace(token, String.valueOf(entry.getValue() == null ? "" : entry.getValue()));
        }
        return restored;
    }

    public static String restoreOrFallback(String markdown, MaskResult maskResult, String fallbackMarkdown) {
        String restored = restoreImageMarkers(markdown, maskResult);
        if (StringUtils.hasText(restored)) {
            return restored;
        }
        return String.valueOf(fallbackMarkdown == null ? "" : fallbackMarkdown);
    }

    private static List<String> extractTokenSequence(String text) {
        List<String> sequence = new ArrayList<>();
        Matcher matcher = TOKEN_PATTERN.matcher(String.valueOf(text == null ? "" : text));
        while (matcher.find()) {
            sequence.add(matcher.group());
        }
        return sequence;
    }

    private static int countMatches(String text, String token) {
        int count = 0;
        int cursor = 0;
        while (true) {
            int next = text.indexOf(token, cursor);
            if (next < 0) {
                return count;
            }
            count += 1;
            cursor = next + token.length();
        }
    }

    private static String buildUniqueToken(String source, Set<String> usedTokens) {
        String safeSource = String.valueOf(source == null ? "" : source);
        for (int i = 0; i < 16; i += 1) {
            String token = "[[PHASE2B_MEDIA_" + UUID.randomUUID()
                    .toString()
                    .replace("-", "")
                    .substring(0, 16)
                    .toUpperCase(Locale.ROOT) + "]]";
            if (safeSource.contains(token)) {
                continue;
            }
            if (usedTokens != null && usedTokens.contains(token)) {
                continue;
            }
            return token;
        }
        return "[[PHASE2B_MEDIA_" + UUID.randomUUID().toString().replace("-", "").toUpperCase(Locale.ROOT) + "]]";
    }
}
