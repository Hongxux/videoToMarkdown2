package com.mvp.module2.fusion.controller;

import com.mvp.module2.fusion.common.VideoInputNormalizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class VideoInputNormalizerBridge {

    private static final Logger logger = LoggerFactory.getLogger(VideoInputNormalizerBridge.class);
    private static final AtomicBoolean FALLBACK_LOGGED = new AtomicBoolean(false);

    private static final Pattern BV_PATTERN = Pattern.compile("(?i)BV[0-9A-Za-z]{10}");
    private static final Pattern WINDOWS_ABSOLUTE_PATH = Pattern.compile("^[A-Za-z]:[\\\\/].*");
    private static final Pattern STRICT_HTTP_URL = Pattern.compile("(?i)https?://\\S+");
    private static final Pattern LOOSE_HTTP_URL = Pattern.compile("(?i)https?:[\\\\/]+\\S+");
    private static final String TRAILING_PUNCTUATION = ".,;:!?)]}\"'，。；：！？】）";

    private VideoInputNormalizerBridge() {
    }

    static String normalizeVideoInput(String rawVideoInput) {
        try {
            return VideoInputNormalizer.normalizeVideoInput(rawVideoInput);
        } catch (NoClassDefFoundError | ExceptionInInitializerError error) {
            if (FALLBACK_LOGGED.compareAndSet(false, true)) {
                logger.error("VideoInputNormalizer is unavailable at runtime, fallback normalizer enabled", error);
            } else {
                logger.debug("VideoInputNormalizer unavailable, keep fallback normalizer: {}", error.toString());
            }
            return normalizeFallback(rawVideoInput);
        }
    }

    private static String normalizeFallback(String rawVideoInput) {
        if (rawVideoInput == null) {
            return "";
        }
        String trimmed = rawVideoInput.trim();
        if (trimmed.isEmpty()) {
            return "";
        }
        if (looksLikeLocalPath(trimmed)) {
            return trimmed;
        }

        String extracted = extractHttpUrl(trimmed);
        if (!extracted.isEmpty()) {
            return canonicalizeBilibiliIfNeeded(extracted);
        }

        Matcher bvMatcher = BV_PATTERN.matcher(trimmed);
        if (bvMatcher.find()) {
            return "https://www.bilibili.com/video/" + bvMatcher.group();
        }
        return trimmed;
    }

    private static boolean looksLikeLocalPath(String value) {
        if (value == null || value.isBlank()) {
            return false;
        }
        if (value.startsWith("file://")) {
            return true;
        }
        if (value.startsWith(".") || value.startsWith("/") || value.startsWith("\\")) {
            return true;
        }
        return WINDOWS_ABSOLUTE_PATH.matcher(value).matches();
    }

    private static String extractHttpUrl(String text) {
        Matcher strict = STRICT_HTTP_URL.matcher(text);
        if (strict.find()) {
            return normalizeHttpLikeToken(strict.group());
        }
        Matcher loose = LOOSE_HTTP_URL.matcher(text);
        if (loose.find()) {
            return normalizeHttpLikeToken(loose.group());
        }
        if (startsWithHttpLikePrefix(text)) {
            return normalizeHttpLikeToken(text);
        }
        return "";
    }

    private static boolean startsWithHttpLikePrefix(String text) {
        String lower = text.toLowerCase(Locale.ROOT);
        return lower.startsWith("http://")
                || lower.startsWith("https://")
                || lower.startsWith("http:/")
                || lower.startsWith("https:/")
                || lower.startsWith("http:\\")
                || lower.startsWith("https:\\");
    }

    private static String normalizeHttpLikeToken(String token) {
        if (token == null || token.isBlank()) {
            return "";
        }
        String normalized = stripTrailingPunctuation(token.trim()).replace('\\', '/');
        normalized = normalized.replaceFirst("(?i)^(https?):/*", "$1://");
        try {
            URI uri = URI.create(normalized);
            if (uri.getScheme() == null || uri.getHost() == null || uri.getHost().isBlank()) {
                return "";
            }
        } catch (Exception ignored) {
            return "";
        }
        return normalized;
    }

    private static String stripTrailingPunctuation(String value) {
        String result = value;
        while (!result.isEmpty()) {
            char ch = result.charAt(result.length() - 1);
            if (TRAILING_PUNCTUATION.indexOf(ch) >= 0) {
                result = result.substring(0, result.length() - 1);
                continue;
            }
            break;
        }
        return result;
    }

    private static String canonicalizeBilibiliIfNeeded(String normalizedUrl) {
        if (normalizedUrl == null || normalizedUrl.isBlank()) {
            return "";
        }
        String lower = normalizedUrl.toLowerCase(Locale.ROOT);
        Matcher matcher = BV_PATTERN.matcher(normalizedUrl);
        if (!lower.contains("bilibili.com") || !matcher.find()) {
            return normalizedUrl;
        }
        String canonical = "https://www.bilibili.com/video/" + matcher.group();
        int episodeIndex = extractPositiveIntQueryParam(normalizedUrl, "p");
        if (episodeIndex > 0) {
            return canonical + "?p=" + episodeIndex;
        }
        return canonical;
    }

    private static int extractPositiveIntQueryParam(String rawUrl, String keyName) {
        if (rawUrl == null || rawUrl.isBlank() || keyName == null || keyName.isBlank()) {
            return 0;
        }
        try {
            URI uri = URI.create(rawUrl);
            String query = uri.getRawQuery();
            if (query == null || query.isBlank()) {
                return 0;
            }
            String[] pairs = query.split("&");
            for (String pair : pairs) {
                if (pair == null || pair.isBlank()) {
                    continue;
                }
                int equalsAt = pair.indexOf('=');
                String rawKey = equalsAt >= 0 ? pair.substring(0, equalsAt) : pair;
                String rawValue = equalsAt >= 0 ? pair.substring(equalsAt + 1) : "";
                if (!keyName.equalsIgnoreCase(rawKey)) {
                    continue;
                }
                try {
                    int value = Integer.parseInt(rawValue);
                    if (value > 0) {
                        return value;
                    }
                } catch (NumberFormatException ignored) {
                    return 0;
                }
            }
            return 0;
        } catch (Exception ignored) {
            return 0;
        }
    }
}
