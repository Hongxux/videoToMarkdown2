package com.mvp.module2.fusion.common;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class TaskDisplayNameResolver {

    private static final Pattern BV_PATTERN = Pattern.compile("(?i)BV[0-9A-Za-z]{10}");
    private static final String DEFAULT_FALLBACK_NAME = "未命名任务";

    private TaskDisplayNameResolver() {
    }

    public static String resolveTaskDisplayTitle(String videoInput, String fallbackTaskId) {
        String normalizedFallback = normalizeText(fallbackTaskId);
        if (videoInput == null || videoInput.isBlank()) {
            return normalizedFallback != null ? normalizedFallback : DEFAULT_FALLBACK_NAME;
        }
        String trimmedInput = videoInput.trim();

        String bvId = extractBvId(trimmedInput);
        if (bvId != null) {
            return bvId;
        }

        String urlDisplay = resolveHttpDisplay(trimmedInput);
        if (urlDisplay != null) {
            return urlDisplay;
        }

        String localPathDisplay = resolveLocalPathDisplay(trimmedInput);
        if (localPathDisplay != null) {
            return localPathDisplay;
        }

        return trimmedInput;
    }

    private static String extractBvId(String value) {
        Matcher matcher = BV_PATTERN.matcher(value);
        if (matcher.find()) {
            return matcher.group();
        }
        return null;
    }

    private static String resolveHttpDisplay(String value) {
        if (!value.startsWith("http://") && !value.startsWith("https://")) {
            return null;
        }
        try {
            URI uri = URI.create(value);
            String path = normalizeText(uri.getPath());
            if (path != null) {
                Path fileName = Paths.get(path).getFileName();
                if (fileName != null) {
                    String tail = normalizeText(fileName.toString());
                    if (tail != null) {
                        return tail;
                    }
                }
            }
            return normalizeText(uri.getHost());
        } catch (Exception ignored) {
            return value;
        }
    }

    private static String resolveLocalPathDisplay(String value) {
        try {
            Path fileName = Paths.get(value).getFileName();
            if (fileName != null) {
                String normalized = normalizeText(fileName.toString());
                if (normalized != null) {
                    return normalized;
                }
            }
        } catch (Exception ignored) {
            return null;
        }
        return null;
    }

    private static String normalizeText(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
