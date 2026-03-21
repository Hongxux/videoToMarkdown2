package com.mvp.module2.fusion.service.llm;

import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

public final class LlmErrorDescriber {
    private static final int DEFAULT_MAX_DEPTH = 4;

    private LlmErrorDescriber() {
    }

    public static String describe(Throwable error) {
        return describe(error, DEFAULT_MAX_DEPTH);
    }

    public static String describe(Throwable error, int maxDepth) {
        if (error == null) {
            return "";
        }
        List<String> parts = new ArrayList<>();
        Throwable cursor = error;
        int depth = 0;
        while (cursor != null && depth < Math.max(1, maxDepth)) {
            String message = String.valueOf(cursor.getMessage() == null ? "" : cursor.getMessage()).trim();
            parts.add(
                    cursor.getClass().getSimpleName()
                            + " ("
                            + (StringUtils.hasText(message) ? message : "<empty>")
                            + ")"
            );
            cursor = cursor.getCause();
            depth += 1;
        }
        return String.join(" -> ", parts);
    }
}
