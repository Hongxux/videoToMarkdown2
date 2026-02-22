package com.mvp.module2.fusion.service;

import java.util.Locale;
import java.util.Map;

public final class DeepSeekModelRouter {

    private static final Map<String, String> MODEL_ALIASES = Map.ofEntries(
            Map.entry("deepseek-r1", "deepseek-reasoner"),
            Map.entry("r1", "deepseek-reasoner"),
            Map.entry("deepseek-v3", "deepseek-chat"),
            Map.entry("v3", "deepseek-chat"),
            Map.entry("v3 reasoner", "deepseek-reasoner"),
            Map.entry("v3_reasoner", "deepseek-reasoner"),
            Map.entry("deepseek-v3-reasoner", "deepseek-reasoner"),
            Map.entry("deepseek-v3.2-reasoner", "deepseek-reasoner"),
            Map.entry("v3.2 reasoner", "deepseek-reasoner"),
            Map.entry("v3.2_reasoner", "deepseek-reasoner"),
            Map.entry("deepseek-resoner", "deepseek-reasoner"),
            Map.entry("v3-resoner", "deepseek-reasoner"),
            Map.entry("v3.2-resoner", "deepseek-reasoner")
    );

    private DeepSeekModelRouter() {
    }

    public static String resolveModel(String configuredModel) {
        String raw = String.valueOf(configuredModel == null ? "" : configuredModel).trim();
        if (raw.isEmpty()) {
            return "";
        }
        String lowered = raw.toLowerCase(Locale.ROOT);
        return MODEL_ALIASES.getOrDefault(lowered, raw);
    }
}
