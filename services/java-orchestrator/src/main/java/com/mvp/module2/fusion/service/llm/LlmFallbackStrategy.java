package com.mvp.module2.fusion.service.llm;

public final class LlmFallbackStrategy {
    public final LlmProviderConfig primaryProvider;
    public final LlmProviderConfig fallbackProvider;

    public LlmFallbackStrategy(LlmProviderConfig primaryProvider, LlmProviderConfig fallbackProvider) {
        this.primaryProvider = primaryProvider;
        this.fallbackProvider = fallbackProvider;
    }

    public boolean hasFallback() {
        return fallbackProvider != null;
    }
}
