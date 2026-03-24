package com.mvp.module2.fusion.service.llm;

public final class LlmGatewayResult {
    public final String content;
    public final LlmProviderConfig provider;
    public final LlmResponse response;
    public final boolean degraded;
    public final boolean partialStreamObserved;
    public final int attemptCount;

    public LlmGatewayResult(
            String content,
            LlmProviderConfig provider,
            LlmResponse response,
            boolean degraded,
            boolean partialStreamObserved,
            int attemptCount
    ) {
        this.content = String.valueOf(content == null ? "" : content).trim();
        this.provider = provider;
        this.response = response;
        this.degraded = degraded;
        this.partialStreamObserved = partialStreamObserved;
        this.attemptCount = Math.max(0, attemptCount);
    }
}
