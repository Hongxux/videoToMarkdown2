package com.mvp.module2.fusion.service.llm;

public final class LlmPromptRequest {
    public final String systemPrompt;
    public final String userPrompt;
    public final double temperature;
    public final int maxTokens;
    public final boolean forceJsonObject;

    public LlmPromptRequest(
            String systemPrompt,
            String userPrompt,
            double temperature,
            int maxTokens,
            boolean forceJsonObject
    ) {
        this.systemPrompt = String.valueOf(systemPrompt == null ? "" : systemPrompt);
        this.userPrompt = String.valueOf(userPrompt == null ? "" : userPrompt);
        this.temperature = temperature;
        this.maxTokens = Math.max(1, maxTokens);
        this.forceJsonObject = forceJsonObject;
    }
}
