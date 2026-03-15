package com.mvp.module2.fusion.service.llm;

import org.springframework.util.StringUtils;

import java.util.Objects;
import java.util.function.UnaryOperator;

public final class LlmProviderConfig {
    public final String displayName;
    public final String providerKey;
    public final String baseUrl;
    public final String model;
    public final String apiKey;
    public final UnaryOperator<String> modelResolver;

    public LlmProviderConfig(
            String displayName,
            String providerKey,
            String baseUrl,
            String model,
            String apiKey,
            UnaryOperator<String> modelResolver
    ) {
        this.displayName = String.valueOf(displayName == null ? "" : displayName).trim();
        this.providerKey = String.valueOf(providerKey == null ? "" : providerKey).trim();
        this.baseUrl = String.valueOf(baseUrl == null ? "" : baseUrl).trim();
        this.model = String.valueOf(model == null ? "" : model).trim();
        this.apiKey = String.valueOf(apiKey == null ? "" : apiKey).trim();
        this.modelResolver = modelResolver != null ? modelResolver : UnaryOperator.identity();
    }

    public String resolveModel() {
        String resolvedModel = modelResolver.apply(model);
        String resolved = String.valueOf(resolvedModel == null ? "" : resolvedModel).trim();
        if (!StringUtils.hasText(resolved)) {
            throw new IllegalStateException(resolveDisplayName() + " model is empty");
        }
        return resolved;
    }

    public String resolveApiKey() {
        if (!StringUtils.hasText(apiKey)) {
            throw new IllegalStateException(resolveDisplayName() + " api key is empty");
        }
        return apiKey.trim();
    }

    public String resolveBaseUrl() {
        if (!StringUtils.hasText(baseUrl)) {
            throw new IllegalStateException(resolveDisplayName() + " baseUrl is empty");
        }
        String normalized = baseUrl.trim();
        if (normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }

    public String resolveDisplayName() {
        if (StringUtils.hasText(displayName)) {
            return displayName.trim();
        }
        if (StringUtils.hasText(providerKey)) {
            return providerKey.trim();
        }
        return "Provider";
    }

    public String resolveProviderKey() {
        if (StringUtils.hasText(providerKey)) {
            return providerKey.trim();
        }
        return resolveDisplayName().toLowerCase();
    }

    public String resolveChatCompletionsEndpoint() {
        String endpoint = resolveBaseUrl();
        if (endpoint.endsWith("/chat/completions")) {
            return endpoint;
        }
        return endpoint + "/chat/completions";
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof LlmProviderConfig that)) {
            return false;
        }
        return Objects.equals(displayName, that.displayName)
                && Objects.equals(providerKey, that.providerKey)
                && Objects.equals(baseUrl, that.baseUrl)
                && Objects.equals(model, that.model)
                && Objects.equals(apiKey, that.apiKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(displayName, providerKey, baseUrl, model, apiKey);
    }
}
