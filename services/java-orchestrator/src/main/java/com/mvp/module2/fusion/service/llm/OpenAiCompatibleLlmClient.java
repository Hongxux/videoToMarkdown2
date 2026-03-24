package com.mvp.module2.fusion.service.llm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.util.StreamUtils;
import org.springframework.util.StringUtils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

public final class OpenAiCompatibleLlmClient implements LlmClient {
    private static final int RESPONSE_SUMMARY_MAX_CHARS = 240;

    private final ObjectMapper objectMapper;
    private final Supplier<HttpClient> httpClientSupplier;
    private final IntSupplier timeoutSecondsSupplier;

    public OpenAiCompatibleLlmClient(
            ObjectMapper objectMapper,
            Supplier<HttpClient> httpClientSupplier,
            IntSupplier timeoutSecondsSupplier
    ) {
        this.objectMapper = objectMapper;
        this.httpClientSupplier = httpClientSupplier;
        this.timeoutSecondsSupplier = timeoutSecondsSupplier;
    }

    @Override
    public LlmResponse complete(LlmProviderConfig provider, LlmPromptRequest request) throws Exception {
        String payloadJson = buildPayloadJson(provider, request, false);
        HttpRequest httpRequest = HttpRequest.newBuilder(URI.create(provider.resolveChatCompletionsEndpoint()))
                .timeout(Duration.ofSeconds(resolveTimeoutSeconds()))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .header("Authorization", "Bearer " + provider.resolveApiKey())
                .POST(HttpRequest.BodyPublishers.ofString(payloadJson))
                .build();
        HttpResponse<String> response = resolveHttpClient().send(httpRequest, HttpResponse.BodyHandlers.ofString());
        String responseBody = String.valueOf(response.body() == null ? "" : response.body());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IllegalStateException(provider.resolveDisplayName() + " HTTP " + response.statusCode() + ": " + summarizeResponseBody(responseBody));
        }
        JsonNode root = objectMapper.readTree(responseBody);
        JsonNode choices = root.path("choices");
        if (!choices.isArray() || choices.isEmpty()) {
            return new LlmResponse("", payloadJson, responseBody, "");
        }
        String content = choices.get(0).path("message").path("content").asText("");
        String finishReason = choices.get(0).path("finish_reason").asText("");
        return new LlmResponse(content, payloadJson, responseBody, finishReason);
    }

    @Override
    public LlmResponse stream(LlmProviderConfig provider, LlmPromptRequest request, Consumer<String> onDelta) throws Exception {
        String payloadJson = buildPayloadJson(provider, request, true);
        HttpRequest httpRequest = HttpRequest.newBuilder(URI.create(provider.resolveChatCompletionsEndpoint()))
                .timeout(Duration.ofSeconds(resolveTimeoutSeconds()))
                .header("Content-Type", "application/json")
                .header("Accept", "text/event-stream")
                .header("Authorization", "Bearer " + provider.resolveApiKey())
                .POST(HttpRequest.BodyPublishers.ofString(payloadJson))
                .build();
        HttpResponse<InputStream> response = resolveHttpClient().send(httpRequest, HttpResponse.BodyHandlers.ofInputStream());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            String responseBody;
            try (InputStream errorStream = response.body()) {
                responseBody = StreamUtils.copyToString(errorStream, StandardCharsets.UTF_8);
            }
            throw new IllegalStateException(provider.resolveDisplayName() + " HTTP " + response.statusCode() + ": " + summarizeResponseBody(responseBody));
        }

        StringBuilder aggregated = new StringBuilder();
        StringBuilder rawStream = new StringBuilder();
        String finishReason = "";
        try (InputStream bodyStream = response.body();
             BufferedReader reader = new BufferedReader(new InputStreamReader(bodyStream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                rawStream.append(line).append('\n');
                String trimmed = String.valueOf(line);
                if (!StringUtils.hasText(trimmed) || !trimmed.startsWith("data:")) {
                    continue;
                }
                String data = trimmed.substring(5);
                if (!StringUtils.hasText(data)) {
                    continue;
                }
                if ("[DONE]".equals(data)) {
                    break;
                }
                JsonNode chunkRoot;
                try {
                    chunkRoot = objectMapper.readTree(data);
                } catch (Exception ignored) {
                    continue;
                }
                JsonNode choices = chunkRoot.path("choices");
                if (!choices.isArray() || choices.isEmpty()) {
                    continue;
                }
                JsonNode first = choices.get(0);
                String delta = first.path("delta").path("content").asText("");
                if (!delta.isEmpty()) {
                    aggregated.append(delta);
                    if (onDelta != null) {
                        onDelta.accept(delta);
                    }
                }
                String nextFinishReason = first.path("finish_reason").asText("");
                if (StringUtils.hasText(nextFinishReason)) {
                    finishReason = nextFinishReason;
                }
            }
        }
        return new LlmResponse(aggregated.toString(), payloadJson, rawStream.toString().trim(), finishReason);
    }

    private String buildPayloadJson(LlmProviderConfig provider, LlmPromptRequest request, boolean stream) throws Exception {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("model", provider.resolveModel());
        payload.put("temperature", request.temperature);
        payload.put("max_tokens", request.maxTokens);
        payload.put("stream", stream);
        payload.put("messages", List.of(
                Map.of("role", "system", "content", request.systemPrompt),
                Map.of("role", "user", "content", request.userPrompt)
        ));
        if (request.forceJsonObject) {
            payload.put("response_format", Map.of("type", "json_object"));
        }
        return objectMapper.writeValueAsString(payload);
    }

    private HttpClient resolveHttpClient() {
        return httpClientSupplier.get();
    }

    private int resolveTimeoutSeconds() {
        int configured = timeoutSecondsSupplier.getAsInt();
        return Math.max(60, configured);
    }

    private String summarizeResponseBody(String body) {
        String normalized = String.valueOf(body == null ? "" : body).replace("\r", " ").replace("\n", " ").trim();
        if (!StringUtils.hasText(normalized)) {
            return "";
        }
        if (normalized.length() <= RESPONSE_SUMMARY_MAX_CHARS) {
            return normalized;
        }
        return normalized.substring(0, RESPONSE_SUMMARY_MAX_CHARS) + "...";
    }
}
