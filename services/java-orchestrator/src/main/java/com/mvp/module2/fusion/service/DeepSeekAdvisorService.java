package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;
import org.springframework.util.StreamUtils;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class DeepSeekAdvisorService {

    private static final Logger logger = LoggerFactory.getLogger(DeepSeekAdvisorService.class);
    private static final String PROMPT_TEMPLATE_CONTEXT_EMPTY = "（无）";

    private static final String DEFAULT_SYSTEM_PROMPT = String.join("\n",
            "你是阅读场景下的语境解释助手。",
            "你的任务是解释“被选中的词或句子”在当前段落中的具体含义。",
            "禁止脱离段落语境给出词典式、百科式定义。",
            "必须基于上下文线索给出可验证的解释，并指出边界条件。",
            "输出要简洁，可直接用于笔记补充。");

    private static final String DEFAULT_USER_PROMPT = String.join("\n",
            "被解释文本：{term}",
            "解释模式：{scenario}",
            "段落上下文：",
            "{context_block}",
            "锚点句（优先参考）：",
            "{example_block}",
            "",
            "请严格输出 3 条中文 bullet：",
            "1) 本段含义：该词/句在当前段落具体指什么。",
            "2) 推理线索：你依据了哪些上下文证据，为什么这样解释。",
            "3) 边界提醒：在什么条件下该解释会失效或被误读。",
            "",
            "硬性约束：",
            "- 不要输出标题、前言、总结。",
            "- 不要写脱离语境的通用术语定义。",
            "- 优先控制在 120 字以内。");
    @Value("${deepseek.advisor.enabled:true}")
    private boolean advisorEnabled;

    @Value("${deepseek.advisor.base-url:https://api.deepseek.com/v1}")
    private String advisorBaseUrl;

    @Value("${deepseek.advisor.model:deepseek-chat}")
    private String advisorModel;

    @Value("${deepseek.advisor.timeout-seconds:60}")
    private int timeoutSeconds;

    @Value("${DEEPSEEK_API_KEY:}")
    private String apiKey;

    @Value("${deepseek.advisor.prompt.system-resource:classpath:prompts/deepseek-advisor/system-zh.txt}")
    private Resource systemPromptResource;

    @Value("${deepseek.advisor.prompt.user-resource:classpath:prompts/deepseek-advisor/user-zh.txt}")
    private Resource userPromptResource;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(8))
            .build();
    private final Map<String, String> promptTemplateCache = new ConcurrentHashMap<>();

    public AdviceResult requestAdvice(String term, String context, boolean contextDependent) {
        return requestAdvice(term, context, "", contextDependent);
    }

    public AdviceResult requestAdvice(String term, String context, String contextExample, boolean contextDependent) {
        String safeTerm = String.valueOf(term == null ? "" : term).trim();
        if (safeTerm.isEmpty()) {
            throw new IllegalArgumentException("term cannot be empty");
        }
        String safeContext = String.valueOf(context == null ? "" : context).trim();
        String safeContextExample = String.valueOf(contextExample == null ? "" : contextExample).trim();

        if (!advisorEnabled) {
            throw new IllegalStateException("deepseek.advisor.enabled=false");
        }
        if (!StringUtils.hasText(apiKey)) {
            throw new IllegalStateException("DEEPSEEK_API_KEY is empty");
        }

        String content;
        try {
            content = callDeepSeek(safeTerm, safeContext, safeContextExample, contextDependent);
        } catch (Exception ex) {
            throw new IllegalStateException("DeepSeek advisor call failed: " + ex.getMessage(), ex);
        }
        if (!StringUtils.hasText(content)) {
            return AdviceResult.empty("deepseek-empty");
        }
        return AdviceResult.deepseek(content.trim());
    }

    private String callDeepSeek(String term, String context, String contextExample, boolean contextDependent) throws Exception {
        String endpoint = normalizeDeepSeekBaseUrl(advisorBaseUrl);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("model", advisorModel);
        payload.put("temperature", 0.35);
        payload.put("max_tokens", 320);
        payload.put("stream", false);
        payload.put("messages", List.of(
                Map.of("role", "system", "content", buildSystemPrompt()),
                Map.of("role", "user", "content", buildUserPrompt(term, context, contextExample, contextDependent))
        ));
        String payloadJson = objectMapper.writeValueAsString(payload);

        HttpRequest request = HttpRequest.newBuilder(URI.create(endpoint + "/chat/completions"))
                .timeout(Duration.ofSeconds(Math.max(60, timeoutSeconds)))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .header("Authorization", "Bearer " + apiKey.trim())
                .POST(HttpRequest.BodyPublishers.ofString(payloadJson))
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IllegalStateException("DeepSeek HTTP " + response.statusCode() + ": " + summarizeResponseBody(response.body()));
        }

        JsonNode root = objectMapper.readTree(response.body());
        JsonNode choices = root.path("choices");
        if (!choices.isArray() || choices.isEmpty()) {
            return "";
        }
        return choices.get(0).path("message").path("content").asText("");
    }

    private String normalizeDeepSeekBaseUrl(String rawBaseUrl) {
        String endpoint = String.valueOf(rawBaseUrl == null ? "" : rawBaseUrl).trim();
        if (endpoint.endsWith("/")) {
            endpoint = endpoint.substring(0, endpoint.length() - 1);
        }
        if (endpoint.isEmpty()) {
            throw new IllegalStateException("deepseek.advisor.base-url is empty");
        }
        if (!endpoint.matches("(?i).*/v\\d+$")) {
            endpoint = endpoint + "/v1";
        }
        return endpoint;
    }

    private String summarizeResponseBody(String body) {
        String raw = String.valueOf(body == null ? "" : body).replace('\n', ' ').trim();
        if (raw.length() <= 260) {
            return raw;
        }
        return raw.substring(0, 260) + "...";
    }

    private String buildSystemPrompt() {
        return loadPromptTemplate("system", systemPromptResource, DEFAULT_SYSTEM_PROMPT);
    }

    private String buildUserPrompt(String term, String context, String contextExample, boolean contextDependent) {
        String safeContext = trimContext(context);
        String safeExample = trimContext(contextExample);
        String scenario = contextDependent ? "段落绑定" : "全局语境";
        Map<String, String> values = new LinkedHashMap<>();
        values.put("term", term);
        values.put("scenario", scenario);
        values.put("context_block", safeContext.isEmpty() ? PROMPT_TEMPLATE_CONTEXT_EMPTY : safeContext);
        values.put("example_block", safeExample.isEmpty() ? PROMPT_TEMPLATE_CONTEXT_EMPTY : safeExample);
        return applyTemplate(
                loadPromptTemplate("user", userPromptResource, DEFAULT_USER_PROMPT),
                values
        );
    }
    private String loadPromptTemplate(String cacheKey, Resource resource, String defaultTemplate) {
        return promptTemplateCache.computeIfAbsent(cacheKey, key -> readPromptTemplate(resource, defaultTemplate, key));
    }

    private String readPromptTemplate(Resource resource, String defaultTemplate, String templateName) {
        if (resource == null || !resource.exists()) {
            logger.warn("DeepSeek advisor prompt template missing ({}), fallback to default", templateName);
            return defaultTemplate;
        }
        try (InputStream input = resource.getInputStream()) {
            String template = StreamUtils.copyToString(input, StandardCharsets.UTF_8).trim();
            if (StringUtils.hasText(template)) {
                return template;
            }
            logger.warn("DeepSeek advisor prompt template empty ({}), fallback to default", templateName);
        } catch (IOException ex) {
            logger.warn("DeepSeek advisor prompt template load failed ({}): {}", templateName, ex.getMessage());
        }
        return defaultTemplate;
    }

    private String applyTemplate(String template, Map<String, String> values) {
        String resolved = String.valueOf(template == null ? "" : template);
        for (Map.Entry<String, String> entry : values.entrySet()) {
            String key = "{" + entry.getKey() + "}";
            String value = String.valueOf(entry.getValue() == null ? "" : entry.getValue());
            resolved = resolved.replace(key, value);
        }
        return resolved;
    }
    private String trimContext(String context) {
        String normalized = String.valueOf(context == null ? "" : context).replace('\n', ' ').trim();
        if (normalized.length() <= 280) {
            return normalized;
        }
        return normalized.substring(0, 280).trim() + "...";
    }

    public static class AdviceResult {
        public final String advice;
        public final String source;

        private AdviceResult(String advice, String source) {
            this.advice = advice;
            this.source = source;
        }

        public static AdviceResult deepseek(String advice) {
            return new AdviceResult(advice, "deepseek");
        }

        public static AdviceResult empty(String source) {
            return new AdviceResult("", String.valueOf(source == null ? "" : source));
        }
    }
}
