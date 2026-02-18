package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class DeepSeekAdvisorService {

    private static final Logger logger = LoggerFactory.getLogger(DeepSeekAdvisorService.class);

    @Value("${deepseek.advisor.enabled:true}")
    private boolean advisorEnabled;

    @Value("${deepseek.advisor.base-url:https://api.deepseek.com}")
    private String advisorBaseUrl;

    @Value("${deepseek.advisor.model:deepseek-chat}")
    private String advisorModel;

    @Value("${deepseek.advisor.timeout-seconds:18}")
    private int timeoutSeconds;

    @Value("${DEEPSEEK_API_KEY:}")
    private String apiKey;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(8))
            .build();

    public AdviceResult requestAdvice(String term, String context, boolean contextDependent) {
        String safeTerm = String.valueOf(term == null ? "" : term).trim();
        if (safeTerm.isEmpty()) {
            throw new IllegalArgumentException("term 不能为空");
        }
        String safeContext = String.valueOf(context == null ? "" : context).trim();
        if (!advisorEnabled || !StringUtils.hasText(apiKey)) {
            return AdviceResult.fallback(buildFallbackAdvice(safeTerm, safeContext, contextDependent));
        }

        try {
            String content = callDeepSeek(safeTerm, safeContext, contextDependent);
            if (!StringUtils.hasText(content)) {
                return AdviceResult.fallback(buildFallbackAdvice(safeTerm, safeContext, contextDependent));
            }
            return AdviceResult.deepseek(content.trim());
        } catch (Exception ex) {
            logger.warn("DeepSeek 顾问调用失败，已回退本地建议: {}", ex.getMessage());
            return AdviceResult.fallback(buildFallbackAdvice(safeTerm, safeContext, contextDependent));
        }
    }

    private String callDeepSeek(String term, String context, boolean contextDependent) throws Exception {
        String endpoint = String.valueOf(advisorBaseUrl == null ? "" : advisorBaseUrl).trim();
        if (endpoint.endsWith("/")) {
            endpoint = endpoint.substring(0, endpoint.length() - 1);
        }
        if (endpoint.isEmpty()) {
            throw new IllegalStateException("deepseek.advisor.base-url 未配置");
        }

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("model", advisorModel);
        payload.put("temperature", 0.45);
        payload.put("max_tokens", 240);
        payload.put("messages", List.of(
                Map.of("role", "system", "content", buildSystemPrompt(contextDependent)),
                Map.of("role", "user", "content", buildUserPrompt(term, context, contextDependent))
        ));
        String payloadJson = objectMapper.writeValueAsString(payload);

        HttpRequest request = HttpRequest.newBuilder(URI.create(endpoint + "/chat/completions"))
                .timeout(Duration.ofSeconds(Math.max(5, timeoutSeconds)))
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + apiKey.trim())
                .POST(HttpRequest.BodyPublishers.ofString(payloadJson))
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IllegalStateException("HTTP " + response.statusCode());
        }

        JsonNode root = objectMapper.readTree(response.body());
        JsonNode choices = root.path("choices");
        if (!choices.isArray() || choices.isEmpty()) {
            return "";
        }
        return choices.get(0).path("message").path("content").asText("");
    }

    private String buildSystemPrompt(boolean contextDependent) {
        if (contextDependent) {
            return "你是一位敏锐的深度阅读顾问（Close Reader）。你关注的不是词典定义，而是词语在特定语境下的微观作用、修辞意图和独特指代。";
        }
        return "你是一位精通第一性原理的认知专家。解释概念时，请直击本质（Essence），拒绝堆砌现象。请用物理学或系统论的视角来拆解概念。";
    }

    private String buildUserPrompt(String term, String context, boolean contextDependent) {
        String safeContext = trimContext(context);
        if (contextDependent && StringUtils.hasText(safeContext)) {
            return "语境：\n" + safeContext + "\n\n请分析关键词【" + term + "】：\n" +
                   "1. **语境义**：它在这里具体指代什么？与通用的词典定义有何微妙不同？\n" +
                   "2. **功能**：作者由它引出了什么思考，或起到了什么修辞作用？";
        }
        return "请解释概念【" + term + "】。\n请输出三点（每点不超过 40 字）：\n" +
               "1. **本质定义**：用第一性原理一句话概括（这是什么）。\n" +
               "2. **直觉模型**：给出一个直观的物理/力学类比（像什么）。\n" +
               "3. **认知误区**：指出人们常犯的一个理解错误（不是什么）。";
    }

    private String buildFallbackAdvice(String term, String context, boolean contextDependent) {
        if (contextDependent && StringUtils.hasText(context)) {
            String snippet = trimContext(context);
            return "在这段语境里，“" + term + "”更像一个作用点，而不是孤立定义。先写下它在原文中具体改变了什么，再补一句你的判断。"
                    + (snippet.isEmpty() ? "" : "\n上下文线索：" + snippet);
        }
        return "先用一句话定义“" + term + "”，再补一个反例或边界条件。这样卡片更容易复习，也更不容易记偏。";
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

        public static AdviceResult fallback(String advice) {
            return new AdviceResult(advice, "fallback");
        }
    }
}
