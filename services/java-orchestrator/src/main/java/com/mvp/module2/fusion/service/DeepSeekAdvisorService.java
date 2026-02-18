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
        return requestAdvice(term, context, "", contextDependent);
    }

    public AdviceResult requestAdvice(String term, String context, String contextExample, boolean contextDependent) {
        String safeTerm = String.valueOf(term == null ? "" : term).trim();
        if (safeTerm.isEmpty()) {
            throw new IllegalArgumentException("term cannot be empty");
        }
        String safeContext = String.valueOf(context == null ? "" : context).trim();
        String safeContextExample = String.valueOf(contextExample == null ? "" : contextExample).trim();

        if (!advisorEnabled || !StringUtils.hasText(apiKey)) {
            return AdviceResult.fallback(buildFallbackAdvice(safeTerm, safeContext, safeContextExample, contextDependent));
        }

        try {
            String content = callDeepSeek(safeTerm, safeContext, safeContextExample, contextDependent);
            if (!StringUtils.hasText(content)) {
                return AdviceResult.fallback(buildFallbackAdvice(safeTerm, safeContext, safeContextExample, contextDependent));
            }
            return AdviceResult.deepseek(content.trim());
        } catch (Exception ex) {
            logger.warn("DeepSeek advisor call failed, fallback to local advice: {}", ex.getMessage());
            return AdviceResult.fallback(buildFallbackAdvice(safeTerm, safeContext, safeContextExample, contextDependent));
        }
    }

    private String callDeepSeek(String term, String context, String contextExample, boolean contextDependent) throws Exception {
        String endpoint = String.valueOf(advisorBaseUrl == null ? "" : advisorBaseUrl).trim();
        if (endpoint.endsWith("/")) {
            endpoint = endpoint.substring(0, endpoint.length() - 1);
        }
        if (endpoint.isEmpty()) {
            throw new IllegalStateException("deepseek.advisor.base-url is empty");
        }

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("model", advisorModel);
        payload.put("temperature", 0.35);
        payload.put("max_tokens", 320);
        payload.put("messages", List.of(
                Map.of("role", "system", "content", buildSystemPrompt()),
                Map.of("role", "user", "content", buildUserPrompt(term, context, contextExample, contextDependent))
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

    private String buildSystemPrompt() {
        return String.join("\n",
                "You are a Zettelkasten writing advisor.",
                "Return a standalone Thought card, never a glossary or dictionary entry.",
                "The response must be a claim that can connect to other cards.",
                "Use the current context only as an example/evidence, not as the whole definition.",
                "Do not start with 'X is ...', 'X refers to ...', or similar definition patterns.",
                "Keep output concise and directly usable in a note.");
    }

    private String buildUserPrompt(String term, String context, String contextExample, boolean contextDependent) {
        String safeContext = trimContext(context);
        String safeExample = trimContext(contextExample);
        String scenario = contextDependent ? "context-linked" : "global";
        return String.join("\n",
                "Target term: " + term,
                "Card mode: thought-only (" + scenario + ")",
                "Context excerpt (for reference):",
                safeContext.isEmpty() ? "(none)" : safeContext,
                "Context example to cite (must be treated as one example, not full definition):",
                safeExample.isEmpty() ? "(none)" : safeExample,
                "",
                "Write exactly 4 bullet points in Chinese:",
                "1) 主张：一句完整判断（可被反驳/支持）。",
                "2) 机制：为什么成立（因果或结构约束）。",
                "3) 语境例子：引用上面的语境作为例子，不得写成术语定义。",
                "4) 边界：何时不成立或容易误用。",
                "",
                "Hard constraints:",
                "- 禁止名词解释、词典口吻、百科口吻。",
                "- 不要输出标题、前言、总结，只输出 4 条 bullet。");
    }

    private String buildFallbackAdvice(String term, String context, String contextExample, boolean contextDependent) {
        String safeContext = trimContext(context);
        String safeExample = trimContext(contextExample);
        String example = firstNonBlank(safeExample, safeContext);
        String mode = contextDependent ? "语境驱动" : "全局驱动";
        if (StringUtils.hasText(example)) {
            return String.join("\n",
                    "- 主张：围绕「" + term + "」写一句可独立成立的判断（" + mode + "）。",
                    "- 机制：补一句“为什么成立”，避免只解释词义。",
                    "- 语境例子：引用这段语境作为例子，而不是定义本身：" + example,
                    "- 边界：补一句反例或不适用条件。");
        }
        return String.join("\n",
                "- 主张：围绕「" + term + "」写一句可独立成立的判断。",
                "- 机制：解释其因果或结构约束。",
                "- 语境例子：补一条具体场景例子（不是定义）。",
                "- 边界：补一条不成立条件或常见误用。");
    }

    private String firstNonBlank(String... values) {
        if (values == null) {
            return "";
        }
        for (String value : values) {
            if (StringUtils.hasText(value)) {
                return value.trim();
            }
        }
        return "";
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
