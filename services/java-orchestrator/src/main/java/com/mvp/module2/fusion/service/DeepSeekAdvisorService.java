package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.core.type.TypeReference;
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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
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
    private static final String DEFAULT_STRUCTURED_SYSTEM_PROMPT = String.join("\n",
            "你是一个阅读语境术语解释助手。",
            "你必须只输出 JSON 对象，禁止输出 markdown、解释文本、代码围栏。",
            "JSON schema 固定为：",
            "{",
            "  \"background\": [\"...\"],",
            "  \"contextual_explanations\": [\"...\"],",
            "  \"depth\": [\"...\"],",
            "  \"breadth\": [\"...\"]",
            "}",
            "四个数组都必须存在，每个数组 1~3 条短句。"
    );

    private static final String DEFAULT_STRUCTURED_USER_PROMPT = String.join("\n",
            "术语：{term}",
            "模式：{scenario}",
            "语境段落：{context_block}",
            "锚点句：{example_block}",
            "",
            "请输出 JSON：",
            "1. background: 背景知识与落地背景（数组）",
            "2. contextual_explanations: 语境化解释（数组）",
            "3. depth: 第一性原理与机制（数组）",
            "4. breadth: 跨场景/行业广度（数组）",
            "",
            "要求：",
            "- 仅输出一个 JSON 对象",
            "- 不要输出任何额外文本"
    );
    private static final String DEFAULT_STRUCTURED_BATCH_SYSTEM_PROMPT = String.join("\n",
            "你是一个阅读语境术语解释助手。",
            "你必须只输出 JSON 对象，禁止输出 markdown、解释文本、代码围栏。",
            "JSON schema 固定为：",
            "{",
            "  \"items\": [",
            "    {",
            "      \"term\": \"术语原文\",",
            "      \"background\": [\"...\"],",
            "      \"contextual_explanations\": [\"...\"],",
            "      \"depth\": [\"...\"],",
            "      \"breadth\": [\"...\"]",
            "    }",
            "  ]",
            "}",
            "items 覆盖全部术语，且每个术语的四个数组都必须存在。"
    );
    private static final String DEFAULT_STRUCTURED_BATCH_USER_PROMPT = String.join("\n",
            "术语列表（同一语境）：",
            "{terms_block}",
            "模式：{scenario}",
            "语境段落：{context_block}",
            "锚点句（共享）：{example_block}",
            "",
            "请输出一个 JSON 对象，格式固定为：",
            "{",
            "  \"items\": [",
            "    {",
            "      \"term\": \"术语原文\",",
            "      \"background\": [\"...\"],",
            "      \"contextual_explanations\": [\"...\"],",
            "      \"depth\": [\"...\"],",
            "      \"breadth\": [\"...\"]",
            "    }",
            "  ]",
            "}",
            "",
            "要求：",
            "- items 覆盖全部术语，顺序与输入保持一致",
            "- term 必须与输入术语原文一致",
            "- background/contextual_explanations/depth/breadth 四个数组都必须存在，且每个数组 1~3 条短句",
            "- 仅输出 JSON，不得输出其他文本"
    );

    @Value("${deepseek.advisor.enabled:true}")
    private boolean advisorEnabled;

    @Value("${deepseek.advisor.base-url:https://api.deepseek.com/v1}")
    private String advisorBaseUrl;

    @Value("${deepseek.advisor.model:deepseek-reasoner}")
    private String advisorModel;

    @Value("${deepseek.advisor.timeout-seconds:60}")
    private int timeoutSeconds;

    @Value("${deepseek.advisor.structured-max-tokens:8000}")
    private int structuredMaxTokens;

    @Value("${DEEPSEEK_API_KEY:}")
    private String apiKey;

    @Value("${deepseek.advisor.prompt.system-resource:classpath:prompts/deepseek-advisor/system-zh.txt}")
    private Resource systemPromptResource;

    @Value("${deepseek.advisor.prompt.user-resource:classpath:prompts/deepseek-advisor/user-zh.txt}")
    private Resource userPromptResource;

    @Value("${deepseek.advisor.prompt.structured-system-resource:classpath:prompts/deepseek-advisor/structured-system-zh.txt}")
    private Resource structuredSystemPromptResource;

    @Value("${deepseek.advisor.prompt.structured-user-resource:classpath:prompts/deepseek-advisor/structured-user-zh.txt}")
    private Resource structuredUserPromptResource;

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

    public StructuredAdviceResult requestStructuredAdvice(String term, String context, String contextExample, boolean contextDependent) {
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

        DeepSeekCallResult callResult;
        String structuredSystemPrompt = buildStructuredSystemPrompt();
        String structuredUserPrompt = buildStructuredUserPrompt(safeTerm, safeContext, safeContextExample, contextDependent);
        try {
            callResult = callStructuredWithRetry(
                    structuredSystemPrompt,
                    structuredUserPrompt,
                    Math.max(256, structuredMaxTokens)
            );
        } catch (Exception ex) {
            throw new IllegalStateException("DeepSeek structured advisor call failed: " + ex.getMessage(), ex);
        }
        String raw = String.valueOf(callResult.content == null ? "" : callResult.content).trim();
        if (isFinishReasonLength(callResult.finishReason)) {
            return StructuredAdviceResult.empty(
                    "deepseek-truncated",
                    raw,
                    callResult.requestPayloadJson,
                    callResult.responseBodyJson
            );
        }
        if (!StringUtils.hasText(raw)) {
            return StructuredAdviceResult.empty(
                    "deepseek-empty",
                    "",
                    callResult.requestPayloadJson,
                    callResult.responseBodyJson
            );
        }

        StructuredAdviceResult parsed = parseStructuredAdvice(raw);
        if (parsed.hasContent()) {
            return StructuredAdviceResult.deepseek(
                    parsed.background,
                    parsed.contextualExplanations,
                    parsed.depth,
                    parsed.breadth,
                    raw,
                    callResult.requestPayloadJson,
                    callResult.responseBodyJson
            );
        }
        return StructuredAdviceResult.empty(
                "deepseek-parse-empty",
                raw,
                callResult.requestPayloadJson,
                callResult.responseBodyJson
        );
    }

    public Map<String, StructuredAdviceResult> requestStructuredAdviceBatch(
            List<String> terms,
            String context,
            String contextExample,
            boolean contextDependent
    ) {
        List<String> safeTerms = normalizeTerms(terms);
        if (safeTerms.isEmpty()) {
            throw new IllegalArgumentException("terms cannot be empty");
        }
        if (safeTerms.size() == 1) {
            String term = safeTerms.get(0);
            StructuredAdviceResult single = requestStructuredAdvice(term, context, contextExample, contextDependent);
            return Map.of(term, single);
        }
        if (!advisorEnabled) {
            throw new IllegalStateException("deepseek.advisor.enabled=false");
        }
        if (!StringUtils.hasText(apiKey)) {
            throw new IllegalStateException("DEEPSEEK_API_KEY is empty");
        }
        String safeContext = String.valueOf(context == null ? "" : context).trim();
        String safeContextExample = String.valueOf(contextExample == null ? "" : contextExample).trim();

        DeepSeekCallResult callResult;
        String structuredSystemPrompt = buildStructuredBatchSystemPrompt();
        String structuredUserPrompt = buildStructuredBatchUserPrompt(
                safeTerms,
                safeContext,
                safeContextExample,
                contextDependent
        );
        try {
            callResult = callStructuredWithRetry(
                    structuredSystemPrompt,
                    structuredUserPrompt,
                    Math.max(256, structuredMaxTokens),
                    true
            );
        } catch (Exception ex) {
            throw new IllegalStateException("DeepSeek structured batch advisor call failed: " + ex.getMessage(), ex);
        }
        String raw = String.valueOf(callResult.content == null ? "" : callResult.content).trim();
        if (isFinishReasonLength(callResult.finishReason)) {
            return buildBatchFallbackResults(
                    safeTerms,
                    "deepseek-batch-truncated",
                    raw,
                    callResult.requestPayloadJson,
                    callResult.responseBodyJson
            );
        }
        if (!StringUtils.hasText(raw)) {
            return buildBatchFallbackResults(
                    safeTerms,
                    "deepseek-batch-empty",
                    "",
                    callResult.requestPayloadJson,
                    callResult.responseBodyJson
            );
        }
        Map<String, StructuredAdviceResult> parsed = parseStructuredAdviceBatch(
                raw,
                callResult.requestPayloadJson,
                callResult.responseBodyJson
        );
        if (parsed.isEmpty()) {
            return buildBatchFallbackResults(
                    safeTerms,
                    "deepseek-batch-parse-empty",
                    raw,
                    callResult.requestPayloadJson,
                    callResult.responseBodyJson
            );
        }
        LinkedHashMap<String, StructuredAdviceResult> resolved = new LinkedHashMap<>();
        for (String term : safeTerms) {
            String key = normalizeTermKey(term);
            StructuredAdviceResult result = parsed.get(key);
            if (result == null || !result.hasContent()) {
                result = StructuredAdviceResult.empty(
                        "deepseek-batch-miss",
                        raw,
                        callResult.requestPayloadJson,
                        callResult.responseBodyJson
                );
            }
            resolved.put(term, result);
        }
        return resolved;
    }

    private DeepSeekCallResult callStructuredWithRetry(
            String systemPrompt,
            String userPrompt,
            int initialMaxTokens
    ) throws Exception {
        return callStructuredWithRetry(systemPrompt, userPrompt, initialMaxTokens, false);
    }

    private DeepSeekCallResult callStructuredWithRetry(
            String systemPrompt,
            String userPrompt,
            int initialMaxTokens,
            boolean forceJsonObject
    ) throws Exception {
        int attempt = 0;
        int currentMaxTokens = Math.max(256, initialMaxTokens);
        DeepSeekCallResult lastResult = new DeepSeekCallResult("", "", "", "");
        while (attempt < 3) {
            attempt += 1;
            lastResult = callDeepSeekWithPromptsDetailed(systemPrompt, userPrompt, 0.2, currentMaxTokens, forceJsonObject);
            if (!isFinishReasonLength(lastResult.finishReason)) {
                return lastResult;
            }
            int next = Math.min(16000, Math.max(currentMaxTokens * 2, currentMaxTokens + 1024));
            if (next <= currentMaxTokens) {
                break;
            }
            currentMaxTokens = next;
        }
        return lastResult;
    }

    private boolean isFinishReasonLength(String finishReason) {
        return "length".equalsIgnoreCase(String.valueOf(finishReason == null ? "" : finishReason).trim());
    }

    private String callDeepSeek(String term, String context, String contextExample, boolean contextDependent) throws Exception {
        return callDeepSeekWithPrompts(
                buildSystemPrompt(),
                buildUserPrompt(term, context, contextExample, contextDependent),
                0.35,
                320
        );
    }

    private String callDeepSeekWithPrompts(
            String systemPrompt,
            String userPrompt,
            double temperature,
            int maxTokens
    ) throws Exception {
        return callDeepSeekWithPromptsDetailed(systemPrompt, userPrompt, temperature, maxTokens, false).content;
    }

    private DeepSeekCallResult callDeepSeekWithPromptsDetailed(
            String systemPrompt,
            String userPrompt,
            double temperature,
            int maxTokens,
            boolean forceJsonObject
    ) throws Exception {
        String endpoint = normalizeDeepSeekBaseUrl(advisorBaseUrl);
        String resolvedModel = DeepSeekModelRouter.resolveModel(advisorModel);
        if (!StringUtils.hasText(resolvedModel)) {
            throw new IllegalStateException("deepseek.advisor.model is empty");
        }
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("model", resolvedModel);
        payload.put("temperature", temperature);
        payload.put("max_tokens", maxTokens);
        payload.put("stream", false);
        payload.put("messages", List.of(
                Map.of("role", "system", "content", String.valueOf(systemPrompt == null ? "" : systemPrompt)),
                Map.of("role", "user", "content", String.valueOf(userPrompt == null ? "" : userPrompt))
        ));
        if (forceJsonObject) {
            payload.put("response_format", Map.of("type", "json_object"));
        }
        String payloadJson = objectMapper.writeValueAsString(payload);

        HttpRequest request = HttpRequest.newBuilder(URI.create(endpoint + "/chat/completions"))
                .timeout(Duration.ofSeconds(Math.max(60, timeoutSeconds)))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .header("Authorization", "Bearer " + apiKey.trim())
                .POST(HttpRequest.BodyPublishers.ofString(payloadJson))
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        String responseBody = String.valueOf(response.body() == null ? "" : response.body());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IllegalStateException("DeepSeek HTTP " + response.statusCode() + ": " + summarizeResponseBody(responseBody));
        }

        JsonNode root = objectMapper.readTree(responseBody);
        JsonNode choices = root.path("choices");
        if (!choices.isArray() || choices.isEmpty()) {
            return new DeepSeekCallResult("", payloadJson, responseBody, "");
        }
        String content = choices.get(0).path("message").path("content").asText("");
        String finishReason = choices.get(0).path("finish_reason").asText("");
        return new DeepSeekCallResult(content, payloadJson, responseBody, finishReason);
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

    private String buildStructuredSystemPrompt() {
        return loadPromptTemplate("structured_system", structuredSystemPromptResource, DEFAULT_STRUCTURED_SYSTEM_PROMPT);
    }

    private String buildStructuredBatchSystemPrompt() {
        return DEFAULT_STRUCTURED_BATCH_SYSTEM_PROMPT;
    }

    private String buildStructuredUserPrompt(String term, String context, String contextExample, boolean contextDependent) {
        String safeContext = trimContext(context);
        String safeExample = trimContext(contextExample);
        String scenario = contextDependent ? "段落绑定" : "全局语境";
        Map<String, String> values = new LinkedHashMap<>();
        values.put("term", term);
        values.put("scenario", scenario);
        values.put("context_block", safeContext.isEmpty() ? PROMPT_TEMPLATE_CONTEXT_EMPTY : safeContext);
        values.put("example_block", safeExample.isEmpty() ? PROMPT_TEMPLATE_CONTEXT_EMPTY : safeExample);
        return applyTemplate(
                loadPromptTemplate("structured_user", structuredUserPromptResource, DEFAULT_STRUCTURED_USER_PROMPT),
                values
        );
    }

    private String buildStructuredBatchUserPrompt(
            List<String> terms,
            String context,
            String contextExample,
            boolean contextDependent
    ) {
        String safeContext = trimContext(context);
        String safeExample = trimContext(contextExample);
        String scenario = contextDependent ? "段落绑定" : "全局语境";
        String termsBlock = renderTermsBlock(terms);
        Map<String, String> values = new LinkedHashMap<>();
        values.put("terms_block", StringUtils.hasText(termsBlock) ? termsBlock : PROMPT_TEMPLATE_CONTEXT_EMPTY);
        values.put("scenario", scenario);
        values.put("context_block", safeContext.isEmpty() ? PROMPT_TEMPLATE_CONTEXT_EMPTY : safeContext);
        values.put("example_block", safeExample.isEmpty() ? PROMPT_TEMPLATE_CONTEXT_EMPTY : safeExample);
        return applyTemplate(DEFAULT_STRUCTURED_BATCH_USER_PROMPT, values);
    }

    private String renderTermsBlock(List<String> terms) {
        if (terms == null || terms.isEmpty()) {
            return "";
        }
        StringBuilder builder = new StringBuilder();
        for (int index = 0; index < terms.size(); index += 1) {
            String term = String.valueOf(terms.get(index) == null ? "" : terms.get(index)).trim();
            if (!StringUtils.hasText(term)) {
                continue;
            }
            builder.append(index + 1).append(". ").append(term).append('\n');
        }
        return builder.toString().trim();
    }

    private StructuredAdviceResult parseStructuredAdvice(String rawText) {
        String text = String.valueOf(rawText == null ? "" : rawText).trim();
        if (!StringUtils.hasText(text)) {
            return StructuredAdviceResult.empty("structured-empty", text);
        }
        String json = extractJsonObject(text);
        if (!StringUtils.hasText(json)) {
            return StructuredAdviceResult.empty("structured-no-json", text);
        }
        try {
            Map<String, Object> root = objectMapper.readValue(
                    json,
                    new TypeReference<Map<String, Object>>() {}
            );
            List<String> background = normalizeStringList(root.get("background"));
            List<String> contextual = normalizeStringList(root.get("contextual_explanations"));
            if (contextual.isEmpty()) {
                contextual = normalizeStringList(root.get("contextualExplanations"));
            }
            List<String> depth = normalizeStringList(root.get("depth"));
            List<String> breadth = normalizeStringList(root.get("breadth"));
            if (breadth.isEmpty()) {
                breadth = normalizeStringList(root.get("width"));
            }
            return new StructuredAdviceResult(background, contextual, depth, breadth, "deepseek", text, "", "");
        } catch (Exception ex) {
            return StructuredAdviceResult.empty("structured-parse-error", text);
        }
    }

    private Map<String, StructuredAdviceResult> parseStructuredAdviceBatch(
            String rawText,
            String requestPayloadJson,
            String responseBodyJson
    ) {
        String text = String.valueOf(rawText == null ? "" : rawText).trim();
        if (!StringUtils.hasText(text)) {
            return Map.of();
        }
        String json = extractJsonObject(text);
        if (!StringUtils.hasText(json)) {
            return Map.of();
        }
        try {
            Map<String, Object> root = objectMapper.readValue(
                    json,
                    new TypeReference<Map<String, Object>>() {}
            );
            Object items = root.get("items");
            if (!(items instanceof List<?> list) || list.isEmpty()) {
                return Map.of();
            }
            LinkedHashMap<String, StructuredAdviceResult> output = new LinkedHashMap<>();
            for (Object item : list) {
                if (!(item instanceof Map<?, ?> rawMap) || rawMap.isEmpty()) {
                    continue;
                }
                String term = normalizeTerm(String.valueOf(readAlias(rawMap, "term", "tag", "keyword", "insight_tag")));
                if (!StringUtils.hasText(term)) {
                    continue;
                }
                List<String> background = normalizeStringList(readAlias(rawMap, "background", "bg"));
                List<String> contextual = normalizeStringList(readAlias(rawMap, "contextual_explanations", "contextualExplanations"));
                List<String> depth = normalizeStringList(readAlias(rawMap, "depth", "deep", "principles", "mechanism"));
                List<String> breadth = normalizeStringList(readAlias(rawMap, "breadth", "width", "cross_domain", "industry"));
                output.put(
                        normalizeTermKey(term),
                        StructuredAdviceResult.deepseek(
                                background,
                                contextual,
                                depth,
                                breadth,
                                text,
                                requestPayloadJson,
                                responseBodyJson
                        )
                );
            }
            return output;
        } catch (Exception ex) {
            return Map.of();
        }
    }

    private Object readAlias(Map<?, ?> source, String... aliases) {
        if (source == null || aliases == null || aliases.length == 0) {
            return null;
        }
        for (String alias : aliases) {
            if (!StringUtils.hasText(alias)) {
                continue;
            }
            if (source.containsKey(alias)) {
                return source.get(alias);
            }
        }
        return null;
    }

    private List<String> normalizeTerms(List<String> terms) {
        if (terms == null || terms.isEmpty()) {
            return List.of();
        }
        LinkedHashMap<String, String> ordered = new LinkedHashMap<>();
        for (String rawTerm : terms) {
            String term = normalizeTerm(rawTerm);
            if (!StringUtils.hasText(term)) {
                continue;
            }
            ordered.putIfAbsent(normalizeTermKey(term), term);
        }
        return new ArrayList<>(ordered.values());
    }

    private String normalizeTerm(String rawTerm) {
        return String.valueOf(rawTerm == null ? "" : rawTerm)
                .replace("\r\n", "\n")
                .replace('\r', '\n')
                .replace('\n', ' ')
                .replaceAll("\\s+", " ")
                .trim();
    }

    private String normalizeTermKey(String term) {
        return normalizeTerm(term).toLowerCase(Locale.ROOT);
    }

    private Map<String, StructuredAdviceResult> buildBatchFallbackResults(
            List<String> terms,
            String source,
            String raw,
            String requestPayloadJson,
            String responseBodyJson
    ) {
        if (terms == null || terms.isEmpty()) {
            return Map.of();
        }
        LinkedHashMap<String, StructuredAdviceResult> output = new LinkedHashMap<>();
        for (String term : terms) {
            String safeTerm = normalizeTerm(term);
            if (!StringUtils.hasText(safeTerm)) {
                continue;
            }
            output.put(
                    safeTerm,
                    StructuredAdviceResult.empty(source, raw, requestPayloadJson, responseBodyJson)
            );
        }
        return output;
    }

    private List<String> normalizeStringList(Object raw) {
        if (raw == null) {
            return List.of();
        }
        List<String> output = new ArrayList<>();
        if (raw instanceof List<?> list) {
            for (Object item : list) {
                String text = String.valueOf(item == null ? "" : item).trim();
                if (StringUtils.hasText(text)) {
                    output.add(trimContext(text));
                }
                if (output.size() >= 3) {
                    break;
                }
            }
            return output;
        }
        String text = String.valueOf(raw).trim();
        if (!StringUtils.hasText(text)) {
            return List.of();
        }
        if (text.startsWith("[") && text.endsWith("]")) {
            try {
                List<String> parsed = objectMapper.readValue(text, new TypeReference<List<String>>() {});
                return normalizeStringList(parsed);
            } catch (Exception ignored) {
                // noop
            }
        }
        output.add(trimContext(text));
        return output;
    }

    private String extractJsonObject(String text) {
        int start = text.indexOf('{');
        if (start < 0) {
            return null;
        }
        int depth = 0;
        for (int i = start; i < text.length(); i += 1) {
            char ch = text.charAt(i);
            if (ch == '{') {
                depth += 1;
            } else if (ch == '}') {
                depth -= 1;
                if (depth == 0) {
                    return text.substring(start, i + 1);
                }
            }
        }
        return null;
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
        return String.valueOf(context == null ? "" : context)
                .replace("\r\n", "\n")
                .replace('\r', '\n')
                .trim();
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

    public static class StructuredAdviceResult {
        public final List<String> background;
        public final List<String> contextualExplanations;
        public final List<String> depth;
        public final List<String> breadth;
        public final String source;
        public final String raw;
        public final String requestPayloadJson;
        public final String responseBodyJson;

        private StructuredAdviceResult(
                List<String> background,
                List<String> contextualExplanations,
                List<String> depth,
                List<String> breadth,
                String source,
                String raw,
                String requestPayloadJson,
                String responseBodyJson
        ) {
            this.background = background == null ? List.of() : List.copyOf(background);
            this.contextualExplanations = contextualExplanations == null ? List.of() : List.copyOf(contextualExplanations);
            this.depth = depth == null ? List.of() : List.copyOf(depth);
            this.breadth = breadth == null ? List.of() : List.copyOf(breadth);
            this.source = String.valueOf(source == null ? "" : source).trim();
            this.raw = String.valueOf(raw == null ? "" : raw).trim();
            this.requestPayloadJson = String.valueOf(requestPayloadJson == null ? "" : requestPayloadJson).trim();
            this.responseBodyJson = String.valueOf(responseBodyJson == null ? "" : responseBodyJson).trim();
        }

        public boolean hasContent() {
            return !background.isEmpty() || !contextualExplanations.isEmpty() || !depth.isEmpty() || !breadth.isEmpty();
        }

        public static StructuredAdviceResult deepseek(
                List<String> background,
                List<String> contextualExplanations,
                List<String> depth,
                List<String> breadth,
                String raw
        ) {
            return new StructuredAdviceResult(background, contextualExplanations, depth, breadth, "deepseek", raw, "", "");
        }

        public static StructuredAdviceResult deepseek(
                List<String> contextualExplanations,
                List<String> depth,
                List<String> breadth,
                String raw
        ) {
            return deepseek(List.of(), contextualExplanations, depth, breadth, raw);
        }

        public static StructuredAdviceResult deepseek(
                List<String> background,
                List<String> contextualExplanations,
                List<String> depth,
                List<String> breadth,
                String raw,
                String requestPayloadJson,
                String responseBodyJson
        ) {
            return new StructuredAdviceResult(
                    background,
                    contextualExplanations,
                    depth,
                    breadth,
                    "deepseek",
                    raw,
                    requestPayloadJson,
                    responseBodyJson
            );
        }

        public static StructuredAdviceResult deepseek(
                List<String> contextualExplanations,
                List<String> depth,
                List<String> breadth,
                String raw,
                String requestPayloadJson,
                String responseBodyJson
        ) {
            return deepseek(List.of(), contextualExplanations, depth, breadth, raw, requestPayloadJson, responseBodyJson);
        }

        public static StructuredAdviceResult empty(String source) {
            return new StructuredAdviceResult(List.of(), List.of(), List.of(), List.of(), source, "", "", "");
        }

        public static StructuredAdviceResult empty(String source, String raw) {
            return new StructuredAdviceResult(List.of(), List.of(), List.of(), List.of(), source, raw, "", "");
        }

        public static StructuredAdviceResult empty(
                String source,
                String raw,
                String requestPayloadJson,
                String responseBodyJson
        ) {
            return new StructuredAdviceResult(
                    List.of(),
                    List.of(),
                    List.of(),
                    List.of(),
                    source,
                    raw,
                    requestPayloadJson,
                    responseBodyJson
            );
        }
    }

    private static class DeepSeekCallResult {
        private final String content;
        private final String requestPayloadJson;
        private final String responseBodyJson;
        private final String finishReason;

        private DeepSeekCallResult(String content, String requestPayloadJson, String responseBodyJson, String finishReason) {
            this.content = String.valueOf(content == null ? "" : content).trim();
            this.requestPayloadJson = String.valueOf(requestPayloadJson == null ? "" : requestPayloadJson).trim();
            this.responseBodyJson = String.valueOf(responseBodyJson == null ? "" : responseBodyJson).trim();
            this.finishReason = String.valueOf(finishReason == null ? "" : finishReason).trim();
        }
    }
}
