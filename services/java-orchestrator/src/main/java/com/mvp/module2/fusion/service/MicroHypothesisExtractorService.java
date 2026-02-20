package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.scheduling.annotation.Async;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * 文章退出时的轻量微观假说提取器。
 *
 * 设计约束：
 * 1. 仅在 article_exit 事件后异步触发，不能阻塞 telemetry 主链路。
 * 2. 使用小模型进行高频小颗粒推理，失败时降级为启发式规则，保证可用性。
 * 3. 输出写入按用户分桶的临时认知缓存（Cognitive Cache Layer）。
 */
@Service
public class MicroHypothesisExtractorService {
    private static final Logger logger = LoggerFactory.getLogger(MicroHypothesisExtractorService.class);

    private static final Pattern UNSAFE_PATH_SEGMENT = Pattern.compile("[^A-Za-z0-9._-]");
    private static final int MAX_CONTENT_TYPE_CHARS = 48;
    private static final int MAX_HYPOTHESIS_CHARS = 72;
    private static final int MAX_CONTEXT_CHARS = 160;
    private static final double DEFAULT_CONFIDENCE = 0.65d;

    private static final String DEFAULT_SYSTEM_PROMPT = String.join("\n",
            "你是微观认知假说提取器。",
            "目标是把阅读交互动作映射成极短可执行判断。",
            "禁止长篇解释，禁止输出多余字段。",
            "输出必须是 JSON 数组。"
    );

    private static final String DEFAULT_USER_PROMPT = String.join("\n",
            "你将收到一篇文章的动作列表（含热点/冷点）和对应文本片段。",
            "请仅输出 JSON 数组，每个元素字段固定为：",
            "- action: DELETED | RESONANCE | NOTE_SAVED | LEXICAL_CARD_OPENED | COLD_SIGNAL",
            "- content_type: 12~28 字摘要短语",
            "- inferred_hypothesis: 18~40 字，必须简短可执行",
            "",
            "约束：",
            "1) 最多输出 8 条，按价值从高到低排序。",
            "2) 不要解释过程，不要输出 markdown，不要代码块。",
            "3) inferred_hypothesis 必须围绕“用户偏好/耐心/关注维度”之一。",
            "",
            "输入动作样本：",
            "{events_json}"
    );

    @Value("${telemetry.micro-hypothesis.enabled:true}")
    private boolean enabled;

    @Value("${telemetry.micro-hypothesis.base-url:}")
    private String baseUrl;

    @Value("${telemetry.micro-hypothesis.model:claude-3-haiku-20240307}")
    private String model;

    @Value("${telemetry.micro-hypothesis.api-key:}")
    private String apiKey;

    @Value("${telemetry.micro-hypothesis.timeout-seconds:20}")
    private int timeoutSeconds;

    @Value("${telemetry.micro-hypothesis.max-events:80}")
    private int maxEvents;

    @Value("${telemetry.micro-hypothesis.cache-root:var/telemetry/cognitive-cache}")
    private String cacheRoot;

    @Value("${telemetry.micro-hypothesis.cache-ttl-hours:168}")
    private long cacheTtlHours;

    @Value("${telemetry.micro-hypothesis.prompt.system-resource:classpath:prompts/telemetry/micro-hypothesis/system-zh.txt}")
    private Resource systemPromptResource;

    @Value("${telemetry.micro-hypothesis.prompt.user-resource:classpath:prompts/telemetry/micro-hypothesis/user-zh.txt}")
    private Resource userPromptResource;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(6))
            .build();
    private final Object writeLock = new Object();
    private final Map<String, String> promptTemplateCache = new ConcurrentHashMap<>();

    @Autowired(required = false)
    private MacroPersonaForgeService macroPersonaForgeService;

    @Autowired(required = false)
    private TelemetryLlmInteractionLogService telemetryLlmInteractionLogService;

    @Async("taskExecutor")
    public void extractArticleHypothesesAsync(ExtractionRequest request) {
        if (!enabled || request == null) {
            return;
        }
        try {
            List<ActionObservation> observations = buildObservations(request.coldRecords, request.hotRecords);
            if (observations.isEmpty()) {
                return;
            }
            List<HypothesisSlice> hypotheses = inferBySmallModel(request, observations);
            String source = "llm";
            if (hypotheses.isEmpty()) {
                hypotheses = fallbackHeuristic(observations);
                source = "heuristic";
            }
            if (hypotheses.isEmpty()) {
                return;
            }
            persistCognitiveCache(request, hypotheses, source, observations.size());
            if (macroPersonaForgeService != null) {
                macroPersonaForgeService.maybeForge(request.userKey);
            }
        } catch (Exception ex) {
            logger.warn("micro hypothesis extraction failed: taskId={} userKey={} err={}",
                    request.taskId, request.userKey, ex.getMessage());
        }
    }

    private List<ActionObservation> buildObservations(
            List<Map<String, Object>> coldRecords,
            List<Map<String, Object>> hotRecords
    ) {
        List<ActionObservation> merged = new ArrayList<>();
        if (hotRecords != null) {
            for (Map<String, Object> record : hotRecords) {
                ActionObservation item = toObservation(record, true);
                if (item != null) {
                    merged.add(item);
                }
            }
        }
        if (coldRecords != null) {
            for (Map<String, Object> record : coldRecords) {
                ActionObservation item = toObservation(record, false);
                if (item != null) {
                    merged.add(item);
                }
            }
        }
        merged.sort(Comparator.comparingLong((ActionObservation it) -> it.timestampMs).reversed());
        if (merged.size() > Math.max(8, maxEvents)) {
            return merged.subList(0, Math.max(8, maxEvents));
        }
        return merged;
    }

    private ActionObservation toObservation(Map<String, Object> record, boolean hot) {
        if (record == null) {
            return null;
        }
        String eventType = trimToNull(String.valueOf(record.getOrDefault("eventType", "")));
        if (eventType == null) {
            return null;
        }
        String action = mapAction(eventType, hot);
        if (action == null) {
            return null;
        }

        long timestampMs = readLong(record.get("timestampMs"), System.currentTimeMillis());
        String nodeId = trimToEmpty(String.valueOf(record.getOrDefault("nodeId", "")));
        String nodeText = trimToEmpty(String.valueOf(record.getOrDefault("nodeText", "")));
        String snippet = trimToEmpty(String.valueOf(record.getOrDefault("nodeMarkdownSnippet", "")));

        @SuppressWarnings("unchecked")
        Map<String, Object> payloadRaw = record.get("payload") instanceof Map<?, ?> map
                ? (Map<String, Object>) map
                : Map.of();
        String payloadHint = extractPayloadHint(payloadRaw);

        String context = firstNonBlank(nodeText, snippet, payloadHint, "");
        if (context.length() > MAX_CONTEXT_CHARS) {
            context = context.substring(0, MAX_CONTEXT_CHARS).trim();
        }
        String contentType = inferContentType(context, eventType);

        return new ActionObservation(action, eventType, nodeId, timestampMs, contentType, context);
    }

    private List<HypothesisSlice> inferBySmallModel(ExtractionRequest request, List<ActionObservation> observations) {
        String endpoint = normalizeEndpoint(baseUrl);
        Map<String, Object> interaction = new LinkedHashMap<>();
        Instant startedAt = Instant.now();
        interaction.put("status", "INIT");
        interaction.put("model", trimToEmpty(model));
        interaction.put("eventCount", observations == null ? 0 : observations.size());
        interaction.put("endpoint", endpoint);
        if (!StringUtils.hasText(endpoint) || !StringUtils.hasText(model)) {
            interaction.put("status", "SKIPPED_CONFIG");
            interaction.put("error", "missing endpoint or model");
            persistLlmInteractionAsync(request, "micro_hypothesis", interaction, startedAt);
            return List.of();
        }
        try {
            List<Map<String, Object>> compactInputs = new ArrayList<>();
            for (ActionObservation item : observations) {
                Map<String, Object> line = new LinkedHashMap<>();
                line.put("action", item.action);
                line.put("event_type", item.eventType);
                line.put("content_type", item.contentType);
                line.put("context", item.context);
                compactInputs.add(line);
            }
            interaction.put("inputSample", compactInputs);

            String systemPrompt = buildSystemPrompt();
            String userPrompt = buildUserPrompt(compactInputs);

            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("model", model);
            payload.put("temperature", 0.2);
            payload.put("max_tokens", 480);
            payload.put("stream", false);
            payload.put("messages", List.of(
                    Map.of("role", "system", "content", systemPrompt),
                    Map.of("role", "user", "content", userPrompt)
            ));
            interaction.put("requestBody", payload);

            HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(endpoint + "/chat/completions"))
                    .timeout(Duration.ofSeconds(Math.max(6, timeoutSeconds)))
                    .header("Content-Type", "application/json")
                    .header("Accept", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(payload)));
            if (StringUtils.hasText(apiKey)) {
                builder.header("Authorization", "Bearer " + apiKey.trim());
            }

            HttpResponse<String> response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            interaction.put("httpStatus", response.statusCode());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                logger.warn("micro hypothesis llm call failed: status={} body={}",
                        response.statusCode(), summarizeBody(response.body()));
                interaction.put("status", "HTTP_ERROR");
                interaction.put("responseBodyPreview", summarizeBody(response.body()));
                return List.of();
            }
            JsonNode root = objectMapper.readTree(response.body());
            String content = root.path("choices").path(0).path("message").path("content").asText("");
            interaction.put("responseBodyPreview", summarizeBody(content));
            List<HypothesisSlice> parsed = parseHypotheses(content);
            interaction.put("status", parsed.isEmpty() ? "PARSE_EMPTY" : "OK");
            interaction.put("parsedCount", parsed.size());
            interaction.put("parsedItems", parsed);
            return parsed;
        } catch (Exception ex) {
            logger.warn("micro hypothesis llm call exception: {}", ex.getMessage());
            interaction.put("status", "EXCEPTION");
            interaction.put("error", ex.getMessage());
            return List.of();
        } finally {
            persistLlmInteractionAsync(request, "micro_hypothesis", interaction, startedAt);
        }
    }

    private String buildUserPrompt(List<Map<String, Object>> compactInputs) throws Exception {
        String json = objectMapper.writeValueAsString(compactInputs);
        return buildUserPromptTemplate().replace("{events_json}", json);
    }

    private String buildSystemPrompt() {
        return loadPromptTemplate("micro_system", systemPromptResource, DEFAULT_SYSTEM_PROMPT);
    }

    private String buildUserPromptTemplate() {
        return loadPromptTemplate("micro_user", userPromptResource, DEFAULT_USER_PROMPT);
    }

    private String loadPromptTemplate(String cacheKey, Resource resource, String fallback) {
        return promptTemplateCache.computeIfAbsent(cacheKey, key -> readPromptTemplate(resource, fallback, cacheKey));
    }

    private String readPromptTemplate(Resource resource, String fallback, String templateName) {
        if (resource == null || !resource.exists()) {
            logger.warn("micro hypothesis prompt missing ({}), fallback to default", templateName);
            return fallback;
        }
        try (InputStream input = resource.getInputStream()) {
            String template = StreamUtils.copyToString(input, StandardCharsets.UTF_8).trim();
            if (StringUtils.hasText(template)) {
                return template;
            }
            logger.warn("micro hypothesis prompt empty ({}), fallback to default", templateName);
        } catch (IOException ex) {
            logger.warn("micro hypothesis prompt load failed ({}): {}", templateName, ex.getMessage());
        }
        return fallback;
    }

    private List<HypothesisSlice> parseHypotheses(String llmText) {
        String text = trimToNull(llmText);
        if (text == null) {
            return List.of();
        }
        List<Map<String, Object>> parsed = parseHypothesisRows(text);
        if (parsed.isEmpty()) {
            return List.of();
        }
        try {
            List<HypothesisSlice> output = new ArrayList<>();
            for (Map<String, Object> row : parsed) {
                String action = normalizeActionFlexible(
                        readFieldByAlias(row, "action", "event_action", "interaction", "行为", "动作"),
                        readFieldByAlias(row, "event_type", "eventType", "事件类型")
                );
                String contentType = firstNonBlank(
                        readFieldByAlias(
                                row,
                                "content_type",
                                "contentType",
                                "content_type_label",
                                "content_focus",
                                "contentFocus",
                                "topic",
                                "主题",
                                "内容类型"
                        ),
                        readFieldByAlias(row, "context", "nodeText", "node_text", "text", "文本")
                );
                String hypothesis = firstNonBlank(
                        readFieldByAlias(row, "inferred_hypothesis", "hypothesis", "inference", "insight", "推断", "假说", "结论"),
                        readFieldByAlias(row, "reason", "analysis", "分析")
                );
                double confidence = normalizeConfidence(
                        firstNonBlank(
                                readFieldByAlias(row, "confidence", "confidence_score", "score", "置信度"),
                                ""
                        ),
                        DEFAULT_CONFIDENCE
                );
                if (action.isEmpty() || contentType.isEmpty() || hypothesis.isEmpty()) {
                    continue;
                }
                output.add(new HypothesisSlice(
                        action,
                        trimToMax(contentType, MAX_CONTENT_TYPE_CHARS),
                        trimToMax(hypothesis, MAX_HYPOTHESIS_CHARS),
                        confidence
                ));
                if (output.size() >= 8) {
                    break;
                }
            }
            return output;
        } catch (Exception ex) {
            logger.warn("micro hypothesis parse failed: {}", ex.getMessage());
            return List.of();
        }
    }

    private List<Map<String, Object>> parseHypothesisRows(String llmText) {
        List<Map<String, Object>> rows = extractRowsFromJsonText(llmText);
        if (!rows.isEmpty()) {
            return rows;
        }
        String jsonArray = extractJsonArray(llmText);
        if (jsonArray == null) {
            return List.of();
        }
        return extractRowsFromJsonText(jsonArray);
    }

    private List<Map<String, Object>> extractRowsFromJsonText(String text) {
        try {
            JsonNode root = objectMapper.readTree(text);
            return extractRowsFromNode(root);
        } catch (Exception ex) {
            return List.of();
        }
    }

    private List<Map<String, Object>> extractRowsFromNode(JsonNode root) {
        if (root == null || root.isMissingNode() || root.isNull()) {
            return List.of();
        }
        if (root.isArray()) {
            return toRowMaps(root);
        }
        if (!root.isObject()) {
            return List.of();
        }
        String[] containerKeys = {"items", "results", "result", "data", "output", "hypotheses", "slices", "records"};
        for (String key : containerKeys) {
            JsonNode node = root.get(key);
            if (node != null && node.isArray()) {
                List<Map<String, Object>> rows = toRowMaps(node);
                if (!rows.isEmpty()) {
                    return rows;
                }
            }
        }
        Deque<JsonNode> queue = new ArrayDeque<>();
        queue.add(root);
        int scanBudget = 256;
        while (!queue.isEmpty() && scanBudget-- > 0) {
            JsonNode node = queue.poll();
            if (node == null || node.isNull() || node.isMissingNode()) {
                continue;
            }
            if (node.isArray()) {
                List<Map<String, Object>> rows = toRowMaps(node);
                if (!rows.isEmpty()) {
                    return rows;
                }
                continue;
            }
            if (node.isObject()) {
                node.fields().forEachRemaining(entry -> queue.add(entry.getValue()));
            }
        }
        return List.of();
    }

    private List<Map<String, Object>> toRowMaps(JsonNode arrayNode) {
        List<Map<String, Object>> rows = new ArrayList<>();
        if (arrayNode == null || !arrayNode.isArray()) {
            return rows;
        }
        for (JsonNode item : arrayNode) {
            if (!item.isObject()) {
                continue;
            }
            Map<String, Object> row = objectMapper.convertValue(item, new TypeReference<Map<String, Object>>() {});
            if (!row.isEmpty()) {
                rows.add(row);
            }
        }
        return rows;
    }

    private String extractJsonArray(String text) {
        int start = text.indexOf('[');
        if (start < 0) {
            return null;
        }
        int depth = 0;
        for (int i = start; i < text.length(); i++) {
            char ch = text.charAt(i);
            if (ch == '[') {
                depth += 1;
            } else if (ch == ']') {
                depth -= 1;
                if (depth == 0) {
                    return text.substring(start, i + 1);
                }
            }
        }
        return null;
    }

    private List<HypothesisSlice> fallbackHeuristic(List<ActionObservation> observations) {
        Map<String, Integer> counts = new LinkedHashMap<>();
        Map<String, ActionObservation> firstHit = new LinkedHashMap<>();
        for (ActionObservation item : observations) {
            counts.put(item.action, counts.getOrDefault(item.action, 0) + 1);
            firstHit.putIfAbsent(item.action, item);
        }
        List<Map.Entry<String, Integer>> ranked = new ArrayList<>(counts.entrySet());
        ranked.sort((a, b) -> Integer.compare(b.getValue(), a.getValue()));

        List<HypothesisSlice> output = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : ranked) {
            String action = entry.getKey();
            ActionObservation seed = firstHit.get(action);
            if (seed == null) {
                continue;
            }
            String hypothesis = switch (action) {
                case "DELETED" -> "用户对这类内容耐心偏低，建议减少纯细节密度。";
                case "RESONANCE" -> "用户对这类主题共鸣强，建议提高同主题占比。";
                case "NOTE_SAVED" -> "用户愿意深度思考，适合追加结构化提问。";
                case "LEXICAL_CARD_OPENED" -> "用户在术语层面有探索意愿，可增强概念解释。";
                default -> "该类行为主要反映操作层反馈，需继续观测。";
            };
            output.add(new HypothesisSlice(
                    action,
                    trimToMax(seed.contentType, MAX_CONTENT_TYPE_CHARS),
                    trimToMax(hypothesis, MAX_HYPOTHESIS_CHARS),
                    heuristicConfidence(action)
            ));
            if (output.size() >= 6) {
                break;
            }
        }
        return output;
    }

    private void persistCognitiveCache(
            ExtractionRequest request,
            List<HypothesisSlice> hypotheses,
            String source,
            int sourceEventCount
    ) throws Exception {
        String userKey = normalizeUserKey(request.userKey);
        Path root = Paths.get(cacheRoot).toAbsolutePath().normalize();
        Path userDir = root.resolve(userKey).normalize();
        if (!userDir.startsWith(root)) {
            throw new IllegalStateException("invalid cognitive cache path");
        }
        Files.createDirectories(userDir);

        Instant now = Instant.now();
        Instant expiresAt = now.plus(Duration.ofHours(Math.max(1, cacheTtlHours)));
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("userKey", userKey);
        envelope.put("taskId", trimToEmpty(request.taskId));
        envelope.put("path", trimToEmpty(request.path));
        envelope.put("flushReason", trimToEmpty(request.flushReason));
        envelope.put("generatedAt", now.toString());
        envelope.put("expiresAt", expiresAt.toString());
        envelope.put("source", source);
        envelope.put("sourceEventCount", sourceEventCount);
        envelope.put("items", hypotheses);

        String line = objectMapper.writeValueAsString(envelope) + '\n';
        Path target = userDir.resolve("cognitive_cache.ndjson");
        synchronized (writeLock) {
            Files.writeString(
                    target,
                    line,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.APPEND
            );
        }
    }

    private String mapAction(String eventType, boolean hot) {
        String normalized = eventType.toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "paragraph_mark_deleted_by_swipe" -> "DELETED";
            case "paragraph_resonance_double_tap" -> "RESONANCE";
            case "note_saved" -> "NOTE_SAVED";
            case "lexical_card_opened" -> "LEXICAL_CARD_OPENED";
            default -> hot ? null : "COLD_SIGNAL";
        };
    }

    private String normalizeAction(String action) {
        String normalized = trimToEmpty(action).toUpperCase(Locale.ROOT);
        return switch (normalized) {
            case "DELETED", "RESONANCE", "NOTE_SAVED", "LEXICAL_CARD_OPENED", "COLD_SIGNAL" -> normalized;
            default -> "";
        };
    }

    private String inferContentType(String context, String eventType) {
        String text = trimToEmpty(context);
        if (!text.isEmpty()) {
            return trimToMax(text.replace('\n', ' '), MAX_CONTENT_TYPE_CHARS);
        }
        if (eventType.contains("lexical")) {
            return "术语与概念解析";
        }
        if (eventType.contains("note")) {
            return "批注与思考记录";
        }
        if (eventType.contains("deleted")) {
            return "低兴趣段落";
        }
        if (eventType.contains("resonance")) {
            return "高共鸣段落";
        }
        return "交互行为片段";
    }

    private String extractPayloadHint(Map<String, Object> payload) {
        if (payload == null || payload.isEmpty()) {
            return "";
        }
        String[] keys = {
                "originalMarkdown", "original_markdown", "nodeText",
                "paragraphText", "paragraph_text", "text", "content"
        };
        for (String key : keys) {
            String value = trimToNull(String.valueOf(payload.getOrDefault(key, "")));
            if (value != null) {
                return value;
            }
        }
        return "";
    }

    private String normalizeActionFlexible(String actionRaw, String eventTypeRaw) {
        String normalized = normalizeAction(actionRaw);
        if (!normalized.isEmpty()) {
            return normalized;
        }
        String action = trimToEmpty(actionRaw).toLowerCase(Locale.ROOT);
        if (action.contains("deleted") || action.contains("remove") || action.contains("swipe")) {
            return "DELETED";
        }
        if (action.contains("resonance") || action.contains("double_tap") || action.contains("doubletap")) {
            return "RESONANCE";
        }
        if (action.contains("note")) {
            return "NOTE_SAVED";
        }
        if (action.contains("lexical") || action.contains("card_opened")) {
            return "LEXICAL_CARD_OPENED";
        }
        if (action.contains("cold")) {
            return "COLD_SIGNAL";
        }
        if (action.contains("删除")) {
            return "DELETED";
        }
        if (action.contains("共鸣") || action.contains("双击")) {
            return "RESONANCE";
        }
        if (action.contains("笔记") || action.contains("批注")) {
            return "NOTE_SAVED";
        }
        if (action.contains("词汇") || action.contains("术语")) {
            return "LEXICAL_CARD_OPENED";
        }
        if (action.contains("冷")) {
            return "COLD_SIGNAL";
        }
        String eventType = trimToEmpty(eventTypeRaw);
        if (!eventType.isEmpty()) {
            String mapped = mapAction(eventType, true);
            if (mapped != null) {
                return mapped;
            }
            mapped = mapAction(eventType, false);
            return mapped != null ? mapped : "";
        }
        return "";
    }

    private String readFieldByAlias(Map<String, Object> row, String... aliases) {
        if (row == null || row.isEmpty() || aliases == null || aliases.length == 0) {
            return "";
        }
        for (String alias : aliases) {
            if (row.containsKey(alias)) {
                return trimToEmpty(String.valueOf(row.get(alias)));
            }
        }
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            String key = normalizeFieldKey(entry.getKey());
            for (String alias : aliases) {
                if (key.equals(normalizeFieldKey(alias))) {
                    return trimToEmpty(String.valueOf(entry.getValue()));
                }
            }
        }
        return "";
    }

    private String normalizeFieldKey(String key) {
        if (key == null) {
            return "";
        }
        return key.replaceAll("[\\s_\\-]", "").toLowerCase(Locale.ROOT);
    }

    private double normalizeConfidence(String raw, double fallback) {
        try {
            double value = Double.parseDouble(trimToEmpty(raw));
            if (Double.isNaN(value) || Double.isInfinite(value)) {
                return fallback;
            }
            return Math.max(0.1d, Math.min(1.0d, value));
        } catch (Exception ex) {
            return fallback;
        }
    }

    private double heuristicConfidence(String action) {
        return switch (action) {
            case "RESONANCE", "NOTE_SAVED", "LEXICAL_CARD_OPENED" -> 0.72d;
            case "DELETED" -> 0.69d;
            default -> 0.58d;
        };
    }

    private void persistLlmInteractionAsync(
            ExtractionRequest request,
            String pipeline,
            Map<String, Object> interaction,
            Instant startedAt
    ) {
        if (telemetryLlmInteractionLogService == null || interaction == null) {
            return;
        }
        interaction.put("durationMs", Duration.between(startedAt, Instant.now()).toMillis());
        telemetryLlmInteractionLogService.appendAsync(
                pipeline,
                normalizeUserKey(request == null ? null : request.userKey),
                request == null ? "" : trimToEmpty(request.taskId),
                interaction
        );
    }

    private String normalizeEndpoint(String raw) {
        String endpoint = trimToEmpty(raw);
        if (endpoint.isEmpty()) {
            return "";
        }
        if (endpoint.endsWith("/")) {
            endpoint = endpoint.substring(0, endpoint.length() - 1);
        }
        if (!endpoint.matches("(?i).*/v\\d+$")) {
            endpoint = endpoint + "/v1";
        }
        return endpoint;
    }

    private String normalizeUserKey(String rawUserKey) {
        String normalized = trimToNull(rawUserKey);
        if (normalized == null) {
            normalized = "anonymous";
        }
        normalized = UNSAFE_PATH_SEGMENT.matcher(normalized).replaceAll("_");
        normalized = normalized.replaceAll("_+", "_");
        if (normalized.isBlank()) {
            return "anonymous";
        }
        return normalized;
    }

    private long readLong(Object value, long fallback) {
        if (value == null) {
            return fallback;
        }
        try {
            return Long.parseLong(String.valueOf(value));
        } catch (Exception ex) {
            return fallback;
        }
    }

    private String summarizeBody(String body) {
        String text = trimToEmpty(body).replace('\n', ' ');
        if (text.length() <= 220) {
            return text;
        }
        return text.substring(0, 220) + "...";
    }

    private String trimToEmpty(String value) {
        return value == null ? "" : value.trim();
    }

    private String trimToNull(String value) {
        String trimmed = trimToEmpty(value);
        return trimmed.isEmpty() ? null : trimmed;
    }

    private String trimToMax(String text, int maxLength) {
        if (text == null) {
            return "";
        }
        String trimmed = text.trim();
        if (trimmed.length() <= maxLength) {
            return trimmed;
        }
        return trimmed.substring(0, maxLength).trim();
    }

    private String firstNonBlank(String... values) {
        if (values == null) {
            return "";
        }
        for (String value : values) {
            String normalized = trimToNull(value);
            if (normalized != null) {
                return normalized;
            }
        }
        return "";
    }

    public static class ExtractionRequest {
        public final String userKey;
        public final String taskId;
        public final String path;
        public final String flushReason;
        public final List<Map<String, Object>> coldRecords;
        public final List<Map<String, Object>> hotRecords;

        public ExtractionRequest(
                String userKey,
                String taskId,
                String path,
                String flushReason,
                List<Map<String, Object>> coldRecords,
                List<Map<String, Object>> hotRecords
        ) {
            this.userKey = userKey;
            this.taskId = taskId;
            this.path = path;
            this.flushReason = flushReason;
            this.coldRecords = coldRecords != null ? coldRecords : List.of();
            this.hotRecords = hotRecords != null ? hotRecords : List.of();
        }
    }

    public static class HypothesisSlice {
        public final String action;
        public final String content_type;
        public final String inferred_hypothesis;
        public final double confidence;

        public HypothesisSlice(String action, String contentType, String inferredHypothesis, double confidence) {
            this.action = action;
            this.content_type = contentType;
            this.inferred_hypothesis = inferredHypothesis;
            this.confidence = confidence;
        }
    }

    private static class ActionObservation {
        private final String action;
        private final String eventType;
        private final String nodeId;
        private final long timestampMs;
        private final String contentType;
        private final String context;

        private ActionObservation(
                String action,
                String eventType,
                String nodeId,
                long timestampMs,
                String contentType,
                String context
        ) {
            this.action = action;
            this.eventType = eventType;
            this.nodeId = nodeId;
            this.timestampMs = timestampMs;
            this.contentType = contentType;
            this.context = context;
        }
    }
}
