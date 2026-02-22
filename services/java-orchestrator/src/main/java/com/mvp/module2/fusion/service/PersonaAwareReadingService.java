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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

@Service
public class PersonaAwareReadingService {
    private static final Logger logger = LoggerFactory.getLogger(PersonaAwareReadingService.class);

    private static final Pattern UNSAFE_PATH_SEGMENT = Pattern.compile("[^A-Za-z0-9._-]");
    private static final Pattern LIST_LINE_PATTERN = Pattern.compile("^\\s*(?:[-*+]\\s+|\\d+[.)]\\s+).+");
    private static final Pattern HEADING_PATTERN = Pattern.compile("^\\s{0,3}#{1,6}\\s+.+");
    private static final Pattern QUOTE_PATTERN = Pattern.compile("^\\s{0,3}>\\s*.+");
    private static final Pattern CODE_FENCE_PATTERN = Pattern.compile("^\\s*```.*");
    private static final Pattern INLINE_CODE_PATTERN = Pattern.compile("`([^`]{2,40})`");
    private static final Pattern ENGLISH_TERM_PATTERN = Pattern.compile("\\b[A-Za-z][A-Za-z0-9_+.-]{2,32}\\b");
    private static final Pattern IMAGE_ONLY_PATTERN = Pattern.compile("^\\s*!\\[[^\\]]*\\]\\([^\\)]+\\)\\s*$");
    private static final int MAX_NODE_TEXT_CHARS = 520;
    private static final int MAX_CHUNK_TEXT_CHARS = 1400;
    private static final int AUX_QUOTE_MAX_CHARS = 160;
    private static final String CHUNK_STRATEGY_SEMANTIC = "semantic_unit";
    private static final String CHUNK_STRATEGY_GROUP = "group";
    private static final String STORAGE_TASK_PREFIX = "storage:";
    private static final String TASK_META_FILE_NAME = "mobile_task_meta.json";

    private static final String DEFAULT_SYSTEM_PROMPT = String.join("\n",
            "你是个性化阅读编排器。",
            "输入包含用户画像 JSON 与文章段落节点数组。",
            "你必须为每个节点输出：node_id, relevance_score(0~1), bridge_text, insights_tags。",
            "必须只输出 JSON 数组，禁止 markdown 与解释。");

    private static final String DEFAULT_USER_PROMPT = String.join("\n",
            "用户画像：",
            "{persona_json}",
            "",
            "段落节点数组：",
            "{nodes_json}",
            "",
            "输出约束：",
            "1) 每个输入 node_id 都必须有对应输出。",
            "2) relevance_score 为 0.0~1.0 浮点。",
            "3) 仅当 relevance_score >= 0.9 或 <= 0.1 时输出 bridge_text，否则为 null。",
            "4) insights_tags 提供 0~6 个术语。",
            "5) 只输出 JSON 数组。");

    @Value("${telemetry.persona-reading.enabled:true}")
    private boolean enabled;

    @Value("${telemetry.persona-reading.base-url:https://api.deepseek.com/v1}")
    private String baseUrl;

    @Value("${telemetry.persona-reading.model:deepseek-v3}")
    private String model;

    @Value("${telemetry.persona-reading.api-key:${DEEPSEEK_API_KEY:}}")
    private String apiKey;

    @Value("${telemetry.persona-reading.timeout-seconds:45}")
    private int timeoutSeconds;

    @Value("${telemetry.persona-reading.max-nodes:220}")
    private int maxNodes;

    @Value("${telemetry.persona-reading.max-tokens:8000}")
    private int maxTokens;

    @Value("${telemetry.persona-reading.max-inflight:64}")
    private int maxInflight;

    @Value("${telemetry.persona-reading.chunk-strategy:semantic_unit}")
    private String chunkStrategy;

    @Value("${telemetry.persona-reading.cache-root:var/telemetry/persona-reading}")
    private String cacheRoot;

    @Value("${telemetry.persona-reading.mock-persona-file:var/tmp_mock_persona.json}")
    private String mockPersonaFile;

    @Value("${telemetry.micro-hypothesis.cache-root:var/telemetry/cognitive-cache}")
    private String personaRoot;

    @Value("${telemetry.persona-reading.prompt.system-resource:classpath:prompts/telemetry/persona-reading/system-zh.txt}")
    private Resource systemPromptResource;

    @Value("${telemetry.persona-reading.prompt.user-resource:classpath:prompts/telemetry/persona-reading/user-zh.txt}")
    private Resource userPromptResource;

    @Autowired(required = false)
    private TelemetryLlmInteractionLogService interactionLogService;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(8))
            .build();
    private final Map<String, String> promptTemplateCache = new ConcurrentHashMap<>();
    private final Object writeLock = new Object();
    private final Object llmPermitLock = new Object();
    private volatile Semaphore llmPermitSemaphore;

    @Async("taskExecutor")
    public void precomputeAsync(String taskId, String userId, String markdownPath) {
        if (!StringUtils.hasText(taskId) || !StringUtils.hasText(markdownPath)) {
            return;
        }
        try {
            Path resolved = resolveMarkdownPath(markdownPath);
            if (resolved == null || !Files.isRegularFile(resolved)) {
                return;
            }
            String markdown = Files.readString(resolved, StandardCharsets.UTF_8);
            loadOrCompute(taskId, userId, resolved, markdown);
        } catch (Exception ex) {
            logger.warn("persona reading precompute failed: taskId={} err={}", taskId, ex.getMessage());
        }
    }

    public PersonalizedReadingPayload loadOrCompute(
            String taskId,
            String userId,
            Path markdownPath,
            String markdown
    ) {
        String effectiveChunkStrategy = resolveChunkStrategy(userId);
        String normalizedTaskId = normalizeSegment(taskId, "unknown_task");
        String userKey = normalizeSegment(userId, "anonymous");
        String markdownText = String.valueOf(markdown == null ? "" : markdown);
        String fingerprint = buildMarkdownFingerprint(markdownPath, markdownText);
        Path cachePath = resolveCachePath(normalizedTaskId, userKey, markdownPath);
        Path legacyCachePath = null;
        try {
            legacyCachePath = resolveLegacyCachePath(normalizedTaskId, userKey, markdownPath);
        } catch (Exception ignored) {
            legacyCachePath = null;
        }
        PersonalizedReadingPayload cached = loadCached(cachePath, fingerprint, normalizedTaskId, userKey);
        if (cached == null && legacyCachePath != null && !legacyCachePath.equals(cachePath)) {
            cached = loadCached(legacyCachePath, fingerprint, normalizedTaskId, userKey);
            if (cached != null) {
                persistCache(cachePath, cached, markdownPath, fingerprint);
                cached.cachePath = cachePath.toString();
                cached.cacheScope = "task_scoped";
            }
        }
        if (cached != null) {
            return cached;
        }

        Map<String, Object> persona = loadPersona(userKey);
        List<ParagraphNode> nodes = parseMarkdownNodes(markdownText);
        List<Map<String, Object>> outputNodes;
        String source = "heuristic";

        if (nodes.isEmpty()) {
            outputNodes = List.of();
        } else {
            List<NodeAnnotation> annotations;
            if (enabled) {
                annotations = inferByLlm(normalizedTaskId, userKey, persona, nodes, effectiveChunkStrategy);
                if (!annotations.isEmpty()) {
                    source = "llm";
                } else {
                    annotations = inferByHeuristic(persona, nodes, effectiveChunkStrategy);
                }
            } else {
                annotations = inferByHeuristic(persona, nodes, effectiveChunkStrategy);
            }
            outputNodes = assembleOutputNodes(nodes, annotations, effectiveChunkStrategy);
        }

        PersonalizedReadingPayload payload = new PersonalizedReadingPayload();
        payload.taskId = normalizedTaskId;
        payload.userKey = userKey;
        payload.source = source;
        payload.generatedAt = Instant.now().toString();
        payload.cachePath = cachePath != null ? cachePath.toString() : "";
        payload.cacheScope = isTaskScopedCachePath(cachePath) ? "task_scoped" : "legacy";
        payload.chunkStrategy = effectiveChunkStrategy;
        payload.nodes = outputNodes;
        payload.persona = persona;

        persistCache(cachePath, payload, markdownPath, fingerprint);
        return payload;
    }

    private List<NodeAnnotation> inferByLlm(
            String taskId,
            String userKey,
            Map<String, Object> persona,
            List<ParagraphNode> nodes,
            String chunkStrategy
    ) {
        String endpoint = normalizeEndpoint(baseUrl);
        String resolvedModel = DeepSeekModelRouter.resolveModel(model);
        Map<String, Object> interaction = new LinkedHashMap<>();
        Instant startedAt = Instant.now();
        interaction.put("status", "INIT");
        interaction.put("endpoint", endpoint);
        interaction.put("model", String.valueOf(model == null ? "" : model).trim());
        interaction.put("resolvedModel", resolvedModel);
        interaction.put("taskId", taskId);
        interaction.put("nodeCount", nodes.size());
        if ("deepseek-reasoner".equalsIgnoreCase(resolvedModel)) {
            // 评分链路要求直接输出 JSON，reasoner 模型更容易把 token 消耗在 reasoning_content，
            // 导致 content 为空并触发 PARSE_EMPTY，因此这里强制路由到 chat 模型。
            resolvedModel = "deepseek-chat";
            interaction.put("reasonerRerouted", true);
            interaction.put("resolvedModel", resolvedModel);
        }

        if (!StringUtils.hasText(endpoint) || !StringUtils.hasText(resolvedModel)) {
            interaction.put("status", "SKIPPED_CONFIG");
            persistInteraction(userKey, taskId, interaction, startedAt);
            return List.of();
        }

        try {
            int cap = Math.max(20, maxNodes);
            List<ParagraphNode> limitedNodes = nodes.subList(0, Math.min(cap, nodes.size()));
            List<NodeChunk> chunks = buildNodeChunks(limitedNodes, chunkStrategy);
            interaction.put("chunkCount", chunks.size());
            interaction.put("chunkMode", "single_chunk_call");
            interaction.put("chunkStrategy", chunkStrategy);

            Map<String, ParagraphNode> byId = new LinkedHashMap<>();
            for (ParagraphNode node : limitedNodes) {
                byId.put(node.nodeId, node);
            }
            List<ChunkAnnotation> chunkAnnotations = new ArrayList<>();
            List<Map<String, Object>> chunkInteractions = new ArrayList<>();

            for (NodeChunk chunk : chunks) {
                Map<String, Object> chunkTrace = new LinkedHashMap<>();
                chunkTrace.put("chunk_id", chunk.chunkId);
                chunkTrace.put("node_ids", chunk.nodeIds);
                chunkTrace.put("node_count", chunk.nodeIds.size());
                chunkTrace.put("primary_node_id", chunk.primaryNodeId);
                try {
                    Map<String, Object> chunkRow = new LinkedHashMap<>();
                    chunkRow.put("chunk_id", chunk.chunkId);
                    chunkRow.put("node_ids", chunk.nodeIds);
                    chunkRow.put("node_count", chunk.nodeIds.size());
                    chunkRow.put("text_chunk", trimText(chunk.chunkText, MAX_CHUNK_TEXT_CHARS));
                    int tokenBudget = Math.max(320, maxTokens);
                    List<Map<String, Object>> attempts = new ArrayList<>();
                    List<NodeAnnotation> parsed = List.of();
                    String finalFinishReason = "";
                    String finalContent = "";
                    String finalResponseBody = "";
                    int finalHttpStatus = 0;
                    String finalStatus = "PARSE_EMPTY";

                    for (int attempt = 1; attempt <= 3; attempt += 1) {
                        Map<String, Object> payload = new LinkedHashMap<>();
                        payload.put("model", resolvedModel);
                        payload.put("temperature", 0.2);
                        payload.put("max_tokens", tokenBudget);
                        payload.put("stream", false);
                        payload.put("messages", List.of(
                                Map.of("role", "system", "content", buildSystemPrompt()),
                                Map.of("role", "user", "content", buildUserPrompt(persona, List.of(chunkRow)))
                        ));
                        if (attempt == 1) {
                            chunkTrace.put("requestBody", payload);
                        }

                        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(endpoint + "/chat/completions"))
                                .timeout(Duration.ofSeconds(Math.max(12, timeoutSeconds)))
                                .header("Content-Type", "application/json")
                                .header("Accept", "application/json")
                                .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(payload)));
                        if (StringUtils.hasText(apiKey)) {
                            builder.header("Authorization", "Bearer " + apiKey.trim());
                        }

                        HttpResponse<String> response;
                        acquireLlmPermit();
                        try {
                            response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
                        } finally {
                            releaseLlmPermit();
                        }
                        finalHttpStatus = response.statusCode();
                        finalResponseBody = String.valueOf(response.body() == null ? "" : response.body());
                        if (response.statusCode() < 200 || response.statusCode() >= 300) {
                            finalStatus = "HTTP_ERROR";
                            attempts.add(Map.of(
                                    "attempt", attempt,
                                    "max_tokens", tokenBudget,
                                    "http_status", response.statusCode(),
                                    "status", finalStatus
                            ));
                            break;
                        }

                        JsonNode root = objectMapper.readTree(finalResponseBody);
                        finalFinishReason = root.path("choices").path(0).path("finish_reason").asText("");
                        finalContent = root.path("choices").path(0).path("message").path("content").asText("");
                        parsed = parseChunkAnnotations(finalContent, List.of(chunk), limitedNodes);
                        boolean truncatedEmpty = "length".equalsIgnoreCase(finalFinishReason) && !StringUtils.hasText(finalContent);
                        attempts.add(Map.of(
                                "attempt", attempt,
                                "max_tokens", tokenBudget,
                                "http_status", response.statusCode(),
                                "finish_reason", String.valueOf(finalFinishReason == null ? "" : finalFinishReason),
                                "content_len", finalContent.length(),
                                "parsed_count", parsed.size()
                        ));

                        if (!parsed.isEmpty()) {
                            finalStatus = "OK";
                            break;
                        }
                        if (truncatedEmpty && attempt < 3) {
                            tokenBudget = Math.min(16000, Math.max(tokenBudget * 2, tokenBudget + 1024));
                            finalStatus = "PARSE_EMPTY_LENGTH";
                            continue;
                        }
                        if ("length".equalsIgnoreCase(finalFinishReason) && attempt < 3) {
                            tokenBudget = Math.min(16000, Math.max(tokenBudget * 2, tokenBudget + 1024));
                            finalStatus = "PARSE_EMPTY_LENGTH";
                            continue;
                        }
                        finalStatus = "PARSE_EMPTY";
                        break;
                    }

                    chunkTrace.put("attempts", attempts);
                    chunkTrace.put("httpStatus", finalHttpStatus);
                    chunkTrace.put("finishReason", finalFinishReason);
                    chunkTrace.put("responseBodyRaw", finalResponseBody);
                    chunkTrace.put("responseContentRaw", finalContent);
                    chunkTrace.put("responseBodyPreview", summarize(finalContent));

                    if (parsed.isEmpty()) {
                        chunkTrace.put("status", finalStatus);
                        chunkTrace.put("parsedCount", 0);
                        chunkInteractions.add(chunkTrace);
                        continue;
                    }
                    NodeAnnotation anchor = parsed.get(0);
                    String reason = normalizeReasonText(anchor.reason);
                    if (reason == null) {
                        ParagraphNode node = byId.get(chunk.primaryNodeId);
                        reason = buildDefaultReason(node, anchor.relevanceScore);
                    }
                    chunkAnnotations.add(new ChunkAnnotation(
                            chunk.chunkId,
                            anchor.relevanceScore,
                            reason,
                            anchor.bridgeText,
                            anchor.insightsTags
                    ));
                    chunkTrace.put("status", "OK");
                    chunkTrace.put("parsedCount", parsed.size());
                    chunkInteractions.add(chunkTrace);
                } catch (Exception chunkEx) {
                    chunkTrace.put("status", "EXCEPTION");
                    chunkTrace.put("error", chunkEx.getMessage());
                    chunkInteractions.add(chunkTrace);
                }
            }

            interaction.put("chunkCallCount", chunks.size());
            interaction.put("chunkSuccessCount", chunkAnnotations.size());
            interaction.put("chunkInteractions", chunkInteractions);

            if (chunkAnnotations.isEmpty()) {
                return List.of();
            }
            List<NodeAnnotation> annotations = expandChunkAnnotations(chunks, byId, chunkAnnotations);
            interaction.put("status", "OK");
            interaction.put("parsedCount", annotations.size());
            return annotations;
        } catch (Exception ex) {
            interaction.put("status", "EXCEPTION");
            interaction.put("error", ex.getMessage());
            logger.warn("persona reading llm failed: taskId={} err={}", taskId, ex.getMessage());
            return List.of();
        } finally {
            persistInteraction(userKey, taskId, interaction, startedAt);
        }
    }

    private List<NodeAnnotation> parseChunkAnnotations(
            String llmText,
            List<NodeChunk> chunks,
            List<ParagraphNode> nodes
    ) {
        String text = String.valueOf(llmText == null ? "" : llmText).trim();
        if (text.isEmpty()) {
            return List.of();
        }
        String jsonArray = extractJsonArray(text);
        if (jsonArray == null) {
            return List.of();
        }
        try {
            List<Map<String, Object>> rows = objectMapper.readValue(
                    jsonArray,
                    new TypeReference<List<Map<String, Object>>>() {}
            );
            Map<String, ParagraphNode> byId = new LinkedHashMap<>();
            for (ParagraphNode node : nodes) {
                byId.put(node.nodeId, node);
            }
            Map<String, NodeChunk> chunkById = new LinkedHashMap<>();
            Map<String, String> nodeToChunk = new LinkedHashMap<>();
            for (NodeChunk chunk : chunks) {
                chunkById.put(chunk.chunkId, chunk);
                for (String nodeId : chunk.nodeIds) {
                    nodeToChunk.put(nodeId, chunk.chunkId);
                }
            }
            List<ChunkAnnotation> chunkAnnotations = new ArrayList<>();
            for (Map<String, Object> row : rows) {
                String chunkId = firstNonBlank(readByAlias(row, "chunk_id", "chunkId", "id"), "");
                if (chunkId.isEmpty()) {
                    String nodeId = firstNonBlank(readByAlias(row, "node_id", "nodeId"), "");
                    chunkId = nodeToChunk.getOrDefault(nodeId, "");
                }
                if (chunkId.isEmpty()) {
                    String indexText = readByAlias(row, "index", "order", "idx");
                    int chunkIndex = readInt(indexText, -1);
                    if (chunkIndex >= 0 && chunkIndex < chunks.size()) {
                        chunkId = chunks.get(chunkIndex).chunkId;
                    }
                }
                if (!chunkById.containsKey(chunkId)) {
                    continue;
                }
                double relevance = normalizeScore(
                        firstNonBlank(readByAlias(row, "relevance_score", "relevance", "score"), "0.5"),
                        0.5d
                );
                String reason = normalizeReasonText(readByAlias(row, "reason", "why", "rationale", "analysis"));
                String bridge = normalizeBridgeText(readByAlias(row, "bridge_text", "bridgeText", "comment"));
                List<String> tags = normalizeTagList(firstNonBlank(
                        readByAlias(row, "insights_tags", "insight_tags", "insight_terms", "insights_terms"),
                        ""
                ));
                if (reason == null) {
                    NodeChunk chunk = chunkById.get(chunkId);
                    ParagraphNode anchor = chunk != null ? byId.get(chunk.primaryNodeId) : null;
                    reason = buildDefaultReason(anchor, relevance);
                }
                chunkAnnotations.add(new ChunkAnnotation(chunkId, relevance, reason, bridge, tags));
            }
            return expandChunkAnnotations(chunks, byId, chunkAnnotations);
        } catch (Exception ex) {
            logger.warn("persona reading parse failed: {}", ex.getMessage());
            return List.of();
        }
    }

    private List<NodeAnnotation> inferByHeuristic(
            Map<String, Object> persona,
            List<ParagraphNode> nodes,
            String chunkStrategy
    ) {
        List<String> skillset = readStringList(persona.get("surface_context"), "skillset");
        List<String> challenges = readStringList(persona.get("surface_context"), "current_challenges");
        Map<String, ParagraphNode> byId = new LinkedHashMap<>();
        for (ParagraphNode node : nodes) {
            byId.put(node.nodeId, node);
        }
        List<NodeChunk> chunks = buildNodeChunks(nodes, chunkStrategy);
        List<ChunkAnnotation> chunkAnnotations = new ArrayList<>();
        for (NodeChunk chunk : chunks) {
            String chunkText = String.valueOf(chunk.chunkText == null ? "" : chunk.chunkText);
            String lower = chunkText.toLowerCase(Locale.ROOT);
            ParagraphNode anchor = byId.get(chunk.primaryNodeId);
            double score = 0.45d;
            boolean hitCodeBlock = false;
            boolean hitListBlock = false;
            boolean hitSkill = false;
            boolean hitChallenge = false;
            boolean shortText = false;
            if (anchor != null && "code_block".equals(anchor.nodeType)) {
                score += 0.16d;
                hitCodeBlock = true;
            }
            if (anchor != null && "list_block".equals(anchor.nodeType)) {
                score += 0.05d;
                hitListBlock = true;
            }
            for (String skill : skillset) {
                String token = String.valueOf(skill == null ? "" : skill).trim().toLowerCase(Locale.ROOT);
                if (!token.isEmpty() && lower.contains(token)) {
                    score += 0.12d;
                    hitSkill = true;
                    break;
                }
            }
            for (String challenge : challenges) {
                String token = String.valueOf(challenge == null ? "" : challenge).trim().toLowerCase(Locale.ROOT);
                if (!token.isEmpty() && lower.contains(token)) {
                    score += 0.18d;
                    hitChallenge = true;
                    break;
                }
            }
            if (chunkText.length() < 28) {
                score -= 0.07d;
                shortText = true;
            }
            score = Math.max(0.05d, Math.min(0.95d, score));
            String reason = buildHeuristicReason(hitCodeBlock, hitListBlock, hitSkill, hitChallenge, shortText, score);
            String bridge = null;
            if (score >= 0.9d) {
                bridge = "High-priority paragraph for the current user context.";
            } else if (score <= 0.1d) {
                bridge = "Low-priority paragraph for the current user context.";
            }
            List<String> tags = extractInsightsTags(chunkText);
            chunkAnnotations.add(new ChunkAnnotation(chunk.chunkId, score, reason, bridge, tags));
        }
        return expandChunkAnnotations(chunks, byId, chunkAnnotations);
    }

    private List<NodeAnnotation> expandChunkAnnotations(
            List<NodeChunk> chunks,
            Map<String, ParagraphNode> byId,
            List<ChunkAnnotation> chunkAnnotations
    ) {
        Map<String, ChunkAnnotation> annotationByChunk = new LinkedHashMap<>();
        for (ChunkAnnotation annotation : chunkAnnotations) {
            if (!annotationByChunk.containsKey(annotation.chunkId)) {
                annotationByChunk.put(annotation.chunkId, annotation);
            }
        }
        List<NodeAnnotation> output = new ArrayList<>();
        for (NodeChunk chunk : chunks) {
            ChunkAnnotation annotation = annotationByChunk.get(chunk.chunkId);
            double score = annotation != null ? annotation.relevanceScore : 0.5d;
            String reason = annotation != null ? annotation.reason : null;
            String bridge = annotation != null ? annotation.bridgeText : null;
            List<String> tags = annotation != null ? annotation.insightsTags : List.of();
            ParagraphNode anchor = byId.get(chunk.primaryNodeId);
            if (reason == null) {
                reason = buildDefaultReason(anchor, score);
            }
            for (String nodeId : chunk.nodeIds) {
                output.add(new NodeAnnotation(nodeId, score, reason, bridge, tags));
            }
        }
        output.sort(Comparator.comparingInt(item -> byId.containsKey(item.nodeId) ? byId.get(item.nodeId).order : Integer.MAX_VALUE));
        return output;
    }
    private List<Map<String, Object>> assembleOutputNodes(
            List<ParagraphNode> nodes,
            List<NodeAnnotation> annotations,
            String chunkStrategy
    ) {
        String resolved = normalizeChunkStrategy(chunkStrategy);
        if (CHUNK_STRATEGY_SEMANTIC.equals(resolved)) {
            return assembleSemanticUnitOutputNodes(nodes, annotations);
        }

        Map<String, NodeAnnotation> byId = new LinkedHashMap<>();
        for (NodeAnnotation item : annotations) {
            byId.put(item.nodeId, item);
        }
        List<Map<String, Object>> output = new ArrayList<>();
        for (ParagraphNode node : nodes) {
            NodeAnnotation annotation = byId.get(node.nodeId);
            double score = annotation != null ? annotation.relevanceScore : 0.5d;
            String reason = annotation != null ? annotation.reason : buildDefaultReason(node, score);
            String bridge = annotation != null ? annotation.bridgeText : null;
            List<String> tags = annotation != null ? annotation.insightsTags : extractInsightsTags(node.rawMarkdown);
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("node_id", node.nodeId);
            item.put("order", node.order);
            item.put("node_type", node.nodeType);
            item.put("raw_markdown", node.rawMarkdown);
            item.put("relevance_score", score);
            item.put("reason", reason);
            item.put("bridge_text", bridge);
            item.put("insights_tags", tags);
            output.add(item);
        }
        return output;
    }

    private List<Map<String, Object>> assembleSemanticUnitOutputNodes(
            List<ParagraphNode> nodes,
            List<NodeAnnotation> annotations
    ) {
        Map<String, NodeAnnotation> byId = new LinkedHashMap<>();
        for (NodeAnnotation item : annotations) {
            byId.put(item.nodeId, item);
        }
        Map<String, ParagraphNode> nodeById = new LinkedHashMap<>();
        for (ParagraphNode node : nodes) {
            nodeById.put(node.nodeId, node);
        }

        List<NodeChunk> chunks = buildSemanticChunks(nodes);
        List<Map<String, Object>> output = new ArrayList<>();
        int order = 0;
        for (NodeChunk chunk : chunks) {
            NodeAnnotation annotation = resolveChunkAnnotation(chunk, byId);
            ParagraphNode anchor = nodeById.get(chunk.primaryNodeId);
            double score = annotation != null ? annotation.relevanceScore : 0.5d;
            String reason = annotation != null ? annotation.reason : buildDefaultReason(anchor, score);
            String bridge = annotation != null ? annotation.bridgeText : null;
            List<String> tags = annotation != null ? annotation.insightsTags : extractInsightsTags(chunk.chunkText);

            Map<String, Object> item = new LinkedHashMap<>();
            item.put("node_id", chunk.chunkId);
            item.put("order", order);
            item.put("node_type", "semantic_unit");
            item.put("raw_markdown", chunk.chunkText);
            item.put("relevance_score", score);
            item.put("reason", reason);
            item.put("bridge_text", bridge);
            item.put("insights_tags", tags);
            item.put("source_node_ids", new ArrayList<>(chunk.nodeIds));
            item.put("primary_node_id", chunk.primaryNodeId);
            output.add(item);
            order += 1;
        }
        return output;
    }

    private NodeAnnotation resolveChunkAnnotation(NodeChunk chunk, Map<String, NodeAnnotation> byId) {
        if (chunk == null || byId == null || byId.isEmpty()) {
            return null;
        }
        if (byId.containsKey(chunk.primaryNodeId)) {
            return byId.get(chunk.primaryNodeId);
        }
        for (String nodeId : chunk.nodeIds) {
            if (byId.containsKey(nodeId)) {
                return byId.get(nodeId);
            }
        }
        return null;
    }

    private List<NodeChunk> buildNodeChunks(List<ParagraphNode> nodes, String strategy) {
        String resolved = normalizeChunkStrategy(strategy);
        if (CHUNK_STRATEGY_GROUP.equals(resolved)) {
            return buildGroupChunks(nodes);
        }
        return buildSemanticChunks(nodes);
    }

    private List<NodeChunk> buildSemanticChunks(List<ParagraphNode> nodes) {
        List<NodeChunk> chunks = new ArrayList<>();
        List<ParagraphNode> buffer = new ArrayList<>();
        int chunkOrder = 0;
        for (ParagraphNode node : nodes) {
            if (node == null) {
                continue;
            }
            if (isEntityNode(node)) {
                List<ParagraphNode> members = new ArrayList<>(buffer);
                members.add(node);
                buffer.clear();
                chunks.add(createChunk(members, node, chunkOrder));
                chunkOrder += 1;
                continue;
            }
            if (isAuxiliaryNode(node)) {
                buffer.add(node);
                continue;
            }
            List<ParagraphNode> members = new ArrayList<>(buffer);
            members.add(node);
            buffer.clear();
            chunks.add(createChunk(members, node, chunkOrder));
            chunkOrder += 1;
        }

        if (!buffer.isEmpty()) {
            if (chunks.isEmpty()) {
                ParagraphNode primary = buffer.get(buffer.size() - 1);
                chunks.add(createChunk(buffer, primary, chunkOrder));
            } else {
                NodeChunk tail = chunks.get(chunks.size() - 1);
                List<String> mergedIds = new ArrayList<>(tail.nodeIds);
                StringBuilder mergedText = new StringBuilder(String.valueOf(tail.chunkText == null ? "" : tail.chunkText).trim());
                for (ParagraphNode node : buffer) {
                    mergedIds.add(node.nodeId);
                    String text = String.valueOf(node.rawMarkdown == null ? "" : node.rawMarkdown).trim();
                    if (!text.isEmpty()) {
                        if (mergedText.length() > 0) {
                            mergedText.append("\n\n");
                        }
                        mergedText.append(text);
                    }
                }
                chunks.set(
                        chunks.size() - 1,
                        new NodeChunk(tail.chunkId, mergedIds, tail.primaryNodeId, mergedText.toString())
                );
            }
        }
        return chunks;
    }

    private List<NodeChunk> buildGroupChunks(List<ParagraphNode> nodes) {
        List<NodeChunk> chunks = new ArrayList<>();
        List<ParagraphNode> current = new ArrayList<>();
        int chunkOrder = 0;
        for (ParagraphNode node : nodes) {
            if (isGroupBoundary(node) && !current.isEmpty()) {
                ParagraphNode primary = pickPrimaryNode(current);
                chunks.add(createChunk(current, primary, chunkOrder));
                chunkOrder += 1;
                current = new ArrayList<>();
            }
            current.add(node);
        }
        if (!current.isEmpty()) {
            ParagraphNode primary = pickPrimaryNode(current);
            chunks.add(createChunk(current, primary, chunkOrder));
        }
        return chunks;
    }

    private boolean isGroupBoundary(ParagraphNode node) {
        if (node == null || !"heading".equals(node.nodeType)) {
            return false;
        }
        return headingLevel(node.rawMarkdown) == 2;
    }

    private ParagraphNode pickPrimaryNode(List<ParagraphNode> members) {
        if (members == null || members.isEmpty()) {
            return null;
        }
        for (ParagraphNode node : members) {
            if (isEntityNode(node)) {
                return node;
            }
        }
        return members.get(members.size() - 1);
    }

    private int headingLevel(String rawMarkdown) {
        String text = String.valueOf(rawMarkdown == null ? "" : rawMarkdown).trim();
        if (text.isEmpty() || !text.startsWith("#")) {
            return 0;
        }
        int level = 0;
        while (level < text.length() && text.charAt(level) == '#') {
            level += 1;
        }
        if (level < 1 || level > 6) {
            return 0;
        }
        if (level < text.length() && Character.isWhitespace(text.charAt(level))) {
            return level;
        }
        return 0;
    }

    private NodeChunk createChunk(List<ParagraphNode> members, ParagraphNode primary, int chunkOrder) {
        List<String> ids = new ArrayList<>();
        StringBuilder text = new StringBuilder();
        for (ParagraphNode node : members) {
            if (node == null) {
                continue;
            }
            ids.add(node.nodeId);
            String raw = String.valueOf(node.rawMarkdown == null ? "" : node.rawMarkdown).trim();
            if (!raw.isEmpty()) {
                if (text.length() > 0) {
                    text.append("\n\n");
                }
                text.append(raw);
            }
        }
        String chunkId = "c-" + (chunkOrder + 1);
        String primaryId = primary != null ? primary.nodeId : (!ids.isEmpty() ? ids.get(ids.size() - 1) : "");
        return new NodeChunk(chunkId, ids, primaryId, text.toString());
    }

    private boolean isEntityNode(ParagraphNode node) {
        if (node == null) {
            return false;
        }
        return "paragraph".equals(node.nodeType)
                || "list_block".equals(node.nodeType)
                || "code_block".equals(node.nodeType);
    }

    private boolean isAuxiliaryNode(ParagraphNode node) {
        if (node == null) {
            return false;
        }
        String type = String.valueOf(node.nodeType == null ? "" : node.nodeType);
        if ("heading".equals(type)) {
            return true;
        }
        if ("quote".equals(type) && isShortGuideQuote(node.rawMarkdown)) {
            return true;
        }
        return isImageOnlyNode(node.rawMarkdown);
    }

    private boolean isShortGuideQuote(String rawMarkdown) {
        String text = String.valueOf(rawMarkdown == null ? "" : rawMarkdown).trim();
        if (text.isEmpty()) {
            return true;
        }
        String normalized = text.replaceAll("(?m)^\\s*>\\s?", "").trim();
        return normalized.length() <= AUX_QUOTE_MAX_CHARS;
    }

    private boolean isImageOnlyNode(String rawMarkdown) {
        String text = String.valueOf(rawMarkdown == null ? "" : rawMarkdown).trim();
        if (text.isEmpty()) {
            return true;
        }
        if (IMAGE_ONLY_PATTERN.matcher(text).matches()) {
            return true;
        }
        String lower = text.toLowerCase(Locale.ROOT);
        return lower.startsWith("<img") && lower.endsWith(">");
    }

    private List<ParagraphNode> parseMarkdownNodes(String markdown) {
        String[] lines = String.valueOf(markdown == null ? "" : markdown).replace("\r\n", "\n").split("\n", -1);
        List<ParagraphNode> nodes = new ArrayList<>();
        int i = 0;
        int order = 0;
        while (i < lines.length) {
            String line = lines[i];
            if (line.trim().isEmpty()) {
                i += 1;
                continue;
            }
            int start = i;
            int endExclusive;
            String type;

            if (CODE_FENCE_PATTERN.matcher(line).matches()) {
                type = "code_block";
                endExclusive = i + 1;
                while (endExclusive < lines.length) {
                    if (CODE_FENCE_PATTERN.matcher(lines[endExclusive]).matches()) {
                        endExclusive += 1;
                        break;
                    }
                    endExclusive += 1;
                }
            } else if (LIST_LINE_PATTERN.matcher(line).matches()) {
                type = "list_block";
                int baseIndent = indentWidth(line);
                endExclusive = i + 1;
                while (endExclusive < lines.length) {
                    String cursor = lines[endExclusive];
                    if (cursor.trim().isEmpty()) {
                        if (endExclusive + 1 < lines.length) {
                            String next = lines[endExclusive + 1];
                            if (LIST_LINE_PATTERN.matcher(next).matches() || indentWidth(next) > baseIndent) {
                                endExclusive += 1;
                                continue;
                            }
                        }
                        break;
                    }
                    if (LIST_LINE_PATTERN.matcher(cursor).matches() || indentWidth(cursor) > baseIndent) {
                        endExclusive += 1;
                        continue;
                    }
                    break;
                }
            } else if (HEADING_PATTERN.matcher(line).matches()) {
                type = "heading";
                endExclusive = i + 1;
            } else if (QUOTE_PATTERN.matcher(line).matches()) {
                type = "quote";
                endExclusive = i + 1;
                while (endExclusive < lines.length) {
                    String cursor = lines[endExclusive];
                    if (cursor.trim().isEmpty()) {
                        break;
                    }
                    if (QUOTE_PATTERN.matcher(cursor).matches()) {
                        endExclusive += 1;
                        continue;
                    }
                    break;
                }
            } else {
                type = "paragraph";
                endExclusive = i + 1;
                while (endExclusive < lines.length) {
                    String cursor = lines[endExclusive];
                    if (cursor.trim().isEmpty()) {
                        break;
                    }
                    if (CODE_FENCE_PATTERN.matcher(cursor).matches()
                            || HEADING_PATTERN.matcher(cursor).matches()
                            || LIST_LINE_PATTERN.matcher(cursor).matches()
                            || QUOTE_PATTERN.matcher(cursor).matches()) {
                        break;
                    }
                    endExclusive += 1;
                }
            }

            String raw = joinLines(lines, start, endExclusive).trim();
            if (!raw.isEmpty()) {
                ParagraphNode node = new ParagraphNode();
                node.nodeId = "p-" + (order + 1);
                node.order = order;
                node.nodeType = type;
                node.rawMarkdown = raw;
                nodes.add(node);
                order += 1;
            }
            i = Math.max(endExclusive, i + 1);
        }
        return nodes;
    }

    private String joinLines(String[] lines, int start, int endExclusive) {
        StringBuilder builder = new StringBuilder();
        for (int i = start; i < endExclusive && i < lines.length; i++) {
            if (i > start) {
                builder.append('\n');
            }
            builder.append(lines[i]);
        }
        return builder.toString();
    }

    private int indentWidth(String line) {
        int width = 0;
        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);
            if (ch == ' ') {
                width += 1;
            } else if (ch == '\t') {
                width += 4;
            } else {
                break;
            }
        }
        return width;
    }

    private Map<String, Object> loadPersona(String userKey) {
        Map<String, Object> mockPersona = loadMockPersona();
        if (!mockPersona.isEmpty()) {
            return mockPersona;
        }
        try {
            Path root = Paths.get(personaRoot).toAbsolutePath().normalize();
            Path target = root.resolve(normalizeSegment(userKey, "anonymous")).resolve("persona_10d.json").normalize();
            if (!target.startsWith(root) || !Files.isRegularFile(target)) {
                return defaultPersona();
            }
            Map<String, Object> parsed = objectMapper.readValue(
                    Files.readString(target, StandardCharsets.UTF_8),
                    new TypeReference<Map<String, Object>>() {}
            );
            if (parsed == null || parsed.isEmpty()) {
                return defaultPersona();
            }
            return parsed;
        } catch (Exception ex) {
            logger.warn("load persona failed: userKey={} err={}", userKey, ex.getMessage());
            return defaultPersona();
        }
    }

    private Map<String, Object> loadMockPersona() {
        Path candidate = resolveMockPersonaPath();
        if (candidate == null || !Files.isRegularFile(candidate)) {
            return Map.of();
        }
        try {
            Map<String, Object> parsed = objectMapper.readValue(
                    Files.readString(candidate, StandardCharsets.UTF_8),
                    new TypeReference<Map<String, Object>>() {}
            );
            if (parsed == null || parsed.isEmpty()) {
                return Map.of();
            }
            return parsed;
        } catch (Exception ex) {
            logger.warn("load mock persona failed: path={} err={}", candidate, ex.getMessage());
            return Map.of();
        }
    }

    private Path resolveMockPersonaPath() {
        String configured = String.valueOf(mockPersonaFile == null ? "" : mockPersonaFile).trim();
        if (configured.isEmpty()) {
            return null;
        }
        Path direct = Paths.get(configured).toAbsolutePath().normalize();
        if (Files.isRegularFile(direct)) {
            return direct;
        }
        Path moduleRelative = Paths.get("services", "java-orchestrator").resolve(configured).toAbsolutePath().normalize();
        if (Files.isRegularFile(moduleRelative)) {
            return moduleRelative;
        }
        return direct;
    }

    private Map<String, Object> defaultPersona() {
        Map<String, Object> surface = new LinkedHashMap<>();
        surface.put("profession", List.of());
        surface.put("skillset", List.of());
        surface.put("current_challenges", List.of());
        Map<String, Object> profile = new LinkedHashMap<>();
        profile.put("surface_context", surface);
        profile.put("deep_soul_matrix", Map.of());
        profile.put("evolution_verdict", "默认画像：尚无足够行为信号。");
        return profile;
    }

    private Path resolveCachePath(String taskId, String userKey, Path markdownPath) {
        Path taskScoped = resolveTaskScopedCachePath(taskId, userKey, markdownPath);
        if (taskScoped != null) {
            return taskScoped;
        }
        return resolveLegacyCachePath(taskId, userKey, markdownPath);
    }

    private Path resolveTaskScopedCachePath(String taskId, String userKey, Path markdownPath) {
        Path taskRoot = resolveTaskRootDirectory(taskId, markdownPath);
        if (taskRoot == null) {
            return null;
        }
        String mdKey = markdownPath != null
                ? normalizeSegment(String.valueOf(Math.abs(markdownPath.toString().hashCode())), "0")
                : "0";
        String fileName = normalizeSegment(userKey, "anonymous") + "_" + mdKey + ".json";
        Path cacheDir = taskRoot.resolve(".mobile_persona_cache").resolve("persona_reading").normalize();
        if (!cacheDir.startsWith(taskRoot)) {
            throw new IllegalStateException("invalid persona reading task cache path");
        }
        return cacheDir.resolve(fileName).normalize();
    }

    private boolean isTaskScopedCachePath(Path cachePath) {
        if (cachePath == null) {
            return false;
        }
        String normalized = cachePath.toString().replace('\\', '/');
        return normalized.contains("/.mobile_persona_cache/persona_reading/");
    }

    private Path resolveLegacyCachePath(String taskId, String userKey, Path markdownPath) {
        Path root = Paths.get(cacheRoot).toAbsolutePath().normalize();
        Path userDir = root.resolve(normalizeSegment(userKey, "anonymous")).normalize();
        if (!userDir.startsWith(root)) {
            throw new IllegalStateException("invalid persona reading cache path");
        }
        String mdKey = markdownPath != null ? normalizeSegment(String.valueOf(Math.abs(markdownPath.toString().hashCode())), "0") : "0";
        return userDir.resolve(normalizeSegment(taskId, "unknown_task") + "_" + mdKey + ".json").normalize();
    }

    private Path resolveTaskRootDirectory(String taskId, Path markdownPath) {
        if (markdownPath == null) {
            return null;
        }
        Path normalized = markdownPath.toAbsolutePath().normalize();
        Path current = Files.isDirectory(normalized) ? normalized : normalized.getParent();
        if (current == null) {
            return null;
        }

        Path byMeta = findAncestorWithTaskMeta(current);
        if (byMeta != null) {
            return byMeta;
        }

        String taskHint = extractTaskHint(taskId);
        Path byName = findAncestorByName(current, taskHint);
        if (byName != null) {
            return byName;
        }
        return current;
    }

    private Path findAncestorWithTaskMeta(Path start) {
        Path current = start;
        while (current != null) {
            try {
                Path metaPath = current.resolve(TASK_META_FILE_NAME).normalize();
                if (Files.isRegularFile(metaPath)) {
                    return current;
                }
            } catch (Exception ignored) {
                return null;
            }
            current = current.getParent();
        }
        return null;
    }

    private Path findAncestorByName(Path start, String taskHint) {
        if (!StringUtils.hasText(taskHint)) {
            return null;
        }
        String normalizedHint = normalizeSegment(taskHint, "");
        Path current = start;
        while (current != null) {
            Path name = current.getFileName();
            if (name != null) {
                String segment = String.valueOf(name).trim();
                if (segment.equalsIgnoreCase(taskHint)) {
                    return current;
                }
                if (StringUtils.hasText(normalizedHint) && normalizeSegment(segment, "").equalsIgnoreCase(normalizedHint)) {
                    return current;
                }
            }
            current = current.getParent();
        }
        return null;
    }

    private String extractTaskHint(String taskId) {
        String raw = String.valueOf(taskId == null ? "" : taskId).trim();
        if (!StringUtils.hasText(raw)) {
            return "";
        }
        if (raw.startsWith(STORAGE_TASK_PREFIX) && raw.length() > STORAGE_TASK_PREFIX.length()) {
            return raw.substring(STORAGE_TASK_PREFIX.length());
        }
        return raw;
    }

    public Map<String, Object> inspectCache(String taskId, String userId, Path markdownPath) {
        String normalizedTaskId = normalizeSegment(taskId, "unknown_task");
        String userKey = normalizeSegment(userId, "anonymous");
        Map<String, Object> output = new LinkedHashMap<>();
        output.put("taskId", normalizedTaskId);
        output.put("userKey", userKey);
        output.put("markdownPath", markdownPath != null ? markdownPath.toString() : "");

        Path taskRoot = resolveTaskRootDirectory(normalizedTaskId, markdownPath);
        output.put("taskRoot", taskRoot != null ? taskRoot.toString() : "");

        Path taskScopedPath = null;
        try {
            taskScopedPath = resolveTaskScopedCachePath(normalizedTaskId, userKey, markdownPath);
        } catch (Exception ignored) {
            taskScopedPath = null;
        }
        Path legacyPath = null;
        try {
            legacyPath = resolveLegacyCachePath(normalizedTaskId, userKey, markdownPath);
        } catch (Exception ignored) {
            legacyPath = null;
        }
        Path activePath = null;
        try {
            activePath = resolveCachePath(normalizedTaskId, userKey, markdownPath);
        } catch (Exception ignored) {
            activePath = taskScopedPath != null ? taskScopedPath : legacyPath;
        }

        output.put("cachePath", activePath != null ? activePath.toString() : "");
        output.put("cacheScope", isTaskScopedCachePath(activePath) ? "task_scoped" : "legacy");
        output.put("cacheExists", activePath != null && Files.isRegularFile(activePath));
        output.put("taskScopedPath", taskScopedPath != null ? taskScopedPath.toString() : "");
        output.put("taskScopedExists", taskScopedPath != null && Files.isRegularFile(taskScopedPath));
        output.put("legacyPath", legacyPath != null ? legacyPath.toString() : "");
        output.put("legacyExists", legacyPath != null && Files.isRegularFile(legacyPath));
        return output;
    }

    private PersonalizedReadingPayload loadCached(Path cachePath, String fingerprint, String taskId, String userKey) {
        try {
            if (cachePath == null || !Files.isRegularFile(cachePath)) {
                return null;
            }
            Map<String, Object> root = objectMapper.readValue(
                    Files.readString(cachePath, StandardCharsets.UTF_8),
                    new TypeReference<Map<String, Object>>() {}
            );
            String cachedFingerprint = String.valueOf(root.getOrDefault("markdownFingerprint", ""));
            if (!fingerprint.equals(cachedFingerprint)) {
                return null;
            }
            Object rawNodes = root.get("nodes");
            if (!(rawNodes instanceof List<?> list)) {
                return null;
            }
            List<Map<String, Object>> nodes = new ArrayList<>();
            for (Object item : list) {
                if (item instanceof Map<?, ?> map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> casted = (Map<String, Object>) map;
                    nodes.add(normalizeCachedNode(casted));
                }
            }
            PersonalizedReadingPayload payload = new PersonalizedReadingPayload();
            payload.taskId = taskId;
            payload.userKey = userKey;
            payload.source = "cache";
            payload.generatedAt = String.valueOf(root.getOrDefault("generatedAt", ""));
            payload.cachePath = cachePath.toString();
            payload.cacheScope = isTaskScopedCachePath(cachePath) ? "task_scoped" : "legacy";
            payload.chunkStrategy = normalizeChunkStrategy(String.valueOf(root.getOrDefault("chunkStrategy", chunkStrategy)));
            payload.nodes = nodes;
            payload.persona = root.get("persona") instanceof Map<?, ?> persona
                    ? objectMapper.convertValue(persona, new TypeReference<Map<String, Object>>() {})
                    : defaultPersona();
            return payload;
        } catch (Exception ex) {
            logger.warn("load persona reading cache failed: path={} err={}", cachePath, ex.getMessage());
            return null;
        }
    }

    private void persistCache(Path cachePath, PersonalizedReadingPayload payload, Path markdownPath, String fingerprint) {
        try {
            if (cachePath == null || payload == null) {
                return;
            }
            Files.createDirectories(cachePath.getParent());
            Map<String, Object> root = new LinkedHashMap<>();
            root.put("taskId", payload.taskId);
            root.put("userKey", payload.userKey);
            root.put("source", payload.source);
            root.put("generatedAt", payload.generatedAt);
            root.put("markdownPath", markdownPath != null ? markdownPath.toString() : "");
            root.put("markdownFingerprint", fingerprint);
            root.put("chunkStrategy", normalizeChunkStrategy(payload.chunkStrategy));
            root.put("persona", payload.persona != null ? payload.persona : Map.of());
            root.put("nodes", payload.nodes != null ? payload.nodes : List.of());

            synchronized (writeLock) {
                Files.writeString(
                        cachePath,
                        objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(root),
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.TRUNCATE_EXISTING
                );
            }
        } catch (Exception ex) {
            logger.warn("persist persona reading cache failed: path={} err={}", cachePath, ex.getMessage());
        }
    }

    private String buildMarkdownFingerprint(Path markdownPath, String markdownText) {
        long fileSize = markdownText == null ? 0L : markdownText.length();
        long modified = 0L;
        if (markdownPath != null) {
            try {
                modified = Files.getLastModifiedTime(markdownPath).toMillis();
            } catch (Exception ignored) {
                modified = 0L;
            }
        }
        String pathText = markdownPath != null ? markdownPath.toString() : "";
        return pathText + "|" + modified + "|" + fileSize;
    }

    private Path resolveMarkdownPath(String rawPath) {
        try {
            Path target = Paths.get(rawPath).toAbsolutePath().normalize();
            if (Files.isRegularFile(target) && isMarkdownFile(target)) {
                return target;
            }
            if (!Files.isDirectory(target)) {
                return null;
            }
            List<Path> markdownFiles = new ArrayList<>();
            try (Stream<Path> stream = Files.walk(target, 4)) {
                stream.filter(Files::isRegularFile)
                        .filter(this::isMarkdownFile)
                        .forEach(markdownFiles::add);
            }
            if (markdownFiles.isEmpty()) {
                return null;
            }
            markdownFiles.sort(
                    Comparator.comparingInt((Path path) -> markdownNamePriority(path.getFileName().toString()))
                            .thenComparing(Comparator.comparingLong(this::safeLastModifiedMillis).reversed())
            );
            return markdownFiles.get(0).toAbsolutePath().normalize();
        } catch (Exception ex) {
            return null;
        }
    }

    private int markdownNamePriority(String filename) {
        String normalized = String.valueOf(filename == null ? "" : filename).toLowerCase(Locale.ROOT);
        if ("enhanced_output.md".equals(normalized)) return 0;
        if ("enhanced_output2.md".equals(normalized)) return 1;
        if ("output.md".equals(normalized)) return 2;
        if (normalized.endsWith(".md") || normalized.endsWith(".markdown")) return 5;
        return 10;
    }

    private long safeLastModifiedMillis(Path path) {
        try {
            return Files.getLastModifiedTime(path).toMillis();
        } catch (IOException ex) {
            return 0L;
        }
    }

    private boolean isMarkdownFile(Path path) {
        if (path == null || path.getFileName() == null) {
            return false;
        }
        String name = path.getFileName().toString().toLowerCase(Locale.ROOT);
        return name.endsWith(".md") || name.endsWith(".markdown");
    }

    private String buildSystemPrompt() {
        return loadPromptTemplate("persona_reading_system", systemPromptResource, DEFAULT_SYSTEM_PROMPT);
    }

    private String buildUserPrompt(Map<String, Object> persona, List<Map<String, Object>> nodes) throws Exception {
        String personaJson = objectMapper.writeValueAsString(persona);
        String nodesJson = objectMapper.writeValueAsString(nodes);
        return loadPromptTemplate("persona_reading_user", userPromptResource, DEFAULT_USER_PROMPT)
                .replace("{persona_json}", personaJson)
                .replace("{nodes_json}", nodesJson);
    }

    private String loadPromptTemplate(String cacheKey, Resource resource, String fallback) {
        return promptTemplateCache.computeIfAbsent(cacheKey, key -> readPromptTemplate(resource, fallback, cacheKey));
    }

    private String readPromptTemplate(Resource resource, String fallback, String templateName) {
        if (resource == null || !resource.exists()) {
            logger.warn("persona reading prompt missing ({}), fallback to default", templateName);
            return fallback;
        }
        try (InputStream input = resource.getInputStream()) {
            String template = StreamUtils.copyToString(input, StandardCharsets.UTF_8).trim();
            if (StringUtils.hasText(template)) {
                return template;
            }
            logger.warn("persona reading prompt empty ({}), fallback to default", templateName);
        } catch (IOException ex) {
            logger.warn("persona reading prompt load failed ({}): {}", templateName, ex.getMessage());
        }
        return fallback;
    }

    private String normalizeEndpoint(String raw) {
        String endpoint = String.valueOf(raw == null ? "" : raw).trim();
        if (endpoint.endsWith("/")) {
            endpoint = endpoint.substring(0, endpoint.length() - 1);
        }
        if (!endpoint.isEmpty() && !endpoint.matches("(?i).*/v\\d+$")) {
            endpoint = endpoint + "/v1";
        }
        return endpoint;
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

    private String resolveChunkStrategy(String userId) {
        String raw = String.valueOf(userId == null ? "" : userId).toLowerCase(Locale.ROOT);
        if (raw.contains("@group") || raw.contains("#group") || raw.contains("mode_group")) {
            return CHUNK_STRATEGY_GROUP;
        }
        if (raw.contains("@semantic") || raw.contains("#semantic") || raw.contains("mode_semantic")) {
            return CHUNK_STRATEGY_SEMANTIC;
        }
        return normalizeChunkStrategy(chunkStrategy);
    }

    private String normalizeChunkStrategy(String raw) {
        String value = String.valueOf(raw == null ? "" : raw).trim().toLowerCase(Locale.ROOT);
        if (CHUNK_STRATEGY_GROUP.equals(value)) {
            return CHUNK_STRATEGY_GROUP;
        }
        return CHUNK_STRATEGY_SEMANTIC;
    }

    private String normalizeSegment(String raw, String fallback) {
        String value = String.valueOf(raw == null ? "" : raw).trim();
        if (value.isBlank()) {
            value = fallback;
        }
        value = UNSAFE_PATH_SEGMENT.matcher(value).replaceAll("_").replaceAll("_+", "_");
        if (value.isBlank()) {
            return fallback;
        }
        return value;
    }

    private String summarize(String body) {
        String text = String.valueOf(body == null ? "" : body).replace('\n', ' ').trim();
        return text.length() <= 260 ? text : text.substring(0, 260) + "...";
    }

    private String trimText(String text, int maxLength) {
        String value = String.valueOf(text == null ? "" : text).trim();
        if (value.length() <= maxLength) {
            return value;
        }
        return value.substring(0, maxLength).trim();
    }

    private String readByAlias(Map<String, Object> row, String... aliases) {
        if (row == null || row.isEmpty() || aliases == null) {
            return "";
        }
        for (String alias : aliases) {
            if (row.containsKey(alias)) {
                return String.valueOf(row.get(alias) == null ? "" : row.get(alias)).trim();
            }
        }
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            String key = normalizeField(entry.getKey());
            for (String alias : aliases) {
                if (key.equals(normalizeField(alias))) {
                    return String.valueOf(entry.getValue() == null ? "" : entry.getValue()).trim();
                }
            }
        }
        return "";
    }

    private String normalizeField(String raw) {
        if (raw == null) {
            return "";
        }
        return raw.replaceAll("[\\s_\\-]", "").toLowerCase(Locale.ROOT);
    }

    private String firstNonBlank(String first, String second) {
        String one = String.valueOf(first == null ? "" : first).trim();
        if (!one.isEmpty()) {
            return one;
        }
        return String.valueOf(second == null ? "" : second).trim();
    }

    private int readInt(String raw, int fallback) {
        try {
            return Integer.parseInt(String.valueOf(raw == null ? "" : raw).trim());
        } catch (Exception ex) {
            return fallback;
        }
    }

    private double normalizeScore(String raw, double fallback) {
        try {
            double score = Double.parseDouble(String.valueOf(raw == null ? "" : raw).trim());
            if (Double.isNaN(score) || Double.isInfinite(score)) {
                return fallback;
            }
            return Math.max(0.0d, Math.min(1.0d, score));
        } catch (Exception ex) {
            return fallback;
        }
    }

    private String normalizeBridgeText(String raw) {
        String value = String.valueOf(raw == null ? "" : raw).trim();
        if (value.isEmpty() || "null".equalsIgnoreCase(value)) {
            return null;
        }
        return trimText(value, 160);
    }

    private String normalizeReasonText(String raw) {
        String value = String.valueOf(raw == null ? "" : raw).trim();
        if (value.isEmpty() || "null".equalsIgnoreCase(value)) {
            return null;
        }
        return trimText(value, 220);
    }

    private List<String> normalizeTagList(String raw) {
        String value = String.valueOf(raw == null ? "" : raw).trim();
        if (value.isEmpty() || "null".equalsIgnoreCase(value)) {
            return List.of();
        }
        if (value.startsWith("[") && value.endsWith("]")) {
            try {
                List<String> parsed = objectMapper.readValue(value, new TypeReference<List<String>>() {});
                return compactTags(parsed);
            } catch (Exception ignored) {
                return compactTags(splitTags(value));
            }
        }
        return compactTags(splitTags(value));
    }

    private List<String> splitTags(String raw) {
        String[] segments = String.valueOf(raw == null ? "" : raw)
                .replace('[', ' ')
                .replace(']', ' ')
                .split("[,，;；|/\\n\\t]");
        List<String> output = new ArrayList<>();
        for (String segment : segments) {
            String token = segment.trim();
            if (!token.isEmpty()) {
                output.add(token);
            }
        }
        return output;
    }

    private List<String> compactTags(List<String> input) {
        Set<String> unique = new LinkedHashSet<>();
        for (String item : input) {
            String value = String.valueOf(item == null ? "" : item).trim();
            if (!value.isEmpty()) {
                unique.add(trimText(value, 32));
            }
            if (unique.size() >= 6) {
                break;
            }
        }
        return new ArrayList<>(unique);
    }

    private List<String> extractInsightsTags(String markdown) {
        String text = String.valueOf(markdown == null ? "" : markdown);
        Set<String> tags = new LinkedHashSet<>();
        Matcher codeMatcher = INLINE_CODE_PATTERN.matcher(text);
        while (codeMatcher.find() && tags.size() < 6) {
            String term = codeMatcher.group(1);
            if (term != null && !term.isBlank()) {
                tags.add(trimText(term.trim(), 32));
            }
        }
        Matcher enMatcher = ENGLISH_TERM_PATTERN.matcher(text);
        while (enMatcher.find() && tags.size() < 6) {
            String term = enMatcher.group();
            if (term == null || term.length() < 3) {
                continue;
            }
            boolean looksTechnical = term.chars().anyMatch(Character::isUpperCase) || term.contains("_") || term.contains(".");
            if (looksTechnical) {
                tags.add(trimText(term, 32));
            }
        }
        return new ArrayList<>(tags);
    }

    private List<String> readStringList(Object surfaceRaw, String key) {
        if (!(surfaceRaw instanceof Map<?, ?> surfaceMap)) {
            return List.of();
        }
        Object value = surfaceMap.get(key);
        if (!(value instanceof List<?> list)) {
            return List.of();
        }
        List<String> output = new ArrayList<>();
        for (Object item : list) {
            String text = String.valueOf(item == null ? "" : item).trim();
            if (!text.isEmpty()) {
                output.add(text);
            }
        }
        return output;
    }

    private void persistInteraction(String userKey, String taskId, Map<String, Object> interaction, Instant startedAt) {
        if (interactionLogService == null || interaction == null) {
            return;
        }
        interaction.put("durationMs", Duration.between(startedAt, Instant.now()).toMillis());
        interactionLogService.appendAsync("persona_reading_ranker", userKey, taskId, interaction);
    }

    private void acquireLlmPermit() {
        try {
            resolveLlmPermitSemaphore().acquire();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("persona reading llm permit interrupted", ex);
        }
    }

    private void releaseLlmPermit() {
        resolveLlmPermitSemaphore().release();
    }

    private Semaphore resolveLlmPermitSemaphore() {
        Semaphore semaphore = llmPermitSemaphore;
        if (semaphore != null) {
            return semaphore;
        }
        synchronized (llmPermitLock) {
            if (llmPermitSemaphore == null) {
                llmPermitSemaphore = new Semaphore(Math.max(1, maxInflight), true);
            }
            return llmPermitSemaphore;
        }
    }

    private Map<String, Object> normalizeCachedNode(Map<String, Object> input) {
        Map<String, Object> node = new LinkedHashMap<>();
        if (input == null) {
            return node;
        }
        node.putAll(input);
        String rawMarkdown = firstNonBlank(
                String.valueOf(input.getOrDefault("raw_markdown", "")),
                String.valueOf(input.getOrDefault("rawMarkdown", ""))
        );
        String nodeType = firstNonBlank(
                String.valueOf(input.getOrDefault("node_type", "")),
                String.valueOf(input.getOrDefault("nodeType", "paragraph"))
        );
        double score = normalizeScore(String.valueOf(input.getOrDefault("relevance_score", "0.5")), 0.5d);
        String reason = normalizeReasonText(readByAlias(input, "reason", "why", "rationale", "analysis"));
        if (reason == null) {
            ParagraphNode shadow = new ParagraphNode();
            shadow.nodeType = StringUtils.hasText(nodeType) ? nodeType : "paragraph";
            shadow.rawMarkdown = rawMarkdown;
            reason = buildDefaultReason(shadow, score);
        }
        node.put("raw_markdown", rawMarkdown);
        node.put("node_type", StringUtils.hasText(nodeType) ? nodeType : "paragraph");
        node.put("relevance_score", score);
        node.put("reason", reason);
        return node;
    }

    private String buildHeuristicReason(
            boolean hitCodeBlock,
            boolean hitListBlock,
            boolean hitSkill,
            boolean hitChallenge,
            boolean shortText,
            double score
    ) {
        List<String> reasons = new ArrayList<>();
        if (hitChallenge) {
            reasons.add("命中当前挑战关键词");
        }
        if (hitSkill) {
            reasons.add("包含技能栈相关信息");
        }
        if (hitCodeBlock) {
            reasons.add("代码段通常信息密度更高");
        }
        if (hitListBlock) {
            reasons.add("结构化列表便于快速吸收");
        }
        if (shortText) {
            reasons.add("段落过短导致信息不足");
        }
        if (reasons.isEmpty()) {
            return buildScoreBucketReason(score);
        }
        return trimText(String.join("；", reasons), 220);
    }

    private String buildDefaultReason(ParagraphNode node, double score) {
        if (node == null) {
            return buildScoreBucketReason(score);
        }
        String text = String.valueOf(node.rawMarkdown == null ? "" : node.rawMarkdown).trim();
        if ("code_block".equals(node.nodeType)) {
            return trimText("代码片段通常与实作问题直接相关，优先级上调。", 220);
        }
        if ("heading".equals(node.nodeType) && text.length() <= 40) {
            return trimText("标题主要用于导航，不承载完整论证。", 220);
        }
        if (text.length() < 28) {
            return trimText("段落信息量偏低，作为背景噪声处理。", 220);
        }
        return buildScoreBucketReason(score);
    }

    private String buildScoreBucketReason(double score) {
        if (score >= 0.85d) {
            return "与用户画像高度相关，属于高价值焦点段落。";
        }
        if (score < 0.3d) {
            return "与当前画像弱相关，优先降噪折叠。";
        }
        return "与用户画像中度相关，保留常态阅读权重。";
    }

    private static class NodeChunk {
        private final String chunkId;
        private final List<String> nodeIds;
        private final String primaryNodeId;
        private final String chunkText;

        private NodeChunk(String chunkId, List<String> nodeIds, String primaryNodeId, String chunkText) {
            this.chunkId = chunkId;
            this.nodeIds = nodeIds != null ? nodeIds : List.of();
            this.primaryNodeId = primaryNodeId;
            this.chunkText = chunkText;
        }
    }

    private static class ChunkAnnotation {
        private final String chunkId;
        private final double relevanceScore;
        private final String reason;
        private final String bridgeText;
        private final List<String> insightsTags;

        private ChunkAnnotation(
                String chunkId,
                double relevanceScore,
                String reason,
                String bridgeText,
                List<String> insightsTags
        ) {
            this.chunkId = chunkId;
            this.relevanceScore = relevanceScore;
            this.reason = reason;
            this.bridgeText = bridgeText;
            this.insightsTags = insightsTags != null ? insightsTags : List.of();
        }
    }

    private static class ParagraphNode {
        private String nodeId;
        private int order;
        private String nodeType;
        private String rawMarkdown;
    }

    private static class NodeAnnotation {
        private final String nodeId;
        private final double relevanceScore;
        private final String reason;
        private final String bridgeText;
        private final List<String> insightsTags;

        private NodeAnnotation(
                String nodeId,
                double relevanceScore,
                String reason,
                String bridgeText,
                List<String> insightsTags
        ) {
            this.nodeId = nodeId;
            this.relevanceScore = relevanceScore;
            this.reason = reason;
            this.bridgeText = bridgeText;
            this.insightsTags = insightsTags != null ? insightsTags : List.of();
        }
    }

    public static class PersonalizedReadingPayload {
        public String taskId;
        public String userKey;
        public String source;
        public String generatedAt;
        public String cachePath;
        public String cacheScope;
        public String chunkStrategy;
        public Map<String, Object> persona;
        public List<Map<String, Object>> nodes = new ArrayList<>();
    }
}
