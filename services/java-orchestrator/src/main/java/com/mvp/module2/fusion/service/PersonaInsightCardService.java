package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class PersonaInsightCardService {
    private static final Logger logger = LoggerFactory.getLogger(PersonaInsightCardService.class);

    private static final Pattern UNSAFE_PATH_SEGMENT = Pattern.compile("[^\\p{L}\\p{N}._-]");
    private static final Pattern LINE_BREAK_PATTERN = Pattern.compile("[\\r\\n]+");
    private static final Pattern SEMANTIC_LABEL_PATTERN = Pattern.compile(
            "^(\\s*(?:[-*+]\\s+|\\d+\\.\\s+)?)((?:【[^】]{1,48}】|[\\p{L}\\p{N}\\u4E00-\\u9FFF_\\-()/（）·]{2,48})[：:])(\\s*)(.+)$"
    );
    private static final String STORAGE_TASK_PREFIX = "storage:";
    private static final String TASK_META_FILE_NAME = "mobile_task_meta.json";
    private static final String TASK_CACHE_DIR = ".mobile_persona_cache";
    private static final String TASK_INSIGHT_DIR = "insight_cards";
    private static final String TASK_INDEX_FILE = "insight_cards_index.json";
    private static final String TASK_INTERACTION_FILE = "llm_interactions.ndjson";
    private static final int DEFAULT_MAX_CONTEXT_CHARS = 240;
    private static final int DEFAULT_MAX_TAGS = 48;
    private static final int DEFAULT_MAX_RELATED_TAGS = 8;
    private static final int DEFAULT_MAX_SNIPPETS = 3;
    private static final String SECTION_BACKGROUND = "## 背景知识";
    private static final String SECTION_CONTEXTUAL = "## 语境化解释";
    private static final String SECTION_DEPTH = "## 深度";
    private static final String SECTION_BREADTH = "## 广度";
    private static final List<String> LEGACY_CARD_MARKERS = List.of(
            "## 语",
            "### 语",
            "LLM原始",
            "证据片段"
    );

    @Value("${telemetry.persona-reading.insight-cards.enabled:true}")
    private boolean enabled;

    @Value("${telemetry.persona-reading.insight-cards.max-tags:48}")
    private int maxTags;

    @Value("${telemetry.persona-reading.insight-cards.max-related-tags:8}")
    private int maxRelatedTags;

    @Value("${telemetry.persona-reading.insight-cards.max-context-chars:240}")
    private int maxContextChars;

    @Value("${telemetry.persona-reading.insight-cards.max-snippets:3}")
    private int maxSnippets;

    @Value("${telemetry.persona-reading.insight-cards.force-regenerate:false}")
    private boolean forceRegenerateExistingCards;

    @Value("${telemetry.persona-reading.insight-cards.regenerate-on-legacy-marker:true}")
    private boolean regenerateOnLegacyMarker;

    @Value("${telemetry.persona-reading.insight-cards.max-inflight:64}")
    private int cardGenerationMaxInflight;

    @Autowired(required = false)
    private DeepSeekAdvisorService deepSeekAdvisorService;

    @Autowired(required = false)
    private CardStorageService cardStorageService;

    @Autowired(required = false)
    private TelemetryLlmInteractionLogService interactionLogService;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Object writeLock = new Object();
    private final Map<String, Object> generationLocks = new ConcurrentHashMap<>();
    private final Map<String, String> optimisticInFlightTokens = new ConcurrentHashMap<>();
    private final Object llmPermitLock = new Object();
    private final Object cardGenerationExecutorLock = new Object();
    private final AtomicInteger cardGenerationThreadIndex = new AtomicInteger(1);
    private volatile ExecutorService cardGenerationExecutor;
    private volatile Semaphore llmPermitSemaphore;

    @Async("taskExecutor")
    public void generateAsync(
            String taskId,
            String userId,
            Path markdownPath,
            List<Map<String, Object>> personalizedNodes
    ) {
        generateWithOptimisticLock(taskId, userId, markdownPath, personalizedNodes, true);
    }

    public void generateSync(
            String taskId,
            String userId,
            Path markdownPath,
            List<Map<String, Object>> personalizedNodes
    ) {
        generateWithOptimisticLock(taskId, userId, markdownPath, personalizedNodes, false);
    }

    @PreDestroy
    public void shutdownExecutor() {
        ExecutorService executor = cardGenerationExecutor;
        if (executor == null) {
            return;
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        } finally {
            cardGenerationExecutor = null;
        }
    }

    private void generateWithOptimisticLock(
            String taskId,
            String userId,
            Path markdownPath,
            List<Map<String, Object>> personalizedNodes,
            boolean skipWhenSameTokenInFlight
    ) {
        if (!enabled || personalizedNodes == null || personalizedNodes.isEmpty()) {
            return;
        }
        if (deepSeekAdvisorService == null || cardStorageService == null) {
            return;
        }
        String safeTaskId = normalizeSegment(taskId, "unknown_task");
        String safeUser = normalizeSegment(userId, "anonymous");
        String lockKey = safeTaskId + "|" + (markdownPath == null ? "" : markdownPath.toAbsolutePath().normalize());
        String optimisticToken = buildOptimisticToken(markdownPath, personalizedNodes);
        String currentInFlightToken = optimisticInFlightTokens.putIfAbsent(lockKey, optimisticToken);
        if (skipWhenSameTokenInFlight
                && currentInFlightToken != null
                && currentInFlightToken.equals(optimisticToken)) {
            return;
        }
        Object lock = generationLocks.computeIfAbsent(lockKey, key -> new Object());
        try {
            synchronized (lock) {
                doGenerate(safeTaskId, safeUser, markdownPath, personalizedNodes);
            }
        } catch (Exception ex) {
            logger.warn("persona insight cards generation failed: taskId={} err={}", safeTaskId, ex.getMessage());
        } finally {
            generationLocks.remove(lockKey, lock);
            if (currentInFlightToken == null) {
                optimisticInFlightTokens.remove(lockKey, optimisticToken);
            }
        }
    }

    public Map<String, Object> loadIndexSnapshot(String taskId, Path markdownPath) {
        String safeTaskId = normalizeSegment(taskId, "unknown_task");
        try {
            Path taskRoot = resolveTaskRootDirectory(safeTaskId, markdownPath);
            if (taskRoot == null) {
                return Map.of();
            }
            Path indexPath = resolveIndexPath(taskRoot);
            if (!Files.isRegularFile(indexPath)) {
                return Map.of();
            }
            Map<String, Object> root = objectMapper.readValue(
                    Files.readString(indexPath, StandardCharsets.UTF_8),
                    new TypeReference<Map<String, Object>>() {}
            );
            root.put("indexPath", indexPath.toString());
            return root;
        } catch (Exception ex) {
            return Map.of();
        }
    }

    private void doGenerate(
            String taskId,
            String userKey,
            Path markdownPath,
            List<Map<String, Object>> personalizedNodes
    ) throws Exception {
        Path taskRoot = resolveTaskRootDirectory(taskId, markdownPath);
        if (taskRoot == null) {
            return;
        }
        Path workDir = resolveWorkDirectory(taskRoot);
        Files.createDirectories(workDir);

        LinkedHashMap<String, TagContext> contexts = collectTagContexts(personalizedNodes);
        if (contexts.isEmpty()) {
            return;
        }
        List<TagContext> tagContexts = contexts.values().stream()
                .sorted(Comparator.comparing((TagContext item) -> item.tag, String.CASE_INSENSITIVE_ORDER))
                .limit(Math.max(1, maxTags > 0 ? maxTags : DEFAULT_MAX_TAGS))
                .toList();
        if (tagContexts.isEmpty()) {
            return;
        }

        String articleKey = buildArticleKey(taskRoot, markdownPath);
        String fingerprint = buildFingerprint(articleKey, tagContexts);
        boolean allCardsReady = areAllCardsPresent(tagContexts);
        Path indexPath = resolveIndexPath(taskRoot);
        Map<String, Object> existingIndex = readIndexIfExists(indexPath);
        String existingFingerprint = String.valueOf(existingIndex.getOrDefault("fingerprint", ""));
        if (fingerprint.equals(existingFingerprint) && allCardsReady) {
            return;
        }

        ExecutorService executor = resolveCardGenerationExecutor();
        List<CompletableFuture<IndexedInsightCardEntry>> futures = new ArrayList<>();
        for (int index = 0; index < tagContexts.size(); index += 1) {
            final int entryIndex = index;
            final TagContext context = tagContexts.get(index);
            futures.add(
                    CompletableFuture
                            .supplyAsync(
                                    () -> buildInsightCardEntry(
                                            taskId,
                                            userKey,
                                    articleKey,
                                    workDir,
                                    context,
                                    entryIndex
                                    ),
                                    executor
                            )
                            .exceptionally(ex -> {
                                logger.warn(
                                        "insight card async generation failed: taskId={} tag={} err={}",
                                        taskId,
                                        context.tag,
                                        ex.getMessage()
                                );
                                return null;
                            })
            );
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        List<Map<String, Object>> resultEntries = futures.stream()
                .map(CompletableFuture::join)
                .filter(Objects::nonNull)
                .sorted(Comparator.comparingInt(item -> item.index))
                .map(item -> item.entry)
                .toList();

        Map<String, Object> root = new LinkedHashMap<>();
        root.put("taskId", taskId);
        root.put("userKey", userKey);
        root.put("articleKey", articleKey);
        root.put("markdownPath", markdownPath != null ? markdownPath.toString() : "");
        root.put("fingerprint", fingerprint);
        root.put("generatedAt", Instant.now().toString());
        root.put("count", resultEntries.size());
        root.put("items", resultEntries);
        persistJsonAtomically(indexPath, root);
    }

    private IndexedInsightCardEntry buildInsightCardEntry(
            String taskId,
            String userKey,
            String articleKey,
            Path workDir,
            TagContext context,
            int index
    ) {
        InsightCardResult cardResult = upsertCardForTag(
                taskId,
                userKey,
                articleKey,
                context,
                workDir
        );
        if (cardResult == null) {
            return null;
        }
        Map<String, Object> line = new LinkedHashMap<>();
        line.put("tag", context.tag);
        line.put("cardTitle", cardResult.cardTitle);
        line.put("cardPath", cardResult.cardPath);
        line.put("source", cardResult.source);
        line.put("generatedAt", cardResult.generatedAt);
        line.put("nodeIds", new ArrayList<>(context.nodeIds));
        line.put("relatedTags", new ArrayList<>(context.relatedTags));
        line.put("snapshotPath", cardResult.snapshotPath);
        return new IndexedInsightCardEntry(index, line);
    }

    private boolean areAllCardsPresent(List<TagContext> contexts) {
        if (contexts == null || contexts.isEmpty()) {
            return true;
        }
        for (TagContext context : contexts) {
            if (context == null || !StringUtils.hasText(context.tag)) {
                continue;
            }
            try {
                CardStorageService.CardReadResult result = cardStorageService.readCard(context.tag);
                if (result == null || !result.exists || !StringUtils.hasText(result.markdown)) {
                    return false;
                }
            } catch (Exception ex) {
                return false;
            }
        }
        return true;
    }

    private InsightCardResult upsertCardForTag(
            String taskId,
            String userKey,
            String articleKey,
            TagContext context,
            Path workDir
    ) {
        Instant startedAt = Instant.now();
        Map<String, Object> llmTrace = new LinkedHashMap<>();
        llmTrace.put("taskId", taskId);
        llmTrace.put("userKey", userKey);
        llmTrace.put("articleKey", articleKey);
        llmTrace.put("term", context.tag);
        llmTrace.put("nodeIds", new ArrayList<>(context.nodeIds));
        llmTrace.put("context", context.primaryContextBlock());

        try {
            CardStorageService.CardReadResult existing = cardStorageService.readCard(context.tag);
            String contextualLine = buildContextualLine(context);

            String source = "reused";
            String llmRaw = "";
            String llmRequestPayloadJson = "";
            String llmResponseBodyJson = "";
            String targetMarkdown;
            String currentBody = String.valueOf(existing.markdown == null ? "" : existing.markdown).trim();
            boolean shouldRegenerate = existing.exists && shouldRegenerateExistingCard(currentBody);

            if (existing.exists && !shouldRegenerate) {
                targetMarkdown = currentBody;
            } else {
                source = existing.exists ? "regenerated" : "generated";
                String contextBlock = context.primaryContextBlock();
                String contextExample = context.primaryReason();
                StructuredAdviceSections sections;
                try {
                    DeepSeekAdvisorService.StructuredAdviceResult advice;
                    acquireLlmPermit();
                    try {
                        advice = deepSeekAdvisorService.requestStructuredAdvice(
                                context.tag,
                                contextBlock,
                                contextExample,
                                true
                        );
                    } finally {
                        releaseLlmPermit();
                    }
                    llmTrace.put("llmMode", "single_term");
                    llmRaw = String.valueOf(advice.raw == null ? "" : advice.raw).trim();
                    llmRequestPayloadJson = String.valueOf(advice.requestPayloadJson == null ? "" : advice.requestPayloadJson).trim();
                    llmResponseBodyJson = String.valueOf(advice.responseBodyJson == null ? "" : advice.responseBodyJson).trim();
                    source = StringUtils.hasText(advice.source) ? advice.source : source;
                    sections = sectionsFromAdvice(advice);
                    if (sections.background.isEmpty()
                            && sections.contextual.isEmpty()
                            && sections.depth.isEmpty()
                            && sections.breadth.isEmpty()) {
                        sections = parseAdviceSectionsFromJson(llmRaw, contextualLine);
                    }
                    sections = fillMissingSectionDefaults(sections, contextualLine);
                    llmTrace.put("status", "OK");
                    llmTrace.put("source", source);
                    llmTrace.put("llmRaw", llmRaw);
                    llmTrace.put("llmRequestPayloadJson", llmRequestPayloadJson);
                    llmTrace.put("llmResponseBodyJson", llmResponseBodyJson);
                } catch (Exception llmEx) {
                    source = "heuristic_fallback";
                    sections = parseAdviceSectionsFromJson("", contextualLine);
                    llmTrace.put("status", "FALLBACK");
                    llmTrace.put("error", llmEx.getMessage());
                }
                targetMarkdown = buildInitialCardBodyFromJson(sections);
            }

            CardStorageService.CardWriteOptions options = new CardStorageService.CardWriteOptions();
            options.contextDependent = Boolean.TRUE;
            options.type = "context";
            options.tags = ("reused".equals(source) && existing.exists) ? null : buildCardTags(taskId, source);
            options.sourceTaskId = taskId;
            options.sourcePath = articleKey;
            CardStorageService.CardSaveResult saved = cardStorageService.saveCard(context.tag, targetMarkdown, options);
            String snapshotPath = "";

            llmTrace.put("status", llmTrace.getOrDefault("status", "SKIPPED_REUSED"));
            llmTrace.put("cardTitle", saved.title);
            llmTrace.put("cardPath", saved.path != null ? saved.path.toString() : "");
            llmTrace.put("snapshotPath", snapshotPath);
            llmTrace.put("durationMs", Duration.between(startedAt, Instant.now()).toMillis());
            persistTaskInteraction(workDir, llmTrace);
            persistGlobalInteraction(userKey, taskId, llmTrace);

            return new InsightCardResult(
                    saved.title,
                    saved.path != null ? saved.path.toString() : "",
                    source,
                    Instant.now().toString(),
                    snapshotPath
            );
        } catch (Exception ex) {
            llmTrace.put("status", "ERROR");
            llmTrace.put("error", ex.getMessage());
            llmTrace.put("durationMs", Duration.between(startedAt, Instant.now()).toMillis());
            persistTaskInteraction(workDir, llmTrace);
            persistGlobalInteraction(userKey, taskId, llmTrace);
            logger.warn("upsert insight card failed: taskId={} term={} err={}", taskId, context.tag, ex.getMessage());
            return null;
        }
    }

    private LinkedHashMap<String, TagContext> collectTagContexts(List<Map<String, Object>> nodes) {
        LinkedHashMap<String, TagContext> contexts = new LinkedHashMap<>();
        if (nodes == null || nodes.isEmpty()) {
            return contexts;
        }
        for (int index = 0; index < nodes.size(); index += 1) {
            Map<String, Object> node = nodes.get(index);
            if (node == null || node.isEmpty()) {
                continue;
            }
            String nodeId = trimText(String.valueOf(node.getOrDefault("node_id", "")), 48);
            String rawMarkdown = normalizeMultilineText(node.getOrDefault("raw_markdown", ""));
            String reason = normalizeMultilineText(node.getOrDefault("reason", ""));
            String contextBlock = buildNeighborContextBlock(nodes, index);
            List<String> tags = filterTagsFromRawMarkdown(
                    normalizeTags(node.get("insights_tags")),
                    rawMarkdown
            );
            if (tags.isEmpty()) {
                continue;
            }
            for (String tag : tags) {
                String canonicalKey = canonicalTagKey(tag);
                TagContext context = contexts.computeIfAbsent(
                        canonicalKey,
                        key -> new TagContext(tag, canonicalKey)
                );
                if (StringUtils.hasText(nodeId)) {
                    context.nodeIds.add(nodeId);
                }
                if (StringUtils.hasText(rawMarkdown) && context.snippets.size() < Math.max(1, maxSnippets > 0 ? maxSnippets : DEFAULT_MAX_SNIPPETS)) {
                    context.snippets.add(rawMarkdown);
                }
                if (StringUtils.hasText(reason)) {
                    context.reasons.add(reason);
                }
                if (StringUtils.hasText(contextBlock) && context.contextBlocks.size() < Math.max(1, maxSnippets > 0 ? maxSnippets : DEFAULT_MAX_SNIPPETS)) {
                    context.contextBlocks.add(contextBlock);
                }
                for (String related : tags) {
                    if (!tag.equalsIgnoreCase(related)) {
                        addDistinctIgnoreCase(context.relatedTags, related);
                    }
                }
            }
        }
        List<TagContext> allContexts = new ArrayList<>(contexts.values());
        for (TagContext context : allContexts) {
            if (context == null) {
                continue;
            }
            for (TagContext other : allContexts) {
                if (other == null || !StringUtils.hasText(other.tag)) {
                    continue;
                }
                if (!context.tag.equalsIgnoreCase(other.tag)) {
                    addDistinctIgnoreCase(context.relatedTags, other.tag);
                }
            }
            int relatedCap = Math.max(1, maxRelatedTags > 0 ? maxRelatedTags : DEFAULT_MAX_RELATED_TAGS);
            if (context.relatedTags.size() > relatedCap) {
                List<String> trimmed = new ArrayList<>(context.relatedTags).subList(0, relatedCap);
                context.relatedTags = new LinkedHashSet<>(trimmed);
            }
        }
        return contexts;
    }

    private List<String> filterTagsFromRawMarkdown(List<String> tags, String rawMarkdown) {
        if (tags == null || tags.isEmpty() || !StringUtils.hasText(rawMarkdown)) {
            return List.of();
        }
        List<String> filtered = new ArrayList<>();
        for (String tag : tags) {
            if (!StringUtils.hasText(tag)) {
                continue;
            }
            if (tagAppearsInRawMarkdown(tag, rawMarkdown) && !containsIgnoreCase(filtered, tag)) {
                filtered.add(tag);
            }
        }
        return filtered;
    }

    private List<String> normalizeTags(Object rawTags) {
        if (rawTags == null) {
            return List.of();
        }
        List<String> output = new ArrayList<>();
        if (rawTags instanceof List<?> list) {
            for (Object item : list) {
                String tag = normalizeTag(String.valueOf(item == null ? "" : item));
                if (StringUtils.hasText(tag) && !containsIgnoreCase(output, tag)) {
                    output.add(tag);
                }
            }
            return output;
        }
        String text = String.valueOf(rawTags).trim();
        if (!StringUtils.hasText(text)) {
            return List.of();
        }
        if (text.startsWith("[") && text.endsWith("]")) {
            try {
                List<String> parsed = objectMapper.readValue(text, new TypeReference<List<String>>() {});
                for (String item : parsed) {
                    String tag = normalizeTag(item);
                    if (StringUtils.hasText(tag) && !containsIgnoreCase(output, tag)) {
                        output.add(tag);
                    }
                }
                return output;
            } catch (Exception ignored) {
                text = text.substring(1, text.length() - 1);
            }
        }
        for (String token : text.split("[,，;；\\n\\t]+")) {
            String tag = normalizeTag(token);
            if (StringUtils.hasText(tag) && !containsIgnoreCase(output, tag)) {
                output.add(tag);
            }
        }
        return output;
    }

    private boolean tagAppearsInRawMarkdown(String tag, String rawMarkdown) {
        String normalizedTag = compactForMatch(tag);
        String normalizedRaw = compactForMatch(rawMarkdown);
        if (!StringUtils.hasText(normalizedTag) || !StringUtils.hasText(normalizedRaw)) {
            return false;
        }
        return normalizedRaw.contains(normalizedTag);
    }

    private String compactForMatch(String raw) {
        return LINE_BREAK_PATTERN.matcher(String.valueOf(raw == null ? "" : raw))
                .replaceAll(" ")
                .replaceAll("\\s+", " ")
                .trim()
                .toLowerCase(Locale.ROOT);
    }

    private String canonicalTagKey(String rawTag) {
        return compactForMatch(rawTag);
    }

    private void addDistinctIgnoreCase(Set<String> values, String candidate) {
        if (values == null || !StringUtils.hasText(candidate)) {
            return;
        }
        for (String item : values) {
            if (candidate.equalsIgnoreCase(item)) {
                return;
            }
        }
        values.add(candidate);
    }

    private boolean containsIgnoreCase(List<String> values, String candidate) {
        if (values == null || candidate == null) {
            return false;
        }
        for (String item : values) {
            if (candidate.equalsIgnoreCase(item)) {
                return true;
            }
        }
        return false;
    }

    private String normalizeTag(String raw) {
        String value = LINE_BREAK_PATTERN.matcher(String.valueOf(raw == null ? "" : raw).trim()).replaceAll(" ");
        value = value.replaceAll("\\s+", " ").trim();
        if (!StringUtils.hasText(value)) {
            return "";
        }
        if (value.length() > 64) {
            value = value.substring(0, 64).trim();
        }
        return value;
    }

    private String buildNeighborContextBlock(List<Map<String, Object>> nodes, int index) {
        String prev = readNodeMarkdown(nodes, index - 1);
        String current = readNodeMarkdown(nodes, index);
        String next = readNodeMarkdown(nodes, index + 1);
        if (!StringUtils.hasText(current)) {
            return "";
        }
        StringBuilder builder = new StringBuilder();
        builder.append("【前文语境】").append('\n');
        builder.append(StringUtils.hasText(prev) ? prev : "（无）").append('\n');
        builder.append('\n');
        builder.append("【当前聚焦段落（在此处该词横空出世）】").append('\n');
        builder.append(current).append('\n');
        builder.append('\n');
        builder.append("【后文发展】").append('\n');
        builder.append(StringUtils.hasText(next) ? next : "（无）");
        return builder.toString().trim();
    }

    private String readNodeMarkdown(List<Map<String, Object>> nodes, int index) {
        if (nodes == null || index < 0 || index >= nodes.size()) {
            return "";
        }
        Map<String, Object> node = nodes.get(index);
        if (node == null || node.isEmpty()) {
            return "";
        }
        return normalizeMultilineText(node.getOrDefault("raw_markdown", ""));
    }

    private String normalizeMultilineText(Object raw) {
        return String.valueOf(raw == null ? "" : raw)
                .replace("\r\n", "\n")
                .replace('\r', '\n')
                .trim();
    }

    private String buildContextualLine(TagContext context) {
        String reason = context.primaryReason();
        String snippet = context.primaryContextBlock();
        if (StringUtils.hasText(reason)) {
            return trimText(reason, 220);
        }
        if (StringUtils.hasText(snippet)) {
            return "该术语在当前语境中承担关键概念锚点作用。";
        }
        return "该术语是本段高信息密度节点中的关键锚点。";
    }

    private StructuredAdviceSections parseAdviceSectionsFromJson(String rawAdvice, String fallbackContextLine) {
        StructuredAdviceSections sections = new StructuredAdviceSections();
        String normalized = stripJsonFence(String.valueOf(rawAdvice == null ? "" : rawAdvice).trim());
        if (!StringUtils.hasText(normalized)) {
            sections.background.add("该术语对应当前主题的关键背景约束与问题起点。");
            sections.contextual.add(trimText(fallbackContextLine, 220));
            sections.depth.add("该术语涉及底层约束、机制与反馈回路。");
            sections.breadth.add("该术语可映射到跨场景的工程与产品协同问题。");
            return sections;
        }
        try {
            String json = extractJsonObjectSafe(normalized);
            if (!StringUtils.hasText(json)) {
                throw new IllegalStateException("json object not found");
            }
            Map<String, Object> root = objectMapper.readValue(
                    json,
                    new TypeReference<Map<String, Object>>() {}
            );
            sections.background = parseJsonListFromRoot(
                    root,
                    "background",
                    "bg",
                    "background_knowledge"
            );
            sections.contextual = parseJsonListFromRoot(
                    root,
                    "contextual_explanations",
                    "contextualExplanations",
                    "contextual",
                    "context"
            );
            sections.depth = parseJsonListFromRoot(
                    root,
                    "depth",
                    "deep",
                    "principles",
                    "mechanism"
            );
            sections.breadth = parseJsonListFromRoot(
                    root,
                    "breadth",
                    "width",
                    "cross_domain",
                    "industry"
            );
        } catch (Exception ex) {
            sections.background = parseLooseArrayByKey(normalized, "background", "bg", "background_knowledge");
            sections.contextual = parseLooseArrayByKey(normalized, "contextual_explanations", "contextualExplanations");
            sections.depth = parseLooseArrayByKey(normalized, "depth", "deep", "principles", "mechanism");
            sections.breadth = parseLooseArrayByKey(normalized, "breadth", "width", "cross_domain", "industry");
            if (sections.background.isEmpty()
                    && sections.contextual.isEmpty()
                    && sections.depth.isEmpty()
                    && sections.breadth.isEmpty()) {
                sections.background = List.of("该术语对应当前主题的关键背景约束与问题起点。");
                sections.contextual = List.of(trimText(fallbackContextLine, 220));
                sections.depth = List.of("模型返回非结构化 JSON，已回退默认解释。");
                sections.breadth = List.of("建议检查 structured prompt 与模型输出格式。");
            }
        }
        if (sections.background.isEmpty()) {
            sections.background = List.of("该术语对应当前主题的关键背景约束与问题起点。");
        }
        if (sections.contextual.isEmpty()) {
            sections.contextual = List.of(trimText(fallbackContextLine, 220));
        }
        if (sections.depth.isEmpty()) {
            sections.depth = List.of("该术语涉及底层约束、机制与反馈回路。");
        }
        if (sections.breadth.isEmpty()) {
            sections.breadth = List.of("该术语可映射到跨场景的工程与产品协同问题。");
        }
        return sections;
    }

    private StructuredAdviceSections sectionsFromAdvice(DeepSeekAdvisorService.StructuredAdviceResult advice) {
        StructuredAdviceSections sections = new StructuredAdviceSections();
        if (advice == null) {
            return sections;
        }
        sections.background = normalizeAdviceLines(advice.background);
        sections.contextual = normalizeAdviceLines(advice.contextualExplanations);
        sections.depth = normalizeAdviceLines(advice.depth);
        sections.breadth = normalizeAdviceLines(advice.breadth);
        return sections;
    }

    private StructuredAdviceSections fillMissingSectionDefaults(StructuredAdviceSections sections, String fallbackContextLine) {
        StructuredAdviceSections resolved = sections != null ? sections : new StructuredAdviceSections();
        if (resolved.background == null || resolved.background.isEmpty()) {
            resolved.background = List.of("该术语对应当前主题的关键背景约束与问题起点。");
        }
        if (resolved.contextual == null || resolved.contextual.isEmpty()) {
            resolved.contextual = List.of(trimText(fallbackContextLine, 220));
        }
        if (resolved.depth == null || resolved.depth.isEmpty()) {
            resolved.depth = List.of("该术语涉及底层约束、机制与反馈回路。");
        }
        if (resolved.breadth == null || resolved.breadth.isEmpty()) {
            resolved.breadth = List.of("该术语可映射到跨场景的工程与产品协同问题。");
        }
        return resolved;
    }

    private List<String> normalizeAdviceLines(List<String> rawLines) {
        if (rawLines == null || rawLines.isEmpty()) {
            return List.of();
        }
        List<String> output = new ArrayList<>();
        for (String line : rawLines) {
            String text = normalizeMarkdownSectionText(String.valueOf(line == null ? "" : line), 1600);
            if (!StringUtils.hasText(text)) {
                continue;
            }
            output.add(text);
            if (output.size() >= 6) {
                break;
            }
        }
        return output;
    }

    private String stripJsonFence(String text) {
        String value = String.valueOf(text == null ? "" : text).trim();
        if (!StringUtils.hasText(value)) {
            return "";
        }
        if (value.startsWith("```")) {
            int firstBreak = value.indexOf('\n');
            if (firstBreak >= 0) {
                value = value.substring(firstBreak + 1).trim();
            }
        }
        if (value.endsWith("```")) {
            value = value.substring(0, value.length() - 3).trim();
        }
        return value;
    }

    private List<String> parseLooseArrayByKey(String text, String... keys) {
        if (!StringUtils.hasText(text) || keys == null || keys.length == 0) {
            return List.of();
        }
        for (String key : keys) {
            if (!StringUtils.hasText(key)) {
                continue;
            }
            String quotedKey = "\"" + key + "\"";
            int keyStart = text.indexOf(quotedKey);
            if (keyStart < 0) {
                continue;
            }
            int bracketStart = text.indexOf('[', keyStart + quotedKey.length());
            if (bracketStart < 0) {
                continue;
            }
            String arrayContent = extractArrayContentLoose(text, bracketStart);
            List<String> lines = extractQuotedStrings(arrayContent);
            if (!lines.isEmpty()) {
                return lines;
            }
        }
        return List.of();
    }

    private String extractArrayContentLoose(String text, int bracketStart) {
        if (!StringUtils.hasText(text) || bracketStart < 0 || bracketStart >= text.length()) {
            return "";
        }
        int depth = 0;
        boolean inString = false;
        boolean escaped = false;
        StringBuilder builder = new StringBuilder();
        for (int i = bracketStart; i < text.length(); i += 1) {
            char ch = text.charAt(i);
            builder.append(ch);
            if (escaped) {
                escaped = false;
                continue;
            }
            if (ch == '\\') {
                escaped = true;
                continue;
            }
            if (ch == '"') {
                inString = !inString;
                continue;
            }
            if (inString) {
                continue;
            }
            if (ch == '[') {
                depth += 1;
            } else if (ch == ']') {
                depth -= 1;
                if (depth == 0) {
                    return builder.toString();
                }
            }
        }
        return builder.toString();
    }

    private List<String> extractQuotedStrings(String text) {
        if (!StringUtils.hasText(text)) {
            return List.of();
        }
        List<String> output = new ArrayList<>();
        Matcher matcher = Pattern.compile("\"((?:\\\\.|[^\"\\\\])*)\"").matcher(text);
        while (matcher.find()) {
            String raw = matcher.group(1);
            String unescaped = raw
                    .replace("\\\\", "\\")
                    .replace("\\\"", "\"")
                    .replace("\\n", "\n")
                    .replace("\\r", "\n")
                    .replace("\\t", "    ")
                    .trim();
            if (!StringUtils.hasText(unescaped)) {
                continue;
            }
            String normalized = normalizeMarkdownSectionText(unescaped, 1600);
            if (!StringUtils.hasText(normalized)) {
                continue;
            }
            output.add(normalized);
            if (output.size() >= 6) {
                break;
            }
        }
        return output;
    }

    private List<String> parseJsonListFromRoot(Map<String, Object> root, String... keys) {
        if (root == null || root.isEmpty() || keys == null || keys.length == 0) {
            return List.of();
        }
        for (String key : keys) {
            if (!root.containsKey(key)) {
                continue;
            }
            Object raw = root.get(key);
            List<String> output = new ArrayList<>();
            if (raw instanceof List<?> list) {
                for (Object item : list) {
                    String line = normalizeMarkdownSectionText(String.valueOf(item == null ? "" : item), 1600);
                    if (StringUtils.hasText(line)) {
                        output.add(line);
                    }
                    if (output.size() >= 6) {
                        break;
                    }
                }
                return output;
            }
            String single = normalizeMarkdownSectionText(String.valueOf(raw == null ? "" : raw), 1600);
            if (StringUtils.hasText(single)) {
                return List.of(single);
            }
        }
        return List.of();
    }

    private String extractJsonObjectSafe(String text) {
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

    private String buildInitialCardBodyFromJson(StructuredAdviceSections sections) {
        StringBuilder builder = new StringBuilder();
        builder.append(SECTION_BACKGROUND).append('\n');
        builder.append(renderOrderedLines(sections.background, 1));
        builder.append('\n');
        builder.append(SECTION_CONTEXTUAL).append('\n');
        builder.append(renderOrderedLines(sections.contextual, 1));
        builder.append('\n');
        builder.append(SECTION_DEPTH).append('\n');
        builder.append(renderOrderedLines(sections.depth, 1));
        builder.append('\n');
        builder.append(SECTION_BREADTH).append('\n');
        builder.append(renderOrderedLines(sections.breadth, 1));
        return builder.toString().trim();
    }

    private String renderOrderedLines(List<String> lines, int startIndex) {
        if (lines == null || lines.isEmpty()) {
            return "";
        }
        int cursor = Math.max(1, startIndex);
        StringBuilder builder = new StringBuilder();
        for (String line : lines) {
            String item = normalizeMarkdownSectionText(String.valueOf(line == null ? "" : line), 1600);
            if (!StringUtils.hasText(item)) {
                continue;
            }
            String renderedItem = renderOrderedItem(cursor, item);
            if (!StringUtils.hasText(renderedItem)) {
                continue;
            }
            builder.append(renderedItem).append('\n');
            cursor += 1;
        }
        return builder.toString();
    }

    private String renderOrderedItem(int index, String item) {
        String normalized = String.valueOf(item == null ? "" : item)
                .replace("\r\n", "\n")
                .replace('\r', '\n')
                .trim();
        if (!StringUtils.hasText(normalized)) {
            return "";
        }
        String[] lines = normalized.split("\n", -1);
        StringBuilder builder = new StringBuilder();
        builder.append(index).append(". ").append(lines[0].stripLeading());
        for (int lineIndex = 1; lineIndex < lines.length; lineIndex += 1) {
            builder.append('\n');
            String line = lines[lineIndex];
            String continuation = String.valueOf(line == null ? "" : line).stripLeading();
            if (StringUtils.hasText(continuation)) {
                builder.append("    ").append(continuation);
            }
        }
        return builder.toString().trim();
    }

    private String normalizeMarkdownSectionText(String raw, int maxLength) {
        String normalized = String.valueOf(raw == null ? "" : raw)
                .replace("\r\n", "\n")
                .replace('\r', '\n')
                .replace('\u0000', ' ')
                .trim();
        if (!StringUtils.hasText(normalized)) {
            return "";
        }
        StringBuilder builder = new StringBuilder();
        boolean previousBlank = false;
        boolean previousTableRow = false;
        for (String segment : normalized.split("\n", -1)) {
            String line = String.valueOf(segment == null ? "" : segment)
                    .replace('\t', ' ')
                    .stripTrailing();
            if (!StringUtils.hasText(line.trim())) {
                if (builder.length() > 0 && !previousBlank) {
                    builder.append('\n');
                }
                previousBlank = true;
                previousTableRow = false;
                continue;
            }
            boolean tableRow = isMarkdownTableRow(line);
            if (tableRow && builder.length() > 0 && !previousBlank && !previousTableRow) {
                builder.append('\n');
                previousBlank = true;
            }
            if (builder.length() > 0) {
                builder.append('\n');
            }
            builder.append(emphasizeSemanticLabel(line));
            previousBlank = false;
            previousTableRow = tableRow;
        }
        String value = builder.toString().trim();
        if (!StringUtils.hasText(value)) {
            return "";
        }
        int safeMaxLength = maxLength > 0 ? maxLength : 1600;
        if (value.length() <= safeMaxLength) {
            return value;
        }
        return value.substring(0, safeMaxLength).trim();
    }

    private boolean isMarkdownTableRow(String rawLine) {
        String trimmed = String.valueOf(rawLine == null ? "" : rawLine).trim();
        if (!StringUtils.hasText(trimmed)) {
            return false;
        }
        if (!trimmed.startsWith("|")) {
            return false;
        }
        return trimmed.indexOf('|', 1) > 1;
    }

    private String emphasizeSemanticLabel(String rawLine) {
        String line = String.valueOf(rawLine == null ? "" : rawLine);
        if (!StringUtils.hasText(line)) {
            return "";
        }
        String trimmed = line.trim();
        if (!StringUtils.hasText(trimmed)
                || trimmed.startsWith("|")
                || trimmed.contains("://")
                || trimmed.startsWith("**")) {
            return line;
        }
        Matcher matcher = SEMANTIC_LABEL_PATTERN.matcher(line);
        if (!matcher.matches()) {
            return line;
        }
        String prefix = matcher.group(1);
        String label = matcher.group(2);
        String spacing = matcher.group(3);
        String body = matcher.group(4);
        if (label.contains("**")) {
            return line;
        }
        String gap = StringUtils.hasText(spacing) ? spacing : " ";
        return prefix + "**" + label + "**" + gap + body;
    }

    private boolean shouldRegenerateExistingCard(String currentBody) {
        if (forceRegenerateExistingCards) {
            return true;
        }
        if (!StringUtils.hasText(currentBody)) {
            return true;
        }
        if (regenerateOnLegacyMarker) {
            for (String marker : LEGACY_CARD_MARKERS) {
                if (StringUtils.hasText(marker) && currentBody.contains(marker)) {
                    return true;
                }
            }
        }
        return !currentBody.contains(SECTION_CONTEXTUAL)
                || !currentBody.contains(SECTION_BACKGROUND)
                || !currentBody.contains(SECTION_DEPTH)
                || !currentBody.contains(SECTION_BREADTH);
    }

    private void persistTaskInteraction(Path workDir, Map<String, Object> trace) {
        try {
            Path target = workDir.resolve(TASK_INTERACTION_FILE).normalize();
            if (!target.startsWith(workDir)) {
                throw new IllegalStateException("invalid insight interaction file");
            }
            String line = objectMapper.writeValueAsString(trace) + '\n';
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
        } catch (Exception ex) {
            logger.warn("persist task insight interaction failed: {}", ex.getMessage());
        }
    }

    private void persistGlobalInteraction(String userKey, String taskId, Map<String, Object> trace) {
        if (interactionLogService == null) {
            return;
        }
        interactionLogService.appendAsync("persona_insight_cards", userKey, taskId, trace);
    }

    private List<String> buildCardTags(String taskId, String source) {
        List<String> tags = new ArrayList<>();
        tags.add("persona-reading");
        tags.add("insight-card");
        if (StringUtils.hasText(source)) {
            tags.add(trimText(source, 24));
        }
        if (StringUtils.hasText(taskId)) {
            String compactTask = trimText(normalizeSegment(taskId, "task"), 30);
            if (StringUtils.hasText(compactTask)) {
                tags.add("task-" + compactTask);
            }
        }
        return tags;
    }

    private String buildArticleKey(Path taskRoot, Path markdownPath) {
        if (markdownPath == null) {
            return "unknown_markdown";
        }
        Path normalized = markdownPath.toAbsolutePath().normalize();
        if (taskRoot == null) {
            return normalizeSegment(normalized.toString(), "markdown");
        }
        try {
            Path relative = taskRoot.toAbsolutePath().normalize().relativize(normalized);
            String value = relative.toString().replace('\\', '/');
            return normalizeSegment(value, normalized.getFileName() != null ? normalized.getFileName().toString() : "markdown");
        } catch (Exception ex) {
            return normalizeSegment(normalized.getFileName() != null ? normalized.getFileName().toString() : normalized.toString(), "markdown");
        }
    }

    private String buildFingerprint(String articleKey, List<TagContext> contexts) {
        StringBuilder builder = new StringBuilder();
        builder.append(articleKey).append('|');
        for (TagContext context : contexts) {
            builder.append(context.canonicalKey).append(':');
            builder.append(String.join(",", context.nodeIds)).append(':');
            builder.append(String.join(",", context.snippets)).append(':');
            builder.append(String.join("||", context.contextBlocks)).append('|');
        }
        return Integer.toHexString(builder.toString().hashCode());
    }

    private String buildOptimisticToken(Path markdownPath, List<Map<String, Object>> personalizedNodes) {
        String markdownKey = markdownPath == null ? "" : markdownPath.toAbsolutePath().normalize().toString();
        LinkedHashMap<String, TagContext> contexts = collectTagContexts(personalizedNodes);
        if (contexts.isEmpty()) {
            return Integer.toHexString((markdownKey + "|empty").hashCode());
        }
        int limit = Math.max(1, maxTags > 0 ? maxTags : DEFAULT_MAX_TAGS);
        List<String> tags = contexts.values().stream()
                .map(context -> context.canonicalKey)
                .filter(StringUtils::hasText)
                .sorted(String.CASE_INSENSITIVE_ORDER)
                .limit(limit)
                .toList();
        StringBuilder builder = new StringBuilder();
        builder.append(markdownKey).append('|');
        for (String tag : tags) {
            builder.append(tag).append('|');
        }
        return Integer.toHexString(builder.toString().hashCode());
    }

    private Path resolveWorkDirectory(Path taskRoot) {
        Path workDir = taskRoot.resolve(TASK_CACHE_DIR).resolve(TASK_INSIGHT_DIR).normalize();
        if (!workDir.startsWith(taskRoot)) {
            throw new IllegalStateException("invalid insight cache path");
        }
        return workDir;
    }

    private Path resolveIndexPath(Path taskRoot) {
        Path index = resolveWorkDirectory(taskRoot).resolve(TASK_INDEX_FILE).normalize();
        if (!index.startsWith(taskRoot)) {
            throw new IllegalStateException("invalid insight index path");
        }
        return index;
    }

    private Map<String, Object> readIndexIfExists(Path indexPath) {
        try {
            if (!Files.isRegularFile(indexPath)) {
                return Map.of();
            }
            return objectMapper.readValue(
                    Files.readString(indexPath, StandardCharsets.UTF_8),
                    new TypeReference<Map<String, Object>>() {}
            );
        } catch (Exception ex) {
            return Map.of();
        }
    }

    private void persistJsonAtomically(Path target, Map<String, Object> payload) throws Exception {
        Path parent = target.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        synchronized (writeLock) {
            Files.writeString(
                    target,
                    objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(payload),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING
            );
        }
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

    private String normalizeSegment(String raw, String fallback) {
        String value = String.valueOf(raw == null ? "" : raw).trim();
        if (!StringUtils.hasText(value)) {
            value = fallback;
        }
        value = value.replace('\\', '_').replace('/', '_');
        value = UNSAFE_PATH_SEGMENT.matcher(value).replaceAll("_").replaceAll("_+", "_");
        if (!StringUtils.hasText(value)) {
            return fallback;
        }
        return value;
    }

    private String trimText(String raw, int maxLength) {
        String value = String.valueOf(raw == null ? "" : raw)
                .replace("\r\n", "\n")
                .replace('\r', '\n')
                .replace('\n', ' ')
                .replaceAll("\\s+", " ")
                .trim();
        if (maxLength <= 0) {
            maxLength = DEFAULT_MAX_CONTEXT_CHARS;
        }
        if (value.length() <= maxLength) {
            return value;
        }
        return value.substring(0, maxLength).trim();
    }

    private void acquireLlmPermit() {
        try {
            resolveLlmPermitSemaphore().acquire();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("insight cards llm permit interrupted", ex);
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
                llmPermitSemaphore = new Semaphore(resolveCardGenerationConcurrency(), true);
            }
            return llmPermitSemaphore;
        }
    }

    private ExecutorService resolveCardGenerationExecutor() {
        ExecutorService executor = cardGenerationExecutor;
        if (executor != null && !executor.isShutdown()) {
            return executor;
        }
        synchronized (cardGenerationExecutorLock) {
            if (cardGenerationExecutor == null || cardGenerationExecutor.isShutdown()) {
                int concurrency = resolveCardGenerationConcurrency();
                cardGenerationExecutor = Executors.newFixedThreadPool(
                        concurrency,
                        runnable -> {
                            Thread thread = new Thread(
                                    runnable,
                                    "InsightCardGen-" + cardGenerationThreadIndex.getAndIncrement()
                            );
                            thread.setDaemon(true);
                            return thread;
                        }
                );
            }
            return cardGenerationExecutor;
        }
    }

    private int resolveCardGenerationConcurrency() {
        int value = cardGenerationMaxInflight;
        if (value <= 0) {
            return 1;
        }
        return Math.min(value, 256);
    }

    private static class TagContext {
        private final String tag;
        private final String canonicalKey;
        private final Set<String> nodeIds = new LinkedHashSet<>();
        private final Set<String> snippets = new LinkedHashSet<>();
        private final Set<String> reasons = new LinkedHashSet<>();
        private final Set<String> contextBlocks = new LinkedHashSet<>();
        private Set<String> relatedTags = new LinkedHashSet<>();

        private TagContext(String tag, String canonicalKey) {
            this.tag = tag;
            this.canonicalKey = canonicalKey;
        }

        private String primarySnippet() {
            if (snippets.isEmpty()) {
                return "";
            }
            return snippets.iterator().next();
        }

        private String primaryReason() {
            if (reasons.isEmpty()) {
                return "";
            }
            return reasons.iterator().next();
        }

        private String primaryContextBlock() {
            if (!contextBlocks.isEmpty()) {
                return contextBlocks.iterator().next();
            }
            return primarySnippet();
        }
    }

    private static class StructuredAdviceSections {
        private List<String> background = new ArrayList<>();
        private List<String> contextual = new ArrayList<>();
        private List<String> depth = new ArrayList<>();
        private List<String> breadth = new ArrayList<>();
    }

    private static class InsightCardResult {
        private final String cardTitle;
        private final String cardPath;
        private final String source;
        private final String generatedAt;
        private final String snapshotPath;

        private InsightCardResult(String cardTitle, String cardPath, String source, String generatedAt, String snapshotPath) {
            this.cardTitle = cardTitle;
            this.cardPath = cardPath;
            this.source = source;
            this.generatedAt = generatedAt;
            this.snapshotPath = snapshotPath;
        }
    }

    private static class IndexedInsightCardEntry {
        private final int index;
        private final Map<String, Object> entry;

        private IndexedInsightCardEntry(int index, Map<String, Object> entry) {
            this.index = index;
            this.entry = entry;
        }
    }
}
