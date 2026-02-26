package com.mvp.module2.fusion.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mvp.module2.fusion.queue.TaskQueueManager;
import com.mvp.module2.fusion.service.MicroHypothesisExtractorService;
import com.mvp.module2.fusion.service.StorageTaskCacheService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * 通用 telemetry 接入端点。
 *
 * 设计目标：
 * 1. 对移动端微批上报提供稳定入口（/api/telemetry/ingest）。
 * 2. 入站后立即分流：冷数据落工程分析层，热数据进入逻辑事件池。
 * 3. 热事件只做分类与上下文挂载，不在本链路调用 LLM。
 */
@RestController
@RequestMapping("/api/telemetry")
public class TelemetryIngestController {
    private static final Logger logger = LoggerFactory.getLogger(TelemetryIngestController.class);

    private static final String STORAGE_TASK_PREFIX = "storage:";
    private static final String DEFAULT_MARKDOWN_NAME = "enhanced_output.md";
    private static final int MARKDOWN_SCAN_DEPTH = 4;
    private static final Pattern NODE_LINE_PATTERN =
            Pattern.compile("(?i)^(?:l|line|p|paragraph|node)[-_]?(\\d+)$");
    private static final Pattern TRAILING_NUMBER_PATTERN = Pattern.compile("(\\d+)(?!.*\\d)");
    private static final List<String> PAYLOAD_CONTEXT_KEYS = List.of(
            "originalMarkdown",
            "original_markdown",
            "nodeText",
            "text",
            "paragraphText",
            "paragraph_text",
            "content",
            "markdown"
    );
    private static final Set<String> HOT_EVENT_TYPES = Set.of(
            "paragraph_resonance_double_tap",
            "paragraph_mark_deleted_by_swipe",
            "paragraph_mark_deleted_by_swipe_confirmed",
            "note_saved",
            "lexical_card_opened",
            "selection_action_search_card",
            "insight_term_tapped",
            "lexical_token_selected",
            "selection_action_copy",
            "selection_action_like",
            "selection_action_unlike",
            "selection_action_bold",
            "selection_action_unbold",
            "selection_action_annotate",
            "paragraph_restore_deleted_by_swipe",
            "paragraph_mark_deleted_armed",
            "paragraph_comment_affordance_triggered",
            "noise_capsule_expanded"
    );
    private static final Set<String> TOKEN_LEVEL_EVENTS = Set.of(
            "lexical_token_selected",
            "selection_action_copy",
            "selection_action_like",
            "selection_action_unlike",
            "selection_action_bold",
            "selection_action_unbold",
            "selection_action_annotate",
            "selection_action_search_card",
            "insight_term_tapped",
            "lexical_card_opened"
    );
    private static final List<String> TOKEN_FOCUS_KEYS = List.of(
            "focusText",
            "focus_text",
            "selectedText",
            "selected_text",
            "token",
            "term",
            "keyword"
    );
    private static final int MAX_FOCUS_TEXT_CHARS = 80;
    private static final int LOCAL_CONTEXT_WINDOW_CHARS = 72;
    private static final int MAX_LOCAL_CONTEXT_CHARS = 220;

    @Value("${telemetry.ingest.file:var/telemetry/mobile_reader_telemetry.ndjson}")
    private String telemetryIngestFile;

    @Value("${telemetry.ingest.cold-file:var/telemetry/mobile_reader_cold.ndjson}")
    private String telemetryColdFile;

    @Value("${telemetry.ingest.logic-pool-file:var/telemetry/mobile_reader_logic_pool.ndjson}")
    private String telemetryLogicPoolFile;

    @Autowired(required = false)
    private TaskQueueManager taskQueueManager;

    @Autowired(required = false)
    private StorageTaskCacheService storageTaskCacheService;

    @Autowired(required = false)
    private MicroHypothesisExtractorService microHypothesisExtractorService;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Object writeLock = new Object();

    @PostMapping(value = "/ingest", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> ingest(@RequestBody TelemetryIngestRequest request) {
        if (request == null || request.events == null || request.events.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("message", "missing telemetry events"));
        }

        List<Map<String, Object>> accepted = new ArrayList<>();
        List<Map<String, Object>> coldRecords = new ArrayList<>();
        List<Map<String, Object>> hotRecords = new ArrayList<>();

        String normalizedTaskId = trimToEmpty(request.taskId);
        String normalizedPath = trimToEmpty(request.path);
        MarkdownContext markdownContext = resolveMarkdownContext(normalizedTaskId, normalizedPath);
        int unresolvedHotContextCount = 0;

        for (TelemetryEventItem item : request.events) {
            if (item == null) {
                continue;
            }
            String eventType = trimToNull(item.eventType);
            if (eventType == null) {
                continue;
            }

            String nodeId = trimToEmpty(item.nodeId);
            Map<String, String> payload = sanitizePayload(item.payload);
            Map<String, Object> record = new LinkedHashMap<>();
            record.put("taskId", normalizedTaskId);
            record.put("path", normalizedPath);
            record.put("flushReason", trimToEmpty(request.flushReason));
            record.put("batchSeq", request.batchSeq != null ? request.batchSeq : -1L);
            record.put("declaredBatchSize", request.batchSize != null ? request.batchSize : request.events.size());
            record.put("nodeId", nodeId);
            record.put("eventType", eventType);
            record.put("relevanceScore", item.relevanceScore != null ? item.relevanceScore : 0.0d);
            record.put("timestampMs", item.timestampMs != null ? item.timestampMs : System.currentTimeMillis());
            record.put("ingestedAt", Instant.now().toString());
            record.put("payload", payload);
            accepted.add(record);

            coldRecords.add(new LinkedHashMap<>(record));

            if (HOT_EVENT_TYPES.contains(eventType)) {
                NodeContextResolution nodeContext = resolveNodeContext(nodeId, payload, markdownContext);
                Map<String, Object> hotRecord = new LinkedHashMap<>(record);
                hotRecord.put("logicPoolCategory", "semantic_hot_event");
                hotRecord.put("nodeContextFound", nodeContext.found);
                hotRecord.put("contextSource", nodeContext.source);
                hotRecord.put("contextLine", nodeContext.lineNumber != null ? nodeContext.lineNumber : -1);
                hotRecord.put("nodeText", nodeContext.nodeText);
                hotRecord.put("nodeMarkdownSnippet", nodeContext.markdownSnippet);
                hotRecord.put("contextMarkdownPath", markdownContext != null ? markdownContext.markdownPath.toString() : "");
                boolean isTokenLevel = TOKEN_LEVEL_EVENTS.contains(eventType);
                TokenContextResolution tokenContext = isTokenLevel
                        ? resolveTokenContext(eventType, payload, nodeContext)
                        : TokenContextResolution.empty();
                hotRecord.put("isTokenLevel", isTokenLevel);
                hotRecord.put("focusText", tokenContext.focusText);
                hotRecord.put("focusSource", tokenContext.focusSource);
                hotRecord.put("tokenType", tokenContext.tokenType);
                hotRecord.put("localContext", tokenContext.localContext);
                hotRecord.put("localContextFound", tokenContext.found);
                hotRecords.add(hotRecord);
                if (!nodeContext.found) {
                    unresolvedHotContextCount += 1;
                }
            }
        }

        if (accepted.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("message", "no valid telemetry events"));
        }

        try {
            Path rawPath = appendNdjson(telemetryIngestFile, accepted);
            Path coldPath = appendNdjson(telemetryColdFile, coldRecords);
            Path hotPath = appendNdjson(telemetryLogicPoolFile, hotRecords);

            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("success", true);
            payload.put("accepted", accepted.size());
            payload.put("coldCount", coldRecords.size());
            payload.put("hotCount", hotRecords.size());
            payload.put("unresolvedHotContextCount", unresolvedHotContextCount);
            payload.put("telemetryPath", rawPath != null ? rawPath.toString() : "");
            payload.put("coldTelemetryPath", coldPath != null ? coldPath.toString() : "");
            payload.put("logicPoolPath", hotPath != null ? hotPath.toString() : "");
            payload.put("markdownContextResolved", markdownContext != null);
            payload.put("markdownPath", markdownContext != null ? markdownContext.markdownPath.toString() : "");
            boolean triggered = maybeTriggerMicroHypothesis(request, normalizedTaskId, normalizedPath, coldRecords, hotRecords);
            payload.put("microHypothesisTriggered", triggered);
            if (triggered) {
                payload.put("cognitiveUserKey", resolveUserKey(request.userId, normalizedTaskId));
            }
            payload.put("updatedAt", Instant.now().toString());
            return ResponseEntity.ok(payload);
        } catch (Exception ex) {
            logger.warn("failed to write telemetry ingest files: raw={} cold={} hot={} err={}",
                    telemetryIngestFile, telemetryColdFile, telemetryLogicPoolFile, ex.getMessage());
            return ResponseEntity.internalServerError().body(Map.of("message", "failed to persist telemetry events"));
        }
    }

    private Path appendNdjson(String filePath, List<Map<String, Object>> records) throws IOException {
        if (filePath == null || filePath.isBlank() || records == null || records.isEmpty()) {
            return null;
        }
        Path target = Paths.get(filePath).toAbsolutePath().normalize();
        if (target.getParent() != null) {
            Files.createDirectories(target.getParent());
        }
        StringBuilder builder = new StringBuilder();
        for (Map<String, Object> record : records) {
            builder.append(objectMapper.writeValueAsString(record)).append('\n');
        }
        synchronized (writeLock) {
            Files.writeString(
                    target,
                    builder.toString(),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.APPEND
            );
        }
        return target;
    }

    private boolean maybeTriggerMicroHypothesis(
            TelemetryIngestRequest request,
            String taskId,
            String path,
            List<Map<String, Object>> coldRecords,
            List<Map<String, Object>> hotRecords
    ) {
        if (microHypothesisExtractorService == null) {
            return false;
        }
        String reason = trimToNull(request != null ? request.flushReason : null);
        if (!"article_exit".equalsIgnoreCase(reason)) {
            return false;
        }
        if ((coldRecords == null || coldRecords.isEmpty()) && (hotRecords == null || hotRecords.isEmpty())) {
            return false;
        }
        String userKey = resolveUserKey(request != null ? request.userId : null, taskId);
        microHypothesisExtractorService.extractArticleHypothesesAsync(
                new MicroHypothesisExtractorService.ExtractionRequest(
                        userKey,
                        taskId,
                        path,
                        reason,
                        coldRecords,
                        hotRecords
                )
        );
        return true;
    }

    private String resolveUserKey(String requestUserId, String taskId) {
        String fromRequest = trimToNull(requestUserId);
        if (fromRequest != null) {
            return fromRequest;
        }
        String normalizedTaskId = trimToNull(taskId);
        if (normalizedTaskId != null && taskQueueManager != null) {
            TaskQueueManager.TaskEntry runtimeTask = taskQueueManager.getTask(normalizedTaskId);
            if (runtimeTask != null && trimToNull(runtimeTask.userId) != null) {
                return runtimeTask.userId.trim();
            }
        }
        if (normalizedTaskId != null) {
            return "task_" + normalizedTaskId;
        }
        return "anonymous";
    }

    private Map<String, String> sanitizePayload(Map<String, ?> input) {
        Map<String, String> output = new LinkedHashMap<>();
        if (input == null) {
            return output;
        }
        for (Map.Entry<String, ?> entry : input.entrySet()) {
            if (entry.getKey() == null || entry.getKey().isBlank()) continue;
            if (entry.getValue() == null) continue;
            String value = String.valueOf(entry.getValue()).trim();
            if (!value.isEmpty()) {
                output.put(entry.getKey(), value);
            }
        }
        return output;
    }

    private NodeContextResolution resolveNodeContext(
            String nodeId,
            Map<String, String> payload,
            MarkdownContext markdownContext
    ) {
        String payloadFallback = extractPayloadContext(payload);
        if (markdownContext == null || markdownContext.lines.isEmpty()) {
            if (payloadFallback != null) {
                return NodeContextResolution.fromPayload(payloadFallback);
            }
            return NodeContextResolution.unresolved();
        }

        Integer lineNumber = parseLineNumber(nodeId, markdownContext.lines.size());
        if (lineNumber != null) {
            int index = lineNumber - 1;
            String nodeText = firstNonBlankInWindow(markdownContext.lines, index);
            String snippet = buildMarkdownSnippet(markdownContext.lines, index);
            if (trimToNull(nodeText) != null || trimToNull(snippet) != null) {
                return NodeContextResolution.fromMarkdown(
                        lineNumber,
                        trimToEmpty(nodeText),
                        trimToEmpty(snippet)
                );
            }
        }

        if (payloadFallback != null) {
            return NodeContextResolution.fromPayload(payloadFallback);
        }
        return NodeContextResolution.unresolved();
    }

    private Integer parseLineNumber(String nodeId, int lineCount) {
        String normalized = trimToNull(nodeId);
        if (normalized == null || lineCount <= 0) {
            return null;
        }

        Matcher strict = NODE_LINE_PATTERN.matcher(normalized);
        if (strict.matches()) {
            Integer parsed = parsePositiveInt(strict.group(1));
            if (parsed != null && parsed <= lineCount) {
                return parsed;
            }
            return null;
        }

        Matcher tail = TRAILING_NUMBER_PATTERN.matcher(normalized);
        if (tail.find()) {
            Integer parsed = parsePositiveInt(tail.group(1));
            if (parsed != null && parsed <= lineCount) {
                return parsed;
            }
        }
        return null;
    }

    private Integer parsePositiveInt(String value) {
        try {
            int parsed = Integer.parseInt(value);
            return parsed > 0 ? parsed : null;
        } catch (Exception ex) {
            return null;
        }
    }

    private String firstNonBlankInWindow(List<String> lines, int centerIndex) {
        if (lines == null || lines.isEmpty()) {
            return "";
        }
        int start = Math.max(0, centerIndex - 1);
        int end = Math.min(lines.size() - 1, centerIndex + 1);

        if (centerIndex >= 0 && centerIndex < lines.size()) {
            String current = trimToNull(lines.get(centerIndex));
            if (current != null) {
                return current;
            }
        }
        for (int i = start; i <= end; i++) {
            String candidate = trimToNull(lines.get(i));
            if (candidate != null) {
                return candidate;
            }
        }
        return "";
    }

    private String buildMarkdownSnippet(List<String> lines, int centerIndex) {
        if (lines == null || lines.isEmpty()) {
            return "";
        }
        int start = Math.max(0, centerIndex - 1);
        int end = Math.min(lines.size() - 1, centerIndex + 1);
        StringBuilder snippet = new StringBuilder();
        for (int i = start; i <= end; i++) {
            String line = trimToNull(lines.get(i));
            if (line == null) {
                continue;
            }
            if (snippet.length() > 0) {
                snippet.append('\n');
            }
            snippet.append(line);
        }
        return snippet.toString();
    }

    private String extractPayloadContext(Map<String, String> payload) {
        if (payload == null || payload.isEmpty()) {
            return null;
        }
        for (String key : PAYLOAD_CONTEXT_KEYS) {
            String value = trimToNull(payload.get(key));
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    private TokenContextResolution resolveTokenContext(
            String eventType,
            Map<String, String> payload,
            NodeContextResolution nodeContext
    ) {
        String focusText = extractTokenFocus(payload);
        String tokenType = inferTokenType(eventType, payload);
        if (focusText == null) {
            String fallbackContext = trimToEmpty(extractPayloadContext(payload));
            return new TokenContextResolution(false, "", "", tokenType, fallbackContext);
        }
        String localContext = buildLocalContextWindow(focusText, nodeContext, payload);
        String source = resolveFocusSource(payload);
        return new TokenContextResolution(true, focusText, source, tokenType, localContext);
    }

    private String extractTokenFocus(Map<String, String> payload) {
        if (payload == null || payload.isEmpty()) {
            return null;
        }
        for (String key : TOKEN_FOCUS_KEYS) {
            String value = normalizeFocusText(payload.get(key));
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    private String resolveFocusSource(Map<String, String> payload) {
        if (payload == null || payload.isEmpty()) {
            return "unknown";
        }
        for (String key : TOKEN_FOCUS_KEYS) {
            if (trimToNull(payload.get(key)) != null) {
                return key;
            }
        }
        return "unknown";
    }

    private String inferTokenType(String eventType, Map<String, String> payload) {
        String source = trimToEmpty(payload != null ? payload.get("source") : "");
        if ("insight_terms".equalsIgnoreCase(source)) {
            return "insight_term";
        }
        String normalized = trimToEmpty(eventType).toLowerCase(Locale.ROOT);
        if (normalized.startsWith("selection_action_")) {
            return "selection";
        }
        if ("lexical_card_opened".equals(normalized)) {
            return "lexical_card";
        }
        return "token";
    }

    private String buildLocalContextWindow(
            String focusText,
            NodeContextResolution nodeContext,
            Map<String, String> payload
    ) {
        String sourceText = firstNonBlank(
                nodeContext != null ? nodeContext.nodeText : "",
                nodeContext != null ? nodeContext.markdownSnippet : "",
                extractPayloadContext(payload)
        );
        if (sourceText.isEmpty()) {
            return focusText;
        }
        int index = indexOfIgnoreCase(sourceText, focusText);
        if (index < 0) {
            return trimToMax(sourceText, MAX_LOCAL_CONTEXT_CHARS);
        }
        int start = Math.max(0, index - LOCAL_CONTEXT_WINDOW_CHARS);
        int end = Math.min(sourceText.length(), index + focusText.length() + LOCAL_CONTEXT_WINDOW_CHARS);
        return trimToMax(sourceText.substring(start, end), MAX_LOCAL_CONTEXT_CHARS);
    }

    private int indexOfIgnoreCase(String source, String target) {
        if (source == null || target == null) {
            return -1;
        }
        String normalizedSource = source.toLowerCase(Locale.ROOT);
        String normalizedTarget = target.toLowerCase(Locale.ROOT);
        return normalizedSource.indexOf(normalizedTarget);
    }

    private String normalizeFocusText(String raw) {
        String text = trimToNull(raw);
        if (text == null) {
            return null;
        }
        String compact = text.replaceAll("\\s+", " ").trim();
        if (compact.isEmpty()) {
            return null;
        }
        return compact.length() <= MAX_FOCUS_TEXT_CHARS
                ? compact
                : compact.substring(0, MAX_FOCUS_TEXT_CHARS).trim();
    }

    private MarkdownContext resolveMarkdownContext(String taskId, String pathHint) {
        String normalizedTaskId = trimToNull(taskId);
        if (normalizedTaskId == null) {
            return null;
        }
        try {
            Path markdownPath = resolveMarkdownPath(normalizedTaskId, pathHint);
            if (markdownPath == null || !Files.isRegularFile(markdownPath)) {
                return null;
            }
            List<String> lines = Files.readAllLines(markdownPath, StandardCharsets.UTF_8);
            return new MarkdownContext(markdownPath, lines);
        } catch (Exception ex) {
            logger.debug("resolve markdown context failed: taskId={} pathHint={} err={}",
                    normalizedTaskId, pathHint, ex.getMessage());
            return null;
        }
    }

    private Path resolveMarkdownPath(String taskId, String pathHint) throws IOException {
        if (taskQueueManager != null) {
            TaskQueueManager.TaskEntry runtimeTask = taskQueueManager.getTask(taskId);
            if (runtimeTask != null) {
                Path runtimeMarkdown = resolveRuntimeTaskMarkdown(runtimeTask, pathHint);
                if (runtimeMarkdown != null) {
                    return runtimeMarkdown;
                }
            }
        }

        if (storageTaskCacheService == null) {
            return null;
        }
        String storageKey = resolveStorageKey(taskId);
        if (storageKey == null) {
            return null;
        }

        Optional<StorageTaskCacheService.CachedTask> cachedTask = storageTaskCacheService.getTask(storageKey);
        if (cachedTask.isPresent()) {
            Path fromCache = resolveCachedTaskMarkdown(cachedTask.get(), pathHint);
            if (fromCache != null) {
                return fromCache;
            }
        }

        Path storageRoot = storageTaskCacheService.getStorageRoot();
        if (storageRoot == null) {
            return null;
        }
        Path normalizedRoot = storageRoot.toAbsolutePath().normalize();
        Path taskDir = normalizedRoot.resolve(storageKey).toAbsolutePath().normalize();
        if (!taskDir.startsWith(normalizedRoot) || !Files.isDirectory(taskDir)) {
            return null;
        }
        return resolveMarkdownInDirectory(taskDir, pathHint, null);
    }

    private Path resolveRuntimeTaskMarkdown(TaskQueueManager.TaskEntry task, String pathHint) throws IOException {
        String resultPath = task != null ? trimToNull(task.resultPath) : null;
        if (resultPath == null) {
            return null;
        }
        Path result = Paths.get(resultPath).toAbsolutePath().normalize();
        if (!Files.exists(result)) {
            return null;
        }
        if (Files.isRegularFile(result)) {
            if (isMarkdownFile(result.getFileName().toString())) {
                if (trimToNull(pathHint) != null && result.getParent() != null) {
                    Path byHint = resolvePathHint(result.getParent(), pathHint);
                    if (isMarkdownReadableFile(byHint)) {
                        return byHint;
                    }
                }
                return result;
            }
            Path parent = result.getParent();
            if (parent == null) {
                return null;
            }
            return resolveMarkdownInDirectory(parent, pathHint, null);
        }
        if (Files.isDirectory(result)) {
            return resolveMarkdownInDirectory(result, pathHint, null);
        }
        return null;
    }

    private Path resolveCachedTaskMarkdown(StorageTaskCacheService.CachedTask cachedTask, String pathHint) throws IOException {
        if (cachedTask == null) {
            return null;
        }
        if (isMarkdownReadableFile(cachedTask.markdownPath)) {
            if (trimToNull(pathHint) != null && cachedTask.baseDir != null) {
                Path byHint = resolvePathHint(cachedTask.baseDir, pathHint);
                if (isMarkdownReadableFile(byHint)) {
                    return byHint;
                }
            }
            return cachedTask.markdownPath.toAbsolutePath().normalize();
        }
        if (cachedTask.taskRootDir != null && Files.isDirectory(cachedTask.taskRootDir)) {
            return resolveMarkdownInDirectory(
                    cachedTask.taskRootDir.toAbsolutePath().normalize(),
                    pathHint,
                    cachedTask.resultPath
            );
        }
        return null;
    }

    private Path resolveMarkdownInDirectory(Path searchRoot, String pathHint, String preferredPath) throws IOException {
        if (searchRoot == null || !Files.isDirectory(searchRoot)) {
            return null;
        }
        Path normalizedRoot = searchRoot.toAbsolutePath().normalize();

        Path fromHint = resolvePathHint(normalizedRoot, pathHint);
        if (isMarkdownReadableFile(fromHint) && fromHint.startsWith(normalizedRoot)) {
            return fromHint;
        }

        Path fromPreferred = resolvePreferredPath(normalizedRoot, preferredPath);
        if (isMarkdownReadableFile(fromPreferred)) {
            return fromPreferred;
        }

        Path defaultMarkdown = normalizedRoot.resolve(DEFAULT_MARKDOWN_NAME).toAbsolutePath().normalize();
        if (isMarkdownReadableFile(defaultMarkdown)) {
            return defaultMarkdown;
        }

        List<Path> markdownFiles = new ArrayList<>();
        try (Stream<Path> stream = Files.walk(normalizedRoot, MARKDOWN_SCAN_DEPTH)) {
            stream.filter(Files::isRegularFile)
                    .filter(path -> isMarkdownFile(path.getFileName().toString()))
                    .filter(path -> !containsHiddenSegment(normalizedRoot, path))
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
    }

    private Path resolvePathHint(Path baseDir, String pathHint) {
        String normalizedHint = trimToNull(pathHint);
        if (baseDir == null || normalizedHint == null) {
            return null;
        }
        try {
            Path raw = Paths.get(normalizedHint);
            Path normalizedBase = baseDir.toAbsolutePath().normalize();
            Path candidate;
            if (raw.isAbsolute()) {
                candidate = raw.toAbsolutePath().normalize();
            } else {
                String safeHint = normalizedHint;
                while (safeHint.startsWith("/") || safeHint.startsWith("\\")) {
                    safeHint = safeHint.substring(1);
                }
                candidate = normalizedBase.resolve(safeHint).toAbsolutePath().normalize();
            }
            if (!candidate.startsWith(normalizedBase)) {
                return null;
            }
            return candidate;
        } catch (Exception ex) {
            return null;
        }
    }

    private Path resolvePreferredPath(Path baseDir, String preferredPath) {
        String normalized = trimToNull(preferredPath);
        if (baseDir == null || normalized == null) {
            return null;
        }
        try {
            Path preferred = Paths.get(normalized).toAbsolutePath().normalize();
            if (preferred.startsWith(baseDir) && Files.isRegularFile(preferred)) {
                return preferred;
            }
        } catch (Exception ignored) {
            // 兜底交给目录扫描。
        }
        return null;
    }

    private boolean containsHiddenSegment(Path root, Path candidate) {
        if (root == null || candidate == null) {
            return false;
        }
        Path relative;
        try {
            relative = root.toAbsolutePath().normalize()
                    .relativize(candidate.toAbsolutePath().normalize());
        } catch (Exception ex) {
            return false;
        }
        for (Path part : relative) {
            if (part.toString().startsWith(".")) {
                return true;
            }
        }
        return false;
    }

    private int markdownNamePriority(String filename) {
        if (filename == null) {
            return 100;
        }
        String lower = filename.toLowerCase(Locale.ROOT);
        if ("enhanced_output.md".equals(lower)) return 0;
        if ("enhanced_output2.md".equals(lower)) return 1;
        if ("output.md".equals(lower)) return 2;
        return 10;
    }

    private long safeLastModifiedMillis(Path path) {
        try {
            return Files.getLastModifiedTime(path).toMillis();
        } catch (Exception ex) {
            return 0L;
        }
    }

    private boolean isMarkdownReadableFile(Path path) {
        return path != null
                && Files.isRegularFile(path)
                && isMarkdownFile(path.getFileName().toString());
    }

    private boolean isMarkdownFile(String filename) {
        if (filename == null) {
            return false;
        }
        String lower = filename.toLowerCase(Locale.ROOT);
        return lower.endsWith(".md") || lower.endsWith(".markdown");
    }

    private String resolveStorageKey(String taskId) {
        String normalizedTaskId = trimToNull(taskId);
        if (normalizedTaskId == null) {
            return null;
        }
        if (normalizedTaskId.startsWith(STORAGE_TASK_PREFIX)) {
            String key = normalizedTaskId.substring(STORAGE_TASK_PREFIX.length());
            return isSafeStorageKey(key) ? key : null;
        }
        return isSafeStorageKey(normalizedTaskId) ? normalizedTaskId : null;
    }

    private boolean isSafeStorageKey(String storageKey) {
        if (storageKey == null || storageKey.isBlank()) {
            return false;
        }
        return !storageKey.contains("..")
                && !storageKey.contains("/")
                && !storageKey.contains("\\");
    }

    private String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private String firstNonBlank(String... values) {
        if (values == null || values.length == 0) {
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

    private String trimToMax(String value, int maxChars) {
        if (value == null) {
            return "";
        }
        String normalized = value.trim();
        if (normalized.length() <= maxChars) {
            return normalized;
        }
        return normalized.substring(0, maxChars).trim();
    }

    private String trimToEmpty(String value) {
        return value == null ? "" : value.trim();
    }

    public static class TelemetryIngestRequest {
        public String userId;
        public String taskId;
        public String path;
        public String flushReason;
        public Long batchSeq;
        public Integer batchSize;
        public List<TelemetryEventItem> events;
    }

    public static class TelemetryEventItem {
        public String nodeId;
        public String eventType;
        public Double relevanceScore;
        public Long timestampMs;
        public Map<String, Object> payload;
    }

    private static class TokenContextResolution {
        private final boolean found;
        private final String focusText;
        private final String focusSource;
        private final String tokenType;
        private final String localContext;

        private TokenContextResolution(
                boolean found,
                String focusText,
                String focusSource,
                String tokenType,
                String localContext
        ) {
            this.found = found;
            this.focusText = focusText != null ? focusText : "";
            this.focusSource = focusSource != null ? focusSource : "";
            this.tokenType = tokenType != null ? tokenType : "";
            this.localContext = localContext != null ? localContext : "";
        }

        private static TokenContextResolution empty() {
            return new TokenContextResolution(false, "", "", "", "");
        }
    }

    private static class MarkdownContext {
        private final Path markdownPath;
        private final List<String> lines;

        private MarkdownContext(Path markdownPath, List<String> lines) {
            this.markdownPath = markdownPath;
            this.lines = lines;
        }
    }

    private static class NodeContextResolution {
        private final boolean found;
        private final String source;
        private final Integer lineNumber;
        private final String nodeText;
        private final String markdownSnippet;

        private NodeContextResolution(
                boolean found,
                String source,
                Integer lineNumber,
                String nodeText,
                String markdownSnippet
        ) {
            this.found = found;
            this.source = source;
            this.lineNumber = lineNumber;
            this.nodeText = nodeText;
            this.markdownSnippet = markdownSnippet;
        }

        private static NodeContextResolution fromMarkdown(
                Integer lineNumber,
                String nodeText,
                String markdownSnippet
        ) {
            return new NodeContextResolution(
                    true,
                    "task_markdown",
                    lineNumber,
                    nodeText != null ? nodeText : "",
                    markdownSnippet != null ? markdownSnippet : ""
            );
        }

        private static NodeContextResolution fromPayload(String payloadContext) {
            return new NodeContextResolution(
                    true,
                    "event_payload",
                    null,
                    payloadContext != null ? payloadContext : "",
                    payloadContext != null ? payloadContext : ""
            );
        }

        private static NodeContextResolution unresolved() {
            return new NodeContextResolution(false, "unresolved", null, "", "");
        }
    }
}
