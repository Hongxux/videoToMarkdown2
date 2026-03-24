package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.util.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * 统一补齐 storage 目录下的自动分类结果。
 */
@Service
public class StorageTaskCategoryService {

    private static final Logger logger = LoggerFactory.getLogger(StorageTaskCategoryService.class);
    private static final Pattern JSON_BLOCK_PATTERN = Pattern.compile("(?s)\\{.*}");
    private static final String CATEGORY_FILE_NAME = "category_classification.json";
    private static final String CATEGORY_LIBRARY_FILE_NAME = "category_paths.txt";
    private static final String RESULT_JSON_FILE_NAME = "result.json";
    private static final String BOOK_SEMANTIC_UNITS_FILE_NAME = "book_semantic_units.json";

    private final StorageTaskCacheService storageTaskCacheService;
    private final CategoryClassificationResultsRepository categoryClassificationResultsRepository;
    private final CollectionRepository collectionRepository;
    private final VideoMetaService videoMetaService;
    private final BookMarkdownService bookMarkdownService;
    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;
    private final ExecutorService backfillExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread thread = new Thread(r, "storage-task-category-backfill");
        thread.setDaemon(true);
        return thread;
    });
    private final AtomicBoolean backfillRunning = new AtomicBoolean(false);

    @Autowired(required = false)
    private TaskRuntimeStageStore taskRuntimeStageStore;

    @Value("${storage.category.classifier.enabled:true}")
    private boolean categoryClassifierEnabled = true;

    @Value("${storage.category.classifier.backfill.enabled:true}")
    private boolean categoryBackfillEnabled = true;

    @Value("${storage.category.classifier.base-url:https://api.deepseek.com/v1}")
    private String classifierBaseUrl;

    @Value("${storage.category.classifier.model:deepseek-chat}")
    private String classifierModel;

    @Value("${storage.category.classifier.timeout-seconds:45}")
    private int classifierTimeoutSeconds;

    @Value("${storage.category.classifier.base-target-level:2}")
    private int baseTargetLevel;

    @Value("${storage.category.classifier.max-target-level:4}")
    private int maxTargetLevel;

    @Value("${DEEPSEEK_API_KEY:}")
    private String apiKey;

    @Value("${storage.category.classifier.prompt.system-resource:classpath:prompts/storage-category/system-zh.txt}")
    private Resource systemPromptResource;

    @Value("${storage.category.classifier.prompt.video-user-resource:classpath:prompts/storage-category/video-user-zh.txt}")
    private Resource videoUserPromptResource;

    @Value("${storage.category.classifier.prompt.book-root-user-resource:classpath:prompts/storage-category/book-root-user-zh.txt}")
    private Resource bookRootUserPromptResource;

    @Value("${storage.category.classifier.prompt.book-leaf-user-resource:classpath:prompts/storage-category/book-leaf-user-zh.txt}")
    private Resource bookLeafUserPromptResource;

    private final Map<String, String> promptTemplateCache = new ConcurrentHashMap<>();

    @Autowired
    public StorageTaskCategoryService(
            StorageTaskCacheService storageTaskCacheService,
            CategoryClassificationResultsRepository categoryClassificationResultsRepository,
            CollectionRepository collectionRepository,
            VideoMetaService videoMetaService,
            BookMarkdownService bookMarkdownService,
            ObjectMapper objectMapper
    ) {
        this.storageTaskCacheService = storageTaskCacheService;
        this.categoryClassificationResultsRepository = categoryClassificationResultsRepository;
        this.collectionRepository = collectionRepository;
        this.videoMetaService = videoMetaService != null ? videoMetaService : new VideoMetaService();
        this.bookMarkdownService = bookMarkdownService != null ? bookMarkdownService : new BookMarkdownService();
        this.objectMapper = objectMapper != null ? objectMapper : new ObjectMapper();
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(8))
                .build();
    }

    public void classifyBookTaskIfNeeded(String outputDir) {
        Path taskDir = normalizeTaskDir(outputDir);
        if (taskDir == null) {
            return;
        }
        try {
            classifyTaskDirectory(taskDir, true);
        } catch (Exception ex) {
            logger.warn("book task category classification failed: outputDir={} err={}", outputDir, ex.getMessage());
        }
    }

    public void triggerBackfillAsync() {
        if (!categoryBackfillEnabled || !categoryClassifierEnabled) {
            return;
        }
        if (!backfillRunning.compareAndSet(false, true)) {
            return;
        }
        backfillExecutor.submit(() -> {
            try {
                backfillMissingClassificationsInternal();
            } finally {
                backfillRunning.set(false);
            }
        });
    }

    @Scheduled(
            initialDelayString = "${storage.category.classifier.backfill.initial-delay-ms:15000}",
            fixedDelayString = "${storage.category.classifier.backfill.fixed-delay-ms:300000}"
    )
    public void scheduleBackfillMissingClassifications() {
        triggerBackfillAsync();
    }

    private void backfillMissingClassificationsInternal() {
        Path storageRoot = storageTaskCacheService.getStorageRoot();
        if (storageRoot == null || !Files.isDirectory(storageRoot)) {
            return;
        }
        try (Stream<Path> stream = Files.list(storageRoot)) {
            stream.filter(Files::isDirectory)
                    .sorted()
                    .forEach(taskDir -> {
                        try {
                            classifyTaskDirectory(taskDir, true);
                        } catch (Exception ex) {
                            logger.warn("storage task category backfill failed: taskDir={} err={}", taskDir, ex.getMessage());
                        }
                    });
        } catch (Exception ex) {
            logger.warn("scan storage task directory for category backfill failed: {}", ex.getMessage());
        }
    }

    private boolean classifyTaskDirectory(Path taskDir, boolean allowRemoteClassification) throws Exception {
        Path normalizedTaskDir = normalizeTaskDir(taskDir != null ? taskDir.toString() : null);
        if (normalizedTaskDir == null || !Files.isDirectory(normalizedTaskDir)) {
            return false;
        }
        String taskPath = resolveTaskPath(normalizedTaskDir);
        if (taskPath.isEmpty()) {
            return false;
        }
        String taskId = resolveTaskId(normalizedTaskDir, taskPath);
        CollectionRepository.EpisodeTaskBinding collectionBinding = findCollectionBinding(taskId);
        Map<String, Path> collectionTaskDirs = collectionBinding != null
                ? resolveCollectionTaskDirs(collectionBinding.collectionId, normalizedTaskDir, taskPath)
                : Map.of();

        ExistingCategoryArtifact existingArtifact = loadExistingCategoryArtifact(normalizedTaskDir);
        if (existingArtifact != null && !existingArtifact.categoryPath().isBlank()) {
            if (collectionBinding != null && !collectionTaskDirs.isEmpty()) {
                persistCategoryDecisionToTaskDirs(
                        collectionTaskDirs,
                        existingArtifact.toDecision(),
                        "继承同合集已存在分类"
                );
            } else {
                persistCategoryDecision(normalizedTaskDir, existingArtifact.toDecision());
            }
            return true;
        }

        if (collectionBinding != null && !collectionTaskDirs.isEmpty()) {
            CategoryDecision inheritedCollectionDecision = findInheritedCollectionDecision(collectionTaskDirs);
            if (inheritedCollectionDecision != null) {
                persistCategoryDecisionToTaskDirs(
                        collectionTaskDirs,
                        inheritedCollectionDecision,
                        "继承合集分类"
                );
                return true;
            }
        }

        if (categoryClassificationResultsRepository.listAutomaticBindings().containsKey(taskPath)) {
            return false;
        }
        if (!allowRemoteClassification || !hasUsableApiKey()) {
            return false;
        }

        TaskCategoryContext context = buildTaskCategoryContext(normalizedTaskDir);
        if (context == null) {
            return false;
        }

        Set<String> knownCategories = loadKnownCategories();
        CategoryDecision finalDecision;
        if (context.collectionTask()) {
            finalDecision = classifyCollectionTask(context, knownCategories);
            persistCategoryDecisionToTaskDirs(
                    collectionTaskDirs.isEmpty() ? Map.of(taskPath, normalizedTaskDir) : collectionTaskDirs,
                    finalDecision,
                    "继承合集分类"
            );
            return true;
        } else if (context.bookTask()) {
            CategoryDecision bookRootDecision = classifyBookRoot(context, knownCategories);
            if (context.leafTask() && pathDepth(bookRootDecision.categoryPath()) < normalizeMaxTargetLevel()) {
                knownCategories.add(bookRootDecision.categoryPath());
                appendCategoryLibrary(bookRootDecision.categoryPath());
                finalDecision = classifyBookLeaf(context, knownCategories, bookRootDecision);
            } else {
                finalDecision = bookRootDecision;
            }
        } else {
            finalDecision = classifyVideoTask(context, knownCategories);
        }
        persistCategoryDecision(normalizedTaskDir, finalDecision);
        return true;
    }

    private TaskCategoryContext buildTaskCategoryContext(Path taskDir) throws Exception {
        Path metricsPath = taskDir.resolve("intermediates").resolve("task_metrics_latest.json");
        ObjectNode metricsRoot = readObjectNode(metricsPath);
        ObjectNode videoMetaRoot = videoMetaService.readOrCreateNode(taskDir);
        ObjectNode resultRoot = readPhase2bResultNode(taskDir);
        ObjectNode bookSemanticRoot = readObjectNode(taskDir.resolve(BOOK_SEMANTIC_UNITS_FILE_NAME));

        ObjectNode flowFlags = metricsRoot.path("flow_flags") instanceof ObjectNode flow
                ? flow
                : objectMapper.createObjectNode();
        boolean bookTask = booleanValue(flowFlags, "used_book_flow")
                || Files.isRegularFile(taskDir.resolve(BOOK_SEMANTIC_UNITS_FILE_NAME))
                || "book".equalsIgnoreCase(trimToEmpty(videoMetaRoot.path("contentType").asText("")));

        String taskPath = resolveTaskPath(taskDir);
        String taskId = firstNonBlank(
                trimToEmpty(metricsRoot.path("task_id").asText("")),
                lastPathSegment(taskPath)
        );
        CollectionRepository.EpisodeTaskBinding collectionBinding = findCollectionBinding(taskId);
        String title = firstNonBlank(
                trimToEmpty(videoMetaRoot.path("title").asText("")),
                trimToEmpty(metricsRoot.path("video_title").asText("")),
                taskDir.getFileName() != null ? taskDir.getFileName().toString() : ""
        );
        String sourcePath = firstNonBlank(
                trimToEmpty(metricsRoot.path("video_path").asText("")),
                trimToEmpty(metricsRoot.path("input_video_url").asText(""))
        );

        if (bookTask) {
            String bookTitle = firstNonBlank(
                    trimToEmpty(flowFlags.path("book_title").asText("")),
                    trimToEmpty(bookSemanticRoot.path("book_title").asText("")),
                    title
            );
            String leafTitle = firstNonBlank(
                    trimToEmpty(flowFlags.path("book_leaf_title").asText("")),
                    trimToEmpty(bookSemanticRoot.path("leaf_title").asText(""))
            );
            String leafOutlineIndex = firstNonBlank(
                    trimToEmpty(flowFlags.path("book_leaf_outline_index").asText("")),
                    trimToEmpty(bookSemanticRoot.path("leaf_outline_index").asText(""))
            );
            List<Map<String, Object>> bookSectionTree = readBookSectionTree(videoMetaRoot);
            BookMarkdownService.BookCategoryEvidence evidence = bookMarkdownService.buildCategoryEvidence(
                    sourcePath,
                    bookTitle,
                    bookSectionTree
            );
            if (evidence == null) {
                return null;
            }
            return new TaskCategoryContext(
                    taskId,
                    taskPath,
                    taskDir,
                    firstNonBlank(leafTitle, bookTitle, title),
                    "book",
                    true,
                    StringUtils.hasText(leafTitle) || StringUtils.hasText(leafOutlineIndex),
                    false,
                    "",
                    "",
                    evidence.bookTitle,
                    leafTitle,
                    leafOutlineIndex,
                    sourcePath,
                    "",
                    List.of(),
                    firstNonBlank(evidence.frontMatterText, ""),
                    firstNonBlank(evidence.prefaceText, ""),
                    evidence.tocTitles != null ? evidence.tocTitles : List.of()
            );
        }

        if (collectionBinding != null && StringUtils.hasText(collectionBinding.collectionTitle)) {
            return new TaskCategoryContext(
                    taskId,
                    taskPath,
                    taskDir,
                    firstNonBlank(collectionBinding.collectionTitle, title),
                    "video",
                    false,
                    false,
                    true,
                    collectionBinding.collectionId,
                    collectionBinding.collectionTitle,
                    "",
                    "",
                    "",
                    sourcePath,
                    "",
                    List.of(),
                    "",
                    "",
                    List.of()
            );
        }

        String firstUnitText = extractFirstUnitText(resultRoot);
        List<String> groupNames = extractGroupNames(resultRoot);
        if (!StringUtils.hasText(title) && !StringUtils.hasText(firstUnitText) && groupNames.isEmpty()) {
            return null;
        }
        return new TaskCategoryContext(
                taskId,
                taskPath,
                taskDir,
                title,
                "video",
                false,
                false,
                false,
                "",
                "",
                "",
                "",
                "",
                sourcePath,
                firstUnitText,
                groupNames,
                "",
                "",
                List.of()
        );
    }

    private CategoryDecision classifyVideoTask(TaskCategoryContext context, Set<String> knownCategories) throws Exception {
        int targetLevel = normalizeBaseTargetLevel();
        String prompt = applyTemplate(
                loadPromptTemplate("video_user", videoUserPromptResource),
                Map.of(
                        "target_level", String.valueOf(targetLevel),
                        "max_target_level", String.valueOf(normalizeMaxTargetLevel()),
                        "video_title", safeMultiline(context.title()),
                        "first_unit_text", safeMultiline(context.firstUnitText()),
                        "group_names", safeMultiline(String.join("\n", context.groupNames())),
                        "categories", safeMultiline(String.join("\n", knownCategories))
                )
        );
        return requestCategoryDecision(
                context,
                prompt,
                targetLevel,
                normalizeMaxTargetLevel(),
                "",
                knownCategories
        );
    }

    private CategoryDecision classifyCollectionTask(TaskCategoryContext context, Set<String> knownCategories) throws Exception {
        int targetLevel = normalizeBaseTargetLevel();
        String prompt = applyTemplate(
                loadPromptTemplate("video_user", videoUserPromptResource),
                Map.of(
                        "target_level", String.valueOf(targetLevel),
                        "max_target_level", String.valueOf(normalizeMaxTargetLevel()),
                        "video_title", safeMultiline(context.collectionTitle()),
                        "first_unit_text", "",
                        "group_names", "",
                        "categories", safeMultiline(String.join("\n", knownCategories))
                )
        );
        return requestCategoryDecision(
                context,
                prompt,
                targetLevel,
                normalizeMaxTargetLevel(),
                "",
                knownCategories
        );
    }

    private CategoryDecision classifyBookRoot(TaskCategoryContext context, Set<String> knownCategories) throws Exception {
        int targetLevel = normalizeBaseTargetLevel();
        String prompt = applyTemplate(
                loadPromptTemplate("book_root_user", bookRootUserPromptResource),
                Map.of(
                        "target_level", String.valueOf(targetLevel),
                        "max_target_level", String.valueOf(normalizeMaxTargetLevel()),
                        "book_title", safeMultiline(context.bookTitle()),
                        "front_matter_text", safeMultiline(context.frontMatterText()),
                        "preface_text", safeMultiline(context.prefaceText()),
                        "toc_titles", safeMultiline(String.join("\n", context.tocTitles())),
                        "categories", safeMultiline(String.join("\n", knownCategories))
                )
        );
        return requestCategoryDecision(
                context,
                prompt,
                targetLevel,
                normalizeMaxTargetLevel(),
                "",
                knownCategories
        );
    }

    private CategoryDecision classifyBookLeaf(
            TaskCategoryContext context,
            Set<String> knownCategories,
            CategoryDecision bookRootDecision
    ) throws Exception {
        String parentCategory = normalizeCategoryPath(bookRootDecision.categoryPath());
        int targetLevel = Math.min(normalizeMaxTargetLevel(), pathDepth(parentCategory) + 1);
        if (targetLevel <= pathDepth(parentCategory)) {
            return bookRootDecision;
        }
        String prompt = applyTemplate(
                loadPromptTemplate("book_leaf_user", bookLeafUserPromptResource),
                Map.of(
                        "parent_category", parentCategory,
                        "target_level", String.valueOf(targetLevel),
                        "max_target_level", String.valueOf(normalizeMaxTargetLevel()),
                        "book_title", safeMultiline(context.bookTitle()),
                        "front_matter_text", safeMultiline(context.frontMatterText()),
                        "preface_text", safeMultiline(context.prefaceText()),
                        "toc_titles", safeMultiline(String.join("\n", context.tocTitles())),
                        "leaf_title", safeMultiline(context.leafTitle()),
                        "leaf_outline_index", safeMultiline(context.leafOutlineIndex()),
                        "categories", safeMultiline(String.join("\n", knownCategories))
                )
        );
        return requestCategoryDecision(
                context,
                prompt,
                targetLevel,
                normalizeMaxTargetLevel(),
                parentCategory,
                knownCategories
        );
    }

    protected CategoryDecision requestCategoryDecision(
            TaskCategoryContext context,
            String userPrompt,
            int targetLevel,
            int maxLevel,
            String requiredPrefix,
            Set<String> knownCategories
    ) throws Exception {
        if (!hasUsableApiKey()) {
            throw new IllegalStateException("DEEPSEEK_API_KEY is empty");
        }
        String systemPrompt = loadPromptTemplate("system", systemPromptResource);

        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("model", firstNonBlank(classifierModel, "deepseek-chat"));
        payload.put("temperature", 0.1);
        payload.put("max_tokens", 600);
        payload.put("stream", false);
        var messagesNode = payload.putArray("messages");
        messagesNode.addObject()
                .put("role", "system")
                .put("content", systemPrompt);
        messagesNode.addObject()
                .put("role", "user")
                .put("content", userPrompt);

        String endpoint = normalizeBaseUrl(classifierBaseUrl);
        HttpRequest request = HttpRequest.newBuilder(URI.create(endpoint + "/chat/completions"))
                .timeout(Duration.ofSeconds(Math.max(10, classifierTimeoutSeconds)))
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + apiKey.trim())
                .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(payload), StandardCharsets.UTF_8))
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IllegalStateException("category classifier http " + response.statusCode());
        }
        JsonNode root = objectMapper.readTree(response.body());
        String content = firstNonBlank(
                trimToEmpty(root.path("choices").path(0).path("message").path("content").asText("")),
                trimToEmpty(root.path("choices").path(0).path("text").asText(""))
        );
        JsonNode parsed = parseDecisionJson(content);
        String categoryPath = normalizeCategoryPath(parsed.path("category_path").asText(""));
        if (categoryPath.isEmpty()) {
            throw new IllegalStateException("category_path is empty");
        }
        int depth = pathDepth(categoryPath);
        if (depth < targetLevel || depth > maxLevel) {
            throw new IllegalStateException("illegal category depth: " + categoryPath);
        }
        String normalizedPrefix = normalizeCategoryPath(requiredPrefix);
        if (!normalizedPrefix.isEmpty() && !categoryPath.startsWith(normalizedPrefix + "/")) {
            throw new IllegalStateException("category_path not under required prefix: " + categoryPath);
        }
        boolean actualIsNew = !knownCategories.contains(categoryPath);
        boolean declaredIsNew = parsed.path("is_new").asBoolean(actualIsNew);
        String reasoning = trimToEmpty(parsed.path("reasoning").asText(""));
        return new CategoryDecision(
                context.taskId(),
                context.taskPath(),
                context.title(),
                context.contentType(),
                categoryPath,
                declaredIsNew || actualIsNew,
                reasoning,
                Instant.now().toString()
        );
    }

    private JsonNode parseDecisionJson(String rawText) throws Exception {
        String text = trimToEmpty(rawText);
        if (text.isBlank()) {
            throw new IllegalStateException("empty classifier response");
        }
        List<String> candidates = new ArrayList<>();
        candidates.add(text);
        Matcher matcher = JSON_BLOCK_PATTERN.matcher(text);
        while (matcher.find()) {
            candidates.add(matcher.group());
        }
        for (String candidate : candidates) {
            try {
                JsonNode node = objectMapper.readTree(candidate);
                if (node != null && node.isObject()) {
                    return node;
                }
            } catch (Exception ignored) {
            }
        }
        throw new IllegalStateException("classifier response is not valid json");
    }

    private void persistCategoryDecision(Path taskDir, CategoryDecision decision) throws Exception {
        if (decision == null || decision.categoryPath().isBlank()) {
            return;
        }
        writeCategoryArtifact(taskDir, decision);
        updateVideoMetaCategory(taskDir, decision);
        appendCategoryLibrary(decision.categoryPath());
        categoryClassificationResultsRepository.upsertAutomaticResults(List.of(
                new CategoryClassificationResultsRepository.AutomaticCategoryResult(
                        decision.taskId(),
                        decision.taskPath(),
                        decision.title(),
                        decision.categoryPath(),
                        decision.isNew(),
                        decision.reasoning(),
                        decision.generatedAt(),
                        decision.contentType()
                )
        ));
    }

    private void writeCategoryArtifact(Path taskDir, CategoryDecision decision) throws Exception {
        ObjectNode artifact = objectMapper.createObjectNode();
        artifact.put("video_id", decision.taskId());
        artifact.put("task_path", decision.taskPath());
        artifact.put("video_title", firstNonBlank(decision.title(), ""));
        artifact.put("content_type", decision.contentType());
        artifact.put("category_path", decision.categoryPath());
        artifact.put("target_level", pathDepth(decision.categoryPath()));
        artifact.put("is_new", decision.isNew());
        artifact.put("reasoning", firstNonBlank(decision.reasoning(), ""));
        artifact.put("generated_at", firstNonBlank(decision.generatedAt(), Instant.now().toString()));
        artifact.set("usage", objectMapper.createObjectNode());

        Path artifactPath = taskDir.resolve(CATEGORY_FILE_NAME);
        Path tmpPath = taskDir.resolve(CATEGORY_FILE_NAME + ".tmp");
        Files.createDirectories(taskDir);
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(tmpPath.toFile(), artifact);
        Files.move(tmpPath, artifactPath, StandardCopyOption.REPLACE_EXISTING);
    }

    private void updateVideoMetaCategory(Path taskDir, CategoryDecision decision) throws Exception {
        ObjectNode root = videoMetaService.readOrCreateNode(taskDir);
        List<String> levels = splitCategoryPath(decision.categoryPath());
        root.put("category_path", decision.categoryPath());
        root.put("category_domain", levels.size() > 0 ? levels.get(0) : "");
        root.put("category_subdomain", levels.size() > 1 ? levels.get(1) : "");
        root.put("category_leaf", levels.isEmpty() ? "" : levels.get(levels.size() - 1));
        root.put("category_depth", levels.size());
        root.put("category_target_level", levels.size());
        root.put("category_is_new", decision.isNew());
        root.put("category_reasoning", firstNonBlank(decision.reasoning(), ""));
        root.put("category_classified_at", firstNonBlank(decision.generatedAt(), Instant.now().toString()));
        root.put("contentType", decision.contentType());

        Path metaPath = taskDir.resolve("video_meta.json");
        Path tmpPath = taskDir.resolve("video_meta.json.tmp");
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(tmpPath.toFile(), root);
        Files.move(tmpPath, metaPath, StandardCopyOption.REPLACE_EXISTING);
    }

    private void appendCategoryLibrary(String categoryPath) throws Exception {
        Path storageRoot = storageTaskCacheService.getStorageRoot();
        if (storageRoot == null) {
            return;
        }
        Path libraryPath = storageRoot.resolve(CATEGORY_LIBRARY_FILE_NAME);
        Set<String> categories = new LinkedHashSet<>();
        if (Files.isRegularFile(libraryPath)) {
            categories.addAll(Files.readAllLines(libraryPath, StandardCharsets.UTF_8));
        }
        categories.add(normalizeCategoryPath(categoryPath));
        List<String> ordered = new ArrayList<>();
        for (String category : categories) {
            String normalized = normalizeCategoryPath(category);
            if (!normalized.isBlank()) {
                ordered.add(normalized);
            }
        }
        ordered.sort(String::compareTo);
        Files.createDirectories(storageRoot);
        Files.writeString(
                libraryPath,
                ordered.isEmpty() ? "" : String.join("\n", ordered) + "\n",
                StandardCharsets.UTF_8
        );
    }

    private Set<String> loadKnownCategories() throws Exception {
        Set<String> categories = new LinkedHashSet<>();
        Path storageRoot = storageTaskCacheService.getStorageRoot();
        if (storageRoot != null) {
            Path libraryPath = storageRoot.resolve(CATEGORY_LIBRARY_FILE_NAME);
            if (Files.isRegularFile(libraryPath)) {
                for (String line : Files.readAllLines(libraryPath, StandardCharsets.UTF_8)) {
                    String normalized = normalizeCategoryPath(line);
                    if (!normalized.isBlank()) {
                        categories.add(normalized);
                    }
                }
            }
        }
        for (String categoryPath : categoryClassificationResultsRepository.listAutomaticBindings().values()) {
            String normalized = normalizeCategoryPath(categoryPath);
            if (!normalized.isBlank()) {
                categories.add(normalized);
            }
        }
        return categories;
    }

    private ExistingCategoryArtifact loadExistingCategoryArtifact(Path taskDir) throws Exception {
        Path categoryArtifactPath = taskDir.resolve(CATEGORY_FILE_NAME);
        if (Files.isRegularFile(categoryArtifactPath)) {
            ObjectNode node = readObjectNode(categoryArtifactPath);
            String categoryPath = normalizeCategoryPath(node.path("category_path").asText(""));
            if (!categoryPath.isBlank()) {
                return new ExistingCategoryArtifact(
                        firstNonBlank(trimToEmpty(node.path("video_id").asText("")), taskDir.getFileName().toString()),
                        resolveTaskPath(taskDir),
                        firstNonBlank(trimToEmpty(node.path("video_title").asText("")), trimToEmpty(node.path("title").asText(""))),
                        firstNonBlank(trimToEmpty(node.path("content_type").asText("")), detectTaskContentType(taskDir)),
                        categoryPath,
                        node.path("is_new").asBoolean(false),
                        trimToEmpty(node.path("reasoning").asText("")),
                        firstNonBlank(trimToEmpty(node.path("generated_at").asText("")), Instant.now().toString())
                );
            }
        }

        ObjectNode videoMetaRoot = videoMetaService.readOrCreateNode(taskDir);
        String metaCategoryPath = normalizeCategoryPath(videoMetaRoot.path("category_path").asText(""));
        if (!metaCategoryPath.isBlank()) {
            return new ExistingCategoryArtifact(
                    taskDir.getFileName() != null ? taskDir.getFileName().toString() : "",
                    resolveTaskPath(taskDir),
                    firstNonBlank(trimToEmpty(videoMetaRoot.path("title").asText("")), taskDir.getFileName() != null ? taskDir.getFileName().toString() : ""),
                    firstNonBlank(trimToEmpty(videoMetaRoot.path("contentType").asText("")), detectTaskContentType(taskDir)),
                    metaCategoryPath,
                    videoMetaRoot.path("category_is_new").asBoolean(false),
                    firstNonBlank(trimToEmpty(videoMetaRoot.path("category_reasoning").asText("")), "imported from video_meta.json"),
                    firstNonBlank(trimToEmpty(videoMetaRoot.path("category_classified_at").asText("")), Instant.now().toString())
            );
        }
        return null;
    }

    private String detectTaskContentType(Path taskDir) {
        return Files.isRegularFile(taskDir.resolve(BOOK_SEMANTIC_UNITS_FILE_NAME)) ? "book" : "video";
    }

    private List<Map<String, Object>> readBookSectionTree(ObjectNode videoMetaRoot) {
        JsonNode treeNode = videoMetaRoot.path("bookSectionTree");
        if (!treeNode.isArray()) {
            return List.of();
        }
        List<Map<String, Object>> result = new ArrayList<>();
        for (JsonNode item : treeNode) {
            if (item == null || !item.isObject()) {
                continue;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> value = objectMapper.convertValue(item, Map.class);
            result.add(value);
        }
        return result;
    }

    private String extractFirstUnitText(ObjectNode resultRoot) {
        JsonNode groupsNode = resultRoot.path("knowledge_groups");
        if (!groupsNode.isArray()) {
            return "";
        }
        for (JsonNode group : groupsNode) {
            JsonNode unitsNode = group.path("units");
            if (!unitsNode.isArray()) {
                continue;
            }
            for (JsonNode unit : unitsNode) {
                String text = firstNonBlank(
                        trimToEmpty(unit.path("body_text").asText("")),
                        trimToEmpty(unit.path("text").asText(""))
                );
                if (!text.isBlank()) {
                    return text.length() <= 2000 ? text : text.substring(0, 2000);
                }
            }
        }
        return "";
    }

    private List<String> extractGroupNames(ObjectNode resultRoot) {
        Set<String> values = new LinkedHashSet<>();
        JsonNode groupsNode = resultRoot.path("knowledge_groups");
        if (!groupsNode.isArray()) {
            return List.of();
        }
        for (JsonNode group : groupsNode) {
            String name = trimToEmpty(group.path("group_name").asText(""));
            if (!name.isBlank()) {
                values.add(name);
            }
        }
        return new ArrayList<>(values);
    }

    private String resolveTaskId(Path taskDir, String taskPath) throws Exception {
        ObjectNode metricsRoot = readObjectNode(taskDir.resolve("intermediates").resolve("task_metrics_latest.json"));
        return firstNonBlank(trimToEmpty(metricsRoot.path("task_id").asText("")), lastPathSegment(taskPath));
    }

    private CollectionRepository.EpisodeTaskBinding findCollectionBinding(String taskId) {
        if (collectionRepository == null || !StringUtils.hasText(taskId)) {
            return null;
        }
        Map<String, CollectionRepository.EpisodeTaskBinding> bindings =
                collectionRepository.findEpisodeBindingsByTaskIds(List.of(taskId));
        return bindings.get(taskId);
    }

    private Map<String, Path> resolveCollectionTaskDirs(String collectionId, Path currentTaskDir, String currentTaskPath) {
        if (collectionRepository == null || !StringUtils.hasText(collectionId)) {
            return Map.of();
        }
        Map<String, Path> taskDirs = new LinkedHashMap<>();
        for (CollectionRepository.EpisodeView episode : collectionRepository.listEpisodes(collectionId)) {
            if (episode == null || !StringUtils.hasText(episode.taskId)) {
                continue;
            }
            storageTaskCacheService.getTaskByTaskId(episode.taskId).ifPresent(cachedTask -> {
                Path taskDir = cachedTask.taskRootDir;
                String taskPath = resolveTaskPath(taskDir);
                if (taskDir != null && !taskPath.isBlank()) {
                    taskDirs.put(taskPath, taskDir);
                }
            });
        }
        if (currentTaskDir != null && !currentTaskPath.isBlank()) {
            taskDirs.putIfAbsent(currentTaskPath, currentTaskDir);
        }
        return taskDirs;
    }

    private CategoryDecision findInheritedCollectionDecision(Map<String, Path> collectionTaskDirs) throws Exception {
        if (collectionTaskDirs == null || collectionTaskDirs.isEmpty()) {
            return null;
        }
        Map<String, String> automaticBindings = categoryClassificationResultsRepository.listAutomaticBindings();
        for (Map.Entry<String, Path> entry : collectionTaskDirs.entrySet()) {
            String taskPath = entry.getKey();
            Path taskDir = entry.getValue();
            ExistingCategoryArtifact artifact = loadExistingCategoryArtifact(taskDir);
            if (artifact != null && !artifact.categoryPath().isBlank()) {
                return artifact.toDecision();
            }
            String categoryPath = normalizeCategoryPath(automaticBindings.get(taskPath));
            if (!categoryPath.isBlank()) {
                return new CategoryDecision(
                        lastPathSegment(taskPath),
                        taskPath,
                        lastPathSegment(taskPath),
                        detectTaskContentType(taskDir),
                        categoryPath,
                        false,
                        "继承合集已有分类",
                        Instant.now().toString()
                );
            }
        }
        return null;
    }

    private void persistCategoryDecisionToTaskDirs(
            Map<String, Path> taskDirs,
            CategoryDecision decision,
            String inheritedReasoning
    ) throws Exception {
        if (taskDirs == null || taskDirs.isEmpty() || decision == null || decision.categoryPath().isBlank()) {
            return;
        }
        List<CategoryClassificationResultsRepository.AutomaticCategoryResult> automaticResults = new ArrayList<>();
        for (Map.Entry<String, Path> entry : taskDirs.entrySet()) {
            String taskPath = entry.getKey();
            Path taskDir = entry.getValue();
            String taskId = resolveTaskId(taskDir, taskPath);
            String title = resolvePersistTitle(taskDir, decision.title());
            CategoryDecision taskDecision = new CategoryDecision(
                    taskId,
                    taskPath,
                    title,
                    detectTaskContentType(taskDir),
                    decision.categoryPath(),
                    decision.isNew(),
                    firstNonBlank(inheritedReasoning, decision.reasoning()),
                    decision.generatedAt()
            );
            writeCategoryArtifact(taskDir, taskDecision);
            updateVideoMetaCategory(taskDir, taskDecision);
            automaticResults.add(new CategoryClassificationResultsRepository.AutomaticCategoryResult(
                    taskDecision.taskId(),
                    taskDecision.taskPath(),
                    taskDecision.title(),
                    taskDecision.categoryPath(),
                    taskDecision.isNew(),
                    taskDecision.reasoning(),
                    taskDecision.generatedAt(),
                    taskDecision.contentType()
            ));
        }
        appendCategoryLibrary(decision.categoryPath());
        categoryClassificationResultsRepository.upsertAutomaticResults(automaticResults);
    }

    private String resolvePersistTitle(Path taskDir, String fallbackTitle) throws Exception {
        ObjectNode videoMetaRoot = videoMetaService.readOrCreateNode(taskDir);
        return firstNonBlank(trimToEmpty(videoMetaRoot.path("title").asText("")), fallbackTitle, taskDir.getFileName().toString());
    }

    private ObjectNode readObjectNode(Path path) throws Exception {
        if (path == null || !Files.isRegularFile(path) || Files.size(path) == 0L) {
            return objectMapper.createObjectNode();
        }
        JsonNode root = objectMapper.readTree(path.toFile());
        return root instanceof ObjectNode objectNode ? objectNode : objectMapper.createObjectNode();
    }

    private ObjectNode readPhase2bResultNode(Path taskDir) throws Exception {
        Path resultPath = taskDir.resolve(RESULT_JSON_FILE_NAME).normalize();
        if (Files.isRegularFile(resultPath) && Files.size(resultPath) > 0L) {
            return readObjectNode(resultPath);
        }
        if (taskRuntimeStageStore == null) {
            return objectMapper.createObjectNode();
        }
        Map<String, Object> artifactPayload = taskRuntimeStageStore.loadProjectionPayload(
                taskDir.toAbsolutePath().normalize().toString(),
                "phase2b",
                "result_document"
        );
        if (artifactPayload.isEmpty()) {
            return objectMapper.createObjectNode();
        }
        JsonNode root = objectMapper.valueToTree(artifactPayload);
        return root instanceof ObjectNode objectNode ? objectNode : objectMapper.createObjectNode();
    }

    private boolean hasUsableApiKey() {
        return StringUtils.hasText(apiKey);
    }

    private Path normalizeTaskDir(String outputDir) {
        if (!StringUtils.hasText(outputDir)) {
            return null;
        }
        try {
            return Path.of(outputDir).toAbsolutePath().normalize();
        } catch (Exception ignored) {
            return null;
        }
    }

    private String resolveTaskPath(Path taskDir) {
        Path storageRoot = storageTaskCacheService.getStorageRoot();
        if (taskDir == null || storageRoot == null) {
            return "";
        }
        try {
            Path normalizedTaskDir = taskDir.toAbsolutePath().normalize();
            Path normalizedStorageRoot = storageRoot.toAbsolutePath().normalize();
            if (!normalizedTaskDir.startsWith(normalizedStorageRoot)) {
                return "";
            }
            Path relative = normalizedStorageRoot.relativize(normalizedTaskDir);
            if (relative.getNameCount() <= 0) {
                return "";
            }
            return "storage/" + TaskManualCollectionRepository.normalizeTaskPath(relative.getName(0).toString());
        } catch (Exception ignored) {
            return "";
        }
    }

    private String normalizeBaseUrl(String rawBaseUrl) {
        String baseUrl = firstNonBlank(rawBaseUrl, "https://api.deepseek.com/v1").trim();
        while (baseUrl.endsWith("/")) {
            baseUrl = baseUrl.substring(0, baseUrl.length() - 1);
        }
        return baseUrl;
    }

    private int normalizeBaseTargetLevel() {
        return Math.max(2, baseTargetLevel);
    }

    private int normalizeMaxTargetLevel() {
        return Math.max(normalizeBaseTargetLevel(), maxTargetLevel);
    }

    private String normalizeCategoryPath(String value) {
        String[] parts = trimToEmpty(value).replace('\\', '/').split("/");
        List<String> kept = new ArrayList<>();
        for (String part : parts) {
            String normalized = trimToEmpty(part);
            if (!normalized.isBlank()) {
                kept.add(normalized);
            }
        }
        return String.join("/", kept);
    }

    private List<String> splitCategoryPath(String value) {
        String normalized = normalizeCategoryPath(value);
        if (normalized.isBlank()) {
            return List.of();
        }
        return List.of(normalized.split("/"));
    }

    private int pathDepth(String value) {
        return splitCategoryPath(value).size();
    }

    private String safeMultiline(String value) {
        return trimToEmpty(value);
    }

    private boolean booleanValue(ObjectNode node, String fieldName) {
        return node != null && node.has(fieldName) && node.path(fieldName).asBoolean(false);
    }

    private String trimToEmpty(String value) {
        return value == null ? "" : value.trim();
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

    private String lastPathSegment(String path) {
        String normalized = TaskManualCollectionRepository.normalizeTaskPath(path);
        if (normalized.isBlank()) {
            return "";
        }
        int slashIndex = normalized.lastIndexOf('/');
        return slashIndex >= 0 ? normalized.substring(slashIndex + 1) : normalized;
    }

    private String loadPromptTemplate(String cacheKey, Resource resource) {
        return promptTemplateCache.computeIfAbsent(cacheKey, key -> readPromptTemplate(resource, key));
    }

    private String readPromptTemplate(Resource resource, String templateName) {
        if (resource == null || !resource.exists()) {
            throw new IllegalStateException("missing category prompt template: " + templateName);
        }
        try (InputStream input = resource.getInputStream()) {
            String template = StreamUtils.copyToString(input, StandardCharsets.UTF_8).trim();
            if (StringUtils.hasText(template)) {
                return template;
            }
            throw new IllegalStateException("empty category prompt template: " + templateName);
        } catch (IOException ex) {
            throw new IllegalStateException("load category prompt template failed: " + templateName + " err=" + ex.getMessage(), ex);
        }
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

    private record ExistingCategoryArtifact(
            String taskId,
            String taskPath,
            String title,
            String contentType,
            String categoryPath,
            boolean isNew,
            String reasoning,
            String generatedAt
    ) {
        private CategoryDecision toDecision() {
            return new CategoryDecision(taskId, taskPath, title, contentType, categoryPath, isNew, reasoning, generatedAt);
        }
    }

    private record TaskCategoryContext(
            String taskId,
            String taskPath,
            Path taskDir,
            String title,
            String contentType,
            boolean bookTask,
            boolean leafTask,
            boolean collectionTask,
            String collectionId,
            String collectionTitle,
            String bookTitle,
            String leafTitle,
            String leafOutlineIndex,
            String sourcePath,
            String firstUnitText,
            List<String> groupNames,
            String frontMatterText,
            String prefaceText,
            List<String> tocTitles
    ) {
    }

    protected record CategoryDecision(
            String taskId,
            String taskPath,
            String title,
            String contentType,
            String categoryPath,
            boolean isNew,
            String reasoning,
            String generatedAt
    ) {
    }
}
