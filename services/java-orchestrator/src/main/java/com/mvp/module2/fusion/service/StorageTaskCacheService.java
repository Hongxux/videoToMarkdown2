package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mvp.module2.fusion.common.TaskDisplayNameResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import java.util.Map;
import java.util.List;
import java.util.Optional;
import java.util.Locale;

/**
 * 缓存和管理存储任务的服务，解决频繁 IO 扫盘导致的性能问题。
 * 支持 TTL 缓存、轻量级增量扫描和分页查询。
 */
@Service
public class StorageTaskCacheService {

    private static final Logger logger = LoggerFactory.getLogger(StorageTaskCacheService.class);
    private static final String STORAGE_TASK_PREFIX = "storage:";
    private static final String DEFAULT_MARKDOWN_NAME = "enhanced_output.md";
    private static final int MARKDOWN_SCAN_DEPTH = 4;
    private static final long CACHE_TTL_MS = 60_000L; // 60秒缓存

    @Value("${task.storage.root:}")
    private String configuredStorageRoot;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private VideoMetaService videoMetaService = new VideoMetaService();
    
    // 缓存数据
    private final Map<String, CachedTask> taskCache = new ConcurrentHashMap<>();
    private final Map<String, String> taskIdToStorageKey = new ConcurrentHashMap<>();
    private final AtomicLong lastRefreshTime = new AtomicLong(0);
    private final AtomicBoolean isRefeshing = new AtomicBoolean(false);
    private Path resolvedStorageRoot;

    @PostConstruct
    public void init() {
        resolvedStorageRoot = resolveStorageRoot();
        refreshCacheAsync(); // 启动时立即预热
    }

    /**
     * 获取历史任务列表（支持分页）
     */
    @Autowired(required = false)
    public void setVideoMetaService(VideoMetaService videoMetaService) {
        if (videoMetaService != null) {
            this.videoMetaService = videoMetaService;
        }
    }

    /**
     * 获取历史任务列表（支持分页）
     */
    public PagedResult getTasks(int page, int pageSize) {
        long now = System.currentTimeMillis();
        if (now - lastRefreshTime.get() > CACHE_TTL_MS) {
            refreshCacheAsync();
        }

        List<CachedTask> allTasks = new ArrayList<>(taskCache.values());
        // 按时间倒序
        allTasks.sort(Comparator.comparing(CachedTask::getSortTimestamp).reversed());

        int normalizedPage = Math.max(0, page);
        if (pageSize <= 0) {
            return new PagedResult(allTasks, allTasks.size(), normalizedPage, pageSize, false);
        }

        int total = allTasks.size();
        int start = Math.max(0, normalizedPage * pageSize);
        int end = Math.min(total, start + pageSize);

        List<CachedTask> pageContent;
        if (start >= total) {
            pageContent = Collections.emptyList();
        } else {
            pageContent = allTasks.subList(start, end);
        }

        return new PagedResult(pageContent, total, normalizedPage, pageSize);
    }

    /**
     * 根据 ID 获取单个任务视图
     */
    public Optional<CachedTask> getTask(String storageKey) {
        return Optional.ofNullable(taskCache.get(storageKey));
    }

    /**
     * 根据 taskId 获取单个任务视图（用于 taskId 与存储目录名不一致的场景）
     */
    public Optional<CachedTask> getTaskByTaskId(String taskId) {
        String normalizedTaskId = trimToNull(taskId);
        if (normalizedTaskId == null) {
            return Optional.empty();
        }

        String indexedStorageKey = taskIdToStorageKey.get(normalizedTaskId);
        if (indexedStorageKey != null) {
            CachedTask indexedTask = taskCache.get(indexedStorageKey);
            if (indexedTask != null) {
                return Optional.of(indexedTask);
            }
            taskIdToStorageKey.remove(normalizedTaskId);
        }

        for (CachedTask task : taskCache.values()) {
            if (task == null) {
                continue;
            }
            String cachedTaskId = trimToNull(task.taskId);
            if (cachedTaskId == null || !cachedTaskId.equals(normalizedTaskId)) {
                continue;
            }
            String storageKey = trimToNull(task.storageKey);
            if (storageKey != null) {
                taskIdToStorageKey.put(normalizedTaskId, storageKey);
            }
            return Optional.of(task);
        }
        return Optional.empty();
    }

    public void evictTaskByStorageKey(String storageKey) {
        String normalizedStorageKey = trimToNull(storageKey);
        if (normalizedStorageKey == null) {
            return;
        }
        CachedTask removed = taskCache.remove(normalizedStorageKey);
        if (removed != null) {
            String removedTaskId = trimToNull(removed.taskId);
            if (removedTaskId != null) {
                taskIdToStorageKey.remove(removedTaskId, normalizedStorageKey);
            }
            return;
        }
        taskIdToStorageKey.entrySet().removeIf(entry -> normalizedStorageKey.equals(entry.getValue()));
    }

    public void evictTaskByTaskId(String taskId) {
        String normalizedTaskId = trimToNull(taskId);
        if (normalizedTaskId == null) {
            return;
        }
        String indexedStorageKey = taskIdToStorageKey.remove(normalizedTaskId);
        if (indexedStorageKey != null) {
            taskCache.remove(indexedStorageKey);
        }
        taskCache.entrySet().removeIf(entry -> {
            CachedTask cachedTask = entry.getValue();
            if (cachedTask == null) {
                return false;
            }
            String cachedTaskId = trimToNull(cachedTask.taskId);
            return normalizedTaskId.equals(cachedTaskId);
        });
    }

    /**
     * 根据路径解析存储键（用于通过 path 查找任务的场景）
     */
    public Optional<CachedTask> findTaskByPath(Path path) {
        if (path == null || !path.startsWith(resolvedStorageRoot)) {
            return Optional.empty();
        }
        Path relative = resolvedStorageRoot.relativize(path);
        if (relative.getNameCount() == 0) return Optional.empty();
        
        String key = relative.getName(0).toString();
        return getTask(key);
    }
    
    public Path getStorageRoot() {
        return resolvedStorageRoot;
    }

    /**
     * 异步刷新缓存
     */
    @Scheduled(fixedDelay = 60000) // 兜底定时刷新
    public void refreshCacheAsync() {
        if (!isRefeshing.compareAndSet(false, true)) {
            return;
        }
        new Thread(() -> {
            try {
                doRefresh();
            } catch (Exception e) {
                logger.error("刷新存储任务缓存失败", e);
            } finally {
                isRefeshing.set(false);
            }
        }, "StorageCacheRefresh").start();
    }

    private void doRefresh() {
        if (resolvedStorageRoot == null || !Files.exists(resolvedStorageRoot)) {
            taskCache.clear();
            lastRefreshTime.set(System.currentTimeMillis());
            return;
        }

        try (Stream<Path> stream = Files.list(resolvedStorageRoot)) {
            stream.filter(Files::isDirectory)
                  .filter(path -> !path.getFileName().toString().startsWith("."))
                  .forEach(this::updateTaskCache);
        } catch (IOException e) {
            logger.warn("扫描存储目录失败: {}", e.getMessage());
        }
        
        // 清理已删除的目录
        taskCache.keySet().removeIf(key -> !Files.exists(resolvedStorageRoot.resolve(key)));
        rebuildTaskIdIndex();
        
        lastRefreshTime.set(System.currentTimeMillis());
        logger.debug("存储任务缓存已刷新，当前任务数: {}", taskCache.size());
    }

    private void updateTaskCache(Path taskDir) {
        String key = taskDir.getFileName().toString();
        if (!isSafeStorageKey(key)) return;

        long dirModified = readLastModified(taskDir);
        CachedTask existing = taskCache.get(key);

        // 如果缓存存在且目录修改时间未变，跳过重读
        if (existing != null && existing.dirLastModified == dirModified) {
            return;
        }

        // 读取元数据和 Markdown
        StorageMetadata metadata = readStorageMetadata(taskDir);
        ResolvedMarkdown resolved = resolveMarkdownInDirectorySafe(taskDir, metadata.resultMarkdownPath);

        CachedTask newTask = new CachedTask();
        newTask.storageKey = key;
        newTask.taskId = metadata.taskId;
        newTask.taskRootDir = taskDir;
        newTask.title = deriveStorageTitle(taskDir, metadata);
        String sourceVideoUrl = firstNonBlank(metadata.inputVideoUrl, metadata.videoPath);
        newTask.videoUrl = sourceVideoUrl != null ? sourceVideoUrl : "";
        newTask.createdAt = metadata.generatedAt != null ? metadata.generatedAt : Instant.ofEpochMilli(dirModified);
        newTask.completedAt = metadata.generatedAt;
        newTask.dirLastModified = dirModified;
        newTask.bookTitle = metadata.bookTitle;
        newTask.bookLeafTitle = metadata.bookLeafTitle;
        newTask.bookLeafOutlineIndex = metadata.bookLeafOutlineIndex;
        
        if (metadata.hasSuccessFlag) {
            newTask.status = metadata.success ? "COMPLETED" : "FAILED";
            newTask.statusMessage = metadata.success ? "历史任务完成" : "任务失败";
        } else {
            newTask.status = resolved != null ? "COMPLETED" : "UNKNOWN";
            newTask.statusMessage = resolved != null ? "历史任务可查看" : "未检测到 markdown";
        }
        
        if (resolved != null) {
            newTask.markdownAvailable = true;
            newTask.markdownPath = resolved.markdownPath;
            newTask.baseDir = resolved.baseDir;
            newTask.resultPath = resolved.markdownPath.toString();
            newTask.progress = 1.0;
        } else {
            newTask.progress = 0.0;
            if (metadata.resultMarkdownPath != null) {
                newTask.resultPath = metadata.resultMarkdownPath;
            }
        }
        
        taskCache.put(key, newTask);
    }

    private void rebuildTaskIdIndex() {
        taskIdToStorageKey.clear();
        for (CachedTask task : taskCache.values()) {
            if (task == null) {
                continue;
            }
            String taskId = trimToNull(task.taskId);
            String storageKey = trimToNull(task.storageKey);
            if (taskId != null && storageKey != null) {
                taskIdToStorageKey.put(taskId, storageKey);
            }
        }
    }
    
    // --- 辅助逻辑 (从 Controller 迁移并优化) ---

    private Path resolveStorageRoot() {
        if (configuredStorageRoot != null && !configuredStorageRoot.isBlank()) {
            return Paths.get(configuredStorageRoot).toAbsolutePath().normalize();
        }
        // 回退逻辑
        Path current = Paths.get(System.getProperty("user.dir")).toAbsolutePath().normalize();
        for (int i = 0; i < 8; i++) {
            Path candidate = current.resolve("var/storage/storage");
            if (Files.isDirectory(candidate)) {
                return candidate.toAbsolutePath().normalize();
            }
            Path parent = current.getParent();
            if (parent == null) break;
            current = parent;
        }
        return Paths.get("var/storage/storage").toAbsolutePath().normalize();
    }

    private boolean isSafeStorageKey(String key) {
        return key != null && !key.isBlank() && !key.contains("..") && !key.contains("/") && !key.contains("\\");
    }

    private long readLastModified(Path path) {
        try {
            return Files.getLastModifiedTime(path).toMillis();
        } catch (Exception e) {
            return 0L;
        }
    }

    private StorageMetadata readStorageMetadata(Path taskDir) {
        StorageMetadata metadata = new StorageMetadata();
        metadata.videoTitle = videoMetaService.readTitle(taskDir);
        Path metricsPath = taskDir.resolve("intermediates/task_metrics_latest.json");
        if (Files.isRegularFile(metricsPath)) {
            try {
                JsonNode root = objectMapper.readTree(metricsPath.toFile());
                if (root.has("success")) {
                    metadata.hasSuccessFlag = true;
                    metadata.success = root.get("success").asBoolean(false);
                }
                if (root.has("generated_at")) metadata.generatedAt = parseInstant(root.get("generated_at").asText());
                metadata.inputVideoUrl = resolveInputVideoUrl(root);
                metadata.videoTitle = firstNonBlank(
                        metadata.videoTitle,
                        jsonText(root, "video_title"),
                        jsonText(root, "document_title"),
                        jsonText(root, "title")
                );
                metadata.taskId = firstNonBlank(
                        jsonText(root, "task_id"),
                        jsonText(root, "taskId")
                );
                if (root.has("video_path")) metadata.videoPath = trimToNull(root.get("video_path").asText());
                if (root.has("result_markdown_path")) metadata.resultMarkdownPath = trimToNull(root.get("result_markdown_path").asText());
                JsonNode flowFlags = root.path("flow_flags");
                metadata.bookTitle = firstNonBlank(
                        jsonText(flowFlags, "book_title"),
                        jsonText(root, "book_title")
                );
                metadata.bookLeafTitle = firstNonBlank(
                        jsonText(flowFlags, "book_leaf_title"),
                        jsonText(root, "book_leaf_title")
                );
                metadata.bookLeafOutlineIndex = firstNonBlank(
                        jsonText(flowFlags, "book_leaf_outline_index"),
                        jsonText(root, "book_leaf_outline_index")
                );
            } catch (Exception e) {
                // ignore
            }
        }
        return metadata;
    }

    private ResolvedMarkdown resolveMarkdownInDirectorySafe(Path searchRoot, String preferredPath) {
        try {
            if (preferredPath != null && !preferredPath.isBlank()) {
                Path p = Paths.get(preferredPath).toAbsolutePath();
                if (Files.isRegularFile(p) && p.startsWith(searchRoot)) {
                    return new ResolvedMarkdown(p, p.getParent());
                }
            }
            
            Path defaultMd = searchRoot.resolve(DEFAULT_MARKDOWN_NAME);
            if (Files.isRegularFile(defaultMd)) {
                Path p = defaultMd.toAbsolutePath();
                return new ResolvedMarkdown(p, p.getParent());
            }

            try (Stream<Path> s = Files.walk(searchRoot, MARKDOWN_SCAN_DEPTH)) {
                return s.filter(Files::isRegularFile)
                        .filter(p -> p.toString().toLowerCase(Locale.ROOT).endsWith(".md"))
                        .filter(p -> !p.toString().contains("/.")) // 简单排除隐藏目录
                        .sorted(Comparator.comparingInt(this::markdownPriority))
                        .findFirst()
                        .map(p -> {
                            Path abs = p.toAbsolutePath().normalize();
                            return new ResolvedMarkdown(abs, abs.getParent());
                        })
                        .orElse(null);
            }
        } catch (Exception e) {
            return null;
        }
    }

    private int markdownPriority(Path p) {
        String name = p.getFileName().toString().toLowerCase(Locale.ROOT);
        if (name.equals("enhanced_output.md")) return 0;
        if (name.equals("enhanced_output2.md")) return 1;
        if (name.equals("output.md")) return 2;
        return 10;
    }

    private String deriveStorageTitle(Path taskDir, StorageMetadata meta) {
        String leafTitle = trimToNull(meta.bookLeafTitle);
        if (leafTitle != null) {
            return leafTitle;
        }
        String metricsTitle = trimToNull(meta.videoTitle);
        if (metricsTitle != null) {
            return metricsTitle;
        }
        String sourceVideoUrl = firstNonBlank(meta.inputVideoUrl, meta.videoPath);
        return TaskDisplayNameResolver.resolveTaskDisplayTitle(sourceVideoUrl, taskDir.getFileName().toString());
    }

    private String resolveInputVideoUrl(JsonNode root) {
        if (root == null) {
            return null;
        }
        String direct = jsonText(root, "input_video_url");
        if (direct != null) {
            return direct;
        }
        // 兼容历史写法，避免旧任务因字段差异回退到目录名映射。
        return firstNonBlank(
                jsonText(root, "video_url"),
                jsonText(root, "source_url"),
                jsonText(root, "original_video_url")
        );
    }

    private String jsonText(JsonNode root, String field) {
        if (root == null || field == null || field.isBlank() || !root.has(field)) {
            return null;
        }
        return trimToNull(root.get(field).asText());
    }

    private String firstNonBlank(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            String normalized = trimToNull(value);
            if (normalized != null) {
                return normalized;
            }
        }
        return null;
    }
    
    private Instant parseInstant(String s) {
        try { return Instant.parse(s); } catch (Exception e) { return null; }
    }
    
    private String trimToNull(String s) {
        return (s == null || s.isBlank()) ? null : s.trim();
    }

    // --- 内部数据类 ---
    
    public static class CachedTask {
        public String storageKey;
        public String taskId;
        public String title;
        public String videoUrl;
        public String bookTitle;
        public String bookLeafTitle;
        public String bookLeafOutlineIndex;
        public String status;
        public String statusMessage;
        public Instant createdAt;
        public Instant completedAt;
        public String resultPath;
        public boolean markdownAvailable;
        public Path markdownPath;
        public Path baseDir;
        public Path taskRootDir;
        public double progress;
        public long dirLastModified;
        
        public long getSortTimestamp() {
            if (createdAt != null) return createdAt.toEpochMilli();
            if (completedAt != null) return completedAt.toEpochMilli();
            return dirLastModified;
        }
    }
    
    public static class PagedResult {
        public List<CachedTask> tasks;
        public int totalCount;
        public int page;
        public int pageSize;
        public boolean hasMore;
        
        public PagedResult(List<CachedTask> tasks, int total, int page, int pageSize) {
            this(tasks, total, page, pageSize, pageSize > 0 && (page + 1) * pageSize < total);
        }

        public PagedResult(List<CachedTask> tasks, int total, int page, int pageSize, boolean hasMore) {
            this.tasks = tasks;
            this.totalCount = total;
            this.page = page;
            this.pageSize = pageSize;
            this.hasMore = hasMore;
        }
    }
    
    private static class StorageMetadata {
        String taskId;
        boolean hasSuccessFlag;
        boolean success;
        Instant generatedAt;
        String inputVideoUrl;
        String videoTitle;
        String videoPath;
        String resultMarkdownPath;
        String bookTitle;
        String bookLeafTitle;
        String bookLeafOutlineIndex;
    }
    
    private static class ResolvedMarkdown {
        Path markdownPath;
        Path baseDir;
        ResolvedMarkdown(Path m, Path b) { markdownPath = m; baseDir = b; }
    }
}
