package com.mvp.module2.fusion.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mvp.module2.fusion.queue.TaskQueueManager;
import com.mvp.module2.fusion.service.StorageTaskCacheService;
import com.mvp.module2.fusion.service.TaskStateRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteDataSource;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MobileMarkdownControllerResidualTaskCleanupTest {

    @TempDir
    Path tempDir;

    @Test
    void listTasksShouldNotDeleteUnknownRuntimeShadowStorageTask() throws Exception {
        MobileMarkdownController controller = new MobileMarkdownController();
        TaskQueueManager queueManager = createQueueManager("runtime-shadow.db");
        StubStorageTaskCacheService storageCache = new StubStorageTaskCacheService();
        Path storageRoot = tempDir.resolve("storage-root");
        Files.createDirectories(storageRoot);

        String videoUrl = "https://www.bilibili.com/video/BV1ABCDEF123?p=2";
        String storageKey = md5Hex("BV1ABCDEF123_2");
        Path shadowDir = storageRoot.resolve(storageKey);
        Files.createDirectories(shadowDir);
        Files.writeString(shadowDir.resolve("partial.tmp"), "shadow", StandardCharsets.UTF_8);

        TaskQueueManager.TaskEntry runtimeTask = queueManager.submitTask(
                "u_runtime_shadow",
                videoUrl,
                "./output",
                TaskQueueManager.Priority.NORMAL,
                "Title A"
        );
        runtimeTask.status = TaskQueueManager.TaskStatus.PROCESSING;
        runtimeTask.progress = 0.42d;
        runtimeTask.statusMessage = "AI processing";

        StorageTaskCacheService.CachedTask cachedTask = new StorageTaskCacheService.CachedTask();
        cachedTask.storageKey = storageKey;
        cachedTask.title = storageKey;
        cachedTask.status = "UNKNOWN";
        cachedTask.markdownAvailable = false;
        cachedTask.taskRootDir = shadowDir;
        cachedTask.createdAt = Instant.parse("2026-03-20T00:00:00Z");
        storageCache.put(cachedTask);

        injectField(controller, "taskQueueManager", queueManager);
        injectField(controller, "storageTaskCacheService", storageCache);
        injectField(controller, "configuredStorageRoot", storageRoot.toString());

        ResponseEntity<Map<String, Object>> response = controller.listTasks(0, 0, false, "full", null);

        assertEquals(200, response.getStatusCode().value());
        assertNotNull(response.getBody());
        Object tasksObject = response.getBody().get("tasks");
        assertTrue(tasksObject instanceof List<?>);
        List<?> tasks = (List<?>) tasksObject;
        assertEquals(1, tasks.size());
        assertTrue(tasks.get(0) instanceof Map<?, ?>);
        Map<?, ?> item = (Map<?, ?>) tasks.get(0);
        assertEquals(runtimeTask.taskId, item.get("taskId"));
        assertEquals("runtime", item.get("source"));
        assertTrue(storageCache.getTask(storageKey).isPresent());
        assertTrue(Files.exists(shadowDir));
    }

    @Test
    void listTasksShouldDeleteUnknownResidualStorageTaskAndHideIt() throws Exception {
        MobileMarkdownController controller = new MobileMarkdownController();
        TaskQueueManager queueManager = createQueueManager("unknown-residual.db");
        StubStorageTaskCacheService storageCache = new StubStorageTaskCacheService();
        Path storageRoot = tempDir.resolve("storage-root");
        Files.createDirectories(storageRoot);
        Path residualDir = storageRoot.resolve("orphan_residual_001");
        Files.createDirectories(residualDir);
        Files.writeString(residualDir.resolve("partial.tmp"), "residual", StandardCharsets.UTF_8);

        StorageTaskCacheService.CachedTask cachedTask = new StorageTaskCacheService.CachedTask();
        cachedTask.storageKey = "orphan_residual_001";
        cachedTask.title = "orphan_residual_001";
        cachedTask.status = "UNKNOWN";
        cachedTask.markdownAvailable = false;
        cachedTask.taskRootDir = residualDir;
        cachedTask.createdAt = Instant.parse("2026-03-20T00:00:00Z");
        storageCache.put(cachedTask);

        injectField(controller, "taskQueueManager", queueManager);
        injectField(controller, "storageTaskCacheService", storageCache);
        injectField(controller, "configuredStorageRoot", storageRoot.toString());

        ResponseEntity<Map<String, Object>> response = controller.listTasks(0, 0, false, "full", null);

        assertEquals(200, response.getStatusCode().value());
        assertNotNull(response.getBody());
        Object tasksObject = response.getBody().get("tasks");
        assertTrue(tasksObject instanceof List<?>);
        assertTrue(((List<?>) tasksObject).isEmpty());
        assertEquals(0, response.getBody().get("totalCount"));
        assertTrue(Files.notExists(residualDir));
        assertTrue(storageCache.getTask("orphan_residual_001").isEmpty());
    }

    private TaskQueueManager createQueueManager(String dbFileName) throws Exception {
        TaskQueueManager queueManager = new TaskQueueManager();
        injectField(queueManager, "taskStateRepository", createTaskStateRepository(dbFileName));
        return queueManager;
    }

    private TaskStateRepository createTaskStateRepository(String dbFileName) throws Exception {
        Path dbPath = tempDir.resolve(dbFileName);
        Files.createDirectories(dbPath.getParent());
        SQLiteConfig config = new SQLiteConfig();
        config.setBusyTimeout(5000);
        config.setJournalMode(SQLiteConfig.JournalMode.WAL);
        SQLiteDataSource dataSource = new SQLiteDataSource(config);
        dataSource.setUrl("jdbc:sqlite:" + dbPath.toAbsolutePath().normalize());

        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS task_runtime_state (
                    task_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    video_url TEXT NOT NULL,
                    normalized_video_key TEXT,
                    title TEXT,
                    output_dir TEXT,
                    priority TEXT NOT NULL,
                    status TEXT NOT NULL,
                    progress REAL NOT NULL DEFAULT 0,
                    status_message TEXT,
                    user_message TEXT,
                    result_path TEXT,
                    cleanup_source_path TEXT,
                    error_message TEXT,
                    duplicate_of_task_id TEXT,
                    book_options_json TEXT,
                    probe_payload_json TEXT,
                    recovery_payload_json TEXT,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    completed_at TEXT,
                    updated_at TEXT NOT NULL
                )
                """);
        return new TaskStateRepository(jdbcTemplate, new ObjectMapper());
    }

    private static String md5Hex(String value) throws Exception {
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] digest = md.digest(String.valueOf(value).getBytes(StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder();
        for (byte one : digest) {
            sb.append(String.format(Locale.ROOT, "%02x", one));
        }
        return sb.toString();
    }

    private static void injectField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static class StubStorageTaskCacheService extends StorageTaskCacheService {
        private final Map<String, CachedTask> byStorageKey = new LinkedHashMap<>();

        void put(CachedTask task) {
            if (task == null || task.storageKey == null || task.storageKey.isBlank()) {
                return;
            }
            byStorageKey.put(task.storageKey, task);
        }

        @Override
        public PagedResult getTasks(int page, int pageSize) {
            return new PagedResult(List.copyOf(byStorageKey.values()), byStorageKey.size(), page, pageSize, false);
        }

        @Override
        public Optional<CachedTask> getTask(String storageKey) {
            return Optional.ofNullable(byStorageKey.get(storageKey));
        }

        @Override
        public Optional<CachedTask> getTaskByTaskId(String taskId) {
            String normalizedTaskId = taskId != null ? taskId.trim() : "";
            if (normalizedTaskId.isEmpty()) {
                return Optional.empty();
            }
            return byStorageKey.values().stream()
                    .filter(task -> task != null && normalizedTaskId.equals(task.taskId))
                    .findFirst();
        }

        @Override
        public void evictTaskByStorageKey(String storageKey) {
            if (storageKey == null || storageKey.isBlank()) {
                return;
            }
            byStorageKey.remove(storageKey);
        }

        @Override
        public void evictTaskByTaskId(String taskId) {
            String normalizedTaskId = taskId != null ? taskId.trim() : "";
            if (normalizedTaskId.isEmpty()) {
                return;
            }
            byStorageKey.entrySet().removeIf(entry -> {
                CachedTask task = entry.getValue();
                return task != null && normalizedTaskId.equals(task.taskId);
            });
        }
    }
}