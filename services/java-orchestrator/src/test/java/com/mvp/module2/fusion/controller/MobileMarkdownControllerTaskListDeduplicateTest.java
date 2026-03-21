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
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MobileMarkdownControllerTaskListDeduplicateTest {

    @TempDir
    Path tempDir;

    @Test
    void listTasksShouldReturnEmptyListWhenNoTasksExist() throws Exception {
        MobileMarkdownController controller = new MobileMarkdownController();
        TaskQueueManager queueManager = createQueueManager("empty.db");
        StubStorageTaskCacheService storageCache = new StubStorageTaskCacheService();
        injectField(controller, "taskQueueManager", queueManager);
        injectField(controller, "storageTaskCacheService", storageCache);

        ResponseEntity<Map<String, Object>> response = controller.listTasks(0, 0, false, "full", null);

        assertEquals(200, response.getStatusCode().value());
        assertNotNull(response.getBody());
        Object tasksObject = response.getBody().get("tasks");
        assertTrue(tasksObject instanceof List<?>);
        assertTrue(((List<?>) tasksObject).isEmpty());
        assertEquals(0, response.getBody().get("totalCount"));
    }

    @Test
    void listTasksShouldDeduplicateRuntimeTaskAndPredictedStorageShadowWhileProcessing() throws Exception {
        MobileMarkdownController controller = new MobileMarkdownController();
        TaskQueueManager queueManager = createQueueManager("dedup-shadow.db");
        StubStorageTaskCacheService storageCache = new StubStorageTaskCacheService();
        injectField(controller, "taskQueueManager", queueManager);
        injectField(controller, "storageTaskCacheService", storageCache);

        String videoUrl = "https://www.bilibili.com/video/BV1ABCDEF123?p=2";
        String storageKey = md5Hex("BV1ABCDEF123_2");
        TaskQueueManager.TaskEntry runtimeTask = queueManager.submitTask(
                "u_task_list_dedup",
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
        cachedTask.taskId = null;
        cachedTask.title = storageKey;
        cachedTask.status = "UNKNOWN";
        cachedTask.markdownAvailable = false;
        storageCache.put(cachedTask);

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
        assertEquals("Title A", item.get("title"));
        assertEquals(TaskQueueManager.TaskStatus.PROCESSING.name(), item.get("status"));
        assertEquals("runtime", item.get("source"));
    }


    @Test
    void listTasksShouldKeepDifferentBookLeafTasksFromSameBook() throws Exception {
        MobileMarkdownController controller = new MobileMarkdownController();
        TaskQueueManager queueManager = createQueueManager("book-leaf.db");
        StubStorageTaskCacheService storageCache = new StubStorageTaskCacheService();
        injectField(controller, "taskQueueManager", queueManager);
        injectField(controller, "storageTaskCacheService", storageCache);

        String videoUrl = "D:/books/distributed-systems.pdf";
        Path storageRoot = resolveControllerStorageRoot();

        TaskQueueManager.BookProcessingOptions leafOne = new TaskQueueManager.BookProcessingOptions();
        leafOne.sectionSelector = "c1s1t1";
        leafOne.bookTitle = "Distributed Systems";
        leafOne.leafTitle = "Introduction";
        leafOne.leafOutlineIndex = "1.1.1";
        leafOne.storageKey = md5Hex("Introduction_1.1.1_Distributed Systems");

        TaskQueueManager.BookProcessingOptions leafTwo = new TaskQueueManager.BookProcessingOptions();
        leafTwo.sectionSelector = "c1s2t1";
        leafTwo.bookTitle = "Distributed Systems";
        leafTwo.leafTitle = "System Models";
        leafTwo.leafOutlineIndex = "1.2.1";
        leafTwo.storageKey = md5Hex("System Models_1.2.1_Distributed Systems");

        TaskQueueManager.TaskEntry firstTask = queueManager.submitTask(
                "u_book_leaf",
                videoUrl,
                storageRoot.resolve(leafOne.storageKey).toString(),
                TaskQueueManager.Priority.NORMAL,
                leafOne.leafTitle,
                leafOne
        );
        TaskQueueManager.TaskEntry secondTask = queueManager.submitTask(
                "u_book_leaf",
                videoUrl,
                storageRoot.resolve(leafTwo.storageKey).toString(),
                TaskQueueManager.Priority.NORMAL,
                leafTwo.leafTitle,
                leafTwo
        );

        ResponseEntity<Map<String, Object>> response = controller.listTasks(0, 0, false, "full", null);

        assertEquals(200, response.getStatusCode().value());
        assertNotNull(response.getBody());
        Object tasksObject = response.getBody().get("tasks");
        assertTrue(tasksObject instanceof List<?>);
        List<?> tasks = (List<?>) tasksObject;
        assertEquals(2, tasks.size());
        assertTrue(tasks.stream().anyMatch(item -> item instanceof Map<?, ?> map && firstTask.taskId.equals(map.get("taskId"))));
        assertTrue(tasks.stream().anyMatch(item -> item instanceof Map<?, ?> map && secondTask.taskId.equals(map.get("taskId"))));
    }

    @Test
    void listTasksShouldKeepFailedUnreadableTasksWhenOnlyMultiSegmentEnabled() throws Exception {
        MobileMarkdownController controller = new MobileMarkdownController();
        TaskQueueManager queueManager = createQueueManager("failed-visible.db");
        StubStorageTaskCacheService storageCache = new StubStorageTaskCacheService();
        injectField(controller, "taskQueueManager", queueManager);
        injectField(controller, "storageTaskCacheService", storageCache);

        TaskQueueManager.TaskEntry failedTask = queueManager.submitTask(
                "u_failed_visible",
                "https://www.bilibili.com/video/BV1FAILED0001?p=4",
                "./output/failed",
                TaskQueueManager.Priority.NORMAL,
                "Failed Task"
        );
        failedTask.status = TaskQueueManager.TaskStatus.FAILED;
        failedTask.statusMessage = "download failed";

        TaskQueueManager.TaskEntry unreadableCompletedTask = queueManager.submitTask(
                "u_failed_visible",
                "https://www.bilibili.com/video/BV1DONE00001?p=5",
                "./output/completed-missing",
                TaskQueueManager.Priority.NORMAL,
                "Completed Without Markdown"
        );
        unreadableCompletedTask.status = TaskQueueManager.TaskStatus.COMPLETED;
        unreadableCompletedTask.statusMessage = "done";

        ResponseEntity<Map<String, Object>> response = controller.listTasks(0, 0, true, "full", null);

        assertEquals(200, response.getStatusCode().value());
        assertNotNull(response.getBody());
        Object tasksObject = response.getBody().get("tasks");
        assertTrue(tasksObject instanceof List<?>);
        List<?> tasks = (List<?>) tasksObject;
        assertEquals(1, tasks.size());
        assertTrue(tasks.get(0) instanceof Map<?, ?>);
        Map<?, ?> item = (Map<?, ?>) tasks.get(0);
        assertEquals(failedTask.taskId, item.get("taskId"));
        assertEquals(TaskQueueManager.TaskStatus.FAILED.name(), item.get("status"));
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

    private static Path resolveControllerStorageRoot() {
        Path current = Paths.get(System.getProperty("user.dir")).toAbsolutePath().normalize();
        for (int i = 0; i < 8; i++) {
            Path candidate = current.resolve("var").resolve("storage").resolve("storage");
            if (Files.isDirectory(candidate)) {
                return candidate.toAbsolutePath().normalize();
            }
            Path parent = current.getParent();
            if (parent == null) {
                break;
            }
            current = parent;
        }
        return Paths.get("var", "storage", "storage").toAbsolutePath().normalize();
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
    }
}
