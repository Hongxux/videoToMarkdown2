package com.mvp.module2.fusion.controller;

import com.mvp.module2.fusion.queue.TaskQueueManager;
import com.mvp.module2.fusion.service.StorageTaskCacheService;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MobileMarkdownControllerTaskListDeduplicateTest {

    @Test
    void listTasksShouldDeduplicateRuntimeTaskAndStorageShadowByStorageIdentity() throws Exception {
        MobileMarkdownController controller = new MobileMarkdownController();
        TaskQueueManager queueManager = new TaskQueueManager();
        StubStorageTaskCacheService storageCache = new StubStorageTaskCacheService();
        injectField(controller, "taskQueueManager", queueManager);
        injectField(controller, "storageTaskCacheService", storageCache);

        String storageKey = "ut_shadow_runtime_task";
        Path outputDir = resolveControllerStorageRoot().resolve(storageKey);
        TaskQueueManager.TaskEntry runtimeTask = queueManager.submitTask(
                "u_task_list_dedup",
                "https://www.bilibili.com/video/BV1ABCDEF123",
                outputDir.toString(),
                TaskQueueManager.Priority.NORMAL,
                "Title A"
        );
        runtimeTask.status = TaskQueueManager.TaskStatus.PROCESSING;
        runtimeTask.progress = 0.42d;
        runtimeTask.statusMessage = "AI processing";

        StorageTaskCacheService.CachedTask cachedTask = new StorageTaskCacheService.CachedTask();
        cachedTask.storageKey = storageKey;
        cachedTask.title = storageKey;
        cachedTask.status = "COMPLETED";
        cachedTask.markdownAvailable = true;
        storageCache.put(cachedTask);

        ResponseEntity<Map<String, Object>> response = controller.listTasks(0, 0, false);

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
