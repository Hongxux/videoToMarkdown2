package com.mvp.module2.fusion.controller;

import com.mvp.module2.fusion.queue.TaskQueueManager;
import com.mvp.module2.fusion.service.StorageTaskCacheService;
import com.mvp.module2.fusion.grpc.PythonGrpcClient;
import com.mvp.module2.fusion.service.TaskCleanupIndexService;
import com.mvp.module2.fusion.service.TaskStateRepository;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MobileMarkdownControllerDeleteTaskTest {

    @TempDir
    Path tempDir;

    @Test
    void deleteMissingTaskShouldBeIdempotent() throws Exception {
        MobileMarkdownController controller = new MobileMarkdownController();
        TaskQueueManager queueManager = new TaskQueueManager();
        injectField(queueManager, "taskStateRepository", mock(TaskStateRepository.class));
        StubStorageTaskCacheService storageCache = new StubStorageTaskCacheService();
        injectField(controller, "taskQueueManager", queueManager);
        injectField(controller, "storageTaskCacheService", storageCache);

        ResponseEntity<Map<String, Object>> response = controller.cancelRuntimeTask("VT_not_exists_001");

        assertEquals(200, response.getStatusCode().value());
        assertNotNull(response.getBody());
        assertEquals(true, response.getBody().get("success"));
        assertEquals("ALREADY_DELETED", response.getBody().get("status"));
    }

    @Test
    void deleteRunningTaskShouldCancelTask() throws Exception {
        MobileMarkdownController controller = new MobileMarkdownController();
        TaskQueueManager queueManager = new TaskQueueManager();
        injectField(queueManager, "taskStateRepository", mock(TaskStateRepository.class));
        StubStorageTaskCacheService storageCache = new StubStorageTaskCacheService();
        injectField(controller, "taskQueueManager", queueManager);
        injectField(controller, "storageTaskCacheService", storageCache);

        TaskQueueManager.TaskEntry task = queueManager.submitTask(
                "u_delete_test",
                "https://example.com/video",
                "var/tmp-delete-test",
                TaskQueueManager.Priority.NORMAL
        );

        ResponseEntity<Map<String, Object>> response = controller.cancelRuntimeTask(task.taskId);

        assertEquals(200, response.getStatusCode().value());
        assertNotNull(response.getBody());
        assertEquals(true, response.getBody().get("success"));
        assertEquals(TaskQueueManager.TaskStatus.CANCELLED.name(), response.getBody().get("status"));
        TaskQueueManager.TaskEntry updated = queueManager.getTask(task.taskId);
        assertNotNull(updated);
        assertEquals(TaskQueueManager.TaskStatus.CANCELLED, updated.status);
    }

    @Test
    void deleteStorageTaskShouldRemoveStorageDirectory() throws Exception {
        MobileMarkdownController controller = new MobileMarkdownController();
        TaskQueueManager queueManager = new TaskQueueManager();
        injectField(queueManager, "taskStateRepository", mock(TaskStateRepository.class));
        StubStorageTaskCacheService storageCache = new StubStorageTaskCacheService();
        injectField(controller, "taskQueueManager", queueManager);
        injectField(controller, "storageTaskCacheService", storageCache);

        String taskId = "VT_delete_storage_001";
        String storageKey = "ut_delete_" + UUID.randomUUID().toString().replace("-", "");
        Path storageRoot = resolveControllerStorageRoot();
        Path targetDir = storageRoot.resolve(storageKey);
        Files.createDirectories(targetDir.resolve("nested"));
        Files.writeString(targetDir.resolve("nested").resolve("sample.txt"), "sample");

        StorageTaskCacheService.CachedTask cachedTask = new StorageTaskCacheService.CachedTask();
        cachedTask.taskId = taskId;
        cachedTask.storageKey = storageKey;
        storageCache.put(cachedTask);

        try {
            ResponseEntity<Map<String, Object>> response = controller.cancelRuntimeTask(taskId);

            assertEquals(200, response.getStatusCode().value());
            assertNotNull(response.getBody());
            assertEquals(true, response.getBody().get("success"));
            assertEquals("DELETED", response.getBody().get("status"));
            assertEquals(true, response.getBody().get("storageDeleted"));
            assertFalse(Files.exists(targetDir));
        } finally {
            if (Files.exists(targetDir)) {
                try (var pathStream = Files.walk(targetDir)) {
                    pathStream.sorted((a, b) -> b.getNameCount() - a.getNameCount())
                            .forEach(path -> {
                                try {
                                    Files.deleteIfExists(path);
                                } catch (Exception ignored) {
                                }
                            });
                }
            }
        }
    }

    @Test
    @EnabledOnOs(OS.WINDOWS)
    void deleteLockedStorageTaskShouldFallbackToDeferredCleanup() throws Exception {
        MobileMarkdownController controller = new MobileMarkdownController();
        TaskQueueManager queueManager = new TaskQueueManager();
        injectField(queueManager, "taskStateRepository", mock(TaskStateRepository.class));
        StubStorageTaskCacheService storageCache = new StubStorageTaskCacheService();
        TaskCleanupIndexService cleanupService = mock(TaskCleanupIndexService.class);
        TaskStateRepository taskStateRepository = mock(TaskStateRepository.class);
        PythonGrpcClient pythonGrpcClient = mock(PythonGrpcClient.class);
        injectField(controller, "taskQueueManager", queueManager);
        injectField(controller, "storageTaskCacheService", storageCache);
        injectField(controller, "taskCleanupIndexService", cleanupService);
        injectField(controller, "taskStateRepository", taskStateRepository);
        injectField(controller, "pythonGrpcClient", pythonGrpcClient);

        Path storageRoot = tempDir.resolve("var").resolve("storage").resolve("storage");
        Files.createDirectories(storageRoot);
        injectField(controller, "configuredStorageRoot", storageRoot.toString());

        String taskId = "VT_delete_locked_001";
        String storageKey = "ut_delete_locked_001";
        Path targetDir = storageRoot.resolve(storageKey);
        Path walPath = targetDir.resolve("intermediates").resolve("rt").resolve("runtime_state.db-wal");
        Files.createDirectories(walPath.getParent());
        Files.writeString(walPath, "wal");

        StorageTaskCacheService.CachedTask cachedTask = new StorageTaskCacheService.CachedTask();
        cachedTask.taskId = taskId;
        cachedTask.storageKey = storageKey;
        storageCache.put(cachedTask);

        when(taskStateRepository.deleteTask(taskId)).thenReturn(true);
        when(cleanupService.scheduleImmediateCleanupForTask(eq(taskId), anyString(), eq("VIDEO"))).thenReturn(true);
        PythonGrpcClient.ReleaseResourcesResult releaseResult = new PythonGrpcClient.ReleaseResourcesResult();
        releaseResult.success = true;
        releaseResult.message = "released";
        when(pythonGrpcClient.releaseCVResources(taskId)).thenReturn(releaseResult);

        try (RandomAccessFile fileHandle = new RandomAccessFile(walPath.toFile(), "rw");
             FileLock ignored = fileHandle.getChannel().lock()) {
            ResponseEntity<Map<String, Object>> response = controller.cancelRuntimeTask(taskId);

            assertEquals(200, response.getStatusCode().value());
            assertNotNull(response.getBody());
            assertEquals(true, response.getBody().get("success"));
            assertEquals("DELETED_PENDING_CLEANUP", response.getBody().get("status"));
            assertEquals(true, response.getBody().get("persistedStateDeleted"));
            assertEquals(false, response.getBody().get("storageDeleted"));
            assertEquals(true, response.getBody().get("cleanupPending"));
            verify(cleanupService).scheduleImmediateCleanupForTask(eq(taskId), anyString(), eq("VIDEO"));
            verify(pythonGrpcClient).releaseCVResources(taskId);
        }
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
        private final Map<String, String> byTaskId = new LinkedHashMap<>();

        void put(CachedTask task) {
            if (task == null || task.storageKey == null) {
                return;
            }
            byStorageKey.put(task.storageKey, task);
            if (task.taskId != null) {
                byTaskId.put(task.taskId, task.storageKey);
            }
        }

        @Override
        public Optional<CachedTask> getTaskByTaskId(String taskId) {
            String storageKey = byTaskId.get(taskId);
            if (storageKey == null) {
                return Optional.empty();
            }
            return Optional.ofNullable(byStorageKey.get(storageKey));
        }

        @Override
        public Optional<CachedTask> getTask(String storageKey) {
            return Optional.ofNullable(byStorageKey.get(storageKey));
        }

        @Override
        public void evictTaskByStorageKey(String storageKey) {
            CachedTask removed = byStorageKey.remove(storageKey);
            if (removed != null && removed.taskId != null) {
                byTaskId.remove(removed.taskId);
            }
        }

        @Override
        public void evictTaskByTaskId(String taskId) {
            String storageKey = byTaskId.remove(taskId);
            if (storageKey != null) {
                byStorageKey.remove(storageKey);
            }
        }
    }
}
