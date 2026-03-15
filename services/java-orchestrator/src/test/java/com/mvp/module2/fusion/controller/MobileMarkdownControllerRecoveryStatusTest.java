package com.mvp.module2.fusion.controller;

import com.mvp.module2.fusion.queue.TaskQueueManager;
import com.mvp.module2.fusion.queue.TaskQueueManager.TaskEntry;
import com.mvp.module2.fusion.service.StorageTaskCacheService;
import com.mvp.module2.fusion.service.TaskStateRepository;
import com.mvp.module2.fusion.websocket.TaskWebSocketHandler;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MobileMarkdownControllerRecoveryStatusTest {

    @Test
    void listTasksShouldExposeBlockedRecoveryFields() throws Exception {
        MobileMarkdownController controller = new MobileMarkdownController();
        TaskQueueManager queueManager = newQueueManager();
        StubStorageTaskCacheService storageCache = new StubStorageTaskCacheService();
        injectField(controller, "taskQueueManager", queueManager);
        injectField(controller, "storageTaskCacheService", storageCache);

        TaskEntry task = queueManager.submitTask(
                "u_mobile_recovery_list",
                "https://example.com/video-recovery-list",
                "./output/mobile-recovery-list",
                TaskQueueManager.Priority.NORMAL,
                "Recovery Demo"
        );
        task.status = TaskQueueManager.TaskStatus.MANUAL_RETRY_REQUIRED;
        task.progress = 0.68d;
        task.statusMessage = "[phase2b/llm_call_commit_pending] repair llm quota and retry";
        task.recoveryPayload = new LinkedHashMap<>();
        task.recoveryPayload.put("stage", "phase2b");
        task.recoveryPayload.put("checkpoint", "llm_call_commit_pending");
        task.recoveryPayload.put("retryMode", "MANUAL_RETRY");
        task.recoveryPayload.put("requiredAction", "repair llm quota and retry");
        task.recoveryPayload.put("retryEntryPoint", "phase2b/chunk-42");
        task.recoveryPayload.put("retryStrategy", "resume_from_checkpoint");
        task.recoveryPayload.put("operatorAction", "requeue_after_quota_fix");
        task.recoveryPayload.put("actionHint", "restore quota first");

        ResponseEntity<Map<String, Object>> listResponse = controller.listTasks(0, 0, false, "full", null);

        assertEquals(200, listResponse.getStatusCode().value());
        assertNotNull(listResponse.getBody());
        Object tasksObject = listResponse.getBody().get("tasks");
        assertTrue(tasksObject instanceof List<?>);
        List<?> tasks = (List<?>) tasksObject;
        assertEquals(1, tasks.size());
        assertTrue(tasks.get(0) instanceof Map<?, ?>);
        Map<?, ?> item = (Map<?, ?>) tasks.get(0);
        assertEquals(TaskQueueManager.TaskStatus.MANUAL_RETRY_REQUIRED.name(), item.get("status"));
        assertEquals(true, item.get("blocked"));
        assertEquals("blocked", item.get("statusCategory"));
        assertEquals(0.68d, ((Number) item.get("progress")).doubleValue(), 0.0001d);
        assertEquals("phase2b", item.get("recoveryStage"));
        assertEquals("llm_call_commit_pending", item.get("recoveryCheckpoint"));
        assertEquals("phase2b/chunk-42", item.get("retryEntryPoint"));
        assertEquals("repair llm quota and retry", item.get("requiredAction"));

        ResponseEntity<?> detailResponse = controller.getTaskRuntimeStatus(task.taskId);
        assertEquals(200, detailResponse.getStatusCode().value());
        assertTrue(detailResponse.getBody() instanceof Map<?, ?>);
        Map<?, ?> detail = (Map<?, ?>) detailResponse.getBody();
        assertEquals(true, detail.get("blocked"));
        assertEquals("blocked", detail.get("statusCategory"));
        assertEquals("phase2b", detail.get("recoveryStage"));
        assertEquals("llm_call_commit_pending", detail.get("recoveryCheckpoint"));
    }

    @Test
    void retryRuntimeTaskShouldRequeueBlockedTask() throws Exception {
        MobileMarkdownController controller = new MobileMarkdownController();
        TaskQueueManager queueManager = newQueueManager();
        StubStorageTaskCacheService storageCache = new StubStorageTaskCacheService();
        TaskWebSocketHandler taskWebSocketHandler = mock(TaskWebSocketHandler.class);
        injectField(controller, "taskQueueManager", queueManager);
        injectField(controller, "storageTaskCacheService", storageCache);
        injectField(controller, "taskWebSocketHandler", taskWebSocketHandler);

        TaskEntry task = queueManager.submitTask(
                "u_mobile_recovery_retry",
                "https://example.com/video-recovery-retry",
                "./output/mobile-recovery-retry",
                TaskQueueManager.Priority.NORMAL,
                "Retry Demo"
        );
        task.status = TaskQueueManager.TaskStatus.MANUAL_RETRY_REQUIRED;
        task.completedAt = java.time.Instant.now();
        task.errorMessage = "quota exhausted";
        task.recoveryPayload = new LinkedHashMap<>();
        task.recoveryPayload.put("stage", "phase2b");
        task.recoveryPayload.put("checkpoint", "llm_call_commit_pending");

        ResponseEntity<Map<String, Object>> response = controller.retryRuntimeTask(task.taskId);

        assertEquals(200, response.getStatusCode().value());
        assertNotNull(response.getBody());
        assertEquals(true, response.getBody().get("success"));
        assertEquals(TaskQueueManager.TaskStatus.QUEUED.name(), response.getBody().get("status"));
        assertEquals("queued", response.getBody().get("statusCategory"));
        assertEquals(TaskQueueManager.TaskStatus.MANUAL_RETRY_REQUIRED.name(), response.getBody().get("previousStatus"));
        TaskEntry updatedTask = queueManager.getTask(task.taskId);
        assertNotNull(updatedTask);
        assertEquals(TaskQueueManager.TaskStatus.QUEUED, updatedTask.status);
        assertNull(updatedTask.recoveryPayload);
        verify(taskWebSocketHandler).broadcastTaskUpdate(argThat(updated ->
                updated != null
                        && task.taskId.equals(updated.taskId)
                        && updated.status == TaskQueueManager.TaskStatus.QUEUED
        ));
    }

    private static TaskQueueManager newQueueManager() throws Exception {
        TaskQueueManager queueManager = new TaskQueueManager();
        TaskStateRepository repository = mock(TaskStateRepository.class);
        doNothing().when(repository).upsertTask(any(TaskQueueManager.TaskEntry.class));
        when(repository.findTask(anyString())).thenReturn(Optional.empty());
        when(repository.listAllTasks()).thenReturn(List.of());
        injectField(queueManager, "taskStateRepository", repository);
        return queueManager;
    }

    private static void injectField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static class StubStorageTaskCacheService extends StorageTaskCacheService {
        @Override
        public PagedResult getTasks(int page, int pageSize) {
            return new PagedResult(List.of(), 0, page, pageSize, false);
        }

        @Override
        public Optional<CachedTask> getTask(String storageKey) {
            return Optional.empty();
        }
    }
}
