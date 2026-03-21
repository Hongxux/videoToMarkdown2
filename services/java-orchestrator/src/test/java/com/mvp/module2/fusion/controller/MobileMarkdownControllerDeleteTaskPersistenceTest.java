package com.mvp.module2.fusion.controller;

import com.mvp.module2.fusion.queue.TaskQueueManager;
import com.mvp.module2.fusion.service.StorageTaskCacheService;
import com.mvp.module2.fusion.service.TaskStateRepository;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MobileMarkdownControllerDeleteTaskPersistenceTest {

    @Test
    void deleteCompletedTaskShouldPurgePersistedRuntimeState() throws Exception {
        MobileMarkdownController controller = new MobileMarkdownController();
        TaskQueueManager queueManager = new TaskQueueManager();
        injectField(queueManager, "taskStateRepository", mock(TaskStateRepository.class));
        StorageTaskCacheService storageCache = mock(StorageTaskCacheService.class);
        TaskStateRepository taskStateRepository = mock(TaskStateRepository.class);
        injectField(controller, "taskQueueManager", queueManager);
        injectField(controller, "storageTaskCacheService", storageCache);
        injectField(controller, "taskStateRepository", taskStateRepository);
        when(taskStateRepository.deleteTask("VT_delete_persisted_001")).thenReturn(true);
        when(storageCache.getTaskByTaskId("VT_delete_persisted_001")).thenReturn(Optional.empty());
        when(storageCache.getTask("VT_delete_persisted_001")).thenReturn(Optional.empty());

        TaskQueueManager.TaskEntry task = queueManager.submitTask(
                "u_delete_persisted",
                "https://example.com/video-delete-persisted",
                "var/tmp-delete-persisted",
                TaskQueueManager.Priority.NORMAL
        );
        String originalTaskId = task.taskId;
        task.taskId = "VT_delete_persisted_001";
        task.status = TaskQueueManager.TaskStatus.COMPLETED;
        task.completedAt = java.time.Instant.now();

        queueManager.removeTask(originalTaskId);
        injectTaskIntoQueueManager(queueManager, task);

        ResponseEntity<Map<String, Object>> response = controller.cancelRuntimeTask(task.taskId);

        assertEquals(200, response.getStatusCode().value());
        assertNotNull(response.getBody());
        assertEquals(true, response.getBody().get("success"));
        assertEquals("DELETED", response.getBody().get("status"));
        assertEquals(true, response.getBody().get("runtimeRemoved"));
        assertEquals(true, response.getBody().get("persistedStateDeleted"));
        verify(taskStateRepository).deleteTask("VT_delete_persisted_001");
    }

    @Test
    void deleteTaskShouldNotReportAlreadyDeletedWhenOnlyPersistedRuntimeStateExists() throws Exception {
        MobileMarkdownController controller = new MobileMarkdownController();
        TaskQueueManager queueManager = new TaskQueueManager();
        injectField(queueManager, "taskStateRepository", mock(TaskStateRepository.class));
        StorageTaskCacheService storageCache = mock(StorageTaskCacheService.class);
        TaskStateRepository taskStateRepository = mock(TaskStateRepository.class);
        injectField(controller, "taskQueueManager", queueManager);
        injectField(controller, "storageTaskCacheService", storageCache);
        injectField(controller, "taskStateRepository", taskStateRepository);
        when(taskStateRepository.deleteTask("VT_persisted_only_001")).thenReturn(true);
        when(storageCache.getTaskByTaskId("VT_persisted_only_001")).thenReturn(Optional.empty());
        when(storageCache.getTask("VT_persisted_only_001")).thenReturn(Optional.empty());

        ResponseEntity<Map<String, Object>> response = controller.cancelRuntimeTask("VT_persisted_only_001");

        assertEquals(200, response.getStatusCode().value());
        assertNotNull(response.getBody());
        assertEquals(true, response.getBody().get("success"));
        assertEquals("DELETED", response.getBody().get("status"));
        assertEquals(false, response.getBody().get("runtimeRemoved"));
        assertEquals(false, response.getBody().get("storageDeleted"));
        assertEquals(true, response.getBody().get("persistedStateDeleted"));
        verify(taskStateRepository).deleteTask("VT_persisted_only_001");
    }

    @SuppressWarnings("unchecked")
    private static void injectTaskIntoQueueManager(TaskQueueManager queueManager, TaskQueueManager.TaskEntry task) throws Exception {
        Field allTasksField = TaskQueueManager.class.getDeclaredField("allTasks");
        allTasksField.setAccessible(true);
        Map<String, TaskQueueManager.TaskEntry> allTasks =
                (Map<String, TaskQueueManager.TaskEntry>) allTasksField.get(queueManager);
        allTasks.put(task.taskId, task);
    }

    private static void injectField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
