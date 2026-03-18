package com.mvp.module2.fusion.queue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mvp.module2.fusion.service.TaskRuntimeRecoveryService;
import com.mvp.module2.fusion.service.TaskRuntimeStageStore;
import com.mvp.module2.fusion.service.TaskRuntimeRedisRetentionService;
import com.mvp.module2.fusion.service.TaskStateRepository;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TaskQueueManagerRedisRetentionTest {

    @Test
    void completeTaskShouldSyncTerminalRedisRetention() throws Exception {
        TaskRuntimeRedisRetentionService retentionService = mock(TaskRuntimeRedisRetentionService.class);
        TaskQueueManager queueManager = newQueueManager(retentionService);
        TaskQueueManager.TaskEntry task = queueManager.submitTask(
                "u_retention_complete",
                "https://example.com/retention-complete",
                "var/tmp-retention-complete",
                TaskQueueManager.Priority.NORMAL
        );

        TaskQueueManager.TaskEntry polled = queueManager.pollNextTask(1, TimeUnit.SECONDS);
        assertNotNull(polled);
        queueManager.markProbeFinished(task.taskId, "probe done", 0.1d, java.util.Map.of(), null);
        queueManager.completeTask(task.taskId, "out.md");

        verify(retentionService).syncTaskRetention(task.taskId, "COMPLETED");
    }

    @Test
    void retryTaskShouldClearTerminalRedisRetention() throws Exception {
        TaskRuntimeRedisRetentionService retentionService = mock(TaskRuntimeRedisRetentionService.class);
        TaskQueueManager queueManager = newQueueManager(retentionService);
        TaskQueueManager.TaskEntry task = queueManager.submitTask(
                "u_retention_retry",
                "https://example.com/retention-retry",
                "var/tmp-retention-retry",
                TaskQueueManager.Priority.NORMAL
        );

        TaskQueueManager.TaskEntry polled = queueManager.pollNextTask(1, TimeUnit.SECONDS);
        assertNotNull(polled);
        queueManager.markProbeFinished(task.taskId, "probe done", 0.1d, java.util.Map.of(), null);
        queueManager.failTask(task.taskId, "provider timeout");
        queueManager.retryTaskTransition(task.taskId);

        verify(retentionService).syncTaskRetention(task.taskId, "FAILED");
        verify(retentionService, atLeastOnce()).syncTaskRetention(task.taskId, "QUEUED");
    }

    @Test
    void blockedTaskShouldSyncNonTerminalRedisRetention() throws Exception {
        Path taskRoot = Files.createTempDirectory("queue-manager-retention-blocked");
        writeStageState(
                taskRoot,
                "phase2b",
                "MANUAL_RETRY_REQUIRED",
                "llm_call_commit_pending",
                "MANUAL_RETRY",
                "repair llm quota and retry",
                "phase2b/chunk-42",
                1773500003210L
        );

        TaskRuntimeRedisRetentionService retentionService = mock(TaskRuntimeRedisRetentionService.class);
        TaskQueueManager queueManager = newQueueManager(retentionService);
        ObjectMapper objectMapper = new ObjectMapper();
        injectField(
                queueManager,
                "taskRuntimeRecoveryService",
                new TaskRuntimeRecoveryService(objectMapper, new TaskRuntimeStageStore(objectMapper))
        );
        TaskQueueManager.TaskEntry task = queueManager.submitTask(
                "u_retention_blocked",
                "https://example.com/retention-blocked",
                taskRoot.toString(),
                TaskQueueManager.Priority.NORMAL
        );

        TaskQueueManager.TaskEntry polled = queueManager.pollNextTask(1, TimeUnit.SECONDS);
        assertNotNull(polled);
        queueManager.failTask(task.taskId, "provider quota exhausted");

        verify(retentionService).syncTaskRetention(task.taskId, "MANUAL_RETRY_REQUIRED");
    }

    private static TaskQueueManager newQueueManager(TaskRuntimeRedisRetentionService retentionService) throws Exception {
        TaskQueueManager queueManager = new TaskQueueManager();
        TaskStateRepository repository = mock(TaskStateRepository.class);
        doNothing().when(repository).upsertTask(any(TaskQueueManager.TaskEntry.class));
        when(repository.findTask(anyString())).thenReturn(Optional.empty());
        when(repository.listAllTasks()).thenReturn(List.of());
        injectField(queueManager, "taskStateRepository", repository);
        injectField(queueManager, "taskRuntimeRedisRetentionService", retentionService);
        return queueManager;
    }

    private static void writeStageState(
            Path taskRoot,
            String stage,
            String status,
            String checkpoint,
            String retryMode,
            String requiredAction,
            String retryEntryPoint,
            long updatedAtMs
    ) throws Exception {
        Path stageDir = taskRoot.resolve("intermediates").resolve("rt").resolve("stage").resolve(stage);
        Files.createDirectories(stageDir);
        String json = """
                {
                  \"stage\": \"%s\",
                  \"status\": \"%s\",
                  \"checkpoint\": \"%s\",
                  \"retry_mode\": \"%s\",
                  \"required_action\": \"%s\",
                  \"retry_entry_point\": \"%s\",
                  \"updated_at_ms\": %d,
                  \"output_dir\": \"%s\"
                }
                """.formatted(
                stage,
                status,
                checkpoint,
                retryMode,
                requiredAction,
                retryEntryPoint,
                updatedAtMs,
                escapeJson(taskRoot.toAbsolutePath().normalize().toString())
        );
        Files.writeString(stageDir.resolve("stage_state.json"), json, StandardCharsets.UTF_8);
    }

    private static String escapeJson(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static void injectField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
