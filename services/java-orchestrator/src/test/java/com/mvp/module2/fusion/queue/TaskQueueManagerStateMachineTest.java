package com.mvp.module2.fusion.queue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mvp.module2.fusion.service.TaskRuntimeRecoveryService;
import com.mvp.module2.fusion.service.TaskStateRepository;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TaskQueueManagerStateMachineTest {

    @Test
    void repeatedCompleteShouldBeIdempotent() throws Exception {
        TaskQueueManager queueManager = newQueueManager();
        TaskQueueManager.TaskEntry task = queueManager.submitTask(
                "u_state_machine_complete",
                "https://example.com/state-machine-complete",
                "var/tmp-state-machine-complete",
                TaskQueueManager.Priority.NORMAL
        );

        TaskQueueManager.TaskEntry polled = queueManager.pollNextTask(1, TimeUnit.SECONDS);
        assertNotNull(polled);
        TaskQueueManager.TaskTransitionResult probeFinished = queueManager.markProbeFinished(task.taskId, "probe done", 0.10d, java.util.Map.of(), null);
        assertTrue(probeFinished.isApplied());

        TaskQueueManager.TaskTransitionResult first = queueManager.completeTask(task.taskId, "out-first.md");
        TaskQueueManager.TaskTransitionResult second = queueManager.completeTask(task.taskId, "out-second.md");

        assertTrue(first.isApplied());
        assertTrue(second.isNoOp());
        assertEquals(TaskQueueManager.TaskStatus.COMPLETED, queueManager.getTask(task.taskId).status);
        assertEquals("out-first.md", queueManager.getTask(task.taskId).resultPath);
    }

    @Test
    void repeatedCancelAndFinalizeShouldBeIdempotent() throws Exception {
        TaskQueueManager queueManager = newQueueManager();
        TaskQueueManager.TaskEntry task = queueManager.submitTask(
                "u_state_machine_cancel",
                "https://example.com/state-machine-cancel",
                "var/tmp-state-machine-cancel",
                TaskQueueManager.Priority.NORMAL
        );

        TaskQueueManager.TaskTransitionResult cancelFirst = queueManager.cancelTaskTransition(task.taskId);
        TaskQueueManager.TaskTransitionResult cancelSecond = queueManager.cancelTaskTransition(task.taskId);
        TaskQueueManager.TaskTransitionResult finalizeFirst = queueManager.finalizeCancelledTask(task.taskId, "task cancelled");
        TaskQueueManager.TaskTransitionResult finalizeSecond = queueManager.finalizeCancelledTask(task.taskId, "task cancelled");

        assertTrue(cancelFirst.isApplied());
        assertTrue(cancelSecond.isNoOp());
        assertTrue(finalizeFirst.isApplied());
        assertTrue(finalizeSecond.isNoOp());
        assertEquals(TaskQueueManager.TaskStatus.CANCELLED, queueManager.getTask(task.taskId).status);
    }

    @Test
    void terminalTaskShouldIgnoreProgressUpdate() throws Exception {
        TaskQueueManager queueManager = newQueueManager();
        TaskQueueManager.TaskEntry task = queueManager.submitTask(
                "u_state_machine_progress",
                "https://example.com/state-machine-progress",
                "var/tmp-state-machine-progress",
                TaskQueueManager.Priority.NORMAL
        );

        TaskQueueManager.TaskEntry polled = queueManager.pollNextTask(1, TimeUnit.SECONDS);
        assertNotNull(polled);
        TaskQueueManager.TaskTransitionResult probeFinished = queueManager.markProbeFinished(task.taskId, "probe done", 0.10d, java.util.Map.of(), null);
        assertTrue(probeFinished.isApplied());
        TaskQueueManager.TaskTransitionResult completed = queueManager.completeTask(task.taskId, "out-progress.md");

        boolean progressApplied = queueManager.updateProgress(task.taskId, 0.33, "should be ignored");

        assertTrue(completed.isApplied());
        assertFalse(progressApplied);
        assertEquals(1.0, queueManager.getTask(task.taskId).progress);
        assertNotNull(queueManager.getTask(task.taskId).statusMessage);
    }

    @Test
    void failTaskShouldBlockAndRetryFromRuntimeRecoveryDirective() throws Exception {
        Path taskRoot = Files.createTempDirectory("queue-manager-runtime-recovery");
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

        TaskQueueManager queueManager = newQueueManager();
        injectField(queueManager, "taskRuntimeRecoveryService", new TaskRuntimeRecoveryService(new ObjectMapper()));
        TaskQueueManager.TaskEntry task = queueManager.submitTask(
                "u_state_machine_manual_retry",
                "https://example.com/state-machine-manual-retry",
                taskRoot.toString(),
                TaskQueueManager.Priority.NORMAL
        );
        TaskQueueManager.TaskEntry polled = queueManager.pollNextTask(1, TimeUnit.SECONDS);
        assertNotNull(polled);
        assertEquals(TaskQueueManager.TaskStatus.PROBING, polled.status);

        TaskQueueManager.TaskTransitionResult blocked = queueManager.failTask(task.taskId, "provider quota exhausted");

        assertTrue(blocked.isApplied());
        TaskQueueManager.TaskEntry blockedTask = queueManager.getTask(task.taskId);
        assertNotNull(blockedTask);
        assertEquals(TaskQueueManager.TaskStatus.MANUAL_RETRY_REQUIRED, blockedTask.status);
        assertNotNull(blockedTask.completedAt);
        assertNotNull(blockedTask.recoveryPayload);
        assertEquals("phase2b", String.valueOf(blockedTask.recoveryPayload.get("stage")));
        assertEquals("llm_call_commit_pending", String.valueOf(blockedTask.recoveryPayload.get("checkpoint")));
        assertEquals("phase2b/chunk-42", String.valueOf(blockedTask.recoveryPayload.get("retryEntryPoint")));
        assertFalse(queueManager.updateProgress(task.taskId, 0.88d, "should be ignored after block"));

        TaskQueueManager.TaskTransitionResult retried = queueManager.retryTaskTransition(task.taskId);

        assertTrue(retried.isApplied());
        TaskQueueManager.TaskEntry retriedTask = queueManager.getTask(task.taskId);
        assertNotNull(retriedTask);
        assertEquals(TaskQueueManager.TaskStatus.QUEUED, retriedTask.status);
        assertNull(retriedTask.startedAt);
        assertNull(retriedTask.completedAt);
        assertNull(retriedTask.errorMessage);
        assertNull(retriedTask.recoveryPayload);
        assertTrue(retriedTask.statusMessage != null && retriedTask.statusMessage.contains("checkpoint"));
        TaskQueueManager.TaskEntry rePolled = queueManager.pollNextTask(1, TimeUnit.SECONDS);
        assertNotNull(rePolled);
        assertEquals(task.taskId, rePolled.taskId);
        assertEquals(TaskQueueManager.TaskStatus.PROBING, rePolled.status);
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
        Path stageDir = taskRoot.resolve("intermediates").resolve("rt").resolve("s").resolve(stage);
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
