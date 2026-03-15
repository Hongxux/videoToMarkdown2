package com.mvp.module2.fusion.websocket;

import com.mvp.module2.fusion.queue.TaskQueueManager;
import com.mvp.module2.fusion.service.TaskStatusPresentationService;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskWebSocketHandlerRecoveryStatusTest {

    @Test
    void buildTaskUpdatePayloadShouldExposeSharedRecoveryFields() throws Exception {
        TaskWebSocketHandler handler = new TaskWebSocketHandler();
        injectField(handler, "taskStatusPresentationService", new TaskStatusPresentationService());

        TaskQueueManager.TaskEntry task = new TaskQueueManager.TaskEntry();
        task.taskId = "VT_ws_recovery_001";
        task.userId = "user-ws";
        task.status = TaskQueueManager.TaskStatus.MANUAL_RETRY_REQUIRED;
        task.progress = 0.73d;
        task.statusMessage = "[phase2b/llm_call_commit_pending] repair llm quota and retry";
        task.resultPath = "";
        task.createdAt = Instant.parse("2026-03-15T10:00:00Z");
        task.recoveryPayload = new LinkedHashMap<>();
        task.recoveryPayload.put("stage", "phase2b");
        task.recoveryPayload.put("checkpoint", "llm_call_commit_pending");
        task.recoveryPayload.put("retryMode", "MANUAL_RETRY");
        task.recoveryPayload.put("requiredAction", "repair llm quota and retry");
        task.recoveryPayload.put("retryEntryPoint", "phase2b/chunk-42");

        Method method = TaskWebSocketHandler.class.getDeclaredMethod(
                "buildTaskUpdatePayload",
                String.class,
                String.class,
                double.class,
                String.class,
                String.class,
                String.class,
                String.class,
                TaskQueueManager.TaskEntry.class
        );
        method.setAccessible(true);
        Object value = method.invoke(
                handler,
                task.taskId,
                task.status.name(),
                task.progress,
                task.statusMessage,
                task.resultPath,
                "",
                "",
                task
        );

        assertTrue(value instanceof Map<?, ?>);
        Map<?, ?> payload = (Map<?, ?>) value;
        assertEquals(true, payload.get("blocked"));
        assertEquals("blocked", payload.get("statusCategory"));
        assertEquals("phase2b", payload.get("recoveryStage"));
        assertEquals("llm_call_commit_pending", payload.get("recoveryCheckpoint"));
        assertEquals("phase2b/chunk-42", payload.get("retryEntryPoint"));
        assertEquals("runtime", payload.get("source"));
    }

    private static void injectField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
