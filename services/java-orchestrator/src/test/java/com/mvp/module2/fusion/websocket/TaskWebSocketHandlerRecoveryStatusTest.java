package com.mvp.module2.fusion.websocket;

import com.mvp.module2.fusion.queue.TaskQueueManager;
import com.mvp.module2.fusion.service.TaskCostSummaryService;
import com.mvp.module2.fusion.service.TaskTerminalEventService;
import com.mvp.module2.fusion.service.TaskStatusPresentationService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TaskWebSocketHandlerRecoveryStatusTest {

    @TempDir
    Path tempDir;

    @Test
    void buildTaskUpdatePayloadShouldExposeSharedRecoveryFields() throws Exception {
        TaskWebSocketHandler handler = new TaskWebSocketHandler();
        injectField(handler, "taskStatusPresentationService", new TaskStatusPresentationService());
        injectField(handler, "taskCostSummaryService", new TaskCostSummaryService());

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

        Map<?, ?> payload = invokeBuildTaskUpdatePayload(handler, task);

        assertEquals(true, payload.get("blocked"));
        assertEquals("blocked", payload.get("statusCategory"));
        assertEquals("phase2b", payload.get("recoveryStage"));
        assertEquals("llm_call_commit_pending", payload.get("recoveryCheckpoint"));
        assertEquals("phase2b/chunk-42", payload.get("retryEntryPoint"));
        assertEquals("runtime", payload.get("source"));
    }

    @Test
    void buildTaskUpdatePayloadShouldExposePersistedCategoryPathOnCompletion() throws Exception {
        TaskWebSocketHandler handler = new TaskWebSocketHandler();
        injectField(handler, "taskStatusPresentationService", new TaskStatusPresentationService());
        injectField(handler, "taskCostSummaryService", new TaskCostSummaryService());

        Path taskDir = tempDir.resolve("VT_ws_category_001");
        Files.createDirectories(taskDir);
        Files.writeString(
                taskDir.resolve("video_meta.json"),
                """
                {
                  "category_path": "编程开发/微服务架构/熔断治理"
                }
                """,
                StandardCharsets.UTF_8
        );

        TaskQueueManager.TaskEntry task = new TaskQueueManager.TaskEntry();
        task.taskId = "VT_ws_category_001";
        task.userId = "user-category";
        task.status = TaskQueueManager.TaskStatus.COMPLETED;
        task.progress = 1.0d;
        task.statusMessage = "处理完成";
        task.outputDir = taskDir.toString();
        task.resultPath = taskDir.resolve("result.md").toString();
        task.createdAt = Instant.parse("2026-03-15T10:00:00Z");

        Map<?, ?> payload = invokeBuildTaskUpdatePayload(handler, task);

        assertEquals("编程开发/微服务架构/熔断治理", payload.get("categoryPath"));
        assertEquals("storage/VT_ws_category_001", payload.get("taskPath"));
    }

    @Test
    void buildTaskUpdatePayloadShouldExposeTaskCostSummary() throws Exception {
        TaskWebSocketHandler handler = new TaskWebSocketHandler();
        injectField(handler, "taskStatusPresentationService", new TaskStatusPresentationService());
        injectField(handler, "taskCostSummaryService", new TaskCostSummaryService());

        Path taskDir = tempDir.resolve("VT_ws_cost_001");
        Files.createDirectories(taskDir.resolve("intermediates"));
        Files.writeString(
                taskDir.resolve("intermediates").resolve("task_metrics_latest.json"),
                """
                {
                  "generated_at": "2026-03-15T11:00:00Z",
                  "llm_cost": {
                    "currency": "CNY",
                    "total_cost": 0.1451296
                  }
                }
                """,
                StandardCharsets.UTF_8
        );

        TaskQueueManager.TaskEntry task = new TaskQueueManager.TaskEntry();
        task.taskId = "VT_ws_cost_001";
        task.userId = "user-cost";
        task.status = TaskQueueManager.TaskStatus.COMPLETED;
        task.progress = 1.0d;
        task.statusMessage = "处理完成";
        task.outputDir = taskDir.toString();
        task.resultPath = taskDir.resolve("result.md").toString();
        task.createdAt = Instant.parse("2026-03-15T10:00:00Z");

        Map<?, ?> payload = invokeBuildTaskUpdatePayload(handler, task);

        assertEquals("¥0.15", payload.get("taskCostSummary"));
        assertTrue(payload.get("taskCost") instanceof Map<?, ?>);
        Map<?, ?> taskCost = (Map<?, ?>) payload.get("taskCost");
        assertEquals("CNY", taskCost.get("currency"));
        assertEquals(0.1451296d, ((Number) taskCost.get("totalCost")).doubleValue());
    }

    @Test
    void afterConnectionEstablishedShouldStoreConcurrentSendDecorator() throws Exception {
        TaskWebSocketHandler handler = new TaskWebSocketHandler();
        WebSocketSession session = mock(WebSocketSession.class);
        when(session.getId()).thenReturn("ws-session-001");
        when(session.getUri()).thenReturn(URI.create("ws://localhost/ws/tasks?userId=user-a&clientType=browser"));
        when(session.isOpen()).thenReturn(true);

        handler.afterConnectionEstablished(session);

        @SuppressWarnings("unchecked")
        Map<String, Map<String, WebSocketSession>> userSessions =
                (Map<String, Map<String, WebSocketSession>>) readField(handler, "userSessions");
        WebSocketSession managedSession = userSessions.get("user-a").get("ws-session-001");

        assertNotNull(managedSession);
        assertTrue(managedSession instanceof ConcurrentWebSocketSessionDecorator);
    }

    @Test
    void subscribeShouldReuseManagedSessionInSubscriberRegistry() throws Exception {
        TaskWebSocketHandler handler = new TaskWebSocketHandler();
        TaskQueueManager taskQueueManager = mock(TaskQueueManager.class);
        injectField(handler, "taskQueueManager", taskQueueManager);

        WebSocketSession session = mock(WebSocketSession.class);
        when(session.getId()).thenReturn("ws-session-002");
        when(session.getUri()).thenReturn(URI.create("ws://localhost/ws/tasks?userId=user-b&clientType=browser"));
        when(session.isOpen()).thenReturn(true);

        handler.afterConnectionEstablished(session);
        handler.handleTextMessage(session, new TextMessage("""
                {"action":"subscribe","taskId":"task-transport-1"}
                """));

        @SuppressWarnings("unchecked")
        Map<String, Map<String, WebSocketSession>> userSessions =
                (Map<String, Map<String, WebSocketSession>>) readField(handler, "userSessions");
        @SuppressWarnings("unchecked")
        Map<String, Map<String, WebSocketSession>> taskSubscribers =
                (Map<String, Map<String, WebSocketSession>>) readField(handler, "taskSubscribers");

        WebSocketSession managedSession = userSessions.get("user-b").get("ws-session-002");
        WebSocketSession subscribedSession = taskSubscribers.get("task-transport-1").get("ws-session-002");

        assertTrue(managedSession instanceof ConcurrentWebSocketSessionDecorator);
        assertSame(managedSession, subscribedSession);
    }

    @Test
    void ackShouldReplyWithAckConfirmedAfterRepositoryAcknowledge() throws Exception {
        TaskWebSocketHandler handler = new TaskWebSocketHandler();
        TaskTerminalEventService terminalEventService = mock(TaskTerminalEventService.class);
        injectField(handler, "taskTerminalEventService", terminalEventService);

        WebSocketSession session = mock(WebSocketSession.class);
        when(session.getId()).thenReturn("ws-session-ack");
        when(session.getUri()).thenReturn(URI.create("ws://localhost/ws/tasks?userId=user-ack&clientType=browser"));
        when(session.isOpen()).thenReturn(true);

        handler.afterConnectionEstablished(session);
        handler.handleTextMessage(session, new TextMessage("""
                {"action":"ack","messageId":21}
                """));

        verify(terminalEventService).acknowledge("user-ack", 21L);
        verify(session).sendMessage(argThat(message ->
                message instanceof TextMessage
                        && ((TextMessage) message).getPayload().contains("\"type\":\"ackConfirmed\"")
                        && ((TextMessage) message).getPayload().contains("\"messageId\":21")
        ));
    }

    private static Map<?, ?> invokeBuildTaskUpdatePayload(TaskWebSocketHandler handler, TaskQueueManager.TaskEntry task)
            throws Exception {
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
        return (Map<?, ?>) value;
    }

    private static void injectField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Object readField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }
}
