package com.mvp.module2.fusion.websocket;

import com.mvp.module2.fusion.service.TaskTerminalEventService;
import org.junit.jupiter.api.Test;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TaskWebSocketHandlerHeartbeatTest {

    @Test
    void browserPingShouldReplyWithPongAndClearSuspendedFlag() throws Exception {
        TaskWebSocketHandler handler = newHandler();
        WebSocketSession session = mockSession(
                "ws-browser-ping",
                "ws://localhost/ws/tasks?userId=user-browser&clientType=browser&streamKey=web-task-updates"
        );

        try {
            handler.afterConnectionEstablished(session);
            TaskWebSocketHeartbeatCoordinator.SessionRuntimeState runtimeState =
                    readSessionRuntimeState(handler, "ws-browser-ping");
            readAtomicBoolean(runtimeState, "applicationSuspended").set(true);

            handler.handleTextMessage(session, new TextMessage("""
                    {"action":"ping","clientTime":123456}
                    """));

            verify(session).sendMessage(argThat(message ->
                    message instanceof TextMessage
                            && ((TextMessage) message).getPayload().contains("\"type\":\"pong\"")
                            && ((TextMessage) message).getPayload().contains("\"clientTime\":123456")
            ));
            assertFalse(readAtomicBoolean(runtimeState, "applicationSuspended").get());
        } finally {
            handler.stopHeartbeatTimer();
        }
    }

    @Test
    void suspendedBrowserSessionShouldSkipProcessingUpdateButKeepTerminalUpdate() throws Exception {
        TaskWebSocketHandler handler = newHandler();
        WebSocketSession session = mockSession(
                "ws-browser-suspend",
                "ws://localhost/ws/tasks?userId=user-browser&clientType=browser&streamKey=web-task-updates"
        );

        try {
            handler.afterConnectionEstablished(session);
            TaskWebSocketHeartbeatCoordinator.SessionRuntimeState runtimeState =
                    readSessionRuntimeState(handler, "ws-browser-suspend");
            readAtomicBoolean(runtimeState, "applicationSuspended").set(true);

            invokeSendPayloadToSessions(
                    handler,
                    List.of(resolveManagedSession(handler, session)),
                    Map.of("type", "taskUpdate", "status", "PROCESSING", "taskId", "task-1")
            );
            verify(session, never()).sendMessage(any(TextMessage.class));

            invokeSendPayloadToSessions(
                    handler,
                    List.of(resolveManagedSession(handler, session)),
                    Map.of("type", "taskUpdate", "status", "COMPLETED", "taskId", "task-1")
            );
            verify(session).sendMessage(argThat(message ->
                    message instanceof TextMessage
                            && ((TextMessage) message).getPayload().contains("\"status\":\"COMPLETED\"")
            ));
        } finally {
            handler.stopHeartbeatTimer();
        }
    }

    @Test
    void nonBrowserApplicationHeartbeatTimeoutShouldCloseSession() throws Exception {
        TaskWebSocketHandler handler = newHandler();
        WebSocketSession session = mockSession(
                "ws-mobile-timeout",
                "ws://localhost/ws/tasks?userId=user-mobile&streamKey=mobile-task-stream"
        );

        try {
            handler.afterConnectionEstablished(session);
            TaskWebSocketHeartbeatCoordinator.SessionRuntimeState runtimeState =
                    readSessionRuntimeState(handler, "ws-mobile-timeout");
            long now = System.currentTimeMillis() + 36_000L;

            handler.getHeartbeatCoordinator().applyApplicationHeartbeatState(runtimeState, now);

            assertTrue(readAtomicBoolean(runtimeState, "closed").get());
        } finally {
            handler.stopHeartbeatTimer();
        }
    }

    @Test
    void browserTransportHeartbeatTimeoutShouldCloseSession() throws Exception {
        TaskWebSocketHandler handler = newHandler();
        WebSocketSession session = mockSession(
                "ws-browser-transport-timeout",
                "ws://localhost/ws/tasks?userId=user-browser&clientType=browser&streamKey=web-task-updates"
        );

        try {
            handler.afterConnectionEstablished(session);
            TaskWebSocketHeartbeatCoordinator.SessionRuntimeState runtimeState =
                    readSessionRuntimeState(handler, "ws-browser-transport-timeout");
            long now = System.currentTimeMillis() + 61_000L;

            handler.getHeartbeatCoordinator().runHeartbeatCheck(runtimeState, now);

            assertTrue(readAtomicBoolean(runtimeState, "closed").get());
        } finally {
            handler.stopHeartbeatTimer();
        }
    }

    private static WebSocketSession mockSession(String sessionId, String uri) {
        WebSocketSession session = mock(WebSocketSession.class);
        when(session.getId()).thenReturn(sessionId);
        when(session.getUri()).thenReturn(URI.create(uri));
        when(session.isOpen()).thenReturn(true);
        return session;
    }

    private static TaskWebSocketHandler newHandler() throws Exception {
        TaskWebSocketHandler handler = new TaskWebSocketHandler();
        TaskTerminalEventService terminalEventService = mock(TaskTerminalEventService.class);
        when(terminalEventService.replayPendingEvents(any(), any(Long.class))).thenReturn(List.of());
        injectField(handler, "taskTerminalEventService", terminalEventService);
        return handler;
    }

    private static TaskWebSocketHeartbeatCoordinator.SessionRuntimeState readSessionRuntimeState(
            TaskWebSocketHandler handler,
            String sessionId
    ) {
        return handler.getHeartbeatCoordinator().findState(sessionId);
    }

    private static WebSocketSession resolveManagedSession(TaskWebSocketHandler handler, WebSocketSession session)
            throws Exception {
        Method method = TaskWebSocketHandler.class.getDeclaredMethod("resolveManagedSession", WebSocketSession.class);
        method.setAccessible(true);
        return (WebSocketSession) method.invoke(handler, session);
    }

    private static void invokeSendPayloadToSessions(
            TaskWebSocketHandler handler,
            Iterable<WebSocketSession> sessions,
            Map<String, Object> payload
    ) throws Exception {
        Method method = TaskWebSocketHandler.class.getDeclaredMethod(
                "sendPayloadToSessions",
                Iterable.class,
                Map.class
        );
        method.setAccessible(true);
        method.invoke(handler, sessions, payload);
    }

    private static AtomicBoolean readAtomicBoolean(Object target, String fieldName) throws Exception {
        return (AtomicBoolean) readField(target, fieldName);
    }

    private static Object readField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }

    private static void injectField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
