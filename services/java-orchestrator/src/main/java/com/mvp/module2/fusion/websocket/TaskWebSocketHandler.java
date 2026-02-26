package com.mvp.module2.fusion.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mvp.module2.fusion.queue.TaskQueueManager;
import com.mvp.module2.fusion.service.CollectionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class TaskWebSocketHandler extends TextWebSocketHandler {

    private static final Logger logger = LoggerFactory.getLogger(TaskWebSocketHandler.class);

    private final ConcurrentHashMap<String, WebSocketSession> userSessions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, WebSocketSession>> taskSubscribers =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, WebSocketSession>> collectionSubscribers =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> taskCollectionCache = new ConcurrentHashMap<>();

    @Autowired
    private TaskQueueManager taskQueueManager;

    @Autowired(required = false)
    private CollectionRepository collectionRepository;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        String userId = getUserIdFromSession(session);
        userSessions.put(userId, session);
        logger.info("WebSocket connected: user={}, session={}", userId, session.getId());
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        String userId = getUserIdFromSession(session);
        userSessions.remove(userId);
        removeSessionFromSubscribers(taskSubscribers, session.getId());
        removeSessionFromSubscribers(collectionSubscribers, session.getId());
        logger.info("WebSocket disconnected: user={}", userId);
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        try {
            Map<String, Object> payload = objectMapper.readValue(message.getPayload(), Map.class);
            String action = normalizeText((String) payload.get("action"));
            if (action.isEmpty()) {
                logger.warn("Unknown action: {}", payload.get("action"));
                return;
            }
            switch (action) {
                case "subscribe":
                    handleSubscribe(session, payload);
                    break;
                case "unsubscribe":
                    handleUnsubscribe(session, payload);
                    break;
                case "subscribeCollection":
                    handleSubscribeCollection(session, payload);
                    break;
                case "unsubscribeCollection":
                    handleUnsubscribeCollection(session, payload);
                    break;
                case "cancel":
                    handleCancel(session, payload);
                    break;
                case "ping":
                    sendMessage(session, Map.of("type", "pong"));
                    break;
                default:
                    logger.warn("Unknown action: {}", action);
            }
        } catch (Exception error) {
            logger.error("Error handling message", error);
        }
    }

    private void handleSubscribe(WebSocketSession session, Map<String, Object> payload) {
        String taskId = normalizeText((String) payload.get("taskId"));
        if (taskId.isEmpty()) {
            return;
        }
        taskSubscribers.computeIfAbsent(taskId, k -> new ConcurrentHashMap<>()).put(session.getId(), session);
        TaskQueueManager.TaskEntry task = taskQueueManager.getTask(taskId);
        if (task != null) {
            sendTaskUpdate(session, task, resolveCollectionId(taskId));
        }
        logger.debug("Session {} subscribed to task {}", session.getId(), taskId);
    }

    private void handleUnsubscribe(WebSocketSession session, Map<String, Object> payload) {
        String taskId = normalizeText((String) payload.get("taskId"));
        if (taskId.isEmpty()) {
            return;
        }
        ConcurrentHashMap<String, WebSocketSession> subscribers = taskSubscribers.get(taskId);
        if (subscribers == null) {
            return;
        }
        subscribers.remove(session.getId());
        if (subscribers.isEmpty()) {
            taskSubscribers.remove(taskId, subscribers);
        }
    }

    private void handleSubscribeCollection(WebSocketSession session, Map<String, Object> payload) {
        String collectionId = normalizeText((String) payload.get("collectionId"));
        if (collectionId.isEmpty()) {
            return;
        }
        collectionSubscribers.computeIfAbsent(collectionId, k -> new ConcurrentHashMap<>())
                .put(session.getId(), session);
        logger.debug("Session {} subscribed to collection {}", session.getId(), collectionId);
    }

    private void handleUnsubscribeCollection(WebSocketSession session, Map<String, Object> payload) {
        String collectionId = normalizeText((String) payload.get("collectionId"));
        if (collectionId.isEmpty()) {
            return;
        }
        ConcurrentHashMap<String, WebSocketSession> subscribers = collectionSubscribers.get(collectionId);
        if (subscribers == null) {
            return;
        }
        subscribers.remove(session.getId());
        if (subscribers.isEmpty()) {
            collectionSubscribers.remove(collectionId, subscribers);
        }
    }

    private void handleCancel(WebSocketSession session, Map<String, Object> payload) {
        String taskId = normalizeText((String) payload.get("taskId"));
        if (taskId.isEmpty()) {
            return;
        }
        boolean cancelled = taskQueueManager.cancelTask(taskId);
        sendMessage(session, Map.of(
                "type", "cancelResult",
                "taskId", taskId,
                "success", cancelled
        ));
    }

    public void broadcastTaskUpdate(TaskQueueManager.TaskEntry task) {
        String collectionId = resolveCollectionId(task.taskId);
        ConcurrentHashMap<String, WebSocketSession> subscribers = taskSubscribers.get(task.taskId);
        if (subscribers != null) {
            for (WebSocketSession session : subscribers.values()) {
                sendTaskUpdate(session, task, collectionId);
            }
        }

        if (!collectionId.isEmpty()) {
            ConcurrentHashMap<String, WebSocketSession> collectionSessions = collectionSubscribers.get(collectionId);
            if (collectionSessions != null) {
                for (WebSocketSession session : collectionSessions.values()) {
                    sendTaskUpdate(session, task, collectionId);
                }
            }
        }

        WebSocketSession userSession = userSessions.get(task.userId);
        if (userSession != null && userSession.isOpen()) {
            sendTaskUpdate(userSession, task, collectionId);
        }
    }

    public void broadcastTaskUpdate(String taskId, String status, double progress, String message, String resultPath) {
        String normalizedTaskId = normalizeText(taskId);
        String collectionId = resolveCollectionId(normalizedTaskId);
        Map<String, Object> update = buildTaskUpdatePayload(
                normalizedTaskId,
                status,
                progress,
                message,
                resultPath,
                "",
                collectionId
        );

        ConcurrentHashMap<String, WebSocketSession> subscribers = taskSubscribers.get(normalizedTaskId);
        if (subscribers != null) {
            for (WebSocketSession session : subscribers.values()) {
                sendMessage(session, update);
            }
        }

        if (!collectionId.isEmpty()) {
            ConcurrentHashMap<String, WebSocketSession> collectionSessions = collectionSubscribers.get(collectionId);
            if (collectionSessions != null) {
                for (WebSocketSession session : collectionSessions.values()) {
                    sendMessage(session, update);
                }
            }
        }

        for (WebSocketSession session : userSessions.values()) {
            if (session.isOpen()) {
                sendMessage(session, update);
            }
        }
    }

    private void sendTaskUpdate(WebSocketSession session, TaskQueueManager.TaskEntry task, String collectionId) {
        Map<String, Object> update = buildTaskUpdatePayload(
                task.taskId,
                task.status.name(),
                task.progress,
                task.statusMessage,
                task.resultPath,
                task.errorMessage,
                collectionId
        );
        sendMessage(session, update);
    }

    private Map<String, Object> buildTaskUpdatePayload(
            String taskId,
            String status,
            double progress,
            String message,
            String resultPath,
            String errorMessage,
            String collectionId
    ) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "taskUpdate");
        payload.put("taskId", taskId != null ? taskId : "");
        payload.put("status", status != null ? status : "");
        payload.put("progress", progress);
        payload.put("message", message != null ? message : "");
        payload.put("resultPath", resultPath != null ? resultPath : "");
        payload.put("errorMessage", errorMessage != null ? errorMessage : "");
        if (!collectionId.isEmpty()) {
            payload.put("collectionId", collectionId);
        }
        return payload;
    }

    private synchronized void sendMessage(WebSocketSession session, Map<String, Object> payload) {
        if (!session.isOpen()) {
            return;
        }
        try {
            String json = objectMapper.writeValueAsString(payload);
            session.sendMessage(new TextMessage(json));
        } catch (IOException error) {
            logger.error("Error sending message", error);
        }
    }

    private void removeSessionFromSubscribers(
            ConcurrentHashMap<String, ConcurrentHashMap<String, WebSocketSession>> subscribersByTopic,
            String sessionId
    ) {
        for (Map.Entry<String, ConcurrentHashMap<String, WebSocketSession>> entry : subscribersByTopic.entrySet()) {
            ConcurrentHashMap<String, WebSocketSession> subscribers = entry.getValue();
            subscribers.remove(sessionId);
            if (subscribers.isEmpty()) {
                subscribersByTopic.remove(entry.getKey(), subscribers);
            }
        }
    }

    private String resolveCollectionId(String taskId) {
        String normalizedTaskId = normalizeText(taskId);
        if (normalizedTaskId.isEmpty()) {
            return "";
        }
        String cached = taskCollectionCache.get(normalizedTaskId);
        if (cached != null) {
            return cached;
        }
        if (collectionRepository == null) {
            return "";
        }
        String collectionId = collectionRepository.findCollectionIdByTaskId(normalizedTaskId).orElse("");
        if (!collectionId.isEmpty()) {
            taskCollectionCache.put(normalizedTaskId, collectionId);
        }
        return collectionId;
    }

    private String normalizeText(String value) {
        if (value == null) {
            return "";
        }
        return value.trim();
    }

    private String getUserIdFromSession(WebSocketSession session) {
        if (session.getUri() == null || session.getUri().getQuery() == null) {
            return session.getId();
        }
        for (String param : session.getUri().getQuery().split("&")) {
            String[] pair = param.split("=");
            if (pair.length == 2 && "userId".equals(pair[0])) {
                return pair[1];
            }
        }
        return session.getId();
    }

    public int getConnectionCount() {
        return userSessions.size();
    }
}
