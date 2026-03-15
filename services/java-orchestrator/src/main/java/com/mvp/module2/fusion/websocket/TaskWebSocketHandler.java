package com.mvp.module2.fusion.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mvp.module2.fusion.queue.TaskQueueManager;
import com.mvp.module2.fusion.service.CollectionRepository;
import com.mvp.module2.fusion.service.TaskStatusPresentationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class TaskWebSocketHandler extends TextWebSocketHandler {

    private static final Logger logger = LoggerFactory.getLogger(TaskWebSocketHandler.class);
    private static final long CLIENT_HEARTBEAT_TIMEOUT_MS = 35_000L;

    private final ConcurrentHashMap<String, ConcurrentHashMap<String, WebSocketSession>> userSessions =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, WebSocketSession>> taskSubscribers =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, WebSocketSession>> collectionSubscribers =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, WebSocketSession>> phase2bSubscribers =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> taskCollectionCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, SessionRuntimeState> sessionStates = new ConcurrentHashMap<>();

    @Autowired
    private TaskQueueManager taskQueueManager;

    @Autowired(required = false)
    private CollectionRepository collectionRepository;

    @Autowired(required = false)
    private TaskStatusPresentationService taskStatusPresentationService = new TaskStatusPresentationService();

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        String userId = getUserIdFromSession(session);
        String streamKey = getTextQueryParam(session, "streamKey");
        userSessions.computeIfAbsent(userId, key -> new ConcurrentHashMap<>()).put(session.getId(), session);
        sessionStates.put(session.getId(), new SessionRuntimeState(session, userId, streamKey));
        logger.info("WebSocket connected: user={}, session={}", userId, session.getId());
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        unregisterSession(session, status);
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) {
        SessionRuntimeState runtimeState = sessionStates.get(session.getId());
        String userId = runtimeState != null ? runtimeState.userId : getUserIdFromSession(session);
        logger.warn("WebSocket transport error: user={}, session={}", userId, session.getId(), exception);
        closeSessionSilently(session, new CloseStatus(4002, "transport error"));
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        try {
            SessionRuntimeState runtimeState = sessionStates.get(session.getId());
            if (runtimeState != null) {
                runtimeState.markClientActivity(System.currentTimeMillis());
            }
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
                case "subscribePhase2b":
                    handleSubscribePhase2b(session, payload);
                    break;
                case "unsubscribePhase2b":
                    handleUnsubscribePhase2b(session, payload);
                    break;
                case "ping":
                    handlePing(session, payload);
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
        sendPayloadToSessions(List.of(session), Map.of(
                "type", "cancelResult",
                "taskId", taskId,
                "success", cancelled
        ));
    }

    private void handleSubscribePhase2b(WebSocketSession session, Map<String, Object> payload) {
        String channel = normalizeText((String) payload.get("channel"));
        if (channel.isEmpty()) {
            return;
        }
        phase2bSubscribers.computeIfAbsent(channel, key -> new ConcurrentHashMap<>()).put(session.getId(), session);
        logger.debug("Session {} subscribed to phase2b channel {}", session.getId(), channel);
    }

    private void handleUnsubscribePhase2b(WebSocketSession session, Map<String, Object> payload) {
        String channel = normalizeText((String) payload.get("channel"));
        if (channel.isEmpty()) {
            return;
        }
        ConcurrentHashMap<String, WebSocketSession> subscribers = phase2bSubscribers.get(channel);
        if (subscribers == null) {
            return;
        }
        subscribers.remove(session.getId());
        if (subscribers.isEmpty()) {
            phase2bSubscribers.remove(channel, subscribers);
        }
    }

    public void broadcastPhase2bProgress(
            String channel,
            String requestId,
            String status,
            String message,
            boolean done,
            boolean success
    ) {
        String normalizedChannel = normalizeText(channel);
        if (normalizedChannel.isEmpty()) {
            return;
        }
        ConcurrentHashMap<String, WebSocketSession> subscribers = phase2bSubscribers.get(normalizedChannel);
        if (subscribers == null || subscribers.isEmpty()) {
            return;
        }
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "phase2bProgress");
        payload.put("channel", normalizedChannel);
        payload.put("requestId", normalizeText(requestId));
        payload.put("status", normalizeText(status));
        payload.put("message", message != null ? message : "");
        payload.put("done", done);
        payload.put("success", success);
        payload.put("updatedAt", System.currentTimeMillis());
        sendPayloadToSessions(subscribers.values(), payload);
    }

    public void broadcastPhase2bMarkdownChunk(
            String channel,
            String requestId,
            String chunk,
            int chunkIndex,
            boolean done
    ) {
        String normalizedChannel = normalizeText(channel);
        if (normalizedChannel.isEmpty()) {
            return;
        }
        ConcurrentHashMap<String, WebSocketSession> subscribers = phase2bSubscribers.get(normalizedChannel);
        if (subscribers == null || subscribers.isEmpty()) {
            return;
        }
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "phase2bMarkdownChunk");
        payload.put("channel", normalizedChannel);
        payload.put("requestId", normalizeText(requestId));
        payload.put("chunk", chunk != null ? chunk : "");
        payload.put("chunkIndex", Math.max(0, chunkIndex));
        payload.put("done", done);
        payload.put("updatedAt", System.currentTimeMillis());
        sendPayloadToSessions(subscribers.values(), payload);
    }

    public void broadcastPhase2bMarkdownFinal(
            String channel,
            String requestId,
            String markdown,
            int finalChunkIndex
    ) {
        String normalizedChannel = normalizeText(channel);
        if (normalizedChannel.isEmpty()) {
            return;
        }
        ConcurrentHashMap<String, WebSocketSession> subscribers = phase2bSubscribers.get(normalizedChannel);
        if (subscribers == null || subscribers.isEmpty()) {
            return;
        }
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "phase2bMarkdownFinal");
        payload.put("channel", normalizedChannel);
        payload.put("requestId", normalizeText(requestId));
        payload.put("markdown", markdown != null ? markdown : "");
        payload.put("chunkIndex", Math.max(0, finalChunkIndex));
        payload.put("done", true);
        payload.put("updatedAt", System.currentTimeMillis());
        sendPayloadToSessions(subscribers.values(), payload);
    }

    public void broadcastTaskUpdate(TaskQueueManager.TaskEntry task) {
        String collectionId = resolveCollectionId(task.taskId);
        sendPayloadToSessions(
                collectSessions(taskSubscribers.get(task.taskId), collectionSubscribers.get(collectionId), userSessions.get(task.userId)),
                buildTaskUpdatePayload(
                        task.taskId,
                        task.status.name(),
                        task.progress,
                        task.statusMessage,
                        task.resultPath,
                        task.errorMessage,
                        collectionId,
                        task
                )
        );
    }

    public void broadcastTaskUpdate(String taskId, String status, double progress, String message, String resultPath) {
        String normalizedTaskId = normalizeText(taskId);
        String collectionId = resolveCollectionId(normalizedTaskId);
        TaskQueueManager.TaskEntry task = taskQueueManager.getTask(normalizedTaskId);
        Map<String, Object> update = buildTaskUpdatePayload(
                normalizedTaskId,
                status,
                progress,
                message,
                resultPath,
                "",
                collectionId,
                task
        );

        TaskQueueManager.TaskEntry ownerTask = taskQueueManager.getTask(normalizedTaskId);
        String userId = ownerTask != null ? ownerTask.userId : "";
        sendPayloadToSessions(
                collectSessions(taskSubscribers.get(normalizedTaskId), collectionSubscribers.get(collectionId), userSessions.get(userId)),
                update
        );
    }

    public void broadcastTaskProbeResult(String taskId, String userId, Map<String, Object> probePayload) {
        String normalizedTaskId = normalizeText(taskId);
        if (normalizedTaskId.isEmpty() || probePayload == null || probePayload.isEmpty()) {
            return;
        }
        String collectionId = resolveCollectionId(normalizedTaskId);
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "taskProbeResult");
        payload.put("taskId", normalizedTaskId);
        payload.put("probe", probePayload);
        payload.put("updatedAt", System.currentTimeMillis());
        if (!collectionId.isEmpty()) {
            payload.put("collectionId", collectionId);
        }
        sendPayloadToSessions(
                collectSessions(taskSubscribers.get(normalizedTaskId), collectionSubscribers.get(collectionId), userSessions.get(userId)),
                payload
        );
    }

    public void broadcastTaskDeduped(
            String taskId,
            String userId,
            String duplicateOfTaskId,
            String normalizedVideoKey,
            String reason
    ) {
        String normalizedTaskId = normalizeText(taskId);
        if (normalizedTaskId.isEmpty()) {
            return;
        }
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "taskDeduped");
        payload.put("taskId", normalizedTaskId);
        payload.put("duplicateOfTaskId", normalizeText(duplicateOfTaskId));
        payload.put("normalizedVideoKey", normalizeText(normalizedVideoKey));
        payload.put("reason", normalizeText(reason));
        payload.put("updatedAt", System.currentTimeMillis());
        sendPayloadToSessions(collectSessions(userSessions.get(userId)), payload);
    }

    public void broadcastTaskMetaSync(
            String taskId,
            String userId,
            String pathKey,
            String changeKind,
            String anchorId
    ) {
        String normalizedTaskId = normalizeText(taskId);
        if (normalizedTaskId.isEmpty()) {
            return;
        }
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "taskMetaSync");
        payload.put("taskId", normalizedTaskId);
        payload.put("pathKey", normalizeText(pathKey));
        payload.put("changeKind", normalizeText(changeKind));
        payload.put("anchorId", normalizeText(anchorId));
        payload.put("updatedAt", System.currentTimeMillis());

        sendPayloadToSessions(
                collectSessions(taskSubscribers.get(normalizedTaskId), userSessions.get(userId)),
                payload
        );
    }

    private void sendTaskUpdate(WebSocketSession session, TaskQueueManager.TaskEntry task, String collectionId) {
        sendPayloadToSessions(
                List.of(session),
                buildTaskUpdatePayload(
                        task.taskId,
                        task.status.name(),
                        task.progress,
                        task.statusMessage,
                        task.resultPath,
                        task.errorMessage,
                        collectionId,
                        task
                )
        );
    }

    private Map<String, Object> buildTaskUpdatePayload(
            String taskId,
            String status,
            double progress,
            String message,
            String resultPath,
            String errorMessage,
            String collectionId,
            TaskQueueManager.TaskEntry task
    ) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "taskUpdate");
        payload.put("taskId", taskId != null ? taskId : "");
        payload.put("status", status != null ? status : "");
        payload.put("progress", progress);
        payload.put("message", message != null ? message : "");
        payload.put("resultPath", resultPath != null ? resultPath : "");
        payload.put("errorMessage", errorMessage != null ? errorMessage : "");
        taskStatusPresentationService.appendRecoveryFields(
                payload,
                status,
                task != null ? task.recoveryPayload : null
        );
        if (task != null) {
            String title = normalizeText(task.title);
            String videoUrl = normalizeText(task.videoUrl);
            String storageKey = task.bookOptions != null ? normalizeText(task.bookOptions.storageKey) : "";
            if (!title.isEmpty()) {
                payload.put("title", title);
            }
            if (!videoUrl.isEmpty()) {
                payload.put("videoUrl", videoUrl);
            }
            if (!storageKey.isEmpty()) {
                payload.put("storageKey", storageKey);
            }
            if (task.createdAt != null) {
                payload.put("createdAt", task.createdAt.toString());
            }
            payload.put("source", "runtime");
            payload.put("markdownAvailable", task.resultPath != null && !task.resultPath.isBlank());
        }
        if (!collectionId.isEmpty()) {
            payload.put("collectionId", collectionId);
        }
        return payload;
    }

    private synchronized void sendRawMessage(WebSocketSession session, Map<String, Object> payload) {
        if (!session.isOpen()) {
            return;
        }
        try {
            String json = objectMapper.writeValueAsString(payload);
            session.sendMessage(new TextMessage(json));
        } catch (IOException error) {
            logger.warn("Error sending message, closing session {}", session.getId(), error);
            closeSessionSilently(session, new CloseStatus(4001, "send failure"));
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

    private void removeSessionFromUserSessions(String userId, String sessionId) {
        String normalizedUserId = normalizeText(userId);
        if (normalizedUserId.isEmpty()) {
            return;
        }
        ConcurrentHashMap<String, WebSocketSession> sessions = userSessions.get(normalizedUserId);
        if (sessions == null) {
            return;
        }
        sessions.remove(sessionId);
        if (sessions.isEmpty()) {
            userSessions.remove(normalizedUserId, sessions);
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

    private void handlePing(WebSocketSession session, Map<String, Object> payload) {
        long now = System.currentTimeMillis();
        SessionRuntimeState runtimeState = sessionStates.get(session.getId());
        if (runtimeState != null) {
            runtimeState.markPing(now);
        }
        Map<String, Object> pong = new LinkedHashMap<>();
        pong.put("type", "pong");
        pong.put("serverTime", now);
        pong.put("clientTime", readLong(payload.get("clientTime")));
        sendRawMessage(session, pong);
    }

    private void sendPayloadToSessions(
            Iterable<WebSocketSession> candidateSessions,
            Map<String, Object> payload
    ) {
        LinkedHashMap<String, WebSocketSession> deduplicated = new LinkedHashMap<>();
        for (WebSocketSession session : candidateSessions) {
            if (session == null) {
                continue;
            }
            deduplicated.putIfAbsent(session.getId(), session);
        }
        for (WebSocketSession session : deduplicated.values()) {
            sendRawMessage(session, payload);
        }
    }

    private void unregisterSession(WebSocketSession session, CloseStatus status) {
        SessionRuntimeState runtimeState = sessionStates.remove(session.getId());
        String userId = runtimeState != null ? runtimeState.userId : getUserIdFromSession(session);
        removeSessionFromUserSessions(userId, session.getId());
        removeSessionFromSubscribers(taskSubscribers, session.getId());
        removeSessionFromSubscribers(collectionSubscribers, session.getId());
        removeSessionFromSubscribers(phase2bSubscribers, session.getId());
        logger.info("WebSocket disconnected: user={}, session={}, status={}", userId, session.getId(), status);
    }

    private void closeSessionSilently(WebSocketSession session, CloseStatus status) {
        try {
            session.close(status);
        } catch (Exception closeError) {
            logger.debug("Ignore websocket close failure: session={}", session.getId(), closeError);
        }
    }

    private List<WebSocketSession> collectSessions(
            ConcurrentHashMap<String, WebSocketSession>... sources
    ) {
        ArrayList<WebSocketSession> sessions = new ArrayList<>();
        for (ConcurrentHashMap<String, WebSocketSession> source : sources) {
            if (source == null || source.isEmpty()) {
                continue;
            }
            sessions.addAll(source.values());
        }
        return sessions;
    }

    private String getTextQueryParam(WebSocketSession session, String key) {
        if (session.getUri() == null || session.getUri().getQuery() == null) {
            return "";
        }
        for (String param : session.getUri().getQuery().split("&")) {
            String[] pair = param.split("=", 2);
            if (pair.length != 2) {
                continue;
            }
            if (key.equals(safeDecodeQueryParam(pair[0]))) {
                return normalizeText(safeDecodeQueryParam(pair[1]));
            }
        }
        return "";
    }

    private long readLong(Object rawValue) {
        if (rawValue == null) {
            return 0L;
        }
        if (rawValue instanceof Number) {
            return ((Number) rawValue).longValue();
        }
        try {
            return Long.parseLong(String.valueOf(rawValue).trim());
        } catch (Exception ignored) {
            return 0L;
        }
    }

    @Scheduled(fixedDelay = 5000L)
    public void reapHalfOpenSessions() {
        long now = System.currentTimeMillis();
        for (SessionRuntimeState runtimeState : new ArrayList<>(sessionStates.values())) {
            if (!runtimeState.heartbeatEnabled) {
                continue;
            }
            long baseline = Math.max(runtimeState.connectedAt, runtimeState.lastClientPingAt);
            if (now - baseline <= CLIENT_HEARTBEAT_TIMEOUT_MS) {
                continue;
            }
            logger.info(
                    "Closing websocket after heartbeat timeout: user={}, stream={}, session={}, lastPingAt={}",
                    runtimeState.userId,
                    runtimeState.streamKey,
                    runtimeState.session.getId(),
                    runtimeState.lastClientPingAt
            );
            closeSessionSilently(runtimeState.session, new CloseStatus(4008, "heartbeat timeout"));
        }
    }

    private String getUserIdFromSession(WebSocketSession session) {
        String userId = getTextQueryParam(session, "userId");
        if (!userId.isEmpty()) {
            return userId;
        }
        return session.getId();
    }

    private String safeDecodeQueryParam(String rawValue) {
        try {
            return URLDecoder.decode(rawValue, StandardCharsets.UTF_8);
        } catch (Exception ignored) {
            return rawValue != null ? rawValue : "";
        }
    }

    public int getConnectionCount() {
        int total = 0;
        for (ConcurrentHashMap<String, WebSocketSession> sessions : userSessions.values()) {
            total += sessions.size();
        }
        return total;
    }

    private static final class SessionRuntimeState {
        private final WebSocketSession session;
        private final String userId;
        private final String streamKey;
        private final boolean heartbeatEnabled;
        private final long connectedAt;
        private volatile long lastClientMessageAt;
        private volatile long lastClientPingAt;

        private SessionRuntimeState(WebSocketSession session, String userId, String streamKey) {
            long now = System.currentTimeMillis();
            this.session = session;
            this.userId = userId;
            this.streamKey = streamKey;
            this.heartbeatEnabled = !streamKey.isBlank();
            this.connectedAt = now;
            this.lastClientMessageAt = now;
            this.lastClientPingAt = now;
        }

        private void markClientActivity(long now) {
            this.lastClientMessageAt = now;
        }

        private void markPing(long now) {
            this.lastClientMessageAt = now;
            this.lastClientPingAt = now;
        }
    }
}
