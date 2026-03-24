package com.mvp.module2.fusion.websocket;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mvp.module2.fusion.queue.TaskQueueManager;
import com.mvp.module2.fusion.service.CollectionRepository;
import com.mvp.module2.fusion.service.TaskCostSummaryService;
import com.mvp.module2.fusion.service.TaskTerminalEventService;
import com.mvp.module2.fusion.service.TaskStatusPresentationService;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.PongMessage;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class TaskWebSocketHandler extends TextWebSocketHandler {

    private static final Logger logger = LoggerFactory.getLogger(TaskWebSocketHandler.class);
    private static final int SEND_TIME_LIMIT_MS = 10_000;
    private static final int SEND_BUFFER_SIZE_LIMIT_BYTES = 512 * 1024;
    private static final String WEB_TASK_UPDATES_STREAM_KEY = "web-task-updates";
    private static final int FAST_PING_MIN_LENGTH = 17;
    private static final int FAST_PING_MAX_LENGTH = 512;
    private static final String ACTION_FIELD_NAME = "action";
    private static final String CLIENT_TIME_FIELD_NAME = "clientTime";
    private static final String PING_ACTION_VALUE = "ping";

    private final ConcurrentHashMap<String, ConcurrentHashMap<String, WebSocketSession>> userSessions =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, WebSocketSession>> taskSubscribers =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, WebSocketSession>> collectionSubscribers =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, WebSocketSession>> phase2bSubscribers =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> taskCollectionCache = new ConcurrentHashMap<>();
    private final TaskWebSocketHeartbeatCoordinator heartbeatCoordinator = new TaskWebSocketHeartbeatCoordinator();

    @Autowired
    private TaskQueueManager taskQueueManager;

    @Autowired(required = false)
    private CollectionRepository collectionRepository;

    @Autowired(required = false)
    private TaskStatusPresentationService taskStatusPresentationService = new TaskStatusPresentationService();

    @Autowired(required = false)
    private TaskCostSummaryService taskCostSummaryService = new TaskCostSummaryService();

    @Autowired
    private TaskTerminalEventService taskTerminalEventService;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        WebSocketSession managedSession = wrapSessionForConcurrentSend(session);
        String userId = getUserIdFromSession(session);
        String streamKey = getTextQueryParam(session, "streamKey");
        String clientType = getTextQueryParam(session, "clientType");
        long lastAckedTerminalEventId = parsePositiveLong(getTextQueryParam(session, "lastAckedTerminalEventId"));
        userSessions.computeIfAbsent(userId, key -> new ConcurrentHashMap<>()).put(session.getId(), managedSession);
        heartbeatCoordinator.registerSession(managedSession, userId, streamKey, clientType);
        if (!streamKey.isBlank() && WEB_TASK_UPDATES_STREAM_KEY.equals(streamKey)) {
            replayPendingTerminalEvents(managedSession, userId, lastAckedTerminalEventId);
        }
        logger.info("WebSocket connected: user={}, session={}", userId, session.getId());
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        unregisterSession(session, status);
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) {
        TaskWebSocketHeartbeatCoordinator.SessionRuntimeState runtimeState =
                heartbeatCoordinator.findState(session.getId());
        String userId = runtimeState != null ? runtimeState.userId() : getUserIdFromSession(session);
        logger.warn("WebSocket transport error: user={}, session={}", userId, session.getId(), exception);
        closeSessionSilently(session, new CloseStatus(4002, "transport error"));
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        long now = System.currentTimeMillis();
        try {
            heartbeatCoordinator.markClientTextSignal(session.getId(), now);
            String rawPayload = message.getPayload();
            if (tryHandleFastPing(session, rawPayload, now)) {
                return;
            }
            Map<String, Object> payload = objectMapper.readValue(rawPayload, Map.class);
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
                    handlePing(session, readLong(payload.get("clientTime")), now);
                    break;
                case "ack":
                    handleAck(session, payload);
                    break;
                default:
                    logger.warn("Unknown action: {}", action);
            }
        } catch (Exception error) {
            logger.error("Error handling message", error);
        }
    }

    @Override
    protected void handlePongMessage(WebSocketSession session, PongMessage message) {
        heartbeatCoordinator.markTransportPong(session.getId(), System.currentTimeMillis());
    }

    private void handleSubscribe(WebSocketSession session, Map<String, Object> payload) {
        String taskId = normalizeText((String) payload.get("taskId"));
        if (taskId.isEmpty()) {
            return;
        }
        WebSocketSession managedSession = resolveManagedSession(session);
        taskSubscribers.computeIfAbsent(taskId, k -> new ConcurrentHashMap<>()).put(session.getId(), managedSession);
        TaskQueueManager.TaskEntry task = taskQueueManager.getTask(taskId);
        if (task != null) {
            sendTaskUpdate(managedSession, task, resolveCollectionId(taskId));
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
        WebSocketSession managedSession = resolveManagedSession(session);
        collectionSubscribers.computeIfAbsent(collectionId, k -> new ConcurrentHashMap<>())
                .put(session.getId(), managedSession);
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
        sendPayloadToSessions(List.of(resolveManagedSession(session)), Map.of(
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
        WebSocketSession managedSession = resolveManagedSession(session);
        phase2bSubscribers.computeIfAbsent(channel, key -> new ConcurrentHashMap<>()).put(session.getId(), managedSession);
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

    public void broadcastTaskTerminalEvent(TaskQueueManager.TaskEntry task) {
        if (task == null || task.status == null) {
            return;
        }
        if (task.status != TaskQueueManager.TaskStatus.COMPLETED
                && task.status != TaskQueueManager.TaskStatus.FAILED) {
            return;
        }
        String collectionId = resolveCollectionId(task.taskId);
        Map<String, Object> payload = buildTaskUpdatePayload(
                task.taskId,
                task.status.name(),
                task.progress,
                task.statusMessage,
                task.resultPath,
                task.errorMessage,
                collectionId,
                task
        );
        payload.put("terminalStatus", task.status.name());
        Map<String, Object> queuedPayload = taskTerminalEventService.enqueue(task, payload);
        if (queuedPayload == null || queuedPayload.isEmpty()) {
            return;
        }
        sendPayloadToSessions(collectSessions(userSessions.get(task.userId)), queuedPayload);
    }

    public void broadcastBenchmarkEvent(
            String userId,
            String taskId,
            String collectionId,
            Map<String, Object> payload
    ) {
        if (payload == null || payload.isEmpty()) {
            return;
        }
        String normalizedUserId = normalizeText(userId);
        String normalizedTaskId = normalizeText(taskId);
        String normalizedCollectionId = normalizeText(collectionId);
        if (normalizedUserId.isEmpty() && normalizedTaskId.isEmpty() && normalizedCollectionId.isEmpty()) {
            return;
        }
        Map<String, Object> eventPayload = new LinkedHashMap<>(payload);
        eventPayload.putIfAbsent("type", "benchmarkEvent");
        eventPayload.putIfAbsent("updatedAt", System.currentTimeMillis());
        sendPayloadToSessions(
                collectSessions(
                        taskSubscribers.get(normalizedTaskId),
                        collectionSubscribers.get(normalizedCollectionId),
                        userSessions.get(normalizedUserId)
                ),
                eventPayload
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
        payload.put(
                "userMessage",
                task != null && task.userMessage != null
                        ? task.userMessage
                        : (message != null ? message : "")
        );
        payload.put("resultPath", resultPath != null ? resultPath : "");
        payload.put(
                "errorMessage",
                task != null && task.errorMessage != null && (errorMessage == null || errorMessage.isBlank())
                        ? task.errorMessage
                        : (errorMessage != null ? errorMessage : "")
        );
        taskStatusPresentationService.appendRecoveryFields(
                payload,
                status,
                task != null ? task.recoveryPayload : null
        );
        appendFinalCategoryFields(payload, task, status);
        appendTaskCostFields(payload, task);
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
            if (task.completedAt != null) {
                payload.put("completedAt", task.completedAt.toString());
            }
            if (task.updatedAt != null) {
                payload.put("runtimeUpdatedAt", task.updatedAt.toString());
            }
            payload.put("source", "runtime");
            payload.put("markdownAvailable", task.resultPath != null && !task.resultPath.isBlank());
        }
        if (!collectionId.isEmpty()) {
            payload.put("collectionId", collectionId);
        }
        return payload;
    }

    private void appendFinalCategoryFields(
            Map<String, Object> payload,
            TaskQueueManager.TaskEntry task,
            String status
    ) {
        if (payload == null || task == null || !isCompletedStatus(status)) {
            return;
        }
        PersistedCategorySnapshot snapshot = loadPersistedCategorySnapshot(task);
        if (snapshot == null || snapshot.categoryPath.isBlank()) {
            return;
        }
        payload.put("categoryPath", snapshot.categoryPath);
        if (!snapshot.taskPath.isBlank()) {
            payload.put("taskPath", snapshot.taskPath);
        }
    }

    private void appendTaskCostFields(Map<String, Object> payload, TaskQueueManager.TaskEntry task) {
        if (payload == null || task == null || taskCostSummaryService == null) {
            return;
        }
        Path taskDir = resolveTaskDirectory(task);
        if (taskDir == null) {
            return;
        }
        taskCostSummaryService.readSummary(taskDir).ifPresent(summary -> {
            payload.put("taskCost", summary.toPayload());
            payload.put("taskCostSummary", summary.displayText());
        });
    }

    private PersistedCategorySnapshot loadPersistedCategorySnapshot(TaskQueueManager.TaskEntry task) {
        Path taskDir = resolveTaskDirectory(task);
        if (taskDir == null) {
            return null;
        }
        PersistedCategorySnapshot fromMeta = readPersistedCategorySnapshot(
                taskDir.resolve("video_meta.json"),
                "category_path"
        );
        if (fromMeta != null) {
            return withTaskPath(fromMeta, resolveTaskPath(task, taskDir));
        }
        PersistedCategorySnapshot fromArtifact = readPersistedCategorySnapshot(
                taskDir.resolve("category_classification.json"),
                "category_path"
        );
        if (fromArtifact != null) {
            return withTaskPath(fromArtifact, resolveTaskPath(task, taskDir));
        }
        return null;
    }

    private PersistedCategorySnapshot withTaskPath(PersistedCategorySnapshot snapshot, String taskPath) {
        if (snapshot == null) {
            return null;
        }
        return new PersistedCategorySnapshot(snapshot.categoryPath, normalizeText(taskPath));
    }

    private PersistedCategorySnapshot readPersistedCategorySnapshot(Path jsonPath, String categoryFieldName) {
        if (jsonPath == null || !Files.isRegularFile(jsonPath)) {
            return null;
        }
        try {
            JsonNode root = objectMapper.readTree(jsonPath.toFile());
            if (root == null || !root.isObject()) {
                return null;
            }
            String categoryPath = normalizeText(root.path(categoryFieldName).asText(""));
            if (categoryPath.isBlank()) {
                return null;
            }
            return new PersistedCategorySnapshot(categoryPath, "");
        } catch (Exception error) {
            logger.debug("Ignore persisted category snapshot read failure: path={}", jsonPath, error);
            return null;
        }
    }

    private Path resolveTaskDirectory(TaskQueueManager.TaskEntry task) {
        if (task == null) {
            return null;
        }
        Path fromOutputDir = toDirectoryPath(task.outputDir);
        if (fromOutputDir != null) {
            return fromOutputDir;
        }
        return toDirectoryPath(task.resultPath);
    }

    private Path toDirectoryPath(String rawPath) {
        String normalized = normalizeText(rawPath);
        if (normalized.isBlank()) {
            return null;
        }
        try {
            Path path = Path.of(normalized).toAbsolutePath().normalize();
            if (Files.isDirectory(path)) {
                return path;
            }
            if (Files.isRegularFile(path)) {
                return path.getParent();
            }
        } catch (Exception ignored) {
        }
        return null;
    }

    private String resolveTaskPath(TaskQueueManager.TaskEntry task, Path taskDir) {
        if (taskDir != null && taskDir.getFileName() != null) {
            String storageKey = normalizeText(taskDir.getFileName().toString());
            if (!storageKey.isBlank()) {
                return "storage/" + storageKey;
            }
        }
        if (task != null && task.bookOptions != null) {
            String storageKey = normalizeText(task.bookOptions.storageKey);
            if (!storageKey.isBlank()) {
                return "storage/" + storageKey;
            }
        }
        return "";
    }

    private boolean isCompletedStatus(String status) {
        return "COMPLETED".equalsIgnoreCase(normalizeText(status));
    }

    private void sendRawMessage(WebSocketSession session, Map<String, Object> payload) {
        if (session == null || !session.isOpen()) {
            return;
        }
        if (Thread.currentThread().isInterrupted()) {
            logInterruptedSendSkip(session);
            return;
        }
        try {
            String json = objectMapper.writeValueAsString(payload);
            session.sendMessage(new TextMessage(json));
        } catch (Exception error) {
            if (isInterruptedSendFailure(error)) {
                Thread.currentThread().interrupt();
                logInterruptedSendSkip(session);
                return;
            }
            TaskWebSocketHeartbeatCoordinator.SessionRuntimeState runtimeState =
                    heartbeatCoordinator.findState(session.getId());
            logger.warn(
                    "Error sending websocket message, closing session: user={}, stream={}, session={}",
                    runtimeState != null ? runtimeState.userId() : "",
                    runtimeState != null ? runtimeState.streamKey() : "",
                    session.getId(),
                    error
            );
            closeSessionSilently(session, new CloseStatus(4001, "send failure"));
        }
    }

    private void sendRawText(WebSocketSession session, String payload) {
        if (session == null || !session.isOpen()) {
            return;
        }
        if (Thread.currentThread().isInterrupted()) {
            logInterruptedSendSkip(session);
            return;
        }
        try {
            session.sendMessage(new TextMessage(payload != null ? payload : ""));
        } catch (Exception error) {
            if (isInterruptedSendFailure(error)) {
                Thread.currentThread().interrupt();
                logInterruptedSendSkip(session);
                return;
            }
            TaskWebSocketHeartbeatCoordinator.SessionRuntimeState runtimeState =
                    heartbeatCoordinator.findState(session.getId());
            logger.warn(
                    "Error sending websocket text message, closing session: user={}, stream={}, session={}",
                    runtimeState != null ? runtimeState.userId() : "",
                    runtimeState != null ? runtimeState.streamKey() : "",
                    session.getId(),
                    error
            );
            closeSessionSilently(session, new CloseStatus(4001, "send failure"));
        }
    }

    private boolean isInterruptedSendFailure(Throwable error) {
        if (Thread.currentThread().isInterrupted()) {
            return true;
        }
        Throwable cursor = error;
        while (cursor != null) {
            if (cursor instanceof InterruptedException) {
                return true;
            }
            String message = normalizeText(cursor.getMessage()).toLowerCase(Locale.ROOT);
            if (message.contains("interrupted while waiting for a blocking send to complete")
                    || message.contains("current thread was interrupted")) {
                return true;
            }
            cursor = cursor.getCause();
        }
        return false;
    }

    private void logInterruptedSendSkip(WebSocketSession session) {
        TaskWebSocketHeartbeatCoordinator.SessionRuntimeState runtimeState =
                heartbeatCoordinator.findState(session.getId());
        logger.info(
                "Skip websocket message because sender thread was interrupted: user={}, stream={}, session={}",
                runtimeState != null ? runtimeState.userId() : "",
                runtimeState != null ? runtimeState.streamKey() : "",
                session.getId()
        );
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

    private boolean tryHandleFastPing(
            WebSocketSession session,
            String rawPayload,
            long now
    ) {
        // 心跳是高频热路径，这里先做有效长度过滤，再做字符扫描，避免正则和 JSON 反序列化分配。
        if (rawPayload == null || rawPayload.isEmpty()) {
            return false;
        }
        int payloadStart = findLeadingNonWhitespace(rawPayload);
        if (payloadStart < 0) {
            return false;
        }
        int payloadEndExclusive = findTrailingNonWhitespaceExclusive(rawPayload);
        int effectiveLength = payloadEndExclusive - payloadStart;
        if (effectiveLength < FAST_PING_MIN_LENGTH || effectiveLength > FAST_PING_MAX_LENGTH) {
            return false;
        }
        if (rawPayload.charAt(payloadStart) != '{' || rawPayload.charAt(payloadEndExclusive - 1) != '}') {
            return false;
        }
        int objectStart = payloadStart + 1;
        int objectEndExclusive = payloadEndExclusive - 1;
        int cursor = skipJsonWhitespace(rawPayload, objectStart, objectEndExclusive);
        boolean actionMatched = false;
        long clientTime = 0L;
        while (cursor < objectEndExclusive) {
            if (rawPayload.charAt(cursor) != '"') {
                return false;
            }
            int fieldNameEnd = findJsonStringEnd(rawPayload, cursor + 1, objectEndExclusive);
            if (fieldNameEnd < 0) {
                return false;
            }
            int fieldNameStart = cursor + 1;
            cursor = skipJsonWhitespace(rawPayload, fieldNameEnd + 1, objectEndExclusive);
            if (cursor >= objectEndExclusive || rawPayload.charAt(cursor) != ':') {
                return false;
            }
            cursor = skipJsonWhitespace(rawPayload, cursor + 1, objectEndExclusive);
            if (cursor >= objectEndExclusive) {
                return false;
            }
            if (matchesAsciiJsonToken(rawPayload, fieldNameStart, fieldNameEnd, ACTION_FIELD_NAME)) {
                int actionValueEnd = matchJsonStringLiteral(rawPayload, cursor, objectEndExclusive, PING_ACTION_VALUE);
                if (actionValueEnd < 0) {
                    return false;
                }
                actionMatched = true;
                cursor = actionValueEnd;
            } else if (matchesAsciiJsonToken(rawPayload, fieldNameStart, fieldNameEnd, CLIENT_TIME_FIELD_NAME)) {
                if (isJsonNumberStart(rawPayload.charAt(cursor))) {
                    int numberEnd = skipJsonNumber(rawPayload, cursor, objectEndExclusive);
                    if (numberEnd < 0) {
                        return false;
                    }
                    clientTime = parseSignedLongOrDefault(rawPayload, cursor, numberEnd, 0L);
                    cursor = numberEnd;
                } else {
                    cursor = skipJsonValue(rawPayload, cursor, objectEndExclusive);
                    if (cursor < 0) {
                        return false;
                    }
                }
            } else {
                cursor = skipJsonValue(rawPayload, cursor, objectEndExclusive);
                if (cursor < 0) {
                    return false;
                }
            }
            cursor = skipJsonWhitespace(rawPayload, cursor, objectEndExclusive);
            if (cursor >= objectEndExclusive) {
                break;
            }
            if (rawPayload.charAt(cursor) != ',') {
                return false;
            }
            cursor = skipJsonWhitespace(rawPayload, cursor + 1, objectEndExclusive);
        }
        if (!actionMatched) {
            return false;
        }
        handlePing(session, clientTime, now);
        return true;
    }

    private int findLeadingNonWhitespace(String payload) {
        for (int i = 0; i < payload.length(); i++) {
            if (!Character.isWhitespace(payload.charAt(i))) {
                return i;
            }
        }
        return -1;
    }

    private int findTrailingNonWhitespaceExclusive(String payload) {
        int index = payload.length();
        while (index > 0 && Character.isWhitespace(payload.charAt(index - 1))) {
            index--;
        }
        return index;
    }

    private int skipJsonWhitespace(String payload, int start, int endExclusive) {
        int cursor = start;
        while (cursor < endExclusive && Character.isWhitespace(payload.charAt(cursor))) {
            cursor++;
        }
        return cursor;
    }

    private int findJsonStringEnd(String payload, int start, int endExclusive) {
        boolean escaped = false;
        for (int cursor = start; cursor < endExclusive; cursor++) {
            char current = payload.charAt(cursor);
            if (escaped) {
                escaped = false;
                continue;
            }
            if (current == '\\') {
                escaped = true;
                continue;
            }
            if (current == '"') {
                return cursor;
            }
        }
        return -1;
    }

    private boolean matchesAsciiJsonToken(
            String payload,
            int start,
            int endExclusive,
            String expected
    ) {
        if (endExclusive - start != expected.length()) {
            return false;
        }
        for (int i = 0; i < expected.length(); i++) {
            if (toLowerAscii(payload.charAt(start + i)) != toLowerAscii(expected.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private int matchJsonStringLiteral(
            String payload,
            int start,
            int endExclusive,
            String expected
    ) {
        if (start >= endExclusive || payload.charAt(start) != '"') {
            return -1;
        }
        int cursor = start + 1;
        for (int i = 0; i < expected.length(); i++) {
            if (cursor >= endExclusive || payload.charAt(cursor) == '\\') {
                return -1;
            }
            if (toLowerAscii(payload.charAt(cursor)) != toLowerAscii(expected.charAt(i))) {
                return -1;
            }
            cursor++;
        }
        if (cursor >= endExclusive || payload.charAt(cursor) != '"') {
            return -1;
        }
        return cursor + 1;
    }

    private int skipJsonValue(String payload, int start, int endExclusive) {
        if (start >= endExclusive) {
            return -1;
        }
        char current = payload.charAt(start);
        if (current == '"') {
            int stringEnd = findJsonStringEnd(payload, start + 1, endExclusive);
            return stringEnd >= 0 ? stringEnd + 1 : -1;
        }
        if (current == '{' || current == '[') {
            return skipJsonCompositeValue(payload, start, endExclusive);
        }
        if (isJsonNumberStart(current)) {
            return skipJsonNumber(payload, start, endExclusive);
        }
        if (matchesJsonLiteral(payload, start, endExclusive, "true")) {
            return start + 4;
        }
        if (matchesJsonLiteral(payload, start, endExclusive, "false")) {
            return start + 5;
        }
        if (matchesJsonLiteral(payload, start, endExclusive, "null")) {
            return start + 4;
        }
        return -1;
    }

    private int skipJsonCompositeValue(String payload, int start, int endExclusive) {
        int objectDepth = 0;
        int arrayDepth = 0;
        for (int cursor = start; cursor < endExclusive; cursor++) {
            char current = payload.charAt(cursor);
            if (current == '"') {
                int stringEnd = findJsonStringEnd(payload, cursor + 1, endExclusive);
                if (stringEnd < 0) {
                    return -1;
                }
                cursor = stringEnd;
                continue;
            }
            if (current == '{') {
                objectDepth++;
                continue;
            }
            if (current == '}') {
                objectDepth--;
                if (objectDepth < 0) {
                    return -1;
                }
                if (objectDepth == 0 && arrayDepth == 0) {
                    return cursor + 1;
                }
                continue;
            }
            if (current == '[') {
                arrayDepth++;
                continue;
            }
            if (current == ']') {
                arrayDepth--;
                if (arrayDepth < 0) {
                    return -1;
                }
                if (objectDepth == 0 && arrayDepth == 0) {
                    return cursor + 1;
                }
            }
        }
        return -1;
    }

    private boolean isJsonNumberStart(char current) {
        return current == '-' || (current >= '0' && current <= '9');
    }

    private int skipJsonNumber(String payload, int start, int endExclusive) {
        int cursor = start;
        if (cursor < endExclusive && payload.charAt(cursor) == '-') {
            cursor++;
        }
        int integerStart = cursor;
        while (cursor < endExclusive && Character.isDigit(payload.charAt(cursor))) {
            cursor++;
        }
        if (cursor == integerStart) {
            return -1;
        }
        if (cursor < endExclusive && payload.charAt(cursor) == '.') {
            cursor++;
            int fractionStart = cursor;
            while (cursor < endExclusive && Character.isDigit(payload.charAt(cursor))) {
                cursor++;
            }
            if (cursor == fractionStart) {
                return -1;
            }
        }
        if (cursor < endExclusive) {
            char exponentFlag = payload.charAt(cursor);
            if (exponentFlag == 'e' || exponentFlag == 'E') {
                cursor++;
                if (cursor < endExclusive) {
                    char exponentSign = payload.charAt(cursor);
                    if (exponentSign == '+' || exponentSign == '-') {
                        cursor++;
                    }
                }
                int exponentStart = cursor;
                while (cursor < endExclusive && Character.isDigit(payload.charAt(cursor))) {
                    cursor++;
                }
                if (cursor == exponentStart) {
                    return -1;
                }
            }
        }
        return cursor;
    }

    private boolean matchesJsonLiteral(String payload, int start, int endExclusive, String literal) {
        if (start + literal.length() > endExclusive) {
            return false;
        }
        for (int i = 0; i < literal.length(); i++) {
            if (payload.charAt(start + i) != literal.charAt(i)) {
                return false;
            }
        }
        return true;
    }

    private long parseSignedLongOrDefault(String payload, int start, int endExclusive, long fallback) {
        if (start >= endExclusive) {
            return fallback;
        }
        boolean negative = payload.charAt(start) == '-';
        int cursor = negative ? start + 1 : start;
        if (cursor >= endExclusive) {
            return fallback;
        }
        long limit = negative ? Long.MIN_VALUE : -Long.MAX_VALUE;
        long result = 0L;
        while (cursor < endExclusive) {
            char current = payload.charAt(cursor);
            if (current < '0' || current > '9') {
                return fallback;
            }
            int digit = current - '0';
            if (result < (limit + digit) / 10L) {
                return fallback;
            }
            result = result * 10L - digit;
            cursor++;
        }
        return negative ? result : -result;
    }

    private char toLowerAscii(char current) {
        if (current >= 'A' && current <= 'Z') {
            return (char) (current + ('a' - 'A'));
        }
        return current;
    }

    private void handlePing(WebSocketSession session, long clientTime, long now) {
        sendRawText(resolveManagedSession(session), buildPongPayload(now, clientTime));
    }

    private String buildPongPayload(long serverTime, long clientTime) {
        return "{\"type\":\"pong\",\"serverTime\":" + serverTime + ",\"clientTime\":" + clientTime + "}";
    }

    private void handleAck(WebSocketSession session, Map<String, Object> payload) {
        String userId = getUserIdFromSession(session);
        long messageId = readLong(payload.get("messageId"));
        if (messageId <= 0L) {
            return;
        }
        taskTerminalEventService.acknowledge(userId, messageId);
        sendPayloadToSessions(List.of(resolveManagedSession(session)), Map.of(
                "type", "ackConfirmed",
                "messageId", messageId,
                "ackedThrough", messageId,
                "serverTime", System.currentTimeMillis()
        ));
    }

    private void replayPendingTerminalEvents(WebSocketSession session, String userId, long lastAckedTerminalEventId) {
        WebSocketSession managedSession = resolveManagedSession(session);
        if (managedSession == null || !managedSession.isOpen()) {
            return;
        }
        List<Map<String, Object>> payloads = taskTerminalEventService.replayPendingEvents(userId, lastAckedTerminalEventId);
        if (payloads.isEmpty()) {
            return;
        }
        sendPayloadToSessions(List.of(managedSession), payloads.get(0));
        for (int i = 1; i < payloads.size(); i++) {
            sendPayloadToSessions(List.of(managedSession), payloads.get(i));
        }
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
            if (Thread.currentThread().isInterrupted()) {
                return;
            }
            if (heartbeatCoordinator.shouldSkipPayload(session, payload)) {
                continue;
            }
            sendRawMessage(session, payload);
        }
    }

    private void unregisterSession(WebSocketSession session, CloseStatus status) {
        TaskWebSocketHeartbeatCoordinator.SessionRuntimeState runtimeState =
                heartbeatCoordinator.unregisterSession(session.getId());
        String userId = runtimeState != null ? runtimeState.userId() : getUserIdFromSession(session);
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

    private long parsePositiveLong(String rawValue) {
        try {
            long parsed = Long.parseLong(normalizeText(rawValue));
            return Math.max(0L, parsed);
        } catch (Exception ignored) {
            return 0L;
        }
    }

    private WebSocketSession wrapSessionForConcurrentSend(WebSocketSession session) {
        if (session == null || session instanceof ConcurrentWebSocketSessionDecorator) {
            return session;
        }
        return new ConcurrentWebSocketSessionDecorator(
                session,
                SEND_TIME_LIMIT_MS,
                SEND_BUFFER_SIZE_LIMIT_BYTES
        );
    }

    private WebSocketSession resolveManagedSession(WebSocketSession session) {
        return heartbeatCoordinator.resolveManagedSession(session);
    }

    @PreDestroy
    void stopHeartbeatTimer() {
        heartbeatCoordinator.shutdown();
    }

    TaskWebSocketHeartbeatCoordinator getHeartbeatCoordinator() {
        return heartbeatCoordinator;
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

    public int getUserConnectionCount(String userId) {
        String normalizedUserId = normalizeText(userId);
        if (normalizedUserId.isEmpty()) {
            return 0;
        }
        ConcurrentHashMap<String, WebSocketSession> sessions = userSessions.get(normalizedUserId);
        return sessions != null ? sessions.size() : 0;
    }

    private record PersistedCategorySnapshot(
            String categoryPath,
            String taskPath
    ) {
    }
}
