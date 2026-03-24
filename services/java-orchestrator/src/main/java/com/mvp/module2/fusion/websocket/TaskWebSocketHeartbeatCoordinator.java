package com.mvp.module2.fusion.websocket;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.PingMessage;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 心跳协调器只负责会话活跃态、时间轮调度与连接回收。
 * 这样 WebSocket handler 可以专注在协议解析、订阅管理与广播编排。
 */
final class TaskWebSocketHeartbeatCoordinator {

    private static final Logger logger = LoggerFactory.getLogger(TaskWebSocketHeartbeatCoordinator.class);
    private static final long HEARTBEAT_WHEEL_TICK_MS = 100L;
    private static final int HEARTBEAT_WHEEL_TICKS = 512;
    private static final long HEARTBEAT_CHECK_INTERVAL_MS = 20_000L;
    private static final long NON_BROWSER_APPLICATION_TIMEOUT_MS = 35_000L;
    private static final long BROWSER_APPLICATION_SUSPEND_TIMEOUT_MS = 60_000L;
    private static final long BROWSER_TRANSPORT_HEARTBEAT_TIMEOUT_MS = 60_000L;
    private static final String WEB_TASK_UPDATES_STREAM_KEY = "web-task-updates";

    private final ConcurrentHashMap<String, SessionRuntimeState> sessionStates = new ConcurrentHashMap<>();
    private final AtomicReference<HashedWheelTimer> heartbeatTimerRef = new AtomicReference<>();

    SessionRuntimeState registerSession(
            WebSocketSession managedSession,
            String userId,
            String streamKey,
            String clientType
    ) {
        SessionRuntimeState runtimeState = new SessionRuntimeState(managedSession, userId, streamKey, clientType);
        sessionStates.put(managedSession.getId(), runtimeState);
        scheduleNextHeartbeat(runtimeState);
        return runtimeState;
    }

    SessionRuntimeState findState(String sessionId) {
        if (sessionId == null || sessionId.isBlank()) {
            return null;
        }
        return sessionStates.get(sessionId);
    }

    SessionRuntimeState unregisterSession(String sessionId) {
        SessionRuntimeState runtimeState = sessionStates.remove(sessionId);
        if (runtimeState != null) {
            runtimeState.markClosed();
            cancelHeartbeat(runtimeState);
        }
        return runtimeState;
    }

    void markClientTextSignal(String sessionId, long now) {
        SessionRuntimeState runtimeState = findState(sessionId);
        if (runtimeState == null) {
            return;
        }
        runtimeState.markClientActivity(now);
        runtimeState.markApplicationSignal(now);
    }

    void markTransportPong(String sessionId, long now) {
        SessionRuntimeState runtimeState = findState(sessionId);
        if (runtimeState == null) {
            return;
        }
        runtimeState.markTransportPong(now);
        runtimeState.markClientActivity(now);
    }

    WebSocketSession resolveManagedSession(WebSocketSession session) {
        if (session == null) {
            return null;
        }
        SessionRuntimeState runtimeState = findState(session.getId());
        if (runtimeState == null || runtimeState.session == null) {
            return session;
        }
        return runtimeState.session;
    }

    boolean shouldSkipPayload(WebSocketSession session, Map<String, Object> payload) {
        if (session == null || payload == null) {
            return false;
        }
        SessionRuntimeState runtimeState = findState(session.getId());
        if (runtimeState == null || !runtimeState.isApplicationSuspended()) {
            return false;
        }
        String type = normalizeText(String.valueOf(payload.getOrDefault("type", "")));
        if (!"taskUpdate".equals(type)) {
            return false;
        }
        return !isTerminalStatus(String.valueOf(payload.get("status")));
    }

    void shutdown() {
        HashedWheelTimer timer = heartbeatTimerRef.getAndSet(null);
        if (timer != null) {
            timer.stop();
        }
    }

    void runHeartbeatCheck(SessionRuntimeState runtimeState, long now) {
        if (runtimeState == null) {
            return;
        }
        runtimeState.clearHeartbeatTimeout();
        if (runtimeState.isClosed()) {
            return;
        }
        WebSocketSession managedSession = runtimeState.session;
        if (managedSession == null || !managedSession.isOpen()) {
            return;
        }
        if (runtimeState.transportHeartbeatEnabled) {
            long transportBaseline = runtimeState.transportBaseline();
            if (now - transportBaseline > BROWSER_TRANSPORT_HEARTBEAT_TIMEOUT_MS) {
                logger.info(
                        "Closing websocket after browser transport heartbeat timeout: user={}, stream={}, session={}, lastPongAt={}",
                        runtimeState.userId,
                        runtimeState.streamKey,
                        managedSession.getId(),
                        runtimeState.lastTransportPongAt()
                );
                runtimeState.markClosed();
                closeSessionSilently(managedSession, new CloseStatus(4008, "transport heartbeat timeout"));
                return;
            }
            if (!sendBrowserTransportPing(runtimeState, now)) {
                return;
            }
        }
        applyApplicationHeartbeatState(runtimeState, now);
        if (!runtimeState.isClosed() && managedSession.isOpen()) {
            scheduleNextHeartbeat(runtimeState);
        }
    }

    void applyApplicationHeartbeatState(SessionRuntimeState runtimeState, long now) {
        if (runtimeState == null || !runtimeState.applicationHeartbeatEnabled) {
            return;
        }
        long idleMs = now - runtimeState.applicationBaseline();
        if (runtimeState.applicationSuspensionEnabled
                && idleMs > BROWSER_APPLICATION_SUSPEND_TIMEOUT_MS
                && runtimeState.markApplicationSuspended()) {
            logger.info(
                    "Suspend websocket progress fanout after application heartbeat timeout: user={}, stream={}, session={}, lastApplicationAt={}",
                    runtimeState.userId,
                    runtimeState.streamKey,
                    runtimeState.session.getId(),
                    runtimeState.lastApplicationSignalAt()
            );
        }
        if (!runtimeState.applicationCloseEnabled || idleMs <= NON_BROWSER_APPLICATION_TIMEOUT_MS) {
            return;
        }
        logger.info(
                "Closing websocket after application heartbeat timeout: user={}, stream={}, session={}, lastApplicationAt={}",
                runtimeState.userId,
                runtimeState.streamKey,
                runtimeState.session.getId(),
                runtimeState.lastApplicationSignalAt()
        );
        runtimeState.markClosed();
        closeSessionSilently(runtimeState.session, new CloseStatus(4008, "heartbeat timeout"));
    }

    private void scheduleNextHeartbeat(SessionRuntimeState runtimeState) {
        if (runtimeState == null || runtimeState.isClosed()) {
            return;
        }
        Timeout timeout = resolveHeartbeatTimer().newTimeout(
                ignored -> runHeartbeatCheck(runtimeState, System.currentTimeMillis()),
                HEARTBEAT_CHECK_INTERVAL_MS,
                TimeUnit.MILLISECONDS
        );
        runtimeState.replaceHeartbeatTimeout(timeout);
    }

    private HashedWheelTimer resolveHeartbeatTimer() {
        HashedWheelTimer current = heartbeatTimerRef.get();
        if (current != null) {
            return current;
        }
        HashedWheelTimer created = new HashedWheelTimer(
                runnable -> {
                    Thread thread = new Thread(runnable, "task-websocket-heartbeat-wheel");
                    thread.setDaemon(true);
                    return thread;
                },
                HEARTBEAT_WHEEL_TICK_MS,
                TimeUnit.MILLISECONDS,
                HEARTBEAT_WHEEL_TICKS
        );
        if (heartbeatTimerRef.compareAndSet(null, created)) {
            return created;
        }
        created.stop();
        return heartbeatTimerRef.get();
    }

    private boolean sendBrowserTransportPing(SessionRuntimeState runtimeState, long now) {
        try {
            runtimeState.session.sendMessage(new PingMessage(ByteBuffer.wrap(new byte[0])));
            runtimeState.markTransportPing(now);
            return true;
        } catch (IOException error) {
            logger.warn(
                    "Browser transport ping failed: user={}, session={}",
                    runtimeState.userId,
                    runtimeState.session.getId(),
                    error
            );
            runtimeState.markClosed();
            closeSessionSilently(runtimeState.session, new CloseStatus(4009, "transport ping failed"));
            return false;
        }
    }

    private void cancelHeartbeat(SessionRuntimeState runtimeState) {
        if (runtimeState == null) {
            return;
        }
        Timeout timeout = runtimeState.detachHeartbeatTimeout();
        if (timeout != null) {
            timeout.cancel();
        }
    }

    private void closeSessionSilently(WebSocketSession session, CloseStatus status) {
        try {
            session.close(status);
        } catch (Exception closeError) {
            logger.debug("Ignore websocket close failure: session={}", session.getId(), closeError);
        }
    }

    private boolean isTerminalStatus(String status) {
        String normalizedStatus = normalizeText(status).toUpperCase(Locale.ROOT);
        return "COMPLETED".equals(normalizedStatus)
                || "FAILED".equals(normalizedStatus)
                || "CANCELLED".equals(normalizedStatus);
    }

    private String normalizeText(String value) {
        if (value == null) {
            return "";
        }
        return value.trim();
    }

    static final class SessionRuntimeState {
        private final WebSocketSession session;
        private final String userId;
        private final String streamKey;
        private final boolean applicationHeartbeatEnabled;
        private final boolean applicationSuspensionEnabled;
        private final boolean applicationCloseEnabled;
        private final boolean transportHeartbeatEnabled;
        private final long connectedAt;
        private final AtomicLong lastClientMessageAt;
        private final AtomicLong lastApplicationSignalAt;
        private final AtomicLong lastTransportPingAt;
        private final AtomicLong lastTransportPongAt;
        private final AtomicBoolean applicationSuspended = new AtomicBoolean(false);
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final AtomicReference<Timeout> heartbeatTimeout = new AtomicReference<>();

        private SessionRuntimeState(WebSocketSession session, String userId, String streamKey, String clientType) {
            long now = System.currentTimeMillis();
            this.session = session;
            this.userId = userId;
            this.streamKey = streamKey;
            String normalizedClientType = clientType != null ? clientType.trim().toLowerCase(Locale.ROOT) : "";
            this.transportHeartbeatEnabled = "browser".equals(normalizedClientType);
            boolean browserTaskUpdatesStream = WEB_TASK_UPDATES_STREAM_KEY.equals(streamKey);
            this.applicationHeartbeatEnabled = !streamKey.isBlank();
            this.applicationSuspensionEnabled = this.transportHeartbeatEnabled && browserTaskUpdatesStream;
            this.applicationCloseEnabled = !this.transportHeartbeatEnabled;
            this.connectedAt = now;
            this.lastClientMessageAt = new AtomicLong(now);
            this.lastApplicationSignalAt = new AtomicLong(now);
            this.lastTransportPingAt = new AtomicLong(0L);
            this.lastTransportPongAt = new AtomicLong(now);
        }

        private void markClientActivity(long now) {
            this.lastClientMessageAt.set(now);
        }

        private void markApplicationSignal(long now) {
            if (!applicationHeartbeatEnabled) {
                return;
            }
            this.lastApplicationSignalAt.set(now);
            this.applicationSuspended.set(false);
        }

        private void markTransportPing(long now) {
            this.lastTransportPingAt.set(now);
        }

        private void markTransportPong(long now) {
            this.lastTransportPongAt.set(now);
        }

        private long transportBaseline() {
            return Math.max(
                    connectedAt,
                    Math.max(lastTransportPongAt.get(), lastClientMessageAt.get())
            );
        }

        private long applicationBaseline() {
            return Math.max(connectedAt, lastApplicationSignalAt.get());
        }

        private boolean markApplicationSuspended() {
            if (!applicationSuspensionEnabled) {
                return false;
            }
            return applicationSuspended.compareAndSet(false, true);
        }

        private boolean isApplicationSuspended() {
            return applicationSuspended.get();
        }

        private long lastApplicationSignalAt() {
            return lastApplicationSignalAt.get();
        }

        private long lastTransportPongAt() {
            return lastTransportPongAt.get();
        }

        String userId() {
            return userId;
        }

        String streamKey() {
            return streamKey;
        }

        private boolean isClosed() {
            return closed.get();
        }

        private void markClosed() {
            closed.set(true);
        }

        private void replaceHeartbeatTimeout(Timeout timeout) {
            Timeout previous = heartbeatTimeout.getAndSet(timeout);
            if (previous != null) {
                previous.cancel();
            }
        }

        private void clearHeartbeatTimeout() {
            heartbeatTimeout.set(null);
        }

        private Timeout detachHeartbeatTimeout() {
            return heartbeatTimeout.getAndSet(null);
        }
    }
}
