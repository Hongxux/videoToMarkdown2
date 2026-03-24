package com.mvp.module2.fusion.service.watchdog;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class Stage1WatchdogBridge {
    private static final Logger logger = LoggerFactory.getLogger(Stage1WatchdogBridge.class);
    private static final String WATCHDOG_SIGNAL_PREFIX = "WATCHDOG_SIGNAL|";
    private static final String STAGE1_HEARTBEAT_FILE = "stage1_watchdog_heartbeat.json";

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ConcurrentHashMap<String, Long> heartbeatSeqCache = new ConcurrentHashMap<>();

    public void resetTask(String taskId) {
        if (taskId == null || taskId.isBlank()) {
            return;
        }
        heartbeatSeqCache.remove(taskId);
    }

    public MonitorHandle startMonitor(String taskId, String outputDir, SignalEmitter emitter) {
        Path heartbeatPath = resolveStage1HeartbeatPath(outputDir);
        AtomicBoolean runningFlag = new AtomicBoolean(true);
        Thread monitorThread = startHeartbeatThread(taskId, heartbeatPath, runningFlag, emitter);
        return new MonitorHandle(heartbeatPath, runningFlag, monitorThread);
    }

    public void stopMonitor(String taskId, MonitorHandle handle, SignalEmitter emitter) {
        if (taskId == null || taskId.isBlank()) {
            return;
        }
        if (handle != null) {
            handle.runningFlag().set(false);
            stopHeartbeatThread(handle.monitorThread());
            Stage1HeartbeatSnapshot finalHeartbeat = readStage1Heartbeat(handle.heartbeatPath());
            if (finalHeartbeat != null) {
                emitWatchdogProgress(taskId, finalHeartbeat, true, emitter);
            }
        }
        heartbeatSeqCache.remove(taskId);
    }

    private Path resolveStage1HeartbeatPath(String outputDir) {
        if (outputDir == null || outputDir.isBlank()) {
            return null;
        }
        return Paths.get(outputDir, "intermediates", STAGE1_HEARTBEAT_FILE);
    }

    private Thread startHeartbeatThread(
            String taskId,
            Path heartbeatPath,
            AtomicBoolean runningFlag,
            SignalEmitter emitter
    ) {
        if (taskId == null || taskId.isBlank() || runningFlag == null) {
            return null;
        }
        Thread monitor = new Thread(() -> {
            while (runningFlag.get() && !Thread.currentThread().isInterrupted()) {
                try {
                    Stage1HeartbeatSnapshot snapshot = readStage1Heartbeat(heartbeatPath);
                    if (snapshot != null) {
                        emitWatchdogProgress(taskId, snapshot, false, emitter);
                    }
                    Thread.sleep(1000L);
                } catch (InterruptedException interruptedError) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Exception monitorError) {
                    logger.debug(
                            "[{}] Stage1 heartbeat monitor iteration failed: {}",
                            taskId,
                            extractThrowableMessage(monitorError)
                    );
                    try {
                        Thread.sleep(1000L);
                    } catch (InterruptedException interruptedError) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }, "Stage1HeartbeatMonitor-" + taskId);
        monitor.setDaemon(true);
        monitor.start();
        return monitor;
    }

    private void stopHeartbeatThread(Thread monitorThread) {
        if (monitorThread == null) {
            return;
        }
        monitorThread.interrupt();
        try {
            monitorThread.join(1500L);
        } catch (InterruptedException interruptedError) {
            Thread.currentThread().interrupt();
        }
    }

    private Stage1HeartbeatSnapshot readStage1Heartbeat(Path heartbeatPath) {
        if (heartbeatPath == null || !Files.exists(heartbeatPath)) {
            return null;
        }
        try {
            byte[] raw = Files.readAllBytes(heartbeatPath);
            if (raw.length == 0) {
                return null;
            }
            JsonNode root = objectMapper.readTree(raw);
            if (root == null || !root.isObject()) {
                return null;
            }
            String stage = firstNonBlank(readTextNode(root, "stage"), "stage1");
            String status = firstNonBlank(readTextNode(root, "status"), "running");
            String checkpoint = firstNonBlank(readTextNode(root, "checkpoint"), "unknown");
            int completed = readIntNode(root, "completed", 0);
            int pending = readIntNode(root, "pending", 0);
            long seq = readLongNode(root, "seq", 0L);
            long updatedAtMs = readLongNode(root, "updated_at_ms", 0L);
            String signalType = firstNonBlank(
                    readTextNode(root, "signal_type"),
                    firstNonBlank(readTextNode(root, "signal"), "hard")
            );
            return new Stage1HeartbeatSnapshot(stage, status, checkpoint, completed, pending, seq, updatedAtMs, signalType);
        } catch (IOException ioError) {
            logger.debug("Stage1 heartbeat parse skipped, file may still be updating: {}", heartbeatPath, ioError);
            return null;
        }
    }

    private void emitWatchdogProgress(String taskId, Stage1HeartbeatSnapshot snapshot, boolean forceEmit, SignalEmitter emitter) {
        if (taskId == null || taskId.isBlank() || snapshot == null || emitter == null) {
            return;
        }
        long seq = Math.max(0L, snapshot.seq());
        Long lastSeq = heartbeatSeqCache.get(taskId);
        if (!forceEmit && seq > 0 && lastSeq != null && seq <= lastSeq) {
            return;
        }
        if (seq > 0) {
            heartbeatSeqCache.put(taskId, seq);
        }
        double progress = computeStage1Progress(snapshot);
        String message = buildWatchdogSignalMessage(snapshot);
        emitter.emit(progress, message);
    }

    private double computeStage1Progress(Stage1HeartbeatSnapshot snapshot) {
        if (snapshot == null) {
            return 0.25d;
        }
        double completed = Math.max(0.0d, snapshot.completed());
        double pending = Math.max(0.0d, snapshot.pending());
        double total = Math.max(1.0d, completed + pending);
        double ratio = Math.max(0.0d, Math.min(1.0d, completed / total));
        double progress = 0.25d + ratio * 0.10d;
        String status = firstNonBlank(snapshot.status(), "").toLowerCase(Locale.ROOT);
        if ("completed".equals(status)) {
            progress = 0.35d;
        } else if ("failed".equals(status)) {
            progress = Math.max(0.25d, Math.min(0.34d, progress));
        }
        return Math.max(0.25d, Math.min(0.35d, progress));
    }

    private String buildWatchdogSignalMessage(Stage1HeartbeatSnapshot snapshot) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("schema", "watchdog_progress.v1");
        payload.put("source", "python_stage1_heartbeat");
        payload.put("stage", firstNonBlank(snapshot.stage(), "stage1"));
        payload.put("status", firstNonBlank(snapshot.status(), "running"));
        payload.put("checkpoint", firstNonBlank(snapshot.checkpoint(), "unknown"));
        payload.put("completed", Math.max(0, snapshot.completed()));
        payload.put("pending", Math.max(0, snapshot.pending()));
        payload.put("seq", Math.max(0L, snapshot.seq()));
        payload.put("updated_at_ms", Math.max(0L, snapshot.updatedAtMs()));
        payload.put("signal_type", firstNonBlank(snapshot.signalType(), "hard"));
        try {
            return WATCHDOG_SIGNAL_PREFIX + objectMapper.writeValueAsString(payload);
        } catch (IOException serializationError) {
            logger.warn("Failed to serialize Stage1 watchdog signal payload", serializationError);
            return WATCHDOG_SIGNAL_PREFIX
                    + "{\"schema\":\"watchdog_progress.v1\",\"stage\":\"stage1\",\"status\":\"running\"}";
        }
    }

    private String readTextNode(JsonNode node, String fieldName) {
        if (node == null || fieldName == null || fieldName.isBlank()) {
            return "";
        }
        JsonNode child = node.get(fieldName);
        if (child == null || child.isNull()) {
            return "";
        }
        if (child.isTextual()) {
            return child.asText("");
        }
        return child.toString();
    }

    private int readIntNode(JsonNode node, String fieldName, int fallback) {
        if (node == null || fieldName == null || fieldName.isBlank()) {
            return fallback;
        }
        JsonNode child = node.get(fieldName);
        if (child == null || child.isNull()) {
            return fallback;
        }
        if (child.isNumber()) {
            return child.asInt(fallback);
        }
        if (child.isTextual()) {
            try {
                return Integer.parseInt(child.asText().trim());
            } catch (NumberFormatException ignored) {
                return fallback;
            }
        }
        return fallback;
    }

    private long readLongNode(JsonNode node, String fieldName, long fallback) {
        if (node == null || fieldName == null || fieldName.isBlank()) {
            return fallback;
        }
        JsonNode child = node.get(fieldName);
        if (child == null || child.isNull()) {
            return fallback;
        }
        if (child.isNumber()) {
            return child.asLong(fallback);
        }
        if (child.isTextual()) {
            try {
                return Long.parseLong(child.asText().trim());
            } catch (NumberFormatException ignored) {
                return fallback;
            }
        }
        return fallback;
    }

    private String firstNonBlank(String value, String fallback) {
        if (value != null && !value.isBlank()) {
            return value;
        }
        return fallback;
    }

    private String extractThrowableMessage(Throwable throwable) {
        if (throwable == null) {
            return "Unknown throwable";
        }
        Throwable cursor = throwable;
        String fallbackType = throwable.getClass().getSimpleName();
        int depth = 0;
        while (cursor != null && depth < 8) {
            String message = cursor.getMessage();
            if (message != null && !message.isBlank()) {
                if (depth == 0) {
                    return message;
                }
                return cursor.getClass().getSimpleName() + ": " + message;
            }
            fallbackType = cursor.getClass().getSimpleName();
            cursor = cursor.getCause();
            depth += 1;
        }
        return fallbackType + " (message unavailable)";
    }

    public record MonitorHandle(Path heartbeatPath, AtomicBoolean runningFlag, Thread monitorThread) {}

    @FunctionalInterface
    public interface SignalEmitter {
        void emit(double progress, String message);
    }

    private record Stage1HeartbeatSnapshot(
            String stage,
            String status,
            String checkpoint,
            int completed,
            int pending,
            long seq,
            long updatedAtMs,
            String signalType
    ) {}
}
