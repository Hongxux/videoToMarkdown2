package com.mvp.module2.fusion.service.watchdog;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mvp.module2.fusion.grpc.PythonGrpcClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
import java.util.concurrent.atomic.AtomicLong;

@Component
public class TaskProgressWatchdogBridge {
    private static final Logger logger = LoggerFactory.getLogger(TaskProgressWatchdogBridge.class);
    private static final String WATCHDOG_SIGNAL_PREFIX = "WATCHDOG_SIGNAL|";
    private static final String HEARTBEAT_FILE = "task_watchdog_heartbeat.json";

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ConcurrentHashMap<String, Long> heartbeatSeqCache = new ConcurrentHashMap<>();

    @Autowired(required = false)
    private PythonGrpcClient grpcClient;

    @Value("${video.task.watchdog.grpc-stream.enabled:true}")
    private boolean grpcStreamEnabled;

    @Value("${video.task.watchdog.grpc-stream.idle-timeout-seconds:25}")
    private int grpcStreamIdleTimeoutSec;

    @Value("${video.task.watchdog.grpc-stream.call-timeout-seconds:8}")
    private int grpcStreamCallTimeoutSec;

    @Value("${video.task.watchdog.grpc-stream.retry-backoff-ms:600}")
    private long grpcStreamRetryBackoffMs;

    @Value("${video.task.watchdog.grpc-stream.fallback-file-polling:true}")
    private boolean grpcFallbackFilePolling;

    @Value("${video.task.watchdog.file-poll-interval-ms:1000}")
    private long filePollIntervalMs;

    public void resetTask(String taskId) {
        if (taskId == null || taskId.isBlank()) {
            return;
        }
        heartbeatSeqCache.remove(taskId);
    }

    public MonitorHandle startMonitor(String taskId, String outputDir, SignalEmitter emitter) {
        return startMonitor(taskId, outputDir, "", emitter);
    }

    public MonitorHandle startMonitor(String taskId, String outputDir, String stage, SignalEmitter emitter) {
        Path heartbeatPath = resolveHeartbeatPath(outputDir);
        AtomicBoolean runningFlag = new AtomicBoolean(true);
        Thread monitorThread = startSignalThread(taskId, heartbeatPath, stage, runningFlag, emitter);
        return new MonitorHandle(heartbeatPath, runningFlag, monitorThread, stage);
    }

    public void stopMonitor(String taskId, MonitorHandle handle, SignalEmitter emitter) {
        if (taskId == null || taskId.isBlank()) {
            return;
        }
        if (handle != null) {
            handle.runningFlag().set(false);
            stopHeartbeatThread(handle.monitorThread());
            HeartbeatSnapshot finalHeartbeat = readHeartbeat(handle.heartbeatPath());
            if (finalHeartbeat != null) {
                emitWatchdogProgress(taskId, finalHeartbeat, handle.stage(), true, emitter);
            }
        }
        heartbeatSeqCache.remove(taskId);
    }

    private Path resolveHeartbeatPath(String outputDir) {
        if (outputDir == null || outputDir.isBlank()) {
            return null;
        }
        return Paths.get(outputDir, "intermediates", HEARTBEAT_FILE);
    }

    private Thread startSignalThread(
            String taskId,
            Path heartbeatPath,
            String stage,
            AtomicBoolean runningFlag,
            SignalEmitter emitter
    ) {
        if (taskId == null || taskId.isBlank() || runningFlag == null) {
            return null;
        }
        Thread monitor = new Thread(() -> {
            boolean needFallback = true;
            if (shouldUseGrpcStream()) {
                needFallback = runGrpcMonitorLoop(taskId, stage, runningFlag, emitter);
            }
            if (needFallback && grpcFallbackFilePolling) {
                runFileMonitorLoop(taskId, heartbeatPath, stage, runningFlag, emitter);
            }
        }, "TaskWatchdogSignalMonitor-" + taskId);
        monitor.setDaemon(true);
        monitor.start();
        return monitor;
    }

    private boolean shouldUseGrpcStream() {
        return grpcStreamEnabled && grpcClient != null;
    }

    private boolean runGrpcMonitorLoop(
            String taskId,
            String stage,
            AtomicBoolean runningFlag,
            SignalEmitter emitter
    ) {
        if (grpcClient == null) {
            return true;
        }
        String expectedStage = normalizeStage(stage);
        AtomicLong streamCursor = new AtomicLong(Math.max(0L, heartbeatSeqCache.getOrDefault(taskId, 0L)));

        while (runningFlag.get() && !Thread.currentThread().isInterrupted()) {
            long fromSeq = Math.max(0L, streamCursor.get());
            PythonGrpcClient.WatchdogSignalStreamResult streamResult =
                grpcClient.streamTaskWatchdogSignalsBlocking(
                    taskId,
                    expectedStage,
                    fromSeq,
                    Math.max(5, grpcStreamIdleTimeoutSec),
                    Math.max(2, grpcStreamCallTimeoutSec),
                    signal -> {
                        if (signal == null) {
                            return;
                        }
                        if (!runningFlag.get()) {
                            return;
                        }
                        long streamSeq = Math.max(0L, signal.streamSeq);
                        if (streamSeq > 0L) {
                            streamCursor.updateAndGet(prev -> Math.max(prev, streamSeq));
                        }
                        HeartbeatSnapshot snapshot = new HeartbeatSnapshot(
                            firstNonBlank(signal.stage, "unknown"),
                            firstNonBlank(signal.status, "running"),
                            firstNonBlank(signal.checkpoint, "unknown"),
                            Math.max(0, signal.completed),
                            Math.max(0, signal.pending),
                            Math.max(0L, signal.seq),
                            streamSeq,
                            Math.max(0L, signal.updatedAtMs),
                            firstNonBlank(signal.signalType, "hard")
                        );
                        emitWatchdogProgress(taskId, snapshot, expectedStage, false, emitter);
                    }
                );

            long latestSeq = Math.max(streamCursor.get(), Math.max(0L, streamResult.lastStreamSeq));
            streamCursor.set(latestSeq);

            if (!runningFlag.get() || Thread.currentThread().isInterrupted()) {
                return false;
            }
            if (streamResult.unsupported) {
                logger.warn("[{}] gRPC watchdog stream unsupported by Python worker, fallback to file polling", taskId);
                return true;
            }
            if (streamResult.cancelled) {
                return false;
            }
            if (streamResult.stageTerminal) {
                return false;
            }
            if (!streamResult.success && !streamResult.deadlineExceeded) {
                logger.warn(
                    "[{}] gRPC watchdog stream error: {} (fallback_file_polling={})",
                    taskId,
                    firstNonBlank(streamResult.errorMsg, "unknown"),
                    grpcFallbackFilePolling
                );
                return grpcFallbackFilePolling;
            }

            sleepQuietly(Math.max(0L, grpcStreamRetryBackoffMs));
        }
        return false;
    }

    private void runFileMonitorLoop(
            String taskId,
            Path heartbeatPath,
            String stage,
            AtomicBoolean runningFlag,
            SignalEmitter emitter
    ) {
        long pollMs = Math.max(200L, filePollIntervalMs);
        while (runningFlag.get() && !Thread.currentThread().isInterrupted()) {
            try {
                HeartbeatSnapshot snapshot = readHeartbeat(heartbeatPath);
                if (snapshot != null) {
                    emitWatchdogProgress(taskId, snapshot, stage, false, emitter);
                }
                Thread.sleep(pollMs);
            } catch (InterruptedException interruptedError) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception monitorError) {
                logger.debug(
                    "[{}] Task heartbeat monitor iteration failed: {}",
                    taskId,
                    extractThrowableMessage(monitorError)
                );
                sleepQuietly(pollMs);
            }
        }
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

    private HeartbeatSnapshot readHeartbeat(Path heartbeatPath) {
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
            String stage = firstNonBlank(readTextNode(root, "stage"), "unknown");
            String status = firstNonBlank(readTextNode(root, "status"), "running");
            String checkpoint = firstNonBlank(readTextNode(root, "checkpoint"), "unknown");
            int completed = readIntNode(root, "completed", 0);
            int pending = readIntNode(root, "pending", 0);
            long seq = readLongNode(root, "seq", 0L);
            long streamSeq = readLongNode(root, "stream_seq", 0L);
            long updatedAtMs = readLongNode(root, "updated_at_ms", 0L);
            String signalType = firstNonBlank(readTextNode(root, "signal_type"), "hard");
            return new HeartbeatSnapshot(stage, status, checkpoint, completed, pending, seq, streamSeq, updatedAtMs, signalType);
        } catch (IOException ioError) {
            logger.debug("Task heartbeat parse skipped, file may still be updating: {}", heartbeatPath, ioError);
            return null;
        }
    }

    private void emitWatchdogProgress(
            String taskId,
            HeartbeatSnapshot snapshot,
            String expectedStage,
            boolean forceEmit,
            SignalEmitter emitter
    ) {
        if (taskId == null || taskId.isBlank() || snapshot == null || emitter == null) {
            return;
        }
        String normalizedExpectedStage = normalizeStage(expectedStage);
        if (!normalizedExpectedStage.isBlank() && !normalizedExpectedStage.equals(normalizeStage(snapshot.stage()))) {
            return;
        }

        long dedupeSeq = Math.max(0L, snapshot.streamSeq() > 0 ? snapshot.streamSeq() : snapshot.seq());
        Long lastSeq = heartbeatSeqCache.get(taskId);
        if (!forceEmit && dedupeSeq > 0 && lastSeq != null && dedupeSeq <= lastSeq) {
            return;
        }
        if (dedupeSeq > 0) {
            heartbeatSeqCache.put(taskId, dedupeSeq);
        }
        double progress = computeProgress(snapshot);
        String message = buildWatchdogSignalMessage(snapshot);
        emitter.emit(progress, message);
    }

    private double computeProgress(HeartbeatSnapshot snapshot) {
        if (snapshot == null) {
            return 0.30d;
        }
        StageRange range = resolveRange(snapshot.stage());
        double completed = Math.max(0.0d, snapshot.completed());
        double pending = Math.max(0.0d, snapshot.pending());
        double total = Math.max(1.0d, completed + pending);
        double ratio = Math.max(0.0d, Math.min(1.0d, completed / total));
        double progress = range.start() + (range.end() - range.start()) * ratio;
        String status = firstNonBlank(snapshot.status(), "").toLowerCase(Locale.ROOT);
        if ("completed".equals(status)) {
            return range.end();
        }
        if ("failed".equals(status)) {
            return Math.max(range.start(), Math.min(range.end() - 0.01d, progress));
        }
        return Math.max(range.start(), Math.min(range.end(), progress));
    }

    private StageRange resolveRange(String stage) {
        String normalized = stage == null ? "" : stage.trim().toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "download" -> new StageRange(0.03d, 0.14d);
            case "transcribe" -> new StageRange(0.14d, 0.25d);
            case "stage1" -> new StageRange(0.25d, 0.35d);
            case "phase2a" -> new StageRange(0.35d, 0.45d);
            case "phase2b" -> new StageRange(0.90d, 0.98d);
            case "finalize" -> new StageRange(0.98d, 0.995d);
            default -> new StageRange(0.30d, 0.70d);
        };
    }

    private String buildWatchdogSignalMessage(HeartbeatSnapshot snapshot) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("schema", "watchdog_progress.v1");
        payload.put("source", "python_task_heartbeat");
        payload.put("stage", firstNonBlank(snapshot.stage(), "unknown"));
        payload.put("status", firstNonBlank(snapshot.status(), "running"));
        payload.put("checkpoint", firstNonBlank(snapshot.checkpoint(), "unknown"));
        payload.put("completed", Math.max(0, snapshot.completed()));
        payload.put("pending", Math.max(0, snapshot.pending()));
        payload.put("seq", Math.max(0L, snapshot.seq()));
        payload.put("stream_seq", Math.max(0L, snapshot.streamSeq()));
        payload.put("updated_at_ms", Math.max(0L, snapshot.updatedAtMs()));
        payload.put("signal_type", firstNonBlank(snapshot.signalType(), "hard"));
        try {
            return WATCHDOG_SIGNAL_PREFIX + objectMapper.writeValueAsString(payload);
        } catch (IOException serializationError) {
            logger.warn("Failed to serialize task watchdog signal payload", serializationError);
            return WATCHDOG_SIGNAL_PREFIX
                + "{\"schema\":\"watchdog_progress.v1\",\"stage\":\"unknown\",\"status\":\"running\",\"signal_type\":\"hard\"}";
        }
    }

    private String normalizeStage(String stage) {
        if (stage == null) {
            return "";
        }
        return stage.trim().toLowerCase(Locale.ROOT);
    }

    private void sleepQuietly(long sleepMs) {
        if (sleepMs <= 0) {
            return;
        }
        try {
            Thread.sleep(sleepMs);
        } catch (InterruptedException interruptedError) {
            Thread.currentThread().interrupt();
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

    public record MonitorHandle(Path heartbeatPath, AtomicBoolean runningFlag, Thread monitorThread, String stage) {}

    @FunctionalInterface
    public interface SignalEmitter {
        void emit(double progress, String message);
    }

    private record HeartbeatSnapshot(
        String stage,
        String status,
        String checkpoint,
        int completed,
        int pending,
        long seq,
        long streamSeq,
        long updatedAtMs,
        String signalType
    ) {}

    private record StageRange(double start, double end) {}
}
