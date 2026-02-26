package com.mvp.module2.fusion.worker.watchdog;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class TaskWatchdog {
    private final boolean enabled;
    private final String taskId;
    private final long startedAtMs;
    private final long maxTotalMs;
    private final int maxIdleStrikes;
    private final int maxRestartPerStage;
    private final long pollIntervalMs;
    private final long idleWindowMinMs;
    private final long idleWindowMaxMs;
    private final double idleWindowMultiplier;
    private final List<Long> restartBackoffMs;

    private final Deque<Long> progressIntervalsMs = new ArrayDeque<>();
    private final Map<String, Integer> stageRestartCounts = new HashMap<>();
    private long lastStrongProgressAtMs;
    private int idleStrikes;
    private double lastProgress;
    private String lastMessage;
    private String currentStage;
    private long lastSignalSeq;
    private int lastSignalCompleted;
    private int lastSignalPending;
    private String lastSignalCheckpoint;
    private String lastSignalStatus;

    private TaskWatchdog(
            boolean enabled,
            String taskId,
            int maxTotalSec,
            int maxIdleStrikes,
            int maxRestartPerStage,
            int pollIntervalSec,
            int idleWindowMinSec,
            int idleWindowMaxSec,
            double idleWindowMultiplier,
            List<Long> restartBackoffMs
    ) {
        this.enabled = enabled;
        this.taskId = taskId;
        this.startedAtMs = System.currentTimeMillis();
        this.maxTotalMs = maxTotalSec * 1000L;
        this.maxIdleStrikes = maxIdleStrikes;
        this.maxRestartPerStage = maxRestartPerStage;
        this.pollIntervalMs = pollIntervalSec * 1000L;
        this.idleWindowMinMs = idleWindowMinSec * 1000L;
        this.idleWindowMaxMs = idleWindowMaxSec * 1000L;
        this.idleWindowMultiplier = idleWindowMultiplier;
        this.restartBackoffMs = restartBackoffMs != null && !restartBackoffMs.isEmpty()
                ? new ArrayList<>(restartBackoffMs)
                : List.of(60_000L);
        this.lastStrongProgressAtMs = this.startedAtMs;
        this.idleStrikes = 0;
        this.lastProgress = -1.0d;
        this.lastMessage = "";
        this.currentStage = "bootstrap";
        this.lastSignalSeq = 0L;
        this.lastSignalCompleted = 0;
        this.lastSignalPending = -1;
        this.lastSignalCheckpoint = "";
        this.lastSignalStatus = "";
    }

    public static TaskWatchdog disabled(String taskId) {
        return new TaskWatchdog(false, taskId, 1, 1, 0, 1, 1, 1, 1.0d, List.of(1000L));
    }

    public static TaskWatchdog enabled(
            String taskId,
            int maxTotalSec,
            int maxIdleStrikes,
            int maxRestartPerStage,
            int pollIntervalSec,
            int idleWindowMinSec,
            int idleWindowMaxSec,
            double idleWindowMultiplier,
            List<Long> restartBackoffMs
    ) {
        return new TaskWatchdog(
                true,
                taskId,
                maxTotalSec,
                maxIdleStrikes,
                maxRestartPerStage,
                pollIntervalSec,
                idleWindowMinSec,
                idleWindowMaxSec,
                idleWindowMultiplier,
                restartBackoffMs
        );
    }

    public boolean enabled() {
        return enabled;
    }

    public long pollIntervalMs() {
        return pollIntervalMs;
    }

    public int maxRestartPerStage() {
        return maxRestartPerStage;
    }

    public synchronized void onAttemptStart(int attemptNo) {
        long now = System.currentTimeMillis();
        this.lastStrongProgressAtMs = now;
        this.idleStrikes = 0;
        this.lastProgress = -1.0d;
        this.lastMessage = "";
        this.currentStage = "attempt_" + attemptNo;
        this.lastSignalSeq = 0L;
        this.lastSignalCompleted = 0;
        this.lastSignalPending = -1;
        this.lastSignalCheckpoint = "";
        this.lastSignalStatus = "";
    }

    public synchronized void recordProgress(double progress, String message, Signal signal) {
        long now = System.currentTimeMillis();
        String normalizedMessage = message == null ? "" : message.trim();
        String previousStage = this.currentStage;
        String stage = signal != null ? signal.stage() : resolveStage(progress, normalizedMessage);
        if (stage != null && !stage.isBlank()) {
            this.currentStage = stage;
        }

        boolean strongSignal = isStrongStructuredSignal(signal, previousStage);
        if (progress > this.lastProgress + 0.0001d) {
            strongSignal = true;
        }
        if (signal == null && !normalizedMessage.isBlank() && !normalizedMessage.equals(this.lastMessage)) {
            strongSignal = true;
        }
        this.lastProgress = progress;
        this.lastMessage = normalizedMessage;
        rememberStructuredSignal(signal);

        if (!strongSignal) {
            return;
        }
        long interval = now - this.lastStrongProgressAtMs;
        if (interval > 0) {
            this.progressIntervalsMs.addLast(interval);
            while (this.progressIntervalsMs.size() > 24) {
                this.progressIntervalsMs.removeFirst();
            }
        }
        this.lastStrongProgressAtMs = now;
        this.idleStrikes = 0;
    }

    private boolean isStrongStructuredSignal(Signal signal, String previousStage) {
        if (signal == null) {
            return false;
        }
        if (!signal.isHard()) {
            return false;
        }
        String stage = normalizeToken(signal.stage());
        String previous = normalizeToken(previousStage);
        if (!stage.isBlank() && !stage.equals(previous)) {
            return true;
        }
        if (signal.seq() > 0 && signal.seq() > this.lastSignalSeq) {
            return true;
        }
        if (signal.completed() > this.lastSignalCompleted) {
            return true;
        }
        if (this.lastSignalPending < 0) {
            return true;
        }
        if (signal.pending() >= 0 && signal.pending() < this.lastSignalPending) {
            return true;
        }
        String checkpoint = signal.checkpoint() == null ? "" : signal.checkpoint().trim();
        if (!checkpoint.isBlank() && !checkpoint.equals(this.lastSignalCheckpoint)) {
            return true;
        }
        String status = signal.status() == null ? "" : signal.status().trim().toLowerCase(Locale.ROOT);
        return !status.isBlank() && !status.equals(this.lastSignalStatus);
    }

    private void rememberStructuredSignal(Signal signal) {
        if (signal == null) {
            return;
        }
        if (!signal.isHard()) {
            return;
        }
        if (signal.seq() > 0) {
            this.lastSignalSeq = Math.max(this.lastSignalSeq, signal.seq());
        }
        this.lastSignalCompleted = Math.max(this.lastSignalCompleted, signal.completed());
        if (signal.pending() >= 0) {
            this.lastSignalPending = this.lastSignalPending < 0
                    ? signal.pending()
                    : Math.min(this.lastSignalPending, signal.pending());
        }
        String checkpoint = signal.checkpoint() == null ? "" : signal.checkpoint().trim();
        if (!checkpoint.isBlank()) {
            this.lastSignalCheckpoint = checkpoint;
        }
        String status = signal.status() == null ? "" : signal.status().trim().toLowerCase(Locale.ROOT);
        if (!status.isBlank()) {
            this.lastSignalStatus = status;
        }
    }

    private String normalizeToken(String value) {
        if (value == null) {
            return "";
        }
        return value.trim().toLowerCase(Locale.ROOT);
    }

    public synchronized Decision evaluate(long nowMs) {
        if (!enabled) {
            return Decision.none();
        }
        long totalElapsedMs = nowMs - this.startedAtMs;
        if (totalElapsedMs >= this.maxTotalMs) {
            return new Decision(
                    Action.FAIL,
                    String.format(
                            "Task watchdog total timeout exceeded: task=%s elapsed=%ds limit=%ds",
                            taskId,
                            totalElapsedMs / 1000,
                            maxTotalMs / 1000
                    ),
                    currentStage,
                    stageRestartCounts.getOrDefault(currentStage, 0),
                    0L
            );
        }

        long idleWindowMs = computeIdleWindowMs();
        long idleElapsedMs = nowMs - this.lastStrongProgressAtMs;
        if (idleElapsedMs < idleWindowMs) {
            return Decision.none();
        }

        this.idleStrikes += 1;
        String stage = currentStage != null && !currentStage.isBlank() ? currentStage : "unknown";
        if (this.idleStrikes >= this.maxIdleStrikes) {
            return new Decision(
                    Action.FAIL,
                    String.format(
                            "Task watchdog idle strikes exhausted: task=%s stage=%s idle_strikes=%d/%d idle_elapsed=%ds window=%ds",
                            taskId,
                            stage,
                            idleStrikes,
                            maxIdleStrikes,
                            idleElapsedMs / 1000,
                            idleWindowMs / 1000
                    ),
                    stage,
                    stageRestartCounts.getOrDefault(stage, 0),
                    0L
            );
        }

        int used = stageRestartCounts.getOrDefault(stage, 0);
        if (used >= maxRestartPerStage) {
            return new Decision(
                    Action.FAIL,
                    String.format(
                            "Task watchdog restart quota exhausted: task=%s stage=%s used=%d limit=%d",
                            taskId,
                            stage,
                            used,
                            maxRestartPerStage
                    ),
                    stage,
                    used,
                    0L
            );
        }

        int stageRestartCount = used + 1;
        stageRestartCounts.put(stage, stageRestartCount);
        this.lastStrongProgressAtMs = nowMs;
        long backoffMs = resolveBackoffMs(stageRestartCount);
        String reason = String.format(
                "Task watchdog idle strike: task=%s stage=%s idle_strike=%d/%d restart=%d/%d backoff=%ds",
                taskId,
                stage,
                idleStrikes,
                maxIdleStrikes,
                stageRestartCount,
                maxRestartPerStage,
                backoffMs / 1000
        );
        return new Decision(Action.RESTART, reason, stage, stageRestartCount, backoffMs);
    }

    private long computeIdleWindowMs() {
        if (progressIntervalsMs.isEmpty()) {
            return idleWindowMinMs;
        }
        List<Long> sorted = new ArrayList<>(progressIntervalsMs);
        Collections.sort(sorted);
        int index = (int) Math.ceil(sorted.size() * 0.95d) - 1;
        index = Math.max(0, Math.min(index, sorted.size() - 1));
        long p95 = sorted.get(index);
        long candidate = (long) Math.ceil(p95 * idleWindowMultiplier);
        candidate = Math.max(candidate, idleWindowMinMs);
        return Math.min(candidate, idleWindowMaxMs);
    }

    private long resolveBackoffMs(int stageRestartCount) {
        if (restartBackoffMs.isEmpty()) {
            return 0L;
        }
        int index = Math.max(0, Math.min(stageRestartCount - 1, restartBackoffMs.size() - 1));
        return Math.max(0L, restartBackoffMs.get(index));
    }

    private String resolveStage(double progress, String message) {
        String lower = message == null ? "" : message.toLowerCase(Locale.ROOT);
        if (lower.contains("download") || lower.contains("下载")) {
            return "download";
        }
        if (lower.contains("transcribe") || lower.contains("转录")) {
            return "transcribe";
        }
        if (lower.contains("stage1") || lower.contains("结构化")) {
            return "stage1";
        }
        if (lower.contains("phase2a") || lower.contains("语义分割")) {
            return "phase2a";
        }
        if (lower.contains("vl") || lower.contains("素材提取") || lower.contains("analysis")) {
            return "analysis_extraction";
        }
        if (lower.contains("assemble") || lower.contains("markdown") || lower.contains("文档")) {
            return "phase2b";
        }
        if (progress <= 0.10d) {
            return "download";
        }
        if (progress <= 0.22d) {
            return "transcribe";
        }
        if (progress <= 0.30d) {
            return "stage1";
        }
        if (progress <= 0.42d) {
            return "phase2a";
        }
        if (progress <= 0.89d) {
            return "analysis_extraction";
        }
        if (progress <= 0.98d) {
            return "phase2b";
        }
        return "finalize";
    }

    public enum Action {
        NONE,
        RESTART,
        FAIL
    }

    public record Decision(
            Action action,
            String reason,
            String stage,
            int stageRestartCount,
            long backoffMs
    ) {
        private static final Decision NONE = new Decision(Action.NONE, "", "unknown", 0, 0L);

        public static Decision none() {
            return NONE;
        }
    }

    public record Signal(
            String stage,
            String status,
            String checkpoint,
            int completed,
            int pending,
            long seq,
            String signalType
    ) {
        public boolean isHard() {
            String normalized = signalType == null ? "" : signalType.trim().toLowerCase(Locale.ROOT);
            return normalized.isBlank() || "hard".equals(normalized);
        }
    }
}
