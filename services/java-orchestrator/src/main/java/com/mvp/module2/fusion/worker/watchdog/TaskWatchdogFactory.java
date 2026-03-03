package com.mvp.module2.fusion.worker.watchdog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

@Component
public class TaskWatchdogFactory {
    private static final Logger logger = LoggerFactory.getLogger(TaskWatchdogFactory.class);

    @Value("${video.task.watchdog.enabled:true}")
    private boolean taskWatchdogEnabled;

    @Value("${video.task.watchdog.max-total-seconds:28800}")
    private int watchdogMaxTotalSec;

    @Value("${video.task.watchdog.max-idle-strikes:3}")
    private int watchdogMaxIdleStrikes;

    @Value("${video.task.watchdog.max-restart-per-stage:2}")
    private int watchdogMaxRestartPerStage;

    @Value("${video.task.watchdog.poll-interval-seconds:10}")
    private int watchdogPollIntervalSec;

    @Value("${video.task.watchdog.idle-window-min-seconds:180}")
    private int watchdogIdleWindowMinSec;

    @Value("${video.task.watchdog.idle-window-max-seconds:1800}")
    private int watchdogIdleWindowMaxSec;

    @Value("${video.task.watchdog.idle-window-multiplier:3.0}")
    private double watchdogIdleWindowMultiplier;

    @Value("${video.task.watchdog.restart-backoff-seconds:60,180,420}")
    private String watchdogRestartBackoffSeconds;

    @Value("${video.task.watchdog.heartbeat-strong-stages:stage1,phase2a,analysis_extraction,phase2b}")
    private String watchdogHeartbeatStrongStages;

    public TaskWatchdog create(String taskId) {
        if (!taskWatchdogEnabled) {
            return TaskWatchdog.disabled(taskId);
        }
        int maxTotal = Math.max(60, watchdogMaxTotalSec);
        int maxIdleStrikes = Math.max(1, watchdogMaxIdleStrikes);
        int maxRestartPerStage = Math.max(0, watchdogMaxRestartPerStage);
        int pollIntervalSec = Math.max(1, watchdogPollIntervalSec);
        int idleWindowMinSec = Math.max(30, watchdogIdleWindowMinSec);
        int idleWindowMaxSec = Math.max(idleWindowMinSec, watchdogIdleWindowMaxSec);
        double idleMultiplier = watchdogIdleWindowMultiplier > 0 ? watchdogIdleWindowMultiplier : 3.0d;
        List<Long> restartBackoffMs = parseBackoffMs(watchdogRestartBackoffSeconds);
        Set<String> heartbeatStrongStages = parseStageSet(watchdogHeartbeatStrongStages);
        return TaskWatchdog.enabled(
                taskId,
                maxTotal,
                maxIdleStrikes,
                maxRestartPerStage,
                pollIntervalSec,
                idleWindowMinSec,
                idleWindowMaxSec,
                idleMultiplier,
                restartBackoffMs,
                heartbeatStrongStages
        );
    }

    private List<Long> parseBackoffMs(String raw) {
        List<Long> result = new ArrayList<>();
        if (raw == null || raw.isBlank()) {
            result.add(60_000L);
            result.add(180_000L);
            result.add(420_000L);
            return result;
        }
        String[] parts = raw.split(",");
        for (String part : parts) {
            String value = part.trim();
            if (value.isEmpty()) {
                continue;
            }
            try {
                long sec = Long.parseLong(value);
                if (sec > 0) {
                    result.add(sec * 1000L);
                }
            } catch (NumberFormatException ignored) {
                logger.warn("Ignore invalid watchdog backoff value: {}", value);
            }
        }
        if (result.isEmpty()) {
            result.add(60_000L);
        }
        return result;
    }

    private Set<String> parseStageSet(String raw) {
        if (raw == null || raw.isBlank()) {
            return Set.of();
        }
        Set<String> result = new LinkedHashSet<>();
        String[] parts = raw.split(",");
        for (String part : parts) {
            String token = part == null ? "" : part.trim().toLowerCase(Locale.ROOT);
            if (!token.isBlank()) {
                result.add(token);
            }
        }
        if (result.isEmpty()) {
            return Set.of();
        }
        return Set.copyOf(result);
    }
}
