package com.mvp.module2.fusion.service;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

@Service
public class TaskRuntimeRedisRetentionService {

    private static final Logger logger = LoggerFactory.getLogger(TaskRuntimeRedisRetentionService.class);
    private static final Set<String> TERMINAL_STATUSES =
            Set.of("COMPLETED", "FAILED", "CANCELLED", "DEDUPED");

    private final boolean enabled;
    private final String redisUrl;
    private final String redisPrefix;
    private final long terminalRetentionMs;
    private volatile JedisPooled client;
    private volatile boolean initializationLogged;

    public TaskRuntimeRedisRetentionService(
            @Value("${task.runtime.redis.enabled:}") String configuredEnabled,
            @Value("${task.runtime.redis.url:}") String configuredUrl,
            @Value("${task.runtime.redis.prefix:}") String configuredPrefix,
            @Value("${task.runtime.redis.task-terminal-retention-hours:}") String configuredRetentionHours
    ) {
        this.enabled = parseBoolean(firstNonBlank(configuredEnabled, System.getenv("TASK_RUNTIME_REDIS_ENABLED")));
        this.redisUrl = firstNonBlank(configuredUrl, System.getenv("TASK_RUNTIME_REDIS_URL"));
        this.redisPrefix = firstNonBlank(configuredPrefix, System.getenv("TASK_RUNTIME_REDIS_PREFIX"), "rt");
        long retentionHours = Math.max(
                1L,
                parseLong(
                        firstNonBlank(
                                configuredRetentionHours,
                                System.getenv("TASK_RUNTIME_TASK_TERMINAL_RETENTION_HOURS")
                        ),
                        168L
                )
        );
        this.terminalRetentionMs = retentionHoursToMillis(retentionHours);
    }

    public void syncTaskRetention(String taskId, String taskStatus) {
        String normalizedTaskId = firstNonBlank(taskId);
        if (!enabled || normalizedTaskId == null || redisUrl == null) {
            return;
        }
        String normalizedStatus = firstNonBlank(taskStatus, "UNKNOWN").toUpperCase(Locale.ROOT);
        try {
            if (TERMINAL_STATUSES.contains(normalizedStatus)) {
                applyTerminalRetention(normalizedTaskId, normalizedStatus);
            } else {
                clearTaskRetention(normalizedTaskId, normalizedStatus);
            }
        } catch (Exception error) {
            logger.warn(
                    "Sync runtime redis retention failed: taskId={} status={} err={}",
                    normalizedTaskId,
                    normalizedStatus,
                    error.getMessage()
            );
        }
    }

    private void applyTerminalRetention(String taskId, String taskStatus) {
        JedisPooled jedis = getClient();
        if (jedis == null) {
            return;
        }
        long cleanupAfterMs = System.currentTimeMillis() + terminalRetentionMs;
        LinkedHashSet<String> keys = resolveTaskKeys(jedis, taskId);
        String metaKey = taskMetaKey(taskId);
        keys.add(metaKey);

        Map<String, String> metaUpdate = new LinkedHashMap<>();
        metaUpdate.put("task_status", taskStatus);
        metaUpdate.put("cleanup_after_ms", String.valueOf(cleanupAfterMs));
        jedis.hset(metaKey, metaUpdate);
        for (String key : keys) {
            jedis.pexpireAt(key, cleanupAfterMs);
        }
    }

    private void clearTaskRetention(String taskId, String taskStatus) {
        JedisPooled jedis = getClient();
        if (jedis == null) {
            return;
        }
        LinkedHashSet<String> keys = resolveTaskKeys(jedis, taskId);
        String metaKey = taskMetaKey(taskId);
        keys.add(metaKey);

        Map<String, String> metaUpdate = new LinkedHashMap<>();
        metaUpdate.put("task_status", taskStatus);
        jedis.hset(metaKey, metaUpdate);
        jedis.hdel(metaKey, "cleanup_after_ms");
        for (String key : keys) {
            jedis.persist(key);
        }
    }

    private LinkedHashSet<String> resolveTaskKeys(JedisPooled jedis, String taskId) {
        LinkedHashSet<String> keys = new LinkedHashSet<>();
        String pattern = taskKeyPrefix(taskId) + "*";
        String cursor = ScanParams.SCAN_POINTER_START;
        ScanParams scanParams = new ScanParams().match(pattern).count(100);
        do {
            ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
            keys.addAll(scanResult.getResult());
            cursor = scanResult.getCursor();
        } while (!ScanParams.SCAN_POINTER_START.equals(cursor));
        keys.add(taskEventsKey(taskId));
        return keys;
    }

    private String taskKeyPrefix(String taskId) {
        return redisPrefix + ":task:" + taskId + ":";
    }

    private String taskMetaKey(String taskId) {
        return taskKeyPrefix(taskId) + "meta";
    }

    private String taskEventsKey(String taskId) {
        return taskKeyPrefix(taskId) + "events";
    }

    private JedisPooled getClient() {
        if (!enabled || redisUrl == null) {
            return null;
        }
        JedisPooled localClient = client;
        if (localClient != null) {
            return localClient;
        }
        synchronized (this) {
            if (client != null) {
                return client;
            }
            try {
                client = new JedisPooled(URI.create(redisUrl));
                if (!initializationLogged) {
                    logger.info("Task runtime redis retention enabled: prefix={} retentionHours={}", redisPrefix, terminalRetentionMs / 3600_000L);
                    initializationLogged = true;
                }
            } catch (Exception error) {
                logger.warn("Init task runtime redis retention client failed: {}", error.getMessage());
                return null;
            }
            return client;
        }
    }

    @PreDestroy
    public void shutdown() {
        JedisPooled localClient = client;
        client = null;
        if (localClient == null) {
            return;
        }
        try {
            localClient.close();
        } catch (Exception error) {
            logger.debug("Close task runtime redis retention client skipped: {}", error.getMessage());
        }
    }

    private static boolean parseBoolean(String value) {
        String normalized = firstNonBlank(value);
        if (normalized == null) {
            return false;
        }
        normalized = normalized.toLowerCase(Locale.ROOT);
        return "1".equals(normalized)
                || "true".equals(normalized)
                || "yes".equals(normalized)
                || "on".equals(normalized);
    }

    private static long parseLong(String value, long defaultValue) {
        try {
            return Long.parseLong(firstNonBlank(value, String.valueOf(defaultValue)));
        } catch (Exception ignored) {
            return defaultValue;
        }
    }

    private static long retentionHoursToMillis(long retentionHours) {
        if (retentionHours >= Long.MAX_VALUE / 3600_000L) {
            return Long.MAX_VALUE;
        }
        return retentionHours * 3600_000L;
    }

    private static String firstNonBlank(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value.trim();
            }
        }
        return null;
    }
}
