package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

@Service
public class TelemetryLlmInteractionLogService {
    private static final Logger logger = LoggerFactory.getLogger(TelemetryLlmInteractionLogService.class);
    private static final Pattern UNSAFE_PATH_SEGMENT = Pattern.compile("[^A-Za-z0-9._-]");

    @Value("${telemetry.llm-interaction-log.root:var/telemetry/llm-interactions}")
    private String logRoot;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Object writeLock = new Object();

    @Async("taskExecutor")
    public void appendAsync(String pipeline, String userKey, String taskId, Map<String, Object> payload) {
        try {
            Path root = Paths.get(logRoot).toAbsolutePath().normalize();
            String pipelineSegment = normalizeSegment(pipeline, "unknown");
            Path dir = root.resolve(pipelineSegment).normalize();
            if (!dir.startsWith(root)) {
                throw new IllegalStateException("invalid interaction log path");
            }
            Files.createDirectories(dir);

            Path target = dir.resolve(LocalDate.now(ZoneOffset.UTC) + ".ndjson");
            Map<String, Object> lineObject = new LinkedHashMap<>();
            lineObject.put("generatedAt", Instant.now().toString());
            lineObject.put("pipeline", pipelineSegment);
            lineObject.put("userKey", normalizeSegment(userKey, "anonymous"));
            lineObject.put("taskId", taskId == null ? "" : taskId.trim());
            lineObject.put("payload", payload == null ? Map.of() : payload);

            String line = objectMapper.writeValueAsString(lineObject) + '\n';
            synchronized (writeLock) {
                Files.writeString(
                        target,
                        line,
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.APPEND
                );
            }
        } catch (Exception ex) {
            logger.warn("append llm interaction log failed: {}", ex.getMessage());
        }
    }

    private String normalizeSegment(String raw, String fallback) {
        String value = raw == null ? "" : raw.trim();
        if (value.isBlank()) {
            value = fallback;
        }
        value = UNSAFE_PATH_SEGMENT.matcher(value).replaceAll("_").replaceAll("_+", "_");
        if (value.isBlank()) {
            return fallback;
        }
        return value;
    }
}
