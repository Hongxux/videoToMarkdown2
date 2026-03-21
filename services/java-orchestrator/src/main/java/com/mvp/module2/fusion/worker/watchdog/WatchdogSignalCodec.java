package com.mvp.module2.fusion.worker.watchdog;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Locale;

@Component
public class WatchdogSignalCodec {
    private static final Logger logger = LoggerFactory.getLogger(WatchdogSignalCodec.class);
    private static final String WATCHDOG_SIGNAL_PREFIX = "WATCHDOG_SIGNAL|";

    private final ObjectMapper mapper = new ObjectMapper();

    public TaskWatchdog.Signal parse(String message) {
        if (message == null) {
            return null;
        }
        String normalized = message.trim();
        if (!normalized.startsWith(WATCHDOG_SIGNAL_PREFIX)) {
            return null;
        }
        String payload = normalized.substring(WATCHDOG_SIGNAL_PREFIX.length()).trim();
        if (payload.isEmpty()) {
            return null;
        }
        try {
            JsonNode root = mapper.readTree(payload);
            if (root == null || !root.isObject()) {
                return null;
            }
            String stage = readTextField(root, "stage", "unknown");
            String status = readTextField(root, "status", "running");
            String checkpoint = readTextField(root, "checkpoint", "");
            int completed = readIntField(root, "completed", 0);
            int pending = readIntField(root, "pending", 0);
            long seq = readLongField(root, "seq", 0L);
            String signalType = readTextField(root, "signal_type", readTextField(root, "signal", ""));
            return new TaskWatchdog.Signal(stage, status, checkpoint, completed, pending, seq, signalType);
        } catch (Exception parseError) {
            logger.debug("Ignore invalid watchdog signal payload: {}", parseError.getMessage());
            return null;
        }
    }

    public String sanitizeForUser(String rawMessage, TaskWatchdog.Signal signal) {
        String normalized = rawMessage == null ? "" : rawMessage.trim();
        if (!normalized.startsWith(WATCHDOG_SIGNAL_PREFIX)) {
            return normalized;
        }
        if (signal == null) {
            return "Pipeline running";
        }
        String status = signal.status() == null ? "" : signal.status().trim().toLowerCase(Locale.ROOT);
        String checkpoint = signal.checkpoint() == null ? "" : signal.checkpoint().trim();
        String stageLabel = toDisplayStageName(signal.stage());
        if ("completed".equals(status)) {
            return stageLabel + " completed";
        }
        if ("failed".equals(status)) {
            return stageLabel + " failed";
        }
        String userFacingCheckpointMessage = resolveCheckpointMessage(signal);
        if (!userFacingCheckpointMessage.isBlank()) {
            return userFacingCheckpointMessage;
        }
        if (!checkpoint.isBlank()) {
            return stageLabel + " running (" + checkpoint + ")";
        }
        return stageLabel + " running";
    }

    private String resolveCheckpointMessage(TaskWatchdog.Signal signal) {
        if (signal == null) {
            return "";
        }
        String stage = signal.stage() == null ? "" : signal.stage().trim().toLowerCase(Locale.ROOT);
        String checkpoint = signal.checkpoint() == null ? "" : signal.checkpoint().trim().toLowerCase(Locale.ROOT);
        if ("phase2a".equals(stage) && "phase2a_segmentation_running".equals(checkpoint)) {
            return "已开始 Phase2A 语义分割 LLM 调用，正在等待结果...";
        }
        return "";
    }

    private String toDisplayStageName(String rawStage) {
        String normalized = rawStage == null ? "" : rawStage.trim().toLowerCase(Locale.ROOT);
        if (normalized.isBlank()) {
            return "Pipeline";
        }
        return switch (normalized) {
            case "download" -> "Download";
            case "transcribe" -> "Transcribe";
            case "book_pdf_extract" -> "Book PDF";
            case "stage1" -> "Stage1";
            case "phase2a" -> "Phase2A";
            case "analysis_extraction" -> "Analysis";
            case "phase2b" -> "Phase2B";
            case "finalize" -> "Finalize";
            default -> normalized;
        };
    }

    private String readTextField(JsonNode node, String field, String fallback) {
        if (node == null || field == null || field.isBlank()) {
            return fallback;
        }
        JsonNode child = node.get(field);
        if (child == null || child.isNull()) {
            return fallback;
        }
        if (child.isTextual()) {
            String value = child.asText("");
            return value == null || value.isBlank() ? fallback : value;
        }
        String value = child.toString();
        return value == null || value.isBlank() ? fallback : value;
    }

    private int readIntField(JsonNode node, String field, int fallback) {
        if (node == null || field == null || field.isBlank()) {
            return fallback;
        }
        JsonNode child = node.get(field);
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

    private long readLongField(JsonNode node, String field, long fallback) {
        if (node == null || field == null || field.isBlank()) {
            return fallback;
        }
        JsonNode child = node.get(field);
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
}
