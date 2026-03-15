package com.mvp.module2.fusion.service;

import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

@Service
public class TaskStatusPresentationService {

    public void appendRecoveryFields(Map<String, Object> response, String rawStatus, Map<String, Object> recoveryPayload) {
        if (response == null) {
            return;
        }
        String status = normalizeStatusUpper(rawStatus);
        response.put("blocked", isBlockedStatus(status));
        response.put("statusCategory", resolveStatusCategory(status));
        Map<String, Object> sanitizedRecoveryPayload = sanitizeRecoveryPayload(recoveryPayload);
        if (sanitizedRecoveryPayload == null || sanitizedRecoveryPayload.isEmpty()) {
            return;
        }
        response.put("recovery", new LinkedHashMap<>(sanitizedRecoveryPayload));
        response.put("recoveryStage", stringifyRecoveryValue(sanitizedRecoveryPayload.get("stage")));
        response.put("recoveryCheckpoint", stringifyRecoveryValue(sanitizedRecoveryPayload.get("checkpoint")));
        response.put("retryMode", stringifyRecoveryValue(sanitizedRecoveryPayload.get("retryMode")));
        response.put("requiredAction", stringifyRecoveryValue(sanitizedRecoveryPayload.get("requiredAction")));
        response.put("retryEntryPoint", stringifyRecoveryValue(sanitizedRecoveryPayload.get("retryEntryPoint")));
        response.put("retryStrategy", stringifyRecoveryValue(sanitizedRecoveryPayload.get("retryStrategy")));
        response.put("operatorAction", stringifyRecoveryValue(sanitizedRecoveryPayload.get("operatorAction")));
        response.put("actionHint", stringifyRecoveryValue(sanitizedRecoveryPayload.get("actionHint")));
    }

    public Map<String, Object> sanitizeRecoveryPayload(Map<String, Object> recoveryPayload) {
        if (recoveryPayload == null || recoveryPayload.isEmpty()) {
            return null;
        }
        Map<String, Object> sanitized = new LinkedHashMap<>();
        copyRecoveryValue(sanitized, "stage", recoveryPayload.get("stage"));
        copyRecoveryValue(sanitized, "stageStatus", recoveryPayload.get("stageStatus"));
        copyRecoveryValue(sanitized, "checkpoint", recoveryPayload.get("checkpoint"));
        copyRecoveryValue(sanitized, "updatedAtMs", recoveryPayload.get("updatedAtMs"));
        copyRecoveryValue(sanitized, "retryMode", recoveryPayload.get("retryMode"));
        copyRecoveryValue(sanitized, "requiredAction", recoveryPayload.get("requiredAction"));
        copyRecoveryValue(sanitized, "retryEntryPoint", recoveryPayload.get("retryEntryPoint"));
        copyRecoveryValue(sanitized, "retryStrategy", recoveryPayload.get("retryStrategy"));
        copyRecoveryValue(sanitized, "operatorAction", recoveryPayload.get("operatorAction"));
        copyRecoveryValue(sanitized, "actionHint", recoveryPayload.get("actionHint"));
        copyRecoveryValue(sanitized, "errorClass", recoveryPayload.get("errorClass"));
        copyRecoveryValue(sanitized, "errorMessage", recoveryPayload.get("errorMessage"));
        copyRecoveryValue(sanitized, "outputDir", recoveryPayload.get("outputDir"));
        return sanitized.isEmpty() ? null : sanitized;
    }

    public boolean isProcessingStatus(String rawStatus) {
        String status = normalizeStatusUpper(rawStatus);
        return "PROBING".equals(status)
                || "PROCESSING".equals(status)
                || "RUNNING".equals(status)
                || "IN_PROGRESS".equals(status);
    }

    public boolean isQueuedStatus(String rawStatus) {
        String status = normalizeStatusUpper(rawStatus);
        return "QUEUED".equals(status)
                || "PENDING".equals(status)
                || "SUBMITTED".equals(status);
    }

    public boolean isCompletedStatus(String rawStatus) {
        return "COMPLETED".equals(normalizeStatusUpper(rawStatus));
    }

    public boolean isFailedStatus(String rawStatus) {
        String status = normalizeStatusUpper(rawStatus);
        return "FAILED".equals(status)
                || "MANUAL_RETRY_REQUIRED".equals(status)
                || "FATAL".equals(status);
    }

    public boolean isBlockedStatus(String rawStatus) {
        String status = normalizeStatusUpper(rawStatus);
        return "MANUAL_RETRY_REQUIRED".equals(status) || "FATAL".equals(status);
    }

    public boolean isCancelledStatus(String rawStatus) {
        return "CANCELLED".equals(normalizeStatusUpper(rawStatus));
    }

    public boolean isRunningStatus(String rawStatus) {
        String status = normalizeStatusUpper(rawStatus);
        return "QUEUED".equals(status)
                || "PENDING".equals(status)
                || "PROBING".equals(status)
                || "PROCESSING".equals(status)
                || "RUNNING".equals(status);
    }

    public String resolveStatusCategory(String rawStatus) {
        String status = normalizeStatusUpper(rawStatus);
        if (isProcessingStatus(status)) {
            return "processing";
        }
        if (isBlockedStatus(status)) {
            return "blocked";
        }
        if (isQueuedStatus(status)) {
            return "queued";
        }
        if (isCompletedStatus(status)) {
            return "completed";
        }
        if (isFailedStatus(status)) {
            return "failed";
        }
        if (isCancelledStatus(status)) {
            return "cancelled";
        }
        return "unknown";
    }

    public String normalizeStatusUpper(String rawStatus) {
        if (rawStatus == null || rawStatus.isBlank()) {
            return "";
        }
        return rawStatus.trim().toUpperCase(Locale.ROOT);
    }

    public String stringifyRecoveryValue(Object value) {
        if (value == null) {
            return "";
        }
        String text = String.valueOf(value).trim();
        return text.isEmpty() ? "" : text;
    }

    private void copyRecoveryValue(Map<String, Object> target, String key, Object value) {
        if (target == null || key == null || key.isBlank() || value == null) {
            return;
        }
        if (value instanceof String text) {
            String normalized = text.trim();
            if (normalized.isEmpty()) {
                return;
            }
            target.put(key, normalized);
            return;
        }
        target.put(key, value);
    }
}
