package com.mvp.module2.fusion.service;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskStatusPresentationServiceTest {

    @Test
    void appendRecoveryFieldsShouldExposeBlockedProjection() {
        TaskStatusPresentationService presentationService = new TaskStatusPresentationService();
        Map<String, Object> recoveryPayload = new LinkedHashMap<>();
        recoveryPayload.put("stage", "phase2b");
        recoveryPayload.put("checkpoint", "llm_call_commit_pending");
        recoveryPayload.put("retryMode", "MANUAL_RETRY");
        recoveryPayload.put("requiredAction", "repair llm quota and retry");
        recoveryPayload.put("retryEntryPoint", "phase2b/chunk-42");
        recoveryPayload.put("retryStrategy", "resume_from_checkpoint");
        recoveryPayload.put("operatorAction", "requeue_after_quota_fix");
        recoveryPayload.put("actionHint", "restore quota first");
        recoveryPayload.put("errorClass", "provider_quota");
        recoveryPayload.put("errorMessage", "quota exhausted");
        recoveryPayload.put("internalOnly", "should-not-leak");

        Map<String, Object> response = new LinkedHashMap<>();
        presentationService.appendRecoveryFields(response, "MANUAL_RETRY_REQUIRED", recoveryPayload);

        assertEquals(true, response.get("blocked"));
        assertEquals("blocked", response.get("statusCategory"));
        assertEquals("phase2b", response.get("recoveryStage"));
        assertEquals("llm_call_commit_pending", response.get("recoveryCheckpoint"));
        assertEquals("phase2b/chunk-42", response.get("retryEntryPoint"));
        assertEquals("repair llm quota and retry", response.get("requiredAction"));
        assertTrue(response.get("recovery") instanceof Map<?, ?>);
        Map<?, ?> projectedRecovery = (Map<?, ?>) response.get("recovery");
        assertFalse(projectedRecovery.containsKey("internalOnly"));
    }

    @Test
    void sanitizeRecoveryPayloadShouldDropBlankFieldsAndKeepExpectedKeys() {
        TaskStatusPresentationService presentationService = new TaskStatusPresentationService();
        Map<String, Object> recoveryPayload = new LinkedHashMap<>();
        recoveryPayload.put("stage", " phase2a ");
        recoveryPayload.put("checkpoint", "  ");
        recoveryPayload.put("updatedAtMs", 123L);
        recoveryPayload.put("outputDir", "./output/demo");

        Map<String, Object> sanitized = presentationService.sanitizeRecoveryPayload(recoveryPayload);

        assertNotNull(sanitized);
        assertEquals("phase2a", sanitized.get("stage"));
        assertEquals(123L, sanitized.get("updatedAtMs"));
        assertEquals("./output/demo", sanitized.get("outputDir"));
        assertNull(sanitized.get("checkpoint"));
    }

    @Test
    void statusCategoryShouldClassifyLifecycleBuckets() {
        TaskStatusPresentationService presentationService = new TaskStatusPresentationService();

        assertEquals("queued", presentationService.resolveStatusCategory("queued"));
        assertEquals("processing", presentationService.resolveStatusCategory("processing"));
        assertEquals("blocked", presentationService.resolveStatusCategory("manual_retry_required"));
        assertEquals("failed", presentationService.resolveStatusCategory("failed"));
        assertEquals("completed", presentationService.resolveStatusCategory("completed"));
        assertEquals("cancelled", presentationService.resolveStatusCategory("cancelled"));
        assertEquals("unknown", presentationService.resolveStatusCategory("mystery"));
    }
}
