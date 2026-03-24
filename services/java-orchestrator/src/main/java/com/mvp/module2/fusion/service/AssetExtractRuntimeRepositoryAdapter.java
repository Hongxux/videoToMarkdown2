package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AssetExtractRuntimeRepositoryAdapter {

    private static final String SCHEMA_VERSION = "asset_extract.runtime_repository.v1";
    private static final List<String> ORDERED_SUBSTAGES = List.of(
            "material_request_plan",
            "asset_extraction",
            "outputs_finalize"
    );

    private final ConcurrentHashMap<String, Map<String, Object>> entries = new ConcurrentHashMap<>();
    private final ObjectMapper objectMapper = new ObjectMapper();

    public Map<String, Object> seed(
            String outputDir,
            String taskId,
            String videoPath,
            String semanticUnitsPath
    ) {
        return upsert(outputDir, current -> {
            Map<String, Object> payload = current != null ? current : buildEmptyRepository(outputDir, taskId, videoPath, semanticUnitsPath);
            payload.put("task_id", normalizeText(taskId));
            payload.put("video_path", normalizeText(videoPath));
            payload.put("semantic_units_path", normalizeText(semanticUnitsPath));
            payload.put("updated_at_ms", System.currentTimeMillis());
            return payload;
        });
    }

    public Map<String, Object> markMaterialRequestsReady(
            String outputDir,
            String materialSource,
            int screenshotCount,
            int clipCount,
            boolean hasInflightExtraction
    ) {
        return upsert(outputDir, current -> {
            Map<String, Object> payload = current != null ? current : buildEmptyRepository(outputDir, "", "", "");
            payload.put("status", "RUNNING");
            payload.put("current_substage", "material_request_plan");
            payload.put("ready", false);
            Map<String, Object> views = ensureViews(payload);
            views.put("material_source", normalizeText(materialSource));
            views.put("screenshot_count", Math.max(0, screenshotCount));
            views.put("clip_count", Math.max(0, clipCount));
            views.put("has_inflight_extraction", hasInflightExtraction);
            views.put("fingerprint", buildFingerprint(views));
            updateWaveState(
                    payload,
                    "material_request_plan",
                    "SUCCESS",
                    Map.of(
                            "material_source", normalizeText(materialSource),
                            "screenshot_count", Math.max(0, screenshotCount),
                            "clip_count", Math.max(0, clipCount),
                            "has_inflight_extraction", hasInflightExtraction
                    )
            );
            payload.put("updated_at_ms", System.currentTimeMillis());
            return payload;
        });
    }

    public Map<String, Object> markExtractionRunning(
            String outputDir,
            int attempt,
            int ffmpegTimeoutSec
    ) {
        return upsert(outputDir, current -> {
            Map<String, Object> payload = current != null ? current : buildEmptyRepository(outputDir, "", "", "");
            payload.put("status", "RUNNING");
            payload.put("current_substage", "asset_extraction");
            payload.put("ready", false);
            ensureAttempts(payload).put("asset_extraction.wave_0001", Math.max(1, attempt));
            Map<String, Object> views = ensureViews(payload);
            views.put("ffmpeg_timeout_sec", Math.max(0, ffmpegTimeoutSec));
            updateWaveState(
                    payload,
                    "asset_extraction",
                    "RUNNING",
                    Map.of("ffmpeg_timeout_sec", Math.max(0, ffmpegTimeoutSec))
            );
            payload.put("updated_at_ms", System.currentTimeMillis());
            return payload;
        });
    }

    public Map<String, Object> markExtractionResult(
            String outputDir,
            JavaCVFFmpegService.ExtractionResult extractResult
    ) {
        return upsert(outputDir, current -> {
            Map<String, Object> payload = current != null ? current : buildEmptyRepository(outputDir, "", "", "");
            payload.put("status", "RUNNING");
            payload.put("current_substage", "asset_extraction");
            payload.put("ready", false);
            Map<String, Object> views = ensureViews(payload);
            views.put("assets_dir", normalizeText(firstNonBlank(extractResult != null ? extractResult.screenshotsDir : "", outputDir + "/assets")));
            views.put("screenshots_dir", normalizeText(firstNonBlank(extractResult != null ? extractResult.screenshotsDir : "", outputDir + "/assets")));
            views.put("clips_dir", normalizeText(firstNonBlank(extractResult != null ? extractResult.clipsDir : "", outputDir + "/assets")));
            views.put("successful_screenshots", Math.max(0, extractResult != null ? extractResult.successfulScreenshots : 0));
            views.put("successful_clips", Math.max(0, extractResult != null ? extractResult.successfulClips : 0));
            views.put("error_count", extractResult != null && extractResult.errors != null ? extractResult.errors.size() : 0);
            views.put("fingerprint", buildFingerprint(views));
            updateWaveState(
                    payload,
                    "asset_extraction",
                    "SUCCESS",
                    buildExtractionResultPayload(extractResult, outputDir)
            );
            payload.put("updated_at_ms", System.currentTimeMillis());
            return payload;
        });
    }

    public Map<String, Object> markOutputsReady(
            String outputDir,
            JavaCVFFmpegService.ExtractionResult extractResult
    ) {
        return upsert(outputDir, current -> {
            Map<String, Object> payload = current != null ? current : buildEmptyRepository(outputDir, "", "", "");
            payload.put("status", "READY");
            payload.put("current_substage", "outputs_finalize");
            payload.put("ready", true);
            Map<String, Object> views = ensureViews(payload);
            views.put("assets_dir", normalizeText(firstNonBlank(extractResult != null ? extractResult.screenshotsDir : "", outputDir + "/assets")));
            views.put("screenshots_dir", normalizeText(firstNonBlank(extractResult != null ? extractResult.screenshotsDir : "", outputDir + "/assets")));
            views.put("clips_dir", normalizeText(firstNonBlank(extractResult != null ? extractResult.clipsDir : "", outputDir + "/assets")));
            views.put("successful_screenshots", Math.max(0, extractResult != null ? extractResult.successfulScreenshots : 0));
            views.put("successful_clips", Math.max(0, extractResult != null ? extractResult.successfulClips : 0));
            views.put("error_count", extractResult != null && extractResult.errors != null ? extractResult.errors.size() : 0);
            views.put("fingerprint", buildFingerprint(views));
            updateWaveState(
                    payload,
                    "outputs_finalize",
                    "SUCCESS",
                    buildExtractionResultPayload(extractResult, outputDir)
            );
            payload.put("updated_at_ms", System.currentTimeMillis());
            return payload;
        });
    }

    public Map<String, Object> markFailed(
            String outputDir,
            String failedSubstage,
            String status,
            String errorMessage
    ) {
        return upsert(outputDir, current -> {
            Map<String, Object> payload = current != null ? current : buildEmptyRepository(outputDir, "", "", "");
            payload.put("status", normalizeText(status, "FAILED"));
            payload.put("current_substage", normalizeText(failedSubstage));
            payload.put("ready", false);
            payload.put("error_message", normalizeText(errorMessage));
            updateWaveState(
                    payload,
                    failedSubstage,
                    normalizeText(status, "FAILED"),
                    Map.of("error_message", normalizeText(errorMessage))
            );
            payload.put("updated_at_ms", System.currentTimeMillis());
            return payload;
        });
    }

    public Map<String, Object> rebuildFromStore(
            String outputDir,
            String taskId,
            String videoPath,
            String semanticUnitsPath,
            TaskRuntimeStageStore stageStore
    ) {
        return upsert(outputDir, current -> {
            Map<String, Object> payload = current != null ? current : buildEmptyRepository(outputDir, taskId, videoPath, semanticUnitsPath);
            payload.put("task_id", normalizeText(taskId));
            payload.put("video_path", normalizeText(videoPath));
            payload.put("semantic_units_path", normalizeText(semanticUnitsPath));
            Map<String, Object> waves = ensureWaves(payload);
            waves.clear();
            for (String substageName : ORDERED_SUBSTAGES) {
                waves.put(substageName + ".wave_0001", buildWaveEntry(substageName));
            }
            if (stageStore != null) {
                Map<String, Map<String, Object>> chunkNodesByScopeId = new LinkedHashMap<>();
                for (Map<String, Object> node : stageStore.listScopeNodes(outputDir, "asset_extract_java", "chunk")) {
                    String scopeId = normalizeText(String.valueOf(node.getOrDefault("scope_id", node.getOrDefault("chunk_id", ""))));
                    if (!scopeId.isBlank()) {
                        chunkNodesByScopeId.put(scopeId, new LinkedHashMap<>(node));
                    }
                }
                restoreWaveFromStore(payload, stageStore, "material_request_plan", chunkNodesByScopeId.get("material_request_plan.wave_0001"));
                restoreWaveFromStore(payload, stageStore, "asset_extraction", chunkNodesByScopeId.get("asset_extraction.wave_0001"));
                restoreWaveFromStore(payload, stageStore, "outputs_finalize", chunkNodesByScopeId.get("outputs_finalize.wave_0001"));
            }
            refreshRepositoryStatusFromWaves(payload);
            payload.put("updated_at_ms", System.currentTimeMillis());
            return payload;
        });
    }

    public Map<String, Object> get(String outputDir) {
        Map<String, Object> payload = entries.get(normalizeOutputDir(outputDir));
        return payload != null ? deepCopy(payload) : null;
    }

    public void clear(String outputDir) {
        entries.remove(normalizeOutputDir(outputDir));
    }

    private Map<String, Object> upsert(String outputDir, java.util.function.Function<Map<String, Object>, Map<String, Object>> mutator) {
        String key = normalizeOutputDir(outputDir);
        if (key.isBlank()) {
            return new LinkedHashMap<>();
        }
        Map<String, Object> updated = entries.compute(key, (ignored, current) -> mutator.apply(current != null ? deepCopy(current) : null));
        return deepCopy(updated);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> ensureViews(Map<String, Object> payload) {
        Object current = payload.get("views");
        if (current instanceof Map<?, ?> rawMap) {
            Map<String, Object> normalized = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
                normalized.put(String.valueOf(entry.getKey()), entry.getValue());
            }
            payload.put("views", normalized);
            return normalized;
        }
        Map<String, Object> views = new LinkedHashMap<>();
        views.put("semantic_units_path", normalizeText(String.valueOf(payload.getOrDefault("semantic_units_path", ""))));
        views.put("material_source", "");
        views.put("screenshot_count", 0);
        views.put("clip_count", 0);
        views.put("has_inflight_extraction", false);
        views.put("assets_dir", "");
        views.put("screenshots_dir", "");
        views.put("clips_dir", "");
        views.put("successful_screenshots", 0);
        views.put("successful_clips", 0);
        views.put("error_count", 0);
        views.put("fingerprint", "");
        payload.put("views", views);
        return views;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> ensureWaves(Map<String, Object> payload) {
        Object current = payload.get("waves");
        if (current instanceof Map<?, ?> rawMap) {
            Map<String, Object> normalized = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
                normalized.put(String.valueOf(entry.getKey()), entry.getValue());
            }
            payload.put("waves", normalized);
            return normalized;
        }
        Map<String, Object> waves = new LinkedHashMap<>();
        payload.put("waves", waves);
        return waves;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Integer> ensureAttempts(Map<String, Object> payload) {
        Object current = payload.get("attempts");
        if (current instanceof Map<?, ?> rawMap) {
            Map<String, Integer> normalized = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
                normalized.put(String.valueOf(entry.getKey()), safeInt(entry.getValue(), 0));
            }
            payload.put("attempts", normalized);
            return normalized;
        }
        Map<String, Integer> attempts = new LinkedHashMap<>();
        payload.put("attempts", attempts);
        return attempts;
    }

    private Map<String, Object> buildEmptyRepository(
            String outputDir,
            String taskId,
            String videoPath,
            String semanticUnitsPath
    ) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("schema_version", SCHEMA_VERSION);
        payload.put("output_dir", normalizeOutputDir(outputDir));
        payload.put("task_id", normalizeText(taskId));
        payload.put("video_path", normalizeText(videoPath));
        payload.put("semantic_units_path", normalizeText(semanticUnitsPath));
        payload.put("status", "PLANNED");
        payload.put("current_substage", "");
        payload.put("ready", false);
        payload.put("error_message", "");
        payload.put("attempts", new LinkedHashMap<>());
        payload.put("waves", new LinkedHashMap<>());
        payload.put("views", new LinkedHashMap<>());
        payload.put("updated_at_ms", System.currentTimeMillis());
        ensureViews(payload);
        ensureWaves(payload);
        return payload;
    }

    private void restoreWaveFromStore(
            Map<String, Object> payload,
            TaskRuntimeStageStore stageStore,
            String substageName,
            Map<String, Object> chunkNode
    ) {
        if (stageStore == null) {
            return;
        }
        Map<String, Object> wavePayload = new LinkedHashMap<>();
        if (chunkNode != null) {
            wavePayload.put("scope_id", normalizeText(String.valueOf(chunkNode.getOrDefault("scope_id", ""))));
            wavePayload.put("scope_ref", normalizeText(String.valueOf(chunkNode.getOrDefault("scope_ref", ""))));
            wavePayload.put("status", normalizeText(String.valueOf(chunkNode.getOrDefault("status", "PLANNED")), "PLANNED"));
            wavePayload.put("input_fingerprint", normalizeText(String.valueOf(chunkNode.getOrDefault("input_fingerprint", ""))));
            wavePayload.put("attempt_count", safeInt(chunkNode.get("attempt_count"), 0));
            wavePayload.put("result_hash", normalizeText(String.valueOf(chunkNode.getOrDefault("result_hash", ""))));
            Object planContext = chunkNode.get("plan_context");
            if (planContext instanceof Map<?, ?>) {
                wavePayload.put("plan_context", planContext);
            }
            Object resourceSnapshot = chunkNode.get("resource_snapshot");
            if (resourceSnapshot instanceof Map<?, ?>) {
                wavePayload.put("resource_snapshot", resourceSnapshot);
            }
            Map<String, Object> resultPayload = stageStore.loadCommittedChunkPayload(
                    normalizeText(String.valueOf(payload.getOrDefault("output_dir", ""))),
                    "asset_extract_java",
                    normalizeText(String.valueOf(chunkNode.getOrDefault("scope_id", ""))),
                    normalizeText(String.valueOf(chunkNode.getOrDefault("input_fingerprint", "")))
            );
            if (!resultPayload.isEmpty()) {
                wavePayload.put("restored", true);
                wavePayload.put("result_payload", resultPayload);
                if ("material_request_plan".equals(substageName)) {
                    applyMaterialRequestViews(payload, resultPayload, false);
                } else {
                    applyExtractionViews(payload, resultPayload);
                }
            }
        }
        updateWaveState(payload, substageName, normalizeText(String.valueOf(wavePayload.getOrDefault("status", "PLANNED")), "PLANNED"), wavePayload);
    }

    private void refreshRepositoryStatusFromWaves(Map<String, Object> payload) {
        Map<String, Object> waves = ensureWaves(payload);
        String repositoryStatus = "PLANNED";
        String currentSubstage = "";
        String errorMessage = "";
        boolean ready = false;
        for (int index = ORDERED_SUBSTAGES.size() - 1; index >= 0; index--) {
            String substageName = ORDERED_SUBSTAGES.get(index);
            Object rawWave = waves.get(substageName + ".wave_0001");
            if (!(rawWave instanceof Map<?, ?> wavePayload)) {
                continue;
            }
            String waveStatus = normalizeText(String.valueOf(mapValue(wavePayload, "status", "PLANNED")), "PLANNED");
            if ("SUCCESS".equals(waveStatus) && "outputs_finalize".equals(substageName)) {
                repositoryStatus = "READY";
                currentSubstage = substageName;
                ready = true;
                break;
            }
            if (List.of("MANUAL_NEEDED", "ERROR", "FAILED").contains(waveStatus)) {
                repositoryStatus = waveStatus;
                currentSubstage = substageName;
                errorMessage = normalizeText(String.valueOf(mapValue(wavePayload, "error_message", "")));
                break;
            }
            if ("RUNNING".equals(waveStatus)) {
                repositoryStatus = "RUNNING";
                currentSubstage = substageName;
                break;
            }
            if ("SUCCESS".equals(waveStatus)) {
                repositoryStatus = "RUNNING";
                currentSubstage = nextSubstageName(substageName);
                break;
            }
            if ("PLANNED".equals(waveStatus)) {
                repositoryStatus = "PLANNED";
                currentSubstage = substageName;
            }
        }
        payload.put("status", repositoryStatus);
        payload.put("current_substage", normalizeText(currentSubstage));
        payload.put("ready", ready);
        payload.put("error_message", normalizeText(errorMessage));
    }

    private Object mapValue(Map<?, ?> rawMap, String key, Object defaultValue) {
        return rawMap.containsKey(key) ? rawMap.get(key) : defaultValue;
    }

    private void updateWaveState(
            Map<String, Object> payload,
            String substageName,
            String status,
            Map<String, Object> extra
    ) {
        String normalizedSubstage = normalizeText(substageName);
        if (normalizedSubstage.isBlank()) {
            return;
        }
        Map<String, Object> waves = ensureWaves(payload);
        String waveKey = normalizedSubstage + ".wave_0001";
        Map<String, Object> wavePayload = buildWaveEntry(normalizedSubstage);
        Object current = waves.get(waveKey);
        if (current instanceof Map<?, ?> rawMap) {
            for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
                wavePayload.put(String.valueOf(entry.getKey()), entry.getValue());
            }
        }
        wavePayload.put("status", normalizeText(status, "PLANNED"));
        if (extra != null) {
            wavePayload.putAll(extra);
        }
        waves.put(waveKey, wavePayload);
    }

    private Map<String, Object> buildWaveEntry(String substageName) {
        Map<String, Object> wavePayload = new LinkedHashMap<>();
        wavePayload.put("substage_name", normalizeText(substageName));
        wavePayload.put("wave_id", "wave_0001");
        wavePayload.put("status", "PLANNED");
        wavePayload.put("input_fingerprint", "");
        wavePayload.put("attempt_count", 0);
        wavePayload.put("result_hash", "");
        return wavePayload;
    }

    private void applyMaterialRequestViews(
            Map<String, Object> payload,
            Map<String, Object> resultPayload,
            boolean hasInflightExtraction
    ) {
        Map<String, Object> views = ensureViews(payload);
        views.put("material_source", normalizeText(String.valueOf(resultPayload.getOrDefault("material_source", ""))));
        views.put("screenshot_count", safeInt(resultPayload.get("screenshot_count"), safeListSize(resultPayload.get("screenshot_requests"))));
        views.put("clip_count", safeInt(resultPayload.get("clip_count"), safeListSize(resultPayload.get("clip_requests"))));
        views.put("has_inflight_extraction", hasInflightExtraction);
        views.put("fingerprint", buildFingerprint(views));
    }

    private void applyExtractionViews(Map<String, Object> payload, Map<String, Object> resultPayload) {
        Map<String, Object> views = ensureViews(payload);
        String outputDir = normalizeText(String.valueOf(payload.getOrDefault("output_dir", "")));
        views.put("assets_dir", normalizeText(String.valueOf(resultPayload.getOrDefault("assets_dir", firstNonBlank(outputDir + "/assets", "")))));
        views.put("screenshots_dir", normalizeText(String.valueOf(resultPayload.getOrDefault("screenshots_dir", firstNonBlank(outputDir + "/assets", "")))));
        views.put("clips_dir", normalizeText(String.valueOf(resultPayload.getOrDefault("clips_dir", firstNonBlank(outputDir + "/assets", "")))));
        views.put("successful_screenshots", safeInt(resultPayload.get("successful_screenshots"), 0));
        views.put("successful_clips", safeInt(resultPayload.get("successful_clips"), 0));
        views.put("error_count", safeInt(resultPayload.get("error_count"), safeListSize(resultPayload.get("errors"))));
        views.put("fingerprint", buildFingerprint(views));
    }

    private Map<String, Object> buildExtractionResultPayload(
            JavaCVFFmpegService.ExtractionResult extractResult,
            String outputDir
    ) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("assets_dir", normalizeText(firstNonBlank(extractResult != null ? extractResult.screenshotsDir : "", outputDir + "/assets")));
        payload.put("screenshots_dir", normalizeText(firstNonBlank(extractResult != null ? extractResult.screenshotsDir : "", outputDir + "/assets")));
        payload.put("clips_dir", normalizeText(firstNonBlank(extractResult != null ? extractResult.clipsDir : "", outputDir + "/assets")));
        payload.put("successful_screenshots", Math.max(0, extractResult != null ? extractResult.successfulScreenshots : 0));
        payload.put("successful_clips", Math.max(0, extractResult != null ? extractResult.successfulClips : 0));
        payload.put("error_count", extractResult != null && extractResult.errors != null ? extractResult.errors.size() : 0);
        payload.put("errors", extractResult != null && extractResult.errors != null ? new ArrayList<>(extractResult.errors) : List.of());
        return payload;
    }

    private String nextSubstageName(String substageName) {
        int index = ORDERED_SUBSTAGES.indexOf(normalizeText(substageName));
        if (index < 0 || index + 1 >= ORDERED_SUBSTAGES.size()) {
            return normalizeText(substageName);
        }
        return ORDERED_SUBSTAGES.get(index + 1);
    }

    private int safeListSize(Object payload) {
        if (payload instanceof List<?> items) {
            return items.size();
        }
        return 0;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> deepCopy(Map<String, Object> payload) {
        try {
            return objectMapper.readValue(objectMapper.writeValueAsBytes(payload), LinkedHashMap.class);
        } catch (Exception error) {
            throw new IllegalStateException("deep copy asset extract repository failed", error);
        }
    }

    private String buildFingerprint(Map<String, Object> payload) {
        try {
            byte[] bytes = objectMapper.writeValueAsBytes(payload);
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(bytes);
            StringBuilder builder = new StringBuilder(hash.length * 2);
            for (byte item : hash) {
                builder.append(String.format("%02x", item));
            }
            return builder.toString();
        } catch (Exception error) {
            return "";
        }
    }

    private int safeInt(Object value, int fallback) {
        try {
            return Integer.parseInt(String.valueOf(value));
        } catch (Exception error) {
            return fallback;
        }
    }

    private String normalizeOutputDir(String outputDir) {
        String normalized = normalizeText(outputDir);
        return normalized.replace('\\', '/');
    }

    private String normalizeText(String value) {
        return normalizeText(value, "");
    }

    private String normalizeText(String value, String fallback) {
        String normalized = value == null ? "" : value.trim();
        return normalized.isBlank() ? fallback : normalized;
    }

    private String firstNonBlank(String first, String second) {
        String normalized = normalizeText(first);
        if (!normalized.isBlank()) {
            return normalized;
        }
        return normalizeText(second);
    }
}
