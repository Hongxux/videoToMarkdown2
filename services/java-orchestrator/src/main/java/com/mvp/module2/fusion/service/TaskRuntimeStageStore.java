package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.zip.InflaterInputStream;

@Service
public class TaskRuntimeStageStore {

    private static final Logger logger = LoggerFactory.getLogger(TaskRuntimeStageStore.class);
    private static final String RESUME_INDEX_SCHEMA_VERSION = "runtime_resume_index_v2";
    private static final String STAGE_STATE_SCHEMA_VERSION = "runtime_stage_state_v2";
    private static final String STAGE_JOURNAL_SCHEMA_VERSION = "runtime_stage_journal_event_v1";
    private static final String OUTPUTS_MANIFEST_SCHEMA_VERSION = "runtime_stage_outputs_manifest_v1";
    private static final String DEFAULT_STAGE_GRAPH_VERSION = "video_pipeline_v2";
    private static final String RUNTIME_FALLBACK_RECORD_SCHEMA_VERSION = "runtime_fallback_record_v1";
    private static final String RUNTIME_ERROR_RECORD_SCHEMA_VERSION = "runtime_error_record_v1";
    private static final String RUNTIME_MANUAL_RETRY_RECORD_SCHEMA_VERSION = "runtime_manual_retry_record_v1";
    private static final String RUNTIME_STATE_DB_FILE_NAME = "runtime_state.db";
    private static final String STAGE_SNAPSHOT_TABLE = "stage_snapshots";
    private static final String SCOPE_NODES_TABLE = "scope_nodes";
    private static final String SCOPE_EDGES_TABLE = "scope_edges";
    private static final int SQLITE_BUSY_TIMEOUT_MS = 5000;
    private static final String STATUS_PLANNED = "PLANNED";
    private static final String STATUS_RUNNING = "RUNNING";
    private static final String STATUS_SUCCESS = "SUCCESS";
    private static final String STATUS_ERROR = "ERROR";
    private static final String STATUS_FAILED = "FAILED";
    private static final String STATUS_MANUAL_NEEDED = "MANUAL_NEEDED";
    private static final Set<String> RESETTABLE_RUNNING_SCOPE_STATUSES = Set.of("RUNNING", "EXECUTING", "LOCAL_WRITING");
    private static final String SCOPE_TYPE_SUBSTAGE = "substage";
    private static final String SCOPE_TYPE_CHUNK = "chunk";

    private final ObjectMapper objectMapper;
    private final Object runtimeStateDbSchemaLock = new Object();

    public TaskRuntimeStageStore(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public record StageSnapshotRecord(
            String stage,
            String taskId,
            String stageOwner,
            String status,
            String checkpoint,
            long updatedAtMs,
            int completed,
            int pending,
            String outputDir,
            String stageStatePath,
            Map<String, Object> payload
    ) {
        public StageSnapshotRecord {
            Map<String, Object> normalizedPayload = payload == null ? Map.of() : new LinkedHashMap<>(payload);
            payload = Collections.unmodifiableMap(normalizedPayload);
        }
    }

    private record ScopeNodeWriteDecision(
            String status,
            String inputFingerprint,
            String localPath,
            String chunkId,
            String unitId,
            String stageStep,
            String retryMode,
            String retryEntryPoint,
            String requiredAction,
            String errorClass,
            String errorCode,
            String errorMessage,
            String dirtyReason,
            long dirtyAtMs,
            Map<String, Object> planContext,
            Map<String, Object> resourceSnapshot,
            int attemptCount,
            String resultHash,
            long updatedAtMs
    ) {
        private static ScopeNodeWriteDecision fromFallback(String inputFingerprint, Map<String, Object> payload) {
            return new ScopeNodeWriteDecision(
                    STATUS_PLANNED,
                    normalizeStaticText(inputFingerprint),
                    normalizeStaticText(String.valueOf(payload.getOrDefault("local_path", ""))),
                    normalizeStaticText(String.valueOf(payload.getOrDefault("chunk_id", ""))),
                    normalizeStaticText(String.valueOf(payload.getOrDefault("unit_id", ""))),
                    normalizeStaticText(String.valueOf(payload.getOrDefault("stage_step", ""))),
                    normalizeStaticText(String.valueOf(payload.getOrDefault("retry_mode", ""))),
                    normalizeStaticText(String.valueOf(payload.getOrDefault("retry_entry_point", ""))),
                    normalizeStaticText(String.valueOf(payload.getOrDefault("required_action", ""))),
                    normalizeStaticText(String.valueOf(payload.getOrDefault("error_class", ""))),
                    normalizeStaticText(String.valueOf(payload.getOrDefault("error_code", ""))),
                    normalizeStaticText(String.valueOf(payload.getOrDefault("error_message", ""))),
                    normalizeStaticText(String.valueOf(payload.getOrDefault("dirty_reason", ""))),
                    parseStaticLong(payload.get("dirty_at_ms"), 0L),
                    extractStaticMapPayload(payload.get("plan_context")),
                    extractStaticMapPayload(payload.get("resource_snapshot")),
                    Math.max(0, parseStaticInt(payload.get("attempt_count"), 0)),
                    normalizeStaticText(String.valueOf(payload.getOrDefault("result_hash", ""))),
                    Math.max(0L, parseStaticLong(payload.get("updated_at_ms"), System.currentTimeMillis()))
            );
        }
    }

    public Path runtimeRoot(String outputDir) {
        return resolveTaskRoot(outputDir).resolve("intermediates").resolve("rt");
    }

    public Path runtimeStateDbPath(String outputDir) {
        return runtimeRoot(outputDir).resolve(RUNTIME_STATE_DB_FILE_NAME);
    }

    public Path stageDir(String outputDir, String stage) {
        return runtimeRoot(outputDir).resolve("stage").resolve(normalizeStage(stage));
    }

    public Path stageStatePath(String outputDir, String stage) {
        return stageDir(outputDir, stage).resolve("stage_state.json");
    }

    public Path stageJournalPath(String outputDir, String stage) {
        return stageDir(outputDir, stage).resolve("stage_journal.jsonl");
    }

    public Path outputsManifestPath(String outputDir, String stage) {
        return stageDir(outputDir, stage).resolve("outputs_manifest.json");
    }

    public Path stageArtifactPath(String outputDir, String stage, String filename) {
        return stageDir(outputDir, stage).resolve(normalizeFilename(filename));
    }

    public Map<String, Object> loadProjectionPayload(String outputDir, String stage, String projectionName) {
        Path dbPath = runtimeStateDbPath(outputDir);
        if (!Files.isRegularFile(dbPath)) {
            return new LinkedHashMap<>();
        }
        try {
            ensureRuntimeStateDbSchema(dbPath);
            try (Connection connection = openRuntimeStateConnection(dbPath);
                 PreparedStatement statement = connection.prepareStatement(
                          """
                         SELECT
                             m.result_hash,
                             c.result_codec,
                             c.result_payload
                         FROM chunk_records m
                         LEFT JOIN chunk_record_content c ON c.chunk_record_id = m.id
                         WHERE m.stage = ? AND m.chunk_id = ? AND m.status IN ('SUCCESS', 'LOCAL_COMMITTED', 'COMPLETED')
                         ORDER BY m.committed_at_ms DESC, m.attempt DESC
                         LIMIT 1
                          """
                  )) {
                statement.setString(1, normalizeStage(stage));
                statement.setString(2, buildProjectionChunkId(projectionName));
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (!resultSet.next()) {
                        return new LinkedHashMap<>();
                    }
                    String codec = normalizeText(resultSet.getString("result_codec"));
                    String resultHash = normalizeText(resultSet.getString("result_hash"));
                    Map<String, Object> payload = decodeJsonMapPayload(codec, resultSet.getBytes("result_payload"));
                    if (payload.isEmpty()) {
                        return new LinkedHashMap<>();
                    }
                    if (!resultHash.isBlank()) {
                        String stableHash = sha256Hex(stableJsonBytes(payload));
                        if (!resultHash.equalsIgnoreCase(stableHash)) {
                            logger.warn(
                                    "load projection payload hash mismatch ignored: outputDir={} stage={} projection={}",
                                    outputDir,
                                    stage,
                                    projectionName
                            );
                        }
                    }
                    return payload;
                }
            }
        } catch (Exception error) {
            logger.warn(
                    "load projection payload failed: outputDir={} stage={} projection={} err={}",
                    outputDir,
                    stage,
                    projectionName,
                    error.getMessage()
            );
            return new LinkedHashMap<>();
        }
    }

    public boolean hasProjectionPayload(String outputDir, String stage, String projectionName) {
        if (outputDir == null || outputDir.isBlank() || stage == null || stage.isBlank() || projectionName == null || projectionName.isBlank()) {
            return false;
        }
        return !loadProjectionPayload(outputDir, stage, projectionName).isEmpty();
    }

    public int loadLatestChunkAttempt(String outputDir, String stage, String chunkId) {
        Path dbPath = runtimeStateDbPath(outputDir);
        if (!Files.isRegularFile(dbPath)) {
            return 0;
        }
        try {
            ensureRuntimeStateDbSchema(dbPath);
            try (Connection connection = openRuntimeStateConnection(dbPath);
                 PreparedStatement statement = connection.prepareStatement(
                         """
                         SELECT attempt
                         FROM chunk_records
                         WHERE stage = ?
                           AND chunk_id = ?
                         ORDER BY attempt DESC, updated_at_ms DESC
                         LIMIT 1
                         """
                 )) {
                statement.setString(1, normalizeStage(stage));
                statement.setString(2, normalizeText(chunkId));
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (!resultSet.next()) {
                        return 0;
                    }
                    return Math.max(0, resultSet.getInt("attempt"));
                }
            }
        } catch (Exception error) {
            logger.warn(
                    "load latest chunk attempt failed: outputDir={} stage={} chunkId={} err={}",
                    outputDir,
                    stage,
                    chunkId,
                    error.getMessage()
            );
            return 0;
        }
    }

    public Map<String, Object> loadCommittedChunkPayload(
            String outputDir,
            String stage,
            String chunkId,
            String inputFingerprint
    ) {
        Path dbPath = runtimeStateDbPath(outputDir);
        if (!Files.isRegularFile(dbPath)) {
            return new LinkedHashMap<>();
        }
        String normalizedStage = normalizeStage(stage);
        String normalizedChunkId = normalizeText(chunkId);
        String normalizedInputFingerprint = normalizeText(inputFingerprint);
        if (normalizedStage.isBlank() || normalizedChunkId.isBlank()) {
            return new LinkedHashMap<>();
        }
        try {
            ensureRuntimeStateDbSchema(dbPath);
            StringBuilder sql = new StringBuilder(
                    """
                    SELECT
                        m.result_hash,
                        c.result_codec,
                        c.result_payload
                    FROM chunk_records m
                    LEFT JOIN chunk_record_content c ON c.chunk_record_id = m.id
                    WHERE m.stage = ?
                      AND m.chunk_id = ?
                      AND m.status IN ('SUCCESS', 'LOCAL_COMMITTED', 'COMPLETED')
                    """
            );
            if (!normalizedInputFingerprint.isBlank()) {
                sql.append(" AND m.input_fingerprint = ?");
            }
            sql.append(" ORDER BY m.committed_at_ms DESC, m.attempt DESC LIMIT 4");
            try (Connection connection = openRuntimeStateConnection(dbPath);
                 PreparedStatement statement = connection.prepareStatement(sql.toString())) {
                statement.setString(1, normalizedStage);
                statement.setString(2, normalizedChunkId);
                if (!normalizedInputFingerprint.isBlank()) {
                    statement.setString(3, normalizedInputFingerprint);
                }
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        String codec = normalizeText(resultSet.getString("result_codec"));
                        Map<String, Object> payload = decodeJsonMapPayload(codec, resultSet.getBytes("result_payload"));
                        if (payload.isEmpty()) {
                            continue;
                        }
                        String resultHash = normalizeText(resultSet.getString("result_hash"));
                        if (!resultHash.isBlank()) {
                            String stableHash = sha256Hex(stableJsonBytes(payload));
                            if (!resultHash.equalsIgnoreCase(stableHash)) {
                                logger.warn(
                                        "load committed chunk payload hash mismatch ignored: outputDir={} stage={} chunkId={}",
                                        outputDir,
                                        normalizedStage,
                                        normalizedChunkId
                                );
                            }
                        }
                        return payload;
                    }
                }
            }
        } catch (Exception error) {
            logger.warn(
                    "load committed chunk payload failed: outputDir={} stage={} chunkId={} err={}",
                    outputDir,
                    normalizedStage,
                    normalizedChunkId,
                    error.getMessage()
            );
        }
        return new LinkedHashMap<>();
    }

    public List<Map<String, Object>> listScopeNodes(
            String outputDir,
            String stage,
            String scopeType
    ) {
        Path dbPath = runtimeStateDbPath(outputDir);
        if (!Files.isRegularFile(dbPath)) {
            return List.of();
        }
        String normalizedStage = normalizeText(stage);
        String normalizedScopeType = normalizeLowercaseText(scopeType, "");
        try {
            ensureRuntimeStateDbSchema(dbPath);
            try (Connection connection = openRuntimeStateConnection(dbPath);
                 PreparedStatement statement = connection.prepareStatement(
                         """
                         SELECT
                             scope_ref,
                             normalized_video_key,
                             stage,
                             scope_type,
                             scope_id,
                             scope_variant,
                             status,
                             input_fingerprint,
                             local_path,
                             chunk_id,
                             unit_id,
                             stage_step,
                             retry_mode,
                             retry_entry_point,
                             required_action,
                             error_class,
                             error_code,
                             error_message,
                             dirty_reason,
                             dirty_at_ms,
                             plan_context_json,
                             resource_snapshot_json,
                             attempt_count,
                             result_hash,
                             updated_at_ms
                         FROM scope_nodes
                         WHERE (? = '' OR stage = ?)
                           AND (? = '' OR scope_type = ?)
                         ORDER BY updated_at_ms DESC, scope_ref ASC
                         """
                 )) {
                statement.setString(1, normalizedStage);
                statement.setString(2, normalizedStage);
                statement.setString(3, normalizedScopeType);
                statement.setString(4, normalizedScopeType);
                List<Map<String, Object>> nodes = new ArrayList<>();
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Map<String, Object> payload = new LinkedHashMap<>();
                        payload.put("scope_ref", normalizeText(resultSet.getString("scope_ref")));
                        payload.put("normalized_video_key", normalizeText(resultSet.getString("normalized_video_key")));
                        payload.put("stage", normalizeText(resultSet.getString("stage")));
                        payload.put("scope_type", normalizeText(resultSet.getString("scope_type")));
                        payload.put("scope_id", normalizeText(resultSet.getString("scope_id")));
                        payload.put("scope_variant", normalizeText(resultSet.getString("scope_variant")));
                        payload.put("status", normalizeStatus(resultSet.getString("status")));
                        payload.put("input_fingerprint", normalizeText(resultSet.getString("input_fingerprint")));
                        payload.put("local_path", normalizeText(resultSet.getString("local_path")));
                        payload.put("chunk_id", normalizeText(resultSet.getString("chunk_id")));
                        payload.put("unit_id", normalizeText(resultSet.getString("unit_id")));
                        payload.put("stage_step", normalizeText(resultSet.getString("stage_step")));
                        payload.put("retry_mode", normalizeText(resultSet.getString("retry_mode")));
                        payload.put("retry_entry_point", normalizeText(resultSet.getString("retry_entry_point")));
                        payload.put("required_action", normalizeText(resultSet.getString("required_action")));
                        payload.put("error_class", normalizeText(resultSet.getString("error_class")));
                        payload.put("error_code", normalizeText(resultSet.getString("error_code")));
                        payload.put("error_message", normalizeText(resultSet.getString("error_message")));
                        payload.put("dirty_reason", normalizeText(resultSet.getString("dirty_reason")));
                        payload.put("dirty_at_ms", resultSet.getLong("dirty_at_ms"));
                        payload.put("plan_context", extractMapPayload(resultSet.getString("plan_context_json")));
                        payload.put("resource_snapshot", extractMapPayload(resultSet.getString("resource_snapshot_json")));
                        payload.put("attempt_count", Math.max(0, resultSet.getInt("attempt_count")));
                        payload.put("result_hash", normalizeText(resultSet.getString("result_hash")));
                        payload.put("updated_at_ms", resultSet.getLong("updated_at_ms"));
                        nodes.add(payload);
                    }
                }
                return nodes;
            }
        } catch (Exception error) {
            logger.warn(
                    "list runtime scope nodes failed: outputDir={} stage={} scopeType={} err={}",
                    outputDir,
                    normalizedStage,
                    normalizedScopeType,
                    error.getMessage()
            );
            return List.of();
        }
    }

    public void recordChunkState(
            String outputDir,
            String stage,
            String chunkId,
            String inputFingerprint,
            int attempt,
            Map<String, Object> chunkStatePayload
    ) {
        writeChunkAttempt(outputDir, stage, chunkId, inputFingerprint, attempt, chunkStatePayload, null, false);
    }

    public void commitChunkPayload(
            String outputDir,
            String stage,
            String chunkId,
            String inputFingerprint,
            int attempt,
            Map<String, Object> chunkStatePayload,
            Map<String, Object> resultPayload
    ) {
        writeChunkAttempt(outputDir, stage, chunkId, inputFingerprint, attempt, chunkStatePayload, resultPayload, true);
    }

    public void failChunkPayload(
            String outputDir,
            String stage,
            String chunkId,
            String inputFingerprint,
            int attempt,
            Map<String, Object> chunkStatePayload
    ) {
        writeChunkAttempt(outputDir, stage, chunkId, inputFingerprint, attempt, chunkStatePayload, null, false);
    }

    public String buildScopeRef(String stage, String scopeType, String scopeId) {
        return buildScopeRef(stage, scopeType, scopeId, "");
    }

    public String buildScopeRef(String stage, String scopeType, String scopeId, String scopeVariant) {
        String safeStage = sanitizeScopeSegment(stage, "stage", 32, true);
        String safeType = sanitizeScopeSegment(scopeType, "scope", 24, true);
        String safeId = sanitizeScopeSegment(scopeId, "unknown", 96, false);
        String scopeRef = safeStage + "/" + safeType + "/" + safeId;
        String safeVariant = sanitizeScopeSegment(scopeVariant, "", 32, true);
        if (!safeVariant.isBlank()) {
            scopeRef = scopeRef + "@" + safeVariant;
        }
        return scopeRef;
    }

    public String buildSubstageScopeId(String substageName, String waveId) {
        String safeSubstage = sanitizeScopeSegment(substageName, "substage", 64, true);
        String safeWave = sanitizeScopeSegment(waveId, "wave_0001", 32, true);
        return safeSubstage + "." + safeWave;
    }

    public String buildSubstageScopeRef(String stage, String substageName, String waveId) {
        return buildSubstageScopeRef(stage, substageName, waveId, "");
    }

    public String buildSubstageScopeRef(String stage, String substageName, String waveId, String scopeVariant) {
        return buildScopeRef(
                stage,
                SCOPE_TYPE_SUBSTAGE,
                buildSubstageScopeId(substageName, waveId),
                scopeVariant
        );
    }

    public void planSubstageScope(
            String outputDir,
            String stage,
            String substageName,
            String waveId,
            String inputFingerprint,
            List<String> dependsOnScopeRefs,
            Map<String, Object> extraPayload
    ) {
        Map<String, Object> mergedPayload = new LinkedHashMap<>(extraPayload != null ? extraPayload : Map.of());
        Map<String, Object> planContext = extractMapPayload(mergedPayload.get("plan_context"));
        planContext.putIfAbsent("substage_name", normalizeText(substageName));
        planContext.putIfAbsent("wave_id", normalizeText(waveId, "wave_0001"));
        mergedPayload.put("plan_context", planContext);
        mergedPayload.putIfAbsent("stage_step", normalizeText(substageName));
        mergedPayload.putIfAbsent("attempt_count", 0);
        planScopeNode(
                outputDir,
                stage,
                SCOPE_TYPE_SUBSTAGE,
                buildSubstageScopeId(substageName, waveId),
                "",
                inputFingerprint,
                dependsOnScopeRefs,
                mergedPayload
        );
    }

    public void planScopeNode(
            String outputDir,
            String stage,
            String scopeType,
            String scopeId,
            String scopeVariant,
            String inputFingerprint,
            List<String> dependsOnScopeRefs,
            Map<String, Object> extraPayload
    ) {
        writeScopeNode(
                outputDir,
                stage,
                scopeType,
                scopeId,
                scopeVariant,
                STATUS_PLANNED,
                inputFingerprint,
                dependsOnScopeRefs,
                extraPayload,
                true
        );
    }

    public void transitionScopeNode(
            String outputDir,
            String stage,
            String scopeType,
            String scopeId,
            String scopeVariant,
            String status,
            String inputFingerprint,
            Map<String, Object> extraPayload
    ) {
        writeScopeNode(
                outputDir,
                stage,
                scopeType,
                scopeId,
                scopeVariant,
                status,
                inputFingerprint,
                null,
                extraPayload,
                false
        );
    }

    public List<String> resetRunningScopesToPlanned(
            String outputDir,
            String stage,
            String scopeType,
            String reason
    ) {
        Path dbPath = runtimeStateDbPath(outputDir);
        List<String> affectedScopeRefs = new ArrayList<>();
        try {
            ensureRuntimeStateDbSchema(dbPath);
            try (Connection connection = openRuntimeStateConnection(dbPath);
                 PreparedStatement selectStatement = connection.prepareStatement(
                         """
                         SELECT scope_ref, status
                         FROM scope_nodes
                         WHERE (? = '' OR stage = ?)
                           AND (? = '' OR scope_type = ?)
                           AND status IN ('RUNNING', 'EXECUTING', 'LOCAL_WRITING')
                         ORDER BY updated_at_ms ASC, scope_ref ASC
                         """
                 );
                 PreparedStatement updateStatement = connection.prepareStatement(
                         """
                         UPDATE scope_nodes
                         SET status = ?,
                             retry_mode = '',
                             retry_entry_point = '',
                             required_action = '',
                             error_class = '',
                             error_code = '',
                             error_message = '',
                             resource_snapshot_json = ?,
                             updated_at_ms = ?
                         WHERE scope_ref = ?
                         """
                 )) {
                String rawStage = normalizeText(stage);
                String normalizedStage = rawStage.isBlank() ? "" : normalizeStage(rawStage);
                String normalizedScopeType = normalizeLowercaseText(scopeType, "");
                selectStatement.setString(1, normalizedStage);
                selectStatement.setString(2, normalizedStage);
                selectStatement.setString(3, normalizedScopeType);
                selectStatement.setString(4, normalizedScopeType);
                long nowMs = System.currentTimeMillis();
                connection.setAutoCommit(false);
                try (ResultSet resultSet = selectStatement.executeQuery()) {
                    while (resultSet.next()) {
                        String scopeRef = normalizeText(resultSet.getString("scope_ref"));
                        String previousStatus = normalizeStatus(resultSet.getString("status"));
                        if (scopeRef.isBlank() || !RESETTABLE_RUNNING_SCOPE_STATUSES.contains(previousStatus)) {
                            continue;
                        }
                        Map<String, Object> resourceSnapshot = new LinkedHashMap<>();
                        resourceSnapshot.put("interrupted_status", previousStatus);
                        resourceSnapshot.put("interrupted_at_ms", nowMs);
                        resourceSnapshot.put("requeue_reason", normalizeText(reason, "runtime_context_lost"));
                        updateStatement.setString(1, STATUS_PLANNED);
                        updateStatement.setString(2, writeJsonString(resourceSnapshot));
                        updateStatement.setLong(3, nowMs);
                        updateStatement.setString(4, scopeRef);
                        updateStatement.addBatch();
                        affectedScopeRefs.add(scopeRef);
                    }
                }
                updateStatement.executeBatch();
                connection.commit();
                connection.setAutoCommit(true);
            }
        } catch (Exception error) {
            throw new IllegalStateException(
                    "reset running scopes to planned failed: stage="
                            + normalizeStage(stage)
                            + ", scopeType="
                            + normalizeLowercaseText(scopeType, ""),
                    error
            );
        }
        return affectedScopeRefs;
    }

    private String buildProjectionChunkId(String projectionName) {
        String normalized = normalizeText(projectionName)
                .replace('\\', '_')
                .replace('/', '_')
                .replace('@', '_')
                .replace(':', '_')
                .replace(' ', '_');
        if (normalized.isBlank()) {
            normalized = "projection";
        }
        if (normalized.length() > 48) {
            normalized = normalized.substring(0, 48);
        }
        return "proj." + normalized.toLowerCase(Locale.ROOT);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> decodeJsonMapPayload(String codec, byte[] blob) throws IOException {
        String normalizedCodec = normalizeLowercaseText(codec, "");
        if (blob == null || blob.length == 0 || normalizedCodec.isBlank()) {
            return new LinkedHashMap<>();
        }
        if (!"json-utf8".equals(normalizedCodec) && !"json-utf8+zlib".equals(normalizedCodec) && !"json".equals(normalizedCodec)) {
            return new LinkedHashMap<>();
        }
        byte[] decoded = decodeJsonBlob(codec, blob);
        if (decoded.length == 0) {
            return new LinkedHashMap<>();
        }
        Object payload = objectMapper.readValue(decoded, LinkedHashMap.class);
        if (!(payload instanceof Map<?, ?> rawMap)) {
            return new LinkedHashMap<>();
        }
        Map<String, Object> normalized = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
            normalized.put(String.valueOf(entry.getKey()), entry.getValue());
        }
        return normalized;
    }

    private byte[] stableJsonBytes(Map<String, Object> payload) throws IOException {
        return objectMapper.writer()
                .with(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
                .writeValueAsBytes(payload);
    }

    private String buildStablePayloadHash(Map<String, Object> payload) {
        if (payload == null || payload.isEmpty()) {
            return "";
        }
        try {
            return sha256Hex(stableJsonBytes(payload));
        } catch (IOException error) {
            throw new IllegalStateException("build stable payload hash failed", error);
        }
    }

    private String sha256Hex(byte[] payload) {
        if (payload == null || payload.length == 0) {
            return "";
        }
        try {
            java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(payload);
            StringBuilder builder = new StringBuilder(hash.length * 2);
            for (byte item : hash) {
                builder.append(String.format("%02x", item));
            }
            return builder.toString();
        } catch (Exception error) {
            throw new IllegalStateException("sha256 digest unavailable", error);
        }
    }

    public Path runtimeFallbackRecordPath(String outputDir) {
        return runtimeRoot(outputDir).resolve("fallback_records.jsonl");
    }

    public Path runtimeErrorRecordPath(String outputDir) {
        return runtimeRoot(outputDir).resolve("error_records.jsonl");
    }

    public Path runtimeManualRetryRecordPath(String outputDir) {
        return runtimeRoot(outputDir).resolve("manual_retry_required_records.jsonl");
    }

    public Map<String, StageSnapshotRecord> loadStageSnapshots(String outputDir) {
        Path dbPath = runtimeStateDbPath(outputDir);
        if (!Files.isRegularFile(dbPath)) {
            return new LinkedHashMap<>();
        }
        try {
            ensureRuntimeStateDbSchema(dbPath);
            Map<String, StageSnapshotRecord> snapshots = new LinkedHashMap<>();
            Path taskRoot = resolveTaskRoot(outputDir);
            try (Connection connection = openRuntimeStateConnection(dbPath);
                 PreparedStatement statement = connection.prepareStatement(
                         """
                         SELECT
                             stage,
                             stage_owner,
                             status,
                             checkpoint,
                             updated_at_ms,
                             completed,
                             pending,
                             stage_state_path,
                             retry_mode,
                             retry_entry_point,
                             required_action,
                             retry_strategy,
                             operator_action,
                             action_hint,
                             error_class,
                             error_code,
                             error_message,
                             subtitle_path,
                             domain,
                             main_topic
                         FROM stage_snapshots
                         ORDER BY updated_at_ms DESC, stage ASC
                         """
                 )) {
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        StageSnapshotRecord record = mapStageSnapshotRecord(resultSet, taskRoot);
                        if (record == null) {
                            continue;
                        }
                        StageSnapshotRecord previous = snapshots.get(record.stage());
                        if (previous == null || record.updatedAtMs() >= previous.updatedAtMs()) {
                            snapshots.put(record.stage(), record);
                        }
                    }
                }
            }
            return snapshots;
        } catch (Exception error) {
            logger.warn("load runtime stage snapshots failed: path={} err={}", dbPath, error.getMessage());
            return new LinkedHashMap<>();
        }
    }

    public Path writeStageState(
            String outputDir,
            String taskId,
            String stage,
            String status,
            String checkpoint,
            int completed,
            int pending,
            Map<String, Object> extraPayload
    ) {
        try {
            Path taskRoot = resolveTaskRoot(outputDir);
            Path stageStatePath = stageStatePath(taskRoot.toString(), stage);
            String normalizedTaskId = normalizeText(taskId);
            String normalizedStage = normalizeStage(stage);
            String normalizedStatus = normalizeStatus(status);
            String normalizedCheckpoint = normalizeText(checkpoint, "unknown");
            int safeCompleted = Math.max(0, completed);
            int safePending = Math.max(0, pending);
            long updatedAtMs = parseLong(
                    extraPayload != null ? extraPayload.get("updated_at_ms") : null,
                    System.currentTimeMillis()
            );
            String normalizedStageStatePath = stageStatePath.toAbsolutePath().normalize().toString();

            Map<String, Object> statePayload = new LinkedHashMap<>();
            if (extraPayload != null && !extraPayload.isEmpty()) {
                statePayload.putAll(extraPayload);
            }
            String stageOwner = normalizeLowercaseText(
                    firstPayloadText(statePayload, "stage_owner", "owner"),
                    "java"
            );
            statePayload.put("schema_version", STAGE_STATE_SCHEMA_VERSION);
            statePayload.put("task_id", normalizedTaskId);
            statePayload.put("stage", normalizedStage);
            statePayload.put("stage_owner", stageOwner);
            statePayload.put("status", normalizedStatus);
            statePayload.put("checkpoint", normalizedCheckpoint);
            statePayload.put("updated_at_ms", updatedAtMs);
            statePayload.put("completed", safeCompleted);
            statePayload.put("pending", safePending);
            statePayload.put("output_dir", taskRoot.toString());
            statePayload.put("stage_state_path", normalizedStageStatePath);
            statePayload.put("local_stage_state_path", normalizedStageStatePath);

            upsertStageSnapshot(
                    runtimeStateDbPath(taskRoot.toString()),
                    new StageSnapshotRecord(
                            normalizedStage,
                            normalizedTaskId,
                            stageOwner,
                            normalizedStatus,
                            normalizedCheckpoint,
                            updatedAtMs,
                            safeCompleted,
                            safePending,
                            taskRoot.toString(),
                            normalizedStageStatePath,
                            statePayload
                    )
            );

            if (shouldWriteStageFileMirrors()) {
                writeJsonAtomically(stageStatePath, statePayload);
            }
            if (List.of("ERROR", "MANUAL_NEEDED", "FAILED").contains(normalizedStatus)) {
                Map<String, Object> errorRecord = new LinkedHashMap<>();
                errorRecord.put("schema_version", RUNTIME_ERROR_RECORD_SCHEMA_VERSION);
                errorRecord.put("record_type", "stage_error");
                errorRecord.put("task_id", normalizedTaskId);
                errorRecord.put("stage", normalizedStage);
                errorRecord.put("status", normalizedStatus);
                errorRecord.put("checkpoint", normalizedCheckpoint);
                errorRecord.put("error_class", normalizeText(firstPayloadText(statePayload, "error_class", "errorClass")));
                errorRecord.put("error_code", normalizeText(firstPayloadText(statePayload, "error_code", "errorCode")));
                errorRecord.put("error_message", normalizeText(firstPayloadText(statePayload, "error_message", "errorMessage")));
                errorRecord.put("retry_mode", normalizeText(firstPayloadText(statePayload, "retry_mode", "retryMode")));
                errorRecord.put("required_action", normalizeText(firstPayloadText(statePayload, "required_action", "requiredAction")));
                errorRecord.put("retry_entry_point", normalizeText(firstPayloadText(statePayload, "retry_entry_point", "retryEntryPoint")));
                errorRecord.put("local_stage_state_path", normalizedStageStatePath);
                errorRecord.put("output_dir", taskRoot.toString());
                errorRecord.put("source", "java");
                errorRecord.put("updated_at_ms", updatedAtMs);
                appendRuntimeErrorRecord(taskRoot.toString(), errorRecord);
                if ("MANUAL_NEEDED".equals(normalizedStatus)) {
                    Map<String, Object> manualRetryRecord = new LinkedHashMap<>(errorRecord);
                    manualRetryRecord.put("schema_version", RUNTIME_MANUAL_RETRY_RECORD_SCHEMA_VERSION);
                    manualRetryRecord.put("record_type", "stage_manual_retry_required");
                    appendRuntimeManualRetryRecord(taskRoot.toString(), manualRetryRecord);
                }
            }
            writeResumeIndexHint(
                    taskRoot.toString(),
                    normalizedTaskId,
                    normalizedStage,
                    normalizedStatus,
                    normalizedCheckpoint,
                    stageStatePath,
                    "latest_stage_checkpoint"
            );
            return stageStatePath;
        } catch (Exception error) {
            throw new IllegalStateException(
                    "write runtime stage state failed: stage=" + normalizeStage(stage) + ", outputDir=" + outputDir,
                    error
            );
        }
    }

    public void appendStageJournalEvent(
            String outputDir,
            String taskId,
            String stage,
            String eventName,
            String status,
            String checkpoint,
            int completed,
            int pending,
            Map<String, Object> extraPayload
    ) {
    }

    public Path writeStageOutputsManifest(
            String outputDir,
            String taskId,
            String stage,
            Map<String, Object> manifestPayload
    ) {
        return null;
    }

    public Path writeStageCheckpoint(
            String outputDir,
            String taskId,
            String stage,
            String status,
            String checkpoint,
            int completed,
            int pending,
            Map<String, Object> extraPayload
    ) {
        return writeStageState(outputDir, taskId, stage, status, checkpoint, completed, pending, extraPayload);
    }

    public void appendStageJournalEvent(String outputDir, String stage, Map<String, Object> payload) {
        Map<String, Object> eventPayload = new LinkedHashMap<>(payload != null ? payload : Map.of());
        String taskId = normalizeText(String.valueOf(eventPayload.getOrDefault("task_id", "")));
        String eventName = normalizeText(String.valueOf(eventPayload.getOrDefault("event", "checkpoint")));
        String status = normalizeText(String.valueOf(eventPayload.getOrDefault("status", "UNKNOWN")), "UNKNOWN");
        String checkpoint = normalizeText(String.valueOf(eventPayload.getOrDefault("checkpoint", "unknown")), "unknown");
        int completed = parseInt(eventPayload.remove("completed"), 0);
        int pending = parseInt(eventPayload.remove("pending"), 0);
        eventPayload.remove("task_id");
        eventPayload.remove("event");
        eventPayload.remove("status");
        eventPayload.remove("checkpoint");
        eventPayload.remove("completed");
        eventPayload.remove("pending");
        appendStageJournalEvent(outputDir, taskId, stage, eventName, status, checkpoint, completed, pending, eventPayload);
    }

    public Path writeStageArtifact(
            String outputDir,
            String stage,
            String filename,
            Map<String, Object> payload
    ) {
        String taskId = payload != null ? normalizeText(String.valueOf(payload.getOrDefault("task_id", ""))) : "";
        return writeStageArtifact(
                outputDir,
                taskId,
                stage,
                filename,
                "runtime_stage_artifact_v1",
                payload
        );
    }

    public Path writeStageArtifact(
            String outputDir,
            String taskId,
            String stage,
            String filename,
            String schemaVersion,
            Map<String, Object> payload
    ) {
        Path artifactPath = stageArtifactPath(outputDir, stage, filename);
        try {
            Map<String, Object> normalizedPayload = new LinkedHashMap<>();
            if (payload != null && !payload.isEmpty()) {
                normalizedPayload.putAll(payload);
            }
            normalizedPayload.putIfAbsent("schema_version", normalizeText(schemaVersion, OUTPUTS_MANIFEST_SCHEMA_VERSION));
            normalizedPayload.putIfAbsent("task_id", normalizeText(taskId));
            normalizedPayload.putIfAbsent("stage", normalizeStage(stage));
            normalizedPayload.putIfAbsent(
                    "stage_owner",
                    normalizeLowercaseText(firstPayloadText(normalizedPayload, "stage_owner", "owner"), "java")
            );
            normalizedPayload.put("updated_at_ms", System.currentTimeMillis());
            writeJsonAtomically(artifactPath, normalizedPayload);
            return artifactPath;
        } catch (Exception error) {
            throw new IllegalStateException(
                    "write runtime stage artifact failed: stage="
                            + normalizeStage(stage)
                            + ", filename="
                            + normalizeFilename(filename),
                    error
            );
        }
    }

    public void writeResumeIndexHint(
            String outputDir,
            String taskId,
            String stage,
            String status,
            String checkpoint,
            Path stageStatePath,
            String reason
    ) {
        Path resumeIndexPath = runtimeRoot(outputDir).resolve("resume_index.json");
        try {
            Files.createDirectories(resumeIndexPath.getParent());
            Map<String, Object> anchorPayload = new LinkedHashMap<>();
            anchorPayload.put("resume_from_stage", normalizeStage(stage));
            anchorPayload.put("reason", normalizeText(reason, "latest_stage_checkpoint"));

            Map<String, Object> resumePayload = new LinkedHashMap<>();
            resumePayload.put("schema_version", RESUME_INDEX_SCHEMA_VERSION);
            resumePayload.put("task_id", normalizeText(taskId));
            resumePayload.put("updated_at_ms", System.currentTimeMillis());
            resumePayload.put("hint_stage", normalizeStage(stage));
            resumePayload.put("hint_status", normalizeStatus(status));
            resumePayload.put("hint_checkpoint", normalizeText(checkpoint, "unknown"));
            resumePayload.put(
                    "hint_stage_state_path",
                    stageStatePath != null ? stageStatePath.toAbsolutePath().normalize().toString() : ""
            );
            resumePayload.put("owner", "java");
            resumePayload.put("stage_graph_version", DEFAULT_STAGE_GRAPH_VERSION);
            resumePayload.put("recovery_anchor", anchorPayload);
            writeJsonAtomically(resumeIndexPath, resumePayload);
        } catch (Exception error) {
            logger.warn(
                    "write runtime resume index failed: stage={} outputDir={} err={}",
                    normalizeStage(stage),
                    outputDir,
                    error.getMessage()
            );
        }
    }

    public void appendRuntimeFallbackRecord(String outputDir, Map<String, Object> payload) {
        appendRuntimeRecord(runtimeFallbackRecordPath(outputDir), RUNTIME_FALLBACK_RECORD_SCHEMA_VERSION, payload);
    }

    public void appendRuntimeErrorRecord(String outputDir, Map<String, Object> payload) {
        appendRuntimeRecord(runtimeErrorRecordPath(outputDir), RUNTIME_ERROR_RECORD_SCHEMA_VERSION, payload);
    }

    public void appendRuntimeManualRetryRecord(String outputDir, Map<String, Object> payload) {
        appendRuntimeRecord(runtimeManualRetryRecordPath(outputDir), RUNTIME_MANUAL_RETRY_RECORD_SCHEMA_VERSION, payload);
    }

    private void appendRuntimeRecord(Path targetPath, String schemaVersion, Map<String, Object> payload) {
        try {
            Files.createDirectories(targetPath.getParent());
            Map<String, Object> recordPayload = new LinkedHashMap<>();
            if (payload != null && !payload.isEmpty()) {
                recordPayload.putAll(payload);
            }
            recordPayload.putIfAbsent("schema_version", normalizeText(schemaVersion));
            recordPayload.putIfAbsent("updated_at_ms", System.currentTimeMillis());
            String line = objectMapper.writeValueAsString(recordPayload) + System.lineSeparator();
            Files.writeString(
                    targetPath,
                    line,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
            );
        } catch (Exception error) {
            logger.warn("append runtime record failed: path={} err={}", targetPath, error.getMessage());
        }
    }

    public Map<String, Object> markScopesDirty(
            String outputDir,
            List<String> seedScopeRefs,
            String reason,
            boolean includeDescendants
    ) {
        List<String> normalizedSeeds = new ArrayList<>();
        Set<String> seenSeeds = new LinkedHashSet<>();
        for (String rawScopeRef : seedScopeRefs != null ? seedScopeRefs : List.<String>of()) {
            String scopeRef = normalizeText(rawScopeRef);
            if (scopeRef.isBlank() || !seenSeeds.add(scopeRef)) {
                continue;
            }
            normalizedSeeds.add(scopeRef);
        }
        Map<String, Object> plan = new LinkedHashMap<>();
        plan.put("seed_scope_refs", List.copyOf(normalizedSeeds));
        plan.put("dirty_scope_refs", List.of());
        plan.put("dirty_scope_count", 0);
        plan.put("dirty_reason", normalizeText(reason));
        if (normalizedSeeds.isEmpty()) {
            return plan;
        }

        Path dbPath = runtimeStateDbPath(outputDir);
        Map<String, Map<String, Object>> nodes;
        Map<String, Map<String, String>> scopeEdges;
        try {
            ensureRuntimeStateDbSchema(dbPath);
            try (Connection connection = openRuntimeStateConnection(dbPath)) {
                nodes = loadScopeNodes(connection);
                scopeEdges = loadScopeEdges(connection);
            }
        } catch (Exception error) {
            logger.warn("load runtime scope graph db failed: outputDir={} err={}", outputDir, error.getMessage());
            return plan;
        }
        if (nodes.isEmpty()) {
            return plan;
        }

        Map<String, List<String>> reverseEdges = new LinkedHashMap<>();
        for (Map.Entry<String, Map<String, String>> entry : scopeEdges.entrySet()) {
            for (String dependencyRef : entry.getValue().keySet()) {
                String normalizedDependencyRef = normalizeText(dependencyRef);
                if (normalizedDependencyRef.isBlank()) {
                    continue;
                }
                reverseEdges.computeIfAbsent(normalizedDependencyRef, ignored -> new ArrayList<>()).add(entry.getKey());
            }
        }

        ArrayDeque<String> queue = new ArrayDeque<>(normalizedSeeds);
        LinkedHashSet<String> dirtyScopeRefs = new LinkedHashSet<>();
        while (!queue.isEmpty()) {
            String currentScopeRef = queue.removeFirst();
            if (currentScopeRef.isBlank() || !dirtyScopeRefs.add(currentScopeRef)) {
                continue;
            }
            if (!includeDescendants) {
                continue;
            }
            for (String downstreamScopeRef : reverseEdges.getOrDefault(currentScopeRef, List.of())) {
                String normalizedScopeRef = normalizeText(downstreamScopeRef);
                if (!normalizedScopeRef.isBlank() && !dirtyScopeRefs.contains(normalizedScopeRef)) {
                    queue.addLast(normalizedScopeRef);
                }
            }
        }

        if (dirtyScopeRefs.isEmpty()) {
            return plan;
        }
        long nowMs = System.currentTimeMillis();
        for (String scopeRef : dirtyScopeRefs) {
            Map<String, Object> nodePayload = nodes.get(scopeRef);
            if (nodePayload == null) {
                continue;
            }
            nodePayload.put("status", "DIRTY");
            nodePayload.put("dirty_reason", normalizeText(reason));
            nodePayload.put("dirty_at_ms", nowMs);
            nodePayload.put("updated_at_ms", nowMs);
        }
        try {
            persistDirtyScopeNodes(dbPath, nodes, dirtyScopeRefs, nowMs);
        } catch (Exception error) {
            throw new IllegalStateException("mark runtime scopes dirty failed: outputDir=" + outputDir, error);
        }
        plan.put("dirty_scope_refs", List.copyOf(dirtyScopeRefs));
        plan.put("dirty_scope_count", dirtyScopeRefs.size());
        return plan;
    }

    private Map<String, Map<String, Object>> loadScopeNodes(Connection connection) throws SQLException {
        Map<String, Map<String, Object>> nodes = new LinkedHashMap<>();
        try (PreparedStatement nodeStatement = connection.prepareStatement(
                """
                SELECT
                    scope_ref,
                    normalized_video_key,
                    stage,
                    scope_type,
                    scope_id,
                    scope_variant,
                    status,
                    input_fingerprint,
                    local_path,
                    dirty_reason,
                    dirty_at_ms,
                    updated_at_ms
                FROM scope_nodes
                ORDER BY updated_at_ms DESC, scope_ref ASC
                """
        )) {
            try (ResultSet resultSet = nodeStatement.executeQuery()) {
                while (resultSet.next()) {
                    String scopeRef = normalizeText(resultSet.getString("scope_ref"));
                    if (scopeRef.isBlank()) {
                        continue;
                    }
                    Map<String, Object> payload = new LinkedHashMap<>();
                    payload.putIfAbsent("normalized_video_key", normalizeText(resultSet.getString("normalized_video_key")));
                    payload.putIfAbsent("scope_ref", scopeRef);
                    payload.putIfAbsent("stage", normalizeText(resultSet.getString("stage")));
                    payload.putIfAbsent("scope_type", normalizeText(resultSet.getString("scope_type")));
                    payload.putIfAbsent("scope_id", normalizeText(resultSet.getString("scope_id")));
                    payload.putIfAbsent("scope_variant", normalizeText(resultSet.getString("scope_variant")));
                    payload.putIfAbsent("status", normalizeText(resultSet.getString("status")));
                    payload.putIfAbsent("input_fingerprint", normalizeText(resultSet.getString("input_fingerprint")));
                    payload.putIfAbsent("local_path", normalizeText(resultSet.getString("local_path")));
                    payload.putIfAbsent("dirty_reason", normalizeText(resultSet.getString("dirty_reason")));
                    long dirtyAtMs = resultSet.getLong("dirty_at_ms");
                    if (dirtyAtMs > 0L) {
                        payload.putIfAbsent("dirty_at_ms", dirtyAtMs);
                    }
                    payload.putIfAbsent("updated_at_ms", resultSet.getLong("updated_at_ms"));
                    nodes.put(scopeRef, payload);
                }
            }
        }
        return nodes;
    }

    private Map<String, Map<String, String>> loadScopeEdges(Connection connection) throws SQLException {
        Map<String, Map<String, String>> edges = new LinkedHashMap<>();
        try (PreparedStatement statement = connection.prepareStatement(
                """
                SELECT scope_ref, depends_on_scope_ref, dependency_fingerprint
                FROM scope_edges
                ORDER BY scope_ref ASC, depends_on_scope_ref ASC
                """
        )) {
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    String scopeRef = normalizeText(resultSet.getString("scope_ref"));
                    String dependsOnScopeRef = normalizeText(resultSet.getString("depends_on_scope_ref"));
                    if (scopeRef.isBlank() || dependsOnScopeRef.isBlank()) {
                        continue;
                    }
                    edges.computeIfAbsent(scopeRef, ignored -> new LinkedHashMap<>())
                            .put(dependsOnScopeRef, normalizeText(resultSet.getString("dependency_fingerprint")));
                }
            }
        }
        return edges;
    }

    private void persistDirtyScopeNodes(
            Path dbPath,
            Map<String, Map<String, Object>> nodes,
            Set<String> dirtyScopeRefs,
            long updatedAtMs
    ) throws SQLException, IOException {
        ensureRuntimeStateDbSchema(dbPath);
        try (Connection connection = openRuntimeStateConnection(dbPath);
             PreparedStatement statement = connection.prepareStatement(
                     """
                     UPDATE scope_nodes
                     SET status = ?, updated_at_ms = ?, dirty_reason = ?, dirty_at_ms = ?
                     WHERE scope_ref = ?
                     """
             )) {
            connection.setAutoCommit(false);
            try {
                for (String scopeRef : dirtyScopeRefs) {
                    Map<String, Object> payload = nodes.get(scopeRef);
                    if (payload == null) {
                        continue;
                    }
                    statement.setString(1, normalizeStatus(String.valueOf(payload.getOrDefault("status", "DIRTY"))));
                    statement.setLong(2, updatedAtMs);
                    statement.setString(3, normalizeText(String.valueOf(payload.getOrDefault("dirty_reason", ""))));
                    statement.setLong(4, parseLong(payload.get("dirty_at_ms"), updatedAtMs));
                    statement.setString(5, scopeRef);
                    statement.addBatch();
                }
                statement.executeBatch();
                connection.commit();
            } catch (SQLException error) {
                connection.rollback();
                throw error;
            } catch (RuntimeException error) {
                connection.rollback();
                throw error;
            } finally {
                connection.setAutoCommit(true);
            }
        }
    }

    private void writeScopeNode(
            String outputDir,
            String stage,
            String scopeType,
            String scopeId,
            String scopeVariant,
            String status,
            String inputFingerprint,
            List<String> dependsOnScopeRefs,
            Map<String, Object> extraPayload,
            boolean replaceDependencies
    ) {
        String normalizedStage = normalizeStage(stage);
        String normalizedScopeType = normalizeLowercaseText(scopeType, "scope");
        String normalizedScopeId = sanitizeScopeSegment(scopeId, "unknown", 96, false);
        String normalizedScopeVariant = sanitizeScopeSegment(scopeVariant, "", 32, true);
        String normalizedStatus = normalizeStatus(status);
        String scopeRef = buildScopeRef(normalizedStage, normalizedScopeType, normalizedScopeId, normalizedScopeVariant);
        Map<String, Object> payload = new LinkedHashMap<>(extraPayload != null ? extraPayload : Map.of());
        long updatedAtMs = parseLong(payload.get("updated_at_ms"), System.currentTimeMillis());
        String localPath = normalizeText(String.valueOf(payload.getOrDefault("local_path", "")));
        String chunkId = normalizeText(String.valueOf(payload.getOrDefault("chunk_id", "")));
        String unitId = normalizeText(String.valueOf(payload.getOrDefault("unit_id", "")));
        String stageStep = normalizeText(String.valueOf(payload.getOrDefault("stage_step", "")));
        String retryMode = normalizeText(String.valueOf(payload.getOrDefault("retry_mode", "")));
        String retryEntryPoint = normalizeText(String.valueOf(payload.getOrDefault("retry_entry_point", "")));
        String requiredAction = normalizeText(String.valueOf(payload.getOrDefault("required_action", "")));
        String errorClass = normalizeText(String.valueOf(payload.getOrDefault("error_class", "")));
        String errorCode = normalizeText(String.valueOf(payload.getOrDefault("error_code", "")));
        String errorMessage = normalizeText(String.valueOf(payload.getOrDefault("error_message", "")));
        String dirtyReason = normalizeText(String.valueOf(payload.getOrDefault("dirty_reason", "")));
        long dirtyAtMs = parseLong(payload.get("dirty_at_ms"), 0L);
        int attemptCount = Math.max(0, parseInt(payload.get("attempt_count"), 0));
        String resultHash = normalizeText(String.valueOf(payload.getOrDefault("result_hash", "")));
        Map<String, Object> planContext = extractMapPayload(payload.get("plan_context"));
        Map<String, Object> resourceSnapshot = extractMapPayload(payload.get("resource_snapshot"));
        if (Set.of(STATUS_PLANNED, STATUS_RUNNING, STATUS_SUCCESS).contains(normalizedStatus)) {
            retryMode = "";
            retryEntryPoint = "";
            requiredAction = "";
            errorClass = "";
            errorCode = "";
            errorMessage = "";
        }

        Path dbPath = runtimeStateDbPath(outputDir);
        try {
            ensureRuntimeStateDbSchema(dbPath);
            try (Connection connection = openRuntimeStateConnection(dbPath)) {
                connection.setAutoCommit(false);
                try {
                    if (replaceDependencies && STATUS_PLANNED.equals(normalizedStatus)) {
                        ScopeNodeWriteDecision decision = resolvePlannedScopeNodeWriteDecision(
                                connection,
                                scopeRef,
                                inputFingerprint,
                                payload
                        );
                        normalizedStatus = decision.status();
                        inputFingerprint = decision.inputFingerprint();
                        localPath = decision.localPath();
                        chunkId = decision.chunkId();
                        unitId = decision.unitId();
                        stageStep = decision.stageStep();
                        retryMode = decision.retryMode();
                        retryEntryPoint = decision.retryEntryPoint();
                        requiredAction = decision.requiredAction();
                        errorClass = decision.errorClass();
                        errorCode = decision.errorCode();
                        errorMessage = decision.errorMessage();
                        dirtyReason = decision.dirtyReason();
                        dirtyAtMs = decision.dirtyAtMs();
                        planContext = decision.planContext();
                        resourceSnapshot = decision.resourceSnapshot();
                        attemptCount = decision.attemptCount();
                        resultHash = decision.resultHash();
                        updatedAtMs = decision.updatedAtMs();
                        if (STATUS_PLANNED.equals(normalizedStatus)) {
                            payload.put("status", STATUS_PLANNED);
                        }
                    }
                    if (replaceDependencies) {
                        replaceScopeEdges(connection, scopeRef, dependsOnScopeRefs, updatedAtMs);
                    }
                    try (PreparedStatement statement = connection.prepareStatement(
                            """
                            INSERT OR REPLACE INTO scope_nodes (
                                scope_ref,
                                normalized_video_key,
                                stage,
                                scope_type,
                                scope_id,
                                scope_variant,
                                status,
                                input_fingerprint,
                                local_path,
                                chunk_id,
                                unit_id,
                                stage_step,
                                retry_mode,
                                retry_entry_point,
                                required_action,
                                error_class,
                                error_code,
                                error_message,
                                dirty_reason,
                                dirty_at_ms,
                                plan_context_json,
                                resource_snapshot_json,
                                attempt_count,
                                result_hash,
                                updated_at_ms
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """
                    )) {
                        statement.setString(1, scopeRef);
                        statement.setString(2, normalizeText(String.valueOf(payload.getOrDefault("normalized_video_key", ""))));
                        statement.setString(3, normalizedStage);
                        statement.setString(4, normalizedScopeType);
                        statement.setString(5, normalizedScopeId);
                        statement.setString(6, normalizedScopeVariant);
                        statement.setString(7, normalizedStatus);
                        statement.setString(8, normalizeText(inputFingerprint));
                        statement.setString(9, localPath);
                        statement.setString(10, chunkId);
                        statement.setString(11, unitId);
                        statement.setString(12, stageStep);
                        statement.setString(13, retryMode);
                        statement.setString(14, retryEntryPoint);
                        statement.setString(15, requiredAction);
                        statement.setString(16, errorClass);
                        statement.setString(17, errorCode);
                        statement.setString(18, errorMessage);
                        statement.setString(19, dirtyReason);
                        statement.setLong(20, dirtyAtMs);
                        statement.setString(21, writeJsonString(planContext));
                        statement.setString(22, writeJsonString(resourceSnapshot));
                        statement.setInt(23, attemptCount);
                        statement.setString(24, resultHash);
                        statement.setLong(25, updatedAtMs);
                        statement.executeUpdate();
                    }
                    connection.commit();
                } catch (SQLException error) {
                    connection.rollback();
                    throw error;
                } catch (RuntimeException error) {
                    connection.rollback();
                    throw error;
                } finally {
                    connection.setAutoCommit(true);
                }
            }
        } catch (Exception error) {
            throw new IllegalStateException(
                    "write runtime scope node failed: stage="
                            + normalizedStage
                            + ", scopeType="
                            + normalizedScopeType
                            + ", scopeId="
                            + normalizedScopeId,
                    error
            );
        }
    }

    private ScopeNodeWriteDecision resolvePlannedScopeNodeWriteDecision(
            Connection connection,
            String scopeRef,
            String inputFingerprint,
            Map<String, Object> fallbackPayload
    ) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                """
                SELECT
                    status,
                    input_fingerprint,
                    local_path,
                    chunk_id,
                    unit_id,
                    stage_step,
                    retry_mode,
                    retry_entry_point,
                    required_action,
                    error_class,
                    error_code,
                    error_message,
                    dirty_reason,
                    dirty_at_ms,
                    plan_context_json,
                    resource_snapshot_json,
                    attempt_count,
                    result_hash,
                    updated_at_ms
                FROM scope_nodes
                WHERE scope_ref = ?
                LIMIT 1
                """
        )) {
            statement.setString(1, scopeRef);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return ScopeNodeWriteDecision.fromFallback(inputFingerprint, fallbackPayload);
                }
                String existingStatus = normalizeStatus(resultSet.getString("status"));
                String existingFingerprint = normalizeText(resultSet.getString("input_fingerprint"));
                if (existingStatus.isBlank() || !existingFingerprint.equals(normalizeText(inputFingerprint))) {
                    return ScopeNodeWriteDecision.fromFallback(inputFingerprint, fallbackPayload);
                }
                if (!Set.of(STATUS_RUNNING, STATUS_SUCCESS, STATUS_ERROR, STATUS_FAILED, STATUS_MANUAL_NEEDED).contains(existingStatus)) {
                    return ScopeNodeWriteDecision.fromFallback(inputFingerprint, fallbackPayload);
                }
                return new ScopeNodeWriteDecision(
                        existingStatus,
                        existingFingerprint,
                        normalizeText(resultSet.getString("local_path")),
                        normalizeText(resultSet.getString("chunk_id")),
                        normalizeText(resultSet.getString("unit_id")),
                        normalizeText(resultSet.getString("stage_step")),
                        normalizeText(resultSet.getString("retry_mode")),
                        normalizeText(resultSet.getString("retry_entry_point")),
                        normalizeText(resultSet.getString("required_action")),
                        normalizeText(resultSet.getString("error_class")),
                        normalizeText(resultSet.getString("error_code")),
                        normalizeText(resultSet.getString("error_message")),
                        normalizeText(resultSet.getString("dirty_reason")),
                        resultSet.getLong("dirty_at_ms"),
                        extractMapPayload(resultSet.getString("plan_context_json")),
                        extractMapPayload(resultSet.getString("resource_snapshot_json")),
                        Math.max(0, resultSet.getInt("attempt_count")),
                        normalizeText(resultSet.getString("result_hash")),
                        Math.max(0L, resultSet.getLong("updated_at_ms"))
                );
            }
        }
    }

    private void replaceScopeEdges(
            Connection connection,
            String scopeRef,
            List<String> dependsOnScopeRefs,
            long updatedAtMs
    ) throws SQLException {
        List<String> normalizedDependencies = new ArrayList<>();
        Set<String> seenScopeRefs = new LinkedHashSet<>();
        for (String rawScopeRef : dependsOnScopeRefs != null ? dependsOnScopeRefs : List.<String>of()) {
            String normalizedScopeRef = normalizeText(rawScopeRef);
            if (normalizedScopeRef.isBlank() || !seenScopeRefs.add(normalizedScopeRef)) {
                continue;
            }
            normalizedDependencies.add(normalizedScopeRef);
        }
        try (PreparedStatement deleteStatement = connection.prepareStatement(
                "DELETE FROM scope_edges WHERE scope_ref = ?"
        )) {
            deleteStatement.setString(1, scopeRef);
            deleteStatement.executeUpdate();
        }
        if (normalizedDependencies.isEmpty()) {
            return;
        }
        try (PreparedStatement insertStatement = connection.prepareStatement(
                """
                INSERT OR REPLACE INTO scope_edges (
                    scope_ref,
                    depends_on_scope_ref,
                    dependency_fingerprint,
                    updated_at_ms
                ) VALUES (?, ?, '', ?)
                """
        )) {
            for (String dependencyScopeRef : normalizedDependencies) {
                insertStatement.setString(1, scopeRef);
                insertStatement.setString(2, dependencyScopeRef);
                insertStatement.setLong(3, updatedAtMs);
                insertStatement.addBatch();
            }
            insertStatement.executeBatch();
        }
    }

    private void writeChunkAttempt(
            String outputDir,
            String stage,
            String chunkId,
            String inputFingerprint,
            int attempt,
            Map<String, Object> chunkStatePayload,
            Map<String, Object> resultPayload,
            boolean committed
    ) {
        Path dbPath = runtimeStateDbPath(outputDir);
        String normalizedStage = normalizeStage(stage);
        String normalizedChunkId = normalizeText(chunkId);
        String normalizedInputFingerprint = normalizeText(inputFingerprint);
        int safeAttempt = Math.max(1, attempt);
        Map<String, Object> statePayload = new LinkedHashMap<>(chunkStatePayload != null ? chunkStatePayload : Map.of());
        String status = normalizeStatus(String.valueOf(statePayload.getOrDefault("status", committed ? "SUCCESS" : "RUNNING")));
        String resultHash = normalizeText(String.valueOf(statePayload.getOrDefault("result_hash", "")));
        if (committed && resultPayload != null && !resultPayload.isEmpty()) {
            resultHash = buildStablePayloadHash(resultPayload);
            if (status.isBlank() || "RUNNING".equals(status)) {
                status = "SUCCESS";
            }
        }
        long updatedAtMs = parseLong(statePayload.get("updated_at_ms"), System.currentTimeMillis());
        long committedAtMs = committed
                ? Math.max(updatedAtMs, parseLong(statePayload.get("committed_at_ms"), updatedAtMs))
                : 0L;
        String errorClass = normalizeText(String.valueOf(statePayload.getOrDefault("error_class", "")));
        String errorCode = normalizeText(String.valueOf(statePayload.getOrDefault("error_code", "")));
        String errorMessage = normalizeText(String.valueOf(statePayload.getOrDefault("error_message", "")));
        String normalizedVideoKey = normalizeText(String.valueOf(statePayload.getOrDefault("normalized_video_key", "")));

        try {
            ensureRuntimeStateDbSchema(dbPath);
            try (Connection connection = openRuntimeStateConnection(dbPath)) {
                connection.setAutoCommit(false);
                try {
                    long recordId = upsertChunkAttemptRow(
                            connection,
                            normalizedVideoKey,
                            normalizedStage,
                            normalizedChunkId,
                            normalizedInputFingerprint,
                            safeAttempt,
                            status,
                            resultHash,
                            errorClass,
                            errorCode,
                            errorMessage,
                            updatedAtMs,
                            committedAtMs
                    );
                    if (committed && resultPayload != null && !resultPayload.isEmpty()) {
                        byte[] blob = stableJsonBytes(resultPayload);
                        try (PreparedStatement contentStatement = connection.prepareStatement(
                                """
                                INSERT INTO chunk_record_content (
                                    chunk_record_id,
                                    result_codec,
                                    result_payload
                                ) VALUES (?, ?, ?)
                                ON CONFLICT(chunk_record_id)
                                DO UPDATE SET
                                    result_codec = excluded.result_codec,
                                    result_payload = excluded.result_payload
                                """
                        )) {
                            contentStatement.setLong(1, recordId);
                            contentStatement.setString(2, "json-utf8");
                            contentStatement.setBytes(3, blob);
                            contentStatement.executeUpdate();
                        }
                    } else {
                        try (PreparedStatement deleteStatement = connection.prepareStatement(
                                "DELETE FROM chunk_record_content WHERE chunk_record_id = ?"
                        )) {
                            deleteStatement.setLong(1, recordId);
                            deleteStatement.executeUpdate();
                        }
                    }
                    connection.commit();
                } catch (SQLException error) {
                    connection.rollback();
                    throw error;
                } catch (RuntimeException error) {
                    connection.rollback();
                    throw error;
                } finally {
                    connection.setAutoCommit(true);
                }
            }
        } catch (Exception error) {
            throw new IllegalStateException(
                    "write runtime chunk attempt failed: stage="
                            + normalizedStage
                            + ", chunkId="
                            + normalizedChunkId
                            + ", attempt="
                            + safeAttempt,
                    error
            );
        }
    }

    private long upsertChunkAttemptRow(
            Connection connection,
            String normalizedVideoKey,
            String stage,
            String chunkId,
            String inputFingerprint,
            int attempt,
            String status,
            String resultHash,
            String errorClass,
            String errorCode,
            String errorMessage,
            long updatedAtMs,
            long committedAtMs
    ) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                """
                INSERT INTO chunk_records (
                    normalized_video_key,
                    stage,
                    chunk_id,
                    input_fingerprint,
                    attempt,
                    status,
                    result_hash,
                    error_class,
                    error_code,
                    error_message,
                    updated_at_ms,
                    committed_at_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(stage, chunk_id, attempt)
                DO UPDATE SET
                    normalized_video_key = excluded.normalized_video_key,
                    input_fingerprint = excluded.input_fingerprint,
                    status = excluded.status,
                    result_hash = excluded.result_hash,
                    error_class = excluded.error_class,
                    error_code = excluded.error_code,
                    error_message = excluded.error_message,
                    updated_at_ms = excluded.updated_at_ms,
                    committed_at_ms = excluded.committed_at_ms
                """
        )) {
            statement.setString(1, normalizedVideoKey);
            statement.setString(2, stage);
            statement.setString(3, chunkId);
            statement.setString(4, inputFingerprint);
            statement.setInt(5, Math.max(1, attempt));
            statement.setString(6, status);
            statement.setString(7, resultHash);
            statement.setString(8, errorClass);
            statement.setString(9, errorCode);
            statement.setString(10, errorMessage);
            statement.setLong(11, updatedAtMs);
            statement.setLong(12, committedAtMs);
            statement.executeUpdate();
        }
        try (PreparedStatement query = connection.prepareStatement(
                """
                SELECT id
                FROM chunk_records
                WHERE stage = ?
                  AND chunk_id = ?
                  AND attempt = ?
                LIMIT 1
                """
        )) {
            query.setString(1, stage);
            query.setString(2, chunkId);
            query.setInt(3, Math.max(1, attempt));
            try (ResultSet resultSet = query.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getLong("id");
                }
            }
        }
        throw new SQLException("chunk attempt row missing after upsert");
    }

    private void upsertStageSnapshot(Path dbPath, StageSnapshotRecord snapshot) throws SQLException, IOException {
        ensureRuntimeStateDbSchema(dbPath);
        try (Connection connection = openRuntimeStateConnection(dbPath);
             PreparedStatement statement = connection.prepareStatement(
                     """
                     INSERT OR REPLACE INTO stage_snapshots (
                         stage,
                         stage_owner,
                         status,
                         checkpoint,
                         updated_at_ms,
                         completed,
                         pending,
                         stage_state_path,
                         retry_mode,
                         retry_entry_point,
                         required_action,
                         retry_strategy,
                         operator_action,
                         action_hint,
                         error_class,
                         error_code,
                         error_message,
                         subtitle_path,
                         domain,
                         main_topic
                     ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                     """
             )) {
            Map<String, Object> payload = snapshot.payload();
            statement.setString(1, snapshot.stage());
            statement.setString(2, snapshot.stageOwner());
            statement.setString(3, snapshot.status());
            statement.setString(4, snapshot.checkpoint());
            statement.setLong(5, snapshot.updatedAtMs());
            statement.setInt(6, Math.max(0, snapshot.completed()));
            statement.setInt(7, Math.max(0, snapshot.pending()));
            statement.setString(8, snapshot.stageStatePath());
            statement.setString(9, firstPayloadText(payload, "retry_mode", "retryMode"));
            statement.setString(10, firstPayloadText(payload, "retry_entry_point", "retryEntryPoint"));
            statement.setString(11, firstPayloadText(payload, "required_action", "requiredAction"));
            statement.setString(12, firstPayloadText(payload, "retry_strategy", "retryStrategy"));
            statement.setString(13, firstPayloadText(payload, "operator_action", "operatorAction"));
            statement.setString(14, firstPayloadText(payload, "action_hint", "actionHint"));
            statement.setString(15, firstPayloadText(payload, "error_class", "errorClass"));
            statement.setString(16, firstPayloadText(payload, "error_code", "errorCode"));
            statement.setString(17, firstPayloadText(payload, "error_message", "errorMessage"));
            statement.setString(18, firstPayloadText(payload, "subtitle_path", "subtitlePath"));
            statement.setString(19, firstPayloadText(payload, "domain"));
            statement.setString(20, firstPayloadText(payload, "main_topic", "mainTopic"));
            statement.executeUpdate();
        }
    }

    private StageSnapshotRecord mapStageSnapshotRecord(ResultSet resultSet, Path taskRoot) throws SQLException {
        String stage = normalizeStage(resultSet.getString("stage"));
        if (stage.isBlank()) {
            return null;
        }
        String stageOwner = normalizeLowercaseText(resultSet.getString("stage_owner"), "");
        String status = normalizeStatus(resultSet.getString("status"));
        String checkpoint = normalizeText(resultSet.getString("checkpoint"), "unknown");
        long updatedAtMs = resultSet.getLong("updated_at_ms");
        int completed = Math.max(0, resultSet.getInt("completed"));
        int pending = Math.max(0, resultSet.getInt("pending"));
        String outputDir = taskRoot.toString();
        String stageStatePath = firstNonBlank(
                resultSet.getString("stage_state_path"),
                stageStatePath(taskRoot.toString(), stage).toAbsolutePath().normalize().toString()
        );
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.putIfAbsent("stage", stage);
        if (!stageOwner.isBlank()) {
            payload.putIfAbsent("stage_owner", stageOwner);
        }
        payload.putIfAbsent("status", status);
        payload.putIfAbsent("checkpoint", checkpoint);
        payload.putIfAbsent("updated_at_ms", updatedAtMs);
        payload.putIfAbsent("completed", completed);
        payload.putIfAbsent("pending", pending);
        payload.putIfAbsent("stage_state_path", stageStatePath);
        payload.putIfAbsent("stage_state_path", stageStatePath);
        payload.putIfAbsent("local_stage_state_path", stageStatePath);
        for (String fieldName : List.of(
                "retry_mode",
                "retry_entry_point",
                "required_action",
                "retry_strategy",
                "operator_action",
                "action_hint",
                "error_class",
                "error_code",
                "error_message",
                "subtitle_path",
                "domain",
                "main_topic"
        )) {
            String fieldValue = normalizeText(resultSet.getString(fieldName));
            if (!fieldValue.isBlank()) {
                payload.putIfAbsent(fieldName, fieldValue);
            }
        }
        return new StageSnapshotRecord(
                stage,
                "",
                stageOwner,
                status,
                checkpoint,
                updatedAtMs,
                completed,
                pending,
                outputDir,
                stageStatePath,
                payload
        );
    }

    private void ensureRuntimeStateDbSchema(Path dbPath) throws SQLException, IOException {
        Path normalizedDbPath = dbPath.toAbsolutePath().normalize();
        Files.createDirectories(normalizedDbPath.getParent());
        synchronized (runtimeStateDbSchemaLock) {
            try (Connection connection = openRuntimeStateConnection(normalizedDbPath);
                 Statement statement = connection.createStatement()) {
                rebuildLegacyTaskLocalTables(connection, statement);
                statement.execute(
                        """
                        CREATE TABLE IF NOT EXISTS stage_snapshots (
                            stage TEXT NOT NULL,
                            stage_owner TEXT NOT NULL DEFAULT '',
                            status TEXT NOT NULL DEFAULT '',
                            checkpoint TEXT NOT NULL DEFAULT '',
                            updated_at_ms INTEGER NOT NULL DEFAULT 0,
                            completed INTEGER NOT NULL DEFAULT 0,
                            pending INTEGER NOT NULL DEFAULT 0,
                            stage_state_path TEXT NOT NULL DEFAULT '',
                            retry_mode TEXT NOT NULL DEFAULT '',
                            retry_entry_point TEXT NOT NULL DEFAULT '',
                            required_action TEXT NOT NULL DEFAULT '',
                            retry_strategy TEXT NOT NULL DEFAULT '',
                            operator_action TEXT NOT NULL DEFAULT '',
                            action_hint TEXT NOT NULL DEFAULT '',
                            error_class TEXT NOT NULL DEFAULT '',
                            error_code TEXT NOT NULL DEFAULT '',
                            error_message TEXT NOT NULL DEFAULT '',
                            subtitle_path TEXT NOT NULL DEFAULT '',
                            domain TEXT NOT NULL DEFAULT '',
                            main_topic TEXT NOT NULL DEFAULT '',
                            PRIMARY KEY (stage)
                        )
                        """
                );
                ensureTableColumn(statement, connection, "stage_snapshots", "stage_owner", "TEXT NOT NULL DEFAULT ''");
                ensureTableColumn(statement, connection, "stage_snapshots", "status", "TEXT NOT NULL DEFAULT ''");
                ensureTableColumn(statement, connection, "stage_snapshots", "checkpoint", "TEXT NOT NULL DEFAULT ''");
                ensureTableColumn(statement, connection, "stage_snapshots", "updated_at_ms", "INTEGER NOT NULL DEFAULT 0");
                ensureTableColumn(statement, connection, "stage_snapshots", "completed", "INTEGER NOT NULL DEFAULT 0");
                ensureTableColumn(statement, connection, "stage_snapshots", "pending", "INTEGER NOT NULL DEFAULT 0");
                ensureTableColumn(statement, connection, "stage_snapshots", "stage_state_path", "TEXT NOT NULL DEFAULT ''");
                ensureTableColumn(statement, connection, "stage_snapshots", "retry_mode", "TEXT NOT NULL DEFAULT ''");
                ensureTableColumn(statement, connection, "stage_snapshots", "retry_entry_point", "TEXT NOT NULL DEFAULT ''");
                ensureTableColumn(statement, connection, "stage_snapshots", "required_action", "TEXT NOT NULL DEFAULT ''");
                ensureTableColumn(statement, connection, "stage_snapshots", "retry_strategy", "TEXT NOT NULL DEFAULT ''");
                ensureTableColumn(statement, connection, "stage_snapshots", "operator_action", "TEXT NOT NULL DEFAULT ''");
                ensureTableColumn(statement, connection, "stage_snapshots", "action_hint", "TEXT NOT NULL DEFAULT ''");
                ensureTableColumn(statement, connection, "stage_snapshots", "error_class", "TEXT NOT NULL DEFAULT ''");
                ensureTableColumn(statement, connection, "stage_snapshots", "error_code", "TEXT NOT NULL DEFAULT ''");
                ensureTableColumn(statement, connection, "stage_snapshots", "error_message", "TEXT NOT NULL DEFAULT ''");
                ensureTableColumn(statement, connection, "stage_snapshots", "subtitle_path", "TEXT NOT NULL DEFAULT ''");
                ensureTableColumn(statement, connection, "stage_snapshots", "domain", "TEXT NOT NULL DEFAULT ''");
                ensureTableColumn(statement, connection, "stage_snapshots", "main_topic", "TEXT NOT NULL DEFAULT ''");
                statement.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_stage_snapshots_updated_at
                        ON stage_snapshots(updated_at_ms DESC)
                        """
                );
                statement.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_stage_snapshots_status
                        ON stage_snapshots(status, updated_at_ms DESC)
                        """
                );
                statement.execute(
                        """
                        CREATE TABLE IF NOT EXISTS scope_nodes (
                            scope_ref TEXT NOT NULL,
                            normalized_video_key TEXT NOT NULL DEFAULT '',
                            stage TEXT NOT NULL DEFAULT '',
                            scope_type TEXT NOT NULL DEFAULT '',
                            scope_id TEXT NOT NULL DEFAULT '',
                            scope_variant TEXT NOT NULL DEFAULT '',
                            status TEXT NOT NULL DEFAULT '',
                            input_fingerprint TEXT NOT NULL DEFAULT '',
                            local_path TEXT NOT NULL DEFAULT '',
                            chunk_id TEXT NOT NULL DEFAULT '',
                            unit_id TEXT NOT NULL DEFAULT '',
                            stage_step TEXT NOT NULL DEFAULT '',
                            retry_mode TEXT NOT NULL DEFAULT '',
                            retry_entry_point TEXT NOT NULL DEFAULT '',
                            required_action TEXT NOT NULL DEFAULT '',
                            error_class TEXT NOT NULL DEFAULT '',
                            error_code TEXT NOT NULL DEFAULT '',
                            error_message TEXT NOT NULL DEFAULT '',
                            dirty_reason TEXT NOT NULL DEFAULT '',
                            dirty_at_ms INTEGER NOT NULL DEFAULT 0,
                            plan_context_json TEXT NOT NULL DEFAULT '',
                            resource_snapshot_json TEXT NOT NULL DEFAULT '',
                            attempt_count INTEGER NOT NULL DEFAULT 0,
                            result_hash TEXT NOT NULL DEFAULT '',
                            updated_at_ms INTEGER NOT NULL DEFAULT 0,
                            PRIMARY KEY (scope_ref)
                        )
                        """
                );
                ensureTableColumn(statement, connection, "scope_nodes", "normalized_video_key", "TEXT NOT NULL DEFAULT ''");
                ensureTableColumn(statement, connection, "scope_nodes", "stage", "TEXT NOT NULL DEFAULT ''");
                ensureTableColumn(statement, connection, "scope_nodes", "scope_type", "TEXT NOT NULL DEFAULT ''");
                ensureTableColumn(statement, connection, "scope_nodes", "scope_id", "TEXT NOT NULL DEFAULT ''");
                ensureTableColumn(statement, connection, "scope_nodes", "scope_variant", "TEXT NOT NULL DEFAULT ''");
                ensureTableColumn(statement, connection, "scope_nodes", "status", "TEXT NOT NULL DEFAULT ''");
                ensureTableColumn(statement, connection, "scope_nodes", "input_fingerprint", "TEXT NOT NULL DEFAULT ''");
                ensureTableColumn(statement, connection, "scope_nodes", "local_path", "TEXT NOT NULL DEFAULT ''");
                ensureTableColumn(statement, connection, "scope_nodes", "chunk_id", "TEXT NOT NULL DEFAULT ''");
                ensureTableColumn(statement, connection, "scope_nodes", "unit_id", "TEXT NOT NULL DEFAULT ''");
                ensureTableColumn(statement, connection, "scope_nodes", "stage_step", "TEXT NOT NULL DEFAULT ''");
                ensureTableColumn(statement, connection, "scope_nodes", "retry_mode", "TEXT NOT NULL DEFAULT ''");
                ensureTableColumn(statement, connection, "scope_nodes", "retry_entry_point", "TEXT NOT NULL DEFAULT ''");
                ensureTableColumn(statement, connection, "scope_nodes", "required_action", "TEXT NOT NULL DEFAULT ''");
                ensureTableColumn(statement, connection, "scope_nodes", "error_class", "TEXT NOT NULL DEFAULT ''");
                ensureTableColumn(statement, connection, "scope_nodes", "error_code", "TEXT NOT NULL DEFAULT ''");
                ensureTableColumn(statement, connection, "scope_nodes", "error_message", "TEXT NOT NULL DEFAULT ''");
                ensureTableColumn(statement, connection, "scope_nodes", "dirty_reason", "TEXT NOT NULL DEFAULT ''");
                ensureTableColumn(statement, connection, "scope_nodes", "dirty_at_ms", "INTEGER NOT NULL DEFAULT 0");
                ensureTableColumn(statement, connection, "scope_nodes", "plan_context_json", "TEXT NOT NULL DEFAULT ''");
                ensureTableColumn(statement, connection, "scope_nodes", "resource_snapshot_json", "TEXT NOT NULL DEFAULT ''");
                ensureTableColumn(statement, connection, "scope_nodes", "attempt_count", "INTEGER NOT NULL DEFAULT 0");
                ensureTableColumn(statement, connection, "scope_nodes", "result_hash", "TEXT NOT NULL DEFAULT ''");
                ensureTableColumn(statement, connection, "scope_nodes", "updated_at_ms", "INTEGER NOT NULL DEFAULT 0");
                statement.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_scope_nodes_stage_status
                        ON scope_nodes(stage, scope_type, status, updated_at_ms DESC)
                        """
                );
                statement.execute(
                        """
                        CREATE TABLE IF NOT EXISTS scope_edges (
                            scope_ref TEXT NOT NULL,
                            depends_on_scope_ref TEXT NOT NULL,
                            dependency_fingerprint TEXT NOT NULL DEFAULT '',
                            updated_at_ms INTEGER NOT NULL DEFAULT 0,
                            PRIMARY KEY (scope_ref, depends_on_scope_ref)
                        )
                        """
                );
                statement.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_scope_edges_dependency
                        ON scope_edges(depends_on_scope_ref, updated_at_ms DESC)
                        """
                );
                statement.execute(
                        """
                        CREATE TABLE IF NOT EXISTS chunk_records (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            normalized_video_key TEXT NOT NULL DEFAULT '',
                            stage TEXT NOT NULL,
                            chunk_id TEXT NOT NULL,
                            input_fingerprint TEXT NOT NULL,
                            attempt INTEGER NOT NULL,
                            status TEXT NOT NULL,
                            result_hash TEXT NOT NULL DEFAULT '',
                            error_class TEXT NOT NULL DEFAULT '',
                            error_code TEXT NOT NULL DEFAULT '',
                            error_message TEXT NOT NULL DEFAULT '',
                            updated_at_ms INTEGER NOT NULL DEFAULT 0,
                            committed_at_ms INTEGER NOT NULL DEFAULT 0,
                            UNIQUE(stage, chunk_id, attempt)
                        )
                        """
                );
                statement.execute(
                        """
                        CREATE TABLE IF NOT EXISTS chunk_record_content (
                            chunk_record_id INTEGER PRIMARY KEY,
                            result_codec TEXT NOT NULL DEFAULT '',
                            result_payload BLOB,
                            FOREIGN KEY(chunk_record_id) REFERENCES chunk_records(id) ON DELETE CASCADE
                        )
                        """
                );
                statement.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_chunk_records_stage_chunk_status
                        ON chunk_records(stage, chunk_id, status, committed_at_ms DESC, attempt DESC)
                        """
                );
                statement.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_chunk_records_stage_status
                        ON chunk_records(stage, status, updated_at_ms DESC)
                        """
                );
                statement.execute("DROP TABLE IF EXISTS task_artifacts");
                statement.execute("DROP TABLE IF EXISTS stage_journal_events");
                statement.execute("DROP TABLE IF EXISTS stage_outputs_manifests");
            }
        }
    }

    private void rebuildLegacyTaskLocalTables(Connection connection, Statement statement) throws SQLException {
        if (hasTableColumn(connection, STAGE_SNAPSHOT_TABLE, "output_dir")
                || hasTableColumn(connection, STAGE_SNAPSHOT_TABLE, "task_id")) {
            rebuildStageSnapshotsTable(connection, statement);
        }
        if (hasTableColumn(connection, SCOPE_NODES_TABLE, "output_dir")
                || hasTableColumn(connection, SCOPE_NODES_TABLE, "task_id")
                || hasTableColumn(connection, SCOPE_NODES_TABLE, "storage_key")) {
            rebuildScopeNodesTable(connection, statement);
        }
        if (hasTableColumn(connection, SCOPE_EDGES_TABLE, "output_dir")) {
            rebuildScopeEdgesTable(statement);
        }
    }

    private void rebuildStageSnapshotsTable(Connection connection, Statement statement) throws SQLException {
        statement.execute("DROP TABLE IF EXISTS stage_snapshots__new");
        statement.execute(
                """
                CREATE TABLE stage_snapshots__new (
                    stage TEXT NOT NULL,
                    stage_owner TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT '',
                    checkpoint TEXT NOT NULL DEFAULT '',
                    updated_at_ms INTEGER NOT NULL DEFAULT 0,
                    completed INTEGER NOT NULL DEFAULT 0,
                    pending INTEGER NOT NULL DEFAULT 0,
                    stage_state_path TEXT NOT NULL DEFAULT '',
                    retry_mode TEXT NOT NULL DEFAULT '',
                    retry_entry_point TEXT NOT NULL DEFAULT '',
                    required_action TEXT NOT NULL DEFAULT '',
                    retry_strategy TEXT NOT NULL DEFAULT '',
                    operator_action TEXT NOT NULL DEFAULT '',
                    action_hint TEXT NOT NULL DEFAULT '',
                    error_class TEXT NOT NULL DEFAULT '',
                    error_code TEXT NOT NULL DEFAULT '',
                    error_message TEXT NOT NULL DEFAULT '',
                    subtitle_path TEXT NOT NULL DEFAULT '',
                    domain TEXT NOT NULL DEFAULT '',
                    main_topic TEXT NOT NULL DEFAULT '',
                    PRIMARY KEY (stage)
                )
                """
        );
        statement.execute(
                "INSERT OR REPLACE INTO stage_snapshots__new ("
                        + "stage, stage_owner, status, checkpoint, updated_at_ms, completed, pending, "
                        + "stage_state_path, retry_mode, retry_entry_point, required_action, retry_strategy, "
                        + "operator_action, action_hint, error_class, error_code, error_message, "
                        + "subtitle_path, domain, main_topic"
                        + ") SELECT "
                        + legacyColumnOrDefault(connection, STAGE_SNAPSHOT_TABLE, "stage", "''") + ", "
                        + legacyColumnOrDefault(connection, STAGE_SNAPSHOT_TABLE, "stage_owner", "''") + ", "
                        + legacyColumnOrDefault(connection, STAGE_SNAPSHOT_TABLE, "status", "''") + ", "
                        + legacyColumnOrDefault(connection, STAGE_SNAPSHOT_TABLE, "checkpoint", "''") + ", "
                        + legacyColumnOrDefault(connection, STAGE_SNAPSHOT_TABLE, "updated_at_ms", "0") + ", "
                        + legacyColumnOrDefault(connection, STAGE_SNAPSHOT_TABLE, "completed", "0") + ", "
                        + legacyColumnOrDefault(connection, STAGE_SNAPSHOT_TABLE, "pending", "0") + ", "
                        + legacyColumnOrDefault(connection, STAGE_SNAPSHOT_TABLE, "stage_state_path", "''") + ", "
                        + legacyColumnOrDefault(connection, STAGE_SNAPSHOT_TABLE, "retry_mode", "''") + ", "
                        + legacyColumnOrDefault(connection, STAGE_SNAPSHOT_TABLE, "retry_entry_point", "''") + ", "
                        + legacyColumnOrDefault(connection, STAGE_SNAPSHOT_TABLE, "required_action", "''") + ", "
                        + legacyColumnOrDefault(connection, STAGE_SNAPSHOT_TABLE, "retry_strategy", "''") + ", "
                        + legacyColumnOrDefault(connection, STAGE_SNAPSHOT_TABLE, "operator_action", "''") + ", "
                        + legacyColumnOrDefault(connection, STAGE_SNAPSHOT_TABLE, "action_hint", "''") + ", "
                        + legacyColumnOrDefault(connection, STAGE_SNAPSHOT_TABLE, "error_class", "''") + ", "
                        + legacyColumnOrDefault(connection, STAGE_SNAPSHOT_TABLE, "error_code", "''") + ", "
                        + legacyColumnOrDefault(connection, STAGE_SNAPSHOT_TABLE, "error_message", "''") + ", "
                        + legacyColumnOrDefault(connection, STAGE_SNAPSHOT_TABLE, "subtitle_path", "''") + ", "
                        + legacyColumnOrDefault(connection, STAGE_SNAPSHOT_TABLE, "domain", "''") + ", "
                        + legacyColumnOrDefault(connection, STAGE_SNAPSHOT_TABLE, "main_topic", "''")
                        + " FROM " + STAGE_SNAPSHOT_TABLE
                        + " ORDER BY updated_at_ms ASC, rowid ASC"
        );
        statement.execute("DROP TABLE " + STAGE_SNAPSHOT_TABLE);
        statement.execute("ALTER TABLE stage_snapshots__new RENAME TO " + STAGE_SNAPSHOT_TABLE);
    }

    private void rebuildScopeNodesTable(Connection connection, Statement statement) throws SQLException {
        statement.execute("DROP TABLE IF EXISTS scope_nodes__new");
        statement.execute(
                """
                CREATE TABLE scope_nodes__new (
                    scope_ref TEXT NOT NULL,
                    normalized_video_key TEXT NOT NULL DEFAULT '',
                    stage TEXT NOT NULL DEFAULT '',
                    scope_type TEXT NOT NULL DEFAULT '',
                    scope_id TEXT NOT NULL DEFAULT '',
                    scope_variant TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT '',
                    input_fingerprint TEXT NOT NULL DEFAULT '',
                    local_path TEXT NOT NULL DEFAULT '',
                    chunk_id TEXT NOT NULL DEFAULT '',
                    unit_id TEXT NOT NULL DEFAULT '',
                    stage_step TEXT NOT NULL DEFAULT '',
                    retry_mode TEXT NOT NULL DEFAULT '',
                    retry_entry_point TEXT NOT NULL DEFAULT '',
                    required_action TEXT NOT NULL DEFAULT '',
                    error_class TEXT NOT NULL DEFAULT '',
                    error_code TEXT NOT NULL DEFAULT '',
                    error_message TEXT NOT NULL DEFAULT '',
                    dirty_reason TEXT NOT NULL DEFAULT '',
                    dirty_at_ms INTEGER NOT NULL DEFAULT 0,
                    plan_context_json TEXT NOT NULL DEFAULT '',
                    resource_snapshot_json TEXT NOT NULL DEFAULT '',
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    result_hash TEXT NOT NULL DEFAULT '',
                    updated_at_ms INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (scope_ref)
                )
                """
        );
        statement.execute(
                "INSERT OR REPLACE INTO scope_nodes__new ("
                        + "scope_ref, normalized_video_key, stage, scope_type, scope_id, scope_variant, "
                        + "status, input_fingerprint, local_path, chunk_id, unit_id, stage_step, "
                        + "retry_mode, retry_entry_point, required_action, error_class, error_code, error_message, "
                        + "dirty_reason, dirty_at_ms, plan_context_json, resource_snapshot_json, attempt_count, result_hash, updated_at_ms"
                        + ") SELECT "
                        + legacyColumnOrDefault(connection, SCOPE_NODES_TABLE, "scope_ref", "''") + ", "
                        + legacyColumnOrDefault(connection, SCOPE_NODES_TABLE, "normalized_video_key", "''") + ", "
                        + legacyColumnOrDefault(connection, SCOPE_NODES_TABLE, "stage", "''") + ", "
                        + legacyColumnOrDefault(connection, SCOPE_NODES_TABLE, "scope_type", "''") + ", "
                        + legacyColumnOrDefault(connection, SCOPE_NODES_TABLE, "scope_id", "''") + ", "
                        + legacyColumnOrDefault(connection, SCOPE_NODES_TABLE, "scope_variant", "''") + ", "
                        + legacyColumnOrDefault(connection, SCOPE_NODES_TABLE, "status", "''") + ", "
                        + legacyColumnOrDefault(connection, SCOPE_NODES_TABLE, "input_fingerprint", "''") + ", "
                        + legacyColumnOrDefault(connection, SCOPE_NODES_TABLE, "local_path", "''") + ", "
                        + legacyColumnOrDefault(connection, SCOPE_NODES_TABLE, "chunk_id", "''") + ", "
                        + legacyColumnOrDefault(connection, SCOPE_NODES_TABLE, "unit_id", "''") + ", "
                        + legacyColumnOrDefault(connection, SCOPE_NODES_TABLE, "stage_step", "''") + ", "
                        + legacyColumnOrDefault(connection, SCOPE_NODES_TABLE, "retry_mode", "''") + ", "
                        + legacyColumnOrDefault(connection, SCOPE_NODES_TABLE, "retry_entry_point", "''") + ", "
                        + legacyColumnOrDefault(connection, SCOPE_NODES_TABLE, "required_action", "''") + ", "
                        + legacyColumnOrDefault(connection, SCOPE_NODES_TABLE, "error_class", "''") + ", "
                        + legacyColumnOrDefault(connection, SCOPE_NODES_TABLE, "error_code", "''") + ", "
                        + legacyColumnOrDefault(connection, SCOPE_NODES_TABLE, "error_message", "''") + ", "
                        + legacyColumnOrDefault(connection, SCOPE_NODES_TABLE, "dirty_reason", "''") + ", "
                        + legacyColumnOrDefault(connection, SCOPE_NODES_TABLE, "dirty_at_ms", "0") + ", "
                        + legacyColumnOrDefault(connection, SCOPE_NODES_TABLE, "plan_context_json", "''") + ", "
                        + legacyColumnOrDefault(connection, SCOPE_NODES_TABLE, "resource_snapshot_json", "''") + ", "
                        + legacyColumnOrDefault(connection, SCOPE_NODES_TABLE, "attempt_count", "0") + ", "
                        + legacyColumnOrDefault(connection, SCOPE_NODES_TABLE, "result_hash", "''") + ", "
                        + legacyColumnOrDefault(connection, SCOPE_NODES_TABLE, "updated_at_ms", "0")
                        + " FROM " + SCOPE_NODES_TABLE
                        + " ORDER BY updated_at_ms ASC, rowid ASC"
        );
        statement.execute("DROP TABLE " + SCOPE_NODES_TABLE);
        statement.execute("ALTER TABLE scope_nodes__new RENAME TO " + SCOPE_NODES_TABLE);
    }

    private void rebuildScopeEdgesTable(Statement statement) throws SQLException {
        statement.execute("DROP TABLE IF EXISTS scope_edges__new");
        statement.execute(
                """
                CREATE TABLE scope_edges__new (
                    scope_ref TEXT NOT NULL,
                    depends_on_scope_ref TEXT NOT NULL,
                    dependency_fingerprint TEXT NOT NULL DEFAULT '',
                    updated_at_ms INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (scope_ref, depends_on_scope_ref)
                )
                """
        );
        statement.execute(
                "INSERT OR REPLACE INTO scope_edges__new (scope_ref, depends_on_scope_ref, dependency_fingerprint, updated_at_ms) "
                        + "SELECT scope_ref, depends_on_scope_ref, COALESCE(dependency_fingerprint, ''), COALESCE(updated_at_ms, 0) "
                        + "FROM " + SCOPE_EDGES_TABLE
        );
        statement.execute("DROP TABLE " + SCOPE_EDGES_TABLE);
        statement.execute("ALTER TABLE scope_edges__new RENAME TO " + SCOPE_EDGES_TABLE);
    }

    private String legacyColumnOrDefault(
            Connection connection,
            String tableName,
            String columnName,
            String defaultLiteral
    ) throws SQLException {
        if (!hasTableColumn(connection, tableName, columnName)) {
            return defaultLiteral;
        }
        return "COALESCE(" + columnName + ", " + defaultLiteral + ")";
    }

    private void ensureTableColumn(
            Statement statement,
            Connection connection,
            String tableName,
            String columnName,
            String columnDefinition
    ) throws SQLException {
        if (hasTableColumn(connection, tableName, columnName)) {
            return;
        }
        statement.execute("ALTER TABLE " + tableName + " ADD COLUMN " + columnName + " " + columnDefinition);
    }

    private boolean hasTableColumn(Connection connection, String tableName, String columnName) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("PRAGMA table_info(" + tableName + ")");
             ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                if (columnName.equalsIgnoreCase(resultSet.getString("name"))) {
                    return true;
                }
            }
            return false;
        }
    }

    private Connection openRuntimeStateConnection(Path dbPath) throws SQLException, IOException {
        Path normalizedDbPath = dbPath.toAbsolutePath().normalize();
        Files.createDirectories(normalizedDbPath.getParent());
        Connection connection = DriverManager.getConnection("jdbc:sqlite:" + normalizedDbPath);
        try (Statement statement = connection.createStatement()) {
            statement.execute("PRAGMA busy_timeout=" + SQLITE_BUSY_TIMEOUT_MS);
            statement.execute("PRAGMA journal_mode=WAL");
            statement.execute("PRAGMA synchronous=FULL");
        }
        return connection;
    }

    private Path resolveTaskRoot(String outputDir) {
        Path resolved = Paths.get(normalizeText(outputDir)).toAbsolutePath().normalize();
        String leaf = normalizeText(resolved.getFileName() != null ? resolved.getFileName().toString() : "");
        if ("intermediates".equalsIgnoreCase(leaf)) {
            Path parent = resolved.getParent();
            return parent != null ? parent : resolved;
        }
        return resolved;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> readJsonMap(Path path) {
        if (path == null || !Files.isRegularFile(path)) {
            return new LinkedHashMap<>();
        }
        try {
            Object payload = objectMapper.readValue(path.toFile(), Map.class);
            if (payload instanceof Map<?, ?> rawMap) {
                Map<String, Object> normalized = new LinkedHashMap<>();
                for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
                    normalized.put(String.valueOf(entry.getKey()), entry.getValue());
                }
                return normalized;
            }
        } catch (Exception error) {
            logger.warn("read runtime json failed: path={} err={}", path, error.getMessage());
        }
        return new LinkedHashMap<>();
    }

    @SuppressWarnings("unchecked")
    private List<String> readJsonList(String payloadJson) {
        if (payloadJson == null || payloadJson.isBlank()) {
            return new ArrayList<>();
        }
        try {
            Object payload = objectMapper.readValue(payloadJson, List.class);
            if (payload instanceof List<?> rawList) {
                List<String> normalized = new ArrayList<>();
                for (Object rawValue : rawList) {
                    String value = normalizeText(rawValue != null ? String.valueOf(rawValue) : "");
                    if (!value.isBlank()) {
                        normalized.add(value);
                    }
                }
                return normalized;
            }
        } catch (Exception error) {
            logger.warn("read runtime stage payload list json failed: err={}", error.getMessage());
        }
        return new ArrayList<>();
    }

    private byte[] decodeJsonBlob(String codec, byte[] blob) throws IOException {
        if (blob == null || blob.length == 0) {
            return new byte[0];
        }
        String normalizedCodec = normalizeLowercaseText(codec, "json-utf8");
        if ("json-utf8".equals(normalizedCodec) || "json".equals(normalizedCodec)) {
            return blob;
        }
        if ("json-utf8+zlib".equals(normalizedCodec) || "zlib".equals(normalizedCodec)) {
            try (java.io.ByteArrayInputStream inputStream = new java.io.ByteArrayInputStream(blob);
                 InflaterInputStream inflaterInputStream = new InflaterInputStream(inputStream);
                 java.io.ByteArrayOutputStream outputStream = new java.io.ByteArrayOutputStream()) {
                inflaterInputStream.transferTo(outputStream);
                return outputStream.toByteArray();
            }
        }
        throw new IOException("unsupported json blob codec: " + normalizedCodec);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> extractMapPayload(Object payload) {
        if (payload instanceof Map<?, ?> rawMap) {
            Map<String, Object> normalized = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
                normalized.put(String.valueOf(entry.getKey()), entry.getValue());
            }
            return normalized;
        }
        if (payload instanceof String rawText && !rawText.isBlank()) {
            try {
                Object parsed = objectMapper.readValue(rawText, LinkedHashMap.class);
                if (parsed instanceof Map<?, ?> rawMap) {
                    Map<String, Object> normalized = new LinkedHashMap<>();
                    for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
                        normalized.put(String.valueOf(entry.getKey()), entry.getValue());
                    }
                    return normalized;
                }
            } catch (Exception error) {
                logger.warn("parse runtime map payload failed: err={}", error.getMessage());
            }
        }
        return new LinkedHashMap<>();
    }

    private String writeJsonString(Map<String, Object> payload) {
        if (payload == null || payload.isEmpty()) {
            return "";
        }
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (Exception error) {
            throw new IllegalStateException("serialize runtime json payload failed", error);
        }
    }

    private void writeJsonAtomically(Path targetPath, Object payload) throws IOException {
        Path normalizedTarget = targetPath.toAbsolutePath().normalize();
        Files.createDirectories(normalizedTarget.getParent());
        Path tempPath = normalizedTarget.resolveSibling(normalizedTarget.getFileName() + ".tmp");
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(tempPath.toFile(), payload);
        try {
            Files.move(tempPath, normalizedTarget, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException atomicError) {
            Files.move(tempPath, normalizedTarget, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private String firstPayloadText(Map<String, Object> payload, String... keys) {
        if (payload == null || keys == null) {
            return "";
        }
        for (String key : keys) {
            Object value = payload.get(key);
            if (value == null) {
                continue;
            }
            String normalized = normalizeText(String.valueOf(value));
            if (!normalized.isBlank()) {
                return normalized;
            }
        }
        return "";
    }

    private String firstNonBlank(String... values) {
        if (values == null) {
            return "";
        }
        for (String value : values) {
            String normalized = normalizeText(value);
            if (!normalized.isBlank()) {
                return normalized;
            }
        }
        return "";
    }

    private String sanitizeScopeSegment(String value, String fallback, int maxLength, boolean lowercase) {
        String normalized = normalizeText(value, fallback);
        normalized = normalized.replaceAll("[^0-9A-Za-z._-]+", "_");
        normalized = normalized.replaceAll("_{2,}", "_");
        normalized = normalized.replaceAll("^[._-]+|[._-]+$", "");
        if (lowercase) {
            normalized = normalized.toLowerCase(Locale.ROOT);
        }
        if (normalized.isBlank()) {
            normalized = normalizeText(fallback);
            if (lowercase) {
                normalized = normalized.toLowerCase(Locale.ROOT);
            }
        }
        if (maxLength > 0 && normalized.length() > maxLength) {
            normalized = normalized.substring(0, maxLength);
        }
        return normalized.isBlank() ? normalizeText(fallback) : normalized;
    }

    private String normalizeStage(String stage) {
        String normalized = normalizeText(stage, "unknown_stage").toLowerCase(Locale.ROOT);
        return normalized.isBlank() ? "unknown_stage" : normalized;
    }

    private boolean shouldWriteStageFileMirrors() {
        String configured = firstNonBlank(
                System.getProperty("task.runtime.writeStageFileMirrors"),
                System.getenv("TASK_RUNTIME_WRITE_STAGE_FILE_MIRRORS")
        ).toLowerCase(Locale.ROOT);
        return Set.of("1", "true", "yes", "on").contains(configured);
    }

    private int parseInt(Object value, int fallback) {
        if (value == null) {
            return fallback;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        try {
            return Integer.parseInt(String.valueOf(value).trim());
        } catch (Exception ignored) {
            return fallback;
        }
    }

    private long parseLong(Object value, long fallback) {
        if (value == null) {
            return fallback;
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        try {
            return Long.parseLong(String.valueOf(value).trim());
        } catch (Exception ignored) {
            return fallback;
        }
    }

    private String normalizeStatus(String status) {
        String normalized = normalizeText(status, "UNKNOWN").toUpperCase(Locale.ROOT);
        if (Set.of("PLANNED", "PLANNING", "RETRYING", "RETRING").contains(normalized)) {
            return "PLANNED";
        }
        if (Set.of("RUNNING", "RUNING", "EXECUTING", "LOCAL_WRITING").contains(normalized)) {
            return "RUNNING";
        }
        if (Set.of("SUCCESS", "COMPLETED", "LOCAL_COMMITTED").contains(normalized)) {
            return "SUCCESS";
        }
        if (Set.of("MANUAL_NEEDED", "MANUAL_RETRY_REQUIRED", "MANUL_NEEDED").contains(normalized)) {
            return "MANUAL_NEEDED";
        }
        if (Set.of("ERROR", "AUTO_RETRY_WAIT").contains(normalized)) {
            return "ERROR";
        }
        if (Set.of("FAILED", "FATAL", "FAIL").contains(normalized)) {
            return "FAILED";
        }
        return normalized.isBlank() ? "UNKNOWN" : normalized;
    }

    private String normalizeFilename(String filename) {
        String normalized = normalizeText(filename, "artifact.json");
        return normalized.isBlank() ? "artifact.json" : normalized;
    }

    private String normalizeLowercaseText(String value, String fallback) {
        String normalized = normalizeText(value, fallback).toLowerCase(Locale.ROOT);
        return normalized.isBlank() ? normalizeText(fallback).toLowerCase(Locale.ROOT) : normalized;
    }

    private String normalizeText(String value) {
        return normalizeText(value, "");
    }

    private String normalizeText(String value, String fallback) {
        String normalized = value == null ? "" : value.trim();
        if (!normalized.isEmpty()) {
            return normalized;
        }
        return fallback == null ? "" : fallback.trim();
    }

    private static String normalizeStaticText(String value) {
        return value == null ? "" : value.trim();
    }

    private static int parseStaticInt(Object value, int fallback) {
        if (value == null) {
            return fallback;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        try {
            return Integer.parseInt(String.valueOf(value).trim());
        } catch (Exception ignored) {
            return fallback;
        }
    }

    private static long parseStaticLong(Object value, long fallback) {
        if (value == null) {
            return fallback;
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        try {
            return Long.parseLong(String.valueOf(value).trim());
        } catch (Exception ignored) {
            return fallback;
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> extractStaticMapPayload(Object payload) {
        if (payload instanceof Map<?, ?> rawMap) {
            Map<String, Object> normalized = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
                normalized.put(String.valueOf(entry.getKey()), entry.getValue());
            }
            return normalized;
        }
        return new LinkedHashMap<>();
    }
}
