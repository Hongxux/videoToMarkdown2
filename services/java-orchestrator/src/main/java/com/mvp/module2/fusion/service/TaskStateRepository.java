package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mvp.module2.fusion.queue.TaskQueueManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Repository
public class TaskStateRepository {

    private static final Logger logger = LoggerFactory.getLogger(TaskStateRepository.class);

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public TaskStateRepository(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public void upsertTask(TaskQueueManager.TaskEntry task) {
        if (task == null) {
            return;
        }
        String taskId = normalize(task.taskId);
        String userId = normalize(task.userId);
        String videoUrl = normalize(task.videoUrl);
        String priority = task.priority != null ? task.priority.name() : TaskQueueManager.Priority.NORMAL.name();
        String status = task.status != null ? task.status.name() : TaskQueueManager.TaskStatus.QUEUED.name();
        if (taskId.isEmpty() || userId.isEmpty() || videoUrl.isEmpty()) {
            return;
        }
        String now = Instant.now().toString();
        jdbcTemplate.update(
                """
                INSERT INTO task_runtime_state (
                    task_id,
                    user_id,
                    video_url,
                    normalized_video_key,
                    title,
                    output_dir,
                    priority,
                    status,
                    progress,
                    status_message,
                    result_path,
                    cleanup_source_path,
                    error_message,
                    duplicate_of_task_id,
                    book_options_json,
                    probe_payload_json,
                    recovery_payload_json,
                    created_at,
                    started_at,
                    completed_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(task_id) DO UPDATE SET
                    user_id = excluded.user_id,
                    video_url = excluded.video_url,
                    normalized_video_key = excluded.normalized_video_key,
                    title = excluded.title,
                    output_dir = excluded.output_dir,
                    priority = excluded.priority,
                    status = excluded.status,
                    progress = excluded.progress,
                    status_message = excluded.status_message,
                    result_path = excluded.result_path,
                    cleanup_source_path = excluded.cleanup_source_path,
                    error_message = excluded.error_message,
                    duplicate_of_task_id = excluded.duplicate_of_task_id,
                    book_options_json = excluded.book_options_json,
                    probe_payload_json = excluded.probe_payload_json,
                    recovery_payload_json = excluded.recovery_payload_json,
                    created_at = excluded.created_at,
                    started_at = excluded.started_at,
                    completed_at = excluded.completed_at,
                    updated_at = excluded.updated_at
                """,
                taskId,
                userId,
                videoUrl,
                nullable(task.normalizedVideoKey),
                nullable(task.title),
                nullable(task.outputDir),
                priority,
                status,
                task.progress,
                nullable(task.statusMessage),
                nullable(task.resultPath),
                nullable(task.cleanupSourcePath),
                nullable(task.errorMessage),
                nullable(task.duplicateOfTaskId),
                serializeBookOptions(task.bookOptions),
                serializeProbePayload(task.probePayload),
                serializeRecoveryPayload(task.recoveryPayload),
                task.createdAt != null ? task.createdAt.toString() : now,
                task.startedAt != null ? task.startedAt.toString() : null,
                task.completedAt != null ? task.completedAt.toString() : null,
                now
        );
    }

    public Optional<PersistedTaskRecord> findTask(String taskId) {
        String normalizedTaskId = normalize(taskId);
        if (normalizedTaskId.isEmpty()) {
            return Optional.empty();
        }
        List<PersistedTaskRecord> rows = jdbcTemplate.query(
                """
                SELECT
                    task_id,
                    user_id,
                    video_url,
                    normalized_video_key,
                    title,
                    output_dir,
                    priority,
                    status,
                    progress,
                    status_message,
                    result_path,
                    cleanup_source_path,
                    error_message,
                    duplicate_of_task_id,
                    book_options_json,
                    probe_payload_json,
                    recovery_payload_json,
                    created_at,
                    started_at,
                    completed_at,
                    updated_at
                FROM task_runtime_state
                WHERE task_id = ?
                LIMIT 1
                """,
                (rs, rowNum) -> new PersistedTaskRecord(
                        rs.getString("task_id"),
                        rs.getString("user_id"),
                        rs.getString("video_url"),
                        rs.getString("normalized_video_key"),
                        rs.getString("title"),
                        rs.getString("output_dir"),
                        rs.getString("priority"),
                        rs.getString("status"),
                        rs.getDouble("progress"),
                        rs.getString("status_message"),
                        rs.getString("result_path"),
                        rs.getString("cleanup_source_path"),
                        rs.getString("error_message"),
                        rs.getString("duplicate_of_task_id"),
                        deserializeBookOptions(rs.getString("book_options_json")),
                        deserializeProbePayload(rs.getString("probe_payload_json")),
                        deserializeRecoveryPayload(rs.getString("recovery_payload_json")),
                        parseInstant(rs.getString("created_at")),
                        parseInstant(rs.getString("started_at")),
                        parseInstant(rs.getString("completed_at")),
                        parseInstant(rs.getString("updated_at"))
                ),
                normalizedTaskId
        );
        if (rows.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(rows.get(0));
    }

    public List<PersistedTaskRecord> listAllTasks() {
        return jdbcTemplate.query(
                """
                SELECT
                    task_id,
                    user_id,
                    video_url,
                    normalized_video_key,
                    title,
                    output_dir,
                    priority,
                    status,
                    progress,
                    status_message,
                    result_path,
                    cleanup_source_path,
                    error_message,
                    duplicate_of_task_id,
                    book_options_json,
                    probe_payload_json,
                    recovery_payload_json,
                    created_at,
                    started_at,
                    completed_at,
                    updated_at
                FROM task_runtime_state
                ORDER BY created_at DESC
                """,
                (rs, rowNum) -> new PersistedTaskRecord(
                        rs.getString("task_id"),
                        rs.getString("user_id"),
                        rs.getString("video_url"),
                        rs.getString("normalized_video_key"),
                        rs.getString("title"),
                        rs.getString("output_dir"),
                        rs.getString("priority"),
                        rs.getString("status"),
                        rs.getDouble("progress"),
                        rs.getString("status_message"),
                        rs.getString("result_path"),
                        rs.getString("cleanup_source_path"),
                        rs.getString("error_message"),
                        rs.getString("duplicate_of_task_id"),
                        deserializeBookOptions(rs.getString("book_options_json")),
                        deserializeProbePayload(rs.getString("probe_payload_json")),
                        deserializeRecoveryPayload(rs.getString("recovery_payload_json")),
                        parseInstant(rs.getString("created_at")),
                        parseInstant(rs.getString("started_at")),
                        parseInstant(rs.getString("completed_at")),
                        parseInstant(rs.getString("updated_at"))
                )
        );
    }

    public Optional<PersistedTaskRecord> findLatestReusableTaskByNormalizedVideoKey(
            String normalizedVideoKey,
            String excludeTaskId
    ) {
        String normalizedKey = normalize(normalizedVideoKey);
        if (normalizedKey.isEmpty()) {
            return Optional.empty();
        }
        List<PersistedTaskRecord> rows = jdbcTemplate.query(
                """
                SELECT
                    task_id,
                    user_id,
                    video_url,
                    normalized_video_key,
                    title,
                    output_dir,
                    priority,
                    status,
                    progress,
                    status_message,
                    result_path,
                    cleanup_source_path,
                    error_message,
                    duplicate_of_task_id,
                    book_options_json,
                    probe_payload_json,
                    recovery_payload_json,
                    created_at,
                    started_at,
                    completed_at,
                    updated_at
                FROM task_runtime_state
                WHERE normalized_video_key = ?
                  AND task_id <> ?
                  AND status NOT IN ('FAILED', 'CANCELLED', 'DEDUPED', 'MANUAL_RETRY_REQUIRED', 'FATAL')
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (rs, rowNum) -> new PersistedTaskRecord(
                        rs.getString("task_id"),
                        rs.getString("user_id"),
                        rs.getString("video_url"),
                        rs.getString("normalized_video_key"),
                        rs.getString("title"),
                        rs.getString("output_dir"),
                        rs.getString("priority"),
                        rs.getString("status"),
                        rs.getDouble("progress"),
                        rs.getString("status_message"),
                        rs.getString("result_path"),
                        rs.getString("cleanup_source_path"),
                        rs.getString("error_message"),
                        rs.getString("duplicate_of_task_id"),
                        deserializeBookOptions(rs.getString("book_options_json")),
                        deserializeProbePayload(rs.getString("probe_payload_json")),
                        deserializeRecoveryPayload(rs.getString("recovery_payload_json")),
                        parseInstant(rs.getString("created_at")),
                        parseInstant(rs.getString("started_at")),
                        parseInstant(rs.getString("completed_at")),
                        parseInstant(rs.getString("updated_at"))
                ),
                normalizedKey,
                normalize(excludeTaskId)
        );
        if (rows.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(rows.get(0));
    }

    private String serializeBookOptions(TaskQueueManager.BookProcessingOptions options) {
        if (options == null) {
            return null;
        }
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("chapterSelector", nullable(options.chapterSelector));
        payload.put("sectionSelector", nullable(options.sectionSelector));
        payload.put("splitByChapter", options.splitByChapter);
        payload.put("splitBySection", options.splitBySection);
        payload.put("pageOffset", options.pageOffset);
        payload.put("bookTitle", nullable(options.bookTitle));
        payload.put("leafTitle", nullable(options.leafTitle));
        payload.put("leafOutlineIndex", nullable(options.leafOutlineIndex));
        payload.put("storageKey", nullable(options.storageKey));
        return writeJson(payload);
    }

    private TaskQueueManager.BookProcessingOptions deserializeBookOptions(String rawJson) {
        String normalizedJson = normalize(rawJson);
        if (normalizedJson.isEmpty()) {
            return null;
        }
        try {
            Map<String, Object> payload = objectMapper.readValue(
                    normalizedJson,
                    new TypeReference<Map<String, Object>>() {
                    }
            );
            TaskQueueManager.BookProcessingOptions options = new TaskQueueManager.BookProcessingOptions();
            options.chapterSelector = asText(payload.get("chapterSelector"));
            options.sectionSelector = asText(payload.get("sectionSelector"));
            options.splitByChapter = asBooleanObject(payload.get("splitByChapter"));
            options.splitBySection = asBooleanObject(payload.get("splitBySection"));
            options.pageOffset = asIntegerObject(payload.get("pageOffset"));
            options.bookTitle = asText(payload.get("bookTitle"));
            options.leafTitle = asText(payload.get("leafTitle"));
            options.leafOutlineIndex = asText(payload.get("leafOutlineIndex"));
            options.storageKey = asText(payload.get("storageKey"));
            if (options.chapterSelector == null
                    && options.sectionSelector == null
                    && options.splitByChapter == null
                    && options.splitBySection == null
                    && options.pageOffset == null
                    && options.bookTitle == null
                    && options.leafTitle == null
                    && options.leafOutlineIndex == null
                    && options.storageKey == null) {
                return null;
            }
            return options;
        } catch (Exception error) {
            logger.warn("deserialize persisted book options failed: {}", error.getMessage());
            return null;
        }
    }

    private String serializeProbePayload(Map<String, Object> probePayload) {
        if (probePayload == null || probePayload.isEmpty()) {
            return null;
        }
        return writeJson(probePayload);
    }

    private Map<String, Object> deserializeProbePayload(String rawJson) {
        String normalizedJson = normalize(rawJson);
        if (normalizedJson.isEmpty()) {
            return null;
        }
        try {
            return objectMapper.readValue(normalizedJson, new TypeReference<LinkedHashMap<String, Object>>() {
            });
        } catch (Exception error) {
            logger.warn("deserialize persisted probe payload failed: {}", error.getMessage());
            return null;
        }
    }

    private String serializeRecoveryPayload(Map<String, Object> recoveryPayload) {
        if (recoveryPayload == null || recoveryPayload.isEmpty()) {
            return null;
        }
        return writeJson(recoveryPayload);
    }

    private Map<String, Object> deserializeRecoveryPayload(String rawJson) {
        String normalizedJson = normalize(rawJson);
        if (normalizedJson.isEmpty()) {
            return null;
        }
        try {
            return objectMapper.readValue(normalizedJson, new TypeReference<LinkedHashMap<String, Object>>() {
            });
        } catch (Exception error) {
            logger.warn("deserialize persisted recovery payload failed: {}", error.getMessage());
            return null;
        }
    }

    private String writeJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException error) {
            logger.warn("serialize task state json failed: {}", error.getMessage());
            return null;
        }
    }

    private Instant parseInstant(String rawValue) {
        String value = normalize(rawValue);
        if (value.isEmpty()) {
            return null;
        }
        try {
            return Instant.parse(value);
        } catch (Exception error) {
            logger.warn("parse task timestamp failed: value={} err={}", value, error.getMessage());
            return null;
        }
    }

    private String normalize(String value) {
        if (value == null) {
            return "";
        }
        return value.trim();
    }

    private String nullable(String value) {
        String normalized = normalize(value);
        return normalized.isEmpty() ? null : normalized;
    }

    private String asText(Object value) {
        if (value == null) {
            return null;
        }
        String text = String.valueOf(value).trim();
        return text.isEmpty() ? null : text;
    }

    private Integer asIntegerObject(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        try {
            return Integer.parseInt(String.valueOf(value).trim());
        } catch (Exception error) {
            return null;
        }
    }

    private Boolean asBooleanObject(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Boolean boolValue) {
            return boolValue;
        }
        String text = String.valueOf(value).trim();
        if (text.isEmpty()) {
            return null;
        }
        return Boolean.parseBoolean(text);
    }

    public static class PersistedTaskRecord {
        public final String taskId;
        public final String userId;
        public final String videoUrl;
        public final String normalizedVideoKey;
        public final String title;
        public final String outputDir;
        public final String priority;
        public final String status;
        public final double progress;
        public final String statusMessage;
        public final String resultPath;
        public final String cleanupSourcePath;
        public final String errorMessage;
        public final String duplicateOfTaskId;
        public final TaskQueueManager.BookProcessingOptions bookOptions;
        public final Map<String, Object> probePayload;
        public final Map<String, Object> recoveryPayload;
        public final Instant createdAt;
        public final Instant startedAt;
        public final Instant completedAt;
        public final Instant updatedAt;

        public PersistedTaskRecord(
                String taskId,
                String userId,
                String videoUrl,
                String normalizedVideoKey,
                String title,
                String outputDir,
                String priority,
                String status,
                double progress,
                String statusMessage,
                String resultPath,
                String cleanupSourcePath,
                String errorMessage,
                String duplicateOfTaskId,
                TaskQueueManager.BookProcessingOptions bookOptions,
                Map<String, Object> probePayload,
                Map<String, Object> recoveryPayload,
                Instant createdAt,
                Instant startedAt,
                Instant completedAt,
                Instant updatedAt
        ) {
            this.taskId = taskId;
            this.userId = userId;
            this.videoUrl = videoUrl;
            this.normalizedVideoKey = normalizedVideoKey;
            this.title = title;
            this.outputDir = outputDir;
            this.priority = priority;
            this.status = status;
            this.progress = progress;
            this.statusMessage = statusMessage;
            this.resultPath = resultPath;
            this.cleanupSourcePath = cleanupSourcePath;
            this.errorMessage = errorMessage;
            this.duplicateOfTaskId = duplicateOfTaskId;
            this.bookOptions = bookOptions;
            this.probePayload = probePayload;
            this.recoveryPayload = recoveryPayload;
            this.createdAt = createdAt;
            this.startedAt = startedAt;
            this.completedAt = completedAt;
            this.updatedAt = updatedAt;
        }
    }
}
