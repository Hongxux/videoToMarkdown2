package com.mvp.module2.fusion.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public class TaskCleanupQueueRepository {

    private final JdbcTemplate jdbcTemplate;
    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    private final RowMapper<PendingCleanupTaskRecord> rowMapper = (rs, rowNum) -> new PendingCleanupTaskRecord(
            rs.getString("task_id"),
            rs.getString("storage_key"),
            rs.getString("task_root"),
            rs.getString("task_type"),
            rs.getString("task_status"),
            rs.getString("policy_version"),
            rs.getLong("ttl_millis"),
            rs.getLong("completed_at_ms"),
            rs.getLong("cleanup_after_ms"),
            rs.getLong("updated_at_ms"),
            rs.getString("last_error")
    );

    public TaskCleanupQueueRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
    }

    public void upsert(PendingCleanupTaskRecord record) {
        if (record == null || isBlank(record.taskId())) {
            return;
        }
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("task_id", record.taskId())
                .addValue("storage_key", record.storageKey())
                .addValue("task_root", record.taskRoot())
                .addValue("task_type", record.taskType())
                .addValue("task_status", record.taskStatus())
                .addValue("policy_version", record.policyVersion())
                .addValue("ttl_millis", record.ttlMillis())
                .addValue("completed_at_ms", record.completedAtMs())
                .addValue("cleanup_after_ms", record.cleanupAfterMs())
                .addValue("updated_at_ms", record.updatedAtMs())
                .addValue("last_error", nullable(record.lastError()));
        namedParameterJdbcTemplate.update(
                """
                INSERT INTO task_cleanup_queue (
                    task_id,
                    storage_key,
                    task_root,
                    task_type,
                    task_status,
                    policy_version,
                    ttl_millis,
                    completed_at_ms,
                    cleanup_after_ms,
                    updated_at_ms,
                    last_error
                )
                VALUES (
                    :task_id,
                    :storage_key,
                    :task_root,
                    :task_type,
                    :task_status,
                    :policy_version,
                    :ttl_millis,
                    :completed_at_ms,
                    :cleanup_after_ms,
                    :updated_at_ms,
                    :last_error
                )
                ON CONFLICT(task_id) DO UPDATE SET
                    storage_key = excluded.storage_key,
                    task_root = excluded.task_root,
                    task_type = excluded.task_type,
                    task_status = excluded.task_status,
                    policy_version = excluded.policy_version,
                    ttl_millis = excluded.ttl_millis,
                    completed_at_ms = excluded.completed_at_ms,
                    cleanup_after_ms = excluded.cleanup_after_ms,
                    updated_at_ms = excluded.updated_at_ms,
                    last_error = excluded.last_error
                """,
                params
        );
    }

    public void delete(String taskId) {
        String normalizedTaskId = trim(taskId);
        if (normalizedTaskId == null) {
            return;
        }
        jdbcTemplate.update("DELETE FROM task_cleanup_queue WHERE task_id = ?", normalizedTaskId);
    }

    public Optional<PendingCleanupTaskRecord> findByTaskId(String taskId) {
        String normalizedTaskId = trim(taskId);
        if (normalizedTaskId == null) {
            return Optional.empty();
        }
        List<PendingCleanupTaskRecord> rows = jdbcTemplate.query(
                """
                SELECT
                    task_id,
                    storage_key,
                    task_root,
                    task_type,
                    task_status,
                    policy_version,
                    ttl_millis,
                    completed_at_ms,
                    cleanup_after_ms,
                    updated_at_ms,
                    last_error
                FROM task_cleanup_queue
                WHERE task_id = ?
                LIMIT 1
                """,
                rowMapper,
                normalizedTaskId
        );
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0));
    }

    public List<PendingCleanupTaskRecord> listDue(long nowMs, int limit) {
        int safeLimit = Math.max(1, limit);
        return jdbcTemplate.query(
                """
                SELECT
                    task_id,
                    storage_key,
                    task_root,
                    task_type,
                    task_status,
                    policy_version,
                    ttl_millis,
                    completed_at_ms,
                    cleanup_after_ms,
                    updated_at_ms,
                    last_error
                FROM task_cleanup_queue
                WHERE cleanup_after_ms <= ?
                ORDER BY cleanup_after_ms ASC, task_id ASC
                LIMIT ?
                """,
                rowMapper,
                nowMs,
                safeLimit
        );
    }

    public int refreshPolicy(String policyVersion, long ttlMillis, long updatedAtMs) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("policy_version", trim(policyVersion))
                .addValue("ttl_millis", ttlMillis)
                .addValue("updated_at_ms", updatedAtMs);
        return namedParameterJdbcTemplate.update(
                """
                UPDATE task_cleanup_queue
                SET
                    policy_version = :policy_version,
                    ttl_millis = :ttl_millis,
                    cleanup_after_ms = completed_at_ms + :ttl_millis,
                    updated_at_ms = :updated_at_ms
                WHERE policy_version <> :policy_version
                   OR ttl_millis <> :ttl_millis
                """,
                params
        );
    }

    public void markFailure(String taskId, String lastError, long updatedAtMs) {
        String normalizedTaskId = trim(taskId);
        if (normalizedTaskId == null) {
            return;
        }
        jdbcTemplate.update(
                """
                UPDATE task_cleanup_queue
                SET last_error = ?, updated_at_ms = ?
                WHERE task_id = ?
                """,
                nullable(lastError),
                updatedAtMs,
                normalizedTaskId
        );
    }

    private String nullable(String value) {
        String normalized = trim(value);
        return normalized == null ? null : normalized;
    }

    private String trim(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private boolean isBlank(String value) {
        return trim(value) == null;
    }

    public record PendingCleanupTaskRecord(
            String taskId,
            String storageKey,
            String taskRoot,
            String taskType,
            String taskStatus,
            String policyVersion,
            long ttlMillis,
            long completedAtMs,
            long cleanupAfterMs,
            long updatedAtMs,
            String lastError
    ) {
    }
}
