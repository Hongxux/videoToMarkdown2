package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Repository
public class TaskTerminalEventRepository {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public TaskTerminalEventRepository(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public TerminalEventRecord appendEvent(
            String userId,
            String taskId,
            String status,
            Map<String, Object> payload
    ) {
        String normalizedUserId = normalize(userId);
        String normalizedTaskId = normalize(taskId);
        String normalizedStatus = normalize(status);
        if (normalizedUserId.isEmpty() || normalizedTaskId.isEmpty() || normalizedStatus.isEmpty() || payload == null) {
            return null;
        }
        String createdAt = Instant.now().toString();
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(connection -> {
            PreparedStatement statement = connection.prepareStatement(
                    """
                    INSERT INTO task_terminal_events (
                        user_id,
                        task_id,
                        status,
                        payload_json,
                        created_at
                    )
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    Statement.RETURN_GENERATED_KEYS
            );
            statement.setString(1, normalizedUserId);
            statement.setString(2, normalizedTaskId);
            statement.setString(3, normalizedStatus);
            statement.setString(4, "{}");
            statement.setString(5, createdAt);
            return statement;
        }, keyHolder);
        Number generatedKey = keyHolder.getKey();
        if (generatedKey == null) {
            return null;
        }
        long eventId = generatedKey.longValue();
        LinkedHashMap<String, Object> persistedPayload = new LinkedHashMap<>(payload);
        persistedPayload.put("eventId", eventId);
        persistedPayload.put("messageId", eventId);
        persistedPayload.put("requiresAck", true);
        persistedPayload.put("type", "taskTerminalEvent");
        jdbcTemplate.update(
                "UPDATE task_terminal_events SET payload_json = ? WHERE event_id = ?",
                serializePayload(persistedPayload),
                eventId
        );
        return new TerminalEventRecord(
                eventId,
                normalizedUserId,
                normalizedTaskId,
                normalizedStatus,
                persistedPayload,
                parseInstant(createdAt)
        );
    }

    public List<TerminalEventRecord> listEventsAfter(String userId, long lastAckedEventId, int limit) {
        String normalizedUserId = normalize(userId);
        if (normalizedUserId.isEmpty()) {
            return List.of();
        }
        int normalizedLimit = Math.max(1, Math.min(200, limit));
        return jdbcTemplate.query(
                """
                SELECT event_id, user_id, task_id, status, payload_json, created_at
                FROM task_terminal_events
                WHERE user_id = ?
                  AND event_id > ?
                ORDER BY event_id ASC
                LIMIT ?
                """,
                (rs, rowNum) -> {
                    long eventId = rs.getLong("event_id");
                    LinkedHashMap<String, Object> payload = deserializePayload(rs.getString("payload_json"));
                    payload.put("eventId", eventId);
                    payload.put("messageId", eventId);
                    payload.put("requiresAck", true);
                    payload.put("type", "taskTerminalEvent");
                    return new TerminalEventRecord(
                            eventId,
                            rs.getString("user_id"),
                            rs.getString("task_id"),
                            rs.getString("status"),
                            payload,
                            parseInstant(rs.getString("created_at"))
                    );
                },
                normalizedUserId,
                Math.max(0L, lastAckedEventId),
                normalizedLimit
        );
    }

    @Transactional
    public void acknowledgeThrough(String userId, long eventId) {
        String normalizedUserId = normalize(userId);
        if (normalizedUserId.isEmpty() || eventId <= 0L) {
            return;
        }
        jdbcTemplate.update(
                "DELETE FROM task_terminal_events WHERE user_id = ? AND event_id <= ?",
                normalizedUserId,
                eventId
        );
    }

    private String serializePayload(Map<String, Object> payload) {
        try {
            return objectMapper.writeValueAsString(payload != null ? payload : Map.of());
        } catch (Exception ex) {
            throw new IllegalStateException("failed to serialize task terminal event payload", ex);
        }
    }

    private LinkedHashMap<String, Object> deserializePayload(String rawPayload) {
        if (rawPayload == null || rawPayload.isBlank()) {
            return new LinkedHashMap<>();
        }
        try {
            Map<String, Object> parsed = objectMapper.readValue(
                    rawPayload,
                    new TypeReference<LinkedHashMap<String, Object>>() {
                    }
            );
            return new LinkedHashMap<>(parsed);
        } catch (Exception ex) {
            return new LinkedHashMap<>();
        }
    }

    private Instant parseInstant(String rawValue) {
        if (rawValue == null || rawValue.isBlank()) {
            return null;
        }
        try {
            return Instant.parse(rawValue);
        } catch (Exception ignored) {
            return null;
        }
    }

    private String normalize(String rawValue) {
        return rawValue == null ? "" : rawValue.trim();
    }

    public record TerminalEventRecord(
            long eventId,
            String userId,
            String taskId,
            String status,
            Map<String, Object> payload,
            Instant createdAt
    ) {
    }
}
