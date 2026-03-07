package com.mvp.module2.fusion.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

@Repository
public class TaskManualCollectionRepository {

    private static final int SQLITE_IN_BATCH_LIMIT = 300;

    private final JdbcTemplate jdbcTemplate;

    public TaskManualCollectionRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public Map<String, String> findCollectionPathByTaskPaths(Collection<String> taskPaths) {
        if (taskPaths == null || taskPaths.isEmpty()) {
            return Map.of();
        }
        List<String> normalizedPaths = new ArrayList<>();
        for (String rawTaskPath : taskPaths) {
            String normalizedTaskPath = normalizeTaskPath(rawTaskPath);
            if (!normalizedTaskPath.isEmpty()) {
                normalizedPaths.add(normalizedTaskPath);
            }
        }
        if (normalizedPaths.isEmpty()) {
            return Map.of();
        }

        List<String> deduped = new ArrayList<>(new LinkedHashSet<>(normalizedPaths));
        Map<String, String> result = new LinkedHashMap<>();
        for (int offset = 0; offset < deduped.size(); offset += SQLITE_IN_BATCH_LIMIT) {
            int end = Math.min(deduped.size(), offset + SQLITE_IN_BATCH_LIMIT);
            List<String> batch = deduped.subList(offset, end);
            String placeholders = String.join(",", java.util.Collections.nCopies(batch.size(), "?"));
            String sql = """
                    SELECT task_path, collection_path
                    FROM task_manual_collection_bindings
                    WHERE task_path IN (%s)
                    """.formatted(placeholders);
            List<Map<String, String>> rows = jdbcTemplate.query(
                    sql,
                    (rs, rowNum) -> {
                        String taskPath = normalizeTaskPath(rs.getString("task_path"));
                        String collectionPath = normalizeCollectionPath(rs.getString("collection_path"));
                        if (taskPath.isEmpty() || collectionPath.isEmpty()) {
                            return Map.of();
                        }
                        return Map.of(taskPath, collectionPath);
                    },
                    batch.toArray()
            );
            for (Map<String, String> row : rows) {
                if (row.isEmpty()) {
                    continue;
                }
                row.forEach(result::put);
            }
        }
        return result;
    }

    public Map<String, String> listAllBindings() {
        List<Map<String, String>> rows = jdbcTemplate.query(
                """
                SELECT task_path, collection_path
                FROM task_manual_collection_bindings
                ORDER BY updated_at DESC
                """,
                (rs, rowNum) -> {
                    String taskPath = normalizeTaskPath(rs.getString("task_path"));
                    String collectionPath = normalizeCollectionPath(rs.getString("collection_path"));
                    if (taskPath.isEmpty() || collectionPath.isEmpty()) {
                        return Map.of();
                    }
                    return Map.of(taskPath, collectionPath);
                }
        );
        Map<String, String> result = new LinkedHashMap<>();
        for (Map<String, String> row : rows) {
            if (row.isEmpty()) {
                continue;
            }
            row.forEach(result::putIfAbsent);
        }
        return result;
    }

    @Transactional
    public int replaceAllBindings(Map<String, String> rawBindings) {
        jdbcTemplate.update("DELETE FROM task_manual_collection_bindings");
        if (rawBindings == null || rawBindings.isEmpty()) {
            return 0;
        }
        String now = Instant.now().toString();
        int inserted = 0;
        for (Map.Entry<String, String> entry : rawBindings.entrySet()) {
            String taskPath = normalizeTaskPath(entry.getKey());
            String collectionPath = normalizeCollectionPath(entry.getValue());
            if (taskPath.isEmpty() || collectionPath.isEmpty()) {
                continue;
            }
            inserted += jdbcTemplate.update(
                    """
                    INSERT INTO task_manual_collection_bindings (
                        task_path, collection_path, updated_at
                    ) VALUES (?, ?, ?)
                    """,
                    taskPath,
                    collectionPath,
                    now
            );
        }
        return inserted;
    }

    public static String normalizeTaskPath(String rawTaskPath) {
        if (rawTaskPath == null) {
            return "";
        }
        String normalized = rawTaskPath.trim().replace('\\', '/');
        while (normalized.startsWith("/")) {
            normalized = normalized.substring(1);
        }
        normalized = normalized.replaceAll("/+", "/");
        return normalized;
    }

    public static String normalizeCollectionPath(String rawCollectionPath) {
        if (rawCollectionPath == null) {
            return "";
        }
        String normalized = rawCollectionPath.trim().replace('\\', '/');
        if (normalized.isEmpty()) {
            return "";
        }
        String[] parts = normalized.split("/");
        List<String> kept = new ArrayList<>(parts.length);
        for (String rawPart : parts) {
            String segment = rawPart == null ? "" : rawPart.trim();
            if (segment.isEmpty()) {
                continue;
            }
            if (segment.length() > 120) {
                segment = segment.substring(0, 120);
            }
            kept.add(segment);
        }
        return String.join("/", kept);
    }
}
