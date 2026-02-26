package com.mvp.module2.fusion.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Repository
public class CollectionRepository {

    private static final Logger logger = LoggerFactory.getLogger(CollectionRepository.class);

    private final JdbcTemplate jdbcTemplate;

    public CollectionRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Transactional
    public void upsertCollection(
            String collectionId,
            String platform,
            String canonicalId,
            String title,
            int totalEpisodes,
            String resolvedUrl,
            List<EpisodeInput> episodes
    ) {
        String normalizedCollectionId = normalizeRequired(collectionId, "collectionId");
        String now = Instant.now().toString();
        jdbcTemplate.update(
                """
                INSERT INTO video_collections (
                    collection_id, platform, canonical_id, title, total_episodes, resolved_url, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(collection_id) DO UPDATE SET
                    platform = excluded.platform,
                    canonical_id = excluded.canonical_id,
                    title = excluded.title,
                    total_episodes = excluded.total_episodes,
                    resolved_url = excluded.resolved_url,
                    updated_at = excluded.updated_at
                """,
                normalizedCollectionId,
                normalizeOptional(platform),
                normalizeOptional(canonicalId),
                normalizeOptional(title),
                Math.max(0, totalEpisodes),
                normalizeOptional(resolvedUrl),
                now,
                now
        );

        List<EpisodeInput> safeEpisodes = episodes != null ? episodes : List.of();
        for (EpisodeInput episode : safeEpisodes) {
            if (episode == null || episode.episodeNo <= 0) {
                continue;
            }
            jdbcTemplate.update(
                    """
                    INSERT INTO collection_episodes (
                        collection_id, episode_no, episode_title, episode_url, duration_sec, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(collection_id, episode_no) DO UPDATE SET
                        episode_title = excluded.episode_title,
                        episode_url = excluded.episode_url,
                        duration_sec = excluded.duration_sec
                    """,
                    normalizedCollectionId,
                    episode.episodeNo,
                    normalizeOptional(episode.episodeTitle),
                    normalizeOptional(episode.episodeUrl),
                    episode.durationSec,
                    now
            );
        }
    }

    @Transactional
    public boolean linkTaskToEpisode(String collectionId, int episodeNo, String taskId) {
        String normalizedCollectionId = normalizeRequired(collectionId, "collectionId");
        String normalizedTaskId = normalizeRequired(taskId, "taskId");
        if (episodeNo <= 0) {
            throw new IllegalArgumentException("episodeNo must be positive");
        }
        int updatedRows = jdbcTemplate.update(
                """
                UPDATE collection_episodes
                SET task_id = ?
                WHERE collection_id = ? AND episode_no = ?
                """,
                normalizedTaskId,
                normalizedCollectionId,
                episodeNo
        );
        if (updatedRows > 0) {
            return true;
        }
        try {
            jdbcTemplate.update(
                    """
                    INSERT INTO collection_episodes (
                        collection_id, episode_no, task_id, created_at
                    )
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(collection_id, episode_no) DO UPDATE SET
                        task_id = excluded.task_id
                    """,
                    normalizedCollectionId,
                    episodeNo,
                    normalizedTaskId,
                    Instant.now().toString()
            );
            return true;
        } catch (DataAccessException ex) {
            logger.warn("link task to episode failed: collectionId={} episodeNo={} taskId={} err={}",
                    normalizedCollectionId, episodeNo, normalizedTaskId, ex.getMessage());
            return false;
        }
    }

    public Optional<CollectionView> findCollection(String collectionId) {
        String normalizedCollectionId = normalizeOptional(collectionId);
        if (normalizedCollectionId.isEmpty()) {
            return Optional.empty();
        }
        List<CollectionView> rows = jdbcTemplate.query(
                """
                SELECT collection_id, platform, canonical_id, title, total_episodes, resolved_url, created_at, updated_at
                FROM video_collections
                WHERE collection_id = ?
                """,
                (rs, rowNum) -> new CollectionView(
                        rs.getString("collection_id"),
                        rs.getString("platform"),
                        rs.getString("canonical_id"),
                        rs.getString("title"),
                        rs.getInt("total_episodes"),
                        rs.getString("resolved_url"),
                        rs.getString("created_at"),
                        rs.getString("updated_at")
                ),
                normalizedCollectionId
        );
        if (rows.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(rows.get(0));
    }

    public List<CollectionView> listCollections() {
        return jdbcTemplate.query(
                """
                SELECT collection_id, platform, canonical_id, title, total_episodes, resolved_url, created_at, updated_at
                FROM video_collections
                ORDER BY updated_at DESC
                """,
                (rs, rowNum) -> new CollectionView(
                        rs.getString("collection_id"),
                        rs.getString("platform"),
                        rs.getString("canonical_id"),
                        rs.getString("title"),
                        rs.getInt("total_episodes"),
                        rs.getString("resolved_url"),
                        rs.getString("created_at"),
                        rs.getString("updated_at")
                )
        );
    }

    public List<EpisodeView> listEpisodes(String collectionId) {
        String normalizedCollectionId = normalizeOptional(collectionId);
        if (normalizedCollectionId.isEmpty()) {
            return List.of();
        }
        return jdbcTemplate.query(
                """
                SELECT collection_id, episode_no, episode_title, episode_url, duration_sec, task_id, created_at
                FROM collection_episodes
                WHERE collection_id = ?
                ORDER BY episode_no ASC
                """,
                (rs, rowNum) -> new EpisodeView(
                        rs.getString("collection_id"),
                        rs.getInt("episode_no"),
                        rs.getString("episode_title"),
                        rs.getString("episode_url"),
                        rs.getObject("duration_sec", Double.class),
                        rs.getString("task_id"),
                        rs.getString("created_at")
                ),
                normalizedCollectionId
        );
    }

    public Optional<String> findCollectionIdByTaskId(String taskId) {
        String normalizedTaskId = normalizeOptional(taskId);
        if (normalizedTaskId.isEmpty()) {
            return Optional.empty();
        }
        List<String> rows = jdbcTemplate.query(
                """
                SELECT collection_id
                FROM collection_episodes
                WHERE task_id = ?
                LIMIT 1
                """,
                (rs, rowNum) -> rs.getString("collection_id"),
                normalizedTaskId
        );
        if (rows.isEmpty()) {
            return Optional.empty();
        }
        return Optional.ofNullable(rows.get(0));
    }

    public Map<String, EpisodeTaskBinding> findEpisodeBindingsByTaskIds(Collection<String> taskIds) {
        if (taskIds == null || taskIds.isEmpty()) {
            return Map.of();
        }
        List<String> normalizedTaskIds = new ArrayList<>();
        for (String taskId : taskIds) {
            String normalized = normalizeOptional(taskId);
            if (normalized.isEmpty()) {
                continue;
            }
            normalizedTaskIds.add(normalized);
        }
        if (normalizedTaskIds.isEmpty()) {
            return Map.of();
        }

        String placeholders = String.join(",", Collections.nCopies(normalizedTaskIds.size(), "?"));
        String sql = """
                SELECT
                    e.task_id,
                    e.collection_id,
                    e.episode_no,
                    e.episode_title,
                    c.title AS collection_title,
                    c.total_episodes
                FROM collection_episodes e
                JOIN video_collections c ON c.collection_id = e.collection_id
                WHERE e.task_id IN (%s)
                """.formatted(placeholders);
        List<EpisodeTaskBinding> bindings = jdbcTemplate.query(
                sql,
                (rs, rowNum) -> new EpisodeTaskBinding(
                        rs.getString("task_id"),
                        rs.getString("collection_id"),
                        rs.getInt("episode_no"),
                        rs.getString("episode_title"),
                        rs.getString("collection_title"),
                        rs.getInt("total_episodes")
                ),
                normalizedTaskIds.toArray()
        );
        Map<String, EpisodeTaskBinding> bindingMap = new LinkedHashMap<>();
        for (EpisodeTaskBinding binding : bindings) {
            if (binding == null || binding.taskId == null || binding.taskId.isBlank()) {
                continue;
            }
            bindingMap.put(binding.taskId, binding);
        }
        return bindingMap;
    }

    private String normalizeRequired(String value, String fieldName) {
        String normalized = normalizeOptional(value);
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException(fieldName + " cannot be empty");
        }
        return normalized;
    }

    private String normalizeOptional(String value) {
        if (value == null) {
            return "";
        }
        return value.trim();
    }

    public static class EpisodeInput {
        public final int episodeNo;
        public final String episodeTitle;
        public final String episodeUrl;
        public final Double durationSec;

        public EpisodeInput(int episodeNo, String episodeTitle, String episodeUrl, Double durationSec) {
            this.episodeNo = episodeNo;
            this.episodeTitle = episodeTitle;
            this.episodeUrl = episodeUrl;
            this.durationSec = durationSec;
        }
    }

    public static class CollectionView {
        public final String collectionId;
        public final String platform;
        public final String canonicalId;
        public final String title;
        public final int totalEpisodes;
        public final String resolvedUrl;
        public final String createdAt;
        public final String updatedAt;

        public CollectionView(
                String collectionId,
                String platform,
                String canonicalId,
                String title,
                int totalEpisodes,
                String resolvedUrl,
                String createdAt,
                String updatedAt
        ) {
            this.collectionId = collectionId;
            this.platform = platform;
            this.canonicalId = canonicalId;
            this.title = title;
            this.totalEpisodes = totalEpisodes;
            this.resolvedUrl = resolvedUrl;
            this.createdAt = createdAt;
            this.updatedAt = updatedAt;
        }
    }

    public static class EpisodeView {
        public final String collectionId;
        public final int episodeNo;
        public final String episodeTitle;
        public final String episodeUrl;
        public final Double durationSec;
        public final String taskId;
        public final String createdAt;

        public EpisodeView(
                String collectionId,
                int episodeNo,
                String episodeTitle,
                String episodeUrl,
                Double durationSec,
                String taskId,
                String createdAt
        ) {
            this.collectionId = collectionId;
            this.episodeNo = episodeNo;
            this.episodeTitle = episodeTitle;
            this.episodeUrl = episodeUrl;
            this.durationSec = durationSec;
            this.taskId = taskId;
            this.createdAt = createdAt;
        }
    }

    public static class EpisodeTaskBinding {
        public final String taskId;
        public final String collectionId;
        public final int episodeNo;
        public final String episodeTitle;
        public final String collectionTitle;
        public final int totalEpisodes;

        public EpisodeTaskBinding(
                String taskId,
                String collectionId,
                int episodeNo,
                String episodeTitle,
                String collectionTitle,
                int totalEpisodes
        ) {
            this.taskId = taskId;
            this.collectionId = collectionId;
            this.episodeNo = episodeNo;
            this.episodeTitle = episodeTitle;
            this.collectionTitle = collectionTitle;
            this.totalEpisodes = totalEpisodes;
        }
    }
}
