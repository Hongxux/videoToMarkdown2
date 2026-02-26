package com.mvp.module2.fusion.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.jdbc.core.JdbcTemplate;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteDataSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CollectionRepositoryTest {

    @TempDir
    Path tempDir;

    @Test
    void upsertShouldBeIdempotentAndUpdateFields() throws Exception {
        CollectionRepository repository = createRepository(tempDir.resolve("collections.db"));
        repository.upsertCollection(
                "bilibili:BV_test_1",
                "bilibili",
                "BV_test_1",
                "old-title",
                1,
                "https://www.bilibili.com/video/BV_test_1",
                List.of(new CollectionRepository.EpisodeInput(1, "ep1", "https://example.com/p1", 100.0))
        );
        repository.upsertCollection(
                "bilibili:BV_test_1",
                "bilibili",
                "BV_test_1",
                "new-title",
                3,
                "https://www.bilibili.com/video/BV_test_1",
                List.of(
                        new CollectionRepository.EpisodeInput(1, "ep1-updated", "https://example.com/p1", 120.0),
                        new CollectionRepository.EpisodeInput(2, "ep2", "https://example.com/p2", 121.0),
                        new CollectionRepository.EpisodeInput(3, "ep3", "https://example.com/p3", 122.0)
                )
        );

        List<CollectionRepository.CollectionView> collections = repository.listCollections();
        assertEquals(1, collections.size());
        assertEquals("new-title", collections.get(0).title);
        assertEquals(3, collections.get(0).totalEpisodes);
    }

    @Test
    void linkTaskToEpisodeShouldPersistTaskId() throws Exception {
        CollectionRepository repository = createRepository(tempDir.resolve("collections.db"));
        repository.upsertCollection(
                "bilibili:BV_link",
                "bilibili",
                "BV_link",
                "link-case",
                1,
                "https://www.bilibili.com/video/BV_link",
                List.of(new CollectionRepository.EpisodeInput(1, "ep1", "https://example.com/p1", 88.0))
        );

        boolean linked = repository.linkTaskToEpisode("bilibili:BV_link", 1, "VT_10001");
        assertTrue(linked);
        List<CollectionRepository.EpisodeView> episodes = repository.listEpisodes("bilibili:BV_link");
        assertEquals(1, episodes.size());
        assertEquals("VT_10001", episodes.get(0).taskId);
    }

    @Test
    void findCollectionIdByTaskIdShouldResolveReverseLookup() throws Exception {
        CollectionRepository repository = createRepository(tempDir.resolve("collections.db"));
        repository.upsertCollection(
                "bilibili:BV_reverse",
                "bilibili",
                "BV_reverse",
                "reverse-case",
                2,
                "https://www.bilibili.com/video/BV_reverse",
                List.of(
                        new CollectionRepository.EpisodeInput(1, "ep1", "https://example.com/p1", 50.0),
                        new CollectionRepository.EpisodeInput(2, "ep2", "https://example.com/p2", 60.0)
                )
        );
        repository.linkTaskToEpisode("bilibili:BV_reverse", 2, "VT_20002");

        Optional<String> collectionId = repository.findCollectionIdByTaskId("VT_20002");
        assertTrue(collectionId.isPresent());
        assertEquals("bilibili:BV_reverse", collectionId.get());
    }

    @Test
    void listEpisodesShouldAlwaysOrderByEpisodeNoAscending() throws Exception {
        CollectionRepository repository = createRepository(tempDir.resolve("collections.db"));
        repository.upsertCollection(
                "bilibili:BV_order",
                "bilibili",
                "BV_order",
                "order-case",
                3,
                "https://www.bilibili.com/video/BV_order",
                List.of(
                        new CollectionRepository.EpisodeInput(3, "ep3", "https://example.com/p3", 33.0),
                        new CollectionRepository.EpisodeInput(1, "ep1", "https://example.com/p1", 11.0),
                        new CollectionRepository.EpisodeInput(2, "ep2", "https://example.com/p2", 22.0)
                )
        );

        List<CollectionRepository.EpisodeView> episodes = repository.listEpisodes("bilibili:BV_order");
        assertEquals(3, episodes.size());
        assertEquals(1, episodes.get(0).episodeNo);
        assertEquals(2, episodes.get(1).episodeNo);
        assertEquals(3, episodes.get(2).episodeNo);
    }

    private CollectionRepository createRepository(Path dbPath) throws Exception {
        Files.createDirectories(dbPath.getParent());
        SQLiteConfig config = new SQLiteConfig();
        config.setBusyTimeout(5000);
        config.setJournalMode(SQLiteConfig.JournalMode.WAL);
        SQLiteDataSource dataSource = new SQLiteDataSource(config);
        dataSource.setUrl("jdbc:sqlite:" + dbPath.toAbsolutePath().normalize());

        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS video_collections (
                    collection_id TEXT PRIMARY KEY,
                    platform TEXT NOT NULL,
                    canonical_id TEXT NOT NULL,
                    title TEXT,
                    total_episodes INTEGER NOT NULL DEFAULT 0,
                    resolved_url TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """);
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS collection_episodes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    collection_id TEXT NOT NULL,
                    episode_no INTEGER NOT NULL,
                    episode_title TEXT,
                    episode_url TEXT,
                    duration_sec REAL,
                    task_id TEXT,
                    created_at TEXT NOT NULL,
                    UNIQUE(collection_id, episode_no),
                    FOREIGN KEY(collection_id) REFERENCES video_collections(collection_id) ON DELETE CASCADE
                )
                """);
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_episode_task ON collection_episodes(task_id)");
        return new CollectionRepository(jdbcTemplate);
    }
}
