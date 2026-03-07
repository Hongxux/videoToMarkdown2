package com.mvp.module2.fusion.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteDataSource;

import javax.sql.DataSource;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;

@Configuration
public class DatabaseInitializer {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseInitializer.class);

    @Bean
    public DataSource dataSource(@Value("${collection.db.path:var/state/collections.db}") String dbPathRaw) {
        Path dbPath = Path.of(dbPathRaw).toAbsolutePath().normalize();
        try {
            Path parentDir = dbPath.getParent();
            if (parentDir != null) {
                Files.createDirectories(parentDir);
            }
        } catch (Exception ex) {
            throw new IllegalStateException("failed to prepare collection database directory: " + dbPath, ex);
        }

        SQLiteConfig config = new SQLiteConfig();
        config.setBusyTimeout(5000);
        config.setJournalMode(SQLiteConfig.JournalMode.WAL);
        SQLiteDataSource dataSource = new SQLiteDataSource(config);
        dataSource.setUrl("jdbc:sqlite:" + dbPath);
        return dataSource;
    }

    @Bean
    public ApplicationRunner collectionSchemaInitializer(DataSource dataSource) {
        return args -> {
            try (Connection connection = dataSource.getConnection();
                 Statement statement = connection.createStatement()) {
                statement.execute("PRAGMA journal_mode=WAL");
                statement.execute("PRAGMA busy_timeout=5000");
                statement.execute("PRAGMA foreign_keys=ON");
                statement.execute("""
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
                statement.execute("""
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
                statement.execute("CREATE INDEX IF NOT EXISTS idx_episode_task ON collection_episodes(task_id)");
                statement.execute("""
                        CREATE TABLE IF NOT EXISTS task_manual_collection_bindings (
                            task_path TEXT PRIMARY KEY,
                            collection_path TEXT NOT NULL,
                            updated_at TEXT NOT NULL
                        )
                        """);
                statement.execute("CREATE INDEX IF NOT EXISTS idx_task_manual_collection_path ON task_manual_collection_bindings(collection_path)");
                statement.execute("""
                        CREATE TABLE IF NOT EXISTS file_metadata (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            file_md5 TEXT NOT NULL,
                            file_ext TEXT NOT NULL,
                            file_path TEXT NOT NULL,
                            file_size INTEGER,
                            original_file_name TEXT,
                            created_at TEXT NOT NULL,
                            updated_at TEXT NOT NULL,
                            UNIQUE(file_md5, file_ext)
                        )
                        """);
                statement.execute("CREATE INDEX IF NOT EXISTS idx_file_metadata_path ON file_metadata(file_path)");
                statement.execute("""
                        CREATE TABLE IF NOT EXISTS file_probe_cache (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            file_md5 TEXT NOT NULL,
                            file_ext TEXT NOT NULL,
                            probe_payload TEXT NOT NULL,
                            created_at TEXT NOT NULL,
                            updated_at TEXT NOT NULL,
                            UNIQUE(file_md5, file_ext)
                        )
                        """);
                logger.info("collection schema initialized");
            }
        };
    }
}
