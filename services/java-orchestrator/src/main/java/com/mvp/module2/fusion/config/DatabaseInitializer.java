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
                statement.execute("""
                        CREATE TABLE IF NOT EXISTS task_terminal_events (
                            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                            user_id TEXT NOT NULL,
                            task_id TEXT NOT NULL,
                            status TEXT NOT NULL,
                            payload_json TEXT NOT NULL,
                            created_at TEXT NOT NULL
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
                statement.execute("""
                        CREATE TABLE IF NOT EXISTS task_runtime_state (
                            task_id TEXT PRIMARY KEY,
                            user_id TEXT NOT NULL,
                            video_url TEXT NOT NULL,
                            normalized_video_key TEXT,
                            title TEXT,
                            output_dir TEXT,
                            priority TEXT NOT NULL,
                            status TEXT NOT NULL,
                            progress REAL NOT NULL DEFAULT 0,
                            status_message TEXT,
                            user_message TEXT,
                            result_path TEXT,
                            cleanup_source_path TEXT,
                            error_message TEXT,
                            duplicate_of_task_id TEXT,
                            book_options_json TEXT,
                            probe_payload_json TEXT,
                            recovery_payload_json TEXT,
                            created_at TEXT NOT NULL,
                            started_at TEXT,
                            completed_at TEXT,
                            updated_at TEXT NOT NULL
                        )
                        """);
                statement.execute("""
                        CREATE TABLE IF NOT EXISTS task_cleanup_queue (
                            task_id TEXT PRIMARY KEY,
                            storage_key TEXT NOT NULL,
                            task_root TEXT NOT NULL,
                            task_type TEXT NOT NULL,
                            task_status TEXT NOT NULL,
                            policy_version TEXT NOT NULL,
                            ttl_millis INTEGER NOT NULL,
                            completed_at_ms INTEGER NOT NULL,
                            cleanup_after_ms INTEGER NOT NULL,
                            updated_at_ms INTEGER NOT NULL,
                            last_error TEXT
                        )
                        """);
                try {
                    statement.execute("ALTER TABLE task_runtime_state ADD COLUMN user_message TEXT");
                } catch (Exception ignored) {
                    // 骞傜瓑杩佺Щ锛氭棫搴撳凡瀛樺湪 user_message 鏃剁洿鎺ヨ烦杩囥€?
                }
                try {
                    statement.execute("ALTER TABLE task_runtime_state ADD COLUMN normalized_video_key TEXT");
                } catch (Exception ignored) {
                    // 幂等迁移：旧库已存在该列时直接跳过。
                }
                try {
                    statement.execute("ALTER TABLE task_runtime_state ADD COLUMN duplicate_of_task_id TEXT");
                } catch (Exception ignored) {
                    // 幂等迁移：旧库已存在该列时直接跳过。
                }
                try {
                    statement.execute("ALTER TABLE task_runtime_state ADD COLUMN recovery_payload_json TEXT");
                } catch (Exception ignored) {
                    // 幂等迁移：旧库已有 recovery_payload_json 时直接跳过。
                }
                statement.execute("CREATE INDEX IF NOT EXISTS idx_task_runtime_state_status ON task_runtime_state(status)");
                statement.execute("CREATE INDEX IF NOT EXISTS idx_task_runtime_state_user_id ON task_runtime_state(user_id)");
                statement.execute("CREATE INDEX IF NOT EXISTS idx_task_runtime_state_video_key ON task_runtime_state(normalized_video_key)");
                statement.execute("CREATE INDEX IF NOT EXISTS idx_task_cleanup_queue_due ON task_cleanup_queue(cleanup_after_ms)");
                statement.execute("CREATE INDEX IF NOT EXISTS idx_task_cleanup_queue_storage_key ON task_cleanup_queue(storage_key)");
                statement.execute("CREATE INDEX IF NOT EXISTS idx_task_terminal_events_user_event ON task_terminal_events(user_id, event_id)");
                logger.info("collection schema initialized");
            }
        };
    }
}
