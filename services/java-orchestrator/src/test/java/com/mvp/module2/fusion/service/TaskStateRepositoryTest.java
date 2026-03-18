package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mvp.module2.fusion.queue.TaskQueueManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.jdbc.core.JdbcTemplate;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteDataSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskStateRepositoryTest {

    @TempDir
    Path tempDir;

    @Test
    void upsertTaskShouldPersistRecoveryPayloadAndRemainIdempotent() throws Exception {
        TaskStateRepository repository = createRepository(tempDir.resolve("task-runtime.db"));

        TaskQueueManager.TaskEntry task = new TaskQueueManager.TaskEntry();
        task.taskId = "VT_30001";
        task.userId = "user-1";
        task.videoUrl = "https://example.com/video/1";
        task.normalizedVideoKey = "example-video-1";
        task.title = "首次写入";
        task.outputDir = "var/output/VT_30001";
        task.priority = TaskQueueManager.Priority.HIGH;
        task.status = TaskQueueManager.TaskStatus.MANUAL_RETRY_REQUIRED;
        task.progress = 66.0;
        task.statusMessage = "等待人工处理";
        task.probePayload = Map.of("durationSec", 120.5, "platform", "example");
        task.recoveryPayload = Map.of(
                "stage", "phase2b",
                "checkpoint", "llm_call_003",
                "retryMode", "MANUAL_RETRY_REQUIRED"
        );
        task.createdAt = Instant.parse("2026-03-15T04:45:00Z");
        task.startedAt = Instant.parse("2026-03-15T04:46:00Z");

        repository.upsertTask(task);

        task.status = TaskQueueManager.TaskStatus.QUEUED;
        task.statusMessage = "人工确认后重新排队";
        task.recoveryPayload = Map.of(
                "stage", "phase2b",
                "checkpoint", "llm_call_003",
                "retryMode", "AUTO_RETRY_WAIT",
                "requiredAction", "none"
        );
        repository.upsertTask(task);

        TaskStateRepository.PersistedTaskRecord persisted = repository.findTask("VT_30001").orElseThrow();
        assertEquals("VT_30001", persisted.taskId);
        assertEquals("user-1", persisted.userId);
        assertEquals("QUEUED", persisted.status);
        assertEquals("人工确认后重新排队", persisted.statusMessage);
        assertEquals("phase2b", persisted.recoveryPayload.get("stage"));
        assertEquals("AUTO_RETRY_WAIT", persisted.recoveryPayload.get("retryMode"));
        assertEquals("llm_call_003", persisted.recoveryPayload.get("checkpoint"));
        assertEquals("example", persisted.probePayload.get("platform"));
        assertNotNull(persisted.updatedAt);

        Integer rowCount = repositoryCount(tempDir.resolve("task-runtime.db"));
        assertEquals(1, rowCount);
    }

    private TaskStateRepository createRepository(Path dbPath) throws Exception {
        Files.createDirectories(dbPath.getParent());
        SQLiteConfig config = new SQLiteConfig();
        config.setBusyTimeout(5000);
        config.setJournalMode(SQLiteConfig.JournalMode.WAL);
        SQLiteDataSource dataSource = new SQLiteDataSource(config);
        dataSource.setUrl("jdbc:sqlite:" + dbPath.toAbsolutePath().normalize());

        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        jdbcTemplate.execute("""
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
        return new TaskStateRepository(jdbcTemplate, new ObjectMapper());
    }

    private Integer repositoryCount(Path dbPath) {
        SQLiteDataSource dataSource = new SQLiteDataSource();
        dataSource.setUrl("jdbc:sqlite:" + dbPath.toAbsolutePath().normalize());
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        Integer rowCount = jdbcTemplate.queryForObject("SELECT COUNT(1) FROM task_runtime_state", Integer.class);
        assertTrue(rowCount != null && rowCount >= 0);
        return rowCount;
    }
}
