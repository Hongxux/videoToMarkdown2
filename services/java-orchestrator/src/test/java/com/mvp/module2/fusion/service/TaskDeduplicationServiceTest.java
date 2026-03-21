package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mvp.module2.fusion.queue.TaskQueueManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.jdbc.core.JdbcTemplate;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteDataSource;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskDeduplicationServiceTest {

    @TempDir
    Path tempDir;

    @Test
    void findReusablePersistedTaskShouldIgnoreCompletedRecordWithoutArtifacts() throws Exception {
        TaskStateRepository repository = createRepository(tempDir.resolve("task-runtime.db"));
        TaskDeduplicationService service = createService(repository);

        TaskQueueManager.TaskEntry staleCompleted = createTask(
                "VT_40001",
                "https://www.bilibili.com/video/BV1k8411r7E4?p=4",
                "bilibili.com/video/BV1k8411r7E4?p=4",
                tempDir.resolve("missing-output").toString(),
                TaskQueueManager.TaskStatus.COMPLETED,
                Instant.parse("2026-03-20T09:00:00Z")
        );
        staleCompleted.resultPath = tempDir.resolve("missing-output/result.md").toString();
        repository.upsertTask(staleCompleted);

        TaskQueueManager.TaskEntry queuedTask = createTask(
                "VT_40002",
                "https://www.bilibili.com/video/BV1k8411r7E4?p=4",
                "bilibili.com/video/BV1k8411r7E4?p=4",
                tempDir.resolve("fresh-output").toString(),
                TaskQueueManager.TaskStatus.QUEUED,
                Instant.parse("2026-03-20T08:59:00Z")
        );
        repository.upsertTask(queuedTask);

        Optional<TaskStateRepository.PersistedTaskRecord> reusable =
                service.findReusablePersistedTask("bilibili.com/video/BV1k8411r7E4?p=4", "VT_49999");

        assertTrue(reusable.isPresent());
        assertEquals("VT_40002", reusable.get().taskId);
    }

    @Test
    void findReusablePersistedTaskShouldKeepCompletedRecordWhenMarkdownStillExists() throws Exception {
        TaskStateRepository repository = createRepository(tempDir.resolve("task-runtime.db"));
        TaskDeduplicationService service = createService(repository);

        Path outputDir = tempDir.resolve("completed-output");
        Files.createDirectories(outputDir);
        Path markdownPath = outputDir.resolve("enhanced_output.md");
        Files.writeString(markdownPath, "# ok\n", StandardCharsets.UTF_8);

        TaskQueueManager.TaskEntry completedTask = createTask(
                "VT_50001",
                "https://www.bilibili.com/video/BV1k8411r7E4?p=4",
                "bilibili.com/video/BV1k8411r7E4?p=4",
                outputDir.toString(),
                TaskQueueManager.TaskStatus.COMPLETED,
                Instant.parse("2026-03-20T09:10:00Z")
        );
        completedTask.resultPath = markdownPath.toString();
        repository.upsertTask(completedTask);

        Optional<TaskStateRepository.PersistedTaskRecord> reusable =
                service.findReusablePersistedTask("bilibili.com/video/BV1k8411r7E4?p=4", "VT_59999");

        assertTrue(reusable.isPresent());
        assertEquals("VT_50001", reusable.get().taskId);
    }

    private static TaskQueueManager.TaskEntry createTask(
            String taskId,
            String videoUrl,
            String normalizedVideoKey,
            String outputDir,
            TaskQueueManager.TaskStatus status,
            Instant createdAt
    ) {
        TaskQueueManager.TaskEntry task = new TaskQueueManager.TaskEntry();
        task.taskId = taskId;
        task.userId = "user-1";
        task.videoUrl = videoUrl;
        task.normalizedVideoKey = normalizedVideoKey;
        task.outputDir = outputDir;
        task.title = taskId;
        task.priority = TaskQueueManager.Priority.NORMAL;
        task.status = status;
        task.progress = TaskQueueManager.TaskStatus.COMPLETED == status ? 1.0d : 0.0d;
        task.statusMessage = status.name();
        task.createdAt = createdAt;
        task.updatedAt = createdAt;
        if (status == TaskQueueManager.TaskStatus.COMPLETED) {
            task.completedAt = createdAt.plusSeconds(60);
        }
        return task;
    }

    private static TaskDeduplicationService createService(TaskStateRepository repository) throws Exception {
        TaskDeduplicationService service = new TaskDeduplicationService();
        Field field = TaskDeduplicationService.class.getDeclaredField("taskStateRepository");
        field.setAccessible(true);
        field.set(service, repository);
        return service;
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
        return new TaskStateRepository(jdbcTemplate, new ObjectMapper());
    }
}
