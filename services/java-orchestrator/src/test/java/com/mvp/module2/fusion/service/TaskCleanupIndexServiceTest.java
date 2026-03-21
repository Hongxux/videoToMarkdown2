package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mvp.module2.fusion.queue.TaskQueueManager;
import com.mvp.module2.fusion.service.TaskCleanupQueueRepository.PendingCleanupTaskRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.jdbc.core.JdbcTemplate;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteDataSource;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskCleanupIndexServiceTest {

    @TempDir
    Path tempDir;

    @Test
    void completedTaskShouldEnterCleanupQueueAndRetryShouldRemoveIt() throws Exception {
        TestContext context = createContext();
        Path taskRoot = createLatestVideoTaskRoot(context.storageRoot, "cleanup-hash-001");

        TaskQueueManager.TaskEntry task = buildCompletedTask(taskRoot, "VT_cleanup_001");
        task.completedAt = Instant.parse("2026-03-15T00:00:00Z");

        context.service.persistTaskState(task);

        PendingCleanupTaskRecord record = context.cleanupRepository.findByTaskId(task.taskId).orElseThrow();
        assertEquals("cleanup-hash-001", record.storageKey());
        assertEquals(task.completedAt.toEpochMilli() + 24L * 3_600_000L, record.cleanupAfterMs());
        assertEquals("COMPLETED", record.taskStatus());

        task.status = TaskQueueManager.TaskStatus.QUEUED;
        task.completedAt = null;
        context.service.persistTaskState(task);

        assertTrue(context.cleanupRepository.findByTaskId(task.taskId).isEmpty());
    }

    @Test
    void reconcileCleanupPolicyShouldRefreshDueTimeWhenTtlChanges() throws Exception {
        TestContext context = createContext();
        Path taskRoot = createLatestVideoTaskRoot(context.storageRoot, "cleanup-hash-002");

        TaskQueueManager.TaskEntry task = buildCompletedTask(taskRoot, "VT_cleanup_002");
        task.completedAt = Instant.parse("2026-03-15T00:00:00Z");
        context.service.persistTaskState(task);

        injectField(context.service, "completedTtlHours", 48L);
        int updatedRows = context.service.reconcileCleanupPolicy();

        PendingCleanupTaskRecord updated = context.cleanupRepository.findByTaskId(task.taskId).orElseThrow();
        assertEquals(1, updatedRows);
        assertEquals(48L * 3_600_000L, updated.ttlMillis());
        assertEquals(task.completedAt.toEpochMilli() + 48L * 3_600_000L, updated.cleanupAfterMs());
    }

    @Test
    void cleanupPassShouldDeleteIntermediateArtifactsButKeepFinalOutputsAndAudit() throws Exception {
        TestContext context = createContext();
        injectField(context.service, "completedTtlHours", 1L);
        Path taskRoot = createLatestVideoTaskRoot(context.storageRoot, "cleanup-hash-003");
        Files.writeString(taskRoot.resolve("video.mp4"), "video");
        Files.writeString(taskRoot.resolve("video.mp4.meta.json"), "{\"created_at\":\"2026-03-14T00:00:00Z\"}");
        Files.createDirectories(taskRoot.resolve("local_storage"));
        Files.writeString(taskRoot.resolve("local_storage").resolve("sentence_timestamps.json"), "{}");
        Files.createDirectories(taskRoot.resolve("intermediates").resolve("rt"));
        Files.writeString(taskRoot.resolve("intermediates").resolve("rt").resolve("resume_index.json"), "{}");
        Files.createDirectories(taskRoot.resolve("intermediates").resolve("stages").resolve("stage1").resolve("outputs"));
        Files.writeString(
                taskRoot.resolve("intermediates").resolve("stages").resolve("stage1").resolve("outputs").resolve("step2_correction.json"),
                "{}"
        );
        Files.createDirectories(taskRoot.resolve("intermediates").resolve("stages").resolve("phase2a").resolve("outputs"));
        Files.writeString(
                taskRoot.resolve("intermediates").resolve("stages").resolve("phase2a").resolve("outputs").resolve("semantic_units.json"),
                "{}"
        );
        Files.createDirectories(taskRoot.resolve("intermediates").resolve("stages").resolve("phase2a").resolve("audits"));
        Files.writeString(
                taskRoot.resolve("intermediates").resolve("stages").resolve("phase2a").resolve("audits").resolve("token_cost_audit.json"),
                "{\"audit\":true}"
        );
        Files.writeString(
                taskRoot.resolve("intermediates").resolve("stages").resolve("phase2a").resolve("audits").resolve("vl_analysis_output_latest.json"),
                "{}"
        );
        Files.writeString(taskRoot.resolve("intermediates").resolve("task_metrics_VT_cleanup_003.json"), "{}");

        TaskQueueManager.TaskEntry task = buildCompletedTask(taskRoot, "VT_cleanup_003");
        task.completedAt = Instant.parse("2026-03-15T00:00:00Z");
        context.service.persistTaskState(task);

        TaskCleanupIndexService.CleanupRunSummary summary = context.service.runDueCleanupPass(
                Instant.parse("2026-03-15T03:00:00Z"),
                true
        );

        assertEquals(1, summary.scanned);
        assertEquals(1, summary.cleanedTasks);
        assertTrue(context.cleanupRepository.findByTaskId(task.taskId).isEmpty());

        assertTrue(Files.exists(taskRoot.resolve("enhanced_output.md")));
        assertTrue(Files.exists(taskRoot.resolve("result.json")));
        assertTrue(Files.exists(taskRoot.resolve("video_meta.json")));
        assertTrue(Files.exists(taskRoot.resolve("video.mp4")));
        assertTrue(Files.exists(taskRoot.resolve("intermediates").resolve("task_metrics_latest.json")));
        assertTrue(Files.exists(taskRoot.resolve("intermediates").resolve("stages").resolve("phase2a").resolve("audits").resolve("token_cost_audit.json")));

        assertFalse(Files.exists(taskRoot.resolve("video.mp4.meta.json")));
        assertFalse(Files.exists(taskRoot.resolve("local_storage")));
        assertFalse(Files.exists(taskRoot.resolve("intermediates").resolve("rt")));
        assertFalse(Files.exists(taskRoot.resolve("intermediates").resolve("stages").resolve("stage1")));
        assertFalse(Files.exists(taskRoot.resolve("intermediates").resolve("stages").resolve("phase2a").resolve("outputs")));
        assertFalse(Files.exists(taskRoot.resolve("intermediates").resolve("stages").resolve("phase2a").resolve("audits").resolve("vl_analysis_output_latest.json")));
        assertFalse(Files.exists(taskRoot.resolve("intermediates").resolve("task_metrics_VT_cleanup_003.json")));
    }

    @Test
    void scheduleImmediateCleanupShouldCreateDueQueueRecord() throws Exception {
        TestContext context = createContext();
        Path taskRoot = createLatestVideoTaskRoot(context.storageRoot, "cleanup-hash-004");

        boolean scheduled = context.service.scheduleImmediateCleanupForTask(
                "VT_cleanup_004",
                taskRoot.toString(),
                "VIDEO"
        );

        assertTrue(scheduled);
        PendingCleanupTaskRecord record = context.cleanupRepository.findByTaskId("VT_cleanup_004").orElseThrow();
        assertEquals("cleanup-hash-004", record.storageKey());
        assertEquals("DELETE_PENDING_CLEANUP", record.taskStatus());
        assertEquals(0L, record.ttlMillis());
        assertEquals(record.completedAtMs(), record.cleanupAfterMs());
    }

    private TestContext createContext() throws Exception {
        Path dbPath = tempDir.resolve("cleanup-index.db");
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
        jdbcTemplate.execute("""
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

        TaskStateRepository taskStateRepository = new TaskStateRepository(jdbcTemplate, new ObjectMapper());
        TaskCleanupQueueRepository cleanupQueueRepository = new TaskCleanupQueueRepository(jdbcTemplate);
        TaskCleanupIndexService service = new TaskCleanupIndexService(taskStateRepository, cleanupQueueRepository);
        Path storageRoot = tempDir.resolve("var").resolve("storage").resolve("storage");
        Files.createDirectories(storageRoot);
        injectField(service, "cleanupEnabled", true);
        injectField(service, "completedTtlHours", 24L);
        injectField(service, "cleanupWindowStartHour", 0);
        injectField(service, "cleanupWindowEndHour", 5);
        injectField(service, "cleanupBatchSize", 16);
        injectField(service, "configuredCleanupZoneId", "Asia/Shanghai");
        injectField(service, "configuredStorageRoot", storageRoot.toString());
        return new TestContext(taskStateRepository, cleanupQueueRepository, service, storageRoot);
    }

    private Path createLatestVideoTaskRoot(Path storageRoot, String storageKey) throws Exception {
        Path taskRoot = storageRoot.resolve(storageKey);
        Files.createDirectories(taskRoot.resolve("intermediates"));
        Files.writeString(taskRoot.resolve("enhanced_output.md"), "# demo");
        Files.writeString(taskRoot.resolve("result.json"), "{\"ok\":true}");
        Files.writeString(taskRoot.resolve("video_meta.json"), "{\"title\":\"demo\"}");
        Files.writeString(taskRoot.resolve("intermediates").resolve("task_metrics_latest.json"), "{\"generated_at\":\"2026-03-15T00:00:00Z\"}");
        return taskRoot;
    }

    private TaskQueueManager.TaskEntry buildCompletedTask(Path taskRoot, String taskId) {
        TaskQueueManager.TaskEntry task = new TaskQueueManager.TaskEntry();
        task.taskId = taskId;
        task.userId = "cleanup-user";
        task.videoUrl = "https://example.com/video/" + taskId;
        task.outputDir = taskRoot.toAbsolutePath().normalize().toString();
        task.resultPath = taskRoot.resolve("enhanced_output.md").toAbsolutePath().normalize().toString();
        task.priority = TaskQueueManager.Priority.NORMAL;
        task.status = TaskQueueManager.TaskStatus.COMPLETED;
        task.createdAt = Instant.parse("2026-03-14T23:00:00Z");
        task.progress = 1.0d;
        task.statusMessage = "done";
        return task;
    }

    private static void injectField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private record TestContext(
            TaskStateRepository taskStateRepository,
            TaskCleanupQueueRepository cleanupRepository,
            TaskCleanupIndexService service,
            Path storageRoot
    ) {
        private TestContext {
            assertNotNull(taskStateRepository);
            assertNotNull(cleanupRepository);
            assertNotNull(service);
            assertNotNull(storageRoot);
        }
    }
}
