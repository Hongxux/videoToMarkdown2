package com.mvp.module2.fusion.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mvp.module2.fusion.queue.TaskQueueManager;
import com.mvp.module2.fusion.queue.TaskQueueManager.TaskEntry;
import com.mvp.module2.fusion.service.TaskDeduplicationService;
import com.mvp.module2.fusion.service.TaskProbeService;
import com.mvp.module2.fusion.service.TaskRuntimeRecoveryService;
import com.mvp.module2.fusion.service.TaskRuntimeStageStore;
import com.mvp.module2.fusion.service.TaskStateRepository;
import com.mvp.module2.fusion.service.VideoProcessingOrchestrator;
import com.mvp.module2.fusion.websocket.TaskWebSocketHandler;
import com.mvp.module2.fusion.worker.watchdog.TaskWatchdog;
import com.mvp.module2.fusion.worker.watchdog.TaskWatchdogFactory;
import com.mvp.module2.fusion.worker.watchdog.WatchdogSignalCodec;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Semaphore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TaskProcessingWorkerRecoveryStatusTest {

    @Test
    void processTaskShouldBroadcastBlockedRecoveryStatusInsteadOfFailed() throws Exception {
        Path taskRoot = Files.createTempDirectory("worker-recovery-status");
        writeStageState(
                taskRoot,
                "phase2b",
                "MANUAL_RETRY_REQUIRED",
                "llm_call_commit_pending",
                "MANUAL_RETRY",
                "repair llm quota and retry",
                "phase2b/chunk-42",
                1773500003210L
        );

        TaskProcessingWorker worker = new TaskProcessingWorker();
        TaskQueueManager queueManager = newQueueManager();
        ObjectMapper objectMapper = new ObjectMapper();
        injectField(
                queueManager,
                "taskRuntimeRecoveryService",
                new TaskRuntimeRecoveryService(objectMapper, new TaskRuntimeStageStore(objectMapper))
        );
        VideoProcessingOrchestrator orchestrator = mock(VideoProcessingOrchestrator.class);
        TaskProbeService taskProbeService = mock(TaskProbeService.class);
        TaskWebSocketHandler webSocketHandler = mock(TaskWebSocketHandler.class);
        TaskWatchdogFactory taskWatchdogFactory = mock(TaskWatchdogFactory.class);
        WatchdogSignalCodec watchdogSignalCodec = mock(WatchdogSignalCodec.class);

        setField(worker, "taskQueueManager", queueManager);
        setField(worker, "orchestrator", orchestrator);
        setField(worker, "taskProbeService", taskProbeService);
        setField(worker, "taskDeduplicationService", new TaskDeduplicationService());
        setField(worker, "taskWatchdogFactory", taskWatchdogFactory);
        setField(worker, "watchdogSignalCodec", watchdogSignalCodec);
        setField(worker, "webSocketHandler", webSocketHandler);
        setField(worker, "downloadSemaphore", new Semaphore(1));
        setField(worker, "transcribeSemaphore", new Semaphore(1));
        setField(worker, "phase2Semaphore", new Semaphore(1));

        TaskEntry task = queueManager.submitTask(
                "user-recovery-status",
                "https://example.com/video-recovery-status",
                taskRoot.toString(),
                TaskQueueManager.Priority.NORMAL
        );
        task.status = TaskQueueManager.TaskStatus.PROBING;
        task.processingSlotAcquired = true;
        task.startedAt = Instant.now();

        when(taskWatchdogFactory.create(task.taskId)).thenReturn(TaskWatchdog.disabled(task.taskId));
        when(orchestrator.shouldRunBookPipeline(anyString(), isNull())).thenReturn(false);
        when(taskProbeService.probeTask(any(TaskEntry.class)))
                .thenReturn(TaskProbeService.ProbeOutcome.success("Recovered title", "probe completed", Map.of()));
        when(orchestrator.processVideoDownloadPhase(anyString(), anyString(), anyString()))
                .thenThrow(new RuntimeException("provider quota exhausted"));

        invokeProcessTask(worker, task);

        TaskEntry updatedTask = queueManager.getTask(task.taskId);
        assertNotNull(updatedTask);
        assertEquals(TaskQueueManager.TaskStatus.MANUAL_RETRY_REQUIRED, updatedTask.status);
        verify(webSocketHandler, org.mockito.Mockito.atLeast(1)).broadcastTaskUpdate(argThat((TaskEntry broadcasted) ->
                broadcasted != null
                        && task.taskId.equals(broadcasted.taskId)
                        && broadcasted.status == TaskQueueManager.TaskStatus.MANUAL_RETRY_REQUIRED
                        && broadcasted.statusMessage != null
                        && broadcasted.statusMessage.contains("phase2b")
        ));
        verify(webSocketHandler, never()).broadcastTaskUpdate(eq(task.taskId), eq("FAILED"), anyDouble(), anyString(), isNull());
    }


    @Test
    void buildRecoveredIoPhaseResultShouldReadPhase2aSemanticUnitsAliasFromResumePayload() throws Exception {
        TaskProcessingWorker worker = new TaskProcessingWorker();
        TaskEntry task = new TaskEntry();
        task.taskId = "task-resume-payload";
        task.videoUrl = "https://example.com/video";
        task.outputDir = Files.createTempDirectory("worker-recovery-alias").toString();

        String semanticUnitsPath = Path.of(task.outputDir)
                .resolve("semantic_units_phase2a.json")
                .toAbsolutePath()
                .normalize()
                .toString();
        Map<String, Object> assetExtractPayload = new java.util.LinkedHashMap<>();
        assetExtractPayload.put("output_dir", task.outputDir);
        assetExtractPayload.put("video_path", Path.of(task.outputDir).resolve("video.mp4").toString());
        assetExtractPayload.put("phase2a_semantic_units_path", semanticUnitsPath);

        TaskRuntimeRecoveryService.StageSnapshot assetExtractSnapshot =
                new TaskRuntimeRecoveryService.StageSnapshot(
                        "asset_extract_java",
                        "java",
                        "EXECUTING",
                        "asset_extract_prepare",
                        1773650189541L,
                        task.outputDir,
                        Path.of(task.outputDir)
                                .resolve("intermediates")
                                .resolve("rt")
                                .resolve("stage")
                                .resolve("asset_extract_java")
                                .resolve("stage_state.json")
                                .toString(),
                        Map.copyOf(assetExtractPayload)
                );
        TaskRuntimeRecoveryService.ResumeDecision resumeDecision =
                new TaskRuntimeRecoveryService.ResumeDecision(
                        Path.of(task.outputDir),
                        "asset_extract_java",
                        "java",
                        assetExtractSnapshot,
                        assetExtractSnapshot,
                        assetExtractSnapshot,
                        Map.of("asset_extract_java", assetExtractSnapshot),
                        "test_alias_payload"
                );

        VideoProcessingOrchestrator.IOPhaseResult ioResult =
                invokeBuildRecoveredIoPhaseResult(worker, task, task.outputDir, resumeDecision);

        assertEquals(semanticUnitsPath, ioResult.phase2aSemanticUnitsPath);
        assertNotNull(ioResult.stage1Result);
        assertEquals(true, ioResult.stage1Result.success);
        assertEquals("", ioResult.stage1Result.step2JsonPath);
        assertEquals("", ioResult.stage1Result.step6JsonPath);
        assertEquals("", ioResult.stage1Result.sentenceTimestampsPath);
    }
    private static TaskQueueManager newQueueManager() throws Exception {
        TaskQueueManager queueManager = new TaskQueueManager(1);
        TaskStateRepository repository = mock(TaskStateRepository.class);
        doNothing().when(repository).upsertTask(any(TaskQueueManager.TaskEntry.class));
        when(repository.findTask(anyString())).thenReturn(Optional.empty());
        when(repository.listAllTasks()).thenReturn(List.of());
        injectField(queueManager, "taskStateRepository", repository);
        return queueManager;
    }

    private static void writeStageState(
            Path taskRoot,
            String stage,
            String status,
            String checkpoint,
            String retryMode,
            String requiredAction,
            String retryEntryPoint,
            long updatedAtMs
    ) throws Exception {
        Path stageDir = taskRoot.resolve("intermediates").resolve("rt").resolve("s").resolve(stage);
        Files.createDirectories(stageDir);
        String json = """
                {
                  \"stage\": \"%s\",
                  \"status\": \"%s\",
                  \"checkpoint\": \"%s\",
                  \"retry_mode\": \"%s\",
                  \"required_action\": \"%s\",
                  \"retry_entry_point\": \"%s\",
                  \"updated_at_ms\": %d,
                  \"output_dir\": \"%s\"
                }
                """.formatted(
                stage,
                status,
                checkpoint,
                retryMode,
                requiredAction,
                retryEntryPoint,
                updatedAtMs,
                escapeJson(taskRoot.toAbsolutePath().normalize().toString())
        );
        Files.writeString(stageDir.resolve("stage_state.json"), json, StandardCharsets.UTF_8);
    }

    private static String escapeJson(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static void invokeProcessTask(TaskProcessingWorker worker, TaskEntry task) {
        try {
            Method method = TaskProcessingWorker.class.getDeclaredMethod("processTask", TaskEntry.class);
            method.setAccessible(true);
            method.invoke(worker, task);
        } catch (InvocationTargetException invocationError) {
            Throwable cause = invocationError.getCause();
            if (cause instanceof RuntimeException runtimeError) {
                throw runtimeError;
            }
            throw new RuntimeException(cause);
        } catch (Exception error) {
            throw new RuntimeException(error);
        }
    }


    private static VideoProcessingOrchestrator.IOPhaseResult invokeBuildRecoveredIoPhaseResult(
            TaskProcessingWorker worker,
            TaskEntry task,
            String outputDir,
            TaskRuntimeRecoveryService.ResumeDecision resumeDecision
    ) throws Exception {
        Method method = TaskProcessingWorker.class.getDeclaredMethod(
                "buildRecoveredIoPhaseResult",
                TaskEntry.class,
                String.class,
                TaskRuntimeRecoveryService.ResumeDecision.class
        );
        method.setAccessible(true);
        return (VideoProcessingOrchestrator.IOPhaseResult) method.invoke(worker, task, outputDir, resumeDecision);
    }
    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static void injectField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}